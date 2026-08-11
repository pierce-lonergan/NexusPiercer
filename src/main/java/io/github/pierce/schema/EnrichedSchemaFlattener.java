package io.github.pierce.schema;

import io.github.pierce.path.FlattenedPath;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * Flattens an Avro schema into enriched leaves.
 *
 * <h2>How this differs from {@code AvroSchemaFlattener}</h2>
 *
 * <p>{@code AvroSchemaFlattener} emits Avro {@link Schema.Field}s and answers one question: what
 * are the flattened column names and types. That is enough to build a Spark {@code StructType}
 * and nothing more. Everything a schema-governance pipeline needs — the producer's custom
 * properties, inherited documentation, whether an array was crossed, the parent record's name —
 * is discarded during the walk and cannot be recovered from the output, because a flattened name
 * is not a parseable structure.</p>
 *
 * <p>This flattener keeps all of it, and adds the seams that let a caller enrich in the same pass:
 * a {@link TypeMapper} for the target's own type system, a {@link LeafInterceptor} for per-leaf
 * annotation, and positional injection for synthetic columns.</p>
 *
 * <h2>Safety</h2>
 *
 * <p>Recursion is detected on the way down by tracking which named types are currently open, so a
 * self-referential schema raises {@link RecursiveSchemaException} at the point of recursion rather
 * than exhausting the stack. Depth and field-count limits raise
 * {@link SchemaLimitExceededException}. Both are typed and name the schema, so a caller
 * flattening untrusted schemas in bulk can quarantine one and continue.</p>
 *
 * <p>Note the distinction the detector preserves: a type used <em>twice</em> is fine — a record
 * with two {@code Address} fields is ordinary and flattens to two branches. Only a type that
 * contains <em>itself</em> is unflattenable. Tracking the open set rather than the seen set is
 * what keeps the common case working.</p>
 *
 * <h2>Thread safety</h2>
 *
 * <p>Immutable and safe to share. All traversal state is local to the call, which is also why it
 * is safe to capture into a Spark closure.</p>
 */
public final class EnrichedSchemaFlattener implements Serializable {

    private static final long serialVersionUID = 1L;

    private final FlattenOptions options;

    /**
     * A flattener on this library's own default conventions.
     *
     * <p>Deliberately {@link FlattenOptions#defaults()} and not
     * {@link FlattenOptions#gAvroParity()}. The parity factory escapes every segment and joins
     * arrays with a single separator, which is right for a caller migrating off
     * {@code GAvroSchemaFlattener} and wrong as a resting state: the escape character is illegal
     * in Avro and SQL identifiers. Pointing the zero-config entry point at the parity factory
     * would turn a migration aid into the library's default output.</p>
     */
    public EnrichedSchemaFlattener() {
        this(FlattenOptions.defaults());
    }

    public EnrichedSchemaFlattener(FlattenOptions options) {
        this.options = Objects.requireNonNull(options, "options");
    }

    public FlattenOptions options() {
        return options;
    }

    /**
     * Flattens to a list, with any injected fields spliced in at their positions.
     *
     * @throws RecursiveSchemaException     if a named type contains itself
     * @throws SchemaLimitExceededException if the depth limit is reached, or if the number of
     *                                      columns produced — source leaves and injected columns
     *                                      together — exceeds {@link FlattenOptions#maxFields()}
     * @throws SchemaFlattenException       if the top-level schema is not a record, or if two
     *                                      columns would be emitted under one flattened name.
     *                                      Under BOTH collision policies, and for injected columns
     *                                      as well as source ones: see {@link NameCollisionPolicy}
     */
    public List<FlattenedField> flatten(Schema schema) {
        List<FlattenedField> out = new ArrayList<>();
        stream(schema, out::add);
        return Collections.unmodifiableList(out);
    }

    /**
     * Streams leaves to a consumer without materialising the result list.
     *
     * <p>For very wide schemas — generated types running to hundreds or thousands of columns —
     * this avoids materialising the result list. Stated exactly, because an earlier version of
     * this sentence promised more than the code delivers: peak memory is proportional to depth,
     * plus the number of configured injections, plus ONE ENTRY PER EMITTED COLUMN held by the
     * collision guard, which records each rendered name against the origin that produced it — a
     * source path, or the {@code injectField} position — so a duplicate can name both. That last
     * term is bounded by {@link FlattenOptions#maxFields()} and is the price of the guard being
     * able to say which two columns collided; it applies to {@code flatten} identically.</p>
     *
     * <p>Injected fields ARE applied here, in the same order and at the same final positions
     * {@link #flatten(Schema)} produces, including the rule that an injection positioned past the
     * final column is appended in ascending order rather than leaving a gap. Splicing by position
     * needs only the current output index and the injection map — never the total field count — so
     * peak memory stays proportional to depth plus the number of injections, which the
     * configuration already holds. An earlier version of this javadoc claimed otherwise and
     * skipped the stage; the claim was false and the columns were silently dropped.</p>
     *
     * <p>The one property NOT guaranteed is identical interleaving of
     * {@link LeafInterceptor} callbacks with delivery to the sink: {@code flatten} materialises,
     * so it cannot match a streaming interleaving and does not claim to. The field SEQUENCE the
     * two produce is identical.</p>
     *
     * <p>Injected fields are handed to the sink as rebuilt instances carrying their final
     * position, exactly as {@code flatten} has always done, so a caller holding the reference it
     * passed to {@code injectField} is not looking at the object the sink receives.</p>
     *
     * <p>They are also GUARDED like source columns, which they were not: an injected name is
     * claimed in the same registry the traversal claims into, and counts against
     * {@link FlattenOptions#maxFields()}. See {@link Output}.</p>
     *
     * @throws RecursiveSchemaException     if a named type contains itself
     * @throws SchemaLimitExceededException if the depth limit is reached, or if the number of
     *                                      columns produced — source leaves and injected columns
     *                                      together — exceeds {@link FlattenOptions#maxFields()}
     * @throws SchemaFlattenException       if the top-level schema is not a record, or if two
     *                                      columns would be emitted under one flattened name
     */
    public void stream(Schema schema, Consumer<FlattenedField> sink) {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(sink, "sink");

        Schema root = resolve(schema);
        if (root.getType() != Schema.Type.RECORD) {
            throw new SchemaFlattenException(
                    "Top-level schema must be a record; got " + root.getType(),
                    String.valueOf(root.getFullName()), "");
        }

        // ONE registry per call, shared by the traversal and by the injection merge. They are the
        // two places a column reaches the caller's sink, and a guard only one of them consults is
        // not a guard - which is precisely what the collision check and the field-count ceiling
        // used to be, under both policies and through both entry points.
        Output out = new Output(root.getFullName(), options);

        // The interceptor wraps the CALLER'S sink, outside the injection merge, so it sees every
        // column that reaches the caller: source leaves at their final positions and injected
        // columns too, in delivery order. It used to be called inside emit(), which put it on the
        // wrong side of the merge — it never saw an injected column at all, and the position() it
        // read was the pre-renumber source ordinal rather than the one the caller receives. Its
        // javadoc said "in emission order" throughout.
        //
        // Wrapped here rather than inside InjectingSink because the zero-injection path skips the
        // merge entirely; interception placed there would have been silently absent for every
        // caller who configured no injections, which is most of them.
        Consumer<FlattenedField> delivery = intercepting(sink);

        Map<Integer, FlattenedField> injections = options.injectedFields();
        if (injections.isEmpty()) {
            // Explicit short circuit: with nothing to splice, no merge is allocated and no field
            // is rebuilt. Every column it produces still passes through Output.
            walk(root, out, delivery);
            return;
        }
        InjectingSink merge = new InjectingSink(delivery, injections, out);
        walk(root, out, merge);
        merge.finish();
    }

    /**
     * Wraps a sink so every delivered column is offered to the configured {@link LeafInterceptor}
     * first.
     *
     * <p>Returns the sink unchanged when no interceptor was configured, so the common path adds no
     * frame and the zero-injection case still reaches the traversal as the caller's own object.
     * The test asserting that lives in {@code EnrichedSchemaFlattenerTest}; an earlier draft of
     * this javadoc claimed the fidelity corpus asserted it, which was not true of any fixture.</p>
     *
     * <p>Keyed on {@link FlattenOptions#hasLeafInterceptor()} rather than on comparing the
     * interceptor against the no-op singleton. Reference comparison would have worked, but only
     * because {@code noop()} is now interned — which it was not until this change, and a
     * correctness argument that rests on an invariant established three commits ago is one nobody
     * will re-check.</p>
     */
    private Consumer<FlattenedField> intercepting(Consumer<FlattenedField> sink) {
        if (!options.hasLeafInterceptor()) {
            return sink;
        }
        LeafInterceptor interceptor = options.leafInterceptor();
        return field -> {
            interceptor.onLeaf(field);
            sink.accept(field);
        };
    }

    /** The traversal both entry points share. Nothing above this line inspects a field. */
    private void walk(Schema root, Output out, Consumer<FlattenedField> sink) {
        Walk walk = new Walk(out, sink);
        walk.openTypes.push(root.getFullName());
        walkRecord(root, "", List.of(), root.getDoc(), Map.of(), false, false, 1, walk);
    }

    /**
     * The streaming form of positional injection: a monotone merge over the OUTPUT index.
     *
     * <p>{@code next} visits 1, 2, 3, … with no gaps, so every claimed position is tested at the
     * moment it comes up and an injection can never be "already passed". Nothing here needs the
     * source length; only {@link #finish()} needs end of input, and the traversal owns that.</p>
     *
     * <p>Static, not inner: the enclosing flattener is {@link Serializable}, and an inner class or
     * a capturing lambda would drag it into anything that captured the sink.</p>
     */
    private static final class InjectingSink implements Consumer<FlattenedField> {

        private final Consumer<FlattenedField> downstream;
        /** Navigable rather than TreeMap: {@link #finish()} needs pollFirstEntry, nothing more. */
        private final NavigableMap<Integer, FlattenedField> pending;
        /** The registry the traversal claims into. Shared, not a second one. */
        private final Output out;
        private int next = 1;

        InjectingSink(Consumer<FlattenedField> downstream, Map<Integer, FlattenedField> injections,
                      Output out) {
            this.downstream = downstream;
            this.pending = new TreeMap<>(injections);
            this.out = out;
        }

        @Override
        public void accept(FlattenedField field) {
            // A while, not an if: consecutive head injections at 1 and 2 must BOTH precede the
            // first source field, or the second silently slides behind it.
            while (pending.containsKey(next)) {
                deliver(pending.remove(next), next);
            }
            // Not claimed here: emit() already claimed this column on the way in. Claiming it a
            // second time would collide with itself.
            downstream.accept(renumber(field, next, false));
            next++;
        }

        /** Injections positioned beyond the last column: appended in ascending order, no gaps. */
        void finish() {
            while (!pending.isEmpty()) {
                Map.Entry<Integer, FlattenedField> head = pending.pollFirstEntry();
                deliver(head.getValue(), head.getKey());
            }
        }

        /**
         * Hands one injected column downstream, after claiming its name and its slot.
         *
         * <p>Shared by {@link #accept} and {@link #finish} deliberately: the append-past-the-end
         * branch is the arm a guard bolted onto {@code accept} alone would miss, and an unguarded
         * arm of a guard is the shape of defect this package is being repaired for.</p>
         *
         * @param declaredPosition the position the caller CLAIMED, which is what the diagnostic
         *                         must name — {@code next} is where the column actually lands, and
         *                         for an appended injection the two differ
         */
        private void deliver(FlattenedField injected, int declaredPosition) {
            out.claim(injected.flattenedName(), Origin.injected(declaredPosition));
            downstream.accept(renumber(injected, next, true));
            next++;
        }
    }

    // ------------------------------------------------------------------ output-side guards

    /**
     * Where a column came from, so a collision diagnostic can name both sides accurately.
     *
     * <p>A source path and an {@code injectField} position are not the same kind of thing and a
     * message that called an injected column a "source path" would send the reader looking through
     * an {@code .avsc} for a field that is not in it.</p>
     */
    private record Origin(String label, boolean injected) {

        static Origin sourcePath(String path) {
            return new Origin(path, false);
        }

        static Origin injected(int position) {
            return new Origin("injectField(" + position + ")", true);
        }

        /** Reads as a noun phrase, so both forms drop into one sentence. */
        String describe() {
            return injected ? "the column injected by " + label : "source path '" + label + "'";
        }
    }

    /**
     * The two guards that belong to the OUTPUT, owned in one place because they apply to every
     * column the caller receives.
     *
     * <p>Both used to live inside {@code emit}, which only the traversal calls, so both were blind
     * to {@code injectField}. Measured before this repair, on {@code Row{order_id, amount}}:
     * {@code injectField(1, "order_id")} returned {@code [order_id, order_id, amount]} under
     * {@link NameCollisionPolicy#FAIL} — whose entire published job is refusing exactly that — and
     * {@code maxFields(2)} with two injections returned four columns. Neither produced a
     * diagnostic. Both entry points did it identically, so the agreement between {@code flatten}
     * and {@code stream} was agreement on the wrong answer.</p>
     *
     * <p>Static, and holding {@link FlattenOptions} rather than the flattener: the enclosing class
     * is {@link Serializable} and this object is reachable from the sink handed to the traversal.</p>
     */
    private static final class Output {

        private final String rootName;
        private final FlattenOptions options;
        /** Flattened name -> the origin that first claimed it. One entry per emitted column. */
        private final Map<String, Origin> claimed = new LinkedHashMap<>();
        /** Columns delivered to the caller: source leaves and injected columns alike. */
        private int columns;

        Output(String rootName, FlattenOptions options) {
            this.rootName = rootName;
            this.options = options;
        }

        /**
         * Claims one output column, or refuses it.
         *
         * <p>The collision half used to return before it checked anything whenever the policy was
         * not {@code FAIL}, on the stated grounds that "under {@code ESCAPE} the rendering is
         * injective by construction". That is a configuration-dependent claim written as an
         * unconditional one: it holds only while the array-boundary separator is spelled from
         * characters {@code escapeSegment} escapes, and
         * {@link FlattenOptions.Builder#arrayBoundarySeparator(String)} is a free-form setter.
         * It now runs under both policies, and for injected columns as well as source ones, so the
         * surviving guarantee — no two emitted columns share a name — is verified rather than
         * believed. Under the defaults and under every configuration in the fidelity corpus it can
         * never fire; that is precisely the property worth asserting instead of assuming.</p>
         *
         * <p>There is no {@code previous.equals(origin)} escape hatch, and its absence is
         * deliberate. An origin identifies exactly one column — a source path names one route
         * through one schema, an injection origin carries its declared position, and the position
         * map cannot hold the same position twice — so a repeated origin could only ever mean a
         * genuine duplicate arriving twice.</p>
         */
        void claim(String flattenedName, Origin origin) {
            Origin previous = claimed.putIfAbsent(flattenedName, origin);
            if (previous != null) {
                throw new SchemaFlattenException(
                        collisionMessage(options, rootName, previous, origin, flattenedName),
                        rootName, origin.label());
            }
            columns++;
            if (columns > options.maxFields()) {
                throw new SchemaLimitExceededException("maxFields", options.maxFields(),
                        columns, rootName, origin.label());
            }
        }
    }

    // ------------------------------------------------------------------ traversal

    /** Mutable state for one flatten call. Local so the flattener stays shareable. */
    private static final class Walk {
        final Output out;
        final Consumer<FlattenedField> sink;
        final Deque<String> openTypes = new ArrayDeque<>();
        /**
         * SOURCE leaves emitted so far, which is what {@code position()} reports on the way out.
         * Deliberately not the output-column count {@link Output} keeps: with injections the two
         * differ, and the merge renumbers to the final index anyway.
         */
        int emitted;

        Walk(Output out, Consumer<FlattenedField> sink) {
            this.out = out;
            this.sink = sink;
        }

        String rootName() {
            return out.rootName;
        }
    }

    private void walkRecord(Schema record, String prefix, List<PathSegment> path,
                            String inheritedDoc, Map<String, Object> inheritedProps,
                            boolean withinArray, boolean prefixEndsAtArray, int depth, Walk walk) {

        if (depth > options.maxDepth()) {
            throw new SchemaLimitExceededException("maxDepth", options.maxDepth(), depth,
                    walk.rootName(), renderPath(path));
        }

        // Gated at the SOURCE, not at the point of use: with inheritance off nothing inherited is
        // carried down the walk at all, so there is one branch to reason about rather than two and
        // no path by which an ancestor's doc can reach a leaf. A leaf's OWN doc still wins below —
        // the flag turns inheritance off, not documentation off.
        String recordDoc = !options.inheritDoc()
                ? null
                : (record.getDoc() != null ? record.getDoc() : inheritedDoc);
        Map<String, Object> recordProps = options.inheritRecordProperties()
                ? merge(inheritedProps, customProps(record))
                : inheritedProps;

        for (Schema.Field field : record.getFields()) {
            Schema fieldSchema = field.schema();
            boolean nullable = isNullableUnion(fieldSchema);
            Schema resolved = options.unwrapNullableUnions() ? resolve(fieldSchema) : fieldSchema;

            String fieldDoc = field.doc() != null ? field.doc() : recordDoc;
            boolean docInherited = field.doc() == null && fieldDoc != null;

            Map<String, Object> props = merge(recordProps, customProps(field));

            switch (resolved.getType()) {
                case RECORD -> {
                    String childPrefix = join(prefix, field.name(), prefixEndsAtArray);
                    List<PathSegment> childPath =
                            append(path, PathSegment.field(field.name(), record.getName()));
                    guardRecursion(resolved, childPath, walk);
                    walk.openTypes.push(resolved.getFullName());
                    walkRecord(resolved, childPrefix, childPath, fieldDoc, props,
                            withinArray, false, depth + 1, walk);
                    walk.openTypes.pop();
                }
                case ARRAY -> {
                    Schema element = resolve(resolved.getElementType());
                    String childPrefix = join(prefix, field.name(), prefixEndsAtArray);
                    List<PathSegment> childPath =
                            append(path, PathSegment.array(field.name(), record.getName()));

                    if (element.getType() == Schema.Type.RECORD) {
                        guardRecursion(element, childPath, walk);
                        walk.openTypes.push(element.getFullName());
                        walkRecord(element, childPrefix, childPath, fieldDoc, props,
                                true, true, depth + 1, walk);
                        walk.openTypes.pop();
                    } else {
                        // Array of scalars: one leaf holding the serialised elements.
                        emit(walk, childPrefix, field.name(), childPath, fieldDoc, docInherited,
                                element, nullable, true, props);
                    }
                }
                case MAP -> {
                    // A map's keys are runtime data, not schema, so its contents cannot be
                    // flattened into named columns. Emit the map itself and let the caller decide.
                    List<PathSegment> childPath =
                            append(path, PathSegment.map(field.name(), record.getName()));
                    emit(walk, join(prefix, field.name(), prefixEndsAtArray), field.name(), childPath,
                            fieldDoc, docInherited, resolved, nullable, withinArray, props);
                }
                default -> emit(walk, join(prefix, field.name(), prefixEndsAtArray), field.name(),
                        append(path, PathSegment.field(field.name(), record.getName())),
                        fieldDoc, docInherited, resolved, nullable, withinArray, props);
            }
        }
    }

    private void emit(Walk walk, String flattenedName, String name, List<PathSegment> path,
                      String doc, boolean docInherited, Schema resolved,
                      boolean nullable, boolean withinArray, Map<String, Object> props) {

        walk.out.claim(flattenedName, Origin.sourcePath(renderPath(path)));
        walk.emitted++;

        FlattenedField field = FlattenedField.builder()
                .flattenedName(flattenedName)
                .name(name)
                .path(path)
                .doc(doc)
                .docInherited(docInherited)
                .schema(resolved)
                .avroType(resolved.getType())
                .nullable(nullable)
                .withinArray(withinArray)
                .position(walk.emitted)
                .properties(props)
                .build();

        String mapped = options.typeMapper().map(field);
        if (mapped != null) {
            // Rebuilt rather than mutated: the mapper sees a complete field, and everything
            // except the property map stays immutable.
            field = rebuildWithMappedType(field, mapped);
        }

        // No onLeaf call here on purpose: interception happens at delivery, in stream(), so the
        // interceptor sees the field the caller sees — final position, injected columns included.
        walk.sink.accept(field);
    }

    private static FlattenedField rebuildWithMappedType(FlattenedField f, String mappedType) {
        return FlattenedField.builder()
                .flattenedName(f.flattenedName())
                .name(f.name())
                .path(f.pathSegments())
                .doc(f.doc().orElse(null))
                .docInherited(f.isDocInherited())
                .schema(f.schema())
                .avroType(f.avroType())
                .mappedType(mappedType)
                .nullable(f.isNullable())
                .withinArray(f.isWithinArray())
                .position(f.position())
                .synthetic(f.isSynthetic())
                .properties(f.properties())
                .build();
    }

    // ------------------------------------------------------------------ helpers

    /**
     * Fails when a named type contains itself.
     *
     * <p>Checks the OPEN set, not everything seen. A record with two {@code Address} fields uses
     * the type twice and flattens fine; only a type nested inside itself is unflattenable.
     * Conflating the two would reject a great many ordinary schemas.</p>
     */
    private void guardRecursion(Schema named, List<PathSegment> path, Walk walk) {
        String fullName = named.getFullName();
        if (fullName != null && walk.openTypes.contains(fullName)) {
            throw new RecursiveSchemaException(fullName, walk.rootName(), renderPath(path));
        }
    }

    /**
     * Joins one segment onto a prefix.
     *
     * <p>{@code afterArray} selects the array-boundary separator, and refers to whether the
     * PREFIX ended at an array — the marker belongs between the array and its children, not
     * before the array itself. Getting that backwards renders {@code items__sku} as
     * {@code __items_sku}, which points the marker at the wrong join.</p>
     *
     * <p>Escaping happens only under {@link NameCollisionPolicy#ESCAPE}. Under the default
     * {@code FAIL} the name is emitted verbatim and ambiguity is caught by
     * {@link #checkCollision}: these names become Avro and SQL identifiers, where the escape
     * character is illegal, so escaping would produce a schema that cannot be constructed.</p>
     *
     * <p>Note what the {@code ESCAPE} arm does <em>not</em> cover. {@code escapeSegment} is called
     * with {@link FlattenOptions#separator()} only, so the array-boundary separator is emitted
     * OUTSIDE the escaped alphabet: an unescaped run of the separator character in a rendered name
     * may be structural, and the name is therefore not decodable across an array boundary. That is
     * a deliberate, documented limit — see {@link NameCollisionPolicy#ESCAPE} — and it is also why
     * {@link #checkCollision} must run under both policies rather than assuming injectivity.</p>
     */
    private String join(String prefix, String name, boolean afterArray) {
        String segment = options.collisionPolicy() == NameCollisionPolicy.ESCAPE
                ? FlattenedPath.escapeSegment(name, options.separator())
                : name;
        if (prefix == null || prefix.isEmpty()) {
            return segment;
        }
        String sep = afterArray ? options.arrayBoundarySeparator() : options.separator();
        return prefix + sep + segment;
    }

    /**
     * The three diagnostics, kept apart on purpose.
     *
     * <p>The {@code FAIL} text is byte-for-byte what it has always been: it is recorded verbatim
     * by the fidelity corpus, truncated mid-word at 240 characters, so merging the policies into
     * one shared template would turn that row red for no behavioural reason. The {@code ESCAPE}
     * text has to be different anyway — telling a caller who is already using {@code ESCAPE} to
     * use {@code ESCAPE} is not advice. And an injected column gets its own text under both
     * policies, because neither policy's advice applies: no separator choice and no escaping can
     * move a name the caller typed.</p>
     */
    private static String collisionMessage(FlattenOptions options, String rootName,
                                           Origin previous, Origin current, String name) {
        if (previous.injected() || current.injected()) {
            return String.format(
                    "Flattened name collision in schema '%s': %s and %s both produce the column "
                            + "name '%s'. injectField() de-duplicates POSITIONS, not names, and an "
                            + "injected column is an emitted column - two columns under one name "
                            + "is not a schema. Rename the injected column, or drop it and let the "
                            + "source field through.",
                    rootName, previous.describe(), current.describe(), name);
        }
        if (options.collisionPolicy() == NameCollisionPolicy.FAIL) {
            return String.format(
                    "Flattened name collision in schema '%s': source paths '%s' and '%s' both "
                            + "render to '%s'. Joining segments with '%s' is ambiguous when a "
                            + "field name contains it. Rename a field, choose a separator absent "
                            + "from your names, or use NameCollisionPolicy.ESCAPE if these names "
                            + "are not destined to become Avro or SQL identifiers.",
                    rootName, previous.label(), current.label(), name, options.separator());
        }
        return String.format(
                "Flattened name collision in schema '%s' under the ESCAPE policy: source paths "
                        + "'%s' and '%s' both render to '%s'. Segment escaping cannot separate "
                        + "them, because the configured arrayBoundarySeparator '%s' is emitted "
                        + "outside the escaped alphabet and can therefore be spelled by an "
                        + "ordinary field name. Choose an arrayBoundarySeparator built from the "
                        + "separator character '%s' - doubling it to '%s' is enough, because "
                        + "segment escaping escapes that character and no field name can forge an "
                        + "unescaped run of it.",
                rootName, previous.label(), current.label(), name,
                options.arrayBoundarySeparator(),
                options.separator(), options.separator() + options.separator());
    }

    private static List<PathSegment> append(List<PathSegment> path, PathSegment segment) {
        List<PathSegment> next = new ArrayList<>(path.size() + 1);
        next.addAll(path);
        next.add(segment);
        return next;
    }

    private static String renderPath(List<PathSegment> path) {
        return String.join(".", path.stream().map(PathSegment::name).toList());
    }

    /** Unwraps a union to its single non-null branch; returns the schema unchanged otherwise. */
    private static Schema resolve(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        List<Schema> nonNull = schema.getTypes().stream()
                .filter(t -> t.getType() != Schema.Type.NULL)
                .toList();
        return nonNull.size() == 1 ? nonNull.get(0) : schema;
    }

    private static boolean isNullableUnion(Schema schema) {
        return schema.getType() == Schema.Type.UNION
                && schema.getTypes().stream().anyMatch(t -> t.getType() == Schema.Type.NULL);
    }

    /**
     * Every custom property declared on a field or schema, filtered by nobody.
     *
     * <p>Avro permits arbitrary JSON properties and most readers discard them. Anything a producer
     * bothered to declare means something to somebody, so nothing is filtered by name — a library
     * that only preserved properties it recognised would silently drop the one the caller cared
     * about.</p>
     *
     * <h2>Why there is no reserved-name list here</h2>
     *
     * <p>There was one, and it was both dead code and a data-loss bug at the same time. The split
     * between "structural attribute" and "user metadata" has already been performed, correctly and
     * <em>contextually</em>, by Avro's parser: structural keys are consumed into typed accessors
     * and only the remainder reaches {@code getObjectProps()}. The reserved set differs per
     * position — measured against avro 1.12.0,
     * {@code Schema.Field.FIELD_RESERVED = [default, aliases, name, doc, type, order]},
     * {@code Schema.SCHEMA_RESERVED = [aliases, size, values, name, namespace, doc, fields, type,
     * items, symbols]}, {@code ENUM_RESERVED} is the latter plus {@code default}, and
     * {@code logicalType} is in none of them.</p>
     *
     * <p>So the same name gets opposite verdicts depending on where it sits: {@code size} is
     * structural on a {@code fixed} schema (Avro keeps it out of the prop map and exposes
     * {@code getFixedSize()}) and ordinary metadata on a record field; {@code default} and
     * {@code order} are structural on a field and ordinary metadata on a record. A filter keyed on
     * a bare {@code String} does not have the position and therefore cannot be right: every entry
     * it could carry is either unreachable — the parser already removed it — or a silent drop of
     * something a producer wrote. There is no third possibility, which is why the list was deleted
     * rather than narrowed.</p>
     *
     * <p>The copy is defensive on purpose: {@code getObjectProps()} returns Avro's own map, and
     * the result is threaded onward as {@code inheritedProps} and can be returned by identity from
     * {@link #merge}.</p>
     */
    private static Map<String, Object> customProps(JsonProperties props) {
        return new LinkedHashMap<>(props.getObjectProps());
    }

    /** Child properties shadow inherited ones. */
    private static Map<String, Object> merge(Map<String, Object> inherited,
                                             Map<String, Object> own) {
        if (inherited.isEmpty()) {
            return own;
        }
        if (own.isEmpty()) {
            return inherited;
        }
        Map<String, Object> merged = new LinkedHashMap<>(inherited);
        merged.putAll(own);
        return merged;
    }

    /**
     * Rebuilds a field at its final 1-based position in the emitted sequence.
     *
     * <p>Source fields keep their relative order. An external contract that depends on a column's
     * index must not be broken by an upstream schema edit, so injection inserts around the source
     * sequence rather than interleaving into it.</p>
     */
    private static FlattenedField renumber(FlattenedField f, int position, boolean synthetic) {
        return FlattenedField.builder()
                .flattenedName(f.flattenedName())
                .name(f.name())
                .path(f.pathSegments())
                .doc(f.doc().orElse(null))
                .docInherited(f.isDocInherited())
                .schema(f.schema())
                .avroType(f.avroType())
                .mappedType(f.mappedType().orElse(null))
                .nullable(f.isNullable())
                .withinArray(f.isWithinArray())
                .position(position)
                .synthetic(synthetic || f.isSynthetic())
                .properties(f.properties())
                .build();
    }
}
