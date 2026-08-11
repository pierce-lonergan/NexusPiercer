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
     * @throws SchemaLimitExceededException if a depth or field-count limit is reached
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
     * plus the number of configured injections, plus ONE ENTRY PER EMITTED LEAF held by the
     * collision guard, which records each rendered name against the source path that produced it
     * so a duplicate can name both paths. That last term is bounded by
     * {@link FlattenOptions#maxFields()} and is the price of the guard being able to say which two
     * columns collided; it applies to {@code flatten} identically.</p>
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
     */
    public void stream(Schema schema, Consumer<FlattenedField> sink) {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(sink, "sink");

        Map<Integer, FlattenedField> injections = options.injectedFields();
        if (injections.isEmpty()) {
            // Explicit short circuit: with nothing to splice, the caller's own sink reaches the
            // traversal unwrapped, so the zero-injection path is byte-identical to what it was
            // before injections were honoured here.
            walk(schema, sink);
            return;
        }
        InjectingSink merge = new InjectingSink(sink, injections);
        walk(schema, merge);
        merge.finish();
    }

    /** The traversal both entry points share. Nothing above this line inspects a field. */
    private void walk(Schema schema, Consumer<FlattenedField> sink) {
        Schema resolved = resolve(schema);
        if (resolved.getType() != Schema.Type.RECORD) {
            throw new SchemaFlattenException(
                    "Top-level schema must be a record; got " + resolved.getType(),
                    String.valueOf(resolved.getFullName()), "");
        }

        Walk walk = new Walk(resolved.getFullName(), sink);
        walk.openTypes.push(resolved.getFullName());
        walkRecord(resolved, "", List.of(), resolved.getDoc(), Map.of(), false, false, 1, walk);
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
        private int next = 1;

        InjectingSink(Consumer<FlattenedField> downstream, Map<Integer, FlattenedField> injections) {
            this.downstream = downstream;
            this.pending = new TreeMap<>(injections);
        }

        @Override
        public void accept(FlattenedField field) {
            // A while, not an if: consecutive head injections at 1 and 2 must BOTH precede the
            // first source field, or the second silently slides behind it.
            while (pending.containsKey(next)) {
                downstream.accept(renumber(pending.remove(next), next, true));
                next++;
            }
            downstream.accept(renumber(field, next, false));
            next++;
        }

        /** Injections positioned beyond the last column: appended in ascending order, no gaps. */
        void finish() {
            while (!pending.isEmpty()) {
                downstream.accept(renumber(pending.pollFirstEntry().getValue(), next, true));
                next++;
            }
        }
    }

    // ------------------------------------------------------------------ traversal

    /** Mutable state for one flatten call. Local so the flattener stays shareable. */
    private static final class Walk {
        final String rootName;
        final Consumer<FlattenedField> sink;
        final Deque<String> openTypes = new ArrayDeque<>();
        /** flattened name -> the source path that first produced it, for collision detection. */
        final Map<String, String> emittedNames = new LinkedHashMap<>();
        int emitted;

        Walk(String rootName, Consumer<FlattenedField> sink) {
            this.rootName = rootName;
            this.sink = sink;
        }
    }

    private void walkRecord(Schema record, String prefix, List<PathSegment> path,
                            String inheritedDoc, Map<String, Object> inheritedProps,
                            boolean withinArray, boolean prefixEndsAtArray, int depth, Walk walk) {

        if (depth > options.maxDepth()) {
            throw new SchemaLimitExceededException("maxDepth", options.maxDepth(), depth,
                    walk.rootName, renderPath(path));
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

        checkCollision(walk, flattenedName, path);

        walk.emitted++;
        if (walk.emitted > options.maxFields()) {
            throw new SchemaLimitExceededException("maxFields", options.maxFields(),
                    walk.emitted, walk.rootName, renderPath(path));
        }

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

        options.leafInterceptor().onLeaf(field);
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
            throw new RecursiveSchemaException(fullName, walk.rootName, renderPath(path));
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
     * Refuses two distinct source paths that render to the same flattened name, under BOTH
     * policies.
     *
     * <p>This used to return before it checked anything whenever the policy was not {@code FAIL},
     * on the stated grounds that "under {@code ESCAPE} the rendering is injective by construction".
     * That is a configuration-dependent claim written as an unconditional one. It holds only while
     * the array-boundary separator is spelled from characters {@code escapeSegment} escapes, and
     * {@link FlattenOptions.Builder#arrayBoundarySeparator(String)} is a free-form setter: with a
     * marker of {@code "x"}, a record containing both an array {@code a} of records with a field
     * {@code b} and a sibling scalar named {@code axb} emitted two leaves called {@code axb} with
     * no diagnostic at all.</p>
     *
     * <p>So the surviving guarantee — injectivity — is now verified rather than assumed. Under
     * every configuration in the corpus, and under the defaults, this can never fire; that is
     * precisely the property worth asserting instead of believing.</p>
     */
    private void checkCollision(Walk walk, String flattenedName, List<PathSegment> path) {
        String source = renderPath(path);
        String previous = walk.emittedNames.putIfAbsent(flattenedName, source);
        if (previous != null && !previous.equals(source)) {
            throw new SchemaFlattenException(
                    collisionMessage(walk.rootName, previous, source, flattenedName),
                    walk.rootName, source);
        }
    }

    /**
     * The two diagnostics, kept apart on purpose.
     *
     * <p>The {@code FAIL} text is byte-for-byte what it has always been: it is recorded verbatim
     * by the fidelity corpus, truncated mid-word at 240 characters, so merging the two policies
     * into one shared template would turn that row red for no behavioural reason. The
     * {@code ESCAPE} text has to be different anyway — telling a caller who is already using
     * {@code ESCAPE} to use {@code ESCAPE} is not advice.</p>
     */
    private String collisionMessage(String rootName, String previous, String source, String name) {
        if (options.collisionPolicy() == NameCollisionPolicy.FAIL) {
            return String.format(
                    "Flattened name collision in schema '%s': source paths '%s' and '%s' both "
                            + "render to '%s'. Joining segments with '%s' is ambiguous when a "
                            + "field name contains it. Rename a field, choose a separator absent "
                            + "from your names, or use NameCollisionPolicy.ESCAPE if these names "
                            + "are not destined to become Avro or SQL identifiers.",
                    rootName, previous, source, name, options.separator());
        }
        return String.format(
                "Flattened name collision in schema '%s' under the ESCAPE policy: source paths "
                        + "'%s' and '%s' both render to '%s'. Segment escaping cannot separate "
                        + "them, because the configured arrayBoundarySeparator '%s' is emitted "
                        + "outside the escaped alphabet and can therefore be spelled by an "
                        + "ordinary field name. Choose an arrayBoundarySeparator built from the "
                        + "separator character '%s' - '%s' is the default and no field name can "
                        + "forge it.",
                rootName, previous, source, name, options.arrayBoundarySeparator(),
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
