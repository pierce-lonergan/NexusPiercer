package io.github.pierce.schema;

import io.github.pierce.path.FlattenedPath;
import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

    /** Avro property names that are structural, not user metadata. */
    private static final Set<String> RESERVED_PROPS =
            Set.of("type", "name", "namespace", "fields", "items", "values", "symbols",
                   "size", "aliases", "doc", "default", "order", "logicalType");

    private final FlattenOptions options;

    public EnrichedSchemaFlattener() {
        this(FlattenOptions.gAvroParity());
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
        return applyInjections(out);
    }

    /**
     * Streams leaves to a consumer without materialising the result list.
     *
     * <p>For very wide schemas — generated types running to hundreds or thousands of columns —
     * this keeps peak memory proportional to depth rather than to field count. The traversal is
     * identical; only the accumulation differs.</p>
     *
     * <p>Injected fields are NOT applied here: splicing by position requires knowing the full
     * sequence, which is exactly what streaming declines to hold. Use {@link #flatten(Schema)}
     * when positional injection matters. Stated rather than silently ignored, because a caller
     * who configured injections and got none would have no way to tell.</p>
     */
    public void stream(Schema schema, Consumer<FlattenedField> sink) {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(sink, "sink");

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

        String recordDoc = record.getDoc() != null ? record.getDoc() : inheritedDoc;
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
     * Refuses two distinct source paths that render to the same flattened name.
     *
     * <p>Only meaningful under {@code FAIL}; under {@code ESCAPE} the rendering is injective by
     * construction and no two distinct paths can collide.</p>
     */
    private void checkCollision(Walk walk, String flattenedName, List<PathSegment> path) {
        if (options.collisionPolicy() != NameCollisionPolicy.FAIL) {
            return;
        }
        String source = renderPath(path);
        String previous = walk.emittedNames.putIfAbsent(flattenedName, source);
        if (previous != null && !previous.equals(source)) {
            throw new SchemaFlattenException(String.format(
                    "Flattened name collision in schema '%s': source paths '%s' and '%s' both "
                            + "render to '%s'. Joining segments with '%s' is ambiguous when a "
                            + "field name contains it. Rename a field, choose a separator absent "
                            + "from your names, or use NameCollisionPolicy.ESCAPE if these names "
                            + "are not destined to become Avro or SQL identifiers.",
                    walk.rootName, previous, source, flattenedName, options.separator()),
                    walk.rootName, source);
        }
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
     * Every non-reserved property declared on a field or schema.
     *
     * <p>Avro permits arbitrary JSON properties and most readers discard them. Anything a producer
     * bothered to declare means something to somebody, so nothing is filtered by name — a library
     * that only preserved properties it recognised would silently drop the one the caller cared
     * about.</p>
     */
    private static Map<String, Object> customProps(Schema.Field field) {
        Map<String, Object> out = new LinkedHashMap<>();
        field.getObjectProps().forEach((k, v) -> {
            if (!RESERVED_PROPS.contains(k)) {
                out.put(k, v);
            }
        });
        return out;
    }

    private static Map<String, Object> customProps(Schema schema) {
        Map<String, Object> out = new LinkedHashMap<>();
        schema.getObjectProps().forEach((k, v) -> {
            if (!RESERVED_PROPS.contains(k)) {
                out.put(k, v);
            }
        });
        return out;
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
     * Splices injected fields in at their 1-based positions, renumbering as it goes.
     *
     * <p>Source fields keep their relative order. An external contract that depends on a column's
     * index must not be broken by an upstream schema edit, so injection inserts around the source
     * sequence rather than interleaving into it.</p>
     */
    private List<FlattenedField> applyInjections(List<FlattenedField> source) {
        Map<Integer, FlattenedField> injections = options.injectedFields();
        if (injections.isEmpty()) {
            return Collections.unmodifiableList(source);
        }

        List<FlattenedField> out = new ArrayList<>(source.size() + injections.size());
        Map<Integer, FlattenedField> pending = new TreeMap<>(injections);
        int next = 1;
        int sourceIndex = 0;

        while (sourceIndex < source.size() || !pending.isEmpty()) {
            FlattenedField injected = pending.remove(next);
            if (injected != null) {
                out.add(renumber(injected, next, true));
            } else if (sourceIndex < source.size()) {
                out.add(renumber(source.get(sourceIndex++), next, false));
            } else {
                // Positions beyond the end: append in ascending order rather than leaving gaps.
                Integer first = ((TreeMap<Integer, FlattenedField>) pending).firstKey();
                out.add(renumber(pending.remove(first), next, true));
            }
            next++;
        }
        return Collections.unmodifiableList(out);
    }

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
