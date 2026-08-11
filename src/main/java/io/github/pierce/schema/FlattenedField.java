package io.github.pierce.schema;

import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * One leaf of a flattened schema, with everything a consumer needs to enrich it.
 *
 * <p>The existing {@code AvroSchemaFlattener} emits Avro {@link Schema.Field}s, which carry the
 * flattened name and the type and nothing else. Every downstream concern — custom properties,
 * inherited documentation, whether an array was crossed, what the parent record was called — has
 * to be recovered by parsing the name, and a name is not a reliable place to recover structure
 * from.</p>
 *
 * <p>So this is the structural half of a schema pipeline's field model, deliberately stopping
 * short of the governance half. It carries {@code properties}, {@code doc}, provenance and types;
 * it does not carry glossary ids, DEID tags or match confidences. Those belong to the caller, and
 * a library that invented fields for them would be guessing at someone else's domain. The
 * {@link #properties()} map and {@link FlattenOptions.Builder#leafInterceptor(LeafInterceptor)}
 * are the seams where that enrichment attaches.</p>
 *
 * <p>Instances are immutable except for {@link #properties()}, which is deliberately mutable so a
 * {@link LeafInterceptor} can annotate a leaf in the same pass that produced it rather than
 * forcing a second sweep.</p>
 */
public final class FlattenedField implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String flattenedName;
    private final String name;
    private final List<PathSegment> path;
    private final String doc;
    private final boolean docInherited;
    private final Schema.Type avroType;
    private final Schema schema;
    private final String mappedType;
    private final boolean nullable;
    private final boolean withinArray;
    private final int position;
    private final boolean synthetic;
    private final Map<String, Object> properties;

    private FlattenedField(Builder b) {
        this.flattenedName = Objects.requireNonNull(b.flattenedName, "flattenedName");
        this.name = Objects.requireNonNull(b.name, "name");
        this.path = List.copyOf(b.path);
        this.doc = b.doc;
        this.docInherited = b.docInherited;
        this.avroType = b.avroType;
        this.schema = b.schema;
        this.mappedType = b.mappedType;
        this.nullable = b.nullable;
        this.withinArray = b.withinArray;
        this.position = b.position;
        this.synthetic = b.synthetic;
        // Mutable on purpose — see the class doc.
        this.properties = b.properties;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * The rendered column name.
     *
     * <p>Escaped only under {@link NameCollisionPolicy#ESCAPE}; under the default {@code FAIL} it
     * is emitted verbatim. Under NEITHER policy is it a parseable structure — across an array
     * boundary the marker is emitted outside the escaped alphabet, so splitting the name yields a
     * phantom empty segment. Use {@link #pathSegments()} for ancestry and
     * {@link #arrayBoundaries()} for repetition. (This used to read "separator-escaped and safe to
     * round-trip", which was false on both halves.)</p>
     */
    public String flattenedName() {
        return flattenedName;
    }

    /** The leaf's own name as the schema declared it, unescaped and un-prefixed. */
    public String name() {
        return name;
    }

    /**
     * Structured ancestry, root first, including this leaf.
     *
     * <p>Prefer this over splitting {@link #flattenedName()}: a field named {@code user_id} and
     * the nested path {@code user} → {@code id} are different here and indistinguishable there.</p>
     */
    public List<PathSegment> pathSegments() {
        return path;
    }

    /** The array boundaries crossed to reach this leaf, in order. Empty for non-repeated data. */
    public List<PathSegment> arrayBoundaries() {
        return path.stream().filter(PathSegment::isArrayBoundary).toList();
    }

    /** Dotted, human-readable original path. For display and logs, never for parsing. */
    public String sourcePath() {
        return String.join(".", path.stream().map(PathSegment::name).toList());
    }

    /**
     * Documentation for this leaf.
     *
     * <p>When the leaf declares none this is the nearest ancestor record's — but only if
     * {@link FlattenOptions.Builder#inheritDoc(boolean)} is left at its default {@code true}.
     * Inheritance is a control, not an invariant; with it switched off this is empty unless the
     * leaf declared documentation itself. Check {@link #isDocInherited()} to tell the two
     * apart.</p>
     */
    public Optional<String> doc() {
        return Optional.ofNullable(doc);
    }

    /** True when {@link #doc()} came from an ancestor rather than from this field. */
    public boolean isDocInherited() {
        return docInherited;
    }

    /** The resolved Avro type, with any nullable union already unwrapped. */
    public Schema.Type avroType() {
        return avroType;
    }

    /** The resolved Avro schema, with any nullable union already unwrapped. */
    public Schema schema() {
        return schema;
    }

    /** The target-system type produced by the configured {@link TypeMapper}. */
    public Optional<String> mappedType() {
        return Optional.ofNullable(mappedType);
    }

    /** True when the source declared a union containing null. */
    public boolean isNullable() {
        return nullable;
    }

    /** True when any ancestor was an array, so this leaf holds repeated data. */
    public boolean isWithinArray() {
        return withinArray;
    }

    /**
     * Whether this leaf could serve as a primary key: non-null, not repeated, and a scalar.
     *
     * <p>A necessary condition, not a sufficient one — uniqueness is a property of the data, which
     * a schema cannot know. Named to describe what it checks.</p>
     */
    public boolean isPrimaryKeyEligible() {
        return !nullable && !withinArray && isScalar();
    }

    private boolean isScalar() {
        return switch (avroType) {
            case RECORD, ARRAY, MAP, UNION, NULL -> false;
            default -> true;
        };
    }

    /** 1-based position in the emitted field list. Stable across runs for the same schema. */
    public int position() {
        return position;
    }

    /** True when injected by the caller rather than present in the source schema. */
    public boolean isSynthetic() {
        return synthetic;
    }

    /**
     * Custom properties carried from the source schema, plus anything an interceptor added.
     *
     * <p>Every custom property on the field is preserved, and properties on the enclosing record
     * are inherited when the field does not shadow them. Avro accepts arbitrary JSON properties
     * and readers routinely drop them; anything a producer bothered to declare is meaningful to
     * somebody, so nothing is filtered by name. The structural/custom split is Avro's parser's,
     * performed per position, which is why a name-keyed filter here was removed.</p>
     *
     * <p>This is a MERGED view, so the presence of a key is not a statement about this leaf's
     * structure. A record-level {@code logicalType}, {@code default} or {@code order} annotation
     * reaches every leaf beneath it; the leaf's own structural facts come from {@link #avroType()},
     * {@link #schema()}, {@link #doc()} and {@link #isNullable()}. In particular
     * {@code properties().get("logicalType")} is a producer annotation, never the Avro logical
     * type, which lives on {@link #schema()}.</p>
     *
     * <p>Mutable by design so a {@link LeafInterceptor} can annotate in-pass.</p>
     */
    public Map<String, Object> properties() {
        return properties;
    }

    /** Convenience for the common string-valued property lookup. */
    public Optional<String> property(String key) {
        Object v = properties.get(key);
        return v == null ? Optional.empty() : Optional.of(String.valueOf(v));
    }

    @Override
    public String toString() {
        return "FlattenedField[" + flattenedName + " " + avroType
                + (nullable ? "?" : "") + (withinArray ? " (in array)" : "") + "]";
    }

    /** Builder. Used by the flattener and by callers injecting synthetic fields. */
    public static final class Builder {
        private String flattenedName;
        private String name;
        private List<PathSegment> path = List.of();
        private String doc;
        private boolean docInherited;
        private Schema.Type avroType = Schema.Type.STRING;
        private Schema schema;
        private String mappedType;
        private boolean nullable;
        private boolean withinArray;
        private int position;
        private boolean synthetic;
        private Map<String, Object> properties = new LinkedHashMap<>();

        public Builder flattenedName(String v) { this.flattenedName = v; return this; }
        public Builder name(String v) { this.name = v; return this; }
        public Builder path(List<PathSegment> v) { this.path = v == null ? List.of() : v; return this; }
        public Builder doc(String v) { this.doc = v; return this; }
        public Builder docInherited(boolean v) { this.docInherited = v; return this; }
        public Builder avroType(Schema.Type v) { this.avroType = v; return this; }
        public Builder schema(Schema v) { this.schema = v; return this; }
        public Builder mappedType(String v) { this.mappedType = v; return this; }
        public Builder nullable(boolean v) { this.nullable = v; return this; }
        public Builder withinArray(boolean v) { this.withinArray = v; return this; }
        public Builder position(int v) { this.position = v; return this; }
        public Builder synthetic(boolean v) { this.synthetic = v; return this; }

        public Builder properties(Map<String, Object> v) {
            this.properties = v == null ? new LinkedHashMap<>() : new LinkedHashMap<>(v);
            return this;
        }

        public Builder property(String k, Object v) {
            this.properties.put(k, v);
            return this;
        }

        public FlattenedField build() {
            if (name == null) {
                throw new IllegalStateException("name is required");
            }
            if (flattenedName == null) {
                flattenedName = name;
            }
            if (schema != null && avroType == null) {
                avroType = schema.getType();
            }
            return new FlattenedField(this);
        }
    }

    /** Unmodifiable empty property map, for callers building fields without properties. */
    public static Map<String, Object> noProperties() {
        return Collections.emptyMap();
    }
}
