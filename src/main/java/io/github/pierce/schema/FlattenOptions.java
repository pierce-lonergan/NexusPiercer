package io.github.pierce.schema;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuration for {@link EnrichedSchemaFlattener}.
 *
 * <p>Immutable and {@link Serializable}, because a configured flattener is routinely captured
 * into a Spark closure and shipped to executors. Every engine in this library declares
 * {@code Serializable}; a configuration object that did not would fail on first use in the one
 * environment the library most targets.</p>
 *
 * <h2>Defaults</h2>
 *
 * <p>Defaults reproduce the widely-used {@code GAvroSchemaFlattener} conventions — {@code "_"}
 * path join, {@code "__"} at array boundaries, nullable unions unwrapped, documentation inherited
 * from the nearest ancestor record — so a caller migrating from that flattener starts at parity
 * and changes only what they mean to change.</p>
 *
 * <p>Limits default to {@code maxDepth = 64} and {@code maxFields = 100_000}. Both are generous
 * for real schemas and finite for hostile ones. Fail-closed is deliberate: a caller flattening
 * untrusted producer schemas gets a typed exception naming the schema, not an OOM that takes the
 * batch with it.</p>
 */
public final class FlattenOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String separator;
    private final String arrayBoundarySeparator;
    private final boolean inheritDoc;
    private final boolean inheritRecordProperties;
    private final boolean unwrapNullableUnions;
    private final NameCollisionPolicy collisionPolicy;
    private final int maxDepth;
    private final int maxFields;
    private final TypeMapper typeMapper;
    private final LeafInterceptor leafInterceptor;
    private final Map<Integer, FlattenedField> injectedFields;

    private FlattenOptions(Builder b) {
        this.separator = b.separator;
        this.arrayBoundarySeparator = b.arrayBoundarySeparator;
        this.inheritDoc = b.inheritDoc;
        this.inheritRecordProperties = b.inheritRecordProperties;
        this.unwrapNullableUnions = b.unwrapNullableUnions;
        this.collisionPolicy = b.collisionPolicy;
        this.maxDepth = b.maxDepth;
        this.maxFields = b.maxFields;
        this.typeMapper = b.typeMapper;
        this.leafInterceptor = b.leafInterceptor;
        this.injectedFields = Map.copyOf(b.injectedFields);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Conventions matching {@code GAvroSchemaFlattener}, which is also the default. */
    public static FlattenOptions gAvroParity() {
        return builder().build();
    }

    public String separator() { return separator; }
    public String arrayBoundarySeparator() { return arrayBoundarySeparator; }
    public boolean inheritDoc() { return inheritDoc; }
    public boolean inheritRecordProperties() { return inheritRecordProperties; }
    public boolean unwrapNullableUnions() { return unwrapNullableUnions; }
    public NameCollisionPolicy collisionPolicy() { return collisionPolicy; }
    public int maxDepth() { return maxDepth; }
    public int maxFields() { return maxFields; }
    public TypeMapper typeMapper() { return typeMapper; }
    public LeafInterceptor leafInterceptor() { return leafInterceptor; }

    /** Synthetic fields to inject, keyed by 1-based position. */
    public Map<Integer, FlattenedField> injectedFields() { return injectedFields; }

    public static final class Builder {
        private String separator = "_";
        private String arrayBoundarySeparator = "__";
        private boolean inheritDoc = true;
        private boolean inheritRecordProperties = true;
        private boolean unwrapNullableUnions = true;
        private NameCollisionPolicy collisionPolicy = NameCollisionPolicy.FAIL;
        private int maxDepth = 64;
        private int maxFields = 100_000;
        private TypeMapper typeMapper = TypeMapper.defaultMapper();
        private LeafInterceptor leafInterceptor = LeafInterceptor.noop();
        private final Map<Integer, FlattenedField> injectedFields = new LinkedHashMap<>();

        /** Path join separator. Default {@code "_"}. */
        public Builder separator(String v) {
            this.separator = require(v, "separator");
            return this;
        }

        /**
         * Separator used where an array boundary is crossed. Default {@code "__"}.
         *
         * <p>Marking the boundary in the rendered name is a convention consumers rely on, but it
         * is lossy — {@code __} also occurs in ordinary names. {@link FlattenedField#arrayBoundaries()}
         * is the reliable answer; this only controls the rendering.</p>
         */
        public Builder arrayBoundarySeparator(String v) {
            this.arrayBoundarySeparator = require(v, "arrayBoundarySeparator");
            return this;
        }

        /** Whether a leaf with no doc inherits the nearest ancestor record's. Default true. */
        public Builder inheritDoc(boolean v) { this.inheritDoc = v; return this; }

        /**
         * Whether a leaf inherits custom properties from enclosing records. Default true; a
         * field's own property always wins.
         */
        public Builder inheritRecordProperties(boolean v) { this.inheritRecordProperties = v; return this; }

        /**
         * How to handle two source paths rendering to the same name. Default
         * {@link NameCollisionPolicy#FAIL}, because this flattener's output becomes column names
         * and the escape character is illegal in Avro and SQL identifiers.
         */
        public Builder collisionPolicy(NameCollisionPolicy v) {
            this.collisionPolicy = v == null ? NameCollisionPolicy.FAIL : v;
            return this;
        }

        /** Whether {@code ["null", T]} resolves to {@code T} with nullable=true. Default true. */
        public Builder unwrapNullableUnions(boolean v) { this.unwrapNullableUnions = v; return this; }

        /** Maximum nesting depth before {@link SchemaLimitExceededException}. Default 64. */
        public Builder maxDepth(int v) {
            if (v < 1) throw new IllegalArgumentException("maxDepth must be >= 1");
            this.maxDepth = v;
            return this;
        }

        /** Maximum emitted leaves before {@link SchemaLimitExceededException}. Default 100,000. */
        public Builder maxFields(int v) {
            if (v < 1) throw new IllegalArgumentException("maxFields must be >= 1");
            this.maxFields = v;
            return this;
        }

        /** Target-system type mapping. Default {@link TypeMapper#defaultMapper()}. */
        public Builder typeMapper(TypeMapper v) {
            this.typeMapper = v == null ? TypeMapper.defaultMapper() : v;
            return this;
        }

        /** Per-leaf hook, invoked in emission order. Composes via {@link LeafInterceptor#andThen}. */
        public Builder leafInterceptor(LeafInterceptor v) {
            this.leafInterceptor = v == null ? LeafInterceptor.noop() : v;
            return this;
        }

        /**
         * Injects a synthetic field at a 1-based position.
         *
         * <p>For operator and event-framework columns that must appear at a fixed index. Source
         * fields are never reordered: injections are spliced in around them, so a schema change
         * upstream cannot silently shuffle a column an external contract depends on.</p>
         *
         * @throws IllegalArgumentException if the position is below 1 or already claimed
         */
        public Builder injectField(int position, FlattenedField field) {
            if (position < 1) {
                throw new IllegalArgumentException("position is 1-based; got " + position);
            }
            if (injectedFields.containsKey(position)) {
                throw new IllegalArgumentException(
                        "position " + position + " already claimed by '"
                                + injectedFields.get(position).flattenedName() + "'");
            }
            injectedFields.put(position, field);
            return this;
        }

        private static String require(String v, String what) {
            if (v == null || v.isEmpty()) {
                throw new IllegalArgumentException(what + " must be non-empty");
            }
            return v;
        }

        public FlattenOptions build() {
            return new FlattenOptions(this);
        }
    }
}
