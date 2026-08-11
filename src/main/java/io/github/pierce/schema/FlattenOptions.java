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
 * <p>The defaults are this library's own recommended conventions, chosen on their own merits and
 * not copied from another flattener: {@code "_"} path join, {@code "__"} marking an array
 * boundary, nullable unions unwrapped, documentation inherited from the nearest ancestor record,
 * and {@link NameCollisionPolicy#FAIL} — because this flattener's output becomes Avro and SQL
 * column names, where the escape character is illegal, so refusing an ambiguous schema beats
 * renaming an unambiguous one.</p>
 *
 * <p>They are deliberately NOT {@code GAvroSchemaFlattener}'s conventions. That flattener escapes
 * every segment and has no array-boundary marker at all, so its names differ from these on both
 * axes. An earlier version of this javadoc claimed parity for the defaults; it was measured and
 * it was false. {@link #gAvroParity()} is the migration path, and it says exactly what it
 * covers.</p>
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

    /**
     * This library's own recommended conventions. Identical to {@code builder().build()} and to
     * what {@code new EnrichedSchemaFlattener()} uses.
     *
     * <p>Separate from {@link #gAvroParity()} by name on purpose. The two were one object, and
     * collapsing "the conventions this library recommends" into "the conventions the flattener you
     * are migrating from produced" is the mechanism by which the parity claim rotted unnoticed:
     * nothing could observe the difference because there was none.</p>
     */
    public static FlattenOptions defaults() {
        return builder().build();
    }

    /**
     * Column names byte-identical to {@code new GAvroSchemaFlattener().flattenSchema(schema)}.
     *
     * <p>Two knobs, both measured against that flattener rather than assumed.
     * {@code GAvroSchemaFlattener.buildPath} calls {@code FlattenedPath.escapeSegment} on EVERY
     * segment, so parity needs {@link NameCollisionPolicy#ESCAPE}; and GAvro has no array-boundary
     * marker at all — its {@code useArrayBoundarySeparator} globally renames the one separator —
     * so parity needs the boundary separator equal to the separator.</p>
     *
     * <p><b>What this covers, and what it does not.</b>
     * NAMES ONLY, and INCLUDING GAvro's backslash escapes. {@code order_id} renders as
     * {@code order\_id}, which is not a legal Avro or SQL identifier — that is inherited from
     * GAvro, which already does it. Use this to keep an existing table's column names while
     * migrating; do not use it for names that are about to become identifiers. It says nothing
     * about GAvro's {@code DataType} mapping, which this API does not reproduce.</p>
     *
     * <p>Four structural divergences no configuration can express, so parity is exact only for
     * record-rooted schemas that avoid them:</p>
     * <ul>
     *   <li>a union with more than one non-null branch: GAvro descends into the first non-null
     *       branch, this flattener emits one leaf for the union;</li>
     *   <li>an array of arrays of records: GAvro digs into the inner record's fields, this
     *       flattener emits one leaf;</li>
     *   <li>depth: GAvro stops at 50 and DEGRADES the column to a string with a log warning, this
     *       flattener stops at 64 and throws {@link SchemaLimitExceededException}. {@code maxDepth}
     *       is deliberately left at 64 — matching the number while the behaviour still differed
     *       would be a more convincing lie than the one being repaired;</li>
     *   <li>a non-record root: GAvro accepts anything and keys it {@code value}/{@code root}, this
     *       flattener refuses.</li>
     * </ul>
     *
     * <p>The first two are not renames — a whole subtree collapses to a single leaf, so a column
     * GAvro produces does not exist here at all. Measured: on a record whose one field is a union
     * of two records, GAvro yields {@code [payload_k]} and this yields {@code [payload]}; on an
     * array of arrays of records, GAvro yields {@code [grid_v]} and this yields {@code [grid]}.
     * Three of the four are pinned by
     * {@code EnrichedSchemaFlattenerTest.parityDivergesStructurallyWhereNoConfigurationCanReach};
     * the depth one is not, and is prose.</p>
     *
     * <p>For a caller who ran GAvro with {@code useArrayBoundarySeparator(true)} — a setting the
     * fidelity corpus classifies as a misnamed control, since it marks no boundaries and only
     * renames the separator globally — the equivalent here is the parity policy with both
     * separators doubled:</p>
     * <pre>{@code
     * FlattenOptions.builder()
     *         .separator("__").arrayBoundarySeparator("__")
     *         .collisionPolicy(NameCollisionPolicy.ESCAPE)
     *         .build();
     * }</pre>
     */
    public static FlattenOptions gAvroParity() {
        return builder()
                .separator("_")
                .arrayBoundarySeparator("_")
                .collisionPolicy(NameCollisionPolicy.ESCAPE)
                .build();
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

        /**
         * Whether a leaf with no doc inherits the nearest ancestor record's. Default true.
         *
         * <p>When false, a leaf reports only the documentation it declares itself,
         * {@link FlattenedField#isDocInherited()} is always false, and no ancestor text is
         * propagated. A leaf's OWN doc is unaffected either way: this turns inheritance off, not
         * documentation off. That is the setting to use where the inherited text would otherwise
         * propagate into a data dictionary as if the producer had written it about that column.</p>
         */
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

        /**
         * Maximum COLUMNS before {@link SchemaLimitExceededException}. Default 100,000.
         *
         * <p>Columns, not source leaves: anything {@link #injectField(int, FlattenedField)} adds
         * counts against the same ceiling, because the ceiling exists to bound what the caller
         * receives. It bounded only the traversal until this was corrected, so
         * {@code maxFields(2)} with two injections on a two-field record returned four columns.</p>
         */
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
         * <p>Honoured identically by {@code EnrichedSchemaFlattener.flatten} and
         * {@code .stream} — the streaming path needs only the current output index, never the
         * total field count. A position beyond the final column APPENDS in ascending order rather
         * than leaving gaps or throwing: {@code injectField(99, x)} on a three-column schema
         * yields column 4.</p>
         *
         * <p>THIS METHOD DE-DUPLICATES POSITIONS, NOT NAMES, and the two guards that catch the
         * rest fire at flatten time rather than here, because both need the schema. An injected
         * {@code flattenedName} equal to another column's — a source column's, or another
         * injection's — is refused with {@link SchemaFlattenException} under BOTH collision
         * policies, and injected columns count against {@link #maxFields(int)}. Neither was true
         * until it was corrected: an injected duplicate was returned silently even under
         * {@link NameCollisionPolicy#FAIL}, whose whole job is refusing exactly that.</p>
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
