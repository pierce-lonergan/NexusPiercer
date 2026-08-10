package io.github.pierce.schema;

import java.io.Serializable;
import java.util.Objects;

/**
 * One step in a flattened field's ancestry.
 *
 * <p>Structured provenance rather than a parsed string. A flattened name like
 * {@code customer_addresses__street_line_1} answers "what is this called"; it does not reliably
 * answer "which of those underscores were structural", "was an array crossed", or "what was the
 * parent record". Recovering that by splitting the name is exactly the ambiguity the injective
 * encoding exists to remove — reintroducing it downstream would put the bug back one layer up.</p>
 *
 * <p>This matters beyond tidiness: parent-path context is a measurably strong signal for schema
 * field matching, and a consumer that has to re-derive it from the rendered name is both slower
 * and wrong on any name containing the separator.</p>
 *
 * @param name       the field or record name at this level, unescaped and exactly as the schema
 *                   declared it
 * @param kind       what was traversed to get here
 * @param recordName the enclosing Avro record's name, when there is one — useful for provenance
 *                   and for resolving ambiguous leaf names against their declaring type
 */
public record PathSegment(String name, Kind kind, String recordName) implements Serializable {

    private static final long serialVersionUID = 1L;

    /** What kind of structure this segment traverses. */
    public enum Kind {
        /** A field of a record. */
        FIELD,
        /** An array boundary: everything below this point repeats. */
        ARRAY,
        /** A map value. The segment name is the declared field, not a runtime key. */
        MAP,
        /** A branch of a union that was unwrapped to its single non-null type. */
        UNION_BRANCH
    }

    public PathSegment {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(kind, "kind");
    }

    public static PathSegment field(String name, String recordName) {
        return new PathSegment(name, Kind.FIELD, recordName);
    }

    public static PathSegment array(String name, String recordName) {
        return new PathSegment(name, Kind.ARRAY, recordName);
    }

    public static PathSegment map(String name, String recordName) {
        return new PathSegment(name, Kind.MAP, recordName);
    }

    /** True when traversing this segment crosses into repeated data. */
    public boolean isArrayBoundary() {
        return kind == Kind.ARRAY;
    }

    @Override
    public String toString() {
        return kind == Kind.FIELD ? name : name + "[" + kind + "]";
    }
}
