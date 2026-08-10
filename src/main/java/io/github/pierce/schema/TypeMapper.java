package io.github.pierce.schema;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

/**
 * Maps a resolved Avro schema to a target-system type name.
 *
 * <p>Injectable because the target is the caller's, not the library's. A warehouse pipeline wants
 * Snowflake or erwin types with widths derived from its own conventions — often from a custom
 * property the producer set, falling back to the Avro type, falling back to a permissive default.
 * A library that hardcoded one mapping would be wrong for everyone else; one that offered none
 * would push the same switch statement into every consumer.</p>
 *
 * <p>Receives the whole {@link FlattenedField} rather than just the type, so an implementation can
 * consult {@link FlattenedField#properties()} — which is exactly how a physical-type hint cascade
 * is expressed:</p>
 *
 * <pre>{@code
 * TypeMapper erwin = field -> field.property("x-erwin-physical-type")
 *         .orElseGet(() -> switch (field.avroType()) {
 *             case INT    -> "NUMBER(10,0)";
 *             case LONG   -> "NUMBER(19,0)";
 *             case STRING -> "VARCHAR(MAX)";
 *             default     -> "VARCHAR(MAX)";
 *         });
 * }</pre>
 */
/*
 * Extends Serializable on purpose. FlattenOptions is Serializable so it can be captured by a
 * Spark closure and shipped to executors; a non-serializable mapper field would make that
 * declaration a lie that only surfaces on a cluster, never in a local test. Java gives a lambda
 * assigned to a Serializable functional interface a writeReplace, so `f -> "..."` still works.
 */
@FunctionalInterface
public interface TypeMapper extends java.io.Serializable {

    /**
     * @param field the leaf, fully populated except for its mapped type
     * @return the target type name, or {@code null} to leave it unset
     */
    String map(FlattenedField field);

    /**
     * A conservative default covering Avro primitives and the common logical types.
     *
     * <p>Deliberately dialect-neutral: it names SQL-ish types without inventing widths, because a
     * guessed width is worse than an absent one. Supply your own for a real target.</p>
     */
    static TypeMapper defaultMapper() {
        return field -> {
            Schema s = field.schema();
            LogicalType logical = (s == null) ? null : s.getLogicalType();
            if (logical != null) {
                switch (logical.getName()) {
                    case "date":
                        return "DATE";
                    case "time-millis":
                    case "time-micros":
                        return "TIME";
                    case "timestamp-millis":
                    case "timestamp-micros":
                        return "TIMESTAMP";
                    case "uuid":
                        return "UUID";
                    case "decimal":
                        return "DECIMAL";
                    default:
                        break;
                }
            }
            return switch (field.avroType()) {
                case BOOLEAN -> "BOOLEAN";
                case INT -> "INTEGER";
                case LONG -> "BIGINT";
                case FLOAT -> "FLOAT";
                case DOUBLE -> "DOUBLE";
                case BYTES, FIXED -> "BINARY";
                default -> "VARCHAR";
            };
        };
    }
}
