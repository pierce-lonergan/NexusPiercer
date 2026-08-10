package io.github.pierce.schema;

/**
 * What to do when two distinct source paths render to the same flattened name.
 *
 * <h2>Why this is a policy and not a fixed behaviour</h2>
 *
 * <p>Joining path segments with a separator is ambiguous whenever a field name contains that
 * separator: a field literally called {@code user_id} and the nested path {@code user} → {@code id}
 * both render to {@code user_id}. There are exactly two honest responses, and which is correct
 * depends entirely on where the name is going.</p>
 *
 * <p>{@link #ESCAPE} is right when the name is a map key or an internal identifier: it is
 * lossless and round-trips. It is <b>wrong</b> for anything destined to become an Avro field or a
 * warehouse column, because the escape character is not legal there —
 * {@code new Schema.Field("user\\_id", ...)} throws {@code SchemaParseException: Illegal character},
 * verified against avro 1.12.0, and Athena and Glue are no more permissive. Escaping such a name
 * produces a schema that cannot be constructed.</p>
 *
 * <p>{@link #FAIL} is right for that case: it leaves every non-colliding name byte-identical — no
 * renamed columns, no downstream table churn — and refuses only the schemas that genuinely cannot
 * be represented. That is why it is the default here: this flattener's output is destined for
 * column names.</p>
 *
 * <p>Separator-doubling is deliberately not offered. It looks like a third option and is not
 * injective: {@code user___id} is ambiguous between {@code ["user_", "id"]} and
 * {@code ["user", "_id"]}, so it trades a detectable failure for a silent one.</p>
 */
public enum NameCollisionPolicy {

    /**
     * Refuse the schema, naming both colliding source paths. Default.
     *
     * <p>Non-colliding names are emitted exactly as a naive join would produce them, so adopting
     * this policy changes nothing for schemas that were already unambiguous.</p>
     */
    FAIL,

    /**
     * Escape the separator inside segment names so the rendering stays injective.
     *
     * <p>Lossless and reversible, and unusable for Avro or SQL column names. Choose it when the
     * flattened name is a map key, a JSON pointer, or any identifier you control the alphabet of.</p>
     */
    ESCAPE
}
