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
 * <p>{@link #ESCAPE} is right when the name is a map key or an internal identifier: no two
 * distinct source paths ever render alike. It is <b>wrong</b> for anything destined to become an
 * Avro field or a warehouse column, because the escape character is not legal there —
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
 *
 * <p>Injectivity is <b>checked, not assumed</b>, under both policies. It used to be asserted in a
 * comment and skipped in code: the guard returned early under {@code ESCAPE} because "the
 * rendering is injective by construction", which is true only while
 * {@link FlattenOptions.Builder#arrayBoundarySeparator(String)} is spelled from characters segment
 * escaping escapes. A boundary marker an ordinary field name can spell is now refused with a
 * diagnostic naming both source paths, rather than emitting the same column name twice.</p>
 *
 * <p>The check covers columns supplied through
 * {@link FlattenOptions.Builder#injectField(int, FlattenedField)} too. An injected name is not
 * rendered by either policy — the caller typed it — so neither policy's remedy applies to it, and
 * it gets its own diagnostic; but it is an emitted column, and the guarantee both policies now
 * make is about the emitted set.</p>
 */
public enum NameCollisionPolicy {

    /**
     * Refuse the schema when the naive join is ambiguous, naming both colliding source paths.
     * Default.
     *
     * <p>Non-colliding names are emitted exactly as a naive join would produce them, so adopting
     * this policy changes nothing for schemas that were already unambiguous. Refusal is not what
     * distinguishes this policy from {@link #ESCAPE} — both refuse rather than emit one name for
     * two columns, and both refuse a colliding {@code injectField} name. What distinguishes them
     * is what happens to the names that DO survive: here, nothing at all.</p>
     */
    FAIL,

    /**
     * Escape the separator inside segment names, so that a collision an ordinary join would create
     * is resolved by the rendering instead of refused.
     *
     * <p>NO TWO EMITTED COLUMNS SHARE A FLATTENED NAME. That is the enforced property, and it is
     * about the OUTPUT rather than about the rendering: escaping resolves the ordinary case — a
     * field literally called {@code user_id} against the nested path {@code user} → {@code id} —
     * and where the configured
     * {@link FlattenOptions.Builder#arrayBoundarySeparator(String) arrayBoundarySeparator} makes
     * that impossible, because the marker is emitted outside the escaped alphabet and an ordinary
     * field name can therefore spell it, the flatten is REFUSED with a
     * {@link SchemaFlattenException} naming both source paths. Choosing this policy to avoid
     * {@link #FAIL}'s refusals narrows them; it does not remove them.</p>
     *
     * <p>Unusable for Avro or SQL column names, where the escape character is illegal. Choose it
     * when the flattened name is a map key, a JSON pointer, or any identifier whose alphabet you
     * control.</p>
     *
     * <p>NOT DECODABLE, and this is the correction of a claim this javadoc used to make. The
     * previous wording said "lossless and reversible". Reversibility is a property of an
     * encode/decode PAIR and this package ships only the encode half; worse, the array-boundary
     * separator is emitted OUTSIDE the escaped alphabet — {@code escapeSegment} is called with the
     * plain separator only — so splitting a rendered name on the separator interposes a phantom
     * empty segment at every array boundary: {@code items__sku} splits to
     * {@code [items, "", sku]}, not {@code [items, sku]}.</p>
     *
     * <p>Do not parse the name. {@link FlattenedField#pathSegments()} carries the ancestry and
     * {@link FlattenedField#arrayBoundaries()} carries the repetition, and both carry strictly
     * more than any decoder could recover — a decoder cannot see
     * {@link PathSegment#recordName()} and cannot tell a map leaf from a scalar one.</p>
     */
    ESCAPE
}
