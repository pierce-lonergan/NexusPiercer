package io.github.pierce;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Flattened-name collisions in the Avro/Spark stack.
 *
 * <h2>Why this stack fails loudly instead of escaping</h2>
 *
 * <p>{@link MapFlattener} and the reconstructors resolve separator collisions with
 * {@link io.github.pierce.path.FlattenedPath}, which escapes a literal separator as {@code \_}.
 * {@link AvroSchemaFlattener} cannot do the same, because it emits real Avro {@link Schema.Field}
 * objects rather than map keys, and Avro rejects the escape character outright:</p>
 *
 * <pre>
 *   new Schema.Field("user\\_id", ...)
 *     -&gt; SchemaParseException: Illegal character in: user\_id
 * </pre>
 *
 * <p>Verified against avro 1.12.0. The names also propagate through
 * {@code CreateSparkStructFromAvroSchema} into Spark, Glue and Athena column names, which are no
 * more permissive. Separator-doubling is not an alternative either — it is not injective, since
 * {@code user___id} is ambiguous between {@code ["user_", "id"]} and {@code ["user", "_id"]}.</p>
 *
 * <p>So this stack detects the collision and refuses. Every non-colliding schema keeps byte-identical
 * column names, which matters: renaming columns would churn every downstream table.</p>
 */
@DisplayName("Avro flattened-name collisions")
class AvroSchemaFlattenerCollisionTest {

    @Test
    @DisplayName("a literal user_id colliding with nested user.id is rejected, not silently merged")
    void collidingNamesAreRejected() {
        Schema colliding = SchemaBuilder.record("Order").namespace("test").fields()
                .requiredString("user_id")
                .name("user").type(
                        SchemaBuilder.record("User").namespace("test").fields()
                                .requiredString("id")
                                .endRecord())
                .noDefault()
                .endRecord();

        assertThatThrownBy(() -> new AvroSchemaFlattener().getFlattenedSchemaNoCache(colliding))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("collision")
                .hasMessageContaining("user_id")
                .hasMessageContaining("user.id");
    }

    @Test
    @DisplayName("ordinary snake_case schemas are unaffected and keep their exact column names")
    void nonCollidingSnakeCaseIsUnchanged() {
        Schema ordinary = SchemaBuilder.record("Order").namespace("test").fields()
                .requiredString("order_id")
                .requiredString("created_at")
                .name("customer").type(
                        SchemaBuilder.record("Customer").namespace("test").fields()
                                .requiredString("full_name")
                                .endRecord())
                .noDefault()
                .endRecord();

        Schema flat = new AvroSchemaFlattener().getFlattenedSchemaNoCache(ordinary);

        // Byte-identical to the pre-guard output: no renaming, no escaping.
        assertThat(flat.getFields()).extracting(Schema.Field::name)
                .contains("order_id", "created_at", "customer_full_name");
    }

    /**
     * Guards the reason escaping was rejected. If someone later "fixes" the collision by escaping,
     * this fails immediately rather than at runtime in a Spark job.
     */
    @Test
    @DisplayName("Avro rejects the backslash escape, which is why this stack cannot use FlattenedPath")
    void avroRejectsTheEscapeCharacter() {
        assertThatThrownBy(() ->
                new Schema.Field("user\\_id", Schema.create(Schema.Type.STRING), null, null))
                .hasMessageContaining("Illegal character");

        assertThatCode(() ->
                new Schema.Field("user_id", Schema.create(Schema.Type.STRING), null, null))
                .doesNotThrowAnyException();
    }
}
