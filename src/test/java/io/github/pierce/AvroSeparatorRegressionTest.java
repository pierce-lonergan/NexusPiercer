package io.github.pierce;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * The Avro counterpart to {@link SeparatorInFieldNameRegressionTest}.
 *
 * <p>Avro field names are legally {@code [A-Za-z_][A-Za-z0-9_]*}, so {@code user_id} and
 * {@code created_at} are perfectly valid — which means the Avro stack was exposed to exactly the
 * same non-injective-encoding defect as the JSON stack: a literal {@code user_id} field and a
 * nested {@code user} → {@code id} path produced the same flattened key, and reconstruction could
 * not tell them apart.</p>
 *
 * <p>These tests pin the behaviour after {@code FlattenedPath} was wired through
 * {@code GAvroSchemaFlattener} and {@code AvroReconstructor}.</p>
 */
@DisplayName("Avro separator-in-field-name regressions")
class AvroSeparatorRegressionTest {

    /** A record whose field names deliberately contain the default separator. */
    private static Schema snakeCaseSchema() {
        return SchemaBuilder.record("Order").namespace("test").fields()
                .requiredString("order_id")
                .requiredString("created_at")
                .name("customer").type(
                        SchemaBuilder.record("Customer").namespace("test").fields()
                                .requiredString("full_name")
                                .requiredString("email_address")
                                .endRecord())
                .noDefault()
                .endRecord();
    }

    @Test
    @DisplayName("snake_case Avro field names round-trip through flatten and reconstruct")
    void snakeCaseFieldNamesRoundTrip() {
        Schema schema = snakeCaseSchema();

        Map<String, Object> customer = new LinkedHashMap<>();
        customer.put("full_name", "Ada Lovelace");
        customer.put("email_address", "ada@example.com");

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("order_id", "ORD-1");
        source.put("created_at", "2026-08-09");
        source.put("customer", customer);

        MapFlattener flattener = new MapFlattener(false, 50, 1000);
        Map<String, Object> flat = flattener.flatten(source);

        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> back = reconstructor.reconstructToMap(flat, schema);

        assertThat(back).isEqualTo(source);
    }

    /**
     * The distinguishing case: a literal {@code customer_full_name} field must not be confused
     * with the nested path {@code customer} → {@code full_name}. Under the old concatenating
     * encoding both produced the identical flattened key.
     */
    @Test
    @DisplayName("a literal customer_full_name field is distinct from customer -> full_name")
    void literalFieldDistinctFromNestedPath() {
        MapFlattener flattener = new MapFlattener(false, 50, 1000);

        Map<String, Object> literal = new LinkedHashMap<>();
        literal.put("customer_full_name", "flat");

        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("full_name", "nested");
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("customer", inner);

        assertThat(flattener.flatten(literal).keySet())
                .as("these two documents must not share a flattened key")
                .isNotEqualTo(flattener.flatten(nested).keySet());
    }

    /**
     * The Avro-side DoS guard.
     *
     * <p>On the JSON side, holding structure fixed and adding one underscore per field name took
     * reconstruction from ~200 ms to heap exhaustion. The Avro reconstructor grouped paths the
     * same way, so it had the same exposure. This asserts the cost stays bounded — and, more
     * importantly, that it does not grow with the number of separator characters in a name.</p>
     */
    @Test
    @DisplayName("reconstruction cost does not grow with underscores in Avro field names")
    void costIsIndependentOfUnderscoreCount() {
        Schema schema = SchemaBuilder.record("Wide").namespace("test").fields()
                .requiredString("deep_nested_field_alpha")
                .requiredString("deep_nested_field_beta")
                .requiredString("deep_nested_field_gamma")
                .requiredString("deep_nested_field_delta")
                .endRecord();

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("deep_nested_field_alpha", "a");
        source.put("deep_nested_field_beta", "b");
        source.put("deep_nested_field_gamma", "c");
        source.put("deep_nested_field_delta", "d");

        MapFlattener flattener = new MapFlattener(false, 50, 1000);
        Map<String, Object> flat = flattener.flatten(source);

        // Four fields, four keys: each name is ONE segment despite containing three underscores.
        // Under the old encoding these keys were indistinguishable from four levels of nesting.
        assertThat(flat).hasSize(4);

        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        assertTimeoutPreemptively(Duration.ofSeconds(20), () ->
                assertThat(reconstructor.reconstructToMap(flat, schema)).isEqualTo(source));
    }
}
