package io.github.pierce;

import org.apache.avro.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A REFUTATION, pinned so it is not re-discovered as a finding a fourth time.
 *
 * <h2>The claim</h2>
 *
 * <p>The analysis that ordered the site-C repair said the index clamp at
 * {@code reconstructNestedArrayOfRecordsAtIndex} -</p>
 * <pre>
 *   Object rawValue = outerIndex &lt; rawValues.size() ? rawValues.get(outerIndex) : rawValues.get(0);
 * </pre>
 * <p>- was "currently feeding a silent duplicator": a short nested-array-of-records column would
 * resolve an out-of-range outer index to element 0 and replicate the first outer position's inner
 * records into every position past the end. Schema-valid, plausible, silent. It named this as the
 * live downstream corruption path that site C's raggedness fed, and as a finding the filings did
 * not contain.</p>
 *
 * <h2>MEASURED: the clamp is not reachable with a disagreeing column count</h2>
 *
 * <p>{@code agreedElementCount} refuses first. The exact shape the claim describes -
 * {@code orders_id} with two entries beside {@code orders_items_sku} with one - throws
 * {@code ArrayCardinalityException} naming both counts before any index is taken.
 * {@code collectElementCounts} deliberately does not descend into an ARRAY field's INNER
 * cardinality, but it does take an array-of-records child's own column sizes as a signal for the
 * OUTER level, so a short nested column is counted and disagrees. There is no measured input for
 * which the clamp fires.</p>
 *
 * <p>The clamp is therefore rewritten as unreachable defence rather than deleted as a live bug,
 * and its comment - which said "KEY FIX: Use outerIndex to select the correct element" directly
 * above a line that does the opposite when the index is out of range - is corrected. No behaviour
 * changes. Saying that plainly is the point of this class: a repair that claims to have closed a
 * corruption path it never opened is the same kind of false record as the CHANGELOG entry that
 * recorded a phantom signature as a correction.</p>
 */
@DisplayName("the nested-array outer-index clamp is guarded upstream, not feeding a duplicator")
class AvroNestedArrayOuterIndexClampTest {

    private static final String SCHEMA = """
            {"type":"record","name":"Root","fields":[
              {"name":"orders","type":{"type":"array","items":
                {"type":"record","name":"Order","fields":[
                  {"name":"id","type":"string"},
                  {"name":"items","type":{"type":"array","items":
                    {"type":"record","name":"Item","fields":[
                      {"name":"sku","type":"string"}
                    ]}}}
                ]}}}
            ]}
            """;

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> orders(Map<String, Object> out) {
        Object raw = out.get("orders");
        assertNotNull(raw, "no orders reconstructed; got " + out.keySet());
        return (List<Map<String, Object>>) raw;
    }

    @Test
    @DisplayName("a short nested column is REFUSED upstream, so the clamp never gets the index")
    void aShortNestedArrayColumnIsRefusedBeforeTheClampCanFire() {
        Schema schema = new Schema.Parser().parse(SCHEMA);

        // An EXTERNALLY produced flat map - two orders, one inner slot. MapFlattener will no
        // longer emit this shape (see the producer test below); Athena and Spark can.
        Map<String, Object> flat = new LinkedHashMap<>();
        flat.put("orders_id", "[\"O1\",\"O2\"]");
        flat.put("orders_items_sku", "[\"[\\\"A\\\"]\"]");

        AvroReconstructor.ArrayCardinalityException thrown = assertThrows(
                AvroReconstructor.ArrayCardinalityException.class,
                () -> AvroReconstructor.builder().build().reconstructToMap(flat, schema),
                "THE REFUTATION. If this stops throwing, the clamp becomes reachable and the "
                        + "duplicator the analysis described becomes real - so this assertion is "
                        + "the thing standing in front of it, not the clamp's own shape.");

        assertTrue(thrown.getMessage().contains("orders_id=2"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("orders_items_sku=1"), thrown.getMessage());
    }

    @Test
    @DisplayName("CONTROL: a well-formed column reconstructs each outer position's own items")
    void aWellFormedColumnStillWorks() {
        Schema schema = new Schema.Parser().parse(SCHEMA);

        Map<String, Object> flat = new LinkedHashMap<>();
        flat.put("orders_id", "[\"O1\",\"O2\"]");
        flat.put("orders_items_sku", "[\"[\\\"A\\\"]\",\"[\\\"B\\\"]\"]");

        List<Map<String, Object>> reconstructed =
                orders(AvroReconstructor.builder().build().reconstructToMap(flat, schema));
        assertEquals(2, reconstructed.size());

        List<String> skus = new ArrayList<>();
        for (Map<String, Object> order : reconstructed) {
            for (Object item : (List<?>) order.get("items")) {
                skus.add(String.valueOf(((Map<?, ?>) item).get("sku")));
            }
        }
        assertEquals(List.of("A", "B"), skus,
                "CONTROL: the refusal above must be about the RAGGED shape, not about this whole "
                        + "schema. A guard that rejects everything is indistinguishable from one "
                        + "that rejects the right thing until both directions are drilled.");
    }

    @Test
    @DisplayName("MapFlattener no longer produces a short column at this site")
    void mapFlattenerNoLongerProducesAShortColumnHere() {
        // The producer half. After the site-C repair every column under a nested-array prefix
        // carries one slot per outer position, so this library can no longer hand the
        // reconstructor the shape that the refusal above rejects.
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("g", List.of(
                List.of(Map.of("a", 1)),
                List.of(Map.of("b", 2))));

        Map<String, Object> flat = MapFlattener.builder().build().flatten(doc);
        assertEquals("[[1],[null]]", flat.get("g_a"));
        assertEquals("[[null],[2]]", flat.get("g_b"));
    }
}
