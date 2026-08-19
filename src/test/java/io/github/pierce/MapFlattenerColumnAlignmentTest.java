package io.github.pierce;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Array-element column ALIGNMENT. Not length - alignment.
 *
 * <p>{@code flattenList} Case 3 and {@code extractFieldsFromList} both built their columns by
 * APPENDING each value as it was encountered and then equalising the columns with a TAIL pad.
 * A column that first appeared at element k therefore landed at index 0, and every later value
 * for that column was shifted left by the number of earlier elements that lacked it.</p>
 *
 * <p>THE TAIL PAD IS WHY NOTHING DOWNSTREAM COULD SEE THIS. It already made every column the
 * same LENGTH, so a length check - including {@code AvroReconstructor}'s ArrayCardinalityException
 * - passed while the values sat under the wrong elements. That is the reason the assertions below
 * are written against the SOURCE ELEMENT COUNT and against specific indices, never against
 * column-to-column length equality, which passes vacuously in both directions.</p>
 *
 * <p>Published as a corpus defect since the corpus was recorded:
 * {@code limits/sparse-array-of-maps-padding-misaligns-elements}, titled "values move to the
 * wrong element". It was never a new finding.</p>
 */
@DisplayName("array-element columns keep every value under the element that carried it")
class MapFlattenerColumnAlignmentTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Map<String, Object> flatten(String json) throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, Object> doc = MAPPER.readValue(json, Map.class);
        return MapFlattener.builder().build().flatten(doc);
    }

    private static List<?> column(Map<String, Object> flat, String key) throws Exception {
        Object raw = flat.get(key);
        assertNotNull(raw, "column " + key + " absent; present columns are " + flat.keySet());
        return MAPPER.readValue(String.valueOf(raw), List.class);
    }

    @Test
    @DisplayName("a branch absent from the first element leaves a positional hole")
    void aBranchAbsentFromTheFirstElementLeavesAPositionalHole() throws Exception {
        Map<String, Object> flat = flatten(
                "{\"items\":[{\"sku\":\"a\",\"meta\":{\"src\":\"web\"}},{\"sku\":\"b\",\"meta\":\"plain\"}]}");

        assertEquals("[null,\"plain\"]", flat.get("items_meta"),
                "element 1's scalar meta must sit at index 1, not be relocated to index 0");
        assertEquals("[\"web\",null]", flat.get("items_meta_src"));
        assertEquals("[\"a\",\"b\"]", flat.get("items_sku"));
    }

    @Test
    @DisplayName("every column has exactly one entry per source element")
    void everyColumnHasExactlyOneEntryPerSourceElement() throws Exception {
        Map<String, Object> flat = flatten("{\"mixed\":[{\"a\":1},{\"a\":2},\"x\",\"y\"]}");

        // The assertion is length == SOURCE ELEMENT COUNT. Column-to-column equality passes
        // vacuously before the fix (both columns are length 2 and both are wrong).
        assertEquals(4, column(flat, "mixed_a").size(), "mixed_a must carry one slot per element");
        assertEquals(4, column(flat, "mixed").size(), "mixed must carry one slot per element");

        assertEquals("[1,2,null,null]", flat.get("mixed_a"));
        assertEquals("[null,null,\"x\",\"y\"]", flat.get("mixed"));
    }

    @Test
    @DisplayName("a sparse array of maps keeps each value under its own element")
    void sparseArrayOfMapsKeepsEachValueUnderItsOwnElement() throws Exception {
        Map<String, Object> flat = flatten(
                "{\"r\":[{\"k1\":1,\"z\":0},{\"k2\":2,\"z\":0},{\"k3\":3,\"z\":0}]}");

        assertEquals("[1,null,null]", flat.get("r_k1"));
        assertEquals("[null,2,null]", flat.get("r_k2"));
        assertEquals("[null,null,3]", flat.get("r_k3"));
        assertEquals("[0,0,0]", flat.get("r_z"));
    }

    @Test
    @DisplayName("the hole is created at depth two as well")
    void theHoleIsCreatedAtDepthTwoAsWell() throws Exception {
        // SITE B, extractFieldsFromList. No corpus row reaches this, which is why it is a unit test.
        Map<String, Object> flat = flatten("{\"g\":[[{\"a\":1},{\"b\":2}]]}");

        assertEquals("[[1,null]]", flat.get("g_a"));
        assertEquals("[[null,2]]", flat.get("g_b"));
    }

    @Test
    @DisplayName("REGRESSION PIN: a field present in the first element and absent later is unchanged")
    void regressionPinAFieldPresentInTheFirstElementAndAbsentLaterIsUnchanged() throws Exception {
        // DECLARED PIN: passes before AND after. Tail-padding and index-padding coincide exactly
        // in this direction, so this is the guard that the fix does not churn the recording of
        // structural/ragged-array-of-objects-absent-vs-null.
        Map<String, Object> flat = flatten("{\"rows\":[{\"a\":1,\"b\":2},{\"a\":3}]}");

        assertEquals("[1,3]", flat.get("rows_a"));
        assertEquals("[2,null]", flat.get("rows_b"));
    }

    @Test
    @DisplayName("a discount stays on the line item that carried it")
    void aDiscountStaysOnTheLineItemThatCarriedIt() throws Exception {
        // real-world/order-optional-discount-absent, verbatim. Silent financial corruption,
        // not loss. Note the escaped keys: an underscore inside a source field name is written
        // "\_" so it cannot be confused with the separator.
        Map<String, Object> flat = flatten(
                "{\"order_id\":\"ORD-2026-0004\",\"currency_code\":\"USD\",\"line_items\":["
                        + "{\"sku\":\"SKU-100\",\"quantity\":1,\"unit_price\":19.99},"
                        + "{\"sku\":\"SKU-205\",\"quantity\":2,\"unit_price\":7.25,\"discount_pct\":15}]}");

        assertEquals("[null,15]", flat.get("line\\_items_discount\\_pct"),
                "the 15% discount belongs to SKU-205, the second line item");
        assertEquals("[\"SKU-100\",\"SKU-205\"]", flat.get("line\\_items_sku"));
    }

    @Test
    @DisplayName("columns under one array prefix agree on cardinality by being right, not by padding")
    void theFixDoesNotMakeColumnsDisagreeOnCardinality() throws Exception {
        // The equal-length half is a PIN (already true today, by tail-padding). The
        // length == element-count half FAILS BEFORE for the heterogeneous documents. Both halves
        // are asserted together on purpose: satisfying only the first is the failure mode this
        // whole test class exists to rule out.
        Map<String, Object> scalarFirst = flatten("{\"mixed\":[1,2,{\"a\":3}]}");
        assertEquals(3, column(scalarFirst, "mixed").size());
        assertEquals(3, column(scalarFirst, "mixed_a").size());

        Map<String, Object> objectFirst = flatten("{\"mixed\":[{\"a\":1},{\"a\":2},\"x\",\"y\"]}");
        assertEquals(4, column(objectFirst, "mixed").size());
        assertEquals(4, column(objectFirst, "mixed_a").size());
    }
}
