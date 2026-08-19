package io.github.pierce;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The THIRD array-element site: {@code extractFieldsPreservingStructure}, the nested-array arm.
 *
 * <h2>What was wrong</h2>
 *
 * <p>{@code flattenList} Case 3 and {@code extractFieldsFromList} were repaired in 2.1.0 to write
 * their columns by INDEX. This site was left appending, deliberately and with a recorded reason
 * ([BL-018]). The consequence is the same class of silent corruption the other two had:
 * {@code {"g":[[{"a":1}],[{"b":2}]]}} emitted {@code g_a="[[1]]"} beside {@code g_b="[[2]]"}, so a
 * consumer zipping the two columns by outer index reads {@code a=1} and {@code b=2} as belonging
 * to the SAME nested group. They came from different groups.</p>
 *
 * <h2>Why the filler is an inner list of nulls and not a bare null</h2>
 *
 * <p>The deferral note objected that padding here "would place a bare null where a nested LIST has
 * always been". That objection is correct and it SELECTS the filler rather than blocking the fix.
 * At the two array-of-maps sites a column entry is a SCALAR, so a hole is a scalar null. Here a
 * column entry is an INNER LIST, so a hole must be an inner list of the right inner cardinality -
 * {@code columnFor}'s own rule applied one level down. A bare null appears only where the outer
 * position holds no nested list at all.</p>
 *
 * <p>{@code []} was rejected as the filler for a reason the corpus already pins:
 * {@code structural/array-of-arrays-with-empty-inner} records {@code grid_rows="[[1,2],[],[3]]"}.
 * Filling holes with {@code []} would make "position 1's inner array was empty" indistinguishable
 * from "position 1's inner array had elements, none of which carried this column". That row
 * re-recording is the signal that this rule was implemented wrong.</p>
 */
@DisplayName("nested-array columns keep every group under the outer position that carried it")
class MapFlattenerNestedArrayAlignmentTest {

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

    // ------------------------------------------------------------------ the minimal defect

    @Test
    @DisplayName("site C keeps each nested group under its own outer position")
    void siteCKeepsEachNestedGroupUnderItsOwnOuterPosition() throws Exception {
        Map<String, Object> flat = flatten("{\"g\":[[{\"a\":1}],[{\"b\":2}]]}");

        assertEquals("[[1],[null]]", flat.get("g_a"));
        assertEquals("[[null],[2]]", flat.get("g_b"));
        assertEquals(2, column(flat, "g_a").size(),
                "g_a must describe both outer positions");
        assertEquals(2, column(flat, "g_b").size(),
                "b came from outer position 1; appending put it at index 0, where a consumer "
                        + "zipping the two columns fuses two different nested groups into one");
    }

    @Test
    @DisplayName("site C columns agree on outer AND inner cardinality, position by position")
    void siteCColumnsAgreeOnOuterAndInnerCardinality() throws Exception {
        // This is the document BL-013's 2.1.0 correction used to REFUTE the equal-length claim.
        Map<String, Object> flat = flatten("{\"g\":[[{\"a\":1},{\"b\":2}],[{\"a\":3}]]}");

        assertEquals("[[1,null],[3]]", flat.get("g_a"));
        assertEquals("[[null,2],[null]]", flat.get("g_b"));

        List<?> a = column(flat, "g_a");
        List<?> b = column(flat, "g_b");
        assertEquals(2, a.size());
        assertEquals(2, b.size());
        // Inner sizes asserted EXPLICITLY, never column-to-column equality alone: two columns
        // can agree with each other and both be wrong, which is how the tail pad hid the
        // original defect at the other two sites.
        assertEquals(2, ((List<?>) a.get(0)).size());
        assertEquals(2, ((List<?>) b.get(0)).size());
        assertEquals(1, ((List<?>) a.get(1)).size());
        assertEquals(1, ((List<?>) b.get(1)).size());
    }

    @Test
    @DisplayName("an outer position holding a scalar leaves a bare null in the field columns")
    void siteCPositionHoldingAScalarLeavesABareNullInFieldColumns() throws Exception {
        // THE ONLY SHAPE IN WHICH A BARE NULL APPEARS IN A COLUMN OF NESTED LISTS, and it appears
        // because outer position 1 holds no nested list at all. These two strings must stay
        // identical to the ones recorded in
        // src/test/resources/fidelity/structural/mixed-nested-array-sentinel-collision.json; if
        // they ever diverge, one of the two was written to match the code rather than the contract.
        Map<String, Object> flat = flatten("{\"data\":[[{\"name\":\"A\"}],\"text\"]}");

        assertEquals("[[\"A\"],null]", flat.get("data_name"));

        // A CORRECTION TO THE DESIGN NOTE THAT ORDERED THIS FIX, stated rather than quietly
        // absorbed. It predicted data="[null,\"text\"]" - a BARE null at outer position 0 in the
        // sentinel column. The shipped rule gives "[[null],\"text\"]" instead, because the rule
        // is uniform: at a position that DOES hold a nested list, a missing column is an inner
        // list of that position's inner cardinality, and the sentinel column is not special.
        //
        // The uniform rule was chosen because it keeps the invariant this whole class exists to
        // establish: at every outer position, every column agrees on inner length. Position 0
        // here holds an inner array of one element, so data_name has an inner list of length 1
        // there and data must too. A bare null would break that agreement in the one column most
        // likely to be read alongside the others, and would discard the inner cardinality for
        // nothing.
        assertEquals("[[null],\"text\"]", flat.get("data"));

        List<?> name = column(flat, "data_name");
        List<?> base = column(flat, "data");
        assertEquals(2, name.size());
        assertEquals(2, base.size());
        assertEquals(1, ((List<?>) name.get(0)).size());
        assertEquals(1, ((List<?>) base.get(0)).size());
    }

    // ------------------------------------------------------------------ the pin that rules out
    //                                                                     the wrong implementation

    @Test
    @DisplayName("PIN: an empty inner list stays an empty list and is not filled with null")
    void anEmptyInnerListStaysAnEmptyListAndIsNotFilledWithNull() throws Exception {
        // DECLARED PIN: passes before AND after. It is the guard that rules out the two wrong
        // fillers. Any fix that pads holes with a bare null, or that pads them with [] without
        // distinguishing "the inner list was empty" from "the inner list had elements, none of
        // which carried this column", changes this string - and takes the corpus row
        // structural/array-of-arrays-with-empty-inner with it.
        Map<String, Object> flat = flatten("{\"grid\":{\"rows\":[[1,2],[],[3]]}}");
        assertEquals("[[1,2],[],[3]]", flat.get("grid_rows"));
    }

    @Test
    @DisplayName("PIN, MAP ARM: N elements that carry no column emit N nulls, never []")
    void innerCardinalitySurvivesWhenNoInnerElementCarriesTheColumn() throws Exception {
        // THE PIN ABOVE IS A SCALAR-ONLY CONTROL AND COULD NOT FAIL FOR THE PROPERTY IT NAMES.
        // {"grid":{"rows":[[1,2],[],[3]]}} is all scalars, so it never reaches the map arm of
        // extractFieldsPreservingStructure - the only arm where "the inner array was empty" and
        // "the inner array had elements, none of which carried this column" can both occur. The
        // whole rule the class javadoc, the changelog and that pin assert lives in this arm and
        // nothing exercised it. Measured at b48e177, all four of these emitted the SAME string:
        //   g_a=[[],[1]]  for inner cardinality 0, 1, 2 AND 3.
        // The guard read `nested.isEmpty()` - the FLATTENED result - so an inner list of N empty
        // maps took the empty-array path and recorded innerSize 0. [] then meant both facts at
        // once, which is exactly the collapse the pin above says changes its string.
        assertEquals("[[],[1]]", flatten("{\"g\":[[],[{\"a\":1}]]}").get("g_a"),
                "genuinely empty inner array: [] is correct and must NOT become [null]");
        assertEquals("[[null],[1]]", flatten("{\"g\":[[{}],[{\"a\":1}]]}").get("g_a"));
        assertEquals("[[null,null],[1]]", flatten("{\"g\":[[{},{}],[{\"a\":1}]]}").get("g_a"));
        assertEquals("[[null,null,null],[1]]",
                flatten("{\"g\":[[{},{},{}],[{\"a\":1}]]}").get("g_a"));

        // The base (sentinel) column obeys the same rule, because the rule is uniform.
        assertEquals("[[],[null]]", flatten("{\"g\":[[],[{\"a\":1}]]}").get("g"));
        assertEquals("[[null],[null]]", flatten("{\"g\":[[{}],[{\"a\":1}]]}").get("g"));
        assertEquals("[[null,null],[null]]", flatten("{\"g\":[[{},{}],[{\"a\":1}]]}").get("g"));
    }

    @Test
    @DisplayName("the base-key column appears from PURE JSON, not only from a Map source")
    void theBaseKeyColumnIsReachableFromPlainJson() throws Exception {
        // CHANGELOG item 22 said the key-set change was "only reachable from a Map source, not
        // from JSON", and the assertion below used to say "THIS IS THE ONE KEY-SET CHANGE IN THE
        // FIX". Both were wrong: the branch that registers the outer position under the base key
        // fires whenever an inner list flattens to no columns, and an inner list of empty maps
        // does that from plain JSON. Four documents with no Java array anywhere:
        assertEquals("[[null]]", flatten("{\"g\":[[{}]]}").get("g"));

        Map<String, Object> two = flatten("{\"g\":[[{}],[{\"a\":1}]]}");
        assertEquals("[[null],[null]]", two.get("g"));
        assertEquals("[[null],[1]]", two.get("g_a"));

        Map<String, Object> reversed = flatten("{\"g\":[[{\"a\":1}],[{}]]}");
        assertEquals("[[null],[null]]", reversed.get("g"));
        assertEquals("[[1],[null]]", reversed.get("g_a"));

        Map<String, Object> explicitNull = flatten("{\"g\":[[{\"a\":null}],[{}]]}");
        assertEquals("[[null],[null]]", explicitNull.get("g"));
        assertEquals("[[null],[null]]", explicitNull.get("g_a"));
    }

    @Test
    @DisplayName("an empty nested Java array no longer loses its outer position")
    void anEmptyNestedJavaArrayNoLongerLosesItsOuterPosition() {
        // A Java array, not JSON: the array arm of extractFieldsPreservingStructure is only
        // reachable from a Map source, and it had no empty check at all, so outer position 0
        // vanished from every column and position 1's value landed at index 0.
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("g", Arrays.asList(new String[0], singletonList(singletonMap("a", 1))));

        Map<String, Object> flat = MapFlattener.builder().build().flatten(doc);

        assertEquals("[[],[1]]", flat.get("g_a"));
        assertTrue(flat.containsKey("g"),
                "the empty array's outer position must be recorded under the base key; it used "
                        + "to disappear entirely. THIS IS THE ONE KEY-SET CHANGE IN THE FIX.");
        assertEquals("[[],[null]]", flat.get("g"));
    }

    // ------------------------------------------------------------------ all three sites at once

    @Test
    @DisplayName("all three array-element sites carry one slot per source element")
    void allThreeArrayElementSitesCarryOneSlotPerSourceElement() throws Exception {
        record Case(String site, String json, String prefix, int outerElements) { }
        List<Case> cases = List.of(
                new Case("SITE A (flattenList Case 3)",
                        "{\"r\":[{\"k1\":1},{\"k2\":2}]}", "r", 2),
                new Case("SITE B (extractFieldsFromList hasMaps)",
                        "{\"g\":[[{\"a\":1},{\"b\":2}]]}", "g", 1),
                new Case("SITE C (extractFieldsPreservingStructure)",
                        "{\"g\":[[{\"a\":1}],[{\"b\":2}]]}", "g", 2));

        int checked = 0;
        for (Case c : cases) {
            Map<String, Object> flat = flatten(c.json());
            List<String> keys = new ArrayList<>(flat.keySet());
            assertTrue(keys.size() >= 2, c.site() + " produced too few columns: " + keys);
            for (String key : keys) {
                List<?> col = column(flat, key);
                assertEquals(c.outerElements(), col.size(),
                        c.site() + ": column " + key + " must carry one slot per source element "
                                + "at this level. A column shorter than the source is a column "
                                + "whose values sit under the wrong element.");
                checked++;
            }
            if (c.site().startsWith("SITE C")) {
                // Inner agreement, position by position - only site C has an inner level.
                List<?> a = column(flat, c.prefix() + "_a");
                List<?> b = column(flat, c.prefix() + "_b");
                for (int i = 0; i < a.size(); i++) {
                    assertEquals(((List<?>) a.get(i)).size(), ((List<?>) b.get(i)).size(),
                            c.site() + ": inner length at outer position " + i + " disagrees");
                }
            }
        }
        assertEquals(6, checked,
                "VERIFY THE COUNT: this is a loop over a table, and an empty table asserts "
                        + "nothing. Six columns across the three sites.");
    }

    // ------------------------------------------------------------------ every array format

    @Test
    @DisplayName("site C under each array format, including what a hole renders as")
    void siteCUnderEachArrayFormat() throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, Object> doc =
                MAPPER.readValue("{\"data\":[[{\"name\":\"A\"}],\"text\"]}", Map.class);

        Map<String, Object> json = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.JSON).build().flatten(doc);
        assertEquals("[[\"A\"],null]", json.get("data_name"));
        assertEquals("[[null],\"text\"]", json.get("data"));

        Map<String, Object> bracket = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.BRACKET_LIST).build().flatten(doc);
        assertEquals("[[\"A\"], null]", bracket.get("data_name"));
        assertEquals("[[null], \"text\"]", bracket.get("data"));

        // RECORDED AS A LOSS, NOT CELEBRATED. In the two delimited formats the new positional
        // hole renders as the EMPTY STRING (data_name gains a TRAILING delimiter), so a splitter
        // cannot tell a hole from an empty value. That is the same ambiguity the class javadoc
        // already documents for scalars, now reaching nested-array columns for the first time.
        // Pinned here so a caller does not discover it in production.
        Map<String, Object> comma = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.COMMA_SEPARATED).build().flatten(doc);
        assertEquals("[A],", comma.get("data_name"));
        assertEquals("[null],text", comma.get("data"));

        Map<String, Object> pipe = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.PIPE_SEPARATED).build().flatten(doc);
        assertEquals("[A]|", pipe.get("data_name"));
        assertEquals("[null]|text", pipe.get("data"));
    }
}
