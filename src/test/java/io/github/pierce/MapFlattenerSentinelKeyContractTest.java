package io.github.pierce;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The EMIT side of [BL-022]: what the sentinel column is called, and why it is not renamed.
 *
 * <h2>The decision this class pins</h2>
 *
 * <p>{@code MapFlattener.VALUE_SENTINEL} is an INTERNAL column name. At both emit sites - Case 2
 * of {@code flattenList} (nested arrays) and Case 3 (array of maps) - the column it stands for is
 * written under the BASE key, verbatim, with no suffix. {@code {"data":[[{"name":"A"}],"text"]}}
 * emits exactly {@code {data, data_name}}. That is deliberate and it stays.</p>
 *
 * <h2>Why {@code data_value} was refused</h2>
 *
 * <p>{@code joinEncodedKey} extends an ALREADY-ENCODED path and does not re-escape it, so a user
 * field literally named {@code value} produces the segment {@code value} and the key
 * {@code data_value}. A suffixed sentinel would produce the identical string. The two tests below
 * measure both collisions the rename would create, at both sites, and they are the reason the
 * rename is not an option rather than a preference:</p>
 * <ul>
 *   <li>Case 2, {@code {"data":[[{"value":"A"}],"text"]}} - both columns would compute the key
 *       {@code data_value} and the second {@code result.put} into a {@code LinkedHashMap} would
 *       drop the first. One column lost at FLATTEN time, where nothing downstream can undo it.</li>
 *   <li>Case 3, {@code {"mixed":[{"value":1},"x"]}} - {@code columnFor} would return the EXISTING
 *       column, so element 0's field and element 1's bare scalar would be fused into
 *       {@code mixed_value="[1,\"x\"]"}, destroying the distinction inside one column.</li>
 * </ul>
 *
 * <p>No suffix escapes this. {@code FlattenedPath} is a bijection over USER segment lists with no
 * reserved namespace: {@code _value} escapes to {@code \_value}, which is exactly what a user
 * field named {@code _value} produces (pinned by the fixture
 * {@code naming/user-field-named-value-vs-flattener-sentinel}). The only unforgeable form,
 * {@code __x__}, is dropped outright by {@code JsonReconstructor.processArrays}.</p>
 *
 * <h2>Honest statement of what these tests are</h2>
 *
 * <p>Every test in this class PASSES against the code as it stood before [BL-022] was resolved.
 * They are REGRESSION PINS and CHARACTERISATION tests, not fails-before tests, and calling them
 * anything else would misrepresent them. Their value is that the flatten-side collisions above
 * had no coverage at all, so an editor implementing the suffix would have seen a green build.</p>
 */
@DisplayName("the sentinel column keeps the base key, and the suffix that was refused would collide")
class MapFlattenerSentinelKeyContractTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @SuppressWarnings("unchecked")
    private static Map<String, Object> flatten(String json) throws Exception {
        return MapFlattener.builder().build().flatten(MAPPER.readValue(json, Map.class));
    }

    // ------------------------------------------------------------------ the two traced collisions

    @Test
    @DisplayName("a user field named value beside the sentinel stays two distinguishable columns")
    void aUserFieldNamedValueBesideTheSentinelStaysTwoDistinguishableColumns() throws Exception {
        // CASE 2 - MapFlattener.java flattenList, the nested-array arm. Under a data_value
        // rename both of these would be keyed data_value and the second result.put would win,
        // leaving one column. Today they are two keys and nothing is lost.
        Map<String, Object> nested = flatten("{\"data\":[[{\"value\":\"A\"}],\"text\"]}");
        assertEquals(Set.of("data_value", "data"), nested.keySet(),
                "renaming the sentinel to data_value collapses these two columns into one at "
                        + "flatten time - see MapFlattener flattenList Case 2");
        assertEquals("[[\"A\"],null]", nested.get("data_value"));
        assertEquals("[[null],\"text\"]", nested.get("data"));

        // CASE 3 - the array-of-maps arm, the site the backlog entry never named. Under a rename
        // columnFor would hand back the SAME column for both writes and fuse them into
        // mixed_value="[1,\"x\"]".
        Map<String, Object> arrayOfMaps = flatten("{\"mixed\":[{\"value\":1},\"x\"]}");
        assertEquals(Set.of("mixed_value", "mixed"), arrayOfMaps.keySet(),
                "renaming the sentinel to mixed_value fuses element 0's field with element 1's "
                        + "bare scalar via columnFor - see MapFlattener flattenList Case 3");
        assertEquals("[1,null]", arrayOfMaps.get("mixed_value"));
        assertEquals("[null,\"x\"]", arrayOfMaps.get("mixed"));
    }

    @Test
    @DisplayName("a user field literally named _value escapes to the encoding any suffix would need")
    void aUserFieldNamedUnderscoreValueOccupiesTheOnlyEscapeASuffixCouldUse() throws Exception {
        // There is no collision-free suffix, because the encoded namespace has no reserved
        // region: whatever a sentinel suffix escapes to, a user field of that name escapes to the
        // same thing.
        Map<String, Object> flat = flatten("{\"a\":{\"_value\":1}}");
        assertEquals(Set.of("a_\\_value"), flat.keySet());
    }

    // ------------------------------------------------------------------ the cost of a blanket
    //                                                                     rename

    @Test
    @DisplayName("array-of-arrays-of-scalars has a sentinel base key with no sibling and no defect")
    void anArrayOfArraysOfScalarsHasASentinelBaseKeyWithNoSiblingAndNoCollision() throws Exception {
        // grid_rows IS a sentinel-sourced base key. It has no sibling column and no collision, so
        // a blanket rename would turn released 2.0.0 output into grid_rows_value for a shape that
        // has nothing wrong with it. This is what makes the rename a cost with no matching
        // benefit, and it is why "rename only when a sibling exists" is worse still: the column
        // name would then depend on the sibling key set, so adding one map element to an array
        // would silently rename the scalar column and no schema generator could predict it.
        Map<String, Object> flat = flatten("{\"grid\":{\"rows\":[[1,2],[],[3]]}}");
        assertEquals(Set.of("grid_rows"), flat.keySet());
        assertEquals("[[1,2],[],[3]]", flat.get("grid_rows"));
    }

    // ------------------------------------------------------------------ the undocumented shape

    @Test
    @DisplayName("CHARACTERISATION: a map beside a nested array is stringified into the base column")
    void aMapBesideANestedArrayIsStringifiedIntoTheBaseKeyColumn() throws Exception {
        // Found by tracing, not by a failing test: isNestedList is false for a Map, so an outer
        // position holding a MAP beside a nested list takes the scalar arm of
        // extractFieldsPreservingStructure and is handed to stringifyObject. Its fields are never
        // extracted. Zero fixtures reach this shape and zero tests asserted it before this one.
        //
        // It is also a second, independent reason the name "_value" would be a misdescription:
        // the base-key column is not type-homogeneous. It carries scalars AND the JSON TEXT of
        // objects, in the same column, at different indices.
        Map<String, Object> flat = flatten("{\"mixed\":[{\"a\":1},[2,3]]}");

        assertEquals(Set.of("mixed"), flat.keySet(),
                "there is no mixed_a: the map at outer position 0 is never field-extracted");
        assertFalse(flat.containsKey("mixed_a"));

        List<?> column = MAPPER.readValue(String.valueOf(flat.get("mixed")), List.class);
        assertEquals(2, column.size());
        assertEquals("{\"a\":1}", column.get(0),
                "slot 0 is the JSON TEXT of the map, produced by stringifyObject");
        assertEquals(List.of(2, 3), column.get(1),
                "slot 1 is the inner list, structurally intact");
    }

    // ------------------------------------------------------------------ the schema agreement the
    //                                                                     rename would have broken

    @Test
    @DisplayName("the data column name still matches what the Avro schema flatteners emit")
    void theDataColumnNameStillMatchesWhatTheSchemaFlattenersEmit() throws Exception {
        // AvroSchemaFlattener and GAvroSchemaFlattener both emit basePath for array-of-primitives
        // and array-of-arrays. A data-side rename without a schema-side rename in the same commit
        // produces a flat row whose column names do not match the flattened schema - a silent
        // field-not-found in Spark or Avro rather than a loud failure. Nothing else in the gate
        // set compares the two, so this is the pin.
        Map<String, Object> flat = flatten("{\"tags\":[\"a\",\"b\"]}");
        assertTrue(flat.containsKey("tags"),
                "an array of primitives is emitted under its base path on BOTH the data side and "
                        + "the schema side; they must be renamed together or not at all");
    }

    // ------------------------------------------------------------------ both emit sites, one fact

    @Test
    @DisplayName("both emit sites map the sentinel to the base key")
    void bothEmitSitesMapTheSentinelToTheBaseKey() throws Exception {
        Map<String, Object> case2 = flatten("{\"data\":[[{\"name\":\"A\"}],\"text\"]}");
        assertEquals(Set.of("data_name", "data"), case2.keySet(),
                "Case 2, the nested-array arm");

        Map<String, Object> case3 = flatten("{\"mixed\":[{\"a\":1},\"x\"]}");
        assertEquals(Set.of("mixed_a", "mixed"), case3.keySet(),
                "Case 3, the array-of-maps arm - the site the backlog entry never named");

        // And with a Map source rather than parsed JSON, so the fact is not an artefact of the
        // JSON parser's typing.
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("mixed", List.of(Map.of("a", 1), "x"));
        assertEquals(Set.of("mixed_a", "mixed"),
                MapFlattener.builder().build().flatten(source).keySet());
    }
}
