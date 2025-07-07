package io.github.pierce.FlattenConsolidatorTests;

import io.github.pierce.JsonFlattenerConsolidator;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class JsonFlattenerConsolidatorTest {

    private JsonFlattenerConsolidator flattenerConsolidator;

    @BeforeEach
    void setUp() {
        flattenerConsolidator = new JsonFlattenerConsolidator(",", "null", 50, 1000, false);
    }

    @Test
    void testFlattenSimpleJson() {
        String input = "{\"name\": \"John\", \"age\": 30}";
        String result = flattenerConsolidator.flattenAndConsolidateJson(input);

        JSONObject resultJson = new JSONObject(result);
        assertThat(resultJson.getString("name")).isEqualTo("John");
        assertThat(resultJson.getInt("age")).isEqualTo(30);
    }

    @Test
    void testFlattenNestedJson() {
        String input = "{\"person\": {\"name\": \"John\", \"address\": {\"city\": \"NYC\"}}}";
        String result = flattenerConsolidator.flattenAndConsolidateJson(input);

        JSONObject resultJson = new JSONObject(result);
        assertThat(resultJson.getString("person_name")).isEqualTo("John");
        assertThat(resultJson.getString("person_address_city")).isEqualTo("NYC");
    }

    @Test
    void testFlattenArrayOfPrimitives() {
        String input = "{\"numbers\": [1, 2, 3, 4, 5]}";
        String result = flattenerConsolidator.flattenAndConsolidateJson(input);

        JSONObject resultJson = new JSONObject(result);
        assertThat(resultJson.getString("numbers")).isEqualTo("1,2,3,4,5");
        assertThat(resultJson.getInt("numbers_count")).isEqualTo(5);
        assertThat(resultJson.getInt("numbers_distinct_count")).isEqualTo(5);
    }

    @Test
    void testFlattenArrayOfObjects() {
        String input = "{\"users\": [{\"name\": \"John\", \"age\": 30}, {\"name\": \"Jane\", \"age\": 25}]}";
        String result = flattenerConsolidator.flattenAndConsolidateJson(input);

        JSONObject resultJson = new JSONObject(result);
        assertThat(resultJson.getString("users_name")).isEqualTo("John,Jane");
        assertThat(resultJson.getString("users_age")).isEqualTo("30,25");
        assertThat(resultJson.getInt("users_name_count")).isEqualTo(2);
        assertThat(resultJson.getInt("users_age_count")).isEqualTo(2);
    }

    @Test
    void testHandleNullValues() {
        JsonFlattenerConsolidator consolidator = new JsonFlattenerConsolidator(",", "NULL", 50, 1000, false);
        String input = "{\"value\": null, \"array\": [1, null, 3]}";
        String result = consolidator.flattenAndConsolidateJson(input);

        JSONObject resultJson = new JSONObject(result);
        assertThat(resultJson.getString("value")).isEqualTo("NULL");
        assertThat(resultJson.getString("array")).isEqualTo("1,NULL,3");
    }


    @Test
    void testArraySizeLimit() {
        JsonFlattenerConsolidator consolidator = new JsonFlattenerConsolidator(",", null, 50, 3, false);
        String input = "{\"numbers\": [1, 2, 3, 4, 5]}";
        String result = consolidator.flattenAndConsolidateJson(input);

        JSONObject resultJson = new JSONObject(result);

        assertThat(resultJson.getString("numbers")).isEqualTo("1,2,3");
    }

    @Test
    void testConsolidateWithMatrixDenotors() {
        JsonFlattenerConsolidator consolidator = new JsonFlattenerConsolidator(",", null, 50, 1000, true);
        String input = "{\"matrix\": [[1, 2], [3, 4]]}";
        String result = consolidator.flattenAndConsolidateJson(input);

        JSONObject resultJson = new JSONObject(result);

        assertThat(resultJson.getString("matrix")).contains("[");
    }

    @Test
    void testEmptyJson() {
        String result = flattenerConsolidator.flattenAndConsolidateJson("{}");
        assertThat(result).isEqualTo("{}");
    }

    @Test
    void testNullInput() {
        String result = flattenerConsolidator.flattenAndConsolidateJson(null);
        assertThat(result).isEqualTo("{}");
    }

    @Test
    void testInvalidJson() {
        String result = flattenerConsolidator.flattenAndConsolidateJson("{invalid json}");
        assertThat(result).isEqualTo("{}");
    }

    @Test
    void testStatisticsGeneration() {
        String input = "{\"words\": [\"hello\", \"world\", \"hello\", \"test\"]}";
        String result = flattenerConsolidator.flattenAndConsolidateJson(input);

        JSONObject resultJson = new JSONObject(result);
        assertThat(resultJson.getInt("words_count")).isEqualTo(4);
        assertThat(resultJson.getInt("words_distinct_count")).isEqualTo(3);
        assertThat(resultJson.getInt("words_min_length")).isEqualTo(4);
        assertThat(resultJson.getInt("words_max_length")).isEqualTo(5);
        assertThat(resultJson.getDouble("words_avg_length")).isCloseTo(4.75, within(0.01));
        assertThat(resultJson.getString("words_type")).isEqualTo("string_list_consolidated");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"mixed\": [1, \"two\", true]}",
            "{\"nested\": [{\"a\": 1}, {\"b\": 2}]}"
    })
    void testMixedTypes(String input) {
        String result = flattenerConsolidator.flattenAndConsolidateJson(input);
        assertThat(result).isNotNull();
        assertThat(new JSONObject(result).length()).isGreaterThan(0);
    }
}