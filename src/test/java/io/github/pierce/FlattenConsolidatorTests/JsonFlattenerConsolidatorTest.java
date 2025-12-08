package io.github.pierce.FlattenConsolidatorTests;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pierce.JsonFlattenerConsolidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class JsonFlattenerConsolidatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private JsonFlattenerConsolidator flattenerConsolidator;

    @BeforeEach
    void setUp() {
        flattenerConsolidator = new JsonFlattenerConsolidator(",", "null", 50, 1000, false);
    }

    @Test
    void testFlattenSimpleJson() throws JsonProcessingException {
        String input = "{\"name\": \"John\", \"age\": 30}";
        String result = flattenerConsolidator.flattenAndConsolidateJson(input);

        JsonNode resultJson = MAPPER.readTree(result);
        assertThat(resultJson.get("name").asText()).isEqualTo("John");
        assertThat(resultJson.get("age").asInt()).isEqualTo(30);
    }

    @Test
    void testFlattenNestedJson() throws JsonProcessingException {
        String input = "{\"person\": {\"name\": \"John\", \"address\": {\"city\": \"NYC\"}}}";
        String result = flattenerConsolidator.flattenAndConsolidateJson(input);

        JsonNode resultJson = MAPPER.readTree(result);
        assertThat(resultJson.get("person_name").asText()).isEqualTo("John");
        assertThat(resultJson.get("person_address_city").asText()).isEqualTo("NYC");
    }

    @Test
    void testFlattenArrayOfPrimitives() throws JsonProcessingException {
        String input = "{\"numbers\": [1, 2, 3, 4, 5]}";
        String result = flattenerConsolidator.flattenAndConsolidateJson(input);

        JsonNode resultJson = MAPPER.readTree(result);
        assertThat(resultJson.get("numbers").asText()).isEqualTo("1,2,3,4,5");
        assertThat(resultJson.get("numbers_count").asInt()).isEqualTo(5);
        assertThat(resultJson.get("numbers_distinct_count").asInt()).isEqualTo(5);
    }

    @Test
    void testFlattenArrayOfObjects() throws JsonProcessingException {
        String input = "{\"users\": [{\"name\": \"John\", \"age\": 30}, {\"name\": \"Jane\", \"age\": 25}]}";
        String result = flattenerConsolidator.flattenAndConsolidateJson(input);

        JsonNode resultJson = MAPPER.readTree(result);
        assertThat(resultJson.get("users_name").asText()).isEqualTo("John,Jane");
        assertThat(resultJson.get("users_age").asText()).isEqualTo("30,25");
        assertThat(resultJson.get("users_name_count").asInt()).isEqualTo(2);
        assertThat(resultJson.get("users_age_count").asInt()).isEqualTo(2);
    }

    @Test
    void testHandleNullValues() throws JsonProcessingException {
        JsonFlattenerConsolidator consolidator = new JsonFlattenerConsolidator(",", "NULL", 50, 1000, false);
        String input = "{\"value\": null, \"array\": [1, null, 3]}";
        String result = consolidator.flattenAndConsolidateJson(input);

        JsonNode resultJson = MAPPER.readTree(result);
        assertThat(resultJson.get("value").asText()).isEqualTo("NULL");
        assertThat(resultJson.get("array").asText()).isEqualTo("1,NULL,3");
    }


    @Test
    void testArraySizeLimit() throws JsonProcessingException {
        JsonFlattenerConsolidator consolidator = new JsonFlattenerConsolidator(",", null, 50, 3, false);
        String input = "{\"numbers\": [1, 2, 3, 4, 5]}";
        String result = consolidator.flattenAndConsolidateJson(input);

        JsonNode resultJson = MAPPER.readTree(result);

        assertThat(resultJson.get("numbers").asText()).isEqualTo("1,2,3");
    }

    @Test
    void testConsolidateWithMatrixDenotors() throws JsonProcessingException {
        JsonFlattenerConsolidator consolidator = new JsonFlattenerConsolidator(",", null, 50, 1000, true);
        String input = "{\"matrix\": [[1, 2], [3, 4]]}";
        String result = consolidator.flattenAndConsolidateJson(input);

        JsonNode resultJson = MAPPER.readTree(result);

        assertThat(resultJson.get("matrix").asText()).contains("[");
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
    void testStatisticsGeneration() throws JsonProcessingException {
        String input = "{\"words\": [\"hello\", \"world\", \"hello\", \"test\"]}";
        String result = flattenerConsolidator.flattenAndConsolidateJson(input);

        JsonNode resultJson = MAPPER.readTree(result);
        assertThat(resultJson.get("words_count").asInt()).isEqualTo(4);
        assertThat(resultJson.get("words_distinct_count").asInt()).isEqualTo(3);
        assertThat(resultJson.get("words_min_length").asInt()).isEqualTo(4);
        assertThat(resultJson.get("words_max_length").asInt()).isEqualTo(5);
        assertThat(resultJson.get("words_avg_length").asDouble()).isCloseTo(4.75, within(0.01));
        assertThat(resultJson.get("words_type").asText()).isEqualTo("string_list_consolidated");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"mixed\": [1, \"two\", true]}",
            "{\"nested\": [{\"a\": 1}, {\"b\": 2}]}"
    })
    void testMixedTypes(String input) throws JsonProcessingException {
        String result = flattenerConsolidator.flattenAndConsolidateJson(input);
        assertThat(result).isNotNull();
        JsonNode resultJson = MAPPER.readTree(result);
        assertThat(resultJson.size()).isGreaterThan(0);
    }
}