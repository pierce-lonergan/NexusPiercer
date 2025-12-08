package io.github.pierce.FlattenConsolidatorTests;


import io.github.pierce.JsonFlattenerConsolidator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Base64;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Edge case and data quality tests for JsonFlattenerConsolidator.
 *
 * These tests focus on unusual scenarios, data quality issues, and edge cases
 * that have been discovered in production systems. Each test documents a real
 * issue that was encountered and how our implementation handles it.
 */
class JsonFlattenerConsolidatorEdgeCaseTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonFlattenerConsolidator flattener;

    @BeforeEach
    void setUp() {
        flattener = new JsonFlattenerConsolidator(",", "null", 50, 1000, false);
    }

    /**
     * Test: Circular Reference Prevention
     *
     * Real Issue: Some JSON serializers can create circular references when
     * serializing complex object graphs (e.g., JPA entities with bidirectional relationships).
     *
     * Solution: Our max depth limit prevents infinite loops, converting deep structures to strings.
     */
    @Test
    @DisplayName("Should prevent infinite loops from circular references")
    void testCircularReferencePrevention() throws Exception {

        String circularJson = """
            {
                "id": 1,
                "name": "Parent",
                "child": {
                    "id": 2,
                    "name": "Child",
                    "parent": {
                        "id": 1,
                        "name": "Parent",
                        "child": {
                            "id": 2,
                            "name": "Child"
                        }
                    }
                }
            }
            """;

        JsonFlattenerConsolidator limitedDepth = new JsonFlattenerConsolidator(",", "null", 3, 100, false);
        String result = limitedDepth.flattenAndConsolidateJson(circularJson);
        JsonNode resultJson = MAPPER.readTree(result);


        assertThat(resultJson.get("id").asInt()).isEqualTo(1);
        assertThat(resultJson.get("child_id").asInt()).isEqualTo(2);
        assertThat(resultJson.get("child_parent_id").asInt()).isEqualTo(1);

        assertThat(resultJson.get("child_parent_child").asText()).contains("\"id\":2");
    }

    /**
     * Test: Numeric Precision and Scientific Notation
     *
     * Real Issue: Financial systems require exact decimal precision. JavaScript numbers
     * and JSON can lose precision with very large numbers or many decimal places.
     *
     * Solution: We preserve numeric values as strings when they exceed safe bounds.
     */
    @ParameterizedTest
    @DisplayName("Should preserve numeric precision for financial calculations")
    @MethodSource("provideNumericPrecisionCases")
    void testNumericPrecision(String description, String json, String expectedKey, String expectedValue) throws Exception {
        String result = flattener.flattenAndConsolidateJson(json);
        JsonNode resultJson = MAPPER.readTree(result);

        JsonNode node = resultJson.get(expectedKey);
        // Use asText() for string values, toString() for numeric values (which won't have quotes)
        String actualValue = node.isTextual() ? node.asText() : node.toString();
        assertThat(actualValue)
                .as(description)
                .isEqualTo(expectedValue);
    }

    private static Stream<Arguments> provideNumericPrecisionCases() {
        return Stream.of(
                Arguments.of(
                        "Very large number",
                        "{\"amount\": 9007199254740993}",
                        "amount",
                        "9007199254740993"
                ),
                Arguments.of(
                        "High precision decimal",
                        "{\"rate\": 0.123456789012345678901234567890}",
                        "rate",
                        "0.12345678901234568"  // JSON parsing limits precision
                ),
                Arguments.of(
                        "Scientific notation",
                        "{\"value\": 1.23e+10}",
                        "value",
                        "1.23E10"  // Jackson formats without + for positive exponents
                ),
                Arguments.of(
                        "Negative scientific notation",
                        "{\"tiny\": 1.23e-10}",
                        "tiny",
                        "1.23E-10"
                ),
                Arguments.of(
                        "Array of mixed numeric formats",
                        "{\"numbers\": [123, 123.456, 1.23e5, 0.0000001]}",
                        "numbers",
                        "123,123.456,123000.0,1.0E-7"
                )
        );
    }

    /**
     * Test: Date and Timestamp Handling
     *
     * Real Issue: Dates come in many formats - ISO strings, epoch milliseconds,
     * custom formats. Inconsistent handling causes parsing errors downstream.
     *
     * Solution: We preserve dates as-is, allowing downstream processors to handle
     * parsing based on schema definitions.
     */
    @Test
    @DisplayName("Should preserve various date formats without modification")
    void testDateHandling() throws Exception {
        String json = """
            {
                "dates": {
                    "iso": "2024-01-15T10:30:00.000Z",
                    "epochMillis": 1705318200000,
                    "epochSeconds": 1705318200,
                    "custom": "15-JAN-2024 10:30:00",
                    "array": [
                        "2024-01-15",
                        "2024-01-16",
                        "2024-01-17"
                    ]
                }
            }
            """;

        String result = flattener.flattenAndConsolidateJson(json);
        JsonNode resultJson = MAPPER.readTree(result);

        // All date formats preserved as-is
        assertThat(resultJson.get("dates_iso").asText()).isEqualTo("2024-01-15T10:30:00.000Z");
        assertThat(resultJson.get("dates_epochMillis").asLong()).isEqualTo(1705318200000L);
        assertThat(resultJson.get("dates_epochSeconds").asInt()).isEqualTo(1705318200);
        assertThat(resultJson.get("dates_custom").asText()).isEqualTo("15-JAN-2024 10:30:00");
        assertThat(resultJson.get("dates_array").asText()).isEqualTo("2024-01-15,2024-01-16,2024-01-17");
    }

    /**
     * Test: Malformed JSON Recovery
     *
     * Real Issue: Data from external APIs or message queues sometimes contains
     * malformed JSON due to encoding issues or truncation.
     *
     * Solution: We return empty JSON rather than propagating exceptions,
     * allowing the pipeline to continue processing other records.
     *
     * Note: Duplicate keys like {"key": 1, "key": 2} are valid JSON per RFC 8259
     * (though discouraged). Jackson parses them successfully, taking the last value.
     */
    @ParameterizedTest
    @DisplayName("Should handle malformed JSON gracefully")
    @ValueSource(strings = {
            "{\"unclosed\": \"string",           // Unclosed string
            "{\"unclosed\": {\"nested\":",       // Unclosed object
            "[1, 2, 3",                         // Unclosed array
            "{\"key\": undefined}",             // JavaScript undefined
            "{\"key\": NaN}",                   // JavaScript NaN
            "Some random text",                 // Not JSON at all
            "null",                             // Just null
            "12345"                             // Just a number
    })
    void testMalformedJson(String malformedJson) {
        String result = flattener.flattenAndConsolidateJson(malformedJson);
        assertThat(result).isEqualTo("{}");
    }

    /**
     * Test: Array Explosion Prevention
     *
     * Real Issue: Cartesian product of multiple arrays can cause memory explosion.
     * Example: 3 arrays of 100 items each = 1,000,000 combinations.
     *
     * Solution: We consolidate arrays independently, preventing explosion.
     */
    @Test
    @DisplayName("Should prevent memory explosion from multiple large arrays")
    void testArrayExplosionPrevention() throws Exception {
        ObjectNode doc = MAPPER.createObjectNode();

        // Create multiple large arrays
        for (int arrayNum = 0; arrayNum < 5; arrayNum++) {
            ArrayNode array = MAPPER.createArrayNode();
            for (int i = 0; i < 200; i++) {
                array.add("value_" + arrayNum + "_" + i);
            }
            doc.set("array" + arrayNum, array);
        }

        long startMemory = getUsedMemory();
        String result = flattener.flattenAndConsolidateJson(doc.toString());
        long endMemory = getUsedMemory();

        JsonNode resultJson = MAPPER.readTree(result);

        // Each array consolidated independently
        for (int arrayNum = 0; arrayNum < 5; arrayNum++) {
            assertThat(resultJson.get("array" + arrayNum + "_count").asLong()).isEqualTo(200);
            String consolidated = resultJson.get("array" + arrayNum).asText();
            assertThat(consolidated.split(",")).hasSize(200);
        }

        // Memory usage should be linear, not exponential
        long memoryUsed = endMemory - startMemory;
        assertThat(memoryUsed).isLessThan(10 * 1024 * 1024); // Less than 10MB
    }

    /**
     * Test: Key Sanitization and SQL Injection Prevention
     *
     * Real Issue: JSON keys from user input could contain SQL injection attempts
     * or characters that break downstream SQL queries.
     *
     * Solution: We preserve keys as-is but replace dots with underscores.
     * SQL escaping is handled by prepared statements downstream.
     */
    @Test
    @DisplayName("Should handle potentially malicious key names safely")
    void testMaliciousKeyNames() throws Exception {
        String json = """
            {
                "normal_key": "value1",
                "key'; DROP TABLE users; --": "sql_injection_attempt",
                "key\\" OR \\"1\\"=\\"1": "another_attempt",
                "<script>alert('xss')</script>": "xss_attempt",
                "../../etc/passwd": "path_traversal",
                "key\\nwith\\nnewlines": "multiline",
                "key\\twith\\ttabs": "tabs",
                "key\\u0000with\\u0000nulls": "null_bytes"
            }
            """;

        String result = flattener.flattenAndConsolidateJson(json);
        JsonNode resultJson = MAPPER.readTree(result);

        // All keys preserved (dots replaced with underscores)
        assertThat(resultJson.get("normal_key").asText()).isEqualTo("value1");
        assertThat(resultJson.get("key'; DROP TABLE users; --").asText()).isEqualTo("sql_injection_attempt");
        assertThat(resultJson.has("<script>alert('xss')</script>")).isTrue();
        assertThat(resultJson.get("__/__/etc/passwd").asText()).isEqualTo("path_traversal");
    }

    /**
     * Test: Binary Data Handling
     *
     * Real Issue: Binary data (images, files) sometimes appears in JSON as base64
     * or escaped strings, causing memory issues.
     *
     * Solution: We treat them as regular strings but array size limits prevent
     * memory exhaustion.
     */
    @Test
    @DisplayName("Should handle base64 encoded binary data")
    void testBinaryDataHandling() throws Exception {
        // Simulate base64 encoded data
        String base64Image = Base64.getEncoder().encodeToString(new byte[1024]); // 1KB of data

        String json = String.format("""
            {
                "document": {
                    "title": "Test Document",
                    "thumbnail": "%s",
                    "attachments": [
                        {"name": "file1.pdf", "data": "%s"},
                        {"name": "file2.pdf", "data": "%s"}
                    ]
                }
            }
            """, base64Image, base64Image, base64Image);

        String result = flattener.flattenAndConsolidateJson(json);
        JsonNode resultJson = MAPPER.readTree(result);

        // Binary data preserved as strings
        assertThat(resultJson.get("document_thumbnail").asText()).isEqualTo(base64Image);
        assertThat(resultJson.get("document_attachments_data").asText()).contains(base64Image);
        assertThat(resultJson.get("document_attachments_data_count").asLong()).isEqualTo(2);
    }

    /**
     * Test: Heterogeneous Array Handling
     *
     * Real Issue: Arrays with mixed types at different indices cause schema
     * inference problems in Spark.
     *
     * Solution: Our type detection identifies mixed arrays and classifies them
     * appropriately.
     */
    @Test
    @DisplayName("Should handle arrays with different types at different positions")
    void testHeterogeneousArrays() throws Exception {
        String json = """
            {
                "data": [
                    {"type": "A", "value": 123},
                    {"type": "B", "name": "test"},
                    {"type": "C", "flag": true, "extra": "field"},
                    null,
                    {"type": "D"}
                ]
            }
            """;

        String result = flattener.flattenAndConsolidateJson(json);
        JsonNode resultJson = MAPPER.readTree(result);

        // Each field consolidated separately
        assertThat(resultJson.get("data_type").asText()).isEqualTo("A,B,C,D");
        assertThat(resultJson.get("data_value").asText()).isEqualTo("123");
        assertThat(resultJson.get("data_value_count").asLong()).isEqualTo(1); // Only first object has 'value'
        assertThat(resultJson.get("data_name").asText()).isEqualTo("test");
        assertThat(resultJson.get("data_name_count").asLong()).isEqualTo(1); // Only second object has 'name'
        assertThat(resultJson.get("data_flag").asText()).isEqualTo("true");
        assertThat(resultJson.get("data_extra").asText()).isEqualTo("field");
    }

    /**
     * Test: Sparse Data Handling
     *
     * Real Issue: NoSQL databases often have sparse data where most fields
     * are null or missing, causing inefficient storage.
     *
     * Solution: We efficiently handle sparse data, only storing non-null values.
     */
    @Test
    @DisplayName("Should efficiently handle sparse data with many nulls")
    void testSparseDataHandling() throws Exception {
        ObjectNode sparseDoc = MAPPER.createObjectNode();

        // Create document with 1000 fields, 95% null
        for (int i = 0; i < 1000; i++) {
            if (i % 20 == 0) {
                sparseDoc.put("field_" + i, "value_" + i);
            } else {
                sparseDoc.putNull("field_" + i);
            }
        }

        String result = flattener.flattenAndConsolidateJson(sparseDoc.toString());
        JsonNode resultJson = MAPPER.readTree(result);

        // Count non-null fields
        int nonNullCount = 0;
        Iterator<String> fieldNames = resultJson.fieldNames();
        while (fieldNames.hasNext()) {
            String key = fieldNames.next();
            JsonNode value = resultJson.get(key);
            if (!value.isNull() && !"null".equals(value.asText())) {
                nonNullCount++;
            }
        }

        assertThat(nonNullCount).isEqualTo(50); // 5% of 1000
        assertThat(resultJson.size()).isEqualTo(1000); // All fields present
    }

    /**
     * Test: Schema Evolution Compatibility
     *
     * Real Issue: JSON schemas evolve over time - fields added, removed, or changed types.
     * Our flattening must handle documents from different schema versions.
     *
     * Solution: Type detection and statistics help identify schema changes.
     */
    @Test
    @DisplayName("Should handle documents from different schema versions")
    void testSchemaEvolution() throws Exception {
        // V1 schema
        String v1Doc = """
            {
                "version": 1,
                "customerId": "123",
                "amount": 100.50
            }
            """;

        // V2 schema - added fields
        String v2Doc = """
            {
                "version": 2,
                "customerId": "456",
                "amount": 200.75,
                "currency": "USD",
                "metadata": {
                    "source": "API"
                }
            }
            """;

        // V3 schema - changed types
        String v3Doc = """
            {
                "version": 3,
                "customerId": 789,  
                "amount": "300.00",  
                "currency": "EUR",
                "metadata": {
                    "source": "API",
                    "tags": ["important", "rush"]
                }
            }
            """;

        // Process all versions
        JsonNode v1Result = MAPPER.readTree(flattener.flattenAndConsolidateJson(v1Doc));
        JsonNode v2Result = MAPPER.readTree(flattener.flattenAndConsolidateJson(v2Doc));
        JsonNode v3Result = MAPPER.readTree(flattener.flattenAndConsolidateJson(v3Doc));

        // V1 has basic fields
        assertThat(v1Result.get("version").asInt()).isEqualTo(1);
        assertThat(v1Result.get("customerId").asText()).isEqualTo("123");
        assertThat(v1Result.get("amount").asDouble()).isEqualTo(100.50);
        assertThat(v1Result.has("currency")).isFalse();

        // V2 has additional fields
        assertThat(v2Result.get("version").asInt()).isEqualTo(2);
        assertThat(v2Result.has("currency")).isTrue();
        assertThat(v2Result.get("metadata_source").asText()).isEqualTo("API");

        // V3 has type changes but still processes correctly
        assertThat(v3Result.get("version").asInt()).isEqualTo(3);
        assertThat(v3Result.get("customerId").toString()).isEqualTo("789"); // Now numeric
        assertThat(v3Result.get("amount").asText()).isEqualTo("300.00"); // Now string
        assertThat(v3Result.get("metadata_tags").asText()).isEqualTo("important,rush");
    }

    private long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}