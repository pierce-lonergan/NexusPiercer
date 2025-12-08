package io.github.pierce.FlattenConsolidatorTests;

import io.github.pierce.JsonFlattenerConsolidator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Comprehensive test suite for JsonFlattenerConsolidator.
 * Refactored to use Jackson instead of org.json for Apache 2.0 license compliance.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JsonFlattenerConsolidatorComprehensiveTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonFlattenerConsolidator standardFlattener;
    private static JsonFlattenerConsolidator performanceFlattener;
    private static JsonFlattenerConsolidator securityFlattener;

    @BeforeAll
    static void setUp() {
        standardFlattener = new JsonFlattenerConsolidator(",", "null", 50, 1000, false);
        performanceFlattener = new JsonFlattenerConsolidator(",", null, 20, 100, false);
        securityFlattener = new JsonFlattenerConsolidator(",", "NULL", 10, 50, false);
    }

    // ==================== CORE FUNCTIONALITY TESTS ====================

    @Test
    @DisplayName("Should flatten simple nested JSON")
    void testSimpleNesting() throws Exception {
        String json = """
            {
                "user": {
                    "name": "John",
                    "address": {
                        "city": "NYC",
                        "zip": "10001"
                    }
                }
            }
            """;

        String result = standardFlattener.flattenAndConsolidateJson(json);
        JsonNode resultJson = MAPPER.readTree(result);

        assertThat(resultJson.get("user_name").asText()).isEqualTo("John");
        assertThat(resultJson.get("user_address_city").asText()).isEqualTo("NYC");
        assertThat(resultJson.get("user_address_zip").asText()).isEqualTo("10001");
    }

    @Test
    @DisplayName("Should consolidate arrays with statistics")
    void testArrayConsolidation() throws Exception {
        String json = """
            {
                "order": {
                    "items": ["apple", "banana", "cherry", "apple"],
                    "quantities": [10, 20, 30]
                }
            }
            """;

        String result = standardFlattener.flattenAndConsolidateJson(json);
        JsonNode resultJson = MAPPER.readTree(result);

        assertThat(resultJson.get("order_items").asText()).isEqualTo("apple,banana,cherry,apple");
        assertThat(resultJson.get("order_items_count").asInt()).isEqualTo(4);
        assertThat(resultJson.get("order_items_distinct_count").asInt()).isEqualTo(3);
        assertThat(resultJson.get("order_quantities").asText()).isEqualTo("10,20,30");
    }

    @Test
    @DisplayName("Should handle nested arrays of objects")
    void testNestedArrayOfObjects() throws Exception {
        String json = """
            {
                "company": {
                    "departments": [
                        {"name": "Engineering", "budget": 100000},
                        {"name": "Sales", "budget": 50000}
                    ]
                }
            }
            """;

        String result = standardFlattener.flattenAndConsolidateJson(json);
        JsonNode resultJson = MAPPER.readTree(result);

        assertThat(resultJson.get("company_departments_name").asText()).isEqualTo("Engineering,Sales");
        assertThat(resultJson.get("company_departments_budget").asText()).isEqualTo("100000,50000");
        assertThat(resultJson.get("company_departments_name_count").asInt()).isEqualTo(2);
    }

    // ==================== EDGE CASE TESTS ====================

    @Test
    @DisplayName("Should handle extreme nesting depth without stack overflow")
    void testExtremeNestingDepth() throws Exception {
        // Create a deeply nested structure programmatically
        ObjectNode deepJson = MAPPER.createObjectNode();
        ObjectNode current = deepJson;

        // Create 100 levels of nesting (way beyond our limit)
        for (int i = 0; i < 100; i++) {
            ObjectNode next = MAPPER.createObjectNode();
            current.set("level" + i, next);
            current = next;
        }
        current.put("deepValue", "finally here!");

        // Should not throw StackOverflowError
        assertDoesNotThrow(() -> {
            String result = securityFlattener.flattenAndConsolidateJson(deepJson.toString());
            JsonNode resultJson = MAPPER.readTree(result);

            // Verify depth limiting worked
            boolean foundDepthLimit = false;
            Iterator<String> fieldNames = resultJson.fieldNames();
            while (fieldNames.hasNext()) {
                String key = fieldNames.next();
                if (key.contains("level9")) { // Should stop at depth 10
                    foundDepthLimit = true;
                    // The deeply nested part should be stringified
                    String value = resultJson.get(key).asText();
                    assertThat(value).contains("level10");
                }
            }
            assertThat(foundDepthLimit).isTrue();
        });
    }

    @Test
    @DisplayName("Should efficiently handle large arrays with proper truncation and statistics")
    void testLargeArrayHandling() throws Exception {
        // Create JSON with various large arrays
        ObjectNode json = MAPPER.createObjectNode();

        // Numeric array with 10,000 elements
        ArrayNode hugeNumbers = MAPPER.createArrayNode();
        for (int i = 0; i < 10000; i++) {
            hugeNumbers.add(i);
        }
        json.set("hugeNumbers", hugeNumbers);

        // String array with long values
        ArrayNode longStrings = MAPPER.createArrayNode();
        String longString = "x".repeat(1000); // 1KB per string
        for (int i = 0; i < 100; i++) {
            longStrings.add(longString + i);
        }
        json.set("longStrings", longStrings);

        // Nested array of objects
        ArrayNode complexArray = MAPPER.createArrayNode();
        for (int i = 0; i < 500; i++) {
            ObjectNode item = MAPPER.createObjectNode();
            item.put("id", i);
            item.put("value", "item" + i);
            ObjectNode nested = MAPPER.createObjectNode();
            nested.put("deep", "value" + i);
            item.set("nested", nested);
            complexArray.add(item);
        }
        json.set("complexArray", complexArray);

        String result = performanceFlattener.flattenAndConsolidateJson(json.toString());
        JsonNode resultJson = MAPPER.readTree(result);

        // Verify array truncation (limited to 100 elements)
        assertThat(resultJson.get("hugeNumbers_count").asLong()).isEqualTo(100); // Truncated
        assertThat(resultJson.get("longStrings_count").asLong()).isEqualTo(100);
        assertThat(resultJson.get("complexArray_id_count").asLong()).isEqualTo(100); // Truncated

        // Verify memory-efficient consolidation
        String numbers = resultJson.get("hugeNumbers").asText();
        assertThat(numbers.split(",")).hasSize(100);
    }

    @Test
    @DisplayName("Should handle special characters in keys and values")
    void testSpecialCharacters() throws Exception {
        String json = """
            {
                "field.with.dots": "value1",
                "field_with_underscores": "value2",
                "field-with-dashes": "value3",
                "field with spaces": "value4",
                "unicode_\\u0041": "value5",
                "quotes\\"inside": "value6",
                "nested": {
                    "special.key": "nested_value"
                }
            }
            """;

        String result = standardFlattener.flattenAndConsolidateJson(json);
        JsonNode resultJson = MAPPER.readTree(result);

        // Dots should be replaced with underscores
        assertThat(resultJson.has("field_with_dots")).isTrue();
        assertThat(resultJson.get("nested_special_key").asText()).isEqualTo("nested_value");
    }

    // ==================== CONCURRENT TESTS ====================

    @Test
    @DisplayName("Should be thread-safe during concurrent processing")
    void testConcurrentProcessing() throws Exception {
        int threadCount = 10;
        int iterationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threadCount);

        String json = """
            {
                "data": {
                    "items": [1, 2, 3, 4, 5],
                    "nested": {"key": "value"}
                }
            }
            """;

        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < iterationsPerThread; i++) {
                        String result = standardFlattener.flattenAndConsolidateJson(json);
                        JsonNode resultJson = MAPPER.readTree(result);

                        if (!"1,2,3,4,5".equals(resultJson.get("data_items").asText())) {
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(errorCount.get()).isZero();
    }

    // ==================== PERFORMANCE TESTS ====================

    @Test
    @DisplayName("Should process JSON efficiently")
    void testProcessingPerformance() throws Exception {
        // Create a moderately complex document
        ObjectNode doc = generateComplexDocument(10, 20, 5);
        String json = doc.toString();

        // Warm up
        for (int i = 0; i < 100; i++) {
            standardFlattener.flattenAndConsolidateJson(json);
        }

        // Measure
        long start = System.currentTimeMillis();
        int iterations = 1000;
        for (int i = 0; i < iterations; i++) {
            standardFlattener.flattenAndConsolidateJson(json);
        }
        long elapsed = System.currentTimeMillis() - start;

        double avgTime = (double) elapsed / iterations;
        System.out.println("Average processing time: " + avgTime + "ms per document");

        // Should process at least 100 documents per second
        assertThat(avgTime).isLessThan(10.0);
    }

    // ==================== NULL HANDLING TESTS ====================

    @Test
    @DisplayName("Should handle null values correctly")
    void testNullHandling() throws Exception {
        String json = """
            {
                "nullField": null,
                "arrayWithNulls": [1, null, 3],
                "nested": {
                    "value": null
                }
            }
            """;

        String result = standardFlattener.flattenAndConsolidateJson(json);
        JsonNode resultJson = MAPPER.readTree(result);

        assertThat(resultJson.get("nullField").asText()).isEqualTo("null");
        assertThat(resultJson.get("arrayWithNulls").asText()).isEqualTo("1,null,3");
        assertThat(resultJson.get("nested_value").asText()).isEqualTo("null");
    }

    // ==================== HELPER METHODS ====================

    private ObjectNode generateComplexDocument(int maxDepth, int maxArraySize, int complexity) {
        ObjectNode doc = MAPPER.createObjectNode();
        doc.put("docId", UUID.randomUUID().toString());
        doc.put("timestamp", System.currentTimeMillis());

        ObjectNode threadInfo = MAPPER.createObjectNode();
        threadInfo.put("id", Thread.currentThread().getId());
        doc.set("threadInfo", threadInfo);

        // Add nested structures
        ObjectNode current = doc;
        for (int depth = 0; depth < Math.min(maxDepth, 10); depth++) {
            ObjectNode nested = MAPPER.createObjectNode();
            nested.put("level", depth);
            nested.put("data", "value_" + depth);

            // Add arrays at various levels
            if (depth % 2 == 0) {
                ArrayNode arr = MAPPER.createArrayNode();
                for (int i = 0; i < Math.min(maxArraySize, 20); i++) {
                    if (depth % 3 == 0) {
                        arr.add("item_" + i);
                    } else {
                        ObjectNode item = MAPPER.createObjectNode();
                        item.put("index", i);
                        item.put("value", "complex_" + i);
                        arr.add(item);
                    }
                }
                nested.set("array_" + depth, arr);
            }

            current.set("nested_" + depth, nested);
            current = nested;
        }

        // Add variety based on complexity
        Random rand = new Random();
        for (int i = 0; i < complexity; i++) {
            switch (rand.nextInt(5)) {
                case 0 -> doc.put("field_" + i, rand.nextInt(1000));
                case 1 -> doc.put("field_" + i, rand.nextDouble() * 1000);
                case 2 -> doc.put("field_" + i, rand.nextBoolean());
                case 3 -> doc.put("field_" + i, "string_" + rand.nextInt(100));
                default -> doc.putNull("field_" + i);
            }
        }

        return doc;
    }

    private long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
