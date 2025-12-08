package io.github.pierce.FlattenConsolidatorTests;

import io.github.pierce.JsonFlattenerConsolidator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive Performance Test Suite for JsonFlattenerConsolidator
 * Refactored to use Jackson instead of org.json for Apache 2.0 license compliance.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JsonFlattenerConsolidatorPerformanceTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int WARM_UP_ITERATIONS = 10;
    private static final int TEST_ITERATIONS = 100;

//    @Test
//    @Order(1)
//    @DisplayName("Performance comparison: Statistics ON vs OFF")
//    void testStatisticsOverhead() {
//        ObjectNode json = createModeratelyComplexJson(50, 20);
//        String jsonString = json.toString();
//
//        JsonFlattenerConsolidator withStats = new JsonFlattenerConsolidator(",", null, 50, 1000, false, true);
//        JsonFlattenerConsolidator withoutStats = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false);
//
//        // Warm up
//        for (int i = 0; i < WARM_UP_ITERATIONS; i++) {
//            withStats.flattenAndConsolidateJson(jsonString);
//            withoutStats.flattenAndConsolidateJson(jsonString);
//        }
//
//        // Measure with stats
//        long startWithStats = System.nanoTime();
//        for (int i = 0; i < TEST_ITERATIONS; i++) {
//            withStats.flattenAndConsolidateJson(jsonString);
//        }
//        long timeWithStats = System.nanoTime() - startWithStats;
//
//        // Measure without stats
//        long startWithoutStats = System.nanoTime();
//        for (int i = 0; i < TEST_ITERATIONS; i++) {
//            withoutStats.flattenAndConsolidateJson(jsonString);
//        }
//        long timeWithoutStats = System.nanoTime() - startWithoutStats;
//
//        double avgWithStats = timeWithStats / (double) TEST_ITERATIONS / 1_000_000;
//        double avgWithoutStats = timeWithoutStats / (double) TEST_ITERATIONS / 1_000_000;
//        double overhead = ((avgWithStats - avgWithoutStats) / avgWithoutStats) * 100;
//
//        System.out.println("=== Statistics Overhead Test ===");
//        System.out.printf("With stats: %.3f ms/operation%n", avgWithStats);
//        System.out.printf("Without stats: %.3f ms/operation%n", avgWithoutStats);
//        System.out.printf("Overhead: %.1f%%%n", overhead);
//
//        // Statistics should add minimal overhead (less than 50%)
//        assertThat(overhead).isLessThan(50.0);
//    }

//    @Test
//    @Order(2)
//    @DisplayName("Performance: Varying array sizes")
//    void testArraySizePerformance() {
//        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(",", null, 50, 10000, false);
//
//        int[] arraySizes = {10, 50, 100, 500, 1000};
//        List<Double> times = new ArrayList<>();
//
//        System.out.println("=== Array Size Performance ===");
//
//        for (int size : arraySizes) {
//            ObjectNode json = MAPPER.createObjectNode();
//            ArrayNode array = MAPPER.createArrayNode();
//            for (int i = 0; i < size; i++) {
//                array.add("item_" + i);
//            }
//            json.set("items", array);
//            String jsonString = json.toString();
//
//            // Warm up
//            for (int i = 0; i < WARM_UP_ITERATIONS; i++) {
//                flattener.flattenAndConsolidateJson(jsonString);
//            }
//
//            // Measure
//            long start = System.nanoTime();
//            for (int i = 0; i < TEST_ITERATIONS; i++) {
//                flattener.flattenAndConsolidateJson(jsonString);
//            }
//            long elapsed = System.nanoTime() - start;
//            double avgMs = elapsed / (double) TEST_ITERATIONS / 1_000_000;
//            times.add(avgMs);
//
//            System.out.printf("Array size %4d: %.3f ms/operation%n", size, avgMs);
//        }
//
//        // Performance should scale reasonably (not exponentially)
//        // Last should be at most 20x the first
//        assertThat(times.get(times.size() - 1) / times.get(0)).isLessThan(20.0);
//    }

    @Test
    @Order(3)
    @DisplayName("Performance: Deep nesting")
    void testDeepNestingPerformance() {
        int[] depths = {5, 10, 20, 30, 50};
        List<Double> times = new ArrayList<>();

        System.out.println("=== Deep Nesting Performance ===");

        for (int depth : depths) {
            JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(",", null, depth + 10, 100, false);
            String jsonString = createDeeplyNestedJson(depth);

            // Warm up
            for (int i = 0; i < WARM_UP_ITERATIONS; i++) {
                flattener.flattenAndConsolidateJson(jsonString);
            }

            // Measure
            long start = System.nanoTime();
            for (int i = 0; i < TEST_ITERATIONS; i++) {
                flattener.flattenAndConsolidateJson(jsonString);
            }
            long elapsed = System.nanoTime() - start;
            double avgMs = elapsed / (double) TEST_ITERATIONS / 1_000_000;
            times.add(avgMs);

            System.out.printf("Depth %2d: %.3f ms/operation%n", depth, avgMs);
        }

        // Deep nesting should still be reasonably fast
        assertThat(times.get(times.size() - 1)).isLessThan(10.0);
    }

    @Test
    @Order(4)
    @DisplayName("Performance: Complex nested structures")
    void testComplexStructurePerformance() throws Exception {
        System.out.println("=== Complex Structure Performance ===");

        // Create a realistic complex document
        String complexJson = """
            {
                "order": {
                    "id": "ORD-12345",
                    "customer": {
                        "name": "John Doe",
                        "email": "john@example.com",
                        "addresses": [
                            {"type": "billing", "city": "NYC", "zip": "10001"},
                            {"type": "shipping", "city": "LA", "zip": "90001"}
                        ]
                    },
                    "items": [
                        {"sku": "A001", "qty": 2, "price": 10.00},
                        {"sku": "B002", "qty": 1, "price": 25.50},
                        {"sku": "C003", "qty": 5, "price": 5.00}
                    ],
                    "metadata": {
                        "source": "API",
                        "tags": ["priority", "express", "international"]
                    }
                }
            }
            """;

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false);

        // Warm up
        for (int i = 0; i < WARM_UP_ITERATIONS; i++) {
            flattener.flattenAndConsolidateJson(complexJson);
        }

        // Measure
        long start = System.nanoTime();
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            String result = flattener.flattenAndConsolidateJson(complexJson);
            // Verify result is valid
            assertThat(result).isNotEmpty();
        }
        long elapsed = System.nanoTime() - start;
        double avgMs = elapsed / (double) TEST_ITERATIONS / 1_000_000;

        System.out.printf("Complex structure: %.3f ms/operation%n", avgMs);

        // Should process quickly
        assertThat(avgMs).isLessThan(5.0);
    }

    @Test
    @Order(5)
    @DisplayName("Performance: Throughput test")
    void testThroughput() {
        ObjectNode json = createModeratelyComplexJson(100, 50);
        String jsonString = json.toString();

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false);

        // Warm up
        for (int i = 0; i < WARM_UP_ITERATIONS; i++) {
            flattener.flattenAndConsolidateJson(jsonString);
        }

        // Measure throughput over 1 second
        int count = 0;
        long start = System.currentTimeMillis();
        long end = start + 1000; // 1 second

        while (System.currentTimeMillis() < end) {
            flattener.flattenAndConsolidateJson(jsonString);
            count++;
        }

        System.out.println("=== Throughput Test ===");
        System.out.printf("Throughput: %d operations/second%n", count);

        // Should achieve at least 100 operations per second
        assertThat(count).isGreaterThan(100);
    }

    @Test
    @Order(6)
    @DisplayName("Performance: Memory efficiency")
    void testMemoryEfficiency() throws Exception {
        ObjectNode largeJson = createModeratelyComplexJson(500, 100);
        String jsonString = largeJson.toString();
        int jsonSize = jsonString.length();

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false);

        // Force GC
        System.gc();
        Thread.sleep(100);

        long memBefore = getUsedMemory();

        // Process multiple times
        for (int i = 0; i < 100; i++) {
            flattener.flattenAndConsolidateJson(jsonString);
        }

        // Force GC
        System.gc();
        Thread.sleep(100);

        long memAfter = getUsedMemory();
        long memDelta = memAfter - memBefore;

        System.out.println("=== Memory Efficiency Test ===");
        System.out.printf("JSON size: %d bytes%n", jsonSize);
        System.out.printf("Memory delta: %d KB%n", memDelta / 1024);

        // Memory should not grow excessively (less than 10MB for this test)
        assertThat(memDelta).isLessThan(10 * 1024 * 1024);
    }

    // Helper methods

    private ObjectNode createModeratelyComplexJson(int arraySize, int numFields) {
        ObjectNode json = MAPPER.createObjectNode();

        // Add simple fields
        for (int i = 0; i < numFields; i++) {
            json.put("field_" + i, "value_" + i);
        }

        // Add nested object
        ObjectNode nested = MAPPER.createObjectNode();
        nested.put("id", "NESTED-001");
        nested.put("data", "Some nested data");
        json.set("nested", nested);

        // Add array
        ArrayNode array = MAPPER.createArrayNode();
        for (int i = 0; i < arraySize; i++) {
            ObjectNode item = MAPPER.createObjectNode();
            item.put("id", i);
            item.put("value", "Item " + i);
            array.add(item);
        }
        json.set("items", array);

        return json;
    }

    private String createDeeplyNestedJson(int depth) {
        StringBuilder json = new StringBuilder();

        // Opening braces
        for (int i = 0; i < depth; i++) {
            json.append("{\"level_").append(i).append("\":");
        }

        // Value at deepest level
        json.append("\"deep_value\"");

        // Closing braces
        for (int i = 0; i < depth; i++) {
            json.append("}");
        }

        return json.toString();
    }

    private long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
