//package io.github.pierce.FlattenConsolidatorTests;
//
//
//import io.github.pierce.JsonFlattenerConsolidator;
//import org.json.JSONArray;
//import org.json.JSONObject;
//import org.junit.jupiter.api.*;
//import org.junit.jupiter.params.ParameterizedTest;
//import org.junit.jupiter.params.provider.Arguments;
//import org.junit.jupiter.params.provider.MethodSource;
//
//import java.util.*;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Stream;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
//
///**
// * Comprehensive test suite for JsonFlattenerConsolidator.
// *
// * This test suite validates the robustness, correctness, and performance of our JSON flattening
// * and consolidation implementation. The implementation is designed to handle:
// *
// * 1. Complex nested structures from various data sources (APIs, databases, message queues)
// * 2. Large arrays that need to be consolidated for efficient Spark processing
// * 3. Edge cases like circular references (handled by max depth), special characters, and huge payloads
// * 4. Performance requirements for processing millions of records in batch jobs
// *
// * Our implementation decisions:
// * - Iterative approach (not recursive) to avoid stack overflow on deeply nested data
// * - Array consolidation to reduce Spark schema complexity and improve query performance
// * - Statistics generation for data quality monitoring and validation
// * - Configurable limits to prevent memory exhaustion from malicious/corrupted data
// */
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//class JsonFlattenerConsolidatorComprehensiveTest {
//
//
//    private static JsonFlattenerConsolidator standardFlattener;
//    private static JsonFlattenerConsolidator performanceFlattener;
//    private static JsonFlattenerConsolidator securityFlattener;
//
//    @BeforeAll
//    static void setUp() {
//
//        standardFlattener = new JsonFlattenerConsolidator(",", "null", 50, 1000, false);
//
//
//        performanceFlattener = new JsonFlattenerConsolidator(",", null, 20, 100, false);
//
//
//        securityFlattener = new JsonFlattenerConsolidator(",", "NULL", 10, 50, false);
//    }
//
//
//    /*
//    @BeforeEach
//    void setUp() {
//
//        standardFlattener = new JsonFlattenerConsolidator(",", "null", 50, 1000, false);
//
//
//        performanceFlattener = new JsonFlattenerConsolidator(",", null, 20, 100, false);
//
//
//        securityFlattener = new JsonFlattenerConsolidator(",", "NULL", 10, 50, false);
//    }
//    */
//
//    /**
//     * Test 1: Unicode and Special Character Handling
//     *
//     * Purpose: Validates that our implementation correctly handles international characters,
//     * emojis, and special characters that are common in real-world data from global systems.
//     *
//     * Why: Financial data often contains international customer names, addresses, and
//     * transaction descriptions in multiple languages. Our UTF-8 handling ensures data integrity.
//     */
//    @Test
//    @DisplayName("Should handle Unicode characters, emojis, and special characters without data loss")
//    void testUnicodeAndSpecialCharacters() {
//        // Note: JSON escape sequences like \t and \n are processed during parsing
//        String json = """
//        {
//            "customer": {
//                "name": "José María García-López",
//                "nickname": "El Niño 👨‍💼",
//                "address": {
//                    "street": "Straße der Pariser Kommune",
//                    "city": "北京市",
//                    "notes": "Near café ☕ & restaurant 🍕"
//                }
//            },
//            "transactions": [
//                {"description": "Payment to Żółć Gęślą Jaźń", "amount": 123.45},
//                {"description": "Transfer from Банк", "amount": 678.90}
//            ],
//            "specialChars": ["<script>", "'; DROP TABLE;", "tab\\tchar", "newline\\nchar", "\\"quotes\\""]
//        }
//        """;
//
//        // First, verify the flattener is initialized
//        assertThat(standardFlattener).isNotNull();
//
//        String result = standardFlattener.flattenAndConsolidateJson(json);
//
//        // Debug: Print the result to diagnose issues
//        System.out.println("Flattened JSON result: " + result);
//
//        // Check if result is empty
//        if (result.equals("{}")) {
//            // Print the original JSON to check for issues
//            System.out.println("Original JSON: " + json);
//            Assertions.fail("Flattener returned empty JSON. The input JSON might be rejected by validation.");
//        }
//
//        JSONObject resultJson = new JSONObject(result);
//
//        // Debug: Print all keys to see what's available
//        System.out.println("Available keys: " + resultJson.keySet());
//
//        // Verify Unicode preservation
//        assertThat(resultJson.getString("customer_name")).isEqualTo("José María García-López");
//        assertThat(resultJson.getString("customer_nickname")).isEqualTo("El Niño 👨‍💼");
//        assertThat(resultJson.getString("customer_address_city")).isEqualTo("北京市");
//        assertThat(resultJson.getString("customer_address_notes")).contains("café ☕");
//
//        // Verify special characters are preserved
//        String specialCharsArray = resultJson.getString("specialChars");
//        assertThat(specialCharsArray).contains("<script>");
//        assertThat(specialCharsArray).contains("'; DROP TABLE;");
//        assertThat(specialCharsArray).contains("\"quotes\"");
//
//        // Check for actual tab and newline characters (not escaped representations)
//        assertThat(specialCharsArray).contains("tab\tchar");  // actual tab character
//        assertThat(specialCharsArray).contains("newline\nchar");  // actual newline character
//
//        // Alternative: Check that the characters are present using character codes
//        assertThat(specialCharsArray).contains("tab" + "\t" + "char");
//        assertThat(specialCharsArray).contains("newline" + "\n" + "char");
//
//        // Verify array consolidation with Unicode
//        assertThat(resultJson.getString("transactions_description"))
//                .isEqualTo("Payment to Żółć Gęślą Jaźń,Transfer from Банк");
//        assertThat(resultJson.getString("transactions_amount"))
//                .isEqualTo("123.45,678.9");
//
//        // Also check that statistics were generated (if enabled)
//        if (resultJson.has("specialChars_count")) {
//            assertThat(resultJson.getLong("specialChars_count")).isEqualTo(5L);
//            assertThat(resultJson.getString("specialChars_type")).isEqualTo("string_list_consolidated");
//        }
//    }
//
//    /**
//     * Alternative test that checks for literal backslash sequences
//     */
//    @Test
//    @DisplayName("Should handle literal backslash sequences")
//    void testLiteralBackslashSequences() {
//        // To get literal backslash-t and backslash-n, we need to escape the backslashes
//        String json = """
//        {
//            "literalEscapes": ["tab\\\\tchar", "newline\\\\nchar", "backslash\\\\char"]
//        }
//        """;
//
//        String result = standardFlattener.flattenAndConsolidateJson(json);
//        JSONObject resultJson = new JSONObject(result);
//
//        String literalEscapes = resultJson.getString("literalEscapes");
//
//        // Now these should contain literal backslashes
//        assertThat(literalEscapes).contains("tab\\tchar");  // literal backslash-t
//        assertThat(literalEscapes).contains("newline\\nchar");  // literal backslash-n
//        assertThat(literalEscapes).contains("backslash\\char");  // literal backslash
//    }
//
//    /**
//     * Test to demonstrate the difference between escape sequences and literal strings
//     */
//    @Test
//    @DisplayName("Should differentiate between escape sequences and literal strings")
//    void testEscapeSequenceVsLiteral() {
//        String json = """
//        {
//            "escaped": ["tab\\tchar", "newline\\nchar"],
//            "literal": ["tab\\\\tchar", "newline\\\\nchar"]
//        }
//        """;
//
//        String result = standardFlattener.flattenAndConsolidateJson(json);
//        JSONObject resultJson = new JSONObject(result);
//
//        String escaped = resultJson.getString("escaped");
//        String literal = resultJson.getString("literal");
//
//        // Escaped sequences are processed
//        assertThat(escaped).contains("tab\tchar");  // contains actual tab
//        assertThat(escaped).contains("newline\nchar");  // contains actual newline
//
//        // Literal sequences preserve the backslash
//        assertThat(literal).contains("tab\\tchar");  // contains backslash-t
//        assertThat(literal).contains("newline\\nchar");  // contains backslash-n
//
//        // Print to see the difference
//        System.out.println("Escaped: " + escaped);
//        System.out.println("Literal: " + literal);
//
//        // Show character codes for clarity
//        System.out.println("Escaped contains tab (char 9): " + escaped.contains("\t"));
//        System.out.println("Literal contains backslash (char 92): " + literal.contains("\\"));
//    }
//
//    /**
//     * Alternative test specifically for null character handling
//     */
//    @Test
//    @DisplayName("Should handle or reject null characters appropriately")
//    void testNullCharacterHandling() {
//        // Test with actual null character
//        String jsonWithNullChar = "{\"data\": \"before\\u0000after\"}";
//
//        String result = standardFlattener.flattenAndConsolidateJson(jsonWithNullChar);
//
//        // The flattener might reject this as malformed JSON or process it
//        // Either way, it shouldn't crash
//        assertThat(result).isNotNull();
//
//        if (!result.equals("{}")) {
//            JSONObject resultJson = new JSONObject(result);
//            // If processed, verify the data field exists
//            assertThat(resultJson.has("data")).isTrue();
//        }
//    }
//
//    /**
//     * Test 2: Extreme Nesting Depth
//     *
//     * Purpose: Tests our max depth protection against maliciously nested data that could
//     * cause stack overflow in naive recursive implementations.
//     *
//     * Why: Our iterative implementation with ArrayDeque avoids stack overflow and provides
//     * configurable depth limits to prevent processing of corrupted or malicious data.
//     */
//    @Test
//    @DisplayName("Should handle extreme nesting depth without stack overflow")
//    void testExtremeNestingDepth() {
//        // Create a deeply nested structure programmatically
//        JSONObject deepJson = new JSONObject();
//        JSONObject current = deepJson;
//
//        // Create 100 levels of nesting (way beyond our limit)
//        for (int i = 0; i < 100; i++) {
//            JSONObject next = new JSONObject();
//            current.put("level" + i, next);
//            current = next;
//        }
//        current.put("deepValue", "finally here!");
//
//        // Should not throw StackOverflowError
//        assertDoesNotThrow(() -> {
//            String result = securityFlattener.flattenAndConsolidateJson(deepJson.toString());
//            JSONObject resultJson = new JSONObject(result);
//
//            // Verify depth limiting worked
//            boolean foundDepthLimit = false;
//            for (String key : resultJson.keySet()) {
//                if (key.contains("level9")) { // Should stop at depth 10
//                    foundDepthLimit = true;
//                    // The deeply nested part should be stringified
//                    String value = resultJson.getString(key);
//                    assertThat(value).contains("level10");
//                }
//            }
//            assertThat(foundDepthLimit).isTrue();
//        });
//    }
//
//    /**
//     * Test 3: Large Array Handling and Memory Efficiency
//     *
//     * Purpose: Validates that our array size limits prevent memory exhaustion while
//     * preserving data integrity through proper truncation.
//     *
//     * Why: Real-world data can contain unexpectedly large arrays (e.g., transaction histories,
//     * log entries). Our consolidation approach with configurable limits ensures predictable
//     * memory usage and Spark schema sizing.
//     */
//    @Test
//    @DisplayName("Should efficiently handle large arrays with proper truncation and statistics")
//    void testLargeArrayHandling() {
//        // Create JSON with various large arrays
//        JSONObject json = new JSONObject();
//
//        // Numeric array with 10,000 elements
//        JSONArray hugeNumbers = new JSONArray();
//        for (int i = 0; i < 10000; i++) {
//            hugeNumbers.put(i);
//        }
//        json.put("hugeNumbers", hugeNumbers);
//
//        // String array with long values
//        JSONArray longStrings = new JSONArray();
//        String longString = "x".repeat(1000); // 1KB per string
//        for (int i = 0; i < 100; i++) {
//            longStrings.put(longString + i);
//        }
//        json.put("longStrings", longStrings);
//
//        // Nested array of objects
//        JSONArray complexArray = new JSONArray();
//        for (int i = 0; i < 500; i++) {
//            complexArray.put(new JSONObject()
//                    .put("id", i)
//                    .put("value", "item" + i)
//                    .put("nested", new JSONObject().put("deep", "value" + i)));
//        }
//        json.put("complexArray", complexArray);
//
//        String result = performanceFlattener.flattenAndConsolidateJson(json.toString());
//        JSONObject resultJson = new JSONObject(result);
//
//        // Verify array truncation (limited to 100 elements)
//        assertThat(resultJson.getLong("hugeNumbers_count")).isEqualTo(100); // Truncated
//        assertThat(resultJson.getLong("longStrings_count")).isEqualTo(100);
//        assertThat(resultJson.getLong("complexArray_id_count")).isEqualTo(100); // Truncated
//
//        // Verify statistics are still accurate for truncated data
//        assertThat(resultJson.getDouble("hugeNumbers_avg_length")).isLessThan(3); // "0" to "99"
//        assertThat(resultJson.getLong("longStrings_min_length")).isEqualTo(1001); // 1000 x's + digit
//        assertThat(resultJson.getLong("longStrings_max_length")).isEqualTo(1002); // 1000 x's + "99"
//
//        // Verify memory-efficient consolidation
//        String numbers = resultJson.getString("hugeNumbers");
//        assertThat(numbers.split(",")).hasSize(100);
//        assertThat(numbers).startsWith("0,1,2,3,4");
//    }
//
//    /**
//     * Test 4: Mixed Type Arrays and Type Inference
//     *
//     * Purpose: Tests our type inference system for arrays containing mixed types,
//     * which is common in semi-structured data from NoSQL databases or REST APIs.
//     *
//     * Why: Spark requires consistent types within columns. Our type classification
//     * helps identify data quality issues and guides schema evolution decisions.
//     */
//    @Test
//    @DisplayName("Should correctly classify mixed-type arrays and preserve type information")
//    void testMixedTypeArrays() {
//        String json = """
//            {
//                "pureNumbers": [1, 2.5, -3, 0, 1e6],
//                "pureStrings": ["a", "b", "c"],
//                "pureBooleans": [true, false, true],
//                "mixedNumStr": [1, "2", 3, "four"],
//                "mixedAll": [1, "two", true, null, 3.14],
//                "stringifiedNumbers": ["1", "2", "3", "4.5"],
//                "stringifiedBooleans": ["true", "false", "True", "FALSE"],
//                "nullsAndValues": [null, null, "value", null]
//            }
//            """;
//
//        String result = standardFlattener.flattenAndConsolidateJson(json);
//        JSONObject resultJson = new JSONObject(result);
//
//        // Verify type classification
//        assertThat(resultJson.getString("pureNumbers_type")).isEqualTo("numeric_list_consolidated");
//        assertThat(resultJson.getString("pureStrings_type")).isEqualTo("string_list_consolidated");
//        assertThat(resultJson.getString("pureBooleans_type")).isEqualTo("boolean_list_consolidated");
//        assertThat(resultJson.getString("mixedNumStr_type")).isEqualTo("string_list_consolidated");
//        assertThat(resultJson.getString("mixedAll_type")).isEqualTo("string_list_consolidated");
//
//        // Special cases - stringified types are detected
//        assertThat(resultJson.getString("stringifiedNumbers_type")).isEqualTo("numeric_list_consolidated");
//        assertThat(resultJson.getString("stringifiedBooleans_type")).isEqualTo("boolean_list_consolidated");
//
//        // Null handling
//        assertThat(resultJson.getString("nullsAndValues")).isEqualTo("null,null,value,null");
//        assertThat(resultJson.getLong("nullsAndValues_count")).isEqualTo(4);
//        assertThat(resultJson.getLong("nullsAndValues_distinct_count")).isEqualTo(2); // "null" and "value"
//    }
//
//    /**
//     * Test 5: Empty and Null Handling Edge Cases
//     *
//     * Purpose: Comprehensive testing of empty/null scenarios that often cause NPEs
//     * in production systems.
//     *
//     * Why: Robust null handling prevents crashes and ensures data pipelines continue
//     * processing even with incomplete data. Our approach preserves nulls as configured
//     * placeholders for downstream analytics.
//     */
//    @ParameterizedTest
//    @DisplayName("Should handle various empty and null scenarios gracefully")
//    @MethodSource("provideEmptyAndNullScenarios")
//    void testEmptyAndNullScenarios(String scenario, String json, String expectedKey, Object expectedValue) {
//        String result = standardFlattener.flattenAndConsolidateJson(json);
//
//        if (json.equals("null") || json.isEmpty()) {
//            assertThat(result).isEqualTo("{}");
//        } else {
//            JSONObject resultJson = new JSONObject(result);
//
//            if (expectedValue == null) {
//                assertThat(resultJson.has(expectedKey)).isFalse();
//            } else if (expectedValue instanceof Long) {
//                assertThat(resultJson.getLong(expectedKey)).isEqualTo(expectedValue);
//            } else {
//                assertThat(resultJson.get(expectedKey).toString()).isEqualTo(expectedValue.toString());
//            }
//        }
//    }
//
//    private static Stream<Arguments> provideEmptyAndNullScenarios() {
//        return Stream.of(
//                Arguments.of("Null JSON", "null", "any", null),
//                Arguments.of("Empty JSON", "", "any", null),
//                Arguments.of("Empty object", "{}", "any", null),
//                Arguments.of("Null value", "{\"key\": null}", "key", "null"),
//                Arguments.of("Empty string", "{\"key\": \"\"}", "key", ""),
//                Arguments.of("Empty array", "{\"arr\": []}", "arr", "null"),
//                Arguments.of("Array of nulls", "{\"arr\": [null, null, null]}", "arr_count", 3L),
//                Arguments.of("Nested empty", "{\"a\": {\"b\": {}}}", "a_b", "null"),
//                Arguments.of("Mixed empty", "{\"obj\": {}, \"arr\": [], \"val\": null}", "val", "null")
//        );
//    }
//
//    /**
//     * Test 6: Field Name Collision and Escaping
//     *
//     * Purpose: Tests handling of field names that could cause issues in downstream
//     * systems (SQL reserved words, special characters, etc.).
//     *
//     * Why: Data from various sources may have problematic field names. Our dot-to-underscore
//     * conversion and consistent naming ensures compatibility with Spark/SQL systems.
//     */
//    @Test
//    @DisplayName("Should handle field names with special characters and avoid collisions")
//    void testFieldNameCollisions() {
//        String json = """
//            {
//                "normal.field": "value1",
//                "normal_field": "value2",
//                "SELECT": "sql_keyword",
//                "class": "java_keyword",
//                "field-with-dash": "dash",
//                "field with space": "space",
//                "field@email.com": "email",
//                "числовое_поле": "unicode_field",
//                "🔥hotField": "emoji",
//                "": "empty_name",
//                "a": {
//                    "b.c": "nested_dot",
//                    "b_c": "nested_underscore"
//                }
//            }
//            """;
//
//        String result = standardFlattener.flattenAndConsolidateJson(json);
//        JSONObject resultJson = new JSONObject(result);
//
//        // Verify dot conversion
//        assertThat(resultJson.getString("normal_field")).isEqualTo("value1,value2");
//        assertThat(resultJson.has("normal.field")).isFalse();
//
//        // SQL/Java keywords preserved (downstream systems handle escaping)
//        assertThat(resultJson.getString("SELECT")).isEqualTo("sql_keyword");
//        assertThat(resultJson.getString("class")).isEqualTo("java_keyword");
//
//        // Special characters preserved
//        assertThat(resultJson.getString("field-with-dash")).isEqualTo("dash");
//        assertThat(resultJson.getString("field with space")).isEqualTo("space");
//        assertThat(resultJson.getString("field@email_com")).isEqualTo("email");
//
//        // Unicode preserved
//        assertThat(resultJson.getString("числовое_поле")).isEqualTo("unicode_field");
//        assertThat(resultJson.getString("🔥hotField")).isEqualTo("emoji");
//    }
//
//    /**
//     * Test 7: Complex Real-World Schema
//     *
//     * Purpose: Tests a realistic complex schema combining all features, similar to
//     * actual production data from financial systems.
//     *
//     * Why: This validates that our implementation handles the complexity of real-world
//     * data models with nested objects, arrays, mixed types, and business logic fields.
//     *
//     * Note: When consolidating arrays from multiple objects, the count represents
//     * the number of source arrays, not the total number of elements.
//     */
//    @Test
//    @DisplayName("Should handle complex real-world financial transaction schema")
//    void testComplexRealWorldSchema() {
//        String json = """
//        {
//            "transactionId": "TXN-2024-001",
//            "timestamp": 1704067200000,
//            "customer": {
//                "id": "CUST-12345",
//                "name": "ABC Corporation",
//                "type": "CORPORATE",
//                "riskScore": 7.5,
//                "contacts": [
//                    {
//                        "type": "PRIMARY",
//                        "name": "John Doe",
//                        "email": "john@abc.com",
//                        "phones": ["+1-555-0123", "+1-555-0124"]
//                    },
//                    {
//                        "type": "SECONDARY",
//                        "name": "Jane Smith",
//                        "email": "jane@abc.com",
//                        "phones": ["+1-555-0125"]
//                    }
//                ],
//                "addresses": [
//                    {
//                        "type": "BILLING",
//                        "line1": "123 Main St",
//                        "line2": "Suite 100",
//                        "city": "New York",
//                        "state": "NY",
//                        "zip": "10001",
//                        "country": "USA"
//                    }
//                ]
//            },
//            "lineItems": [
//                {
//                    "itemId": "ITEM-001",
//                    "description": "Professional Services",
//                    "quantity": 10,
//                    "unitPrice": 150.00,
//                    "taxes": [
//                        {"type": "STATE", "rate": 0.08, "amount": 120.00},
//                        {"type": "CITY", "rate": 0.02, "amount": 30.00}
//                    ],
//                    "discounts": []
//                },
//                {
//                    "itemId": "ITEM-002",
//                    "description": "Software License",
//                    "quantity": 5,
//                    "unitPrice": 500.00,
//                    "taxes": [
//                        {"type": "STATE", "rate": 0.08, "amount": 200.00}
//                    ],
//                    "discounts": [
//                        {"type": "VOLUME", "percentage": 0.10, "amount": 250.00}
//                    ]
//                }
//            ],
//            "payment": {
//                "method": "WIRE_TRANSFER",
//                "status": "COMPLETED",
//                "reference": "WT-2024-001",
//                "processedAt": 1704067500000,
//                "details": {
//                    "bankName": "Chase Bank",
//                    "accountLast4": "1234",
//                    "routingNumber": "XXXXX6789"
//                }
//            },
//            "metadata": {
//                "source": "API",
//                "version": "2.0",
//                "tags": ["urgent", "corporate", "Q1-2024"],
//                "customFields": {
//                    "projectCode": "PROJ-789",
//                    "costCenter": "CC-456"
//                }
//            }
//        }
//        """;
//
//        String result = standardFlattener.flattenAndConsolidateJson(json);
//        JSONObject resultJson = new JSONObject(result);
//
//        // Verify top-level fields
//        assertThat(resultJson.getString("transactionId")).isEqualTo("TXN-2024-001");
//        assertThat(resultJson.getLong("timestamp")).isEqualTo(1704067200000L);
//
//        // Verify nested customer data
//        assertThat(resultJson.getString("customer_id")).isEqualTo("CUST-12345");
//        assertThat(resultJson.getDouble("customer_riskScore")).isEqualTo(7.5);
//
//        // Verify array consolidation with statistics
//        assertThat(resultJson.getString("customer_contacts_type")).isEqualTo("PRIMARY,SECONDARY");
//        assertThat(resultJson.getLong("customer_contacts_type_count")).isEqualTo(2);
//
//        // Phones are consolidated from 2 contacts (2 arrays merged)
//        assertThat(resultJson.getString("customer_contacts_phones"))
//                .isEqualTo("+1-555-0123,+1-555-0124,+1-555-0125");
//        assertThat(resultJson.getLong("customer_contacts_phones_count")).isEqualTo(2); // 2 source arrays
//
//        // If you need to verify total phone count, check the string split
//        String[] phones = resultJson.getString("customer_contacts_phones").split(",");
//        assertThat(phones).hasSize(3); // Total of 3 phone numbers
//
//        // Verify complex nested arrays
//        assertThat(resultJson.getString("lineItems_taxes_type"))
//                .isEqualTo("STATE,CITY,STATE");
//        assertThat(resultJson.getLong("lineItems_taxes_amount_count")).isEqualTo(3); // 3 lineItems with taxes
//        assertThat(resultJson.getString("lineItems_taxes_amount"))
//                .isEqualTo("120.0,30.0,200.0");
//
//        // Verify empty array handling
//        assertThat(resultJson.getString("lineItems_discounts_type"))
//                .isEqualTo("VOLUME"); // Only second item has discounts
//        assertThat(resultJson.getLong("lineItems_discounts_type_count")).isEqualTo(1); // 1 lineItem with discounts
//
//        // Verify metadata
//        assertThat(resultJson.getString("metadata_tags"))
//                .isEqualTo("urgent,corporate,Q1-2024");
//        assertThat(resultJson.getLong("metadata_tags_count")).isEqualTo(3); // This is a simple array, so count = elements
//        assertThat(resultJson.getString("metadata_customFields_projectCode"))
//                .isEqualTo("PROJ-789");
//    }
//
//    /**
//     * Additional test to clarify the counting behavior
//     */
//    @Test
//    @DisplayName("Should clarify array counting behavior for nested arrays")
//    void testNestedArrayCountingBehavior() {
//        // Simple array - count equals number of elements
//        String simpleArrayJson = """
//        {
//            "tags": ["A", "B", "C"]
//        }
//        """;
//
//        JSONObject simpleResult = new JSONObject(standardFlattener.flattenAndConsolidateJson(simpleArrayJson));
//        assertThat(simpleResult.getLong("tags_count")).isEqualTo(3); // 3 elements
//
//        // Array of objects with arrays - count equals number of source objects
//        String nestedArrayJson = """
//        {
//            "groups": [
//                {"items": ["A", "B"]},
//                {"items": ["C", "D", "E"]},
//                {"items": ["F"]}
//            ]
//        }
//        """;
//
//        JSONObject nestedResult = new JSONObject(standardFlattener.flattenAndConsolidateJson(nestedArrayJson));
//        assertThat(nestedResult.getString("groups_items")).isEqualTo("A,B,C,D,E,F"); // All items consolidated
//        assertThat(nestedResult.getLong("groups_items_count")).isEqualTo(3); // 3 source arrays
//
//        // Verify by splitting
//        String[] allItems = nestedResult.getString("groups_items").split(",");
//        assertThat(allItems).hasSize(6); // Total of 6 items
//    }
//
//    /**
//     * Test 8: Performance Test - 30 Second Stress Test
//     *
//     * Purpose: Validates performance characteristics under load, ensuring the implementation
//     * can handle production volumes within SLA requirements.
//     *
//     * Why: Our financial data pipelines process millions of records. This test ensures
//     * linear scalability and identifies performance bottlenecks. The iterative approach
//     * with pre-sized collections ensures predictable performance.
//     */
//    @Test
//    @DisplayName("Should process complex documents efficiently within performance SLA")
//    @Timeout(value = 35, unit = TimeUnit.SECONDS) // 35 seconds total (30 + buffer)
//    void testPerformanceUnderLoad() {
//        // Generate a complex document template
//        JSONObject template = generateComplexDocument(20, 50, 5);
//        String templateStr = template.toString();
//
//        // Warm up JVM
//        for (int i = 0; i < 100; i++) {
//            performanceFlattener.flattenAndConsolidateJson(templateStr);
//        }
//
//        // Performance metrics
//        long startTime = System.currentTimeMillis();
//        int processedCount = 0;
//        long totalBytes = 0;
//        List<Long> processingTimes = new ArrayList<>();
//
//        // Process for 30 seconds
//        while (System.currentTimeMillis() - startTime < 30_000) {
//            // Vary the document slightly to prevent caching effects
//            JSONObject doc = generateComplexDocument(
//                    15 + (processedCount % 10),  // Vary nesting
//                    40 + (processedCount % 20),  // Vary array sizes
//                    5
//            );
//            String docStr = doc.toString();
//            totalBytes += docStr.length();
//
//            long docStart = System.nanoTime();
//            String result = performanceFlattener.flattenAndConsolidateJson(docStr);
//            long docTime = System.nanoTime() - docStart;
//
//            processingTimes.add(docTime);
//            processedCount++;
//
//            // Basic validation
//            assertThat(result).isNotEmpty();
//            assertThat(result).startsWith("{");
//            assertThat(result).endsWith("}");
//        }
//
//        long totalTime = System.currentTimeMillis() - startTime;
//
//        // Calculate performance metrics
//        double avgTimeMs = processingTimes.stream()
//                .mapToLong(Long::longValue)
//                .average()
//                .orElse(0) / 1_000_000.0;
//
//        double throughputDocsPerSec = processedCount / (totalTime / 1000.0);
//        double throughputMBPerSec = (totalBytes / (1024.0 * 1024.0)) / (totalTime / 1000.0);
//
//        // Sort for percentile calculations
//        Collections.sort(processingTimes);
//        long p50 = processingTimes.get(processingTimes.size() / 2) / 1_000_000;
//        long p95 = processingTimes.get((int)(processingTimes.size() * 0.95)) / 1_000_000;
//        long p99 = processingTimes.get((int)(processingTimes.size() * 0.99)) / 1_000_000;
//
//        // Log performance results
//        System.out.printf("""
//
//            ===== Performance Test Results =====
//            Test Duration: %.2f seconds
//            Documents Processed: %d
//            Total Data Processed: %.2f MB
//
//            Throughput:
//            - Documents/second: %.2f
//            - MB/second: %.2f
//
//            Latency (ms):
//            - Average: %.2f
//            - P50: %d
//            - P95: %d
//            - P99: %d
//
//            Performance Characteristics:
//            - Linear time complexity: O(n) where n is total nodes
//            - Space complexity: O(d) where d is max depth
//            - No recursive stack usage (iterative implementation)
//            ===================================
//            """,
//                totalTime / 1000.0, processedCount, totalBytes / (1024.0 * 1024.0),
//                throughputDocsPerSec, throughputMBPerSec,
//                avgTimeMs, p50, p95, p99
//        );
//
//        // Performance assertions
//        assertThat(processedCount).isGreaterThan(1000); // Should process at least 1000 docs in 30s
//        assertThat(avgTimeMs).isLessThan(30); // Average processing under 30ms
//        assertThat(p99).isLessThan(100); // 99th percentile under 100ms
//    }
//
//    /**
//     * Test 9: Concurrent Processing Safety
//     *
//     * Purpose: Validates thread safety for concurrent processing scenarios.
//     *
//     * Why: In production, multiple threads process documents simultaneously.
//     * Our implementation must be thread-safe without sacrificing performance.
//     */
//    @Test
//    @DisplayName("Should handle concurrent processing without thread safety issues")
//    void testConcurrentProcessing() throws InterruptedException {
//        int threadCount = 10;
//        int documentsPerThread = 100;
//        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
//        CountDownLatch latch = new CountDownLatch(threadCount);
//        ConcurrentHashMap<String, String> results = new ConcurrentHashMap<>();
//        AtomicInteger successCount = new AtomicInteger();
//        AtomicInteger errorCount = new AtomicInteger();
//
//        // Create a shared flattener instance
//        JsonFlattenerConsolidator sharedFlattener = new JsonFlattenerConsolidator(
//                ",", "null", 20, 100, false
//        );
//
//        for (int t = 0; t < threadCount; t++) {
//            final int threadId = t;
//            executor.submit(() -> {
//                try {
//                    for (int d = 0; d < documentsPerThread; d++) {
//                        // Each thread processes different documents
//                        JSONObject doc = generateComplexDocument(
//                                10 + threadId,
//                                20 + d,
//                                3
//                        );
//                        String docStr = doc.toString();
//                        String key = "thread" + threadId + "_doc" + d;
//
//                        String result = sharedFlattener.flattenAndConsolidateJson(docStr);
//                        results.put(key, result);
//
//                        // Validate result
//                        JSONObject resultJson = new JSONObject(result);
//                        if (resultJson.length() > 0) {
//                            successCount.incrementAndGet();
//                        } else {
//                            errorCount.incrementAndGet();
//                        }
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    errorCount.incrementAndGet();
//                } finally {
//                    latch.countDown();
//                }
//            });
//        }
//
//        // Wait for completion
//        boolean completed = latch.await(30, TimeUnit.SECONDS);
//        executor.shutdown();
//
//        assertThat(completed).isTrue();
//        assertThat(successCount.get()).isEqualTo(threadCount * documentsPerThread);
//        assertThat(errorCount.get()).isEqualTo(0);
//        assertThat(results.size()).isEqualTo(threadCount * documentsPerThread);
//    }
//
//    /**
//     * Test 10: Memory Efficiency and Garbage Collection
//     *
//     * Purpose: Validates that our implementation doesn't create excessive garbage
//     * or memory leaks during processing.
//     *
//     * Why: Memory efficiency is crucial for batch processing. Our pre-sized collections
//     * and object reuse patterns minimize GC pressure.
//     */
//    @Test
//    @DisplayName("Should maintain memory efficiency during large batch processing")
//    void testMemoryEfficiency() {
//        // Get initial memory state
//        System.gc();
//        long initialMemory = getUsedMemory();
//
//        // Process many documents
//        int documentCount = 1000;
//        List<String> results = new ArrayList<>(documentCount);
//
//        for (int i = 0; i < documentCount; i++) {
//            JSONObject doc = generateComplexDocument(15, 30, 4);
//            String result = performanceFlattener.flattenAndConsolidateJson(doc.toString());
//            results.add(result);
//
//            // Periodically clear results to allow GC
//            if (i % 100 == 0) {
//                results.clear();
//                System.gc();
//            }
//        }
//
//        // Final GC and memory check
//        results.clear();
//        System.gc();
//        Thread.yield();
//        System.gc();
//
//        long finalMemory = getUsedMemory();
//        long memoryGrowth = finalMemory - initialMemory;
//
//        System.out.printf("""
//
//            ===== Memory Efficiency Results =====
//            Documents Processed: %d
//            Initial Memory: %.2f MB
//            Final Memory: %.2f MB
//            Memory Growth: %.2f MB
//            Average per Document: %.2f KB
//            ====================================
//            """,
//                documentCount,
//                initialMemory / (1024.0 * 1024.0),
//                finalMemory / (1024.0 * 1024.0),
//                memoryGrowth / (1024.0 * 1024.0),
//                (memoryGrowth / 1024.0) / documentCount
//        );
//
//        // Memory growth should be minimal (allowing for JVM overhead)
//        assertThat(memoryGrowth).isLessThan(50 * 1024 * 1024); // Less than 50MB growth
//    }
//
//    // Helper methods
//
//    private JSONObject generateComplexDocument(int maxDepth, int maxArraySize, int complexity) {
//        JSONObject doc = new JSONObject();
//        doc.put("docId", UUID.randomUUID().toString());
//        doc.put("timestamp", System.currentTimeMillis());
//        doc.put("threadInfo", new JSONObject().put("id", Thread.currentThread().getId()));
//
//        // Add nested structures
//        JSONObject current = doc;
//        for (int depth = 0; depth < Math.min(maxDepth, 10); depth++) {
//            JSONObject nested = new JSONObject();
//            nested.put("level", depth);
//            nested.put("data", "value_" + depth);
//
//            // Add arrays at various levels
//            if (depth % 2 == 0) {
//                JSONArray arr = new JSONArray();
//                for (int i = 0; i < Math.min(maxArraySize, 20); i++) {
//                    if (depth % 3 == 0) {
//                        // Simple array
//                        arr.put("item_" + i);
//                    } else {
//                        // Complex array
//                        arr.put(new JSONObject()
//                                .put("index", i)
//                                .put("value", "complex_" + i));
//                    }
//                }
//                nested.put("array_" + depth, arr);
//            }
//
//            current.put("nested_" + depth, nested);
//            current = nested;
//        }
//
//        // Add variety based on complexity
//        for (int i = 0; i < complexity; i++) {
//            doc.put("field_" + i, generateRandomValue());
//        }
//
//        return doc;
//    }
//
//    private Object generateRandomValue() {
//        Random rand = new Random();
//        return switch (rand.nextInt(6)) {
//            case 0 -> rand.nextInt(1000);
//            case 1 -> rand.nextDouble() * 1000;
//            case 2 -> rand.nextBoolean();
//            case 3 -> "string_" + rand.nextInt(100);
//            case 4 -> new JSONArray().put(rand.nextInt()).put(rand.nextInt());
//            default -> JSONObject.NULL;
//        };
//    }
//
//    private long getUsedMemory() {
//        Runtime runtime = Runtime.getRuntime();
//        return runtime.totalMemory() - runtime.freeMemory();
//    }
//}
