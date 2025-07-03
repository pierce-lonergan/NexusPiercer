package io.github.pierce.FlattenConsolidatorTests;

import io.github.pierce.JsonFlattenerConsolidator;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Extended test suite for the array explosion feature in JsonFlattenerConsolidator
 * Now includes comprehensive edge cases and complex use cases
 */
public class JsonFlattenerExplosionTest {

    // ===== EXISTING TESTS =====

    @Test
    @DisplayName("Default behavior - no explosion returns single record")
    void testNoExplosion() {
        String json = """
            {
                "orderId": "123",
                "items": ["A", "B", "C"],
                "customer": {
                    "name": "John",
                    "addresses": [
                        {"city": "NYC"},
                        {"city": "LA"}
                    ]
                }
            }
            """;

        // No explosion paths
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false
        );

        // Should return single consolidated record
        String result = flattener.flattenAndConsolidateJson(json);
        assertThat(result).contains("\"items\":\"A,B,C\"");
        assertThat(result).contains("\"customer_addresses_city\":\"NYC,LA\"");

        // Explode method should also return single record when no paths specified
        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(1);
        assertThat(exploded.get(0)).isEqualTo(result);
    }

    @Test
    @DisplayName("Explode terminal array (simple array of primitives)")
    void testExplodeTerminalArray() {
        String json = """
            {
                "orderId": "123",
                "customer": "ABC Corp",
                "items": ["Widget", "Gadget", "Tool"]
            }
            """;

        // Explode on items
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "items"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(3);

        // Each record should have one item
        JSONObject record1 = new JSONObject(exploded.get(0));
        assertThat(record1.getString("items")).isEqualTo("Widget");
        assertThat(record1.getLong("items_explosion_index")).isEqualTo(0);
        assertThat(record1.getString("orderId")).isEqualTo("123");
        assertThat(record1.getString("customer")).isEqualTo("ABC Corp");

        JSONObject record2 = new JSONObject(exploded.get(1));
        assertThat(record2.getString("items")).isEqualTo("Gadget");
        assertThat(record2.getLong("items_explosion_index")).isEqualTo(1);

        JSONObject record3 = new JSONObject(exploded.get(2));
        assertThat(record3.getString("items")).isEqualTo("Tool");
        assertThat(record3.getLong("items_explosion_index")).isEqualTo(2);
    }

    @Test
    @DisplayName("Explode non-terminal array (array of objects)")
    void testExplodeNonTerminalArray() {
        String json = """
            {
                "orderId": "123",
                "departments": [
                    {
                        "name": "Sales",
                        "manager": "Alice",
                        "budget": 100000
                    },
                    {
                        "name": "IT",
                        "manager": "Bob",
                        "budget": 150000
                    }
                ]
            }
            """;

        // Explode on departments
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "departments"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(2);

        JSONObject dept1 = new JSONObject(exploded.get(0));
        assertThat(dept1.getString("departments_name")).isEqualTo("Sales");
        assertThat(dept1.getString("departments_manager")).isEqualTo("Alice");
        assertThat(dept1.getDouble("departments_budget")).isEqualTo(100000);
        assertThat(dept1.getString("orderId")).isEqualTo("123");

        JSONObject dept2 = new JSONObject(exploded.get(1));
        assertThat(dept2.getString("departments_name")).isEqualTo("IT");
        assertThat(dept2.getString("departments_manager")).isEqualTo("Bob");
        assertThat(dept2.getDouble("departments_budget")).isEqualTo(150000);
    }

    @Test
    @DisplayName("Hierarchical explosion - parent and child arrays")
    void testHierarchicalExplosion() {
        String json = """
            {
                "company": "TechCorp",
                "departments": [
                    {
                        "name": "Engineering",
                        "employees": [
                            {"id": "E1", "name": "Alice", "skills": ["Java", "Python"]},
                            {"id": "E2", "name": "Bob", "skills": ["JavaScript", "React"]}
                        ]
                    },
                    {
                        "name": "Marketing",
                        "employees": [
                            {"id": "M1", "name": "Charlie", "skills": ["SEO", "Content"]}
                        ]
                    }
                ]
            }
            """;

        // Explode on departments.employees (should respect hierarchy)
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "departments.employees"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(3); // Total employees across all departments

        // Verify each employee record maintains department context
        JSONObject emp1 = new JSONObject(exploded.get(0));
        System.out.println(emp1.toString());
        assertThat(emp1.getString("departments_name")).isEqualTo("Engineering");
        assertThat(emp1.getString("departments_employees_id")).isEqualTo("E1");
        assertThat(emp1.getString("departments_employees_name")).isEqualTo("Alice");
        assertThat(emp1.getString("departments_employees_skills")).isEqualTo("Java,Python");

        JSONObject emp3 = new JSONObject(exploded.get(2));
        assertThat(emp3.getString("departments_name")).isEqualTo("Marketing");
        assertThat(emp3.getString("departments_employees_id")).isEqualTo("M1");
    }

    @Test
    @DisplayName("Multiple explosion paths")
    void testMultipleExplosionPaths() {
        String json = """
            {
                "orderId": "123",
                "lineItems": [
                    {"product": "A", "quantity": 2},
                    {"product": "B", "quantity": 1}
                ],
                "shipments": [
                    {"carrier": "FedEx", "tracking": "123"},
                    {"carrier": "UPS", "tracking": "456"}
                ]
            }
            """;

        // Explode on both lineItems and shipments
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "lineItems", "shipments"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        // Should create cartesian product: 2 lineItems × 2 shipments = 4 records
        assertThat(exploded).hasSize(4);

        // Verify combinations
        JSONObject combo1 = new JSONObject(exploded.get(0));
        assertThat(combo1.getString("lineItems_product")).isEqualTo("A");
        assertThat(combo1.getString("shipments_carrier")).isEqualTo("FedEx");

        JSONObject combo4 = new JSONObject(exploded.get(3));
        assertThat(combo4.getString("lineItems_product")).isEqualTo("B");
        assertThat(combo4.getString("shipments_carrier")).isEqualTo("UPS");
    }

    @Test
    @DisplayName("Empty array handling in explosion")
    void testEmptyArrayExplosion() {
        String json = """
            {
                "orderId": "123",
                "items": [],
                "customer": "ABC Corp"
            }
            """;

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "items"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        // Empty array should return original record
        assertThat(exploded).hasSize(1);
        assertThat(exploded.get(0)).contains("\"orderId\":\"123\"");
        assertThat(exploded.get(0)).contains("\"customer\":\"ABC Corp\"");
    }

    @Test
    @DisplayName("Complex real-world explosion scenario")
    void testComplexRealWorldExplosion() {
        String json = """
            {
                "orderId": "ORD-2024-001",
                "customer": {
                    "id": "CUST-123",
                    "name": "Global Corp"
                },
                "orderDate": "2024-01-15",
                "lineItems": [
                    {
                        "sku": "WIDGET-A",
                        "quantity": 10,
                        "unitPrice": 25.00,
                        "discounts": [
                            {"type": "VOLUME", "amount": 2.50},
                            {"type": "PROMO", "amount": 1.00}
                        ]
                    },
                    {
                        "sku": "GADGET-B",
                        "quantity": 5,
                        "unitPrice": 50.00,
                        "discounts": [
                            {"type": "VOLUME", "amount": 5.00}
                        ]
                    }
                ],
                "tags": ["URGENT", "PREMIUM", "INTERNATIONAL"]
            }
            """;

        // Explode on lineItems.discounts to get one record per discount
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "lineItems.discounts"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        // Should have 3 records (2 discounts for first item + 1 for second)
        assertThat(exploded).hasSize(3);

        // Each record should maintain the line item context
        JSONObject discount1 = new JSONObject(exploded.get(0));
        System.out.println(discount1);
        assertThat(discount1.getString("lineItems_sku")).isEqualTo("WIDGET-A");
        assertThat(discount1.getDouble("lineItems_quantity")).isEqualTo(10);
        assertThat(discount1.getString("lineItems_discounts_type")).isEqualTo("VOLUME");
        assertThat(discount1.getDouble("lineItems_discounts_amount")).isEqualTo(2.50);

        JSONObject discount3 = new JSONObject(exploded.get(2));
        assertThat(discount3.getString("lineItems_sku")).isEqualTo("GADGET-B");
        assertThat(discount3.getString("lineItems_discounts_type")).isEqualTo("VOLUME");

        // Non-array fields should be preserved in all records
        for (String record : exploded) {
            JSONObject obj = new JSONObject(record);
            assertThat(obj.getString("orderId")).isEqualTo("ORD-2024-001");
            assertThat(obj.getString("customer_name")).isEqualTo("Global Corp");
            assertThat(obj.getString("tags")).isEqualTo("URGENT,PREMIUM,INTERNATIONAL");
        }
    }

    // ===== NEW EXTENDED TESTS =====

    @Test
    @DisplayName("Performance test for explosion")
    void testExplosionPerformance() {
        // Create JSON with larger arrays
        StringBuilder json = new StringBuilder("{\"data\": [");
        for (int i = 0; i < 100; i++) {
            if (i > 0) json.append(",");
            json.append("{\"id\":").append(i).append(",\"value\":\"item_").append(i).append("\"}");
        }
        json.append("]}");

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "data"
        );

        long start = System.currentTimeMillis();
        List<String> exploded = flattener.flattenAndExplodeJson(json.toString());
        long elapsed = System.currentTimeMillis() - start;

        assertThat(exploded).hasSize(100);
        assertThat(elapsed).isLessThan(1000); // Should complete in under 1 second

        System.out.println("Explosion of 100 records completed in " + elapsed + "ms");
    }

    @Test
    @DisplayName("Explosion with null values and missing fields")
    void testExplosionWithNullsAndMissingFields() {
        String json = """
            {
                "orderId": "123",
                "items": [
                    {"id": "A", "name": "Item A", "price": 10.0},
                    {"id": "B", "name": null, "price": 20.0},
                    {"id": "C", "price": 30.0},
                    null,
                    {"id": "D", "name": "Item D"}
                ]
            }
            """;

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", "NULL", 50, 1000, false, false, "items"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        // Should handle nulls gracefully
        assertThat(exploded).hasSize(5);

        // Check each record
        JSONObject item1 = new JSONObject(exploded.get(0));
        assertThat(item1.getString("items_name")).isEqualTo("Item A");
        assertThat(item1.getDouble("items_price")).isEqualTo(10.0);

        JSONObject item2 = new JSONObject(exploded.get(1));
        assertThat(item2.getString("items_name")).isEqualTo("NULL");

        JSONObject item3 = new JSONObject(exploded.get(2));
        assertThat(item3.has("items_name")).isFalse(); // Missing field

        JSONObject item4 = new JSONObject(exploded.get(3));
        // This was a null array element - check how it's handled
        assertThat(item4.getString("orderId")).isEqualTo("123"); // Parent fields preserved

        JSONObject item5 = new JSONObject(exploded.get(4));
        assertThat(item5.has("items_price")).isFalse(); // Missing price
    }

    @Test
    @DisplayName("Deep nested explosion at various levels")
    void testDeepNestedExplosion() {
        String json = """
            {
                "company": "TechCorp",
                "regions": [
                    {
                        "name": "North",
                        "countries": [
                            {
                                "code": "US",
                                "offices": [
                                    {
                                        "city": "NYC",
                                        "employees": [
                                            {"id": "E1", "name": "Alice"},
                                            {"id": "E2", "name": "Bob"}
                                        ]
                                    },
                                    {
                                        "city": "LA",
                                        "employees": [
                                            {"id": "E3", "name": "Charlie"}
                                        ]
                                    }
                                ]
                            },
                            {
                                "code": "CA",
                                "offices": [
                                    {
                                        "city": "Toronto",
                                        "employees": [
                                            {"id": "E4", "name": "David"}
                                        ]
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "name": "South",
                        "countries": [
                            {
                                "code": "BR",
                                "offices": [
                                    {
                                        "city": "Rio",
                                        "employees": [
                                            {"id": "E5", "name": "Eve"}
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
            """;

        // Test explosion at different levels

        // Level 1: Explode regions
        JsonFlattenerConsolidator level1 = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "regions"
        );
        List<String> exploded1 = level1.flattenAndExplodeJson(json);
        assertThat(exploded1).hasSize(2); // North and South

        // Level 2: Explode countries
        JsonFlattenerConsolidator level2 = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "regions.countries"
        );
        List<String> exploded2 = level2.flattenAndExplodeJson(json);
        assertThat(exploded2).hasSize(3); // US, CA, BR

        // Level 3: Explode offices
        JsonFlattenerConsolidator level3 = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "regions.countries.offices"
        );
        List<String> exploded3 = level3.flattenAndExplodeJson(json);
        assertThat(exploded3).hasSize(4); // NYC, LA, Toronto, Rio

        // Level 4: Explode employees (deepest level)
        JsonFlattenerConsolidator level4 = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "regions.countries.offices.employees"
        );
        List<String> exploded4 = level4.flattenAndExplodeJson(json);
        assertThat(exploded4).hasSize(5); // E1-E5

        // Verify deep context is maintained
        JSONObject employee = new JSONObject(exploded4.get(0));
        assertThat(employee.getString("regions_name")).isEqualTo("North");
        assertThat(employee.getString("regions_countries_code")).isEqualTo("US");
        assertThat(employee.getString("regions_countries_offices_city")).isEqualTo("NYC");
        assertThat(employee.getString("regions_countries_offices_employees_id")).isEqualTo("E1");
    }

    @Test
    @DisplayName("Explosion with mixed type arrays")
    void testExplosionWithMixedTypes() {
        String json = """
            {
                "data": [
                    "string value",
                    123,
                    true,
                    {"type": "object", "value": "complex"},
                    ["nested", "array"],
                    null,
                    45.67
                ]
            }
            """;

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", "NULL", 50, 1000, false, false, "data"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(7);

        // Verify each type is handled correctly
        assertThat(new JSONObject(exploded.get(0)).getString("data")).isEqualTo("string value");
        assertThat(new JSONObject(exploded.get(1)).get("data").toString()).isEqualTo("123");
        assertThat(new JSONObject(exploded.get(2)).get("data").toString()).isEqualTo("true");

        JSONObject complexItem = new JSONObject(exploded.get(3));
        assertThat(complexItem.getString("data_type")).isEqualTo("object");
        assertThat(complexItem.getString("data_value")).isEqualTo("complex");

        // Nested array should be consolidated
        assertThat(new JSONObject(exploded.get(4)).getString("data")).isEqualTo("nested,array");
    }

    @Test
    @DisplayName("Explosion with array size limits")
    void testExplosionWithArraySizeLimits() {
        // Create JSON with array larger than limit
        JSONObject json = new JSONObject();
        JSONArray items = new JSONArray();
        for (int i = 0; i < 150; i++) {
            items.put(new JSONObject()
                    .put("id", i)
                    .put("value", "item_" + i));
        }
        json.put("items", items);

        // Limit to 100 items
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 100, false, false, "items"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json.toString());
        assertThat(exploded).hasSize(100); // Should respect the limit

        // Verify first and last items
        JSONObject first = new JSONObject(exploded.get(0));
        assertThat(first.getInt("items_id")).isEqualTo(0);

        JSONObject last = new JSONObject(exploded.get(99));
        assertThat(last.getInt("items_id")).isEqualTo(99);
    }

    @Test
    @DisplayName("Explosion with special characters and Unicode")
    void testExplosionWithSpecialCharacters() {
        String json = """
            {
                "company": "Tech & Co.",
                "products": [
                    {"name": "Widget™", "description": "Best widget €99"},
                    {"name": "Gadget®", "description": "Zürich-made 製品"},
                    {"name": "Tool<script>", "description": "'; DROP TABLE; 🔧"}
                ]
            }
            """;

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "products"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(3);

        // Verify special characters are preserved
        JSONObject product1 = new JSONObject(exploded.get(0));
        assertThat(product1.getString("products_name")).isEqualTo("Widget™");
        assertThat(product1.getString("products_description")).contains("€");

        JSONObject product2 = new JSONObject(exploded.get(1));
        assertThat(product2.getString("products_description")).contains("Zürich");
        assertThat(product2.getString("products_description")).contains("製品");

        JSONObject product3 = new JSONObject(exploded.get(2));
        assertThat(product3.getString("products_name")).contains("<script>");
        assertThat(product3.getString("products_description")).contains("🔧");
    }

    @Test
    @DisplayName("Complex Cartesian product with multiple nested paths")
    void testComplexCartesianProduct() {
        String json = """
            {
                "orderId": "123",
                "warehouses": [
                    {"id": "W1", "location": "East"},
                    {"id": "W2", "location": "West"}
                ],
                "products": [
                    {"sku": "A", "name": "Product A"},
                    {"sku": "B", "name": "Product B"},
                    {"sku": "C", "name": "Product C"}
                ],
                "shippingOptions": [
                    {"carrier": "FedEx", "speed": "Express"},
                    {"carrier": "UPS", "speed": "Ground"}
                ]
            }
            """;

        // Explode all three arrays - should create 2 x 3 x 2 = 12 records
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false,
                "warehouses", "products", "shippingOptions"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(12);

        // Verify some combinations
        Set<String> combinations = new HashSet<>();
        for (String record : exploded) {
            JSONObject obj = new JSONObject(record);
            String combo = obj.getString("warehouses_id") + "-" +
                    obj.getString("products_sku") + "-" +
                    obj.getString("shippingOptions_carrier");
            combinations.add(combo);
        }

        // Should have all unique combinations
        assertThat(combinations).hasSize(12);
        assertThat(combinations).contains("W1-A-FedEx", "W2-C-UPS");
    }

    @Test
    @DisplayName("Explosion with consolidateWithMatrixDenotorsInValue option")
    void testExplosionWithMatrixDenotors() {
        String json = """
            {
                "matrix": [
                    [1, 2, 3],
                    [4, 5, 6],
                    [7, 8, 9]
                ]
            }
            """;

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, true, false, "matrix"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(3);

        // Each exploded record should have matrix indices preserved
        JSONObject row1 = new JSONObject(exploded.get(0));
        String matrixRow1 = row1.getString("matrix");
        // With matrix denotors, the consolidated value should include indices
        assertThat(matrixRow1).contains("[");
    }



    @Test
    @DisplayName("Explosion with non-existent paths")
    void testExplosionWithNonExistentPaths() {
        String json = """
            {
                "orderId": "123",
                "customer": "ABC Corp",
                "items": ["A", "B", "C"]
            }
            """;

        // Try to explode on non-existent path
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "nonexistent.path"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        // Should return single record (no explosion)
        assertThat(exploded).hasSize(1);
        assertThat(exploded.get(0)).contains("\"items\":\"A,B,C\"");
    }

    @Test
    @DisplayName("Explosion with partial path matches")
    void testExplosionWithPartialPaths() {
        String json = """
            {
                "data": {
                    "items": ["A", "B"],
                    "itemsExtra": ["X", "Y", "Z"]
                },
                "dataItems": ["1", "2", "3", "4"]
            }
            """;

        // Explode on "data.items" - should not match "dataItems" or "data.itemsExtra"
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "data.items"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(2); // Only data.items

        JSONObject item1 = new JSONObject(exploded.get(0));
        assertThat(item1.getString("data_items")).isEqualTo("A");
        // Other arrays should be consolidated
        assertThat(item1.getString("data_itemsExtra")).isEqualTo("X,Y,Z");
        assertThat(item1.getString("dataItems")).isEqualTo("1,2,3,4");
    }

    @Test
    @DisplayName("Performance test with large Cartesian product")
    void testLargeCartesianProductPerformance() {
        // Create JSON with multiple arrays that create large Cartesian product
        JSONObject json = new JSONObject();

        // 10 x 10 x 10 = 1000 combinations
        for (String arrayName : Arrays.asList("array1", "array2", "array3")) {
            JSONArray array = new JSONArray();
            for (int i = 0; i < 10; i++) {
                array.put(new JSONObject().put("value", arrayName + "_" + i));
            }
            json.put(arrayName, array);
        }

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false,
                "array1", "array2", "array3"
        );

        long start = System.currentTimeMillis();
        List<String> exploded = flattener.flattenAndExplodeJson(json.toString());
        long elapsed = System.currentTimeMillis() - start;

        assertThat(exploded).hasSize(1000);
        assertThat(elapsed).isLessThan(5000); // Should complete in under 5 seconds

        System.out.println("Large Cartesian product (1000 records) completed in " + elapsed + "ms");
    }

    @Test
    @DisplayName("Memory efficiency during explosion")
    void testExplosionMemoryEfficiency() {
        // Create a document that could cause memory issues if not handled properly
        JSONObject json = new JSONObject();
        json.put("metadata", "x".repeat(1000)); // 1KB of metadata

        JSONArray items = new JSONArray();
        for (int i = 0; i < 100; i++) {
            JSONObject item = new JSONObject();
            item.put("id", i);
            item.put("data", "y".repeat(100)); // 100 bytes per item
            items.put(item);
        }
        json.put("items", items);

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "items"
        );

        // Get initial memory
        System.gc();
        long memBefore = getUsedMemory();

        List<String> exploded = flattener.flattenAndExplodeJson(json.toString());

        // Get final memory
        System.gc();
        long memAfter = getUsedMemory();
        long memUsed = (memAfter - memBefore) / 1024; // KB

        assertThat(exploded).hasSize(100);

        // Each record should contain the metadata
        for (String record : exploded) {
            assertThat(record).contains("x".repeat(1000));
        }

        System.out.println("Memory used for explosion: " + memUsed + " KB");
        // Memory usage should be reasonable (not exponential)
        assertThat(memUsed).isLessThan(10000); // Less than 10MB
    }

    @Test
    @DisplayName("Explosion with circular-like structures up to max depth")
    void testExplosionWithDeepRecursiveLikeStructure() {
        // Create a structure that references similar patterns at different levels
        String json = """
            {
                "level1": {
                    "items": [
                        {
                            "id": "1A",
                            "nested": {
                                "items": [
                                    {
                                        "id": "2A",
                                        "nested": {
                                            "items": [
                                                {"id": "3A"},
                                                {"id": "3B"}
                                            ]
                                        }
                                    },
                                    {
                                        "id": "2B",
                                        "nested": {
                                            "items": [
                                                {"id": "3C"}
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "id": "1B",
                            "nested": {
                                "items": [
                                    {"id": "2C"}
                                ]
                            }
                        }
                    ]
                }
            }
            """;

        // Explode at the deepest level
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 10, 1000, false, false,
                "level1.items.nested.items.nested.items"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(3); // 3A, 3B, 3C

        // Verify the deep nesting is preserved
        JSONObject deepItem = new JSONObject(exploded.get(0));
        assertThat(deepItem.getString("level1_items_id")).isEqualTo("1A");
        assertThat(deepItem.getString("level1_items_nested_items_id")).isEqualTo("2A");
        assertThat(deepItem.getString("level1_items_nested_items_nested_items_id")).isEqualTo("3A");
    }

    @Test
    @DisplayName("Explosion with arrays at multiple levels of same path")
    void testExplosionWithMultipleLevelArrays() {
        String json = """
            {
                "organizations": [
                    {
                        "name": "Org1",
                        "departments": [
                            {
                                "name": "Sales",
                                "teams": [
                                    {"name": "Team A", "members": 5},
                                    {"name": "Team B", "members": 3}
                                ]
                            },
                            {
                                "name": "Marketing",
                                "teams": [
                                    {"name": "Team C", "members": 4}
                                ]
                            }
                        ]
                    },
                    {
                        "name": "Org2",
                        "departments": [
                            {
                                "name": "IT",
                                "teams": [
                                    {"name": "Team D", "members": 6}
                                ]
                            }
                        ]
                    }
                ]
            }
            """;

        // Test different explosion levels and verify counts
        Map<String, Integer> explosionTests = new HashMap<>();
        explosionTests.put("organizations", 2);  // 2 orgs
        explosionTests.put("organizations.departments", 3);  // 2 + 1 departments
        explosionTests.put("organizations.departments.teams", 4);  // 2 + 1 + 1 teams

        for (Map.Entry<String, Integer> test : explosionTests.entrySet()) {
            JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                    ",", null, 50, 1000, false, false, test.getKey()
            );

            List<String> exploded = flattener.flattenAndExplodeJson(json);
            assertThat(exploded)
                    .as("Explosion path: " + test.getKey())
                    .hasSize(test.getValue());
        }
    }

    @Test
    @DisplayName("Edge case: Explosion with empty objects in arrays")
    void testExplosionWithEmptyObjects() {
        String json = """
            {
                "data": [
                    {"id": 1, "value": "A"},
                    {},
                    {"id": 3, "value": "C"},
                    {},
                    {"id": 5}
                ]
            }
            """;

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "data"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(5);

        // Empty objects should still create records
        JSONObject empty1 = new JSONObject(exploded.get(1));
        assertThat(empty1.has("data_id")).isFalse();
        assertThat(empty1.has("data_value")).isFalse();
        assertThat(empty1.getLong("data_explosion_index")).isEqualTo(1);
    }

    @Test
    @DisplayName("Concurrent explosion safety")
    void testConcurrentExplosion() throws InterruptedException {
        String json = """
            {
                "items": [
                    {"id": 1, "value": "A"},
                    {"id": 2, "value": "B"},
                    {"id": 3, "value": "C"}
                ]
            }
            """;

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "items"
        );

        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<List<String>> allResults = new CopyOnWriteArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    List<String> result = flattener.flattenAndExplodeJson(json);
                    allResults.add(result);
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(allResults).hasSize(threadCount);

        // All results should be identical
        List<String> firstResult = allResults.get(0);
        for (List<String> result : allResults) {
            assertThat(result).hasSize(firstResult.size());
            for (int i = 0; i < result.size(); i++) {
                JSONObject expected = new JSONObject(firstResult.get(i));
                JSONObject actual = new JSONObject(result.get(i));
                assertThat(actual.toString()).isEqualTo(expected.toString());
            }
        }
    }

    // Additional edge case tests

    @Test
    @DisplayName("Explosion with nested arrays containing nested arrays")
    void testExplosionWithDeeplyNestedArrays() {
        String json = """
            {
                "catalog": {
                    "categories": [
                        {
                            "name": "Electronics",
                            "subcategories": [
                                {
                                    "name": "Computers",
                                    "products": [
                                        {
                                            "sku": "LAPTOP-1",
                                            "variants": [
                                                {"size": "13-inch", "price": 999},
                                                {"size": "15-inch", "price": 1299}
                                            ]
                                        },
                                        {
                                            "sku": "DESKTOP-1",
                                            "variants": [
                                                {"size": "Mini", "price": 599}
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            }
            """;

        // Explode at the deepest level - variants
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false,
                "catalog.categories.subcategories.products.variants"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(3); // 2 laptop variants + 1 desktop variant

        // Verify all context is preserved
        JSONObject variant1 = new JSONObject(exploded.get(0));
        assertThat(variant1.getString("catalog_categories_name")).isEqualTo("Electronics");
        assertThat(variant1.getString("catalog_categories_subcategories_name")).isEqualTo("Computers");
        assertThat(variant1.getString("catalog_categories_subcategories_products_sku")).isEqualTo("LAPTOP-1");
        assertThat(variant1.getString("catalog_categories_subcategories_products_variants_size")).isEqualTo("13-inch");
    }

    @Test
    @DisplayName("Explosion with same field names at different levels")
    void testExplosionWithDuplicateFieldNames() {
        String json = """
            {
                "items": [
                    {
                        "id": "parent-1",
                        "items": [
                            {
                                "id": "child-1",
                                "items": [
                                    {"id": "grandchild-1"},
                                    {"id": "grandchild-2"}
                                ]
                            }
                        ]
                    }
                ]
            }
            """;

        // Explode at the grandchild level
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "items.items.items"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(2);

        JSONObject grandchild1 = new JSONObject(exploded.get(0));
        assertThat(grandchild1.getString("items_id")).isEqualTo("parent-1");
        assertThat(grandchild1.getString("items_items_id")).isEqualTo("child-1");
        assertThat(grandchild1.getString("items_items_items_id")).isEqualTo("grandchild-1");
    }

    @Test
    @DisplayName("Explosion behavior with primitive array inside object array")
    void testExplosionWithMixedArrayTypes() {
        String json = """
            {
                "orders": [
                    {
                        "id": "ORDER-1",
                        "items": ["A", "B", "C"],
                        "quantities": [1, 2, 3]
                    },
                    {
                        "id": "ORDER-2", 
                        "items": ["X", "Y"],
                        "quantities": [5, 10]
                    }
                ]
            }
            """;

        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, false, "orders"
        );

        List<String> exploded = flattener.flattenAndExplodeJson(json);
        assertThat(exploded).hasSize(2);

        // Primitive arrays within exploded objects should be consolidated
        JSONObject order1 = new JSONObject(exploded.get(0));
        assertThat(order1.getString("orders_items")).isEqualTo("A,B,C");
        assertThat(order1.getString("orders_quantities")).isEqualTo("1,2,3");
    }

    private long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}