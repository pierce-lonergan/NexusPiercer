package io.github.pierce;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive stress tests for Avro flatten/reconstruct pipeline.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Deep nesting (5+ levels)</li>
 *   <li>Arrays at various positions</li>
 *   <li>Nested arrays (arrays within arrays)</li>
 *   <li>All primitive types</li>
 *   <li>All logical types</li>
 *   <li>Unions (nullable and complex)</li>
 *   <li>Maps</li>
 *   <li>Enums</li>
 *   <li>Fixed types</li>
 *   <li>Edge cases (nulls, empty collections, special characters)</li>
 *   <li>Round-trip verification</li>
 * </ul>
 *
 * <p>Ported from src/test/groovy/AvroFlattenReconstructStressTest.groovy. The Groovy original
 * used map/list literals, which are mutable and null-tolerant; the {@link #mapOf} and
 * {@link #listOf} helpers below reproduce that exactly (LinkedHashMap / ArrayList). They are
 * deliberately NOT Map.of / List.of, which reject nulls and are immutable.
 */
class AvroFlattenReconstructStressTest {

    private MapFlattener flattener;
    private AvroReconstructor reconstructor;

    @BeforeEach
    void setup() {
        flattener = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.JSON)
                .build();

        reconstructor = AvroReconstructor.builder()
                .arrayFormat(AvroReconstructor.ArraySerializationFormat.JSON)
                .strictValidation(true)
                .build();
    }

    // ==================== HELPER METHODS ====================

    /**
     * Mutable, insertion-ordered, null-tolerant map — the Java equivalent of a Groovy map literal.
     *
     * @param keyValuePairs alternating key (String) and value (Object)
     * @return a LinkedHashMap containing the supplied pairs
     */
    private static Map<String, Object> mapOf(final Object... keyValuePairs) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            result.put((String) keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return result;
    }

    /**
     * Mutable, null-tolerant list — the Java equivalent of a Groovy list literal.
     *
     * @param items the elements, which may include nulls
     * @return an ArrayList containing the supplied elements
     */
    private static List<Object> listOf(final Object... items) {
        return new ArrayList<>(Arrays.asList(items));
    }

    private void verifyRoundTrip(Map<String, Object> original, Schema schema, String testName) {
        // Flatten
        Map<String, Object> flattened = flattener.flatten(original);
        System.out.println("=== " + testName + " ===");
        System.out.println("Original keys: " + original.keySet());
        System.out.println("Flattened keys (" + flattened.size() + "): " + flattened.keySet());

        // DEBUG: Print actual flattened values for arrays
        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String && ((String) value).contains("[")) {
                System.out.println("DEBUG flattened[" + entry.getKey() + "] = '" + value
                        + "' (length: " + ((String) value).length() + ")");
            }
        }

        // Reconstruct
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        if (!verification.isPerfect()) {
            System.out.println("DIFFERENCES FOUND:");
            for (String difference : verification.getDifferences()) {
                System.out.println("  - " + difference);
            }
            System.out.println("Original: " + original);
            System.out.println("Reconstructed: " + reconstructed);
        }

        assertThat(verification.isPerfect())
                .as("%s should reconstruct perfectly. Differences: %s",
                        testName, verification.getDifferences())
                .isTrue();
    }

    // ==================== BASIC PRIMITIVE TESTS ====================

    @Nested
    @DisplayName("Primitive Type Tests")
    class PrimitiveTests {

        @Test
        @DisplayName("All primitive types in flat record")
        void testAllPrimitives() {
            Schema schema = SchemaBuilder.record("AllPrimitives")
                    .fields()
                    .requiredString("stringField")
                    .requiredInt("intField")
                    .requiredLong("longField")
                    .requiredFloat("floatField")
                    .requiredDouble("doubleField")
                    .requiredBoolean("booleanField")
                    .requiredBytes("bytesField")
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "stringField", "hello world",
                    "intField", 42,
                    "longField", 9876543210L,
                    "floatField", 3.14f,
                    "doubleField", 2.718281828d,
                    "booleanField", true,
                    "bytesField", ByteBuffer.wrap("binary data".getBytes(StandardCharsets.UTF_8)));

            verifyRoundTrip(data, schema, "All Primitives");
        }

        @Test
        @DisplayName("Nullable primitives with null values")
        void testNullablePrimitivesWithNulls() {
            Schema schema = SchemaBuilder.record("NullablePrimitives")
                    .fields()
                    .optionalString("nullableString")
                    .optionalInt("nullableInt")
                    .optionalLong("nullableLong")
                    .optionalBoolean("nullableBool")
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "nullableString", null,
                    "nullableInt", null,
                    "nullableLong", 12345L,
                    "nullableBool", null);

            verifyRoundTrip(data, schema, "Nullable Primitives with Nulls");
        }

        @Test
        @DisplayName("Edge case string values")
        void testEdgeCaseStrings() {
            Schema schema = SchemaBuilder.record("EdgeStrings")
                    .fields()
                    .requiredString("emptyString")
                    .requiredString("whitespace")
                    .requiredString("specialChars")
                    .requiredString("unicode")
                    .requiredString("jsonLike")
                    .requiredString("newlines")
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "emptyString", "",
                    "whitespace", "   \t\n   ",
                    "specialChars", "!@#$%^&*()_+-=[]{}|;':\",./<>?",
                    "unicode", "日本語 中文 한국어 العربية",
                    "jsonLike", "{\"key\": \"value\", \"array\": [1,2,3]}",
                    "newlines", "line1\nline2\rline3\r\nline4");

            verifyRoundTrip(data, schema, "Edge Case Strings");
        }

        @Test
        @DisplayName("Numeric edge cases")
        void testNumericEdgeCases() {
            Schema schema = SchemaBuilder.record("NumericEdges")
                    .fields()
                    .requiredInt("intMax")
                    .requiredInt("intMin")
                    .requiredInt("intZero")
                    .requiredLong("longMax")
                    .requiredLong("longMin")
                    .requiredDouble("doubleMax")
                    .requiredDouble("doubleMin")
                    .requiredDouble("doubleNegZero")
                    // Note: Infinity values are problematic in JSON serialization
                    // JSON spec doesn't support Infinity, so it gets stringified
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "intMax", Integer.MAX_VALUE,
                    "intMin", Integer.MIN_VALUE,
                    "intZero", 0,
                    "longMax", Long.MAX_VALUE,
                    "longMin", Long.MIN_VALUE,
                    "doubleMax", Double.MAX_VALUE,
                    "doubleMin", Double.MIN_VALUE,
                    "doubleNegZero", -0.0d);

            verifyRoundTrip(data, schema, "Numeric Edge Cases");
        }
    }

    // ==================== NESTED RECORD TESTS ====================

    @Nested
    @DisplayName("Nested Record Tests")
    class NestedRecordTests {

        @Test
        @DisplayName("Simple two-level nesting")
        void testTwoLevelNesting() {
            Schema addressSchema = SchemaBuilder.record("Address")
                    .fields()
                    .requiredString("street")
                    .requiredString("city")
                    .requiredString("zipCode")
                    .endRecord();

            Schema schema = SchemaBuilder.record("Person")
                    .fields()
                    .requiredString("name")
                    .name("address").type(addressSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "John Doe",
                    "address", mapOf(
                            "street", "123 Main St",
                            "city", "Springfield",
                            "zipCode", "12345"));

            verifyRoundTrip(data, schema, "Two Level Nesting");
        }

        @Test
        @DisplayName("Five-level deep nesting")
        void testFiveLevelNesting() {
            Schema level5 = SchemaBuilder.record("Level5")
                    .fields()
                    .requiredString("deepValue")
                    .requiredInt("deepNumber")
                    .endRecord();

            Schema level4 = SchemaBuilder.record("Level4")
                    .fields()
                    .requiredString("l4Value")
                    .name("level5").type(level5).noDefault()
                    .endRecord();

            Schema level3 = SchemaBuilder.record("Level3")
                    .fields()
                    .requiredString("l3Value")
                    .name("level4").type(level4).noDefault()
                    .endRecord();

            Schema level2 = SchemaBuilder.record("Level2")
                    .fields()
                    .requiredString("l2Value")
                    .name("level3").type(level3).noDefault()
                    .endRecord();

            Schema schema = SchemaBuilder.record("Level1")
                    .fields()
                    .requiredString("l1Value")
                    .name("level2").type(level2).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "l1Value", "level 1",
                    "level2", mapOf(
                            "l2Value", "level 2",
                            "level3", mapOf(
                                    "l3Value", "level 3",
                                    "level4", mapOf(
                                            "l4Value", "level 4",
                                            "level5", mapOf(
                                                    "deepValue", "deepest level",
                                                    "deepNumber", 999)))));

            verifyRoundTrip(data, schema, "Five Level Nesting");
        }

        @Test
        @DisplayName("Multiple nested records at same level")
        void testMultipleNestedRecords() {
            Schema coordinateSchema = SchemaBuilder.record("Coordinate")
                    .fields()
                    .requiredDouble("lat")
                    .requiredDouble("lon")
                    .endRecord();

            Schema schema = SchemaBuilder.record("Route")
                    .fields()
                    .requiredString("name")
                    .name("start").type(coordinateSchema).noDefault()
                    .name("end").type(coordinateSchema).noDefault()
                    .name("midpoint").type(coordinateSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "Highway 101",
                    "start", mapOf("lat", 37.7749d, "lon", -122.4194d),
                    "end", mapOf("lat", 34.0522d, "lon", -118.2437d),
                    "midpoint", mapOf("lat", 35.9d, "lon", -120.3d));

            verifyRoundTrip(data, schema, "Multiple Nested Records");
        }

        @Test
        @DisplayName("Nullable nested record with null value")
        void testNullableNestedRecordNull() {
            Schema addressSchema = SchemaBuilder.record("Address")
                    .fields()
                    .requiredString("street")
                    .requiredString("city")
                    .endRecord();

            Schema nullableAddress =
                    Schema.createUnion(Schema.create(Schema.Type.NULL), addressSchema);

            Schema schema = SchemaBuilder.record("Person")
                    .fields()
                    .requiredString("name")
                    .name("primaryAddress").type(nullableAddress).noDefault()
                    .name("secondaryAddress").type(nullableAddress).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "Jane Doe",
                    "primaryAddress", mapOf("street", "456 Oak Ave", "city", "Portland"),
                    "secondaryAddress", null);

            verifyRoundTrip(data, schema, "Nullable Nested Record Null");
        }
    }

    // ==================== ARRAY TESTS ====================

    @Nested
    @DisplayName("Array Tests")
    class ArrayTests {

        @Test
        @DisplayName("Array of strings")
        void testStringArray() {
            Schema schema = SchemaBuilder.record("Tags")
                    .fields()
                    .requiredString("id")
                    .name("tags").type().array().items().stringType().noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "id", "item-1",
                    "tags", listOf("electronics", "laptop", "premium", "business"));

            verifyRoundTrip(data, schema, "String Array");
        }

        @Test
        @DisplayName("Array of integers")
        void testIntegerArray() {
            Schema schema = SchemaBuilder.record("Numbers")
                    .fields()
                    .requiredString("name")
                    .name("values").type().array().items().intType().noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "fibonacci",
                    "values", listOf(1, 1, 2, 3, 5, 8, 13, 21, 34, 55));

            verifyRoundTrip(data, schema, "Integer Array");
        }

        @Test
        @DisplayName("Empty array")
        void testEmptyArray() {
            Schema schema = SchemaBuilder.record("EmptyList")
                    .fields()
                    .requiredString("name")
                    .name("items").type().array().items().stringType().noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "empty",
                    "items", listOf());

            verifyRoundTrip(data, schema, "Empty Array");
        }

        @Test
        @DisplayName("Array with null elements (nullable element type)")
        // BUG: Null elements in arrays are being dropped during flatten/reconstruct
        // Original: ["first", null, "third", null, "fifth"]
        // Reconstructed: ["first", "third", "fifth"]
        void testArrayWithNulls() {
            Schema nullableString = Schema.createUnion(
                    Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
            Schema arraySchema = Schema.createArray(nullableString);

            Schema schema = SchemaBuilder.record("NullableElements")
                    .fields()
                    .requiredString("id")
                    .name("values").type(arraySchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "id", "test",
                    "values", listOf("first", null, "third", null, "fifth"));

            verifyRoundTrip(data, schema, "Array with Nulls");
        }

        @Test
        @DisplayName("Array of records")
        void testArrayOfRecords() {
            Schema itemSchema = SchemaBuilder.record("Item")
                    .fields()
                    .requiredString("name")
                    .requiredInt("quantity")
                    .requiredDouble("price")
                    .endRecord();

            Schema schema = SchemaBuilder.record("Order")
                    .fields()
                    .requiredString("orderId")
                    .name("items").type().array().items(itemSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "orderId", "ORD-001",
                    "items", listOf(
                            mapOf("name", "Widget", "quantity", 5, "price", 9.99d),
                            mapOf("name", "Gadget", "quantity", 2, "price", 24.99d),
                            mapOf("name", "Gizmo", "quantity", 10, "price", 4.99d)));

            verifyRoundTrip(data, schema, "Array of Records");
        }

        @Test
        @DisplayName("Array of records with nested records")
        void testArrayOfRecordsWithNestedRecords() {
            Schema priceSchema = SchemaBuilder.record("Price")
                    .fields()
                    .requiredDouble("amount")
                    .requiredString("currency")
                    .endRecord();

            Schema productSchema = SchemaBuilder.record("Product")
                    .fields()
                    .requiredString("sku")
                    .requiredString("name")
                    .name("price").type(priceSchema).noDefault()
                    .endRecord();

            Schema schema = SchemaBuilder.record("Catalog")
                    .fields()
                    .requiredString("catalogId")
                    .name("products").type().array().items(productSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "catalogId", "CAT-2025",
                    "products", listOf(
                            mapOf("sku", "SKU-001", "name", "Laptop",
                                    "price", mapOf("amount", 999.99d, "currency", "USD")),
                            mapOf("sku", "SKU-002", "name", "Mouse",
                                    "price", mapOf("amount", 29.99d, "currency", "USD")),
                            mapOf("sku", "SKU-003", "name", "Keyboard",
                                    "price", mapOf("amount", 79.99d, "currency", "EUR"))));

            verifyRoundTrip(data, schema, "Array of Records with Nested Records");
        }

        @Test
        @DisplayName("Array of records with nested arrays")
        void testArrayOfRecordsWithNestedArrays() {
            Schema attributeSchema = SchemaBuilder.record("Attribute")
                    .fields()
                    .requiredString("name")
                    .requiredString("value")
                    .endRecord();

            Schema productSchema = SchemaBuilder.record("Product")
                    .fields()
                    .requiredString("productId")
                    .requiredString("name")
                    .name("tags").type().array().items().stringType().noDefault()
                    .name("attributes").type().array().items(attributeSchema).noDefault()
                    .endRecord();

            Schema schema = SchemaBuilder.record("Store")
                    .fields()
                    .requiredString("storeId")
                    .name("products").type().array().items(productSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "storeId", "STORE-001",
                    "products", listOf(
                            mapOf(
                                    "productId", "PROD-1",
                                    "name", "Laptop",
                                    "tags", listOf("electronics", "computer", "portable"),
                                    "attributes", listOf(
                                            mapOf("name", "RAM", "value", "16GB"),
                                            mapOf("name", "Storage", "value", "512GB SSD"),
                                            mapOf("name", "CPU", "value", "Intel i7"))),
                            mapOf(
                                    "productId", "PROD-2",
                                    "name", "Phone",
                                    "tags", listOf("electronics", "mobile"),
                                    "attributes", listOf(
                                            mapOf("name", "Screen", "value", "6.5 inch"),
                                            mapOf("name", "Battery", "value", "5000mAh")))));

            verifyRoundTrip(data, schema, "Array of Records with Nested Arrays");
        }

        @Test
        @DisplayName("Asymmetric arrays (different sizes)")
        void testAsymmetricArrays() {
            Schema itemSchema = SchemaBuilder.record("Item")
                    .fields()
                    .requiredString("name")
                    .name("tags").type().array().items().stringType().noDefault()
                    .endRecord();

            Schema schema = SchemaBuilder.record("Container")
                    .fields()
                    .requiredString("id")
                    .name("items").type().array().items(itemSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "id", "container-1",
                    "items", listOf(
                            // 5 tags
                            mapOf("name", "first", "tags", listOf("a", "b", "c", "d", "e")),
                            // 1 tag
                            mapOf("name", "second", "tags", listOf("x")),
                            // 3 tags
                            mapOf("name", "third", "tags", listOf("p", "q", "r"))));

            verifyRoundTrip(data, schema, "Asymmetric Arrays");
        }

        @Test
        @DisplayName("Single element array")
        void testSingleElementArray() {
            Schema schema = SchemaBuilder.record("Single")
                    .fields()
                    .requiredString("id")
                    .name("items").type().array().items().stringType().noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "id", "single",
                    "items", listOf("only-one"));

            verifyRoundTrip(data, schema, "Single Element Array");
        }

        @Test
        @DisplayName("Large array (100 elements)")
        void testLargeArray() {
            Schema itemSchema = SchemaBuilder.record("Item")
                    .fields()
                    .requiredInt("index")
                    .requiredString("value")
                    .endRecord();

            Schema schema = SchemaBuilder.record("LargeCollection")
                    .fields()
                    .requiredString("name")
                    .name("items").type().array().items(itemSchema).noDefault()
                    .endRecord();

            List<Object> items = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                items.add(mapOf("index", i, "value", "item-" + i));
            }

            Map<String, Object> data = mapOf(
                    "name", "large-collection",
                    "items", items);

            verifyRoundTrip(data, schema, "Large Array (100 elements)");
        }
    }

    // ==================== NESTED ARRAY TESTS ====================

    @Nested
    @DisplayName("Nested Array Tests (Arrays within Arrays)")
    class NestedArrayTests {

        @Test
        @DisplayName("Array of arrays of strings (2D)")
        void testTwoDimensionalStringArray() {
            Schema innerArray = Schema.createArray(Schema.create(Schema.Type.STRING));
            Schema outerArray = Schema.createArray(innerArray);

            Schema schema = SchemaBuilder.record("Matrix")
                    .fields()
                    .requiredString("name")
                    .name("grid").type(outerArray).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "string-matrix",
                    "grid", listOf(
                            listOf("a", "b", "c"),
                            listOf("d", "e", "f"),
                            listOf("g", "h", "i")));

            verifyRoundTrip(data, schema, "2D String Array");
        }

        @Test
        @DisplayName("Array of arrays of integers (2D)")
        void testTwoDimensionalIntArray() {
            Schema innerArray = Schema.createArray(Schema.create(Schema.Type.INT));
            Schema outerArray = Schema.createArray(innerArray);

            Schema schema = SchemaBuilder.record("IntMatrix")
                    .fields()
                    .requiredString("name")
                    .name("values").type(outerArray).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "int-matrix",
                    "values", listOf(
                            listOf(1, 2, 3),
                            listOf(4, 5, 6),
                            listOf(7, 8, 9)));

            verifyRoundTrip(data, schema, "2D Int Array");
        }

        @Test
        @DisplayName("Three-dimensional array")
        void testThreeDimensionalArray() {
            Schema level1 = Schema.createArray(Schema.create(Schema.Type.INT));
            Schema level2 = Schema.createArray(level1);
            Schema level3 = Schema.createArray(level2);

            Schema schema = SchemaBuilder.record("Cube")
                    .fields()
                    .requiredString("name")
                    .name("data").type(level3).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "3d-cube",
                    "data", listOf(
                            listOf(listOf(1, 2), listOf(3, 4)),
                            listOf(listOf(5, 6), listOf(7, 8))));

            verifyRoundTrip(data, schema, "3D Array");
        }

        @Test
        @DisplayName("Jagged 2D array (rows of different lengths)")
        // BUG: Empty arrays are being dropped during flatten/reconstruct
        // Original: [[a], [b,c], [d,e,f], [g,h,i,j], []] (5 rows)
        // Reconstructed: [[a], [b,c], [d,e,f], [g,h,i,j]] (4 rows - empty row dropped)
        void testJagged2DArray() {
            Schema innerArray = Schema.createArray(Schema.create(Schema.Type.STRING));
            Schema outerArray = Schema.createArray(innerArray);

            Schema schema = SchemaBuilder.record("JaggedMatrix")
                    .fields()
                    .requiredString("name")
                    .name("rows").type(outerArray).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "jagged",
                    "rows", listOf(
                            listOf("a"),
                            listOf("b", "c"),
                            listOf("d", "e", "f"),
                            listOf("g", "h", "i", "j"),
                            listOf()));  // Empty row

            verifyRoundTrip(data, schema, "Jagged 2D Array");
        }

        @Test
        @DisplayName("Array of records containing arrays within parent array")
        void testNestedArraysInRecordArray() {
            Schema eventSchema = SchemaBuilder.record("TrackingEvent")
                    .fields()
                    .requiredLong("timestamp")
                    .requiredString("status")
                    .optionalString("notes")
                    .endRecord();

            Schema shipmentSchema = SchemaBuilder.record("Shipment")
                    .fields()
                    .requiredString("trackingNumber")
                    .requiredString("carrier")
                    .name("events").type().array().items(eventSchema).noDefault()
                    .endRecord();

            Schema schema = SchemaBuilder.record("Order")
                    .fields()
                    .requiredString("orderId")
                    .name("shipments").type().array().items(shipmentSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "orderId", "ORD-123",
                    "shipments", listOf(
                            mapOf(
                                    "trackingNumber", "1Z999",
                                    "carrier", "UPS",
                                    "events", listOf(
                                            mapOf("timestamp", 1000L, "status", "PICKED_UP",
                                                    "notes", "From warehouse"),
                                            mapOf("timestamp", 2000L, "status", "IN_TRANSIT",
                                                    "notes", null),
                                            mapOf("timestamp", 3000L, "status", "DELIVERED",
                                                    "notes", "Left at door"))),
                            mapOf(
                                    "trackingNumber", "FEDEX123",
                                    "carrier", "FedEx",
                                    "events", listOf(
                                            mapOf("timestamp", 1500L, "status", "PICKED_UP",
                                                    "notes", null)))));

            verifyRoundTrip(data, schema, "Nested Arrays in Record Array");
        }
    }

    // ==================== LOGICAL TYPE TESTS ====================

    @Nested
    @DisplayName("Logical Type Tests")
    class LogicalTypeTests {

        @Test
        @DisplayName("Decimal logical type (bytes)")
        void testDecimalBytes() {
            Schema decimalSchema = LogicalTypes.decimal(10, 2)
                    .addToSchema(Schema.create(Schema.Type.BYTES));

            Schema schema = SchemaBuilder.record("Money")
                    .fields()
                    .requiredString("description")
                    .name("amount").type(decimalSchema).noDefault()
                    .endRecord();

            // Create the decimal as ByteBuffer (how Avro stores it)
            BigDecimal value = new BigDecimal("12345.67");
            Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
            ByteBuffer decimalBytes = conversion.toBytes(value, decimalSchema,
                    LogicalTypes.decimal(10, 2));

            Map<String, Object> data = mapOf(
                    "description", "Test amount",
                    "amount", decimalBytes);

            verifyRoundTrip(data, schema, "Decimal (Bytes)");
        }

        @Test
        @DisplayName("Timestamp millis logical type")
        void testTimestampMillis() {
            Schema timestampSchema = LogicalTypes.timestampMillis()
                    .addToSchema(Schema.create(Schema.Type.LONG));

            Schema schema = SchemaBuilder.record("Event")
                    .fields()
                    .requiredString("name")
                    .name("timestamp").type(timestampSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "Test Event",
                    "timestamp", System.currentTimeMillis());

            verifyRoundTrip(data, schema, "Timestamp Millis");
        }

        @Test
        @DisplayName("Date logical type")
        void testDateLogicalType() {
            Schema dateSchema = LogicalTypes.date()
                    .addToSchema(Schema.create(Schema.Type.INT));

            Schema schema = SchemaBuilder.record("Birthday")
                    .fields()
                    .requiredString("name")
                    .name("birthDate").type(dateSchema).noDefault()
                    .endRecord();

            // Days since epoch
            int daysSinceEpoch = (int) LocalDate.of(1990, 5, 15).toEpochDay();

            Map<String, Object> data = mapOf(
                    "name", "John",
                    "birthDate", daysSinceEpoch);

            verifyRoundTrip(data, schema, "Date Logical Type");
        }

        @Test
        @DisplayName("UUID logical type")
        void testUuidLogicalType() {
            Schema uuidSchema = LogicalTypes.uuid()
                    .addToSchema(Schema.create(Schema.Type.STRING));

            Schema schema = SchemaBuilder.record("Entity")
                    .fields()
                    .name("id").type(uuidSchema).noDefault()
                    .requiredString("name")
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "id", UUID.randomUUID().toString(),
                    "name", "Test Entity");

            verifyRoundTrip(data, schema, "UUID Logical Type");
        }

        @Test
        @DisplayName("Time millis logical type")
        void testTimeMillis() {
            Schema timeSchema = LogicalTypes.timeMillis()
                    .addToSchema(Schema.create(Schema.Type.INT));

            Schema schema = SchemaBuilder.record("Schedule")
                    .fields()
                    .requiredString("event")
                    .name("startTime").type(timeSchema).noDefault()
                    .endRecord();

            // Milliseconds since midnight
            int timeMillis = 9 * 3600 * 1000 + 30 * 60 * 1000;  // 9:30 AM

            Map<String, Object> data = mapOf(
                    "event", "Meeting",
                    "startTime", timeMillis);

            verifyRoundTrip(data, schema, "Time Millis");
        }

        @Test
        @DisplayName("Multiple decimals in array")
        void testDecimalArray() {
            Schema decimalSchema = LogicalTypes.decimal(10, 2)
                    .addToSchema(Schema.create(Schema.Type.BYTES));
            Schema decimalArraySchema = Schema.createArray(decimalSchema);

            Schema schema = SchemaBuilder.record("Invoice")
                    .fields()
                    .requiredString("invoiceId")
                    .name("lineAmounts").type(decimalArraySchema).noDefault()
                    .endRecord();

            Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
            LogicalTypes.Decimal logicalType = LogicalTypes.decimal(10, 2);

            List<ByteBuffer> amounts = new ArrayList<>(Arrays.asList(
                    conversion.toBytes(new BigDecimal("100.00"), decimalSchema, logicalType),
                    conversion.toBytes(new BigDecimal("250.50"), decimalSchema, logicalType),
                    conversion.toBytes(new BigDecimal("75.25"), decimalSchema, logicalType)));

            Map<String, Object> data = mapOf(
                    "invoiceId", "INV-001",
                    "lineAmounts", amounts);

            verifyRoundTrip(data, schema, "Decimal Array");
        }
    }

    // ==================== ENUM TESTS ====================

    @Nested
    @DisplayName("Enum Tests")
    class EnumTests {

        @Test
        @DisplayName("Simple enum field")
        void testSimpleEnum() {
            Schema statusEnum = SchemaBuilder.enumeration("Status")
                    .symbols("PENDING", "ACTIVE", "COMPLETED", "CANCELLED");

            Schema schema = SchemaBuilder.record("Task")
                    .fields()
                    .requiredString("name")
                    .name("status").type(statusEnum).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "Important Task",
                    "status", new GenericData.EnumSymbol(statusEnum, "ACTIVE"));

            verifyRoundTrip(data, schema, "Simple Enum");
        }

        @Test
        @DisplayName("Nullable enum")
        void testNullableEnum() {
            Schema statusEnum = SchemaBuilder.enumeration("Status")
                    .symbols("PENDING", "ACTIVE", "COMPLETED");
            Schema nullableStatus =
                    Schema.createUnion(Schema.create(Schema.Type.NULL), statusEnum);

            Schema schema = SchemaBuilder.record("Task")
                    .fields()
                    .requiredString("name")
                    .name("status").type(nullableStatus).noDefault()
                    .endRecord();

            Map<String, Object> dataWithEnum = mapOf(
                    "name", "Task 1",
                    "status", new GenericData.EnumSymbol(statusEnum, "PENDING"));

            Map<String, Object> dataWithNull = mapOf(
                    "name", "Task 2",
                    "status", null);

            verifyRoundTrip(dataWithEnum, schema, "Nullable Enum (with value)");
            verifyRoundTrip(dataWithNull, schema, "Nullable Enum (null)");
        }

        @Test
        @DisplayName("Array of enums")
        // BUG: Enum arrays reconstruct as String instead of GenericData.EnumSymbol
        // The reconstructor needs to convert strings back to EnumSymbol for arrays
        void testEnumArray() {
            Schema dayEnum = SchemaBuilder.enumeration("Day")
                    .symbols("MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY",
                            "SATURDAY", "SUNDAY");
            Schema dayArraySchema = Schema.createArray(dayEnum);

            Schema schema = SchemaBuilder.record("Schedule")
                    .fields()
                    .requiredString("name")
                    .name("workDays").type(dayArraySchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "Work Schedule",
                    "workDays", listOf(
                            new GenericData.EnumSymbol(dayEnum, "MONDAY"),
                            new GenericData.EnumSymbol(dayEnum, "TUESDAY"),
                            new GenericData.EnumSymbol(dayEnum, "WEDNESDAY"),
                            new GenericData.EnumSymbol(dayEnum, "THURSDAY"),
                            new GenericData.EnumSymbol(dayEnum, "FRIDAY")));

            verifyRoundTrip(data, schema, "Enum Array");
        }
    }

    // ==================== MAP TESTS ====================

    @Nested
    @DisplayName("Map Tests")
    class MapTests {

        @Test
        @DisplayName("Map of strings")
        void testStringMap() {
            Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));

            Schema schema = SchemaBuilder.record("Config")
                    .fields()
                    .requiredString("name")
                    .name("properties").type(mapSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "app-config",
                    "properties", mapOf(
                            "host", "localhost",
                            "port", "8080",
                            "debug", "true"));

            verifyRoundTrip(data, schema, "String Map");
        }

        @Test
        @DisplayName("Map of integers")
        void testIntMap() {
            Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));

            Schema schema = SchemaBuilder.record("Scores")
                    .fields()
                    .requiredString("game")
                    .name("playerScores").type(mapSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "game", "Championship",
                    "playerScores", mapOf(
                            "Alice", 150,
                            "Bob", 120,
                            "Charlie", 180));

            verifyRoundTrip(data, schema, "Int Map");
        }

        @Test
        @DisplayName("Map of records")
        void testRecordMap() {
            Schema addressSchema = SchemaBuilder.record("Address")
                    .fields()
                    .requiredString("street")
                    .requiredString("city")
                    .endRecord();

            Schema mapSchema = Schema.createMap(addressSchema);

            Schema schema = SchemaBuilder.record("AddressBook")
                    .fields()
                    .requiredString("owner")
                    .name("addresses").type(mapSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "owner", "John",
                    "addresses", mapOf(
                            "home", mapOf("street", "123 Home St", "city", "Hometown"),
                            "work", mapOf("street", "456 Work Ave", "city", "Workville"),
                            "vacation", mapOf("street", "789 Beach Rd", "city", "Paradise")));

            verifyRoundTrip(data, schema, "Record Map");
        }

        @Test
        @DisplayName("Empty map")
        void testEmptyMap() {
            Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));

            Schema schema = SchemaBuilder.record("EmptyConfig")
                    .fields()
                    .requiredString("name")
                    .name("settings").type(mapSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "empty-config",
                    "settings", mapOf());

            verifyRoundTrip(data, schema, "Empty Map");
        }

        @Test
        @Disabled("KNOWN LIMITATION: Map keys containing underscore separator collide with "
                + "flattened path structure")
        @DisplayName("Map with special key characters")
        void testMapSpecialKeys() {
            Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));

            Schema schema = SchemaBuilder.record("SpecialKeys")
                    .fields()
                    .requiredString("name")
                    .name("data").type(mapSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "name", "special",
                    "data", mapOf(
                            "key_with_underscore", "value1",
                            "key.with.dots", "value2",
                            "key-with-dashes", "value3",
                            "key with spaces", "value4"));

            verifyRoundTrip(data, schema, "Map with Special Keys");
        }
    }

    // ==================== COMPLEX UNION TESTS ====================

    @Nested
    @DisplayName("Complex Union Tests")
    class ComplexUnionTests {

        @Test
        @DisplayName("Union of multiple record types")
        void testMultiTypeUnion() {
            Schema emailSchema = SchemaBuilder.record("Email")
                    .fields()
                    .requiredString("address")
                    .requiredString("subject")
                    .endRecord();

            Schema smsSchema = SchemaBuilder.record("SMS")
                    .fields()
                    .requiredString("phoneNumber")
                    .requiredString("message")
                    .endRecord();

            Schema unionSchema = Schema.createUnion(
                    Schema.create(Schema.Type.NULL),
                    emailSchema,
                    smsSchema);

            Schema schema = SchemaBuilder.record("Notification")
                    .fields()
                    .requiredString("id")
                    .name("channel").type(unionSchema).noDefault()
                    .endRecord();

            // Test with Email
            Map<String, Object> emailData = mapOf(
                    "id", "notif-1",
                    "channel", mapOf("address", "test@example.com", "subject", "Hello"));

            // Test with SMS
            Map<String, Object> smsData = mapOf(
                    "id", "notif-2",
                    "channel", mapOf("phoneNumber", "+1234567890", "message", "Hi there"));

            // Test with null
            Map<String, Object> nullData = mapOf(
                    "id", "notif-3",
                    "channel", null);

            verifyRoundTrip(emailData, schema, "Union with Email");
            verifyRoundTrip(smsData, schema, "Union with SMS");
            verifyRoundTrip(nullData, schema, "Union with null");
        }

        @Test
        @DisplayName("Union of primitive and complex type")
        // BUG: Union with record branch returns null instead of the record
        // When the union contains [null, string, record] and record is present,
        // the reconstructor incorrectly returns null
        void testPrimitiveComplexUnion() {
            Schema addressSchema = SchemaBuilder.record("Address")
                    .fields()
                    .requiredString("street")
                    .requiredString("city")
                    .endRecord();

            Schema unionSchema = Schema.createUnion(
                    Schema.create(Schema.Type.NULL),
                    Schema.create(Schema.Type.STRING),
                    addressSchema);

            Schema schema = SchemaBuilder.record("Location")
                    .fields()
                    .requiredString("name")
                    .name("details").type(unionSchema).noDefault()
                    .endRecord();

            // Test with string
            Map<String, Object> stringData = mapOf(
                    "name", "Simple Location",
                    "details", "Just a description");

            // Test with record
            Map<String, Object> recordData = mapOf(
                    "name", "Full Location",
                    "details", mapOf("street", "123 Main St", "city", "Springfield"));

            verifyRoundTrip(stringData, schema, "Union with String");
            verifyRoundTrip(recordData, schema, "Union with Record");
        }
    }

    // ==================== FIXED TYPE TESTS ====================

    @Nested
    @DisplayName("Fixed Type Tests")
    class FixedTypeTests {

        @Test
        @DisplayName("Fixed type field")
        // BUG: Fixed type expects 16 bytes but receives 73 - the bytes are being
        // converted to string representation (Base64) during flatten and not
        // properly decoded during reconstruct
        void testFixedType() {
            Schema fixedSchema = Schema.createFixed("MD5Hash", "", "", 16);

            Schema schema = SchemaBuilder.record("Document")
                    .fields()
                    .requiredString("name")
                    .name("hash").type(fixedSchema).noDefault()
                    .endRecord();

            byte[] hashBytes = new byte[16];
            new Random(42).nextBytes(hashBytes);

            Map<String, Object> data = mapOf(
                    "name", "test-document.pdf",
                    "hash", new GenericData.Fixed(fixedSchema, hashBytes));

            verifyRoundTrip(data, schema, "Fixed Type");
        }
    }

    // ==================== EDGE CASE TESTS ====================

    @Nested
    @DisplayName("Edge Case Tests")
    class EdgeCaseTests {

        @Test
        @Disabled("KNOWN LIMITATION: Underscore separator cannot distinguish 'field_name' field "
                + "from nested 'field.name' path")
        @DisplayName("Field names with underscores (separator collision)")
        void testUnderscoreFieldNames() {
            Schema schema = SchemaBuilder.record("UnderscoreFields")
                    .fields()
                    .requiredString("field_name")
                    .requiredString("field_name_two")
                    .requiredString("another_field")
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "field_name", "value1",
                    "field_name_two", "value2",
                    "another_field", "value3");

            verifyRoundTrip(data, schema, "Underscore Field Names");
        }

        @Test
        @Disabled("KNOWN LIMITATION: 'user_name' field collides with 'user.name' nested path "
                + "when using underscore separator")
        @DisplayName("Deeply nested field name collision")
        void testFieldNameCollision() {
            Schema innerSchema = SchemaBuilder.record("Inner")
                    .fields()
                    .requiredString("name")
                    .endRecord();

            Schema schema = SchemaBuilder.record("Outer")
                    .fields()
                    // Flattens to user_name
                    .requiredString("user_name")
                    // user.name flattens to user_name
                    .name("user").type(innerSchema).noDefault()
                    .endRecord();

            // This is a known problematic case - field name collision
            Map<String, Object> data = mapOf(
                    "user_name", "direct_value",
                    "user", mapOf("name", "nested_value"));

            verifyRoundTrip(data, schema, "Field Name Collision");
        }

        @Test
        @DisplayName("Very long field values")
        void testLongFieldValues() {
            Schema schema = SchemaBuilder.record("LongValues")
                    .fields()
                    .requiredString("shortField")
                    .requiredString("longField")
                    .endRecord();

            String longValue = "x".repeat(10000);  // 10KB string

            Map<String, Object> data = mapOf(
                    "shortField", "short",
                    "longField", longValue);

            verifyRoundTrip(data, schema, "Long Field Values");
        }

        @Test
        @DisplayName("Record with only optional fields all null")
        void testAllOptionalFieldsNull() {
            Schema schema = SchemaBuilder.record("AllOptional")
                    .fields()
                    .optionalString("field1")
                    .optionalInt("field2")
                    .optionalBoolean("field3")
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "field1", null,
                    "field2", null,
                    "field3", null);

            verifyRoundTrip(data, schema, "All Optional Fields Null");
        }

        @Test
        @DisplayName("Boolean edge cases")
        void testBooleanEdgeCases() {
            Schema schema = SchemaBuilder.record("Booleans")
                    .fields()
                    .requiredBoolean("trueValue")
                    .requiredBoolean("falseValue")
                    .optionalBoolean("nullableTrue")
                    .optionalBoolean("nullableFalse")
                    .optionalBoolean("nullableNull")
                    .endRecord();

            Map<String, Object> data = mapOf(
                    "trueValue", true,
                    "falseValue", false,
                    "nullableTrue", true,
                    "nullableFalse", false,
                    "nullableNull", null);

            verifyRoundTrip(data, schema, "Boolean Edge Cases");
        }

        @Test
        @DisplayName("Bytes field with various content")
        void testBytesVariousContent() {
            Schema schema = SchemaBuilder.record("BytesTest")
                    .fields()
                    .requiredString("name")
                    .requiredBytes("emptyBytes")
                    .requiredBytes("textBytes")
                    .requiredBytes("binaryBytes")
                    .endRecord();

            byte[] binaryContent = new byte[256];
            for (int i = 0; i < 256; i++) {
                binaryContent[i] = (byte) i;
            }

            Map<String, Object> data = mapOf(
                    "name", "bytes-test",
                    "emptyBytes", ByteBuffer.wrap(new byte[0]),
                    "textBytes", ByteBuffer.wrap("Hello World".getBytes(StandardCharsets.UTF_8)),
                    "binaryBytes", ByteBuffer.wrap(binaryContent));

            verifyRoundTrip(data, schema, "Bytes Various Content");
        }
    }
}
