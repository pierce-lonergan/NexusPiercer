package io.github.pierce.avro;

import io.github.pierce.GAvroSchemaFlattener;
import io.github.pierce.MapFlattener;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Comprehensive test suite to verify that:
 * <ol>
 *   <li>{@link GAvroSchemaFlattener} produces field names that EXACTLY match
 *       {@link MapFlattener} keys</li>
 *   <li>Both flatteners handle all Avro types correctly</li>
 *   <li>Edge cases are handled consistently</li>
 * </ol>
 *
 * <p>CRITICAL: The flattened schema keys MUST match the flattened data keys for downstream
 * systems (Glue, Athena, etc.) to work correctly.</p>
 *
 * <p>Ported from {@code src/test/groovy/AvroSchemaFlattenerAlignmentTest.groovy}. The Groovy
 * original printed the two key sets and any mismatches to stdout before asserting; that
 * diagnostic is preserved here as the AssertJ failure description, so it surfaces on failure
 * instead of on every run.</p>
 */
class AvroSchemaFlattenerAlignmentTest {

    private MapFlattener dataFlattener;
    private GAvroSchemaFlattener schemaFlattener;

    @BeforeEach
    void setup() {
        // Use default configuration for both - they must align
        dataFlattener = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.JSON)
                .build();
        schemaFlattener = new GAvroSchemaFlattener();
    }

    // ==================== HELPER METHODS ====================

    /**
     * Core verification method - ensures schema keys match data keys exactly.
     */
    private void verifyKeyAlignment(Schema schema, Map<String, Object> data, String testName) {
        // Flatten schema
        Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                schemaFlattener.flattenSchema(schema);
        Set<String> schemaKeys = flattenedSchema.keySet();

        // Flatten data
        Map<String, Object> flattenedData = dataFlattener.flatten(data);
        Set<String> dataKeys = flattenedData.keySet();

        List<String> sortedSchemaKeys = sorted(schemaKeys);
        List<String> sortedDataKeys = sorted(dataKeys);

        // Verify exact match. Mismatches are folded into the description so a failure names them.
        assertThat(sortedDataKeys)
                .as(alignmentDescription(testName, "Schema keys must exactly match data keys",
                        schemaKeys, dataKeys))
                .isEqualTo(sortedSchemaKeys);
    }

    /**
     * Verify alignment with array boundary separator enabled.
     */
    private void verifyKeyAlignmentWithArrayBoundary(Schema schema, Map<String, Object> data,
                                                     String testName) {
        MapFlattener dataFlattenerAB = MapFlattener.builder()
                .useArrayBoundarySeparator(true)
                .arrayFormat(MapFlattener.ArraySerializationFormat.JSON)
                .build();
        GAvroSchemaFlattener schemaFlattenerAB = new GAvroSchemaFlattener(
                GAvroSchemaFlattener.AvroFlatteningConfig.builder()
                        .useArrayBoundarySeparator(true)
                        .build()
        );

        Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                schemaFlattenerAB.flattenSchema(schema);
        Map<String, Object> flattenedData = dataFlattenerAB.flatten(data);

        Set<String> schemaKeys = flattenedSchema.keySet();
        Set<String> dataKeys = flattenedData.keySet();

        assertThat(sorted(dataKeys))
                .as(alignmentDescription(testName,
                        "Schema keys must match data keys with array boundary separator",
                        schemaKeys, dataKeys))
                .isEqualTo(sorted(schemaKeys));
    }

    private static String alignmentDescription(String testName, String requirement,
                                               Set<String> schemaKeys, Set<String> dataKeys) {
        Set<String> inSchemaNotData = difference(schemaKeys, dataKeys);
        Set<String> inDataNotSchema = difference(dataKeys, schemaKeys);
        return testName + ": " + requirement
                + System.lineSeparator() + "  schema keys (" + schemaKeys.size() + "): "
                + sorted(schemaKeys)
                + System.lineSeparator() + "  data keys (" + dataKeys.size() + "): "
                + sorted(dataKeys)
                + System.lineSeparator() + "  in schema but NOT in data: " + inSchemaNotData
                + System.lineSeparator() + "  in data but NOT in schema: " + inDataNotSchema;
    }

    private static List<String> sorted(Collection<String> keys) {
        List<String> result = new ArrayList<>(keys);
        Collections.sort(result);
        return result;
    }

    private static Set<String> difference(Set<String> left, Set<String> right) {
        Set<String> result = new LinkedHashSet<>(left);
        result.removeAll(right);
        return result;
    }

    /**
     * Mutable, null-tolerant, insertion-ordered map literal — the Java equivalent of the Groovy
     * {@code [a: 1, b: null]} literal. Deliberately NOT {@code Map.of}, which rejects nulls and
     * does not preserve order.
     */
    private static Map<String, Object> map(Object... keyValuePairs) {
        if (keyValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("map() requires an even number of arguments");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            result.put((String) keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return result;
    }

    /**
     * Mutable, null-tolerant list literal — the Java equivalent of Groovy {@code [1, 2, 3]}.
     * Deliberately NOT {@code List.of}, which rejects nulls.
     */
    @SafeVarargs
    private static <T> List<T> list(T... items) {
        return new ArrayList<>(Arrays.asList(items));
    }

    // ==================== BASIC PRIMITIVE TESTS ====================

    @Nested
    @DisplayName("Primitive Field Tests")
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

            Map<String, Object> data = map(
                    "stringField", "hello",
                    "intField", 42,
                    "longField", 123456789L,
                    "floatField", 3.14f,
                    "doubleField", 2.718281828,
                    "booleanField", true,
                    "bytesField", "base64data".getBytes(StandardCharsets.UTF_8)
            );

            verifyKeyAlignment(schema, data, "All Primitives");
        }

        @Test
        @DisplayName("Nullable primitive fields")
        void testNullablePrimitives() {
            Schema schema = SchemaBuilder.record("NullablePrimitives")
                    .fields()
                    .requiredString("requiredString")
                    .optionalString("optionalString")
                    .optionalInt("optionalInt")
                    .optionalLong("optionalLong")
                    .optionalDouble("optionalDouble")
                    .optionalBoolean("optionalBoolean")
                    .endRecord();

            // Test with values
            Map<String, Object> dataWithValues = map(
                    "requiredString", "required",
                    "optionalString", "optional",
                    "optionalInt", 42,
                    "optionalLong", 100L,
                    "optionalDouble", 3.14,
                    "optionalBoolean", true
            );
            verifyKeyAlignment(schema, dataWithValues, "Nullable Primitives (with values)");

            // Test with nulls
            Map<String, Object> dataWithNulls = map(
                    "requiredString", "required",
                    "optionalString", null,
                    "optionalInt", null,
                    "optionalLong", null,
                    "optionalDouble", null,
                    "optionalBoolean", null
            );
            verifyKeyAlignment(schema, dataWithNulls, "Nullable Primitives (with nulls)");
        }
    }

    // ==================== NESTED RECORD TESTS ====================

    @Nested
    @DisplayName("Nested Record Tests")
    class NestedRecordTests {

        @Test
        @DisplayName("Single level nesting")
        void testSingleLevelNesting() {
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

            Map<String, Object> data = map(
                    "name", "Alice",
                    "address", map(
                            "street", "123 Main St",
                            "city", "Springfield",
                            "zipCode", "12345"
                    )
            );

            verifyKeyAlignment(schema, data, "Single Level Nesting");
        }

        @Test
        @DisplayName("Two level nesting")
        void testTwoLevelNesting() {
            Schema countrySchema = SchemaBuilder.record("Country")
                    .fields()
                    .requiredString("name")
                    .requiredString("code")
                    .endRecord();

            Schema addressSchema = SchemaBuilder.record("Address")
                    .fields()
                    .requiredString("street")
                    .requiredString("city")
                    .name("country").type(countrySchema).noDefault()
                    .endRecord();

            Schema schema = SchemaBuilder.record("Person")
                    .fields()
                    .requiredString("name")
                    .name("address").type(addressSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "name", "Bob",
                    "address", map(
                            "street", "456 Oak Ave",
                            "city", "Portland",
                            "country", map(
                                    "name", "United States",
                                    "code", "US"
                            )
                    )
            );

            verifyKeyAlignment(schema, data, "Two Level Nesting");
        }

        @Test
        @DisplayName("Three level nesting")
        void testThreeLevelNesting() {
            Schema geoSchema = SchemaBuilder.record("GeoLocation")
                    .fields()
                    .requiredDouble("latitude")
                    .requiredDouble("longitude")
                    .endRecord();

            Schema citySchema = SchemaBuilder.record("City")
                    .fields()
                    .requiredString("name")
                    .name("geo").type(geoSchema).noDefault()
                    .endRecord();

            Schema addressSchema = SchemaBuilder.record("Address")
                    .fields()
                    .requiredString("street")
                    .name("city").type(citySchema).noDefault()
                    .endRecord();

            Schema schema = SchemaBuilder.record("Person")
                    .fields()
                    .requiredString("name")
                    .name("address").type(addressSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "name", "Charlie",
                    "address", map(
                            "street", "789 Pine Blvd",
                            "city", map(
                                    "name", "Seattle",
                                    "geo", map(
                                            "latitude", 47.6062,
                                            "longitude", -122.3321
                                    )
                            )
                    )
            );

            verifyKeyAlignment(schema, data, "Three Level Nesting");
        }

        @Test
        @DisplayName("Multiple nested records at same level")
        void testMultipleNestedRecords() {
            Schema addressSchema = SchemaBuilder.record("Address")
                    .fields()
                    .requiredString("street")
                    .requiredString("city")
                    .endRecord();

            Schema contactSchema = SchemaBuilder.record("Contact")
                    .fields()
                    .requiredString("email")
                    .requiredString("phone")
                    .endRecord();

            Schema schema = SchemaBuilder.record("Person")
                    .fields()
                    .requiredString("name")
                    .name("homeAddress").type(addressSchema).noDefault()
                    .name("workAddress").type(addressSchema).noDefault()
                    .name("contact").type(contactSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "name", "Diana",
                    "homeAddress", map("street", "Home St", "city", "Home City"),
                    "workAddress", map("street", "Work St", "city", "Work City"),
                    "contact", map("email", "diana@example.com", "phone", "555-1234")
            );

            verifyKeyAlignment(schema, data, "Multiple Nested Records");
        }
    }

    // ==================== ARRAY TESTS ====================

    @Nested
    @DisplayName("Array Tests")
    class ArrayTests {

        @Test
        @DisplayName("Array of strings")
        void testArrayOfStrings() {
            Schema schema = SchemaBuilder.record("Tags")
                    .fields()
                    .requiredString("name")
                    .name("tags").type().array().items().stringType().noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "name", "Item1",
                    "tags", list("tag1", "tag2", "tag3")
            );

            verifyKeyAlignment(schema, data, "Array of Strings");
        }

        @Test
        @DisplayName("Array of integers")
        void testArrayOfIntegers() {
            Schema schema = SchemaBuilder.record("Scores")
                    .fields()
                    .requiredString("player")
                    .name("scores").type().array().items().intType().noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "player", "Player1",
                    "scores", list(100, 200, 300, 400)
            );

            verifyKeyAlignment(schema, data, "Array of Integers");
        }

        @Test
        @DisplayName("Array of records - simple")
        void testArrayOfRecordsSimple() {
            Schema itemSchema = SchemaBuilder.record("Item")
                    .fields()
                    .requiredString("name")
                    .requiredDouble("price")
                    .endRecord();

            Schema schema = SchemaBuilder.record("Order")
                    .fields()
                    .requiredString("orderId")
                    .name("items").type().array().items(itemSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "orderId", "ORD-001",
                    "items", list(
                            map("name", "Widget", "price", 9.99),
                            map("name", "Gadget", "price", 19.99),
                            map("name", "Gizmo", "price", 29.99)
                    )
            );

            verifyKeyAlignment(schema, data, "Array of Records - Simple");
        }

        @Test
        @DisplayName("Array of records with nested record")
        void testArrayOfRecordsWithNestedRecord() {
            Schema dimensionsSchema = SchemaBuilder.record("Dimensions")
                    .fields()
                    .requiredDouble("width")
                    .requiredDouble("height")
                    .requiredDouble("depth")
                    .endRecord();

            Schema itemSchema = SchemaBuilder.record("Item")
                    .fields()
                    .requiredString("sku")
                    .requiredString("name")
                    .name("dimensions").type(dimensionsSchema).noDefault()
                    .endRecord();

            Schema schema = SchemaBuilder.record("Shipment")
                    .fields()
                    .requiredString("shipmentId")
                    .name("items").type().array().items(itemSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "shipmentId", "SHIP-001",
                    "items", list(
                            map("sku", "SKU-A", "name", "Box A",
                                    "dimensions", map("width", 10, "height", 20, "depth", 30)),
                            map("sku", "SKU-B", "name", "Box B",
                                    "dimensions", map("width", 15, "height", 25, "depth", 35))
                    )
            );

            verifyKeyAlignment(schema, data, "Array of Records with Nested Record");
            verifyKeyAlignmentWithArrayBoundary(schema, data, "Array of Records with Nested Record");
        }

        @Test
        @DisplayName("Array of records with array field")
        void testArrayOfRecordsWithArrayField() {
            Schema itemSchema = SchemaBuilder.record("LineItem")
                    .fields()
                    .requiredString("productId")
                    .requiredInt("quantity")
                    .name("tags").type().array().items().stringType().noDefault()
                    .endRecord();

            Schema schema = SchemaBuilder.record("Cart")
                    .fields()
                    .requiredString("cartId")
                    .name("lineItems").type().array().items(itemSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "cartId", "CART-001",
                    "lineItems", list(
                            map("productId", "PROD-A", "quantity", 2,
                                    "tags", list("sale", "popular")),
                            map("productId", "PROD-B", "quantity", 1,
                                    "tags", list("new"))
                    )
            );

            verifyKeyAlignment(schema, data, "Array of Records with Array Field");
        }

        @Test
        @DisplayName("Multiple arrays at same level")
        void testMultipleArrays() {
            Schema schema = SchemaBuilder.record("Document")
                    .fields()
                    .requiredString("title")
                    .name("authors").type().array().items().stringType().noDefault()
                    .name("keywords").type().array().items().stringType().noDefault()
                    .name("pageNumbers").type().array().items().intType().noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "title", "Research Paper",
                    "authors", list("Alice", "Bob", "Charlie"),
                    "keywords", list("AI", "ML", "Data"),
                    "pageNumbers", list(1, 15, 30, 45)
            );

            verifyKeyAlignment(schema, data, "Multiple Arrays");
        }

        @Test
        @DisplayName("Empty array")
        void testEmptyArray() {
            Schema schema = SchemaBuilder.record("Container")
                    .fields()
                    .requiredString("name")
                    .name("items").type().array().items().stringType().noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "name", "Empty Container",
                    "items", new ArrayList<>()
            );

            verifyKeyAlignment(schema, data, "Empty Array");
        }
    }

    // ==================== NESTED ARRAY TESTS ====================

    @Nested
    @DisplayName("Nested Array Tests")
    class NestedArrayTests {

        @Test
        @DisplayName("2D array of primitives")
        void test2DArrayOfPrimitives() {
            Schema innerArray = Schema.createArray(Schema.create(Schema.Type.INT));
            Schema outerArray = Schema.createArray(innerArray);

            Schema schema = SchemaBuilder.record("Matrix")
                    .fields()
                    .requiredString("name")
                    .name("data").type(outerArray).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "name", "IntMatrix",
                    "data", list(list(1, 2, 3), list(4, 5, 6), list(7, 8, 9))
            );

            verifyKeyAlignment(schema, data, "2D Array of Primitives");
        }

        @Test
        @DisplayName("Array of arrays of records")
        void testArrayOfArraysOfRecords() {
            Schema eventSchema = SchemaBuilder.record("Event")
                    .fields()
                    .requiredLong("timestamp")
                    .requiredString("type")
                    .endRecord();

            Schema innerArray = Schema.createArray(eventSchema);
            Schema outerArray = Schema.createArray(innerArray);

            Schema schema = SchemaBuilder.record("EventGroups")
                    .fields()
                    .requiredString("groupName")
                    .name("eventBatches").type(outerArray).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "groupName", "BatchGroup1",
                    "eventBatches", list(
                            list(
                                    map("timestamp", 1000L, "type", "A"),
                                    map("timestamp", 2000L, "type", "B")
                            ),
                            list(
                                    map("timestamp", 3000L, "type", "C")
                            )
                    )
            );

            verifyKeyAlignment(schema, data, "Array of Arrays of Records");
        }
    }

    // ==================== COMPLEX REAL-WORLD SCENARIOS ====================

    @Nested
    @DisplayName("Real-World Scenarios")
    class RealWorldScenarios {

        @Test
        @DisplayName("E-commerce order with full structure")
        void testEcommerceOrder() {
            // Address schema
            Schema addressSchema = SchemaBuilder.record("Address")
                    .fields()
                    .requiredString("street")
                    .requiredString("city")
                    .requiredString("state")
                    .requiredString("zipCode")
                    .requiredString("country")
                    .endRecord();

            // Line item schema
            Schema lineItemSchema = SchemaBuilder.record("LineItem")
                    .fields()
                    .requiredString("sku")
                    .requiredString("name")
                    .requiredInt("quantity")
                    .requiredDouble("unitPrice")
                    .requiredDouble("totalPrice")
                    .endRecord();

            // Payment schema
            Schema paymentSchema = SchemaBuilder.record("Payment")
                    .fields()
                    .requiredString("method")
                    .requiredString("transactionId")
                    .requiredDouble("amount")
                    .requiredString("status")
                    .endRecord();

            // Order schema
            Schema schema = SchemaBuilder.record("Order")
                    .fields()
                    .requiredString("orderId")
                    .requiredString("customerId")
                    .requiredLong("orderDate")
                    .requiredString("status")
                    .name("shippingAddress").type(addressSchema).noDefault()
                    .name("billingAddress").type(addressSchema).noDefault()
                    .name("lineItems").type().array().items(lineItemSchema).noDefault()
                    .name("payment").type(paymentSchema).noDefault()
                    .requiredDouble("subtotal")
                    .requiredDouble("tax")
                    .requiredDouble("total")
                    .endRecord();

            Map<String, Object> data = map(
                    "orderId", "ORD-12345",
                    "customerId", "CUST-67890",
                    "orderDate", 1699900000000L,
                    "status", "CONFIRMED",
                    "shippingAddress", map(
                            "street", "123 Main St",
                            "city", "Springfield",
                            "state", "IL",
                            "zipCode", "62701",
                            "country", "USA"
                    ),
                    "billingAddress", map(
                            "street", "456 Oak Ave",
                            "city", "Chicago",
                            "state", "IL",
                            "zipCode", "60601",
                            "country", "USA"
                    ),
                    "lineItems", list(
                            map("sku", "WIDGET-001", "name", "Blue Widget", "quantity", 2,
                                    "unitPrice", 9.99, "totalPrice", 19.98),
                            map("sku", "GADGET-002", "name", "Red Gadget", "quantity", 1,
                                    "unitPrice", 24.99, "totalPrice", 24.99),
                            map("sku", "GIZMO-003", "name", "Green Gizmo", "quantity", 3,
                                    "unitPrice", 14.99, "totalPrice", 44.97)
                    ),
                    "payment", map(
                            "method", "CREDIT_CARD",
                            "transactionId", "TXN-ABCDEF",
                            "amount", 97.88,
                            "status", "APPROVED"
                    ),
                    "subtotal", 89.94,
                    "tax", 7.94,
                    "total", 97.88
            );

            verifyKeyAlignment(schema, data, "E-commerce Order");
            verifyKeyAlignmentWithArrayBoundary(schema, data, "E-commerce Order");
        }

        @Test
        @DisplayName("User profile with preferences and history")
        void testUserProfile() {
            Schema preferenceSchema = SchemaBuilder.record("Preference")
                    .fields()
                    .requiredString("key")
                    .requiredString("value")
                    .requiredBoolean("enabled")
                    .endRecord();

            Schema sessionSchema = SchemaBuilder.record("Session")
                    .fields()
                    .requiredString("sessionId")
                    .requiredLong("startTime")
                    .optionalLong("endTime")
                    .requiredString("ipAddress")
                    .requiredString("userAgent")
                    .endRecord();

            Schema schema = SchemaBuilder.record("UserProfile")
                    .fields()
                    .requiredString("userId")
                    .requiredString("email")
                    .optionalString("displayName")
                    .requiredLong("createdAt")
                    .optionalLong("lastLoginAt")
                    .requiredBoolean("emailVerified")
                    .name("roles").type().array().items().stringType().noDefault()
                    .name("preferences").type().array().items(preferenceSchema).noDefault()
                    .name("recentSessions").type().array().items(sessionSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "userId", "user-123",
                    "email", "user@example.com",
                    "displayName", "John Doe",
                    "createdAt", 1600000000000L,
                    "lastLoginAt", 1699900000000L,
                    "emailVerified", true,
                    "roles", list("user", "admin", "moderator"),
                    "preferences", list(
                            map("key", "theme", "value", "dark", "enabled", true),
                            map("key", "notifications", "value", "email", "enabled", false)
                    ),
                    "recentSessions", list(
                            map("sessionId", "sess-1", "startTime", 1699800000000L,
                                    "endTime", 1699803600000L,
                                    "ipAddress", "192.168.1.1", "userAgent", "Chrome/119"),
                            map("sessionId", "sess-2", "startTime", 1699900000000L,
                                    "endTime", null,
                                    "ipAddress", "10.0.0.1", "userAgent", "Firefox/120")
                    )
            );

            verifyKeyAlignment(schema, data, "User Profile");
        }

        @Test
        @DisplayName("IoT sensor data with nested measurements")
        void testIoTSensorData() {
            Schema readingSchema = SchemaBuilder.record("Reading")
                    .fields()
                    .requiredLong("timestamp")
                    .requiredDouble("value")
                    .optionalString("unit")
                    .requiredInt("quality")
                    .endRecord();

            Schema sensorSchema = SchemaBuilder.record("Sensor")
                    .fields()
                    .requiredString("sensorId")
                    .requiredString("type")
                    .name("readings").type().array().items(readingSchema).noDefault()
                    .endRecord();

            Schema schema = SchemaBuilder.record("DeviceReport")
                    .fields()
                    .requiredString("deviceId")
                    .requiredString("deviceName")
                    .requiredLong("reportTimestamp")
                    .requiredDouble("batteryLevel")
                    .name("sensors").type().array().items(sensorSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "deviceId", "DEV-001",
                    "deviceName", "Weather Station Alpha",
                    "reportTimestamp", 1699900000000L,
                    "batteryLevel", 87.5,
                    "sensors", list(
                            map(
                                    "sensorId", "TEMP-01",
                                    "type", "temperature",
                                    "readings", list(
                                            map("timestamp", 1699899000000L, "value", 22.5,
                                                    "unit", "C", "quality", 100),
                                            map("timestamp", 1699899300000L, "value", 22.7,
                                                    "unit", "C", "quality", 98)
                                    )
                            ),
                            map(
                                    "sensorId", "HUM-01",
                                    "type", "humidity",
                                    "readings", list(
                                            map("timestamp", 1699899000000L, "value", 65.2,
                                                    "unit", "%", "quality", 100)
                                    )
                            )
                    )
            );

            verifyKeyAlignment(schema, data, "IoT Sensor Data");
            verifyKeyAlignmentWithArrayBoundary(schema, data, "IoT Sensor Data");
        }

        @Test
        @DisplayName("Financial transaction with audit trail")
        void testFinancialTransaction() {
            Schema auditEntrySchema = SchemaBuilder.record("AuditEntry")
                    .fields()
                    .requiredLong("timestamp")
                    .requiredString("action")
                    .requiredString("userId")
                    .optionalString("notes")
                    .endRecord();

            Schema accountSchema = SchemaBuilder.record("Account")
                    .fields()
                    .requiredString("accountNumber")
                    .requiredString("accountType")
                    .requiredString("currency")
                    .endRecord();

            Schema schema = SchemaBuilder.record("Transaction")
                    .fields()
                    .requiredString("transactionId")
                    .requiredString("type")
                    .requiredDouble("amount")
                    .requiredString("currency")
                    .requiredLong("timestamp")
                    .requiredString("status")
                    .name("sourceAccount").type(accountSchema).noDefault()
                    .name("destinationAccount").type(accountSchema).noDefault()
                    .name("auditTrail").type().array().items(auditEntrySchema).noDefault()
                    .name("tags").type().array().items().stringType().noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "transactionId", "TXN-999888777",
                    "type", "TRANSFER",
                    "amount", 1500.00,
                    "currency", "USD",
                    "timestamp", 1699900000000L,
                    "status", "COMPLETED",
                    "sourceAccount", map(
                            "accountNumber", "1234567890",
                            "accountType", "CHECKING",
                            "currency", "USD"
                    ),
                    "destinationAccount", map(
                            "accountNumber", "0987654321",
                            "accountType", "SAVINGS",
                            "currency", "USD"
                    ),
                    "auditTrail", list(
                            map("timestamp", 1699899000000L, "action", "INITIATED",
                                    "userId", "user-1", "notes", "Transfer request"),
                            map("timestamp", 1699899500000L, "action", "VALIDATED",
                                    "userId", "system", "notes", null),
                            map("timestamp", 1699900000000L, "action", "COMPLETED",
                                    "userId", "system", "notes", "Funds transferred")
                    ),
                    "tags", list("high-value", "cross-account", "automated")
            );

            verifyKeyAlignment(schema, data, "Financial Transaction");
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
                    .requiredString("taskId")
                    .requiredString("title")
                    .name("status").type(statusEnum).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "taskId", "TASK-001",
                    "title", "Complete report",
                    "status", "ACTIVE"
            );

            verifyKeyAlignment(schema, data, "Simple Enum");
        }

        @Test
        @DisplayName("Array of enums")
        void testArrayOfEnums() {
            Schema dayEnum = SchemaBuilder.enumeration("DayOfWeek")
                    .symbols("MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY",
                            "SATURDAY", "SUNDAY");

            Schema schema = SchemaBuilder.record("Schedule")
                    .fields()
                    .requiredString("name")
                    .name("workDays").type().array().items(dayEnum).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "name", "Work Schedule",
                    "workDays", list("MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY")
            );

            verifyKeyAlignment(schema, data, "Array of Enums");
        }
    }

    // ==================== MAP TESTS ====================

    @Nested
    @DisplayName("Map Tests")
    class MapTests {

        @Test
        @DisplayName("Map schema produces single serialized field (keys unknown at schema time)")
        void testMapSchemaProducesSingleField() {
            // This test documents the EXPECTED behavior - maps become single fields in schema
            // because map keys are only known at runtime, not from schema
            Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));

            Schema schema = SchemaBuilder.record("Config")
                    .fields()
                    .requiredString("name")
                    .name("settings").type(mapSchema).noDefault()
                    .endRecord();

            Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                    schemaFlattener.flattenSchema(schema);

            // Schema flattener correctly identifies 2 fields
            assertThat(flattenedSchema).hasSize(2);
            assertThat(flattenedSchema).containsKey("name");
            assertThat(flattenedSchema).containsKey("settings");

            // settings is marked as a serialized field (since we don't know keys at schema time)
            assertThat(flattenedSchema.get("settings").getDataType())
                    .isEqualTo(GAvroSchemaFlattener.DataType.STRING);
        }
    }

    // ==================== EDGE CASES ====================

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("Single field record")
        void testSingleFieldRecord() {
            Schema schema = SchemaBuilder.record("SingleField")
                    .fields()
                    .requiredString("onlyField")
                    .endRecord();

            Map<String, Object> data = map("onlyField", "value");

            verifyKeyAlignment(schema, data, "Single Field Record");
        }

        @Test
        @DisplayName("Deeply nested single path")
        void testDeeplyNestedSinglePath() {
            Schema level4 = SchemaBuilder.record("Level4")
                    .fields().requiredString("value").endRecord();
            Schema level3 = SchemaBuilder.record("Level3")
                    .fields().name("level4").type(level4).noDefault().endRecord();
            Schema level2 = SchemaBuilder.record("Level2")
                    .fields().name("level3").type(level3).noDefault().endRecord();
            Schema level1 = SchemaBuilder.record("Level1")
                    .fields().name("level2").type(level2).noDefault().endRecord();
            Schema schema = SchemaBuilder.record("Root")
                    .fields().name("level1").type(level1).noDefault().endRecord();

            Map<String, Object> data = map(
                    "level1", map("level2", map("level3", map("level4", map("value", "deep"))))
            );

            verifyKeyAlignment(schema, data, "Deeply Nested Single Path");
        }

        @Test
        @DisplayName("Special characters in values (not keys)")
        void testSpecialCharactersInValues() {
            Schema schema = SchemaBuilder.record("SpecialChars")
                    .fields()
                    .requiredString("normalField")
                    .requiredString("fieldWithQuotes")
                    .requiredString("fieldWithNewlines")
                    .endRecord();

            Map<String, Object> data = map(
                    "normalField", "normal value",
                    "fieldWithQuotes", "He said \"hello\"",
                    "fieldWithNewlines", "line1\nline2\nline3"
            );

            verifyKeyAlignment(schema, data, "Special Characters in Values");
        }

        @Test
        @DisplayName("Unicode values")
        void testUnicodeValues() {
            Schema schema = SchemaBuilder.record("Unicode")
                    .fields()
                    .requiredString("english")
                    .requiredString("japanese")
                    .requiredString("arabic")
                    .requiredString("emoji")
                    .endRecord();

            Map<String, Object> data = map(
                    "english", "Hello World",
                    "japanese", "こんにちは世界",
                    "arabic", "مرحبا بالعالم",
                    "emoji", "👋🌍"
            );

            verifyKeyAlignment(schema, data, "Unicode Values");
        }

        @Test
        @DisplayName("Very long field names")
        void testLongFieldNames() {
            Schema schema = SchemaBuilder.record("LongNames")
                    .fields()
                    .requiredString("shortName")
                    .requiredString("thisIsAVeryLongFieldNameThatMightCauseIssuesInSomeSystems")
                    .requiredInt("anotherExtremelyLongFieldNameForTestingPurposesOnly")
                    .endRecord();

            Map<String, Object> data = map(
                    "shortName", "short",
                    "thisIsAVeryLongFieldNameThatMightCauseIssuesInSomeSystems", "long value",
                    "anotherExtremelyLongFieldNameForTestingPurposesOnly", 42
            );

            verifyKeyAlignment(schema, data, "Long Field Names");
        }

        @Test
        @DisplayName("Array with single element")
        void testSingleElementArray() {
            Schema itemSchema = SchemaBuilder.record("Item")
                    .fields()
                    .requiredString("id")
                    .requiredDouble("value")
                    .endRecord();

            Schema schema = SchemaBuilder.record("Container")
                    .fields()
                    .requiredString("name")
                    .name("items").type().array().items(itemSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "name", "SingleItemContainer",
                    "items", list(map("id", "ITEM-1", "value", 99.99))
            );

            verifyKeyAlignment(schema, data, "Single Element Array");
        }

        @Test
        @DisplayName("All nullable fields with nulls")
        void testAllNullableFieldsWithNulls() {
            Schema schema = SchemaBuilder.record("AllNullable")
                    .fields()
                    .optionalString("field1")
                    .optionalInt("field2")
                    .optionalDouble("field3")
                    .optionalBoolean("field4")
                    .endRecord();

            Map<String, Object> data = map(
                    "field1", null,
                    "field2", null,
                    "field3", null,
                    "field4", null
            );

            verifyKeyAlignment(schema, data, "All Nullable Fields with Nulls");
        }
    }

    // ==================== REGRESSION TESTS ====================

    @Nested
    @DisplayName("Regression Tests")
    class RegressionTests {

        @Test
        @DisplayName("JPMorgan electronic delivery structure")
        void testJPMorganElectronicDelivery() {
            Schema electronicDeliverySchema = SchemaBuilder.record("ElectronicDelivery")
                    .fields()
                    .requiredBoolean("electronicDeliveryConsentIndicator")
                    .endRecord();

            Schema accountSchema = SchemaBuilder.record("Account")
                    .fields()
                    .requiredString("signingOrderCode")
                    .name("electronicDelivery").type(electronicDeliverySchema).noDefault()
                    .endRecord();

            Schema schema = SchemaBuilder.record("Root")
                    .fields()
                    .name("accounts").type().array().items(accountSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "accounts", list(
                            map("signingOrderCode", "10721557",
                                    "electronicDelivery",
                                    map("electronicDeliveryConsentIndicator", true)),
                            map("signingOrderCode", "10721558",
                                    "electronicDelivery",
                                    map("electronicDeliveryConsentIndicator", false))
                    )
            );

            verifyKeyAlignment(schema, data, "JPMorgan Electronic Delivery");
            verifyKeyAlignmentWithArrayBoundary(schema, data, "JPMorgan Electronic Delivery");
        }

        @Test
        @DisplayName("Complex order from previous stress tests")
        void testComplexOrderStructure() {
            // Address
            Schema addressSchema = SchemaBuilder.record("Address")
                    .fields()
                    .requiredString("street")
                    .requiredString("city")
                    .requiredString("state")
                    .requiredString("zip")
                    .optionalString("country")
                    .endRecord();

            // Tracking Event
            Schema trackingEventSchema = SchemaBuilder.record("TrackingEvent")
                    .fields()
                    .requiredLong("timestamp")
                    .requiredString("status")
                    .optionalString("location")
                    .endRecord();

            // Shipment
            Schema shipmentSchema = SchemaBuilder.record("Shipment")
                    .fields()
                    .requiredString("carrier")
                    .requiredString("trackingNumber")
                    .name("events").type().array().items(trackingEventSchema).noDefault()
                    .endRecord();

            // Line Item
            Schema lineItemSchema = SchemaBuilder.record("LineItem")
                    .fields()
                    .requiredString("productId")
                    .requiredString("productName")
                    .requiredInt("quantity")
                    .requiredDouble("unitPrice")
                    .endRecord();

            // Order
            Schema schema = SchemaBuilder.record("ComplexOrder")
                    .fields()
                    .requiredString("orderId")
                    .requiredString("customerEmail")
                    .requiredLong("orderDate")
                    .name("shippingAddress").type(addressSchema).noDefault()
                    .name("items").type().array().items(lineItemSchema).noDefault()
                    .name("shipments").type().array().items(shipmentSchema).noDefault()
                    .endRecord();

            Map<String, Object> data = map(
                    "orderId", "ORD-2024-001",
                    "customerEmail", "customer@example.com",
                    "orderDate", 1699900000000L,
                    "shippingAddress", map(
                            "street", "123 Main St",
                            "city", "Anytown",
                            "state", "CA",
                            "zip", "90210",
                            "country", "USA"
                    ),
                    "items", list(
                            map("productId", "PROD-A", "productName", "Widget A",
                                    "quantity", 2, "unitPrice", 19.99),
                            map("productId", "PROD-B", "productName", "Widget B",
                                    "quantity", 1, "unitPrice", 29.99)
                    ),
                    "shipments", list(
                            map(
                                    "carrier", "UPS",
                                    "trackingNumber", "1Z999AA10123456784",
                                    "events", list(
                                            map("timestamp", 1699900000000L,
                                                    "status", "SHIPPED",
                                                    "location", "Warehouse"),
                                            map("timestamp", 1699986400000L,
                                                    "status", "IN_TRANSIT",
                                                    "location", "Chicago Hub")
                                    )
                            )
                    )
            );

            verifyKeyAlignment(schema, data, "Complex Order Structure");
            verifyKeyAlignmentWithArrayBoundary(schema, data, "Complex Order Structure");
        }
    }

    // ==================== TYPE VERIFICATION TESTS ====================

    @Nested
    @DisplayName("Type Application Tests")
    class TypeApplicationTests {

        @Test
        @DisplayName("Verify applyTypes produces correct types")
        void testApplyTypesCorrectTypes() {
            Schema schema = SchemaBuilder.record("TypeTest")
                    .fields()
                    .requiredInt("intField")
                    .requiredLong("longField")
                    .requiredDouble("doubleField")
                    .requiredBoolean("boolField")
                    .requiredString("stringField")
                    .endRecord();

            Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                    schemaFlattener.flattenSchema(schema);

            // Simulate string data (as might come from CSV or JSON parsing)
            Map<String, Object> flattenedData = map(
                    "intField", "42",
                    "longField", "9876543210",
                    "doubleField", "3.14159",
                    "boolField", "true",
                    "stringField", "hello"
            );

            Map<String, Object> typedData = schemaFlattener.applyTypes(flattenedData, flattenedSchema);

            assertThat(typedData.get("intField")).isEqualTo(42);
            assertThat(typedData.get("intField")).isInstanceOf(Integer.class);

            assertThat(typedData.get("longField")).isEqualTo(9876543210L);
            assertThat(typedData.get("longField")).isInstanceOf(Long.class);

            assertThat((Double) typedData.get("doubleField")).isCloseTo(3.14159, within(0.00001));
            assertThat(typedData.get("doubleField")).isInstanceOf(Double.class);

            assertThat(typedData.get("boolField")).isEqualTo(true);
            assertThat(typedData.get("boolField")).isInstanceOf(Boolean.class);

            assertThat(typedData.get("stringField")).isEqualTo("hello");
            assertThat(typedData.get("stringField")).isInstanceOf(String.class);
        }

        @Test
        @DisplayName("Verify array element types are converted")
        void testArrayElementTypeConversion() {
            Schema schema = SchemaBuilder.record("ArrayTypes")
                    .fields()
                    .name("intArray").type().array().items().intType().noDefault()
                    .name("doubleArray").type().array().items().doubleType().noDefault()
                    .endRecord();

            Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                    schemaFlattener.flattenSchema(schema);

            // Data with string representations
            Map<String, Object> flattenedData = map(
                    "intArray", "[\"1\",\"2\",\"3\"]",
                    "doubleArray", "[\"1.1\",\"2.2\",\"3.3\"]"
            );

            Map<String, Object> typedData = schemaFlattener.applyTypes(flattenedData, flattenedSchema);

            // Should be re-serialized as JSON with correct types
            assertThat(typedData.get("intArray")).isEqualTo("[1,2,3]");
            assertThat(typedData.get("doubleArray")).isEqualTo("[1.1,2.2,3.3]");
        }
    }
}
