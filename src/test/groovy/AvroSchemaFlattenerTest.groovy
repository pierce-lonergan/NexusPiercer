package io.github.pierce.avro;

import io.github.pierce.MapFlattener
import io.github.pierce.GAvroSchemaFlattener
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.*;

public class AvroSchemaFlattenerTest {

    @Test
    public void testSimpleRecordFlattening() {
        // Create schema: {id: int, name: string}
        Schema schema = SchemaBuilder.record("User")
                .fields()
                .requiredInt("id")
                .requiredString("name")
                .endRecord();

        GAvroSchemaFlattener flattener = new GAvroSchemaFlattener();
        Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                flattener.flattenSchema(schema);

        assertEquals(2, flattenedSchema.size());
        assertEquals(GAvroSchemaFlattener.DataType.INT,
                flattenedSchema.get("id").getDataType());
        assertEquals(GAvroSchemaFlattener.DataType.STRING,
                flattenedSchema.get("name").getDataType());
    }

    @Test
    public void testNestedRecordFlattening() {
        // Create schema: {user: {name: string, address: {city: string}}}
        Schema addressSchema = SchemaBuilder.record("Address")
                .fields()
                .requiredString("city")
                .requiredString("street")
                .endRecord();

        Schema userSchema = SchemaBuilder.record("User")
                .fields()
                .requiredString("name")
                .name("address").type(addressSchema).noDefault()
                .endRecord();

        GAvroSchemaFlattener flattener = new GAvroSchemaFlattener();
        Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                flattener.flattenSchema(userSchema);

        assertTrue(flattenedSchema.containsKey("name"));
        assertTrue(flattenedSchema.containsKey("address_city"));
        assertTrue(flattenedSchema.containsKey("address_street"));
    }

    @Test
    public void testArrayOfPrimitivesFlattening() {
        // Create schema: {scores: array<int>}
        Schema schema = SchemaBuilder.record("Test")
                .fields()
                .name("scores").type().array().items().intType().noDefault()
                .endRecord();

        GAvroSchemaFlattener flattener = new GAvroSchemaFlattener();
        Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                flattener.flattenSchema(schema);

        GAvroSchemaFlattener.FlattenedFieldType scoresType = flattenedSchema.get("scores");
        assertNotNull(scoresType);
        assertTrue(scoresType.isArraySerialized());
        assertEquals(GAvroSchemaFlattener.DataType.STRING, scoresType.getDataType());
        assertEquals(GAvroSchemaFlattener.DataType.INT, scoresType.getArrayElementType());
    }

    @Test
    public void testArrayOfRecordsFlattening() {
        // Create schema: {accounts: array<{id: string, balance: double}>}
        Schema accountSchema = SchemaBuilder.record("Account")
                .fields()
                .requiredString("id")
                .requiredDouble("balance")
                .endRecord();

        Schema schema = SchemaBuilder.record("User")
                .fields()
                .name("accounts").type().array().items(accountSchema).noDefault()
                .endRecord();

        GAvroSchemaFlattener flattener = new GAvroSchemaFlattener();
        Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                flattener.flattenSchema(schema);

        // Array of records creates separate fields for each record field
        assertTrue(flattenedSchema.containsKey("accounts_id"));
        assertTrue(flattenedSchema.containsKey("accounts_balance"));

        // Both should be array serialized
        assertTrue(flattenedSchema.get("accounts_id").isArraySerialized());
        assertTrue(flattenedSchema.get("accounts_balance").isArraySerialized());
    }

    @Test
    public void testNullableFieldFlattening() {
        // Create schema with nullable field
        Schema schema = SchemaBuilder.record("User")
                .fields()
                .requiredInt("id")
                .optionalString("nickname") // nullable
                .endRecord();

        GAvroSchemaFlattener flattener = new GAvroSchemaFlattener();
        Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                flattener.flattenSchema(schema);

        assertFalse(flattenedSchema.get("id").isNullable());
        assertTrue(flattenedSchema.get("nickname").isNullable());
    }

    @Test
    public void testApplyTypesToFlattenedData() {
        // Create schema
        Schema schema = SchemaBuilder.record("User")
                .fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredDouble("balance")
                .endRecord();

        // Flatten schema
        GAvroSchemaFlattener flattener = new GAvroSchemaFlattener();
        Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                flattener.flattenSchema(schema);

        // Create flattened data (simulating MapFlattener output)
        Map<String, Object> flattenedData = new HashMap<>();
        flattenedData.put("id", "123"); // String instead of int
        flattenedData.put("name", "Alice");
        flattenedData.put("balance", "456.78"); // String instead of double

        // Apply types
        Map<String, Object> typedData = flattener.applyTypes(flattenedData, flattenedSchema);

        // Verify types are corrected
        assertEquals(123, typedData.get("id"));
        assertEquals("Alice", typedData.get("name"));
        assertEquals(456.78, typedData.get("balance"));
    }

    @Test
    public void testApplyTypesToArrayFields() {
        // Create schema with array
        Schema schema = SchemaBuilder.record("Test")
                .fields()
                .name("scores").type().array().items().intType().noDefault()
                .endRecord();

        GAvroSchemaFlattener flattener = new GAvroSchemaFlattener();
        Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                flattener.flattenSchema(schema);

        // Flattened data with serialized array (from MapFlattener)
        Map<String, Object> flattenedData = new HashMap<>();
        flattenedData.put("scores", "[\"1\",\"2\",\"3\"]"); // Strings in JSON

        // Apply types
        Map<String, Object> typedData = flattener.applyTypes(flattenedData, flattenedSchema);

        // Should parse and convert to correct types, then re-serialize
        String result = (String) typedData.get("scores");
        assertEquals("[1,2,3]", result); // Integers, not strings
    }

    @Test
    public void testEndToEndWithMapFlattener() {
        // 1. Create Avro schema
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

        // 2. Flatten schema (cache this)
        GAvroSchemaFlattener avroFlattener = new GAvroSchemaFlattener();
        Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                avroFlattener.flattenSchema(schema);

        // 3. Create JSON data
        Map<String, Object> electronicDelivery = new HashMap<>();
        electronicDelivery.put("electronicDeliveryConsentIndicator", true);

        Map<String, Object> account = new HashMap<>();
        account.put("signingOrderCode", "10721557");
        account.put("electronicDelivery", electronicDelivery);

        Map<String, Object> jsonData = new HashMap<>();
        jsonData.put("accounts", Collections.singletonList(account));

        // 4. Flatten data with MapFlattener
        MapFlattener dataFlattener = new MapFlattener();
        Map<String, Object> flattenedData = dataFlattener.flatten(jsonData);

        // 5. Apply types from schema
        Map<String, Object> typedData = avroFlattener.applyTypes(flattenedData, flattenedSchema);

        // 6. Verify results
        assertTrue(typedData.containsKey("accounts_signingOrderCode"));
        assertTrue(typedData.containsKey("accounts_electronicDelivery_electronicDeliveryConsentIndicator"));

        // Both should be properly typed arrays
        String signingCodes = (String) typedData.get("accounts_signingOrderCode");
        String indicators = (String) typedData.get("accounts_electronicDelivery_electronicDeliveryConsentIndicator");

        assertEquals("[\"10721557\"]", signingCodes);
        assertEquals("[true]", indicators); // Boolean, not string "true"
    }

    @Test
    public void testDeepNestedArrayFlattening() {
        // Nested structure in array
        Schema innerSchema = SchemaBuilder.record("Inner")
                .fields()
                .requiredInt("value")
                .endRecord();

        Schema outerSchema = SchemaBuilder.record("Outer")
                .fields()
                .name("items").type().array().items(innerSchema).noDefault()
                .endRecord();

        Schema schema = SchemaBuilder.record("Root")
                .fields()
                .name("data").type().array().items(outerSchema).noDefault()
                .endRecord();

        GAvroSchemaFlattener flattener = new GAvroSchemaFlattener();
        Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                flattener.flattenSchema(schema);

        // Should have flattened nested array structure
        assertTrue(flattenedSchema.containsKey("data_items_value"));
    }
}