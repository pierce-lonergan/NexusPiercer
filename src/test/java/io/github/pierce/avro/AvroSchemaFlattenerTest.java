package io.github.pierce.avro;

import io.github.pierce.GAvroSchemaFlattener;
import io.github.pierce.MapFlattener;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ported from src/test/groovy/AvroSchemaFlattenerTest.groovy.
 *
 * <p>Package {@code io.github.pierce.avro} is preserved deliberately. It is a distinct class from
 * {@code io.github.pierce.avroTesting.AvroSchemaFlattenerTest}, which tests a different class
 * ({@code AvroSchemaFlattener}); this one tests {@code GAvroSchemaFlattener}. The simple-name
 * collision is intentional and must not be "resolved".</p>
 */
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

        assertThat(flattenedSchema).hasSize(2);
        assertThat(flattenedSchema.get("id").getDataType())
                .isEqualTo(GAvroSchemaFlattener.DataType.INT);
        assertThat(flattenedSchema.get("name").getDataType())
                .isEqualTo(GAvroSchemaFlattener.DataType.STRING);
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

        assertThat(flattenedSchema).containsKey("name");
        assertThat(flattenedSchema).containsKey("address_city");
        assertThat(flattenedSchema).containsKey("address_street");
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
        assertThat(scoresType).isNotNull();
        assertThat(scoresType.isArraySerialized()).isTrue();
        assertThat(scoresType.getDataType())
                .isEqualTo(GAvroSchemaFlattener.DataType.STRING);
        assertThat(scoresType.getArrayElementType())
                .isEqualTo(GAvroSchemaFlattener.DataType.INT);
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
        assertThat(flattenedSchema).containsKey("accounts_id");
        assertThat(flattenedSchema).containsKey("accounts_balance");

        // Both should be array serialized
        assertThat(flattenedSchema.get("accounts_id").isArraySerialized()).isTrue();
        assertThat(flattenedSchema.get("accounts_balance").isArraySerialized()).isTrue();
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

        assertThat(flattenedSchema.get("id").isNullable()).isFalse();
        assertThat(flattenedSchema.get("nickname").isNullable()).isTrue();
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
        assertThat(typedData.get("id")).isEqualTo(123);
        assertThat(typedData.get("name")).isEqualTo("Alice");
        assertThat(typedData.get("balance")).isEqualTo(456.78);
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
        assertThat(result).isEqualTo("[1,2,3]"); // Integers, not strings
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
        assertThat(typedData).containsKey("accounts_signingOrderCode");
        assertThat(typedData)
                .containsKey("accounts_electronicDelivery_electronicDeliveryConsentIndicator");

        // Both should be properly typed arrays
        String signingCodes = (String) typedData.get("accounts_signingOrderCode");
        String indicators = (String) typedData
                .get("accounts_electronicDelivery_electronicDeliveryConsentIndicator");

        assertThat(signingCodes).isEqualTo("[\"10721557\"]");
        assertThat(indicators).isEqualTo("[true]"); // Boolean, not string "true"
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
        assertThat(flattenedSchema).containsKey("data_items_value");
    }
}
