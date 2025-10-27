import io.github.pierce.AvroReconstructor
import io.github.pierce.MapFlattener
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.LogicalTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite demonstrating perfect reconstruction
 */
public class AvroReconstructorTest {

    @Test
    @DisplayName("Simple record reconstruction")
    public void testSimpleRecordReconstruction() {
        // Create schema
        Schema schema = SchemaBuilder.record("User")
                .fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredDouble("balance")
                .endRecord();

        // Create original data
        Map<String, Object> original = new LinkedHashMap<>();
        original.put("id", 123);
        original.put("name", "Alice");
        original.put("balance", 456.78);

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());
        assertTrue(verification.isPerfect(), "Reconstruction should be perfect");
    }

    @Test
    @DisplayName("Nested record reconstruction")
    public void testNestedRecordReconstruction() {
        // Create nested schema
        Schema addressSchema = SchemaBuilder.record("Address")
                .fields()
                .requiredString("street")
                .requiredString("city")
                .requiredInt("zipCode")
                .endRecord();

        Schema schema = SchemaBuilder.record("Person")
                .fields()
                .requiredString("name")
                .name("address").type(addressSchema).noDefault()
                .endRecord();

        // Create original data
        Map<String, Object> address = new LinkedHashMap<>();
        address.put("street", "123 Main St");
        address.put("city", "Boston");
        address.put("zipCode", 12345);

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("name", "Bob");
        original.put("address", address);

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("Flattened: " + flattened);

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        System.out.println("Reconstructed: " + reconstructed);

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());
        assertTrue(verification.isPerfect(), "Nested reconstruction should be perfect");
    }

    @Test
    @DisplayName("Array of primitives reconstruction")
    public void testArrayOfPrimitivesReconstruction() {
        Schema schema = SchemaBuilder.record("Test")
                .fields()
                .name("scores").type().array().items().intType().noDefault()
                .endRecord();

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("scores", Arrays.asList(10, 20, 30, 40));

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("Flattened: " + flattened);

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        System.out.println("Reconstructed: " + reconstructed);

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());
        assertTrue(verification.isPerfect(), "Array reconstruction should be perfect");
    }

    @Test
    @DisplayName("Array of records reconstruction - THE CRITICAL TEST")
    public void testArrayOfRecordsReconstruction() {
        // This is the test case from the white paper!
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

        // Create original data
        Map<String, Object> electronicDelivery = new LinkedHashMap<>();
        electronicDelivery.put("electronicDeliveryConsentIndicator", true);

        Map<String, Object> account = new LinkedHashMap<>();
        account.put("signingOrderCode", "10721557");
        account.put("electronicDelivery", electronicDelivery);

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("accounts", Collections.singletonList(account));

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("Flattened: " + flattened);

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        System.out.println("Reconstructed: " + reconstructed);

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());
        assertTrue(verification.isPerfect(),
                "Complex nested array reconstruction should be perfect");
    }

    @Test
    @DisplayName("Nullable fields reconstruction")
    public void testNullableFieldsReconstruction() {
        Schema schema = SchemaBuilder.record("User")
                .fields()
                .requiredInt("id")
                .optionalString("nickname")
                .optionalInt("age")
                .endRecord();

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("id", 999);
        original.put("nickname", null);
        original.put("age", 25);

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());
        assertTrue(verification.isPerfect(), "Nullable fields should reconstruct perfectly");
    }

    @Test
    @DisplayName("Multiple array formats compatibility")
    public void testMultipleArrayFormats() {
        Schema schema = SchemaBuilder.record("Test")
                .fields()
                .name("values").type().array().items().stringType().noDefault()
                .endRecord();

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("values", Arrays.asList("alpha", "beta", "gamma"));

        // Test each format
        for (MapFlattener.ArraySerializationFormat format : MapFlattener.ArraySerializationFormat.values()) {

            System.out.println("\nTesting format: " + format);

            // Flatten with format
            MapFlattener flattener = MapFlattener.builder()
                    .arrayFormat(format)
                    .build();
            Map<String, Object> flattened = flattener.flatten(original);

            System.out.println("Flattened: " + flattened);

            // Reconstruct with matching format
            AvroReconstructor reconstructor = AvroReconstructor.builder()
                    .arrayFormat(AvroReconstructor.ArraySerializationFormat.valueOf(format.name()))
                    .build();
            Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

            System.out.println("Reconstructed: " + reconstructed);

            // Verify
            AvroReconstructor.ReconstructionVerification verification =
                    reconstructor.verifyReconstruction(original, reconstructed, schema);

            assertTrue(verification.isPerfect(),
                    "Format " + format + " should reconstruct perfectly");
        }
    }

    @Test
    @DisplayName("Deep nesting reconstruction")
    public void testDeepNestingReconstruction() {
        // Create deeply nested schema
        Schema level3Schema = SchemaBuilder.record("Level3")
                .fields()
                .requiredString("value")
                .endRecord();

        Schema level2Schema = SchemaBuilder.record("Level2")
                .fields()
                .name("level3").type(level3Schema).noDefault()
                .endRecord();

        Schema level1Schema = SchemaBuilder.record("Level1")
                .fields()
                .name("level2").type(level2Schema).noDefault()
                .endRecord();

        Schema schema = SchemaBuilder.record("Root")
                .fields()
                .name("level1").type(level1Schema).noDefault()
                .endRecord();

        // Create deeply nested data
        Map<String, Object> level3 = new LinkedHashMap<>();
        level3.put("value", "deep");

        Map<String, Object> level2 = new LinkedHashMap<>();
        level2.put("level3", level3);

        Map<String, Object> level1 = new LinkedHashMap<>();
        level1.put("level2", level2);

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("level1", level1);

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("Flattened: " + flattened);

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        System.out.println("Reconstructed: " + reconstructed);

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());
        assertTrue(verification.isPerfect(), "Deep nesting should reconstruct perfectly");
    }

    @Test
    @DisplayName("Round-trip test: flatten -> reconstruct -> flatten")
    public void testRoundTripFlatten() {
        Schema schema = SchemaBuilder.record("User")
                .fields()
                .requiredInt("id")
                .requiredString("name")
                .name("tags").type().array().items().stringType().noDefault()
                .endRecord();

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("id", 42);
        original.put("name", "Charlie");
        original.put("tags", Arrays.asList("admin", "user", "developer"));

        // First flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened1 = flattener.flatten(original);

        System.out.println("First flatten: " + flattened1);

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened1, schema);

        System.out.println("Reconstructed: " + reconstructed);

        // Flatten again
        Map<String, Object> flattened2 = flattener.flatten(reconstructed);

        System.out.println("Second flatten: " + flattened2);

        // Compare both flattened versions
        AvroReconstructor.ComparisonResult comparison =
                reconstructor.compareFlattenedMaps(flattened1, flattened2);

        System.out.println(comparison);
        assertTrue(comparison.isIdentical(),
                "Round-trip flatten should produce identical results");
    }

    @Test
    @DisplayName("Empty structures reconstruction")
    public void testEmptyStructuresReconstruction() {
        Schema schema = SchemaBuilder.record("Test")
                .fields()
                .name("emptyArray").type().array().items().stringType().noDefault()
                .requiredString("name")
                .endRecord();

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("emptyArray", Collections.emptyList());
        original.put("name", "test");

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("Flattened: " + flattened);

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        System.out.println("Reconstructed: " + reconstructed);

        // Verify
        assertNotNull(reconstructed.get("emptyArray"));
        assertTrue(((List<?>) reconstructed.get("emptyArray")).isEmpty());
        assertEquals("test", reconstructed.get("name"));
    }
}