package io.github.pierce;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Diagnostic tests for deep nesting issue.
 *
 * <p>Ported verbatim from {@code src/test/groovy/DeepNestingDiagnosticTest.groovy}
 * (4 {@code @Test} methods in, 4 out). The {@code System.out} tracing is the point of these
 * tests — they exist to make a reconstruction failure readable — so it is preserved as-is.</p>
 */
class DeepNestingDiagnosticTest {

    @Test
    @DisplayName("Diagnostic: 3 levels - Array → Record → Array")
    void testThreeLevels() {
        // Level 3 (deepest)
        Schema level3Schema = SchemaBuilder.record("Level3")
                .fields()
                .requiredString("value")
                .endRecord();

        // Level 2 - has array of level 3
        Schema level2Schema = SchemaBuilder.record("Level2")
                .fields()
                .name("items").type().array().items(level3Schema).noDefault()
                .requiredString("name")
                .endRecord();

        // Level 1 (root) - has array of level 2
        Schema schema = SchemaBuilder.record("Level1")
                .fields()
                .name("data").type().array().items(level2Schema).noDefault()
                .endRecord();

        // Create data
        Map<String, Object> level3_1 = new LinkedHashMap<>();
        level3_1.put("value", "A");

        Map<String, Object> level3_2 = new LinkedHashMap<>();
        level3_2.put("value", "B");

        Map<String, Object> level2 = new LinkedHashMap<>();
        level2.put("items", Arrays.asList(level3_1, level3_2));
        level2.put("name", "Test");

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("data", Collections.singletonList(level2));

        System.out.println("=== THREE LEVELS: Array → Record → Array ===");
        System.out.println("Original structure:");
        System.out.println("  data[0].name: " + level2.get("name"));
        System.out.println("  data[0].items: " + level2.get("items"));

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("\nFlattened:");
        flattened.forEach((k, v) -> System.out.println("  " + k + ": " + v));

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        System.out.println("\nReconstructed:");
        System.out.println("  data: " + reconstructed.get("data"));

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println("\n" + verification.getReport());
        assertThat(verification.isPerfect()).isTrue();
    }

    @Test
    @DisplayName("Diagnostic: 4 levels - Record → Array → Record → Array")
    void testFourLevels() {
        // Level 4 (deepest)
        Schema level4Schema = SchemaBuilder.record("Level4")
                .fields()
                .requiredString("value")
                .endRecord();

        // Level 3 - has array of level 4
        Schema level3Schema = SchemaBuilder.record("Level3")
                .fields()
                .name("items").type().array().items(level4Schema).noDefault()
                .endRecord();

        // Level 2 - has array of level 3
        Schema level2Schema = SchemaBuilder.record("Level2")
                .fields()
                .name("containers").type().array().items(level3Schema).noDefault()
                .endRecord();

        // Level 1 (root) - has level 2 record
        Schema schema = SchemaBuilder.record("Level1")
                .fields()
                .name("root").type(level2Schema).noDefault()
                .endRecord();

        // Create data
        Map<String, Object> level4_1 = new LinkedHashMap<>();
        level4_1.put("value", "Deep Value 1");

        Map<String, Object> level4_2 = new LinkedHashMap<>();
        level4_2.put("value", "Deep Value 2");

        Map<String, Object> level3 = new LinkedHashMap<>();
        level3.put("items", Arrays.asList(level4_1, level4_2));

        Map<String, Object> level2 = new LinkedHashMap<>();
        level2.put("containers", Collections.singletonList(level3));

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("root", level2);

        System.out.println("=== FOUR LEVELS: Record → Array → Record → Array ===");
        System.out.println("Original structure:");
        System.out.println("  root: " + original.get("root"));

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("\nFlattened:");
        flattened.forEach((k, v) -> System.out.println("  " + k + ": " + v));

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        System.out.println("\nReconstructed:");
        System.out.println("  root: " + reconstructed.get("root"));

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println("\n" + verification.getReport());
        assertThat(verification.isPerfect()).isTrue();
    }

    @Test
    @DisplayName("Diagnostic: Simplified 5 levels like testDeepNesting")
    void testFiveLevelsSimplified() {
        // Level 5 (deepest)
        Schema level5Schema = SchemaBuilder.record("Level5")
                .fields()
                .requiredString("deepValue")
                .requiredInt("depth")
                .endRecord();

        // Level 4 - array of level 5 + string field
        Schema level4Schema = SchemaBuilder.record("Level4")
                .fields()
                .name("items").type().array().items(level5Schema).noDefault()
                .requiredString("level4Name")
                .endRecord();

        // Level 3 - nested record + string field
        Schema level3Schema = SchemaBuilder.record("Level3")
                .fields()
                .name("data").type(level4Schema).noDefault()
                .requiredString("level3Name")
                .endRecord();

        // Level 2 - array of level 3
        Schema level2Schema = SchemaBuilder.record("Level2")
                .fields()
                .name("containers").type().array().items(level3Schema).noDefault()
                .endRecord();

        // Level 1 (root)
        Schema schema = SchemaBuilder.record("Level1")
                .fields()
                .name("root").type(level2Schema).noDefault()
                .requiredString("rootId")
                .endRecord();

        // Create deeply nested data (simplified - just 1 element at each array level)
        Map<String, Object> level5_1 = new LinkedHashMap<>();
        level5_1.put("deepValue", "I am at level 5!");
        level5_1.put("depth", 5);

        Map<String, Object> level4 = new LinkedHashMap<>();
        level4.put("items", Collections.singletonList(level5_1));  // Just 1 item
        level4.put("level4Name", "Fourth Level");

        Map<String, Object> level3 = new LinkedHashMap<>();
        level3.put("data", level4);
        level3.put("level3Name", "Third Level");

        Map<String, Object> level2 = new LinkedHashMap<>();
        level2.put("containers", Collections.singletonList(level3));  // Just 1 container

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("root", level2);
        original.put("rootId", "ROOT-001");

        System.out.println("=== FIVE LEVELS (SIMPLIFIED) ===");
        System.out.println("Original structure:");
        System.out.println("  rootId: " + original.get("rootId"));
        System.out.println("  root.containers[0].level3Name: " + level3.get("level3Name"));
        System.out.println("  root.containers[0].data.level4Name: " + level4.get("level4Name"));
        System.out.println("  root.containers[0].data.items[0]: " + level5_1);

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("\nFlattened:");
        flattened.forEach((k, v) -> System.out.println("  " + k + ": " + v));

        // Analyze the structure
        System.out.println("\n=== ANALYZING FLATTENED STRUCTURE ===");
        Object itemsDeepValue = flattened.get("root_containers_data_items_deepValue");
        Object itemsDepth = flattened.get("root_containers_data_items_depth");

        System.out.println("root_containers_data_items_deepValue:");
        System.out.println("  Type: " + (itemsDeepValue == null ? "null" : itemsDeepValue.getClass().getName()));
        System.out.println("  Value: " + itemsDeepValue);

        System.out.println("root_containers_data_items_depth:");
        System.out.println("  Type: " + (itemsDepth == null ? "null" : itemsDepth.getClass().getName()));
        System.out.println("  Value: " + itemsDepth);

        // Reconstruct
        System.out.println("\n=== RECONSTRUCTION ===");
        try {
            AvroReconstructor reconstructor = AvroReconstructor.builder()
                    .strictValidation(false)
                    .build();
            Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

            System.out.println("Successfully reconstructed!");
            System.out.println("Reconstructed structure:");
            System.out.println("  rootId: " + reconstructed.get("rootId"));
            System.out.println("  root: " + reconstructed.get("root"));

            // Verify
            AvroReconstructor.ReconstructionVerification verification =
                    reconstructor.verifyReconstruction(original, reconstructed, schema);

            System.out.println("\n" + verification.getReport());
            assertThat(verification.isPerfect()).isTrue();
        } catch (RuntimeException e) {
            // The Groovy original caught Exception and rethrew it. AssertionError is not an
            // Exception, so the assertThat above was never caught there either — the catch only
            // ever saw reconstruction blowing up, and it always rethrew.
            System.out.println("Reconstruction failed!");
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    @DisplayName("Diagnostic: Full testDeepNesting with 2 items at level 5")
    void testFiveLevelsFull() {
        // This is the EXACT structure from testDeepNesting
        Schema level5Schema = SchemaBuilder.record("Level5")
                .fields()
                .requiredString("deepValue")
                .requiredInt("depth")
                .endRecord();

        Schema level4Schema = SchemaBuilder.record("Level4")
                .fields()
                .name("items").type().array().items(level5Schema).noDefault()
                .requiredString("level4Name")
                .endRecord();

        Schema level3Schema = SchemaBuilder.record("Level3")
                .fields()
                .name("data").type(level4Schema).noDefault()
                .requiredString("level3Name")
                .endRecord();

        Schema level2Schema = SchemaBuilder.record("Level2")
                .fields()
                .name("containers").type().array().items(level3Schema).noDefault()
                .endRecord();

        Schema schema = SchemaBuilder.record("Level1")
                .fields()
                .name("root").type(level2Schema).noDefault()
                .requiredString("rootId")
                .endRecord();

        // Create with 2 items at level 5 (like the original test)
        Map<String, Object> level5_1 = new LinkedHashMap<>();
        level5_1.put("deepValue", "I am at level 5!");
        level5_1.put("depth", 5);

        Map<String, Object> level5_2 = new LinkedHashMap<>();
        level5_2.put("deepValue", "Me too!");
        level5_2.put("depth", 5);

        Map<String, Object> level4 = new LinkedHashMap<>();
        level4.put("items", Arrays.asList(level5_1, level5_2));  // 2 items
        level4.put("level4Name", "Fourth Level");

        Map<String, Object> level3 = new LinkedHashMap<>();
        level3.put("data", level4);
        level3.put("level3Name", "Third Level");

        Map<String, Object> level2 = new LinkedHashMap<>();
        level2.put("containers", Collections.singletonList(level3));

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("root", level2);
        original.put("rootId", "ROOT-001");

        System.out.println("=== FIVE LEVELS (FULL - 2 ITEMS AT DEEPEST LEVEL) ===");
        System.out.println("Original:");
        System.out.println("  root.containers[0].data.items: " + level4.get("items"));

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("\nFlattened:");
        flattened.forEach((k, v) -> System.out.println("  " + k + ": " + v));

        // Reconstruct
        System.out.println("\n=== RECONSTRUCTION ===");
        try {
            AvroReconstructor reconstructor = AvroReconstructor.builder()
                    .strictValidation(false)
                    .build();
            Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

            System.out.println("Successfully reconstructed!");

            // Verify
            AvroReconstructor.ReconstructionVerification verification =
                    reconstructor.verifyReconstruction(original, reconstructed, schema);

            System.out.println("\n" + verification.getReport());
            assertThat(verification.isPerfect()).isTrue();
        } catch (RuntimeException e) {
            System.out.println("Reconstruction failed!");
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
}
