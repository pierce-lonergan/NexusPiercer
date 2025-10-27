package io.github.pierce

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import io.github.pierce.AvroReconstructor
import io.github.pierce.MapFlattener
import java.util.*

import static org.junit.jupiter.api.Assertions.*

/**
 * Diagnostic tests for deep nesting issue
 */
class DeepNestingDiagnosticTest {

    @Test
    @DisplayName("Diagnostic: 3 levels - Array → Record → Array")
    void testThreeLevels() {
        // Level 3 (deepest)
        Schema level3Schema = SchemaBuilder.record("Level3")
                .fields()
                .requiredString("value")
                .endRecord()

        // Level 2 - has array of level 3
        Schema level2Schema = SchemaBuilder.record("Level2")
                .fields()
                .name("items").type().array().items(level3Schema).noDefault()
                .requiredString("name")
                .endRecord()

        // Level 1 (root) - has array of level 2
        Schema schema = SchemaBuilder.record("Level1")
                .fields()
                .name("data").type().array().items(level2Schema).noDefault()
                .endRecord()

        // Create data
        Map<String, Object> level3_1 = new LinkedHashMap<>()
        level3_1.put("value", "A")

        Map<String, Object> level3_2 = new LinkedHashMap<>()
        level3_2.put("value", "B")

        Map<String, Object> level2 = new LinkedHashMap<>()
        level2.put("items", Arrays.asList(level3_1, level3_2))
        level2.put("name", "Test")

        Map<String, Object> original = new LinkedHashMap<>()
        original.put("data", Collections.singletonList(level2))

        println("=== THREE LEVELS: Array → Record → Array ===")
        println("Original structure:")
        println("  data[0].name: ${level2.get('name')}")
        println("  data[0].items: ${level2.get('items')}")

        // Flatten
        MapFlattener flattener = new MapFlattener()
        Map<String, Object> flattened = flattener.flatten(original)

        println("\nFlattened:")
        flattened.each { k, v ->
            println("  $k: $v")
        }

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build()
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema)

        println("\nReconstructed:")
        println("  data: ${reconstructed.get('data')}")

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema)

        println("\n" + verification.getReport())
        assertTrue(verification.isPerfect())
    }

    @Test
    @DisplayName("Diagnostic: 4 levels - Record → Array → Record → Array")
    void testFourLevels() {
        // Level 4 (deepest)
        Schema level4Schema = SchemaBuilder.record("Level4")
                .fields()
                .requiredString("value")
                .endRecord()

        // Level 3 - has array of level 4
        Schema level3Schema = SchemaBuilder.record("Level3")
                .fields()
                .name("items").type().array().items(level4Schema).noDefault()
                .endRecord()

        // Level 2 - has array of level 3
        Schema level2Schema = SchemaBuilder.record("Level2")
                .fields()
                .name("containers").type().array().items(level3Schema).noDefault()
                .endRecord()

        // Level 1 (root) - has level 2 record
        Schema schema = SchemaBuilder.record("Level1")
                .fields()
                .name("root").type(level2Schema).noDefault()
                .endRecord()

        // Create data
        Map<String, Object> level4_1 = new LinkedHashMap<>()
        level4_1.put("value", "Deep Value 1")

        Map<String, Object> level4_2 = new LinkedHashMap<>()
        level4_2.put("value", "Deep Value 2")

        Map<String, Object> level3 = new LinkedHashMap<>()
        level3.put("items", Arrays.asList(level4_1, level4_2))

        Map<String, Object> level2 = new LinkedHashMap<>()
        level2.put("containers", Collections.singletonList(level3))

        Map<String, Object> original = new LinkedHashMap<>()
        original.put("root", level2)

        println("=== FOUR LEVELS: Record → Array → Record → Array ===")
        println("Original structure:")
        println("  root: ${original.get('root')}")

        // Flatten
        MapFlattener flattener = new MapFlattener()
        Map<String, Object> flattened = flattener.flatten(original)

        println("\nFlattened:")
        flattened.each { k, v ->
            println("  $k: $v")
        }

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build()
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema)

        println("\nReconstructed:")
        println("  root: ${reconstructed.get('root')}")

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema)

        println("\n" + verification.getReport())
        assertTrue(verification.isPerfect())
    }

    @Test
    @DisplayName("Diagnostic: Simplified 5 levels like testDeepNesting")
    void testFiveLevelsSimplified() {
        // Level 5 (deepest)
        Schema level5Schema = SchemaBuilder.record("Level5")
                .fields()
                .requiredString("deepValue")
                .requiredInt("depth")
                .endRecord()

        // Level 4 - array of level 5 + string field
        Schema level4Schema = SchemaBuilder.record("Level4")
                .fields()
                .name("items").type().array().items(level5Schema).noDefault()
                .requiredString("level4Name")
                .endRecord()

        // Level 3 - nested record + string field
        Schema level3Schema = SchemaBuilder.record("Level3")
                .fields()
                .name("data").type(level4Schema).noDefault()
                .requiredString("level3Name")
                .endRecord()

        // Level 2 - array of level 3
        Schema level2Schema = SchemaBuilder.record("Level2")
                .fields()
                .name("containers").type().array().items(level3Schema).noDefault()
                .endRecord()

        // Level 1 (root)
        Schema schema = SchemaBuilder.record("Level1")
                .fields()
                .name("root").type(level2Schema).noDefault()
                .requiredString("rootId")
                .endRecord()

        // Create deeply nested data (simplified - just 1 element at each array level)
        Map<String, Object> level5_1 = new LinkedHashMap<>()
        level5_1.put("deepValue", "I am at level 5!")
        level5_1.put("depth", 5)

        Map<String, Object> level4 = new LinkedHashMap<>()
        level4.put("items", Collections.singletonList(level5_1))  // Just 1 item
        level4.put("level4Name", "Fourth Level")

        Map<String, Object> level3 = new LinkedHashMap<>()
        level3.put("data", level4)
        level3.put("level3Name", "Third Level")

        Map<String, Object> level2 = new LinkedHashMap<>()
        level2.put("containers", Collections.singletonList(level3))  // Just 1 container

        Map<String, Object> original = new LinkedHashMap<>()
        original.put("root", level2)
        original.put("rootId", "ROOT-001")

        println("=== FIVE LEVELS (SIMPLIFIED) ===")
        println("Original structure:")
        println("  rootId: ${original.get('rootId')}")
        println("  root.containers[0].level3Name: ${level3.get('level3Name')}")
        println("  root.containers[0].data.level4Name: ${level4.get('level4Name')}")
        println("  root.containers[0].data.items[0]: ${level5_1}")

        // Flatten
        MapFlattener flattener = new MapFlattener()
        Map<String, Object> flattened = flattener.flatten(original)

        println("\nFlattened:")
        flattened.each { k, v ->
            println("  $k: $v")
        }

        // Analyze the structure
        println("\n=== ANALYZING FLATTENED STRUCTURE ===")
        def itemsDeepValue = flattened.get("root_containers_data_items_deepValue")
        def itemsDepth = flattened.get("root_containers_data_items_depth")

        println("root_containers_data_items_deepValue:")
        println("  Type: ${itemsDeepValue?.getClass()?.name}")
        println("  Value: $itemsDeepValue")

        println("root_containers_data_items_depth:")
        println("  Type: ${itemsDepth?.getClass()?.name}")
        println("  Value: $itemsDepth")

        // Reconstruct
        println("\n=== RECONSTRUCTION ===")
        try {
            AvroReconstructor reconstructor = AvroReconstructor.builder()
                    .strictValidation(false)
                    .build()
            Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema)

            println("Successfully reconstructed!")
            println("Reconstructed structure:")
            println("  rootId: ${reconstructed.get('rootId')}")
            println("  root: ${reconstructed.get('root')}")

            // Verify
            AvroReconstructor.ReconstructionVerification verification =
                    reconstructor.verifyReconstruction(original, reconstructed, schema)

            println("\n" + verification.getReport())
            assertTrue(verification.isPerfect())
        } catch (Exception e) {
            println("Reconstruction failed!")
            println("Error: ${e.message}")
            e.printStackTrace()
            throw e
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
                .endRecord()

        Schema level4Schema = SchemaBuilder.record("Level4")
                .fields()
                .name("items").type().array().items(level5Schema).noDefault()
                .requiredString("level4Name")
                .endRecord()

        Schema level3Schema = SchemaBuilder.record("Level3")
                .fields()
                .name("data").type(level4Schema).noDefault()
                .requiredString("level3Name")
                .endRecord()

        Schema level2Schema = SchemaBuilder.record("Level2")
                .fields()
                .name("containers").type().array().items(level3Schema).noDefault()
                .endRecord()

        Schema schema = SchemaBuilder.record("Level1")
                .fields()
                .name("root").type(level2Schema).noDefault()
                .requiredString("rootId")
                .endRecord()

        // Create with 2 items at level 5 (like the original test)
        Map<String, Object> level5_1 = new LinkedHashMap<>()
        level5_1.put("deepValue", "I am at level 5!")
        level5_1.put("depth", 5)

        Map<String, Object> level5_2 = new LinkedHashMap<>()
        level5_2.put("deepValue", "Me too!")
        level5_2.put("depth", 5)

        Map<String, Object> level4 = new LinkedHashMap<>()
        level4.put("items", Arrays.asList(level5_1, level5_2))  // 2 items
        level4.put("level4Name", "Fourth Level")

        Map<String, Object> level3 = new LinkedHashMap<>()
        level3.put("data", level4)
        level3.put("level3Name", "Third Level")

        Map<String, Object> level2 = new LinkedHashMap<>()
        level2.put("containers", Collections.singletonList(level3))

        Map<String, Object> original = new LinkedHashMap<>()
        original.put("root", level2)
        original.put("rootId", "ROOT-001")

        println("=== FIVE LEVELS (FULL - 2 ITEMS AT DEEPEST LEVEL) ===")
        println("Original:")
        println("  root.containers[0].data.items: ${level4.get('items')}")

        // Flatten
        MapFlattener flattener = new MapFlattener()
        Map<String, Object> flattened = flattener.flatten(original)

        println("\nFlattened:")
        flattened.each { k, v ->
            println("  $k: $v")
        }

        // Reconstruct
        println("\n=== RECONSTRUCTION ===")
        try {
            AvroReconstructor reconstructor = AvroReconstructor.builder()
                    .strictValidation(false)
                    .build()
            Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema)

            println("Successfully reconstructed!")

            // Verify
            AvroReconstructor.ReconstructionVerification verification =
                    reconstructor.verifyReconstruction(original, reconstructed, schema)

            println("\n" + verification.getReport())
            assertTrue(verification.isPerfect())
        } catch (Exception e) {
            println("Reconstruction failed!")
            println("Error: ${e.message}")
            e.printStackTrace()
            throw e
        }
    }
}