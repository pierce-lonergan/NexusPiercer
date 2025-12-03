package io.github.pierce

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import io.github.pierce.AvroReconstructor
import io.github.pierce.MapFlattener
import java.util.*

import static org.junit.jupiter.api.Assertions.*

/**
 * Detailed diagnostic tests to understand nested array flattening and reconstruction
 */
class NestedArrayDiagnosticTest {


    @Test
    @DisplayName("Diagnostic: Empty array handling")
    void testEmptyArrayHandling() {
        Schema itemSchema = SchemaBuilder.record("Item")
                .fields()
                .requiredString("name")
                .name("tags").type().array().items().stringType().noDefault()
                .endRecord()

        Schema schema = SchemaBuilder.record("EmptyArrayTest")
                .fields()
                .name("items").type().array().items(itemSchema).noDefault()
                .endRecord()

        // ONE item with EMPTY tags array
        Map<String, Object> item1 = new LinkedHashMap<>()
        item1.put("name", "ItemWithEmptyTags")
        item1.put("tags", Collections.emptyList())

        Map<String, Object> original = new LinkedHashMap<>()
        original.put("items", Collections.singletonList(item1))

        println("=== EMPTY ARRAY TEST ===")
        println("Original:")
        println("  items: " + original.get("items"))
        println("  item tags type: " + item1.get("tags").getClass().name)
        println("  item tags: " + item1.get("tags"))

        // Flatten
        MapFlattener flattener = new MapFlattener()
        Map<String, Object> flattened = flattener.flatten(original)

        println("\nFlattened:")
        flattened.each { k, v ->
            println("  $k: $v (type: ${v?.getClass()?.simpleName}, toString: '${v.toString()}')")
        }

        // Try to understand what happens during deserialization
        ObjectMapper mapper = new ObjectMapper()
        flattened.each { k, v ->
            if (v instanceof String && v.toString().startsWith("[")) {
                try {
                    def parsed = mapper.readValue(v.toString(), List.class)
                    println("  Parsed $k: $parsed (type: ${parsed?.getClass()?.simpleName}, size: ${parsed?.size()})")
                } catch (Exception e) {
                    println("  Failed to parse $k: ${e.message}")
                }
            }
        }

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build()
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema)

        println("\nReconstructed:")
        println("  items: " + reconstructed.get("items"))

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema)

        println("\n" + verification.getReport())
        assertTrue(verification.isPerfect())
    }

    @Test
    @DisplayName("Diagnostic: Two items - one with tags, one with empty array")
    void testMixedArraySizes() {
        Schema itemSchema = SchemaBuilder.record("Item")
                .fields()
                .requiredString("name")
                .name("tags").type().array().items().stringType().noDefault()
                .endRecord()

        Schema schema = SchemaBuilder.record("MixedTest")
                .fields()
                .name("items").type().array().items(itemSchema).noDefault()
                .endRecord()

        Map<String, Object> item1 = new LinkedHashMap<>()
        item1.put("name", "ItemWithTags")
        item1.put("tags", Arrays.asList("tag1", "tag2"))

        Map<String, Object> item2 = new LinkedHashMap<>()
        item2.put("name", "ItemWithEmptyTags")
        item2.put("tags", Collections.emptyList())

        Map<String, Object> original = new LinkedHashMap<>()
        original.put("items", Arrays.asList(item1, item2))

        println("=== MIXED ARRAY SIZES TEST ===")
        println("Original:")
        println("  items[0]: " + item1)
        println("  items[1]: " + item2)

        // Flatten
        MapFlattener flattener = new MapFlattener()
        Map<String, Object> flattened = flattener.flatten(original)

        println("\nFlattened:")
        flattened.each { k, v ->
            println("  $k: $v (type: ${v?.getClass()?.simpleName})")
        }

        // Manually check what's in items_tags
        def itemsTags = flattened.get("items_tags")
        println("\nDetailed items_tags analysis:")
        println("  Raw value: $itemsTags")
        println("  Type: ${itemsTags?.getClass()?.name}")

        if (itemsTags instanceof String) {
            println("  As String: '$itemsTags'")
            ObjectMapper mapper = new ObjectMapper()
            try {
                def parsed = mapper.readValue(itemsTags.toString(), List.class)
                println("  Parsed to List: $parsed")
                println("  Parsed size: ${parsed.size()}")
                parsed.eachWithIndex { elem, idx ->
                    println("    [$idx]: $elem (type: ${elem?.getClass()?.simpleName})")
                    if (elem instanceof String && elem.startsWith("[")) {
                        def innerParsed = mapper.readValue(elem, List.class)
                        println("      Inner parsed: $innerParsed (size: ${innerParsed.size()})")
                    }
                }
            } catch (Exception e) {
                println("  Parse error: ${e.message}")
            }
        }

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build()
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema)

        println("\nReconstructed:")
        println("  items: " + reconstructed.get("items"))

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema)

        println("\n" + verification.getReport())
        assertTrue(verification.isPerfect())
    }

    @Test
    @DisplayName("Diagnostic: Understand deserializeArrayStatic behavior")
    void testDeserializeArrayStatic() {
        println("=== TESTING deserializeArrayStatic BEHAVIOR ===")

        ObjectMapper mapper = new ObjectMapper()

        // Test different JSON array formats
        String[] testCases = [
                '["tag1","tag2","tag3"]',  // Normal array
                '["single"]',               // Single element
                '[]',                       // Empty array
                '["[]"]',                   // Array containing empty array string
                '[["tag1","tag2"]]',       // Nested array
                '[[]]'                      // Array containing empty array
        ]

        testCases.each { testCase ->
            println("\nTest case: $testCase")
            try {
                def result = mapper.readValue(testCase, List.class)
                println("  Parsed: $result")
                println("  Type: ${result.getClass().simpleName}")
                println("  Size: ${result.size()}")
                result.eachWithIndex { elem, idx ->
                    println("    [$idx]: $elem (type: ${elem?.getClass()?.simpleName ?: 'null'})")
                }
            } catch (Exception e) {
                println("  Error: ${e.message}")
            }
        }
    }

    @Test
    @DisplayName("Diagnostic: Full variable size array test with detailed logging")
    void testVariableSizeNestedArraysDetailed() {
        Schema itemSchema = SchemaBuilder.record("Item")
                .fields()
                .requiredString("name")
                .name("tags").type().array().items().stringType().noDefault()
                .endRecord()

        Schema schema = SchemaBuilder.record("VariableArrays")
                .fields()
                .name("items").type().array().items(itemSchema).noDefault()
                .endRecord()

        Map<String, Object> original = new LinkedHashMap<>()
        List<Map<String, Object>> items = new ArrayList<>()

        // Item with 3 tags
        Map<String, Object> item1 = new LinkedHashMap<>()
        item1.put("name", "Item1")
        item1.put("tags", Arrays.asList("tag1", "tag2", "tag3"))
        items.add(item1)

        // Item with 1 tag
        Map<String, Object> item2 = new LinkedHashMap<>()
        item2.put("name", "Item2")
        item2.put("tags", Collections.singletonList("single"))
        items.add(item2)

        // Item with 5 tags
        Map<String, Object> item3 = new LinkedHashMap<>()
        item3.put("name", "Item3")
        item3.put("tags", Arrays.asList("a", "b", "c", "d", "e"))
        items.add(item3)

        // Item with empty array
        Map<String, Object> item4 = new LinkedHashMap<>()
        item4.put("name", "Item4")
        item4.put("tags", Collections.emptyList())
        items.add(item4)

        original.put("items", items)

        println("=== VARIABLE SIZE NESTED ARRAYS - DETAILED ===")
        println("Original items:")
        items.eachWithIndex { item, idx ->
            println("  [$idx] name: ${item.get('name')}, tags: ${item.get('tags')}, tags.size: ${item.get('tags').size()}")
        }

        // Flatten
        MapFlattener flattener = new MapFlattener()
        Map<String, Object> flattened = flattener.flatten(original)

        println("\nFlattened:")
        flattened.each { k, v ->
            println("  $k: $v")
            println("    Type: ${v?.getClass()?.name}")
        }

        // Deep dive into items_tags
        def itemsTags = flattened.get("items_tags")
        println("\n=== DEEP DIVE: items_tags ===")
        println("Raw: $itemsTags")
        println("Type: ${itemsTags?.getClass()?.name}")

        ObjectMapper mapper = new ObjectMapper()
        if (itemsTags instanceof String) {
            try {
                def outerArray = mapper.readValue(itemsTags.toString(), List.class)
                println("Outer array size: ${outerArray.size()}")
                outerArray.eachWithIndex { elem, idx ->
                    println("\n  Element [$idx]:")
                    println("    Raw: $elem")
                    println("    Type: ${elem?.getClass()?.name ?: 'null'}")
                    println("    String representation: '${elem.toString()}'")

                    if (elem != null && elem.toString().startsWith("[")) {
                        try {
                            def innerArray = mapper.readValue(elem.toString(), List.class)
                            println("    Parsed as array: $innerArray")
                            println("    Inner array size: ${innerArray.size()}")
                        } catch (Exception e) {
                            println("    Failed to parse as array: ${e.message}")
                        }
                    }
                }
            } catch (Exception e) {
                println("Failed to parse outer array: ${e.message}")
            }
        }

        // Reconstruct with detailed error handling
        println("\n=== RECONSTRUCTION ===")
        try {
            AvroReconstructor reconstructor = AvroReconstructor.builder()
                    .strictValidation(false)  // Try with non-strict first
                    .build()
            Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema)

            println("Successfully reconstructed!")
            println("Reconstructed items:")
            def reconstructedItems = reconstructed.get("items")
            reconstructedItems.eachWithIndex { item, idx ->
                println("  [$idx]: $item")
            }

            // Verify
            AvroReconstructor.ReconstructionVerification verification =
                    reconstructor.verifyReconstruction(original, reconstructed, schema)

            println("\n" + verification.getReport())
        } catch (Exception e) {
            println("Reconstruction failed: ${e.message}")
            e.printStackTrace()
        }
    }
}