package io.github.pierce;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Detailed diagnostic tests to understand nested array flattening and reconstruction.
 *
 * <p>Ported from {@code src/test/groovy/NestedArrayDiagnosticTest.groovy}
 * (4 {@code @Test} methods in, 4 out).</p>
 *
 * <p>Two of the four originals assert nothing — {@code testDeserializeArrayStatic} is pure
 * {@code System.out} exploration of Jackson, and {@code testVariableSizeNestedArraysDetailed}
 * wraps its reconstruction in a catch-and-print that swallows every failure. They are ported
 * as they stand rather than silently strengthened; see the port's behaviour notes.</p>
 */
class NestedArrayDiagnosticTest {

    @Test
    @DisplayName("Diagnostic: Empty array handling")
    void testEmptyArrayHandling() {
        Schema itemSchema = SchemaBuilder.record("Item")
                .fields()
                .requiredString("name")
                .name("tags").type().array().items().stringType().noDefault()
                .endRecord();

        Schema schema = SchemaBuilder.record("EmptyArrayTest")
                .fields()
                .name("items").type().array().items(itemSchema).noDefault()
                .endRecord();

        // ONE item with EMPTY tags array
        Map<String, Object> item1 = new LinkedHashMap<>();
        item1.put("name", "ItemWithEmptyTags");
        item1.put("tags", Collections.emptyList());

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("items", Collections.singletonList(item1));

        System.out.println("=== EMPTY ARRAY TEST ===");
        System.out.println("Original:");
        System.out.println("  items: " + original.get("items"));
        System.out.println("  item tags type: " + item1.get("tags").getClass().getName());
        System.out.println("  item tags: " + item1.get("tags"));

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("\nFlattened:");
        flattened.forEach((k, v) -> System.out.println("  " + k + ": " + v
                + " (type: " + (v == null ? "null" : v.getClass().getSimpleName())
                + ", toString: '" + v.toString() + "')"));

        // Try to understand what happens during deserialization
        ObjectMapper mapper = new ObjectMapper();
        flattened.forEach((k, v) -> {
            if (v instanceof String && v.toString().startsWith("[")) {
                try {
                    List<?> parsed = mapper.readValue(v.toString(), List.class);
                    System.out.println("  Parsed " + k + ": " + parsed
                            + " (type: " + (parsed == null ? "null" : parsed.getClass().getSimpleName())
                            + ", size: " + (parsed == null ? "null" : String.valueOf(parsed.size())) + ")");
                } catch (Exception e) {
                    System.out.println("  Failed to parse " + k + ": " + e.getMessage());
                }
            }
        });

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        System.out.println("\nReconstructed:");
        System.out.println("  items: " + reconstructed.get("items"));

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println("\n" + verification.getReport());
        assertThat(verification.isPerfect()).isTrue();
    }

    @Test
    @DisplayName("Diagnostic: Two items - one with tags, one with empty array")
    void testMixedArraySizes() {
        Schema itemSchema = SchemaBuilder.record("Item")
                .fields()
                .requiredString("name")
                .name("tags").type().array().items().stringType().noDefault()
                .endRecord();

        Schema schema = SchemaBuilder.record("MixedTest")
                .fields()
                .name("items").type().array().items(itemSchema).noDefault()
                .endRecord();

        Map<String, Object> item1 = new LinkedHashMap<>();
        item1.put("name", "ItemWithTags");
        item1.put("tags", Arrays.asList("tag1", "tag2"));

        Map<String, Object> item2 = new LinkedHashMap<>();
        item2.put("name", "ItemWithEmptyTags");
        item2.put("tags", Collections.emptyList());

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("items", Arrays.asList(item1, item2));

        System.out.println("=== MIXED ARRAY SIZES TEST ===");
        System.out.println("Original:");
        System.out.println("  items[0]: " + item1);
        System.out.println("  items[1]: " + item2);

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("\nFlattened:");
        flattened.forEach((k, v) -> System.out.println("  " + k + ": " + v
                + " (type: " + (v == null ? "null" : v.getClass().getSimpleName()) + ")"));

        // Manually check what's in items_tags
        Object itemsTags = flattened.get("items_tags");
        System.out.println("\nDetailed items_tags analysis:");
        System.out.println("  Raw value: " + itemsTags);
        System.out.println("  Type: " + (itemsTags == null ? "null" : itemsTags.getClass().getName()));

        if (itemsTags instanceof String) {
            System.out.println("  As String: '" + itemsTags + "'");
            ObjectMapper mapper = new ObjectMapper();
            try {
                List<?> parsed = mapper.readValue(itemsTags.toString(), List.class);
                System.out.println("  Parsed to List: " + parsed);
                System.out.println("  Parsed size: " + parsed.size());
                for (int idx = 0; idx < parsed.size(); idx++) {
                    Object elem = parsed.get(idx);
                    System.out.println("    [" + idx + "]: " + elem
                            + " (type: " + (elem == null ? "null" : elem.getClass().getSimpleName()) + ")");
                    if (elem instanceof String && ((String) elem).startsWith("[")) {
                        List<?> innerParsed = mapper.readValue((String) elem, List.class);
                        System.out.println("      Inner parsed: " + innerParsed
                                + " (size: " + innerParsed.size() + ")");
                    }
                }
            } catch (Exception e) {
                System.out.println("  Parse error: " + e.getMessage());
            }
        }

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        System.out.println("\nReconstructed:");
        System.out.println("  items: " + reconstructed.get("items"));

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println("\n" + verification.getReport());
        assertThat(verification.isPerfect()).isTrue();
    }

    @Test
    @DisplayName("Diagnostic: Understand deserializeArrayStatic behavior")
    void testDeserializeArrayStatic() {
        System.out.println("=== TESTING deserializeArrayStatic BEHAVIOR ===");

        ObjectMapper mapper = new ObjectMapper();

        // Test different JSON array formats
        String[] testCases = {
                "[\"tag1\",\"tag2\",\"tag3\"]",  // Normal array
                "[\"single\"]",                  // Single element
                "[]",                            // Empty array
                "[\"[]\"]",                      // Array containing empty array string
                "[[\"tag1\",\"tag2\"]]",         // Nested array
                "[[]]"                           // Array containing empty array
        };

        for (String testCase : testCases) {
            System.out.println("\nTest case: " + testCase);
            try {
                List<?> result = mapper.readValue(testCase, List.class);
                System.out.println("  Parsed: " + result);
                System.out.println("  Type: " + result.getClass().getSimpleName());
                System.out.println("  Size: " + result.size());
                for (int idx = 0; idx < result.size(); idx++) {
                    Object elem = result.get(idx);
                    System.out.println("    [" + idx + "]: " + elem
                            + " (type: " + (elem == null ? "null" : elem.getClass().getSimpleName()) + ")");
                }
            } catch (Exception e) {
                System.out.println("  Error: " + e.getMessage());
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
                .endRecord();

        Schema schema = SchemaBuilder.record("VariableArrays")
                .fields()
                .name("items").type().array().items(itemSchema).noDefault()
                .endRecord();

        Map<String, Object> original = new LinkedHashMap<>();
        List<Map<String, Object>> items = new ArrayList<>();

        // Item with 3 tags
        Map<String, Object> item1 = new LinkedHashMap<>();
        item1.put("name", "Item1");
        item1.put("tags", Arrays.asList("tag1", "tag2", "tag3"));
        items.add(item1);

        // Item with 1 tag
        Map<String, Object> item2 = new LinkedHashMap<>();
        item2.put("name", "Item2");
        item2.put("tags", Collections.singletonList("single"));
        items.add(item2);

        // Item with 5 tags
        Map<String, Object> item3 = new LinkedHashMap<>();
        item3.put("name", "Item3");
        item3.put("tags", Arrays.asList("a", "b", "c", "d", "e"));
        items.add(item3);

        // Item with empty array
        Map<String, Object> item4 = new LinkedHashMap<>();
        item4.put("name", "Item4");
        item4.put("tags", Collections.emptyList());
        items.add(item4);

        original.put("items", items);

        System.out.println("=== VARIABLE SIZE NESTED ARRAYS - DETAILED ===");
        System.out.println("Original items:");
        for (int idx = 0; idx < items.size(); idx++) {
            Map<String, Object> item = items.get(idx);
            System.out.println("  [" + idx + "] name: " + item.get("name")
                    + ", tags: " + item.get("tags")
                    + ", tags.size: " + ((List<?>) item.get("tags")).size());
        }

        // Flatten
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("\nFlattened:");
        flattened.forEach((k, v) -> {
            System.out.println("  " + k + ": " + v);
            System.out.println("    Type: " + (v == null ? "null" : v.getClass().getName()));
        });

        // Deep dive into items_tags
        Object itemsTags = flattened.get("items_tags");
        System.out.println("\n=== DEEP DIVE: items_tags ===");
        System.out.println("Raw: " + itemsTags);
        System.out.println("Type: " + (itemsTags == null ? "null" : itemsTags.getClass().getName()));

        ObjectMapper mapper = new ObjectMapper();
        if (itemsTags instanceof String) {
            try {
                List<?> outerArray = mapper.readValue(itemsTags.toString(), List.class);
                System.out.println("Outer array size: " + outerArray.size());
                for (int idx = 0; idx < outerArray.size(); idx++) {
                    Object elem = outerArray.get(idx);
                    System.out.println("\n  Element [" + idx + "]:");
                    System.out.println("    Raw: " + elem);
                    System.out.println("    Type: " + (elem == null ? "null" : elem.getClass().getName()));
                    System.out.println("    String representation: '" + elem.toString() + "'");

                    if (elem != null && elem.toString().startsWith("[")) {
                        try {
                            List<?> innerArray = mapper.readValue(elem.toString(), List.class);
                            System.out.println("    Parsed as array: " + innerArray);
                            System.out.println("    Inner array size: " + innerArray.size());
                        } catch (Exception e) {
                            System.out.println("    Failed to parse as array: " + e.getMessage());
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Failed to parse outer array: " + e.getMessage());
            }
        }

        // Reconstruct with detailed error handling
        System.out.println("\n=== RECONSTRUCTION ===");
        try {
            AvroReconstructor reconstructor = AvroReconstructor.builder()
                    .strictValidation(false)  // Try with non-strict first
                    .build();
            Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

            System.out.println("Successfully reconstructed!");
            System.out.println("Reconstructed items:");
            List<?> reconstructedItems = (List<?>) reconstructed.get("items");
            for (int idx = 0; idx < reconstructedItems.size(); idx++) {
                System.out.println("  [" + idx + "]: " + reconstructedItems.get(idx));
            }

            // Verify
            AvroReconstructor.ReconstructionVerification verification =
                    reconstructor.verifyReconstruction(original, reconstructed, schema);

            System.out.println("\n" + verification.getReport());
        } catch (Exception e) {
            // NOTE: faithful to the Groovy original, which also swallowed the failure here.
            // Nothing after this point can fail the test.
            System.out.println("Reconstruction failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
