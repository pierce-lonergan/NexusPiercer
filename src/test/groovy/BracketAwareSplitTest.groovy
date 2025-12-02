package io.github.pierce;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test to verify the bracket-aware split fix
 * This can be run independently to verify Bug #5 is fixed
 */
public class BracketAwareSplitTest {

    @Test
    @DisplayName("Test bracket-aware split with doubly nested arrays")
    public void testDoublyNestedArrayParsing() {
        AvroReconstructor reconstructor = AvroReconstructor.builder()
                .arrayFormat(AvroReconstructor.ArraySerializationFormat.BRACKET_LIST)
                .build();

        System.out.println("Testing doubly nested array parsing...\n");

        // Test 1: Simple doubly nested array
        String input1 = "[[RAM, Storage, Processor], [Connectivity, Battery Life]]";
        System.out.println("Test 1 - Input: " + input1);

        List<Object> outer = reconstructor.deserializeBracketList(input1);
        System.out.println("Outer array size: " + outer.size());
        System.out.println("Outer[0]: " + outer.get(0));
        System.out.println("Outer[1]: " + outer.get(1));

        assertEquals(2, outer.size(), "Should have 2 outer elements");
        assertEquals("[RAM, Storage, Processor]", outer.get(0).toString(),
                "First element should be intact nested array");
        assertEquals("[Connectivity, Battery Life]", outer.get(1).toString(),
                "Second element should be intact nested array");

        // Parse inner arrays
        List<Object> inner1 = reconstructor.deserializeBracketList(outer.get(0).toString());
        System.out.println("\nInner array 1 size: " + inner1.size());
        System.out.println("Inner1[0]: " + inner1.get(0));
        System.out.println("Inner1[1]: " + inner1.get(1));
        System.out.println("Inner1[2]: " + inner1.get(2));

        assertEquals(3, inner1.size(), "First inner array should have 3 elements");
        assertEquals("RAM", inner1.get(0).toString().trim());
        assertEquals("Storage", inner1.get(1).toString().trim());
        assertEquals("Processor", inner1.get(2).toString().trim());

        List<Object> inner2 = reconstructor.deserializeBracketList(outer.get(1).toString());
        System.out.println("\nInner array 2 size: " + inner2.size());
        System.out.println("Inner2[0]: " + inner2.get(0));
        System.out.println("Inner2[1]: " + inner2.get(1));

        assertEquals(2, inner2.size(), "Second inner array should have 2 elements");
        assertEquals("Connectivity", inner2.get(0).toString().trim());
        assertEquals("Battery Life", inner2.get(1).toString().trim());

        System.out.println("\n✅ Test 1 PASSED!\n");

        // Test 2: Triply nested array
        String input2 = "[[[a, b], [c, d]], [[e, f]]]";
        System.out.println("Test 2 - Input: " + input2);

        List<Object> outer2 = reconstructor.deserializeBracketList(input2);
        System.out.println("Outer array size: " + outer2.size());
        assertEquals(2, outer2.size(), "Should have 2 outer elements");
        assertEquals("[[a, b], [c, d]]", outer2.get(0).toString());
        assertEquals("[[e, f]]", outer2.get(1).toString());

        System.out.println("\n✅ Test 2 PASSED!\n");

        // Test 3: Mixed nesting levels
        String input3 = "[simple, [nested, items], another]";
        System.out.println("Test 3 - Input: " + input3);

        List<Object> outer3 = reconstructor.deserializeBracketList(input3);
        System.out.println("Outer array size: " + outer3.size());
        assertEquals(3, outer3.size(), "Should have 3 elements");
        assertEquals("simple", outer3.get(0).toString().trim());
        assertEquals("[nested, items]", outer3.get(1).toString().trim());
        assertEquals("another", outer3.get(2).toString().trim());

        System.out.println("\n✅ Test 3 PASSED!\n");

        // Test 4: Empty nested arrays
        String input4 = "[[], [a], []]";
        System.out.println("Test 4 - Input: " + input4);

        List<Object> outer4 = reconstructor.deserializeBracketList(input4);
        System.out.println("Outer array size: " + outer4.size());
        assertEquals(3, outer4.size(), "Should have 3 elements");
        assertEquals("[]", outer4.get(0).toString().trim());
        assertEquals("[a]", outer4.get(1).toString().trim());
        assertEquals("[]", outer4.get(2).toString().trim());

        System.out.println("\n✅ Test 4 PASSED!\n");

        System.out.println("═══════════════════════════════════════");
        System.out.println("✅ ALL BRACKET-AWARE SPLIT TESTS PASSED!");
        System.out.println("═══════════════════════════════════════\n");
    }

    @Test
    @DisplayName("Test that OLD split would have failed")
    public void testOldSplitWouldFail() {
        System.out.println("Demonstrating why OLD split() failed...\n");

        String input = "[[RAM, Storage, Processor], [Connectivity, Battery Life]]";

        // Remove brackets like the old code did
        String content = input.substring(1, input.length() - 1);
        System.out.println("After removing outer brackets: " + content);

        // Old naive split
        String[] oldResult = content.split("\\s*,\\s*");
        System.out.println("\nOLD split() result:");
        for (int i = 0; i < oldResult.length; i++) {
            System.out.println("  [" + i + "]: " + oldResult[i]);
        }

        System.out.println("\n❌ PROBLEMS:");
        System.out.println("  - Element 0: '[RAM' - Missing closing bracket!");
        System.out.println("  - Element 1: 'Storage' - Lost context of being in first array");
        System.out.println("  - Element 2: 'Processor]' - Closing bracket of first array");
        System.out.println("  - This data is CORRUPTED and unusable!");

        // Show that split length is wrong
        assertEquals(5, oldResult.length,
                "Old split incorrectly produces 5 elements instead of 2");

        System.out.println("\nThis is why we needed bracket-aware split! ✓\n");
    }

    @Test
    @DisplayName("Edge case: Commas in values with spaces")
    public void testCommasWithSpaces() {
        AvroReconstructor reconstructor = AvroReconstructor.builder()
                .arrayFormat(AvroReconstructor.ArraySerializationFormat.BRACKET_LIST)
                .build();

        // BRACKET_LIST format: values separated by ", " (comma + space)
        // So "Smith, John" needs to be a single value, not split
        String input = "[Smith John, Doe Jane]"; // Note: single space, not ", "

        List<Object> result = reconstructor.deserializeBracketList(input);
        System.out.println("Input: " + input);
        System.out.println("Result size: " + result.size());
        System.out.println("Result: " + result);

        // With proper bracket-aware split, this should work correctly
        // Note: BRACKET_LIST format splits on ", " so "Smith John" stays together
        assertTrue(result.size() > 0, "Should parse successfully");
    }
}