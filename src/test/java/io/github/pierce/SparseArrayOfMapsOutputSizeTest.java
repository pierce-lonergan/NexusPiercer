package io.github.pierce;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The RESOURCE cost of the 2.1.0 alignment fix, measured rather than described.
 *
 * <p>{@code MapFlattener} now pre-sizes every array-element column to the element count and
 * writes each value with {@code set(i, ..)}. That is what puts a value back under its own
 * element - and it also makes the padding UNCONDITIONAL. The old tail pad only fired when some
 * other column was longer, so a document whose columns were all length one stayed length one.
 * Pre-sizing gives every column N slots whether or not anything else is dense.</p>
 *
 * <p>For the worst shape - an array of N maps where every map carries a DIFFERENT key, so there
 * are N columns of N slots - the emitted cell count goes from O(elements) to O(elements x
 * columns). This test pins that number so the figure published in CHANGELOG item 13 cannot drift
 * away from the code, and so a future bound on the column count of a sparse array (the deferred
 * item) has something concrete to move.</p>
 *
 * <p>THIS IS NOT AN ASSERTION THAT THE COST IS ACCEPTABLE. It is an assertion that the cost is
 * what the release note says it is.</p>
 */
@DisplayName("a sparse array of maps emits columns x elements cells, and the note says so")
class SparseArrayOfMapsOutputSizeTest {

    /** N maps in one array, map i carrying only the key {@code k<i>}. The worst sparse shape. */
    private static Map<String, Object> sparseArrayOfMaps(int n) {
        List<Object> elements = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            elements.add(Collections.singletonMap("k" + i, i));
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("a", elements);
        return doc;
    }

    private static int cells(Map<String, Object> flat) {
        int total = 0;
        for (Object v : flat.values()) {
            String s = String.valueOf(v);
            // Every column is a JSON array of the same arity; count its slots by its commas.
            int slots = 1;
            for (int i = 0; i < s.length(); i++) {
                if (s.charAt(i) == ',') {
                    slots++;
                }
            }
            total += slots;
        }
        return total;
    }

    @Test
    @DisplayName("N distinct keys across N elements emits N columns of N slots, not N cells")
    void quadraticCellCount() {
        for (int n : new int[] {10, 100, 500}) {
            Map<String, Object> flat = MapFlattener.builder().maxArraySize(1000).build()
                    .flatten(sparseArrayOfMaps(n));

            assertEquals(n, flat.size(),
                    "expected one column per distinct key at n=" + n);
            assertEquals((long) n * n, cells(flat),
                    "expected columns x elements cells at n=" + n
                            + "; the alignment fix pre-sizes every column to the element count");
        }
    }

    @Test
    @DisplayName("the emitted character count at n=1000 is the figure the changelog publishes")
    void publishedCharacterFigure() {
        Map<String, Object> flat = MapFlattener.builder().maxArraySize(1000).build()
                .flatten(sparseArrayOfMaps(1000));

        long chars = 0;
        for (Map.Entry<String, Object> e : flat.entrySet()) {
            chars += e.getKey().length() + String.valueOf(e.getValue()).length();
        }

        // MEASURED 5,005,780 characters including the 1000 key names. The published figure in
        // CHANGELOG item 13 is the value half, 4,999,890. Pinned to the order of magnitude and
        // not to the exact byte: the key names and the null literal are an implementation
        // detail, the SHAPE is not. Falling below 4,000,000 would mean the padding stopped
        // being unconditional and item 13 needs rewriting.
        assertTrue(chars > 4_000_000L && chars < 8_000_000L,
                "expected several million characters from a 1000-element sparse array; measured "
                        + chars + ". CHANGELOG item 13 publishes this figure - if this assertion "
                        + "moved, the release note is now wrong.");
    }
}
