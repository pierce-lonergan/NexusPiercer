package io.github.pierce;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The one axis nothing bounded, now bounded - and the four axes that already did, pinned.
 *
 * <h2>The measurement that motivated this</h2>
 *
 * <p>A 4,057,897-character well-formed JSON document (1000 array elements, 300 distinct keys
 * each) exhausted a 1 GB heap inside {@code MapFlattener.columnFor}. Not a pathological
 * document - just a wide sparse one.</p>
 *
 * <p>THE BRIEF SAID "nothing bounds the emitted column count". Quadratic is right; unbounded as
 * stated is wrong, and the difference sent the first analysis at the wrong axis.
 * {@code maxArraySize} bounds the SLOT axis absolutely, and bounds the column axis too whenever
 * each element carries exactly one distinct key - measured, 1500, 2000 and 4000 elements all
 * plateau at exactly 1000 columns. The genuinely unbounded quantity is the UNION OF DISTINCT KEYS
 * across elements, capped only by {@code maxArraySize x maxMapSize} = 1e7 columns, giving a
 * per-array ceiling near 1e10 cells and no ceiling at all across sibling arrays. Those four
 * measurements are pinned below so the "maxArraySize already covers this" argument cannot be
 * re-made from a note.</p>
 *
 * <h2>Why it refuses rather than truncates</h2>
 *
 * <p>Dropping columns past a budget yields a flat map whose SURVIVING columns are all still
 * exactly the right length - so {@code ArrayCardinalityException}, {@code agreedElementCount} and
 * every length assertion in the reconstructors stay green while whole fields have vanished. That
 * is bit-for-bit the defect class of the 2.1.0 alignment repair: a length invariant satisfied
 * while the data is wrong. Refusal is the only option that cannot be silent.</p>
 */
@DisplayName("a per-invocation array-cell budget, and the four bounds that do not cover it")
class MapFlattenerArrayCellBudgetTest {

    /** N maps in one array, map i carrying k distinct keys nobody else carries. */
    private static Map<String, Object> sparse(String field, int elements, int keysEach) {
        List<Object> list = new ArrayList<>(elements);
        for (int i = 0; i < elements; i++) {
            Map<String, Object> m = new LinkedHashMap<>();
            for (int j = 0; j < keysEach; j++) {
                m.put("k" + i + "_" + j, j);
            }
            list.add(m);
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put(field, list);
        return doc;
    }

    private static Map<String, Object> siblings(int arrays, int elements) {
        Map<String, Object> doc = new LinkedHashMap<>();
        for (int a = 0; a < arrays; a++) {
            List<Object> list = new ArrayList<>(elements);
            for (int i = 0; i < elements; i++) {
                list.add(Collections.singletonMap("a" + a + "k" + i, i));
            }
            doc.put("arr" + a, list);
        }
        return doc;
    }

    // ------------------------------------------------------------------ the bound

    @Test
    @DisplayName("a wide sparse array is refused at the default budget")
    void aWideSparseArrayIsRefusedAtTheDefaultBudget() {
        // 1000 elements x 20 distinct keys = 20,000 columns x 1000 slots = 20,000,000 cells.
        // Measured before the bound: returns normally, 100,146,690 output characters.
        Map<String, Object> doc = sparse("a", 1000, 20);

        MapFlattener.FlattenLimitExceededException e = assertThrows(
                MapFlattener.FlattenLimitExceededException.class,
                () -> MapFlattener.builder().build().flatten(doc));

        assertTrue(e.getMessage().contains("maxArrayCells"), e.getMessage());
        assertTrue(e.getMessage().contains("1048576"), e.getMessage());
    }

    @Test
    @DisplayName("the budget is cumulative across sibling arrays, not per array")
    void theBudgetIsCumulativeAcrossSiblingArraysNotPerArray() {
        // 50 sibling arrays of 500 sparse elements: 250,000 cells EACH - under any sane per-array
        // budget - and 12,500,000 in total. A per-array budget is the wrong design and this is
        // the document that proves it; measured before the bound at 62,778,390 characters.
        Map<String, Object> doc = siblings(50, 500);

        MapFlattener.FlattenLimitExceededException e = assertThrows(
                MapFlattener.FlattenLimitExceededException.class,
                () -> MapFlattener.builder().build().flatten(doc));
        // THE PROPERTY, stated so it cannot be satisfied by accident: the refusal happens in a
        // LATER sibling array, which can only happen if the counter carried across the boundary.
        // Each array alone is 250,000 cells, comfortably under the 1,048,576 default; only the
        // running total trips it.
        assertTrue(e.getMessage().contains("column 'arr") && !e.getMessage().contains("column 'arr0"),
                "the budget was reset per array - it tripped inside the FIRST one, or not at a "
                        + "sibling boundary at all: " + e.getMessage());
        assertTrue(e.getMessage().contains("1048576"), e.getMessage());
    }

    @Test
    @DisplayName("the typed exception escapes flatten() unwrapped")
    void theTypedExceptionEscapesFlattenUnwrapped() {
        // THE NEW SILENT FAILURE THIS REPAIR CREATES, and the only test that sees it.
        // flatten() ends in `catch (Exception e) { ... throw new RuntimeException("Failed to
        // flatten map", e); }`. Without a rethrow arm placed FIRST, the typed exception is
        // rewrapped, and a caller who writes `catch (FlattenLimitExceededException)` - exactly
        // what the javadoc tells them to write - catches nothing. It is not silent in the log;
        // it is silent to the type system, which is where the guarantee was supposed to live.
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> MapFlattener.builder().build().flatten(sparse("a", 1000, 20)));

        assertEquals(MapFlattener.FlattenLimitExceededException.class, thrown.getClass(),
                "the refusal was rewrapped; the catch(Exception) arm is in front of the rethrow");
        assertNull(thrown.getCause(), "a rewrapped refusal carries a cause; this one must not");
        assertTrue(thrown instanceof IllegalStateException,
                "it must stay inside the existing depth/circular failure contract");
    }

    // ------------------------------------------------------------------ vacuity controls

    @Test
    @DisplayName("CONTROL: a document just under the budget still flattens")
    void aDocumentJustUnderTheBudgetStillFlattens() {
        // Straddles the boundary. Without this, an implementation that throws on EVERY array of
        // maps would pass every other test in this class.
        Map<String, Object> ok = MapFlattener.builder().maxArrayCells(10_000).build()
                .flatten(sparse("a", 99, 1));            // 99 columns x 99 slots = 9,801 cells
        assertEquals(99, ok.size());

        assertThrows(MapFlattener.FlattenLimitExceededException.class,
                () -> MapFlattener.builder().maxArrayCells(10_000).build()
                        .flatten(sparse("a", 101, 1)),   // 101 x 101 = 10,201 cells
                "101 elements of one distinct key each is 10,201 cells and must be refused");
    }

    @Test
    @DisplayName("CONTROL: a dense array of the same element count is unaffected")
    void aDenseArrayOfTheSameElementCountIsUnaffected() {
        // Passes before and after by design - a characterization control. It pins that the bound
        // discriminates on CELLS, so a lazy implementation keyed on list.size() is caught.
        List<Object> dense = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            Map<String, Object> m = new LinkedHashMap<>();
            for (int j = 0; j < 10; j++) {
                m.put("c" + j, i * 10 + j);
            }
            dense.add(m);
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("a", dense);

        Map<String, Object> flat = MapFlattener.builder().build().flatten(doc);
        assertEquals(10, flat.size(), "1000 elements sharing 10 columns is 10,000 cells");
    }

    @Test
    @DisplayName("CONTROL: the four existing bounds measured, so the argument cannot be re-made")
    void theExistingBoundsStillDoNotBoundThis() {
        int unbounded = Integer.MAX_VALUE;

        // maxDepth is ALL-OR-NOTHING at the array boundary. There is nothing in between.
        Map<String, Object> depthOne = MapFlattener.builder().maxArrayCells(unbounded)
                .maxDepth(1).build().flatten(sparse("a", 200, 1));
        assertEquals(1, depthOne.size(), "maxDepth(1) collapses the whole array into one column");
        Map<String, Object> depthTwo = MapFlattener.builder().maxArrayCells(unbounded)
                .maxDepth(2).build().flatten(sparse("a", 200, 1));
        assertEquals(200, depthTwo.size(), "maxDepth(2) restores all 200");

        // maxMapSize cuts the second axis but only PER ELEMENT, and its default is 10,000.
        Map<String, Object> wide = MapFlattener.builder().maxArrayCells(unbounded)
                .maxMapSize(5).build().flatten(sparse("a", 50, 100));
        assertEquals(250, wide.size(), "maxMapSize(5) keeps 5 keys per element x 50 elements");
        Map<String, Object> wideDefault = MapFlattener.builder().maxArrayCells(unbounded)
                .build().flatten(sparse("a", 50, 100));
        assertEquals(5000, wideDefault.size(), "nothing bounds the UNION of keys across elements");

        // maxArraySize plateaus the slot axis - and the column axis too, but only incidentally,
        // when each element carries exactly one distinct key.
        Map<String, Object> plateau = MapFlattener.builder().maxArrayCells(unbounded)
                .maxArraySize(1000).build().flatten(sparse("a", 4000, 1));
        assertEquals(1000, plateau.size(), "1000 columns at 4000 elements: the plateau is real");
    }

    // ------------------------------------------------------------------ site C, newly quadratic

    @Test
    @DisplayName("the NESTED-array site is inside the budget too, because BL-018 made it quadratic")
    void theNestedArraySiteIsCountedToo() {
        // WHAT NEW SILENT FAILURE DID BL-018 CREATE? This one. Before that repair the
        // nested-array site APPENDED, so its cost was linear in PRESENT values and the analysis
        // that designed this budget explicitly excluded it on those grounds. It now pre-sizes to
        // the outer element count exactly like the other two sites, so it is quadratic exactly
        // like them - measured, 1000 sparse nested positions emit 6,999,890 characters against
        // 4,999,890 for the flat equivalent. A budget enforced only in columnFor would leave the
        // WIDEST of the three sites unbounded.
        List<Object> outer = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            outer.add(List.of(Collections.singletonMap("k" + i, i)));
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("g", outer);

        assertThrows(MapFlattener.FlattenLimitExceededException.class,
                () -> MapFlattener.builder().maxArrayCells(500_000).build().flatten(doc),
                "1000 columns x 1000 outer positions is 1,000,000 cells at the nested site");

        Map<String, Object> ok = MapFlattener.builder().maxArrayCells(2_000_000).build()
                .flatten(doc);
        assertEquals(1000, ok.size(), "CONTROL: it still works under a budget that allows it");
    }

    @Test
    @DisplayName("a nested position that flattens to NO columns is charged for its holes")
    void aNestedPositionThatFlattensToNoColumnsIsChargedForItsHoles() {
        // WHAT NEW SILENT FAILURE DID THE CARDINALITY REPAIR CREATE? This one, and the first
        // analysis of it was WRONG in a way worth recording. The premise was "the nested site's
        // inner axis is uncharged". DRILLED: it is charged, by columnFor inside the recursive
        // extraction, which bills (inner element count) per inner column as each is created -
        // {"g":[[k0..k999]]} costs 1,000,000 cells and lands one outer position short of the
        // default budget. A charge for the whole inner axis here would have double-billed every
        // ordinary nested document and halved its ceiling; the control below catches exactly
        // that and failed against the first draft.
        //
        // The real gap is narrow: an inner list that is NON-EMPTY but flattens to no columns
        // ([{}], [{},{}]) creates no inner column, so columnFor never runs and it costs nothing.
        // Before the cardinality repair it also produced nothing - one [] per column. Now it
        // produces innerSize nulls in EVERY column. Measured with the repair and no hole charge:
        // 13,901 input bytes -> 10,012,005 output characters, accepted.
        List<Object> distinctKeys = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            distinctKeys.add(Collections.singletonMap("k" + i, i));
        }
        List<Object> allEmpty = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            allEmpty.add(new LinkedHashMap<String, Object>());
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("g", List.of(distinctKeys, allEmpty));

        MapFlattener.FlattenLimitExceededException e = assertThrows(
                MapFlattener.FlattenLimitExceededException.class,
                () -> MapFlattener.builder().build().flatten(doc),
                "1001 columns each gaining 999 hole cells is 1,000,999 cells that nothing "
                        + "counted; uncharged this document emits ten million characters from "
                        + "thirteen kilobytes and is accepted");
        assertTrue(e.getMessage().contains("maxArrayCells="),
                "the refusal must name the knob that caused it: " + e.getMessage());

        // CONTROL: an ordinary nested document must NOT be refused. Every position here carries
        // its column, so columnFor bills the inner axis once and the hole charge adds zero.
        // 1000 outer x 1000 inner x 1 column = 1,000,000 cells, inside the default budget. This
        // is the assertion that refuted the double-charging draft.
        List<Object> wide = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            wide.add(Collections.singletonMap("same", i));
        }
        List<Object> outer = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            outer.add(wide);
        }
        Map<String, Object> single = new LinkedHashMap<>();
        single.put("g", outer);
        assertEquals(1, MapFlattener.builder().build().flatten(single).size(),
                "CONTROL: one column x 1000 outer x 1000 inner stays under the default budget");
    }

    @Test
    @DisplayName("maxArrayCells validates like its four siblings")
    void maxArrayCellsValidatesLikeItsSiblings() {
        assertThrows(IllegalArgumentException.class,
                () -> MapFlattener.builder().maxArrayCells(0));
        assertThrows(IllegalArgumentException.class,
                () -> MapFlattener.builder().maxArrayCells(-1));
    }

    @Test
    @DisplayName("the budget is per invocation, so a reused flattener does not accumulate")
    void theBudgetIsPerInvocationNotPerFlattener() {
        MapFlattener f = MapFlattener.builder().maxArrayCells(20_000).build();
        for (int call = 0; call < 5; call++) {
            assertEquals(99, f.flatten(sparse("a", 99, 1)).size(),
                    "call " + call + " must not inherit the previous call's cell total; a "
                            + "counter that never resets turns a bound into a quota");
        }
    }
}
