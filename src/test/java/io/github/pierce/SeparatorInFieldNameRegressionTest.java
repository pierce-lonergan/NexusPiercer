package io.github.pierce;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import io.github.pierce.path.FlattenedPath;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;

/**
 * Regression tests for separator characters appearing inside field names.
 *
 * <h2>Why this exists</h2>
 *
 * <p>The flattened key encoding is not injective: the separator is not escaped, so a field
 * literally named {@code user_id} and a nested {@code user.id} flatten to the same key. That was
 * already known to cause silent data loss. What the JMH harness found on its very first run is
 * that it is also a <b>denial-of-service vector</b>.</p>
 *
 * <p>Measured on 2026-08-09, JDK 21, 1 GB heap. Structure held completely fixed at 5 sibling
 * record arrays x N records x 8 fields, producing an identical <b>40 flat keys</b> in every case.
 * The only variable is how many literal underscores appear in each field name:</p>
 *
 * <pre>
 *   field name        25 records   50 records   75 records
 *   field_{n}             196 ms       174 ms       233 ms     (flat through 150 records)
 *   nested_field_{n}    1,198 ms     3,435 ms         OOM
 * </pre>
 *
 * <p>One additional underscore per field name takes reconstruction from flat-and-linear to heap
 * exhaustion. The reconstructor cannot distinguish a structural separator from a literal one, so
 * the number of candidate groupings it must consider grows with the count of underscores per
 * name and the field count. {@code nested_field_x} is an unremarkable snake_case name; so are
 * {@code user_id}, {@code created_at}, and {@code order_total}, which dominate this library's
 * target domain.</p>
 *
 * <h2>Fixed by the injective encoding</h2>
 *
 * <p>{@link io.github.pierce.path.FlattenedPath} escapes separator characters inside field names,
 * so {@code record_array_0} is one segment rather than three and the reconstructor no longer has
 * candidate groupings to search. Re-measured on the same machine and heap after wiring it in:</p>
 *
 * <pre>
 *   field name              25 records  50 records  75 records  150 records
 *   nested_field_{n} before   1,198 ms    3,435 ms         OOM            -
 *   nested_field_{n} after        3 ms        4 ms        4 ms         7 ms
 *   deep_nested_field_{n}         3 ms        3 ms        6 ms         6 ms
 * </pre>
 *
 * <p>Roughly 860x at 50 records, and OOM to 4 ms at 75. Note the third row: with three
 * underscores per field name the cost is unchanged, so reconstruction is now <b>independent of
 * how many separator characters a field name contains</b>. That independence — not the raw
 * speedup — is the property worth guarding, because it is what makes the input space safe rather
 * than merely faster.</p>
 *
 * <h2>What these tests assert</h2>
 *
 * <p>The bounded-reconstruction tests remain as guards against a return to superlinear behaviour.
 * Their timeouts are deliberately generous (well above the measured figure) because a CI runner
 * is slower and noisier than a workstation; they are not performance benchmarks, which live in
 * {@code benchmarks/}.</p>
 *
 * <p>One test still documents an unfixed defect: a field literally named {@code ___} collides
 * with the reconstructor's {@code __*__} sentinel namespace. That is a separate bug from the
 * encoding, and it is asserted as present rather than hidden.</p>
 *
 * @see <a href="../../../../../docs/audit/FINDINGS.md">docs/audit/FINDINGS.md</a> — arch/NP-002
 */
@DisplayName("Separator-in-field-name regressions")
class SeparatorInFieldNameRegressionTest {

    private static Map<String, Object> recordArrays(String fieldPrefix, int arrays, int records, int fields) {
        Map<String, Object> src = new LinkedHashMap<>();
        for (int a = 0; a < arrays; a++) {
            List<Object> arr = new ArrayList<>();
            for (int i = 0; i < records; i++) {
                Map<String, Object> rec = new LinkedHashMap<>();
                for (int f = 0; f < fields; f++) {
                    rec.put(fieldPrefix + f, "value");
                }
                arr.add(rec);
            }
            src.put("record_array_" + a, arr);
        }
        return src;
    }

    @Nested
    @DisplayName("Reconstruction must stay bounded")
    class BoundedReconstruction {

        @Test
        @DisplayName("single-underscore field names reconstruct in linear time")
        void singleUnderscoreIsLinear() {
            MapFlattener flattener = new MapFlattener(false, 100, 100_000);
            Map<String, Object> flat = flattener.flatten(recordArrays("field_", 5, 100, 8));

            // 40 keys: 5 arrays x 8 fields, consolidated. Guards the premise of the test below —
            // if this ever changes, the comparison is no longer apples-to-apples.
            assertThat(flat).hasSize(40);

            assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                    assertThat(JsonReconstructor.quickReconstruct(flat)).isNotNull());
        }

        /**
         * The DoS case. Identical key count, one more underscore per field name.
         *
         * <p>Pinned at 50 records because 75 exhausts a 1 GB heap outright — and a test that OOMs
         * takes the whole JVM fork with it rather than failing cleanly.</p>
         */
        @Test
        @DisplayName("double-underscore field names must not blow up (currently ~3.4s at 50 records)")
        void doubleUnderscoreMustNotBlowUp() {
            MapFlattener flattener = new MapFlattener(false, 100, 100_000);
            Map<String, Object> flat = flattener.flatten(recordArrays("nested_field_", 5, 50, 8));

            assertThat(flat).hasSize(40);

            // Measured at ~3.4s today against ~0.2s for the single-underscore equivalent.
            // 30s leaves generous CI headroom while still catching a return to the OOM regime.
            assertTimeoutPreemptively(Duration.ofSeconds(30), () ->
                    assertThat(JsonReconstructor.quickReconstruct(flat)).isNotNull());
        }
    }

    @Nested
    @DisplayName("Injective encoding")
    class InjectiveEncoding {

        /**
         * This test previously asserted that the two documents COLLIDED, and was written to fail
         * once the encoding became injective. It did, so it has been inverted into the real
         * round-trip assertion it was always a placeholder for.
         */
        @Test
        @DisplayName("user_id and user.id produce distinct keys and survive a round trip")
        void flatFieldAndNestedFieldNoLongerCollide() {
            MapFlattener flattener = new MapFlattener(false, 100, 100_000);

            Map<String, Object> flatField = new LinkedHashMap<>();
            flatField.put("user_id", "from-flat-field");

            Map<String, Object> nested = new LinkedHashMap<>();
            Map<String, Object> user = new LinkedHashMap<>();
            user.put("id", "from-nested-field");
            nested.put("user", user);

            Map<String, Object> a = flattener.flatten(flatField);
            Map<String, Object> b = flattener.flatten(nested);

            assertThat(a.keySet())
                    .as("A literal field named user_id must not encode to the same key as the "
                            + "nested path user -> id")
                    .isNotEqualTo(b.keySet());

            // And each must reconstruct to the shape it came from, not the other one.
            assertThat(JsonReconstructor.quickReconstruct(a)).isEqualTo(flatField);
            assertThat(JsonReconstructor.quickReconstruct(b)).isEqualTo(nested);
        }

        @Test
        @DisplayName("separator-bearing names round-trip at every level")
        void separatorBearingNamesRoundTrip() {
            MapFlattener flattener = new MapFlattener(false, 100, 100_000);

            Map<String, Object> src = new LinkedHashMap<>();
            Map<String, Object> inner = new LinkedHashMap<>();
            inner.put("created_at", "2026-08-09");
            inner.put("order_total", "12.50");
            src.put("a_b", inner);
            src.put("user_id", "flat-leaf");

            Map<String, Object> flat = flattener.flatten(src);
            assertThat(flat).hasSize(3);
            assertThat(JsonReconstructor.quickReconstruct(flat)).isEqualTo(src);
        }

        @Test
        @DisplayName("a lone separator as a field name round-trips")
        void loneSeparatorFieldNameRoundTrips() {
            MapFlattener flattener = new MapFlattener(false, 100, 100_000);

            Map<String, Object> src = new LinkedHashMap<>();
            Map<String, Object> inner = new LinkedHashMap<>();
            inner.put("_", "value");
            src.put("a", inner);

            assertThat(JsonReconstructor.quickReconstruct(flattener.flatten(src))).isEqualTo(src);
        }

        /**
         * A separate, pre-existing defect that the injective encoding does NOT fix, recorded here
         * so it is visible rather than latent.
         *
         * <p>{@code JsonReconstructor} reserves the {@code __*__} shape for its own bookkeeping
         * ({@code __isArray__}, {@code __arrayPath__}) and skips any key matching
         * {@code key.startsWith("__") && key.endsWith("__")} at JsonReconstructor.groovy:769.
         * A field literally named {@code ___} satisfies both tests — characters 0-1 and 1-2 are
         * each {@code "__"} — so it is silently dropped during reconstruction.</p>
         *
         * <p>The encoding round-trips this name correctly: it flattens to {@code a\_b_\_\_\_} and
         * decodes back to {@code ["a_b", "___"]}. The loss happens downstream, so fixing it means
         * replacing the magic-string sentinels with a private holder type rather than changing
         * the path encoding.</p>
         *
         * <p>Written to fail once that is done, which is the signal to fold it into the test
         * above.</p>
         */
        @Test
        @DisplayName("KNOWN DEFECT: a field named ___ collides with the reconstructor's sentinel namespace")
        void sentinelNamespaceCollisionIsStillPresent() {
            MapFlattener flattener = new MapFlattener(false, 100, 100_000);

            Map<String, Object> src = new LinkedHashMap<>();
            Map<String, Object> inner = new LinkedHashMap<>();
            inner.put("___", "triple");
            src.put("a_b", inner);

            Map<String, Object> flat = flattener.flatten(src);

            // The encoding itself is fine - the key decodes back to the right segments.
            assertThat(flat).hasSize(1);
            assertThat(FlattenedPath.decodeSegments(flat.keySet().iterator().next(), "_"))
                    .containsExactly("a_b", "___");

            Map<String, Object> reconstructed = JsonReconstructor.quickReconstruct(flat);
            assertThat(reconstructed)
                    .as("Documents the sentinel collision at JsonReconstructor.groovy:769. "
                            + "When the sentinels stop being magic strings, this becomes a "
                            + "round-trip and the assertion should be replaced.")
                    .isEqualTo(Map.of("a_b", Map.of()));
        }
    }
}
