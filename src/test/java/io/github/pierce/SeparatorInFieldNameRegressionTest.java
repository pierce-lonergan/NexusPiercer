package io.github.pierce;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

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
 * <h2>What these tests assert</h2>
 *
 * <p>They do <b>not</b> assert the bug is fixed — it is not. They pin current behaviour so the
 * situation cannot silently worsen, and they are written so that fixing the encoding makes them
 * pass more comfortably rather than fail.</p>
 *
 * <p>The timeouts are deliberately generous (10-20x the measured healthy figure) because a CI
 * runner is slower and noisier than a workstation. They are a guard against a return to
 * superlinear behaviour, not a performance benchmark — that lives in {@code benchmarks/}.</p>
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
    @DisplayName("Known lossy encoding")
    class KnownLossyEncoding {

        /**
         * Documents the collision itself rather than asserting correct behaviour, because correct
         * behaviour does not exist yet.
         *
         * <p>Written to FAIL LOUDLY once the encoding is made injective: at that point the two
         * inputs produce different keys and the assertion below stops holding, which is the
         * signal to promote this into a real round-trip test.</p>
         */
        @Test
        @DisplayName("user_id and user.id collide today — flip this test when the encoding is fixed")
        void flatFieldAndNestedFieldCollide() {
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
                    .as("Two structurally different documents currently produce the same flattened "
                            + "key, which is why reconstruction is lossy. When the separator is "
                            + "escaped this assertion should be inverted.")
                    .isEqualTo(b.keySet());
        }
    }
}
