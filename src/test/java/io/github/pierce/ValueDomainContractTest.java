package io.github.pierce;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The value-domain fidelity contract.
 *
 * <h2>Why this file exists</h2>
 *
 * <p>{@code FlattenedPath} made the <b>key</b> domain injective: distinct paths produce distinct
 * keys. Nothing had ever asked the same question of the <b>value</b> domain, and the answer turned
 * out to be no — several distinct documents flatten to byte-identical output and cannot be told
 * apart afterwards.</p>
 *
 * <p>Every collapse below is <b>pinned deliberately</b>, not endorsed. A test that reproduces
 * current behaviour is not a test that current behaviour is correct; these exist so that the
 * behaviour is stated, discoverable, and cannot change silently. Each carries a verdict:
 * <b>ACCEPTED</b> (intended, documented) or <b>DEFECT</b> (wrong, scheduled, not yet fixed).</p>
 *
 * <p>What these tests catch: a change in value-domain fidelity, in either direction. What they
 * cannot catch: whether the accepted verdicts are the <em>right</em> product decisions — that is
 * a judgement, recorded here so it can be argued with rather than rediscovered.</p>
 */
@DisplayName("Value-domain fidelity contract")
class ValueDomainContractTest {

    private static MapFlattener flattener() {
        return new MapFlattener(false, 100, 100_000);
    }

    private static Map<String, Object> doc(String key, Object value) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(key, value);
        return m;
    }

    private static Map<String, Object> flat(Object value) {
        return flattener().flatten(doc("a", value));
    }

    @Nested
    @DisplayName("DEFECT: empty containers are indistinguishable from null")
    class EmptyContainerCollapse {

        /**
         * DEFECT. An empty map, an empty list and an explicit null all flatten to a single entry
         * with a null value. Reconstruction therefore cannot restore which one was written, and a
         * document containing {@code {}} round-trips to one containing {@code null}.
         *
         * <p>Scheduled fix: a type-tagged null sentinel in the value domain, or a presence/type
         * sidecar emitted alongside the flattened output. Either is a wire-format change, so 2.0
         * is the window.</p>
         */
        @Test
        @DisplayName("{} and [] and null all flatten to null")
        void emptyContainersCollapseToNull() {
            assertThat(flat(new LinkedHashMap<>())).isEqualTo(flat(null));
            assertThat(flat(new ArrayList<>())).isEqualTo(flat(null));
            assertThat(flat(new ArrayList<>())).isEqualTo(flat(new LinkedHashMap<>()));
        }

        @Test
        @DisplayName("the collapse propagates through nested empty chains")
        void nestedEmptyChainsCollapse() {
            assertThat(flattener().flatten(doc("a", doc("b", new LinkedHashMap<>()))))
                    .isEqualTo(flattener().flatten(doc("a", doc("b", null))));
        }

        /** The chain terminates at the deepest declared level, so depth itself is preserved. */
        @Test
        @DisplayName("ACCEPTED: nesting depth of an empty chain IS preserved")
        void emptyChainDepthIsPreserved() {
            assertThat(flattener().flatten(doc("a", new LinkedHashMap<>())).keySet())
                    .containsExactly("a");
            assertThat(flattener().flatten(doc("a", doc("b", new LinkedHashMap<>()))).keySet())
                    .containsExactly("a_b");
        }
    }

    @Nested
    @DisplayName("DEFECT: non-finite doubles are indistinguishable from their text")
    class NonFiniteCollapse {

        /**
         * DEFECT. {@code Double.NaN} and the string {@code "NaN"} both flatten to the String
         * "NaN"; likewise Infinity. JSON has no literal for either, so stringifying is a
         * reasonable transport choice — but it is currently indistinguishable from a user who
         * genuinely stored that text, and reconstruction picks the string every time.
         */
        @Test
        @DisplayName("NaN and Infinity collapse into their string forms")
        void nonFiniteDoublesCollapseWithStrings() {
            assertThat(flat(Double.NaN)).isEqualTo(flat("NaN"));
            assertThat(flat(Double.POSITIVE_INFINITY)).isEqualTo(flat("Infinity"));
        }
    }

    @Nested
    @DisplayName("ACCEPTED: distinctions that ARE preserved")
    class PreservedDistinctions {

        /** An absent key stays absent; it does not become a null entry. */
        @Test
        @DisplayName("an absent key is distinct from a present null")
        void absentIsNotNull() {
            assertThat(flattener().flatten(new LinkedHashMap<>()))
                    .isNotEqualTo(flat(null));
        }

        /** The classic Hive/Athena footgun, and this library gets it right. */
        @Test
        @DisplayName("empty string is distinct from null")
        void emptyStringIsNotNull() {
            assertThat(flat("")).isNotEqualTo(flat(null));
        }

        @Test
        @DisplayName("negative zero survives")
        void negativeZeroIsPreserved() {
            assertThat(flat(-0.0d)).isNotEqualTo(flat(0.0d));
        }

        @Test
        @DisplayName("numeric type is preserved — 1, 1.0 and \"1\" stay distinct")
        void numericTypeIsPreserved() {
            assertThat(flat(1)).isNotEqualTo(flat(1.0d));
            assertThat(flat(1.0d)).isNotEqualTo(flat("1.0"));
            assertThat(flat(1)).isNotEqualTo(flat("1"));
        }

        /**
         * Longs beyond 2^53 keep full precision because they are never routed through double.
         * The JSON-parsing path is a separate question and is covered where that path is tested.
         */
        @Test
        @DisplayName("a long past 2^53 keeps full precision")
        void largeLongKeepsPrecision() {
            long beyondDouble = 9007199254740993L; // 2^53 + 1
            assertThat(flat(beyondDouble)).containsValue(beyondDouble);
            assertThat(flat(beyondDouble)).isNotEqualTo(flat((double) beyondDouble));
        }
    }

    @Nested
    @DisplayName("FIXED: BigInteger overflow no longer produces a wrong number")
    class BigIntegerOverflow {

        /**
         * Was audit finding NP-015, and it was worse than "lossy": {@code BigInteger.longValue()}
         * truncates the low 64 bits, so a 30-digit positive integer flattened to
         * {@code -4362896299872285998} — a plausible-looking, silently incorrect, NEGATIVE value.
         * Now falls back to exact decimal text, which is lossless.
         */
        @Test
        @DisplayName("a BigInteger beyond long range survives exactly, not wrapped")
        void hugeBigIntegerIsNotTruncated() {
            BigInteger huge = new BigInteger("123456789012345678901234567890");

            assertThat(flat(huge)).containsValue("123456789012345678901234567890");
            assertThat(flat(huge).values())
                    .as("must never silently wrap to a long")
                    .doesNotContain(huge.longValue());
        }

        @Test
        @DisplayName("a BigInteger inside long range still narrows to a long")
        void inRangeBigIntegerStillNarrows() {
            assertThat(flat(BigInteger.valueOf(42L))).containsValue(42L);
        }
    }

    @Nested
    @DisplayName("DEFECT: BigDecimal loses declared scale by default")
    class BigDecimalScale {

        /**
         * DEFECT, with an existing opt-out. By default a BigDecimal is narrowed to a double, so
         * {@code 37.7740} becomes {@code 37.774} — the same trailing-zero loss found in the Spark
         * examples suite — and arbitrary precision beyond a double is lost outright.
         *
         * <p>{@code preserveBigDecimalPrecision(true)} keeps the exact text. That should arguably
         * be the default in a library whose headline claim is lossless reconstruction; changing it
         * is a fidelity-vs-throughput decision for 2.0.</p>
         */
        @Test
        @DisplayName("default narrows to double and drops declared scale")
        void defaultLosesScale() {
            assertThat(flat(new BigDecimal("37.7740"))).containsValue(37.774d);
        }

        @Test
        @DisplayName("preserveBigDecimalPrecision keeps the exact text")
        void optInPreservesScale() {
            Map<String, Object> preserved = MapFlattener.builder()
                    .preserveBigDecimalPrecision(true)
                    .build()
                    .flatten(doc("a", new BigDecimal("37.7740")));

            assertThat(preserved).containsValue("37.7740");
        }
    }
}
