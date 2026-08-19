package io.github.pierce;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What {@code verifyReconstruction()} actually compares, pinned - because the published
 * description of it was wrong.
 *
 * <h2>Why this exists</h2>
 *
 * <p>{@code docs/PROJECT_OVERVIEW.md} retracted the phrase "perfect reconstruction" and replaced
 * it with a description of the oracle's weakness. The replacement named the wrong mechanism: it
 * said {@code verify()} "treats String and Number as a compatible pair". Measured, it does not -
 * {@code compatibleTypes} has arms for same-class, Number/Number, Map/Map and List/List and then
 * returns false, so a String beside a Number is reported as a type mismatch before
 * {@code valuesEqual} is ever reached.</p>
 *
 * <p>The real weakness is one line further on. {@code compatibleTypes} admits ANY {@code Number}
 * beside ANY {@code Number}, and {@code compareNumbers} then falls through to
 * {@code a.longValue() == b.longValue()} for every non-floating type. Two {@code BigInteger}s
 * that agree only in their low 64 bits verify as identical. That is the money-losing case, and
 * this class is the worked example [BL-021] asks for: prose about behaviour is ungated, so the
 * behaviour gets a test and the prose gets to point at it.</p>
 */
@DisplayName("verifyReconstruction: what the oracle actually compares")
class VerifyReconstructionOracleWeaknessTest {

    private static final AvroReconstructor ORACLE =
            AvroReconstructor.builder().enableVerification(true).build();

    private static AvroReconstructor.ReconstructionVerification verify(Object a, Object b) {
        Map<String, Object> left = new LinkedHashMap<>();
        left.put("v", a);
        Map<String, Object> right = new LinkedHashMap<>();
        right.put("v", b);
        return ORACLE.verifyReconstruction(left, right, null);
    }

    @Test
    @DisplayName("THE MONEY-LOSING CASE: two different BigIntegers verify as PERFECT")
    void twoDifferentBigIntegersVerifyAsPerfect() {
        BigInteger low = new BigInteger("18446744073709551621");
        BigInteger high = new BigInteger("129127208515966861317");

        assertEquals(5L, low.longValue(), "the premise: both truncate to the same long");
        assertEquals(5L, high.longValue());
        assertFalse(low.equals(high), "and they are genuinely different numbers");

        assertTrue(verify(low, high).isPerfect(),
                "compareNumbers ends in a.longValue() == b.longValue(), so any two integers "
                        + "agreeing in their low 64 bits are indistinguishable to this oracle. "
                        + "Use Map.equals when the question is whether the data survived.");
    }

    @Test
    @DisplayName("String beside Number is a TYPE MISMATCH, not a compatible pair")
    void stringBesideNumberIsNotACompatiblePair() {
        // The claim docs/PROJECT_OVERVIEW.md used to make. It is false, and a false description
        // of a weakness is worse than none: it sends the next reader to the wrong method.
        assertFalse(verify("1", 1).isPerfect());
        assertTrue(verify("1", 1).getDifferences().toString().contains("type mismatch"),
                "the difference must be reported as a TYPE mismatch: " + verify("1", 1));
        assertFalse(verify("1.0", 1.0d).isPerfect());
    }

    @Test
    @DisplayName("the 1e-6 double tolerance is real, and it is the half the docs got right")
    void theDoubleToleranceIsReal() {
        assertTrue(verify(1.0d, 1.0000001d).isPerfect(),
                "a difference below 1e-6 is accepted");
        assertFalse(verify(1.0d, 1.001d).isPerfect(),
                "VACUITY CONTROL: a difference above it is still reported, so the assertion "
                        + "above is about the tolerance and not about a broken comparison");
    }
}
