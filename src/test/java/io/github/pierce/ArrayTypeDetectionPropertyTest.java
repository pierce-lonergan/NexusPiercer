package io.github.pierce;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Label;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Safety proof for the {@code cannotBeNumeric} fast-path added to
 * {@code JsonFlattenerConsolidator.determineArrayType}.
 *
 * <p>That method used to call {@link Double#parseDouble} in a try/catch for every array element,
 * constructing a stack-filling {@link NumberFormatException} per non-numeric value and continuing
 * even after the answer was already determined. The replacement skips {@code parseDouble} when a
 * character scan proves the string cannot possibly parse.</p>
 *
 * <p><b>The entire correctness argument rests on one invariant:</b> the filter must never reject
 * a string that {@code parseDouble} would accept. If it over-rejects, values silently change
 * classification from numeric to string — exactly the kind of quiet corruption this project has
 * already shipped once.</p>
 *
 * <p>That invariant is one-directional, which is why it is stated as a property rather than a
 * handful of examples. Over-<em>acceptance</em> is harmless: the value simply falls through to
 * {@code parseDouble}, which remains the sole authority.</p>
 *
 * <p>Tests live in a flat class because jqwik does not discover {@code @Property} methods inside
 * JUnit {@code @Nested} classes — it silently finds nothing rather than failing.</p>
 */
@Label("Array type detection fast path")
class ArrayTypeDetectionPropertyTest {

    private static final Method CANNOT_BE_NUMERIC = resolve();

    private static Method resolve() {
        try {
            Method m = JsonFlattenerConsolidator.class
                    .getDeclaredMethod("cannotBeNumeric", String.class);
            m.setAccessible(true);
            return m;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                    "cannotBeNumeric is the subject of this test; if it was renamed or removed, "
                            + "update this test rather than deleting it", e);
        }
    }

    private static boolean cannotBeNumeric(String s) {
        try {
            return (boolean) CANNOT_BE_NUMERIC.invoke(null, s);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean parseDoubleSucceeds(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException | NullPointerException e) {
            return false;
        }
    }

    /**
     * THE invariant. Everything else here is secondary.
     */
    @Property(tries = 5000)
    @Label("cannotBeNumeric never rejects a string that parseDouble accepts")
    void neverRejectsAParseableString(@ForAll("anyCandidate") String value) {
        if (parseDoubleSucceeds(value)) {
            assertThat(cannotBeNumeric(value))
                    .as("'%s' parses as a double, so the fast path must not reject it", value)
                    .isFalse();
        }
    }

    /**
     * Generator deliberately weighted toward the awkward end of the double grammar — hex floats,
     * type suffixes, exponent forms, signed infinities — because those are the cases a
     * hand-written character filter is most likely to get wrong.
     */
    @Provide
    Arbitrary<String> anyCandidate() {
        Arbitrary<String> exotic = Arbitraries.of(
                "1", "-1", "+1", "1.", ".5", "1e10", "1E-10", "1e+10",
                "0x1p3", "0X1P-3", "0x1.8p1",
                "1d", "1D", "1f", "1F", "1.5d", "1.5F",
                "NaN", "-NaN", "Infinity", "-Infinity", "+Infinity",
                "  42  ", "\t7\n", "", " ", "abc", "1abc", "a1", "--1", "1.2.3",
                "0xg", "1e", "e10", "null", "true", "false");
        Arbitrary<String> random = Arbitraries.strings()
                .withCharRange('a', 'z').withCharRange('0', '9')
                .withChars('+', '-', '.', ' ')
                .ofMaxLength(10);
        return Arbitraries.oneOf(exotic, random);
    }

    /**
     * The fast path is only worth having if it actually fires. Identifier-like values are the
     * common case in real data and the shape the array-heavy corpus is built from.
     */
    @Property(tries = 1000)
    @Label("identifier-like values are rejected without reaching parseDouble")
    void rejectsIdentifierLikeValuesCheaply(
            @ForAll("identifierLike") String value) {
        assertThat(parseDoubleSucceeds(value))
                .as("generator should only produce unparseable values")
                .isFalse();
        assertThat(cannotBeNumeric(value))
                .as("'%s' should be rejected by the character scan, not by an exception", value)
                .isTrue();
    }

    @Provide
    Arbitrary<String> identifierLike() {
        // Guarantee at least one letter outside the double grammar, so the scan must reject.
        Arbitrary<String> disallowed = Arbitraries.of(
                "g", "h", "j", "k", "l", "m", "o", "q", "r", "s", "u", "v", "w", "z");
        Arbitrary<String> rest = Arbitraries.strings()
                .withCharRange('a', 'z').withCharRange('0', '9').ofMinLength(1).ofMaxLength(8);
        return Combinators.combine(rest, disallowed).as((r, c) -> r + c);
    }
}
