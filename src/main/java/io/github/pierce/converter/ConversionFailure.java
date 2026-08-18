package io.github.pierce.converter;

/**
 * Holds the FIRST failure of a format cascade so the terminal exception can carry a cause.
 *
 * <p>Package-private and deliberately tiny. The four date/time cascades each try several formats
 * in turn and used to discard every {@code DateTimeParseException} into an empty catch block, so
 * the exception the caller finally received had {@code getCause() == null} and a message naming
 * neither what was tried nor why it failed - even though {@link AbstractTypeConverter} has carried
 * a {@code conversionError(value, message, cause)} overload the whole time.</p>
 *
 * <p>WHY A METHOD RATHER THAN THE OBVIOUS TWO LINES, and both alternatives were measured rather
 * than assumed:</p>
 * <ul>
 *   <li>{@code lastFailure = e;} unconditionally in every catch cost 18 PMD
 *       {@code UnusedAssignment} findings, and PMD was right: each assignment is overwritten by
 *       the next catch without ever being read.</li>
 *   <li>{@code if (firstFailure == null) { firstFailure = e; }} fixed that and cost 12 SpotBugs
 *       findings instead - {@code NP_LOAD_OF_KNOWN_NULL_VALUE} plus two
 *       {@code RCN_REDUNDANT_NULLCHECK_*} per converter - because at the first catch the analyser
 *       can prove the variable is null and calls the test redundant.</li>
 * </ul>
 *
 * <p>Passing the held value through a method makes the assignment genuinely read AND puts the
 * null test behind a parameter the caller's dataflow cannot see through. That is not a trick to
 * quiet two linters: both linters were describing real properties of the two-line versions, and
 * this is the shape that has neither property.</p>
 *
 * <p>FIRST rather than LAST is a diagnostic choice, not an accident. The first branch tried is the
 * canonical format for the type, so its failure names the shape the caller most likely intended;
 * the last branch in every one of these cascades is {@code Long.parseLong}, whose
 * {@code NumberFormatException} says only {@code For input string: "..."}.</p>
 */
final class ConversionFailure {

    private ConversionFailure() {
    }

    /**
     * @param held      the failure already recorded, or null if none yet
     * @param candidate the failure just caught
     * @return {@code held} when it is non-null, otherwise {@code candidate}
     */
    static RuntimeException first(RuntimeException held, RuntimeException candidate) {
        return held != null ? held : candidate;
    }
}
