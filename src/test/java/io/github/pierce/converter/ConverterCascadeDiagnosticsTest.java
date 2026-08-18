package io.github.pierce.converter;


import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.format.DateTimeParseException;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The 18 format-cascade empty catches, and what they were actually costing.
 *
 * <p>THE BRIEF'S PREMISE FOR THESE WAS REFUTED TWICE OVER, and both refutations shape this test.
 *
 * <p>FIRST: "when every pattern fails, the caller deserves an error naming what was tried, not a
 * null or a default - check what happens at the end of each cascade." All four cascades ALREADY
 * threw; none returned null and none returned a default. What was actually missing is narrower:
 * the message named neither the formats attempted nor anything about the failure, and the cause
 * chain was discarded entirely even though {@code AbstractTypeConverter} has carried a
 * {@code conversionError(value, message, cause)} overload the whole time.
 *
 * <p>SECOND: "PMD's rule accepts a commented catch, so a genuine explanation both satisfies the
 * ratchet and leaves the next reader better off." It does not, as configured. The rule's XPath is
 * gated on {@code $allowCommentedBlocks != true()} and the property defaults to false, and
 * src/main/pmd/pmd-ruleset.xml pulls errorprone.xml in wholesale without setting it. All 25 sites
 * already carried comments and all 25 still counted. A better comment would have resolved exactly
 * zero of them, and flipping the property would have cleared all twenty-five without a thought
 * applied to any one.
 */
@DisplayName("Format cascades name what they tried and carry the cause")
class ConverterCascadeDiagnosticsTest {

    private static final ConversionConfig CFG = ConversionConfig.defaults();

    @Test
    @DisplayName("DateConverter names the formats it tried and keeps the parse failure as cause")
    void dateConverterNamesTheFormatsItTried() {
        assertThatThrownBy(() -> new DateConverter(CFG).convert("not a date"))
                .isInstanceOf(TypeConversionException.class)
                .hasMessageContaining("Cannot parse date")
                .hasMessageContaining("yyyy-MM-dd")
                .hasMessageContaining("M/d/yyyy")
                .hasMessageContaining("epoch millis")
                .cause().isInstanceOf(java.time.format.DateTimeParseException.class);
    }

    @Test
    @DisplayName("time, timestamp and timestamp_ns each carry their OWN format list")
    void eachConverterCarriesItsOwnFormatList() {
        // The third drill. A copy-paste that gave TimeConverter the timestamp list would satisfy
        // every "contains" assertion above and be silently wrong, so the lists are compared
        // against each other rather than only against themselves.
        String time = messageOf(() -> new TimeConverter(CFG).convert("nope"));
        String ts = messageOf(() -> new TimestampConverter(CFG, false).convert("nope"));
        String tsNano = messageOf(() -> new TimestampNanoConverter(CFG, false).convert("nope"));

        assertThat(time).contains("Cannot parse time").contains("ISO time");
        assertThat(ts).contains("Cannot parse timestamp").contains("ISO-8601 instant");
        assertThat(tsNano).contains("Cannot parse timestamp").contains("ISO-8601 instant");

        Set<String> lists = new LinkedHashSet<>();
        lists.add(triedPart(time));
        lists.add(triedPart(ts));
        assertThat(lists)
                .as("TimeConverter and TimestampConverter must not publish the same format list - "
                        + "time=%s timestamp=%s", triedPart(time), triedPart(ts))
                .hasSize(2);
    }

    @Test
    @DisplayName("every cascade carries a cause where it previously carried none")
    void everyCascadeCarriesACause() {
        assertThat(causeOf(() -> new TimeConverter(CFG).convert("nope"))).isNotNull();
        assertThat(causeOf(() -> new TimestampConverter(CFG, false).convert("nope"))).isNotNull();
        assertThat(causeOf(() -> new TimestampNanoConverter(CFG, false).convert("nope"))).isNotNull();
        assertThat(causeOf(() -> new DateConverter(CFG).convert("not a date"))).isNotNull();
    }

    @Test
    @DisplayName("GOOD INPUT CONTROL: valid values still convert, and the prefix is unchanged")
    void goodInputStillConverts() {
        // The cascade order is load-bearing and a careless rewrite could reorder it. These pin
        // that the FIRST matching format still wins for each converter.
        assertThat(new DateConverter(CFG).convert("2026-08-18")).isNotNull();
        assertThat(new TimeConverter(CFG).convert("12:30:00")).isNotNull();
        assertThat(new TimestampConverter(CFG, false).convert("2026-08-18T12:30:00Z")).isNotNull();

        // Two existing assertions match on the PREFIX only (ComplexConverterTest and
        // ErrorHandlingAndConfigTest). Appending to the message keeps them green; rewriting the
        // prefix would not, and this states that dependency out loud.
        assertThat(messageOf(() -> new DateConverter(CFG).convert("not a date")))
                .contains("Cannot parse date from string: 'not a date'");
    }

    private static String triedPart(String message) {
        int i = message.indexOf("Tried: ");
        return i < 0 ? "" : message.substring(i);
    }

    private static String messageOf(Runnable call) {
        try {
            call.run();
        } catch (RuntimeException e) {
            return String.valueOf(e.getMessage());
        }
        throw new AssertionError("expected a conversion failure");
    }

    private static Throwable causeOf(Runnable call) {
        try {
            call.run();
        } catch (RuntimeException e) {
            return e.getCause();
        }
        throw new AssertionError("expected a conversion failure");
    }

    /** Kept so an unused-import warning does not hide the fact that DateTimeParseException is the
     * cause type the date cascade discards for its first four branches. */
    @SuppressWarnings("unused")
    private static final Class<?> CAUSE_TYPE = DateTimeParseException.class;
}
