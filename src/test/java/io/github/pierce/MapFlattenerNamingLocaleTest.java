package io.github.pierce;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The emitted COLUMN NAME must not be a function of the JVM's default locale.
 *
 * <h2>Why this file exists at all</h2>
 *
 * <p>{@code MapFlattener.applyNamingStrategy} was pinned to {@link Locale#ROOT} in the
 * {@code bd5b070} commit, and nothing tested it. Adversarial review measured that: reverting the
 * three {@code Locale.ROOT} arguments in the working tree and running the whole suite gave
 * <b>2684 tests, 0 failures, BUILD SUCCESS</b> - not one test noticed a released-output column
 * rename. What DID notice was SpotBugs, through {@code DM_CONVERT_CASE}, and a static-analysis
 * rule is a proxy, not a behaviour test: it fires on any locale-less case conversion anywhere in
 * the class, so it says nothing about which key is emitted, and a refactor that routes case
 * conversion through a helper or a configurable locale silences it while the column name goes
 * back to being environment-dependent.</p>
 *
 * <h2>What the default locale actually did to the key set</h2>
 *
 * <p>Measured on two separately compiled builds under {@code -Duser.language=tr
 * -Duser.country=TR}, {@code 24dc5a5} (before the pin) against {@code bd5b070} (after):</p>
 *
 * <pre>
 * LOWER_CASE {"ID":1}            24dc5a5 -&gt; U+0131 U+0064  |  bd5b070 -&gt; U+0069 U+0064
 * UPPER_CASE {"id":1}            24dc5a5 -&gt; U+0130 U+0044  |  bd5b070 -&gt; U+0049 U+0044
 * SNAKE_CASE {"userID":1}        24dc5a5 -&gt; user_[U+0131]_d  |  bd5b070 -&gt; user_i_d
 * LOWER_CASE {"user":{ID,NAME}}  24dc5a5 -&gt; user_[U+0131]d  |  bd5b070 -&gt; user_id
 * </pre>
 *
 * <p>Three of the four naming strategies changed the emitted key, so an {@code ID} field became
 * a column spelled with the DOTLESS i (U+0131) on a Turkish executor and with an ordinary
 * {@code i} everywhere else. A column name is a data-format decision, not a presentation one;
 * Athena, Spark and Avro all resolve it by exact string. The assertions below are written against
 * CODE POINTS rather than against string literals, because the two spellings are visually
 * near-identical and a reviewer comparing them by eye is exactly how this survived.</p>
 *
 * <h2>Why the default locale is set here rather than in a fixture</h2>
 *
 * <p>{@code FidelityJavaInput} supports a per-fixture {@code environment.locale} and one fixture
 * uses it, so the corpus could express this. It is a JUnit test instead because the assertion is
 * about a key SET under four strategies rather than about one document's round trip, and because
 * the corpus records what a document loses - this loses nothing, it renames. Surefire runs
 * {@code forkCount=1} with no parallel execution configured, and the locale is restored in a
 * {@code finally}, so the blast radius is this method.</p>
 */
@DisplayName("naming strategies emit the same column name on every locale")
class MapFlattenerNamingLocaleTest {

    /** Turkish: the locale whose ASCII case mapping differs from ROOT's. Also az, lt. */
    private static final Locale TURKISH = Locale.forLanguageTag("tr-TR");

    private static String codePoints(String s) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            b.append(String.format("U+%04X", (int) s.charAt(i)));
            if (i < s.length() - 1) {
                b.append(' ');
            }
        }
        return b.toString();
    }

    private static String onlyKey(MapFlattener.FieldNamingStrategy strategy, String field) {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put(field, 1);
        Map<String, Object> flat = MapFlattener.builder().namingStrategy(strategy).build()
                .flatten(source);
        assertEquals(1, flat.size(), "expected exactly one column from " + strategy);
        return flat.keySet().iterator().next();
    }

    @Test
    @DisplayName("a tr-TR JVM emits the same keys as a ROOT one, checked by code point")
    void aTurkishDefaultLocaleDoesNotRenameAnyColumn() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(TURKISH);

            // U+0069 LATIN SMALL LETTER I, U+0064 d. Without the pin this is U+0131 U+0064,
            // the DOTLESS i - a different column, on a JVM the caller did not choose.
            assertEquals("U+0069 U+0064",
                    codePoints(onlyKey(MapFlattener.FieldNamingStrategy.LOWER_CASE, "ID")),
                    "LOWER_CASE renamed the ID column on a Turkish JVM");

            // U+0049 LATIN CAPITAL LETTER I, U+0044 D. Without the pin the first is U+0130,
            // CAPITAL I WITH DOT ABOVE.
            assertEquals("U+0049 U+0044",
                    codePoints(onlyKey(MapFlattener.FieldNamingStrategy.UPPER_CASE, "id")),
                    "UPPER_CASE renamed the id column on a Turkish JVM");

            // SNAKE_CASE lowercases after splitting, so it carries the same hazard.
            assertEquals("user_i_d",
                    onlyKey(MapFlattener.FieldNamingStrategy.SNAKE_CASE, "userID"),
                    "SNAKE_CASE renamed the userID column on a Turkish JVM");

            // AS_IS touches nothing and must stay that way on every locale.
            assertEquals("ID", onlyKey(MapFlattener.FieldNamingStrategy.AS_IS, "ID"));

            // A nested field, because the strategy is applied per SEGMENT after the path is
            // joined and the single-field case would not catch a regression that only fires on
            // a compound key.
            Map<String, Object> user = new LinkedHashMap<>();
            user.put("ID", 1);
            user.put("NAME", "x");
            Map<String, Object> source = new LinkedHashMap<>();
            source.put("user", user);
            Map<String, Object> flat = MapFlattener.builder()
                    .namingStrategy(MapFlattener.FieldNamingStrategy.LOWER_CASE).build()
                    .flatten(source);
            assertEquals("[user_id, user_name]", flat.keySet().toString(),
                    "LOWER_CASE renamed a nested column on a Turkish JVM");
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    @DisplayName("the tr-TR keys are byte-identical to the ROOT keys")
    void theTurkishKeySetEqualsTheRootKeySet() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.ROOT);
            String rootLower = onlyKey(MapFlattener.FieldNamingStrategy.LOWER_CASE, "ID");
            String rootUpper = onlyKey(MapFlattener.FieldNamingStrategy.UPPER_CASE, "id");
            String rootSnake = onlyKey(MapFlattener.FieldNamingStrategy.SNAKE_CASE, "userID");

            Locale.setDefault(TURKISH);
            assertEquals(rootLower, onlyKey(MapFlattener.FieldNamingStrategy.LOWER_CASE, "ID"));
            assertEquals(rootUpper, onlyKey(MapFlattener.FieldNamingStrategy.UPPER_CASE, "id"));
            assertEquals(rootSnake, onlyKey(MapFlattener.FieldNamingStrategy.SNAKE_CASE, "userID"));
        } finally {
            Locale.setDefault(previous);
        }
    }
}
