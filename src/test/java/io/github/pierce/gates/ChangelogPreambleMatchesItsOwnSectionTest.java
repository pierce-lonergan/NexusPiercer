package io.github.pierce.gates;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The changelog's own summary of its Behaviour-changes section must match the section.
 *
 * <p>WHY. `CHANGELOG.md` opens by telling the reader "read this before upgrading" and then
 * states how many places a 2.0.0 caller gets a different answer. That sentence said **twelve**
 * while the section beneath it had grown to **twenty**, and the throw count said **three** while
 * the real figure was eight — because each of two successive passes appended items and neither
 * went back to the paragraph that counts them. A summary that understates the size of a release
 * is worse than no summary: it is the one line a hurried reader trusts.</p>
 *
 * <p>This gate checks the two numbers that drifted, and nothing else. The prose is not gated —
 * only the counts, and the requirement that every item the throw sentence names actually exists
 * and is numbered.</p>
 */
@DisplayName("the changelog preamble counts match the section it summarises")
class ChangelogPreambleMatchesItsOwnSectionTest {

    private static final Path CHANGELOG = Paths.get("CHANGELOG.md");

    private static final Pattern ITEM = Pattern.compile("^(\\d+)\\. \\*\\*", Pattern.MULTILINE);

    /** English number words, index = value. Only as many as this section could plausibly reach. */
    private static final String[] WORDS = {
        "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
        "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen",
        "nineteen", "twenty", "twenty-one", "twenty-two", "twenty-three", "twenty-four",
        "twenty-five", "twenty-six", "twenty-seven", "twenty-eight", "twenty-nine", "thirty",
    };

    private static String changelog() throws IOException {
        return new String(Files.readAllBytes(CHANGELOG), StandardCharsets.UTF_8);
    }

    private static String behaviourChangesSection(String doc) {
        int start = doc.indexOf("### Behaviour changes");
        assertTrue(start >= 0, "CHANGELOG.md has no '### Behaviour changes' section");
        int end = doc.indexOf("### Added", start);
        assertTrue(end > start, "the Behaviour changes section is not followed by '### Added'");
        return doc.substring(start, end);
    }

    private static List<Integer> itemNumbers(String section) {
        List<Integer> numbers = new ArrayList<>();
        Matcher m = ITEM.matcher(section);
        while (m.find()) {
            numbers.add(Integer.parseInt(m.group(1)));
        }
        return numbers;
    }

    @Test
    @DisplayName("the item count in the preamble is the number of items in the section")
    void preambleItemCountMatches() throws IOException {
        String doc = changelog();
        List<Integer> items = itemNumbers(behaviourChangesSection(doc));
        assertTrue(items.size() >= 12, "expected the section to still be populated");

        String word = WORDS[items.size()];
        String preamble = doc.substring(0, doc.indexOf("### Behaviour changes"));

        assertTrue(preamble.contains("**" + word + "** places")
                        || preamble.contains(word + " places"),
                "the preamble does not say '" + word + " places' but the Behaviour changes "
                        + "section has " + items.size() + " numbered items. Update the paragraph "
                        + "that counts them, not this test.");
    }

    @Test
    @DisplayName("the numbering runs 1..N with no gap and no repeat")
    void numberingIsContiguous() throws IOException {
        List<Integer> items = itemNumbers(behaviourChangesSection(changelog()));
        for (int i = 0; i < items.size(); i++) {
            assertEquals(i + 1, items.get(i).intValue(),
                    "Behaviour changes item numbering is not contiguous at position " + (i + 1));
        }
    }

    @Test
    @DisplayName("every item the throw sentence names exists in the section")
    void throwSentenceNamesRealItems() throws IOException {
        String doc = changelog();
        int max = itemNumbers(behaviourChangesSection(doc)).size();
        String preamble = doc.substring(0, doc.indexOf("### Behaviour changes"));

        int sentence = preamble.indexOf("turn a previously-successful call into a throw");
        assertTrue(sentence >= 0,
                "the preamble no longer states how many calls now throw; that sentence is the "
                        + "one an upgrading caller reads first");

        Matcher named = Pattern.compile("item (\\d+)").matcher(preamble.substring(sentence));
        int found = 0;
        while (named.find()) {
            int n = Integer.parseInt(named.group(1));
            assertTrue(n >= 1 && n <= max,
                    "the throw sentence names item " + n + " but the section has " + max
                            + " items");
            found++;
        }
        assertTrue(found >= 3,
                "the throw sentence names only " + found + " items; it is meant to enumerate "
                        + "them so a caller can find each one");
    }

    @Test
    @DisplayName("the throw sentence's own arithmetic adds up, in both places it is stated")
    void throwSentenceArithmeticIsInternallyConsistent() throws IOException {
        // TWO CLAUSES DRIFTED WHILE THE THREE CHECKS ABOVE STAYED GREEN, because none of them
        // compared the preamble to ITSELF. At 24dc5a5 the sentence read "Nine ... across seven
        // items" while naming eight distinct items, and fifteen lines later the same preamble
        // said "because eight previously-successful calls now throw" - three numbers, two of
        // them wrong, in one paragraph a reader is told to read before upgrading.
        String doc = changelog();
        String preamble = doc.substring(0, doc.indexOf("### Behaviour changes"));

        int sentence = preamble.indexOf("turn a previously-successful call into a throw");
        assertTrue(sentence >= 0, "the preamble no longer states how many calls now throw");
        String throwSentence = preamble.substring(sentence);

        // 1. "across N items" must equal the number of DISTINCT items the sentence goes on to
        //    name. An item carrying two cases is still one item.
        Matcher across = Pattern.compile("across ([a-z-]+) items").matcher(throwSentence);
        assertTrue(across.find(),
                "the throw sentence no longer says 'across N items'; that clause is how a reader "
                        + "knows whether the enumeration that follows is complete");
        List<Integer> named = new ArrayList<>();
        Matcher item = Pattern.compile("item (\\d+)").matcher(throwSentence);
        while (item.find()) {
            int n = Integer.parseInt(item.group(1));
            if (!named.contains(n)) {
                named.add(n);
            }
        }
        assertEquals(wordFor(named.size()), across.group(1),
                "the throw sentence says 'across " + across.group(1) + " items' but names "
                        + named.size() + " distinct items " + named + ". Update the paragraph "
                        + "that counts them, not this test.");

        // 2. The leading count and the restatement below the section heading must be the same
        //    number. The restatement sits in the section's own preamble rather than in the
        //    document's, which is precisely why the three checks above never reached it.
        Matcher leading = Pattern.compile("\\*\\*([A-Z][a-z-]+) of the ").matcher(preamble);
        assertTrue(leading.find(), "the preamble no longer opens the throw sentence with a count");
        Matcher restated =
                Pattern.compile("because ([a-z-]+) previously-successful\\s+calls now throw")
                        .matcher(doc.substring(0, doc.indexOf("### Added")));
        assertTrue(restated.find(),
                "the preamble no longer restates the throw count above the Behaviour changes "
                        + "section; that restatement said 'eight' while the header said 'Nine'");
        assertEquals(leading.group(1).toLowerCase(Locale.ROOT), restated.group(1),
                "the preamble states the throw count twice and the two disagree: '"
                        + leading.group(1) + "' at the top, '" + restated.group(1) + "' lower down");
    }

    /** The English word this file uses for {@code n}. */
    private static String wordFor(int n) {
        assertTrue(n >= 0 && n < WORDS.length,
                "the throw sentence names " + n + " items, which is off the end of the word table");
        return WORDS[n];
    }
}
