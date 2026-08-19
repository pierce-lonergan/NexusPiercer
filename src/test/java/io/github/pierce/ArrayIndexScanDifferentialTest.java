package io.github.pierce;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Label;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Differential proof that the hand-rolled array-index scans in {@code JsonFlattenerConsolidator}
 * compute exactly what the regexes they replaced computed.
 *
 * <p>{@code consolidateFlattened} used to allocate a {@link java.util.regex.Matcher} for every
 * flattened key ({@code ARRAY_INDEX_PATTERN.matcher(key).find()}) and a second Matcher plus a
 * StringBuilder for every array key ({@code ARRAY_INDEX_STRIP_PATTERN...replaceAll("")}). Both
 * are now character scans.</p>
 *
 * <p><b>The two regexes below are FROZEN ORACLES.</b> They are the exact source that shipped in
 * {@code JsonFlattenerConsolidator} through 2026-08-19, and they are kept here rather than in
 * the production class for a reason worth stating: once the production code stopped calling
 * them they stopped being the specification of anything that runs, and PMD correctly counted
 * them as dead fields. What they still are is the historical definition of the emitted column
 * names, which every future edit to the scans must stay bug-compatible with.</p>
 *
 * <p>They must not be "corrected". Each of the three narrowings pinned in
 * {@link #narrowingsAreContract()} looks like a defect and is instead the shipped contract; a
 * change to any of them renames output columns on reachable input and belongs in a deliberate
 * correctness commit with the byte-identity golden re-recorded on purpose.</p>
 *
 * <h2>Why a property and not examples</h2>
 * <p>Both functions are total functions of one String, which makes an exhaustive differential
 * possible over the alphabet that matters. Three of the four ways this replacement can go wrong
 * are invisible to example-based tests, because no benchmark corpus and no fidelity fixture
 * contains any of the shapes:</p>
 * <ol>
 *   <li>A key beginning with {@code '['} — the regex's {@code .+?} needs a character before the
 *       bracket, so {@code "[0]"} is NOT array-indexed.</li>
 *   <li>A bracket preceded by a line terminator — {@code .} excludes {@code \n \r 
 *        }, so {@code "a\n[0]"} is NOT array-indexed either.</li>
 *   <li>A non-ASCII digit inside the brackets — {@code \d} is ASCII-only here, so {@code "a[०]"}
 *       is left alone. A scan using {@code Character.isDigit} would strip it and merge two
 *       columns.</li>
 *   <li>Advancing past a failed bracket instead of by one character — {@code "a[[0]"} must strip
 *       to {@code "a["}, not to {@code "a"}.</li>
 * </ol>
 *
 * <p>Tests live in a flat class because jqwik does not discover {@code @Property} methods inside
 * JUnit {@code @Nested} classes — it silently finds nothing rather than failing.</p>
 */
@Label("Array-index scan matches the regex it replaced")
class ArrayIndexScanDifferentialTest {

    private static final Method HAS_ARRAY_INDEX = resolveMethod("hasArrayIndex");
    private static final Method STRIP_ARRAY_INDICES = resolveMethod("stripArrayIndices");

    /** Verbatim {@code ARRAY_INDEX_PATTERN} as it shipped. Frozen; see the class javadoc. */
    private static final Pattern FIND_ORACLE = Pattern.compile("(.+?)\\[(\\d+)\\](.*)");

    /** Verbatim {@code ARRAY_INDEX_STRIP_PATTERN} as it shipped. Frozen; see the class javadoc. */
    private static final Pattern STRIP_ORACLE = Pattern.compile("\\[\\d+\\]");

    private static Method resolveMethod(String name) {
        try {
            Method m = JsonFlattenerConsolidator.class.getDeclaredMethod(name, String.class);
            m.setAccessible(true);
            return m;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                    name + " is the subject of this test; if it was renamed or removed, update "
                            + "this test rather than deleting it", e);
        }
    }

    private static boolean scanFind(String s) {
        try {
            return (boolean) HAS_ARRAY_INDEX.invoke(null, s);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String scanStrip(String s) {
        try {
            return (String) STRIP_ARRAY_INDICES.invoke(null, s);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean regexFind(String s) {
        return FIND_ORACLE.matcher(s).find();
    }

    private static String regexStrip(String s) {
        return STRIP_ORACLE.matcher(s).replaceAll("");
    }

    // ------------------------------------------------------------------ exhaustive

    /**
     * Every string of length 0..5 over the nine symbols that can change either answer: a normal
     * character, the two separators the flattener uses, both brackets, a digit, a line
     * terminator, a non-ASCII digit, and a space. 66,430 strings, which is every structural
     * arrangement of those symbols short enough to matter.
     */
    @Test
    @DisplayName("exhaustive over all 66,430 strings of length 0..5 from the 9 deciding symbols")
    void exhaustiveOverDecidingAlphabet() {
        char[] alphabet = {'a', '.', '_', '[', ']', '0', '\n', '०', ' '};
        List<String> disagreements = new ArrayList<>();
        int checked = 0;

        StringBuilder sb = new StringBuilder(5);
        for (int len = 0; len <= 5; len++) {
            checked += enumerate(alphabet, len, sb, disagreements);
        }

        assertThat(checked)
                .as("the enumeration itself must not silently do nothing")
                .isEqualTo(66_430);
        assertThat(disagreements)
                .as("scan and regex must agree on every string; each entry below is an input "
                        + "where the replacement would change an emitted column name")
                .isEmpty();
    }

    private int enumerate(char[] alphabet, int len, StringBuilder sb, List<String> out) {
        if (sb.length() == len) {
            check(sb.toString(), out);
            return 1;
        }
        int count = 0;
        for (char c : alphabet) {
            sb.append(c);
            count += enumerate(alphabet, len, sb, out);
            sb.setLength(sb.length() - 1);
        }
        return count;
    }

    private void check(String s, List<String> out) {
        boolean scanned = scanFind(s);
        boolean expected = regexFind(s);
        if (scanned != expected) {
            out.add("find(" + describe(s) + "): scan=" + scanned + " regex=" + expected);
        }
        String stripped = scanStrip(s);
        String expectedStrip = regexStrip(s);
        if (!stripped.equals(expectedStrip)) {
            out.add("strip(" + describe(s) + "): scan=" + describe(stripped)
                    + " regex=" + describe(expectedStrip));
        }
    }

    private static String describe(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 0x20 || c > 0x7e) {
                sb.append(String.format("\\u%04x", (int) c));
            } else {
                sb.append(c);
            }
        }
        return sb.append('"').toString();
    }

    // ------------------------------------------------------------------ corners

    @Test
    @DisplayName("the named traps, pinned by name so a regression says which one broke")
    void namedCorners() {
        String[] corners = {
            "", "[", "]", "[]", "[0]", "[0]a", "a[0]", "a[]", "a[0", "a[x]", "a[0x]",
            "a[[0]", "a[0][1]", "a[00]", "a[1]b[2]", "a[-1]", "a[ 1]", "[0][1]",
            "\n[0]", "a\n[0]", "a\r[0]", "a[0]", "a [0]", "a [0]", "a [0]",
            "a[०]", "a[۰]", "a[０]", "a[0०]",
            "a[99999999999999999999999]", "a[0].b[1].c",
            "line_items[0].sku_code", "numeric_array_0[499]", "record_array[99].nested_field_7",
        };
        List<String> disagreements = new ArrayList<>();
        for (String c : corners) {
            check(c, disagreements);
        }
        assertThat(disagreements).isEmpty();
    }

    /**
     * Pins the four narrowings as facts rather than only as "agrees with the regex".
     *
     * <p>If someone later decides the regex itself was wrong, this test states plainly what the
     * current contract is, so the decision to change it has to be taken deliberately.</p>
     */
    @Test
    @DisplayName("the four narrowings are contract, not accident")
    void narrowingsAreContract() {
        assertThat(scanFind("[0]"))
                .as("a key that starts with a bracket is NOT array-indexed: the regex's .+? "
                        + "requires a character before it")
                .isFalse();
        assertThat(scanFind("a\n[0]"))
                .as("a bracket preceded by a line terminator is NOT array-indexed: . excludes "
                        + "line terminators")
                .isFalse();
        assertThat(scanFind("a[०]"))
                .as("Devanagari zero is not an ASCII digit and \\d here is ASCII-only")
                .isFalse();
        assertThat(scanStrip("a[[0]"))
                .as("replaceAll advances one character past a failed bracket, it does not skip it")
                .isEqualTo("a[");
        assertThat(scanStrip("[0]"))
                .as("the strip has no preceding-character requirement, unlike the find")
                .isEmpty();
    }

    /**
     * The identity fast path is an allocation optimisation, so assert it is actually taken —
     * otherwise the scan is correct and pointless.
     */
    @Test
    @DisplayName("strip returns the same instance when nothing is stripped")
    void stripReturnsIdentityWhenUnchanged() {
        String key = "user_id";
        assertThat(scanStrip(key)).isSameAs(key);
        String bracketButNoIndex = "a[]";
        assertThat(scanStrip(bracketButNoIndex)).isSameAs(bracketButNoIndex);
    }

    // ------------------------------------------------------------------ random

    @Property(tries = 200_000)
    @Label("find agrees with the regex over random keys")
    void findAgreesEverywhere(@ForAll("keys") String key) {
        assertThat(scanFind(key)).isEqualTo(regexFind(key));
    }

    @Property(tries = 200_000)
    @Label("strip agrees with the regex over random keys")
    void stripAgreesEverywhere(@ForAll("keys") String key) {
        assertThat(scanStrip(key)).isEqualTo(regexStrip(key));
    }

    /**
     * A wider alphabet than the exhaustive pass, at lengths up to 40: both bracket kinds, digits
     * at both ends of the ASCII range, all five line terminators, three non-ASCII digit systems,
     * a surrogate pair, and the separators the flattener actually emits.
     */
    @Provide
    Arbitrary<String> keys() {
        return Arbitraries.of(
                        'a', 'Z', '.', '_', '[', ']', '0', '9', '5', '-', '+', ' ',
                        '\n', '\r', '', ' ', ' ',
                        '०', '۰', '０', '\ud83d', '\ude00', '\\')
                .list().ofMinSize(0).ofMaxSize(40)
                .map(chars -> {
                    StringBuilder sb = new StringBuilder(chars.size());
                    chars.forEach(sb::append);
                    return sb.toString();
                });
    }
}
