package io.github.pierce.gates;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Guards the SpotBugs exclude filter against a blanket class-level exemption returning.
 *
 * <p>WHY THIS EXISTS. Until 2026-08-17 this file carried a five-class x three-pattern
 * {@code <Match>} with no {@code <Method>} narrowing, covering AvroReconstructor,
 * GAvroSchemaFlattener, JsonFlattener, JsonReconstructor and MapFlattener — five of the
 * largest classes in the library. It masked exactly ten real findings in hand-written Java
 * (SpotBugs measured 241 with the block, 251 without it). All ten were fixed and the block
 * deleted. A suppression that broad also silently absolves the ELEVENTH finding, which is
 * what this test exists to prevent.
 *
 * <p>The rule enforced is deliberately narrow and mechanical: a {@code <Match>} may name
 * several classes only if it also names a method. Naming one class, or naming a method
 * alongside several classes, is fine — that is a targeted, reviewable suppression. What is
 * forbidden is "this pattern, anywhere in these classes".
 *
 * <p>DRILLED THREE WAYS, because a predicate that only ever passes is not a gate:
 * <ul>
 *   <li>GOOD INPUT — the real committed file passes, and the surviving narrowed
 *       FlattenedField {@code properties()}/{@code schema()} suppression is specifically
 *       asserted to survive. A rule that also rejected that entry would be the wrong rule.</li>
 *   <li>SYNTHETIC VIOLATION — the same predicate run over an in-test XML string carrying the
 *       exact five-class blanket that used to be committed must BLOCK.</li>
 *   <li>MISSING / EMPTY INPUT — a missing file, an empty file, and a well-formed file with
 *       zero {@code <Match>} elements must each BLOCK rather than pass vacuously. A file that
 *       cannot be read is not a file that contains no blanket exemptions.</li>
 * </ul>
 */
@DisplayName("SpotBugs exclude filter carries no blanket class-level exemption")
class SpotBugsExcludeHasNoBlanketClassBlockTest {

    private static final Path EXCLUDE_FILE =
            Paths.get("src", "main", "spotbugs", "spotbugs-exclude.xml");

    /**
     * The three patterns the deleted block suppressed. Named explicitly so that re-adding a
     * suppression for any of them against the five classes fails here even if it is narrowed
     * enough to satisfy the structural rule above.
     */
    private static final Set<String> FORMERLY_MASKED_PATTERNS = Set.of(
            "UPM_UNCALLED_PRIVATE_METHOD",
            "SIC_INNER_SHOULD_BE_STATIC_ANON",
            "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE");

    private static final Set<String> FORMERLY_SUPPRESSED_CLASSES = Set.of(
            "io.github.pierce.AvroReconstructor",
            "io.github.pierce.GAvroSchemaFlattener",
            "io.github.pierce.JsonFlattener",
            "io.github.pierce.JsonReconstructor",
            "io.github.pierce.MapFlattener");

    // ===================== the predicate under test =====================

    /** A single {@code <Match>} element, reduced to the three things this gate cares about. */
    private record MatchEntry(List<String> classes, List<String> methods, List<String> patterns) {

        /**
         * Class names SpotBugs treats as regular expressions carry a leading {@code ~}.
         *
         * <p>THE DISTINCTION THIS DRAWS, and why. The first draft of this rule was simply
         * "more than one class and no method", and running it refuted that draft: it also
         * flagged the EI_EXPOSE_REP entry covering
         * {@code ~io\.github\.pierce\.converter\.ConversionConfig.*} and
         * {@code ~io\.github\.pierce\..*\$.*Builder}, which is a legitimate, reasoned,
         * still-wanted suppression. Rather than whitelist that entry by name, the rule was
         * narrowed to what actually distinguishes the two cases.
         *
         * <p>A regex over a structural category ("every Builder") is a POLICY: it is stated
         * once, applies uniformly, and a reviewer can judge it on its rationale. An enumerated
         * list of specific hand-named classes with no method narrowing is not a policy — it is
         * a list of the places findings happened to be, which is the signature of hiding
         * findings rather than deciding something. The deleted block named five literal
         * classes for three unrelated bug patterns; that is the shape being forbidden.
         */
        private static boolean isLiteral(String className) {
            return !className.startsWith("~");
        }

        List<String> literalClasses() {
            return classes.stream().filter(MatchEntry::isLiteral).toList();
        }

        boolean isBlanketOverManyLiteralClasses() {
            return literalClasses().size() > 1 && methods.isEmpty();
        }
    }

    /**
     * Parses a filter document into match entries. Throws — never returns empty — when the
     * document is absent, unreadable, or carries no {@code <Match>} at all. That refusal is the
     * whole point of the missing-input drill: silently returning an empty list would make every
     * assertion below pass against a file that was never read.
     */
    private static List<MatchEntry> parseMatches(String xml) {
        if (xml == null || xml.isBlank()) {
            throw new IllegalArgumentException(
                    "SpotBugs exclude filter is missing or empty. Refusing to report a passing "
                            + "gate against a file that was never read.");
        }
        Document doc;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            doc = builder.parse(new InputSource(new StringReader(xml)));
        } catch (Exception e) {
            throw new IllegalArgumentException("SpotBugs exclude filter is not parseable XML", e);
        }

        NodeList matches = doc.getElementsByTagName("Match");
        if (matches.getLength() == 0) {
            throw new IllegalArgumentException(
                    "SpotBugs exclude filter contains zero <Match> elements. Either the file is "
                            + "not the filter, or the parse failed silently; refusing to pass "
                            + "vacuously.");
        }

        List<MatchEntry> entries = new ArrayList<>();
        for (int i = 0; i < matches.getLength(); i++) {
            Element match = (Element) matches.item(i);
            entries.add(new MatchEntry(
                    namedAttributesOfDescendants(match, "Class", "name"),
                    namedAttributesOfDescendants(match, "Method", "name"),
                    namedAttributesOfDescendants(match, "Bug", "pattern")));
        }
        return entries;
    }

    /**
     * Collects an attribute from every descendant of the given tag. Descendant rather than
     * child, deliberately: the deleted block wrapped its five {@code <Class>} elements in an
     * {@code <Or>}, so a child-only scan would have seen zero classes and reported the blanket
     * as compliant.
     */
    private static List<String> namedAttributesOfDescendants(Element match, String tag, String attr) {
        List<String> out = new ArrayList<>();
        NodeList nodes = match.getElementsByTagName(tag);
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            String v = ((Element) n).getAttribute(attr);
            if (!v.isEmpty()) {
                out.add(v);
            }
        }
        return out;
    }

    /**
     * True when a {@code <Class name=...>} value would match one of the five formerly-suppressed
     * classes, whether spelled literally or as a SpotBugs regex.
     *
     * <p>Spelling the same five names as regexes would otherwise be a free re-entry for all ten
     * findings: it clears the structural literal-enumeration rule by construction. Both spellings
     * are resolved here so neither rule can be bypassed by changing notation.
     */
    private static boolean coversAFormerlySuppressedClass(String className) {
        if (FORMERLY_SUPPRESSED_CLASSES.contains(className)) {
            return true;
        }
        if (!className.startsWith("~")) {
            return false;
        }
        String regex = className.substring(1);
        for (String suppressed : FORMERLY_SUPPRESSED_CLASSES) {
            try {
                if (suppressed.matches(regex)) {
                    return true;
                }
            } catch (RuntimeException ignored) {
                // An unparseable pattern cannot be shown to cover anything; the structural rule
                // and a human reviewer handle that case.
            }
        }
        return false;
    }

    private static String readCommittedFilter() {
        try {
            return Files.readString(EXCLUDE_FILE);
        } catch (Exception e) {
            throw new IllegalStateException("Could not read " + EXCLUDE_FILE.toAbsolutePath(), e);
        }
    }

    // ===================== GOOD INPUT: the committed file =====================

    @Nested
    @DisplayName("Good input - the committed filter")
    class GoodInput {

        @Test
        @DisplayName("no <Match> enumerates several literal class names without also naming a method")
        void noBlanketMultiClassMatch() {
            List<MatchEntry> entries = parseMatches(readCommittedFilter());

            List<String> offenders = new ArrayList<>();
            for (MatchEntry e : entries) {
                if (e.isBlanketOverManyLiteralClasses()) {
                    offenders.add("classes=" + e.literalClasses() + " patterns=" + e.patterns());
                }
            }

            assertTrue(offenders.isEmpty(),
                    "A <Match> enumerating several literal class names with no <Method> is a "
                            + "blanket exemption: every future finding of that pattern in those "
                            + "classes vanishes unreviewed. Narrow it to a named class AND "
                            + "method, or fix the finding. Offending entries: " + offenders);
        }

        @Test
        @DisplayName("none of the five formerly-suppressed classes is exempted for the three masked patterns")
        void theFiveClassesAreNoLongerExemptedForTheTenFindings() {
            List<MatchEntry> entries = parseMatches(readCommittedFilter());

            List<String> offenders = new ArrayList<>();
            for (MatchEntry e : entries) {
                for (String cls : e.classes()) {
                    if (!coversAFormerlySuppressedClass(cls)) {
                        continue;
                    }
                    for (String pattern : e.patterns()) {
                        if (FORMERLY_MASKED_PATTERNS.contains(pattern)) {
                            offenders.add(cls + " / " + pattern
                                    + (e.methods().isEmpty() ? " (no method narrowing)"
                                                             : " method=" + e.methods()));
                        }
                    }
                }
            }

            assertTrue(offenders.isEmpty(),
                    "These ten findings were fixed on 2026-08-17 by deleting five dead private "
                            + "methods, hoisting three anonymous TypeReference instances into "
                            + "shared static constants, and removing two unreachable null checks. "
                            + "Re-suppressing any of them re-hides work that was already done: "
                            + offenders);
        }

        @Test
        @DisplayName("the narrowed FlattenedField suppression survives - this rule targets blankets, not all suppressions")
        void theLegitimateNarrowedSuppressionIsNotCollateral() {
            List<MatchEntry> entries = parseMatches(readCommittedFilter());

            boolean found = entries.stream().anyMatch(e ->
                    e.classes().contains("io.github.pierce.schema.FlattenedField")
                            && e.methods().containsAll(List.of("properties", "schema"))
                            && e.patterns().contains("EI_EXPOSE_REP"));

            assertTrue(found,
                    "The FlattenedField properties()/schema() suppression is a legitimate, "
                            + "reasoned, method-narrowed entry and must survive. If this rule "
                            + "removed it too, the rule is wrong - it is meant to forbid blanket "
                            + "class exemptions, not all suppression.");
        }
    }

    // ===================== SYNTHETIC VIOLATION: must block =====================

    @Nested
    @DisplayName("Synthetic violation - the predicate must actually block")
    class SyntheticViolation {

        /** Byte-for-byte the shape of the block deleted on 2026-08-17. */
        private static final String BLANKET = """
                <FindBugsFilter>
                    <Match>
                        <Or>
                            <Class name="io.github.pierce.AvroReconstructor"/>
                            <Class name="io.github.pierce.GAvroSchemaFlattener"/>
                            <Class name="io.github.pierce.JsonFlattener"/>
                            <Class name="io.github.pierce.JsonReconstructor"/>
                            <Class name="io.github.pierce.MapFlattener"/>
                        </Or>
                        <Or>
                            <Bug pattern="RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"/>
                            <Bug pattern="SIC_INNER_SHOULD_BE_STATIC_ANON"/>
                            <Bug pattern="UPM_UNCALLED_PRIVATE_METHOD"/>
                        </Or>
                    </Match>
                </FindBugsFilter>
                """;

        @Test
        @DisplayName("the five-class blanket that used to be committed is detected")
        void theDeletedBlockIsDetectedIfItReturns() {
            List<MatchEntry> entries = parseMatches(BLANKET);

            assertEquals(1, entries.size(), "fixture should carry exactly one <Match>");
            MatchEntry only = entries.get(0);
            assertEquals(5, only.literalClasses().size(),
                    "the <Or>-wrapped classes must be seen; a child-only scan would report 0 here "
                            + "and pass the blanket as compliant");
            assertTrue(only.isBlanketOverManyLiteralClasses(),
                    "the predicate must classify the historical block as a blanket exemption - "
                            + "if it does not, the good-input assertions above prove nothing");
        }

        @Test
        @DisplayName("adding a <Method> to the same block clears it - the rule is about narrowing, not about class count")
        void narrowingTheSameBlockClearsIt() {
            String narrowed = BLANKET.replace("<Or>\n            <Bug",
                    "<Method name=\"someSpecificMethod\"/>\n        <Or>\n            <Bug");
            List<MatchEntry> entries = parseMatches(narrowed);
            assertFalse(entries.get(0).isBlanketOverManyLiteralClasses(),
                    "a multi-class match that names a method is a targeted suppression and must "
                            + "be permitted; otherwise the rule cannot be complied with");
        }

        @Test
        @DisplayName("re-writing the deleted block with regex class names does NOT slip past the rule")
        void regexSpellingOfTheSameBlanketIsStillCaught() {
            // The literal-vs-regex distinction must not become a loophole. Spelling the same
            // five classes as regexes clears the structural rule by construction - so the
            // pattern-specific assertion in GoodInput is what catches it, and this leg proves
            // that second rule is doing the work rather than riding on the first.
            String regexed = BLANKET.replace("name=\"io.github.pierce.",
                    "name=\"~io\\.github\\.pierce\\.");
            List<MatchEntry> entries = parseMatches(regexed);

            assertFalse(entries.get(0).isBlanketOverManyLiteralClasses(),
                    "regex names are deliberately outside the structural rule");

            boolean caughtByPatternRule = entries.stream().anyMatch(e ->
                    e.classes().stream().anyMatch(
                            SpotBugsExcludeHasNoBlanketClassBlockTest
                                    ::coversAFormerlySuppressedClass)
                            && e.patterns().stream().anyMatch(FORMERLY_MASKED_PATTERNS::contains));
            assertTrue(caughtByPatternRule,
                    "the pattern-specific rule must catch what the structural rule lets through; "
                            + "otherwise the regex spelling is a free re-entry for all ten "
                            + "findings");
        }
    }

    // ===================== MISSING / EMPTY: must block, not pass vacuously =====================

    @Nested
    @DisplayName("Missing or empty input - must block rather than pass vacuously")
    class MissingInput {

        @Test
        @DisplayName("an empty file is rejected")
        void emptyFileIsRejected() {
            assertThrows(IllegalArgumentException.class, () -> parseMatches(""));
            assertThrows(IllegalArgumentException.class, () -> parseMatches("   \n  "));
        }

        @Test
        @DisplayName("a null document is rejected")
        void nullDocumentIsRejected() {
            assertThrows(IllegalArgumentException.class, () -> parseMatches(null));
        }

        @Test
        @DisplayName("a well-formed filter with zero <Match> elements is rejected, not treated as clean")
        void zeroMatchFileIsRejected() {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> parseMatches("<FindBugsFilter></FindBugsFilter>"));
            assertTrue(e.getMessage().contains("zero <Match>"),
                    "the refusal must name the reason; got: " + e.getMessage());
        }

        @Test
        @DisplayName("unparseable content is rejected")
        void unparseableContentIsRejected() {
            assertThrows(IllegalArgumentException.class, () -> parseMatches("not xml at all <<<"));
        }

        @Test
        @DisplayName("the file this gate reads actually exists at the path the gate reads")
        void theFileExists() {
            if (!Files.exists(EXCLUDE_FILE)) {
                fail("Expected the SpotBugs exclude filter at " + EXCLUDE_FILE.toAbsolutePath()
                        + ". If it moved, this gate is reading nothing and every assertion above "
                        + "is meaningless.");
            }
        }
    }
}
