package io.github.pierce.gates;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Three numbers the documentation publishes, checked against the thing they claim to describe.
 *
 * <h2>Why these three and not a general "docs are correct" gate</h2>
 *
 * <p>Each of the three had actually drifted, and each is checkable without running a build:</p>
 *
 * <ul>
 *   <li>{@code docs/ANTI_REGRESSION.md} published the static-analysis ceilings as
 *       {@code (0 / 323 / 231)} while {@code .github/quality-baseline.json} enforced PMD at 322.
 *       The document that exists to explain the ratchets published a ratchet value the ratchet
 *       does not use.</li>
 *   <li>{@code CONTRIBUTING.md} said 2,372 test invocations, {@code docs/INSTALL.md} said 2,401,
 *       and the baseline recorded 2,530. Three documents, three numbers, none current. The check
 *       later read only the FIRST occurrence per document, and a pass that corrected
 *       {@code CONTRIBUTING.md} line 26 left line 30 four lines below it saying the previous
 *       figure - two suite sizes in one file, both dated the same day, gate green. It now reads
 *       EVERY occurrence of the canonical phrase in all three documents, and checks the
 *       surefire-XML pair that the same paragraph publishes.</li>
 *   <li>{@code docs/ANTI_REGRESSION.md} is where README sends a reader for "how the gates and
 *       ratchets work", and it named none of the gates added over the last five passes.</li>
 * </ul>
 *
 * <p>Semantic accuracy of prose is NOT gated and cannot be by this technique - see [BL-021].</p>
 */
@DisplayName("published project facts match the source of truth they restate")
class PublishedProjectFactsMatchTheSourceTest {

    private static final Path BASELINE = Paths.get(".github/quality-baseline.json");
    private static final Path ANTI_REGRESSION = Paths.get("docs/ANTI_REGRESSION.md");
    private static final Path CONTRIBUTING = Paths.get("CONTRIBUTING.md");
    private static final Path INSTALL = Paths.get("docs/INSTALL.md");

    private static final Pattern CEILINGS = Pattern.compile("\\((\\d+)\\s*/\\s*(\\d+)\\s*/\\s*(\\d+)\\)");

    // ------------------------------------------------------------------ 1. the ceilings

    @Test
    @DisplayName("ANTI_REGRESSION publishes the ceilings the baseline enforces")
    void antiRegressionPublishesTheEnforcedCeilings() throws IOException {
        JsonNode baseline = new ObjectMapper().readTree(Files.readString(BASELINE, StandardCharsets.UTF_8));
        int checkstyle = baseline.get("checkstyle").get("ceiling").asInt();
        int pmd = baseline.get("pmd").get("ceiling").asInt();
        int spotbugs = baseline.get("spotbugs").get("ceiling").asInt();

        Matcher m = CEILINGS.matcher(read(ANTI_REGRESSION));
        assertThat(m.find())
                .as("docs/ANTI_REGRESSION.md no longer carries a '(checkstyle / pmd / spotbugs)' "
                        + "triple. THE ANCHOR MUST BIND: rewording the row out of existence would "
                        + "otherwise make this gate silently stop measuring, which is the exact "
                        + "failure mode it was written to prevent one level up.")
                .isTrue();

        assertThat(List.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)),
                        Integer.parseInt(m.group(3))))
                .as("docs/ANTI_REGRESSION.md publishes ceilings that .github/quality-baseline.json "
                        + "does not enforce. A ratchet whose documented value disagrees with its "
                        + "enforced value is a control that has stopped meaning what it says.")
                .isEqualTo(List.of(checkstyle, pmd, spotbugs));
    }

    // ------------------------------------------------------------------ 2. the suite size

    @Test
    @DisplayName("every published suite size is the same number")
    void everyPublishedSuiteSizeIsTheSameNumber() {
        int recorded = recordedTestCount();
        assertThat(recorded).as("no test count recorded in the quality baseline").isPositive();

        // EVERY OCCURRENCE, NOT THE FIRST. This assertion used to read the FIRST integer before
        // "test invocations" in each document, and a pass that corrected CONTRIBUTING.md line 26
        // to 2,684 left line 30 saying 2,634 four lines below it - two suite sizes in one file,
        // both dated the same day, and the gate green because it never looked past the first.
        List<Integer> published = new ArrayList<>();
        published.addAll(allIntsBefore(read(CONTRIBUTING), "test invocations"));
        published.addAll(allIntsBefore(read(INSTALL), "test invocations"));
        published.addAll(allIntsBefore(read(ANTI_REGRESSION), "test invocations"));
        published.addAll(allIntsBefore(read(INSTALL), "tests, about"));

        // THE MARKER MUST BIND. A document that stops using the canonical phrase stops being
        // measured, which is the failure this whole class exists to prevent one level up.
        assertThat(published)
                .as("the three documents no longer publish a suite size in the canonical form "
                        + "'N test invocations' (or 'N tests, about' in docs/INSTALL.md). "
                        + "Rewording it out of existence makes this gate silently stop measuring.")
                .hasSizeGreaterThanOrEqualTo(3);

        assertThat(published)
                .as("A PUBLISHED SUITE SIZE DISAGREES WITH THE BASELINE. The documents state %s "
                        + "and .github/quality-baseline.json records %d. Take the number from the "
                        + "most recent measurement; a suite size is the one figure a new "
                        + "contributor uses to decide whether their run looks right. A HISTORICAL "
                        + "figure must NOT be written in the canonical phrase - date it and say "
                        + "'the suite was N invocations' instead, which this gate deliberately "
                        + "does not match.",
                        published, recorded)
                .containsOnly(recorded);
    }

    @Test
    @DisplayName("the published surefire-XML undercount is the pair the baseline recorded")
    void theSurefireUndercountMatchesTheBaseline() {
        // CONTRIBUTING.md tells a contributor to read Maven's summary line rather than sum the
        // surefire XML, and backs it with a measured pair. Both halves of that pair go stale
        // together, and the pair is the reason a reader trusts the instruction.
        Matcher recorded = Pattern.compile("surefire-XML sum reads (\\d+)").matcher(read(BASELINE));
        assertThat(recorded.find())
                .as(".github/quality-baseline.json no longer records the surefire-XML sum, so the "
                        + "figure CONTRIBUTING.md publishes cannot be checked against anything")
                .isTrue();
        int baselineXml = Integer.parseInt(recorded.group(1));

        String contributing = read(CONTRIBUTING);
        // \s+ rather than a literal space: the sentence wraps, and a line break between
        // "against" and "Maven's" is a formatting choice that must not switch a gate off.
        Matcher published =
                Pattern.compile("measured ([\\d,]+)\\s+against\\s+Maven's").matcher(contributing);
        assertThat(published.find())
                .as("CONTRIBUTING.md no longer states the measured surefire-XML sum")
                .isTrue();
        int publishedXml = Integer.parseInt(published.group(1).replace(",", ""));

        assertThat(publishedXml)
                .as("CONTRIBUTING.md publishes a surefire-XML sum of %d and "
                        + ".github/quality-baseline.json records %d", publishedXml, baselineXml)
                .isEqualTo(baselineXml);

        // And the stated gap must actually be the gap, so the two numbers cannot drift apart
        // while each stays individually defensible.
        Matcher gap = Pattern.compile("UNDERCOUNTS here by \\*\\*exactly (\\d+)\\*\\*")
                .matcher(contributing);
        assertThat(gap.find()).as("CONTRIBUTING.md no longer states the undercount gap").isTrue();
        assertThat(recordedTestCount() - publishedXml)
                .as("CONTRIBUTING.md says the surefire XML undercounts by exactly %s, but the "
                        + "figures it publishes differ by %d", gap.group(1),
                        recordedTestCount() - publishedXml)
                .isEqualTo(Integer.parseInt(gap.group(1)));
    }

    // ------------------------------------------------------------------ 3. the gate inventory

    @Test
    @DisplayName("ANTI_REGRESSION names every gate in the gates package")
    void antiRegressionNamesEveryGate() {
        String doc = read(ANTI_REGRESSION);
        Set<String> gates = new TreeSet<>();
        for (Path p : listGates()) {
            gates.add(p.getFileName().toString().replace(".java", ""));
        }
        gates.add("ReadmeFidelityCountsTest");
        gates.add("RoundTripFidelityDocTest");
        gates.add("PublishedSnippetsCompileTest");
        gates.add("DocumentedJavaSnippetsCompileTest");

        assertThat(gates).as("no gates found at all").hasSizeGreaterThan(8);

        List<String> missing = new ArrayList<>();
        for (String gate : gates) {
            if (!doc.contains(gate)) {
                missing.add(gate);
            }
        }
        assertThat(missing)
                .as("README sends readers to docs/ANTI_REGRESSION.md for 'how the gates and "
                        + "ratchets work', and these gates are absent from it. A gate inventory "
                        + "that omits most of the gates is the document version of a control that "
                        + "does nothing.")
                .isEmpty();
    }

    // ------------------------------------------------------------------ helpers

    /** The most recently recorded "Test count NNNN -> MMMM" figure in the quality baseline. */
    private static int recordedTestCount() {
        Matcher m = Pattern.compile("Test count\\s+(\\d+)\\s*->\\s*(\\d+)")
                .matcher(read(BASELINE));
        // THE FIRST MATCH, NOT THE LAST. The scope blocks in that file are ordered newest first,
        // so scanning to the end returns the OLDEST measurement - which is how this gate reported
        // a two-passes-stale figure as the current one on its first run.
        int last = -1;
        if (m.find()) {
            last = Integer.parseInt(m.group(2));
        }
        if (last < 0) {
            throw new AssertionError("no 'Test count N -> M' line in " + BASELINE
                    + "; the gate cannot compare three numbers to a measurement that is not "
                    + "recorded, and must fail rather than skip");
        }
        return last;
    }

    /**
     * EVERY integer immediately preceding {@code marker}, commas allowed.
     *
     * <p>The marker's own spaces become {@code \s+}, so a line break inside the phrase - which is
     * a formatting choice a Markdown editor makes without thinking - cannot switch this gate off
     * while leaving it green.</p>
     */
    private static List<Integer> allIntsBefore(String doc, String marker) {
        StringBuilder pattern = new StringBuilder("([\\d,]+)");
        for (String word : marker.split(" ")) {
            pattern.append("\\s+").append(Pattern.quote(word));
        }
        List<Integer> found = new ArrayList<>();
        Matcher m = Pattern.compile(pattern.toString()).matcher(doc);
        while (m.find()) {
            found.add(Integer.parseInt(m.group(1).replace(",", "")));
        }
        return found;
    }

    private static List<Path> listGates() {
        Path dir = Paths.get("src/test/java/io/github/pierce/gates");
        try (var walk = Files.walk(dir, 1)) {
            return walk.filter(p -> p.toString().endsWith("Test.java")).toList();
        } catch (IOException e) {
            throw new AssertionError("cannot list " + dir, e);
        }
    }

    private static String read(Path p) {
        try {
            return Files.readString(p, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AssertionError("cannot read " + p, e);
        }
    }
}
