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
    private static final Path README = Paths.get("README.md");
    private static final Path CHANGELOG = Paths.get("CHANGELOG.md");
    private static final Path SUPPORT = Paths.get("SUPPORT.md");
    private static final Path BENCHMARK_WORKFLOW = Paths.get(".github/workflows/benchmark.yml");

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

    // ------------------------------------------------------------------ 3. the install route

    /**
     * Every document that routes a reader to Maven Central agrees with what Central actually has.
     *
     * <p>2.0.0 was tagged, built and attached to a GitHub Release, and never reached Central:
     * {@code release.yml} falls back to {@code mode=artifacts-only} when the three publishing
     * secrets are absent, emits a {@code ::warning::} and still succeeds. A warning is invisible
     * under a green check, so five documents went on telling a stranger that the primary install
     * route worked when the only version a resolver could get was 1.0.8 - the exact version
     * {@code docs/INSTALL.md} tells them not to use.</p>
     *
     * <p>The check is offline by construction. It binds the documents to a measured fact recorded
     * in {@code .github/quality-baseline.json} rather than to the network, because a gate that
     * needs {@code repo1.maven.org} to be reachable is a gate that goes yellow on a train.</p>
     *
     * <p>IT BINDS IN BOTH DIRECTIONS. When the flag is false every document must carry the
     * disclosure; when it flips to true every document must have dropped it. A one-directional
     * version of this check would let the documents stay permanently pessimistic after a
     * successful publish, which is the same drift wearing the opposite sign.</p>
     */
    @Test
    @DisplayName("no document offers Central as a route for a version Central does not have")
    void publishedInstallRoutesMatchTheRecordedCentralState() throws IOException {
        JsonNode baseline = new ObjectMapper().readTree(Files.readString(BASELINE, StandardCharsets.UTF_8));
        JsonNode publishing = baseline.get("publishing");
        assertThat(publishing)
                .as(".github/quality-baseline.json no longer records a 'publishing' block. THE "
                        + "ANCHOR MUST BIND: deleting it would make this gate stop measuring "
                        + "whether the documented install route actually resolves.")
                .isNotNull();

        boolean onCentral = publishing.get("currentReleaseOnMavenCentral").asBoolean();
        String disclosure = publishing.get("unpublishedDisclosure").asText();
        assertThat(disclosure).as("the disclosure sentence must not be empty").isNotBlank();

        // WHITESPACE-TOLERANT, for the reason allIntsBefore already documents below: the sentence
        // wraps, and a line break between "not on" and "Maven Central" is a formatting choice a
        // Markdown editor makes without thinking. The first version of this gate matched the
        // literal and reported SUPPORT.md as non-compliant purely because the phrase spanned two
        // lines - a gate that a reflow can switch off is the failure this class exists to prevent.
        Pattern spaced = Pattern.compile(
                String.join("\\s+", java.util.Arrays.stream(disclosure.trim().split("\\s+"))
                        .map(Pattern::quote).toArray(String[]::new)));

        List<String> wrong = new ArrayList<>();
        for (Path doc : List.of(README, INSTALL, CHANGELOG, SUPPORT)) {
            if (spaced.matcher(read(doc)).find() == onCentral) {
                wrong.add(doc.toString());
            }
        }

        assertThat(wrong)
                .as(onCentral
                        ? "quality-baseline.json records the current release AS PUBLISHED to "
                                + "Maven Central, but these documents still carry the "
                                + "\"%s\" disclosure. Publishing is only half the change - the "
                                + "documents that were corrected while it was unpublished have to "
                                + "be corrected back, or the project now understates itself."
                        : "quality-baseline.json records that the current release is NOT on Maven "
                                + "Central, and these documents do not say so. Each one routes a "
                                + "stranger to a coordinate that 404s on first use. Add the "
                                + "sentence \"%s\" where the document offers the Central route, or "
                                + "publish the release and flip currentReleaseOnMavenCentral.",
                        disclosure)
                .isEmpty();
    }

    /**
     * The documented strength of the performance gate matches the flag the workflow actually runs.
     *
     * <p>{@code compare.py} resolves {@code --allocation auto} to blocking only when the baseline
     * and the current report share a runner class. The committed baseline is a Windows / JDK 21
     * recording and {@code benchmark.yml} installs JDK 17 on {@code ubuntu-latest}, so the
     * cross-runner branch is the only branch CI can take and neither tier blocks there today.
     * Measured on 2026-09-20 by running {@code compare.py} with the workflow's exact flags against
     * a report with every {@code gc.alloc.rate.norm} inflated 50%: exit 0, "No blocking
     * regressions detected across 43 benchmarks". The same report with the baseline's own runner
     * metadata exits 1. The gate is correctly built and correctly scoped; what had drifted was
     * every document describing it.</p>
     *
     * <p>Nothing bound the cross-document claim, which is the lesson {@code ANTI_REGRESSION.md}
     * already records against itself two rows above the one this fixes.</p>
     */
    @Test
    @DisplayName("the documented performance tier matches benchmark.yml's --allocation flag")
    void documentedPerformanceTierMatchesTheWorkflowFlag() {
        String workflow = read(BENCHMARK_WORKFLOW);
        Matcher mode = Pattern.compile("--allocation\\s+(auto|blocking|advisory)").matcher(workflow);
        assertThat(mode.find())
                .as(".github/workflows/benchmark.yml no longer passes an --allocation mode to "
                        + "compare.py. THE ANCHOR MUST BIND: without it this gate cannot tell "
                        + "whether the documented strength is the enforced strength.")
                .isTrue();

        String doc = read(ANTI_REGRESSION);
        // The marker the corrected rows share. Deliberately a phrase, not a row index: rows move.
        boolean discloses = doc.contains("runner class") && doc.contains("BL-026");

        if ("auto".equals(mode.group(1))) {
            assertThat(discloses)
                    .as("benchmark.yml passes --allocation auto and the committed baseline was "
                            + "recorded on a different runner class from CI's, so NEITHER tier "
                            + "blocks a merge today. docs/ANTI_REGRESSION.md is where README sends "
                            + "a reader for 'how the gates and ratchets work' and it must say so: "
                            + "mention the runner-class scoping and cross-reference BL-026, so the "
                            + "row and the backlog item cannot drift apart again.")
                    .isTrue();
        } else {
            assertThat(discloses)
                    .as("benchmark.yml now passes --allocation %s, so the runner-class caveat and "
                            + "the BL-026 cross-reference in docs/ANTI_REGRESSION.md describe a "
                            + "state that no longer exists. A gate documented as weaker than it is "
                            + "is the same defect as one documented as stronger.", mode.group(1))
                    .isFalse();
        }

        // The README's one-line capability list is the copy a stranger actually reads, and it
        // asserted the premise the scoping exists to deny. docs/PERFORMANCE.md states it
        // correctly ("machine-independent, NOT JVM-independent"); the README said it flatly.
        assertThat(read(README))
                .as("README.md still calls the allocation counter machine-independent. The counter "
                        + "is exact for the JVM that produced it and moves across JDK majors - "
                        + "measured at +0.95%% for one Avro Schema.Parser().parse() between "
                        + "Temurin 21.0.7 and 17.0.15, against a 2%% tolerance. benchmarks/"
                        + "README.md retracted this premise; the README is the copy the public "
                        + "reads and must not still assert it.")
                .doesNotContain("machine-independent");
    }

    // ------------------------------------------------------------------ 4. the gate inventory

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
