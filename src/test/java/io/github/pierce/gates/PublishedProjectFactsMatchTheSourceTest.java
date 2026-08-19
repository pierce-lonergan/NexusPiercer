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
 *       and the baseline recorded 2,530. Three documents, three numbers, none current.</li>
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
        int contributing = firstIntBefore(read(CONTRIBUTING), "test invocations");
        int install = firstIntBefore(read(INSTALL), "tests, about");
        int recorded = recordedTestCount();

        assertThat(contributing).as("CONTRIBUTING.md no longer states a suite size").isPositive();
        assertThat(install).as("docs/INSTALL.md no longer states a suite size").isPositive();
        assertThat(recorded).as("no test count recorded in the quality baseline").isPositive();

        assertThat(List.of(contributing, install))
                .as("THREE DOCUMENTS, THREE NUMBERS. CONTRIBUTING.md says %d, docs/INSTALL.md says "
                        + "%d, and .github/quality-baseline.json records %d. Take the number from "
                        + "the most recent measurement; a suite size is the one figure a new "
                        + "contributor uses to decide whether their run looks right.",
                        contributing, install, recorded)
                .containsOnly(recorded);
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

    /** The integer immediately preceding {@code marker}, commas allowed. */
    private static int firstIntBefore(String doc, String marker) {
        Matcher m = Pattern.compile("([\\d,]+)\\s+" + Pattern.quote(marker)).matcher(doc);
        return m.find() ? Integer.parseInt(m.group(1).replace(",", "")) : -1;
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
