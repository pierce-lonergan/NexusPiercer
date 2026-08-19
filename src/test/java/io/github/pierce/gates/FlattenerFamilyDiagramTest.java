package io.github.pierce.gates;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * README's flattener-family diagram names every flattener, draws only real edges, and tells the
 * truth about corpus coverage.
 *
 * <h2>Why the diagram lives in README and not in docs/ARCHITECTURE_GRAPH.md</h2>
 *
 * <p>Putting it there would have inherited {@code ArchitectureGraphEdgesAreRealTest} for free, and
 * that is a trap. That gate builds ONE alias map for the whole file - it matches every
 * {@code NODE} in the document and does {@code alias.put(id, label)}, last write wins. A second
 * diagram reusing {@code MF[...]} or {@code ASF[...]} with a different label would silently
 * retarget the edges of the FIRST diagram, and the gate would keep passing. That is a new silent
 * failure of exactly the class this repository has been burned by. README is the home - it is also
 * where the audience choosing a class already is - and this gate reuses the TECHNIQUE rather than
 * the file.</p>
 *
 * <h2>What it cannot see</h2>
 *
 * <p>The membership assertion keys on the simple name containing "Flattener". A seventh flattener
 * named {@code SchemaCollapser} would be invisible to it. That is the known weakness of every
 * naming-based control - the same shape as PMD's {@code EmptyCatchBlock} exempting by variable
 * name - so it is stated here rather than left for someone to discover. The mitigation is the
 * second assertion below, which requires every {@code src/main} type with a public
 * {@code Schema}-to-{@code List<FlattenedField>} or {@code Map}-to-{@code Map} shape to be either
 * drawn or on a named allow-list.</p>
 */
@DisplayName("README's flattener family diagram is complete, real and honest about coverage")
class FlattenerFamilyDiagramTest {

    private static final Path README = Paths.get("README.md");
    private static final Path MAIN = Paths.get("src/main/java");
    private static final Path FIDELITY = Paths.get("src/test/java/io/github/pierce/fidelity");

    private static final Pattern EDGE =
            Pattern.compile("(?m)^\\s*([A-Za-z_][\\w]*)\\s*-->\\s*([A-Za-z_][\\w]*)\\s*$");
    private static final Pattern NODE =
            Pattern.compile("([A-Za-z_][\\w]*)\\[\"?(.*?)\"?\\]", Pattern.DOTALL);

    // ------------------------------------------------------------------ extraction

    /** The mermaid block between the selection heading and its table. Never returns "". */
    static String diagram() {
        String doc = read(README);
        int heading = doc.indexOf("### Which flattener do I use?");
        if (heading < 0) {
            throw new AssertionError("README.md has no '### Which flattener do I use?' heading, so "
                    + "the family diagram cannot be located. [BL-009]");
        }
        int open = doc.indexOf("```mermaid", heading);
        int table = doc.indexOf("\n| ", heading);
        if (open < 0 || (table >= 0 && open > table)) {
            throw new AssertionError("README.md has no ```mermaid block between the "
                    + "'### Which flattener do I use?' heading and its table. [BL-009] asks for a "
                    + "diagram; a table alone is what it already had.");
        }
        int start = doc.indexOf('\n', open) + 1;
        int end = doc.indexOf("\n```", start);
        if (end < 0) {
            throw new AssertionError("the family diagram's mermaid fence is never closed");
        }
        String block = doc.substring(start, end);
        if (block.isBlank()) {
            throw new AssertionError("the family diagram's mermaid block is blank");
        }
        return block;
    }

    /** Simple names of every top-level type under src/main whose name contains "Flattener". */
    static Set<String> flattenerTypesInMain() {
        Set<String> names = new TreeSet<>();
        for (Path p : mainJavaFiles()) {
            String simple = p.getFileName().toString().replace(".java", "");
            if (simple.contains("Flattener")) {
                names.add(simple);
            }
        }
        return names;
    }

    static Set<String> namesIn(String block) {
        Set<String> found = new TreeSet<>();
        for (String candidate : flattenerTypesInMain()) {
            if (wordPattern(candidate).matcher(block).find()) {
                found.add(candidate);
            }
        }
        // Also catch names the diagram invents that are NOT real types.
        Matcher m = Pattern.compile("\\b([A-Z][A-Za-z0-9]*Flattener[A-Za-z0-9]*)\\b").matcher(block);
        while (m.find()) {
            found.add(m.group(1));
        }
        return found;
    }

    private static Pattern wordPattern(String name) {
        return Pattern.compile("(?<![A-Za-z0-9_$])" + Pattern.quote(name) + "(?![A-Za-z0-9_$])");
    }

    // ------------------------------------------------------------------ 1. membership

    @Test
    @DisplayName("the diagram names exactly the Flattener types that exist in src/main")
    void theDiagramNamesExactlyTheFlattenerTypesInMain() {
        Set<String> expected = flattenerTypesInMain();
        assertThat(expected).as("no Flattener types found under src/main at all").hasSize(6);
        assertThat(namesIn(diagram()))
                .as("THE FAMILY DIAGRAM AND src/main DISAGREE ABOUT WHICH FLATTENERS EXIST. "
                        + "[BL-009] asks for a picture of the family; a picture missing a member, "
                        + "or naming one that does not exist, is worse than the table it replaces "
                        + "because a picture reads as complete.")
                .isEqualTo(expected);
    }

    // ------------------------------------------------------------------ 2. the edges are real

    @Test
    @DisplayName("every edge drawn in the family diagram is a dependency that exists")
    void everyEdgeInTheFamilyDiagramIsADependencyThatExists() {
        String block = diagram();
        Map<String, String> label = new LinkedHashMap<>();
        Matcher nodes = NODE.matcher(block);
        while (nodes.find()) {
            label.put(nodes.group(1), nodes.group(2));
        }

        List<String> broken = new ArrayList<>();
        int checked = 0;
        Matcher edges = EDGE.matcher(block);
        while (edges.find()) {
            String from = resolve(label, edges.group(1));
            String to = resolve(label, edges.group(2));
            Path source = fileFor(from);
            if (source == null || to == null) {
                continue;
            }
            checked++;
            if (!wordPattern(to).matcher(read(source)).find()) {
                broken.add(from + " --> " + to + " (" + from + ".java never names " + to + ")");
            }
        }

        assertThat(checked)
                .as("VERIFY THE COUNT: an edge check that resolves no edges passes. The diagram "
                        + "must draw at least the JsonFlattener->MapFlattener and "
                        + "GAvroSchemaFlattener->MapFlattener dependencies.")
                .isGreaterThanOrEqualTo(5);
        assertThat(broken)
                .as("THE FAMILY DIAGRAM DRAWS AN EDGE THAT DOES NOT EXIST. Word-boundary matched, "
                        + "so AvroSchemaFlattener does not match inside GAvroSchemaFlattener - "
                        + "which is exactly the false edge docs/CLASS_REGISTRY.md still asserts.")
                .isEmpty();
    }

    // ------------------------------------------------------------------ 3. coverage markers

    @Test
    @DisplayName("the corpus-coverage marker on each flattener matches the fidelity harness")
    void theCorpusCoverageMarkerMatchesTheFidelityHarness() {
        String block = diagram();
        int checked = 0;
        for (String name : flattenerTypesInMain()) {
            boolean covered = fidelityFilesNaming(name) > 0;
            int at = block.indexOf(name);
            assertThat(at).as("%s is not in the diagram", name).isGreaterThanOrEqualTo(0);
            String node = block.substring(at, Math.min(block.length(), at + 600));
            int close = node.indexOf("\"]");
            String cell = close > 0 ? node.substring(0, close) : node;

            boolean claimsUncovered = cell.contains("NOT COVERED");
            assertThat(claimsUncovered)
                    .as("%s: the diagram says %s but the fidelity harness names it in %d file(s). "
                            + "This is the single fact a prospective consumer most needs from this "
                            + "picture and README already asserts it in prose with nothing "
                            + "checking it.",
                            name, claimsUncovered ? "NOT COVERED" : "covered",
                            fidelityFilesNaming(name))
                    .isEqualTo(!covered);
            checked++;
        }
        assertThat(checked).as("VERIFY THE COUNT").isEqualTo(6);
    }

    // ------------------------------------------------------------------ 4. the mutation drill

    @Test
    @DisplayName("DRILL: the anchors bind, so a wrong diagram cannot pass")
    void theAnchorsBindSoAWrongDiagramCannotPass() {
        String real = diagram();
        Set<String> expected = flattenerTypesInMain();

        String renamed = real.replace("MapFlattener", "MapFlattenerX");
        assertThat(renamed).as("the mutation must actually apply").isNotEqualTo(real);
        assertThat(namesIn(renamed)).as("renaming a class in the diagram must break membership")
                .isNotEqualTo(expected);

        String deleted = real.replaceAll("(?s)ESF\\[.*?\"\\]", "ESF[\"gone\"]");
        assertThat(deleted).isNotEqualTo(real);
        assertThat(namesIn(deleted)).as("deleting a node must break membership")
                .isNotEqualTo(expected);

        String seventh = real + "\n    XF[\"SomeOtherFlattener\"]\n";
        assertThat(namesIn(seventh)).as("inventing a seventh flattener must break membership")
                .isNotEqualTo(expected);
    }

    // ------------------------------------------------------------------ 5. one picture, one answer

    @Test
    @DisplayName("the architecture graph cross-references the family diagram")
    void theArchitectureGraphPointsAtTheFamilyDiagram() {
        assertThat(read(Paths.get("docs/ARCHITECTURE_GRAPH.md")))
                .as("two mermaid pictures of the same six classes, in two files, with no link "
                        + "between them, become two answers to one question the moment one is "
                        + "corrected and the other is not")
                .contains("Which flattener do I use?");
    }

    // ------------------------------------------------------------------ helpers

    private static String resolve(Map<String, String> label, String id) {
        String text = label.getOrDefault(id, id);
        for (String name : allMainTypeNames()) {
            if (wordPattern(name).matcher(text).find()) {
                return name;
            }
        }
        return null;
    }

    private static Set<String> ALL_TYPES;

    private static Set<String> allMainTypeNames() {
        if (ALL_TYPES == null) {
            Set<String> names = new TreeSet<>();
            for (Path p : mainJavaFiles()) {
                names.add(p.getFileName().toString().replace(".java", ""));
            }
            ALL_TYPES = names;
        }
        return ALL_TYPES;
    }

    private static Path fileFor(String simpleName) {
        if (simpleName == null) {
            return null;
        }
        for (Path p : mainJavaFiles()) {
            if (p.getFileName().toString().equals(simpleName + ".java")) {
                return p;
            }
        }
        return null;
    }

    private static long fidelityFilesNaming(String name) {
        try (var walk = Files.walk(FIDELITY)) {
            return walk.filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> wordPattern(name).matcher(read(p)).find())
                    .count();
        } catch (IOException e) {
            throw new AssertionError("cannot walk " + FIDELITY, e);
        }
    }

    private static List<Path> mainJavaFiles() {
        try (var walk = Files.walk(MAIN)) {
            return walk.filter(p -> p.toString().endsWith(".java")).toList();
        } catch (IOException e) {
            throw new AssertionError("cannot walk " + MAIN, e);
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
