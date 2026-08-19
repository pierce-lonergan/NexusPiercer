package io.github.pierce.gates;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The literal {@code Imports:} trees in {@code docs/DEPENDENCY_MAP.md} must be real imports.
 *
 * <h2>Why this exists</h2>
 *
 * <p>The 2.1.0 repoint of {@code NexusPiercerSparkPipeline} and {@code AvroSchemaFlattener} from
 * {@code FileFinder} onto {@code SchemaFiles} falsified four statements in this one file. A
 * documentation sweep corrected THREE of them - the header, the coupling table row and a prose
 * bullet - and left the two {@code Imports:} trees, which are the most literal statement of the
 * false edge, saying {@code io.github.pierce.files.FileFinder}. The header it stamped in the same
 * diff reads "re-measured against src/main", asserting a measurement that did not reach them.</p>
 *
 * <p>{@link ArchitectureGraphEdgesAreRealTest} covers {@code docs/ARCHITECTURE_GRAPH.md} and was
 * written after the same class of defect there. This is the second document where a FileFinder
 * edge survived a sweep because no test read it. A prose sweep is a person re-reading; a gate is
 * the machine re-reading, and only one of them scales to the next repoint.</p>
 *
 * <h2>What is checked, and what deliberately is not</h2>
 *
 * <p>Only {@code io.github.pierce.*} lines are checked, and only where the heading names a class
 * that exists under {@code src/main/java}. Third-party lines are frequently wildcards
 * ({@code org.apache.spark.sql.*}, {@code java.util.*}) that summarise several real imports, and
 * a gate that failed on those would be worked around rather than obeyed. An in-repo import is
 * exact, checkable, and is the kind that goes stale on a refactor.</p>
 *
 * <p>MISSING imports are not checked. A tree is allowed to be a summary; it is not allowed to
 * name a dependency that is gone.</p>
 */
@DisplayName("every in-repo import tree in the dependency map is an import that exists")
class DependencyMapImportTreesAreRealTest {

    private static final Path DOC = Paths.get("docs", "DEPENDENCY_MAP.md");
    private static final Path MAIN = Paths.get("src", "main", "java");

    private static Map<String, Path> mainClasses() throws IOException {
        Map<String, Path> byName = new HashMap<>();
        try (Stream<Path> files = Files.walk(MAIN)) {
            files.filter(p -> p.getFileName().toString().endsWith(".java")).forEach(p -> {
                String n = p.getFileName().toString();
                byName.put(n.substring(0, n.length() - ".java".length()), p);
            });
        }
        return byName;
    }

    /** Every {@code import io.github...} actually declared by a source file. */
    private static Set<String> declaredImports(Path src) throws IOException {
        Set<String> out = new LinkedHashSet<>();
        for (String line : Files.readString(src, StandardCharsets.UTF_8).split("\r?\n")) {
            String t = line.strip();
            if (t.startsWith("import io.github") && t.endsWith(";")) {
                out.add(t.substring("import ".length(), t.length() - 1).replace("static ", "").strip());
            }
        }
        return out;
    }

    @Test
    @DisplayName("no Imports: tree names an io.github dependency the source file no longer has")
    void importTreesResolve() throws IOException {
        Map<String, Path> classes = mainClasses();
        String[] lines = Files.readString(DOC, StandardCharsets.UTF_8).split("\r?\n");

        List<String> broken = new ArrayList<>();
        int checked = 0;
        String heading = null;
        boolean inTree = false;

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            String t = line.strip();

            if (t.startsWith("#")) {
                heading = t.replaceAll("^#+\\s*", "").strip();
                inTree = false;
                continue;
            }
            if (t.equals("```")) {
                // The tree opens with a bare fence and closes with one; `Imports:` on the next
                // line is what distinguishes it from every other fenced block in this document.
                inTree = !inTree && i + 1 < lines.length && lines[i + 1].strip().equals("Imports:");
                continue;
            }
            if (!inTree || heading == null) {
                continue;
            }

            Path src = classes.get(heading);
            if (src == null) {
                continue;
            }
            String entry = t.replaceAll("^[\\u2500\\u251c\\u2514\\u2502|`+\\-\\s]+", "").strip();
            if (!entry.startsWith("io.github") || entry.endsWith("*")) {
                continue;
            }
            checked++;
            if (!declaredImports(src).contains(entry)) {
                broken.add(DOC + ":" + (i + 1) + "  " + heading + " does not import " + entry
                        + "  (it imports " + declaredImports(src) + ")");
            }
        }

        assertTrue(checked > 3,
                "VERIFY THE COUNT: this gate is a loop over parsed tree entries and a parser that "
                        + "matches nothing passes silently - exactly the failure it exists to "
                        + "catch. Only " + checked + " in-repo import lines were read from " + DOC
                        + "; the fence or heading syntax has changed.");

        assertTrue(broken.isEmpty(),
                "docs/DEPENDENCY_MAP.md names imports the code does not have:\n  "
                        + String.join("\n  ", broken)
                        + "\nFix the document, not this test.");
    }
}
