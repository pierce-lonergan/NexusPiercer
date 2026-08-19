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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code docs/ARCHITECTURE_GRAPH.md} must not claim a dependency the code does not have.
 *
 * <p>WHY THIS EXISTS. The 2.1.0 repoint of {@code AvroSchemaFlattener} from {@code FileFinder}
 * onto {@code SchemaFiles} falsified the edge {@code ASF --> FF} and the registry row
 * {@code AvroSchemaFlattener | DEPENDS_ON | FileFinder}, and nothing went red, because no test
 * had ever read this file. Adversarial review found it by hand. Measurement then found four MORE
 * edges that had been false for longer - {@code GASF --> ASF}, {@code CSFS --> ASF},
 * {@code NPSP --> NPF} and {@code ASC --> TCR} - none of which any reader could have known were
 * stale. A diagram nobody checks is not documentation, it is a claim.</p>
 *
 * <p>WHAT IS CHECKED, AND WHAT IS DELIBERATELY NOT. Only edges whose BOTH ends resolve to a real
 * top-level class under {@code src/main/java} are checked; nodes like {@code JSON Sources},
 * {@code POI} or {@code Flattening Engine} have no file and are skipped. The assertion is that
 * the source file mentions the target's simple name - deliberately weak, because a stronger
 * import-only rule would false-positive on a fully-qualified use and this gate must not itself
 * become the thing people work around. It catches a dependency that is GONE, which is the failure
 * mode this file actually has.</p>
 *
 * <p>MISSING edges are not checked. A graph is allowed to be a summary; it is not allowed to
 * lie. The two classes the graph was measured to be missing entirely are pinned separately
 * below, because they were absent while being named the default choice elsewhere in the docs.</p>
 */
@DisplayName("every class-to-class edge in the architecture graph is a dependency that exists")
class ArchitectureGraphEdgesAreRealTest {

    private static final Path GRAPH = Paths.get("docs", "ARCHITECTURE_GRAPH.md");
    private static final Path MAIN = Paths.get("src", "main", "java");

    /** {@code ALIAS[Label]} or {@code ALIAS[Label]} appearing inline in an edge. */
    private static final Pattern NODE = Pattern.compile("\\b([A-Za-z_][A-Za-z0-9_]*)\\[([^\\]|]+)\\]");

    /** {@code A --> B}, {@code A --> |"x"| B}, {@code A -->|"x"| B}, with optional inline labels. */
    private static final Pattern EDGE = Pattern.compile(
            "([A-Za-z_][A-Za-z0-9_]*)(?:\\[[^\\]]*\\])?\\s*-->\\s*(?:\\|[^|]*\\|\\s*)?"
                    + "([A-Za-z_][A-Za-z0-9_]*)(?:\\[[^\\]]*\\])?");

    /** {@code | Source | RELATIONSHIP | Target | Evidence |}. */
    private static final Pattern ROW = Pattern.compile(
            "^\\|\\s*([^|]+?)\\s*\\|\\s*([A-Z_]+)\\s*\\|\\s*([^|]+?)\\s*\\|");

    private static Map<String, Path> mainClasses() throws IOException {
        Map<String, Path> byName = new HashMap<>();
        try (Stream<Path> files = Files.walk(MAIN)) {
            files.filter(p -> p.getFileName().toString().endsWith(".java")).forEach(p -> {
                String name = p.getFileName().toString();
                byName.put(name.substring(0, name.length() - ".java".length()), p);
            });
        }
        return byName;
    }

    private static String read(Path p) throws IOException {
        return new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("no mermaid edge names a dependency the source file no longer has")
    void mermaidEdgesResolve() throws IOException {
        String doc = read(GRAPH);
        Map<String, Path> classes = mainClasses();

        Map<String, String> alias = new LinkedHashMap<>();
        Matcher nodes = NODE.matcher(doc);
        while (nodes.find()) {
            alias.put(nodes.group(1), nodes.group(2).trim());
        }

        List<String> broken = new ArrayList<>();
        Matcher edges = EDGE.matcher(doc);
        while (edges.find()) {
            String from = alias.getOrDefault(edges.group(1), edges.group(1));
            String to = alias.getOrDefault(edges.group(2), edges.group(2));
            Path src = classes.get(from);
            if (src == null || !classes.containsKey(to)) {
                continue;
            }
            if (!read(src).matches("(?s).*\\b" + Pattern.quote(to) + "\\b.*")) {
                broken.add(from + " --> " + to + "  (no occurrence of '" + to + "' in " + src + ")");
            }
        }

        assertTrue(broken.isEmpty(),
                "docs/ARCHITECTURE_GRAPH.md claims dependencies the code does not have:\n  "
                        + String.join("\n  ", broken)
                        + "\nFix the diagram, not this test.");
    }

    @Test
    @DisplayName("no Relationship Registry row names a dependency the source file no longer has")
    void registryRowsResolve() throws IOException {
        Map<String, Path> classes = mainClasses();
        List<String> broken = new ArrayList<>();

        for (String line : read(GRAPH).split("\r?\n")) {
            Matcher row = ROW.matcher(line);
            if (!row.find()) {
                continue;
            }
            String from = row.group(1).trim();
            String to = row.group(3).trim();
            Path src = classes.get(from);
            if (src == null || !classes.containsKey(to)) {
                continue;
            }
            if (!read(src).matches("(?s).*\\b" + Pattern.quote(to) + "\\b.*")) {
                broken.add(from + " " + row.group(2) + " " + to
                        + "  (no occurrence of '" + to + "' in " + src + ")");
            }
        }

        assertTrue(broken.isEmpty(),
                "the Relationship Registry claims dependencies the code does not have:\n  "
                        + String.join("\n  ", broken)
                        + "\nFix the table, not this test.");
    }

    @Test
    @DisplayName("the graph names the schema reader and the current schema flattener")
    void doesNotOmitTheClassesTheDocsCallDefault() throws IOException {
        String doc = read(GRAPH);
        for (String required : new String[] {"SchemaFiles", "EnrichedSchemaFlattener"}) {
            assertTrue(doc.contains(required),
                    "docs/ARCHITECTURE_GRAPH.md does not mention " + required
                            + ", which README names as the thing to reach for. Both were measured "
                            + "absent in 2.1.0 review while FileFinder and AvroSchemaFlattener - "
                            + "the classes they replace - were drawn.");
        }
    }
}
