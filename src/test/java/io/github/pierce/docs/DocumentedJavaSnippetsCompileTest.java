package io.github.pierce.docs;

import io.github.pierce.docs.DocSnippetSource.Kind;
import io.github.pierce.docs.DocSnippetSource.Snippet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every published Java snippet in every tracked markdown file compiles - and none of them can
 * escape by being awkward.
 *
 * <h2>The defect this closes</h2>
 *
 * <p>{@code docs/SPARK_PIPELINE.md} published six calls to four {@code NexusPiercerPatterns}
 * methods that have never existed. That was filed as OSS-01 and "fixed" by adding a warning
 * banner. The banner named the two REAL methods with signatures that were also invented, and the
 * same commit published a third instance of the same defect in {@code README.md} and a fourth in
 * the {@code NexusPiercerPatterns} class javadoc - which {@code CHANGELOG.md} then recorded as a
 * correction. Hand-correcting unchecked prose has now demonstrably failed in this repository, in
 * the same place, twice. The gate is the fix; rewriting the snippets is only what the gate
 * demanded first.</p>
 *
 * <h2>Five properties, all of them tested here</h2>
 *
 * <ol>
 *   <li>DEFAULT DENY. A java block with no directive FAILS. It is not skipped. This single
 *       property is the gate; the rest is bookkeeping that stops it being hollowed out.</li>
 *   <li>PSEUDO IS RATCHETED. Every escape carries a reason of at least
 *       {@value DocSnippetSource#MIN_REASON_CHARS} characters, and the TOTAL number of escapes is
 *       asserted equal to a recorded constant. Adding one turns the build red until a human
 *       raises the number in the diff - the same doctrine as
 *       {@code .github/quality-baseline.json}.</li>
 *   <li>TEMPLATES MAY DECLARE VARIABLES, NEVER METHODS OR TYPES. Drilled adversarially below. A
 *       stubbed helper makes a phantom API compile, which is the whole defect relocated into the
 *       gate.</li>
 *   <li>DEAD AND UNDEFINED ENVIRONMENTS BOTH FAIL. An unused template is the residue of somebody
 *       widening the hatch for one snippet and leaving the hole open.</li>
 *   <li>FILE-LEVEL EXEMPTIONS ARE ASSERTED BY SET EQUALITY. Containment would let a third file in
 *       silently.</li>
 * </ol>
 *
 * <h2>What this gate does NOT prove</h2>
 *
 * <p>TYPE-CORRECTNESS ONLY. A snippet can compile and still be wrong: the output tables in
 * {@code docs/SPARK_PIPELINE.md}, the comment naming a constructor argument in {@code README.md},
 * and every claim about what a value will be are invisible to javac. Green here means "a reader
 * who pastes this will not get a compile error"; it does not mean the document is correct.</p>
 */
@DisplayName("Every published Java snippet compiles, and none can escape the gate quietly")
class DocumentedJavaSnippetsCompileTest {

    /**
     * Java blocks per tracked file. Measured, not estimated; update deliberately.
     *
     * <p>VERIFY THE COUNT. Gut the examples out of a document and a parameterized compile test
     * runs zero invocations and passes green - the repository's signature pathology, which
     * {@code PublishedSnippetsCompileTest}'s own javadoc calls out. These constants are what turn
     * "the snippets all compiled" into "all of the snippets compiled".</p>
     */
    private static final Map<String, Integer> EXPECTED_BLOCKS = new TreeMap<>(Map.of(
            "CHANGELOG.md", 1,
            "README.md", 8,
            "docs/JSON_FLATTENER_CONSOLIDATOR.md", 14,
            "docs/ROUND_TRIP_FIDELITY.md", 5,
            "docs/SPARK_PIPELINE.md", 18,
            "docs/audit/FINDINGS.md", 20,
            "src/main/java/io/github/pierce/converter/README.md", 3,
            "src/main/java/io/github/pierce/converter/RESEARCH_README.md", 13));

    /**
     * The escape-hatch ratchet. Pseudo is the ONLY way a published block avoids the compiler, so
     * it is the only place this gate can be hollowed out without deleting a test.
     */
    private static final int EXPECTED_PSEUDO_BLOCKS = 3;

    /** A named upper bound, so the hatch cannot grow quietly even if somebody edits the number. */
    private static final int PSEUDO_UPPER_BOUND = 8;

    private static Path out() {
        return DocSnippetSource.moduleRoot().resolve("target/doc-snippets");
    }

    @AfterAll
    static void releaseTheSharedCompilerFileManager() {
        DocSnippetCompiler.closeFiles();
    }

    // ------------------------------------------------------------------ 1. it compiles

    static Stream<org.junit.jupiter.params.provider.Arguments> compilableBlocks() {
        List<Snippet> all = DocSnippetSource.blocks();
        List<org.junit.jupiter.params.provider.Arguments> rows = new ArrayList<>();
        for (int i = 0; i < all.size(); i++) {
            Snippet s = all.get(i);
            if (s.kind() != Kind.PSEUDO) {
                rows.add(org.junit.jupiter.params.provider.Arguments.of(s.where(), s, i));
            }
        }
        if (rows.isEmpty()) {
            throw new AssertionError("DOC SNIPPET GATE DID NOT RUN: zero compilable blocks. A "
                    + "parameterized test with no parameters passes, and it proves nothing.");
        }
        return rows.stream();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("compilableBlocks")
    @DisplayName("every published java block compiles against the real test classpath")
    void everyPublishedJavaBlockCompiles(String where, Snippet s, int ordinal) {
        String source = DocSnippetCompiler.wrap(s, ordinal);
        String unit = DocSnippetCompiler.unitName(s, ordinal, source);
        List<String> errors = DocSnippetCompiler.errors(unit, source, out());
        assertThat(errors)
                .as("THE JAVA BLOCK PUBLISHED AT %s DOES NOT COMPILE.\n"
                        + "A reader who pastes it gets these errors:\n  %s\n"
                        + "Directive: %s%s. Rewrite the snippet against the API that exists, or - "
                        + "if it genuinely is not Java - mark it pseudo with a stated reason, "
                        + "which is counted.\n"
                        + "----- what was compiled -----\n%s",
                        s.where(), String.join("\n  ", errors), s.kind(),
                        s.env() == null ? "" : " env=" + s.env(), source)
                .isEmpty();
    }

    // ------------------------------------------------------------------ 2. default deny

    @Test
    @DisplayName("every java block outside the exempt files carries a parseable directive")
    void everyJavaBlockCarriesADirective() {
        // DocSnippetSource throws on a missing directive, so reaching the assertion at all is
        // most of the proof. The count is asserted too, because "it did not throw" is also what
        // an extractor that found nothing would report.
        List<Snippet> blocks = DocSnippetSource.blocks();
        assertThat(blocks).as("no gated blocks at all").isNotEmpty();
        for (Snippet s : blocks) {
            assertThat(s.kind()).as("%s parsed to a null kind", s.where()).isNotNull();
        }

        List<String> undirected = List.of(
                "Some prose.",
                "```java",
                "int x = 1;",
                "```");
        assertThatThrownBy(() -> DocSnippetSource.parse("synthetic.md", undirected, true))
                .as("A JAVA BLOCK WITH NO DIRECTIVE MUST FAIL, NEVER BE SKIPPED. Without this "
                        + "property every future snippet escapes simply by not opting in, which "
                        + "is exactly how the six phantom Spark calls survived four passes.")
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("no <!-- snippet:");

        assertThat(DocSnippetSource.parse("synthetic.md", undirected, false))
                .as("CONTROL: the same block parses when the file is exempt, so the failure above "
                        + "is the missing directive and not a broken extractor")
                .hasSize(1);
    }

    // ------------------------------------------------------------------ 3. the wrapper drill

    @Test
    @DisplayName("no wrapper template can make a phantom method compile")
    void aWrapperTemplateCannotMakeAPhantomMethodCompile() {
        int drilled = 0;
        for (String env : SnippetEnvironments.names()) {
            for (String phantom : List.of(
                    "NexusPiercerPatterns.jsonToDelta(spark, \"s.avsc\", \"in\", \"out\");",
                    "java.util.List<String> x = noSuchHelperAtAll(\"x\");")) {
                Snippet fake = new Snippet("synthetic.md", 1, Kind.BODY, env, null, phantom);
                String source = DocSnippetCompiler.wrap(fake, 900000 + drilled);
                assertThat(DocSnippetCompiler.errors("DocSnippet_" + (900000 + drilled), source, out()))
                        .as("ENV '%s' ACCEPTED A PHANTOM. A template that predeclares a stub "
                                + "method or a stub type makes a non-existent API compile: the "
                                + "gate goes green while the document advertises something nobody "
                                + "can call. That is the defect this gate exists to catch, "
                                + "relocated into the gate.", env)
                        .isNotEmpty();
                drilled++;
            }

            SnippetEnvironments.assertRegionDeclaresNoMethodOrType(
                    env, "LOCALS", SnippetEnvironments.locals(env));
            String imports = SnippetEnvironments.imports(env);
            assertThat(imports.lines().map(String::strip).filter(l -> !l.isEmpty()))
                    .as("env '%s' IMPORTS region must contain nothing but import statements", env)
                    .allMatch(l -> l.startsWith("import ") || l.startsWith("//"));
        }
        assertThat(drilled)
                .as("VERIFY THE COUNT: this drill is a loop over the environment set, and an "
                        + "empty set means zero assertions and a green test. If the templates "
                        + "were ever reduced to none, that is what this catches.")
                .isEqualTo(SnippetEnvironments.names().size() * 2)
                .isGreaterThan(0);

        // CONTROL, in the other direction: the same wrapper compiles something real, so the
        // drill above proves the templates reject phantoms rather than rejecting everything.
        Snippet real = new Snippet("synthetic.md", 1, Kind.BODY, "core", null,
                "MapFlattener f = MapFlattener.builder().build();\n"
                        + "Map<String, Object> flat = f.flatten(sourceMap);");
        assertThat(DocSnippetCompiler.errors("DocSnippet_910000",
                DocSnippetCompiler.wrap(real, 910000), out()))
                .as("CONTROL: env 'core' must still compile a real call. A gate that rejects "
                        + "everything looks identical from the outside to one that rejects the "
                        + "right things.")
                .isEmpty();
    }

    // ------------------------------------------------------------------ 4. the compiler is real

    @Test
    @DisplayName("the compiler and the classpath are real, proved in both directions")
    void theCompilerAndClasspathAreRealBothWays() {
        assertThat(DocSnippetCompiler.javac())
                .as("running under a JRE, not a JDK")
                .isNotNull();

        Path cp = DocSnippetSource.moduleRoot().resolve("target/test-cp.txt");
        assertThat(cp).as("target/test-cp.txt is written by the maven-dependency-plugin "
                + "'build-test-classpath' execution at generate-test-resources").exists();

        String entries = DocSnippetCompiler.classpath();
        assertThat(entries).isNotBlank();
        List<String> missing = new ArrayList<>();
        for (String e : entries.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
            if (!e.isBlank() && !Files.exists(Path.of(e))) {
                missing.add(e);
            }
        }
        assertThat(missing).as("every classpath entry must resolve to a real file; a gate "
                + "compiling against a phantom classpath fails everything, and the repair a "
                + "hurried reader reaches for is to stop failing").isEmpty();

        Snippet good = new Snippet("synthetic.md", 1, Kind.BODY, "core", null,
                "new JsonFlattenerConsolidator(\",\", null, 50, 1000, false)"
                        + ".flattenAndConsolidateJson(\"{}\");");
        assertThat(DocSnippetCompiler.errors("DocSnippet_920000",
                DocSnippetCompiler.wrap(good, 920000), out()))
                .as("KNOWN-GOOD control: a real call on a real class must compile. If this fails "
                        + "the classpath is wrong and every other result in this class is noise.")
                .isEmpty();

        Snippet bad = new Snippet("synthetic.md", 1, Kind.BODY, "core", null,
                "new JsonFlattenerConsolidator(\",\", null, 50, 1000, false).noSuchMethod();");
        assertThat(DocSnippetCompiler.errors("DocSnippet_920001",
                DocSnippetCompiler.wrap(bad, 920001), out()))
                .as("KNOWN-BAD control: a gate that can only ever pass and a gate that can only "
                        + "ever fail look the same from the outside. Both directions are drilled.")
                .isNotEmpty();
    }

    // ------------------------------------------------------------------ 5. the escape hatch

    @Test
    @DisplayName("pseudo blocks are reasoned, counted and bounded")
    void pseudoBlocksAreReasonedAndRatcheted() {
        List<Snippet> pseudo = DocSnippetSource.blocks().stream()
                .filter(s -> s.kind() == Kind.PSEUDO).toList();
        for (Snippet s : pseudo) {
            assertThat(s.reason()).as("%s", s.where()).isNotBlank();
            assertThat(s.reason().length())
                    .as("%s carries a %d-character pseudo reason", s.where(), s.reason().length())
                    .isGreaterThanOrEqualTo(DocSnippetSource.MIN_REASON_CHARS);
        }
        assertThat(pseudo)
                .as("THE ESCAPE HATCH CHANGED SIZE. Pseudo is the only way a published block "
                        + "avoids the compiler, so the count is ratcheted like a static-analysis "
                        + "ceiling: adding one turns the build red until a human raises the "
                        + "number in the diff and has to say why. Currently escaping:\n  %s",
                        pseudo.stream().map(Snippet::where).toList())
                .hasSize(EXPECTED_PSEUDO_BLOCKS);
        assertThat(EXPECTED_PSEUDO_BLOCKS)
                .as("the recorded pseudo count itself must stay small; a hatch big enough to "
                        + "hold every awkward snippet is not a hatch, it is the door")
                .isLessThanOrEqualTo(PSEUDO_UPPER_BOUND);
    }

    // ------------------------------------------------------------------ 6. verify the count

    @Test
    @DisplayName("the java-block count per file matches the recorded count")
    void theBlockCountPerFileMatchesTheRecordedCount() {
        Map<String, Integer> measured = new TreeMap<>();
        for (String file : DocSnippetSource.trackedMarkdown()) {
            int n = DocSnippetSource.blocksIn(file).size();
            if (n > 0) {
                measured.put(file, n);
            }
        }
        assertThat(measured)
                .as("A document that loses its examples loses its coverage silently: the "
                        + "parameterized compile test above simply runs fewer times and stays "
                        + "green. VERIFY THE COUNT, never the exit code.")
                .isEqualTo(new TreeMap<>(EXPECTED_BLOCKS));

        assertThat(DocSnippetSource.blocks().size())
                .as("gated blocks = all blocks minus the two exempt files")
                .isEqualTo(EXPECTED_BLOCKS.values().stream().mapToInt(Integer::intValue).sum()
                        - EXPECTED_BLOCKS.get("docs/audit/FINDINGS.md")
                        - EXPECTED_BLOCKS.get("src/main/java/io/github/pierce/converter/RESEARCH_README.md"));
    }

    // ------------------------------------------------------------------ 7. the second scanner

    @Test
    @DisplayName("no fence escapes the extractor by its syntax")
    void noFenceEscapesTheExtractorByItsSyntax() throws Exception {
        int compared = 0;
        for (String file : DocSnippetSource.trackedMarkdown()) {
            List<String> lines = Files.readAllLines(
                    DocSnippetSource.moduleRoot().resolve(file), StandardCharsets.UTF_8);
            Set<Integer> naive = new TreeSet<>(DocSnippetSource.naiveJavaFenceLines(lines));
            Set<Integer> parsed = new TreeSet<>(DocSnippetSource.blocksIn(file).stream()
                    .map(Snippet::fenceLine).toList());
            if (!DocSnippetSource.exemptFiles().contains(file)) {
                for (int i = 0; i < lines.size(); i++) {
                    String l = lines.get(i);
                    assertThat(l.contains("```java") && !l.stripLeading().startsWith("```"))
                            .as("%s:%d opens a java fence MID-LINE. Both scanners anchor at the "
                                    + "start of a line, so a mid-line fence is invisible to both "
                                    + "and its block escapes the gate in silence - which is "
                                    + "exactly how docs/audit/FINDINGS.md ends up reporting 9 "
                                    + "blocks where it holds 20.", file, i + 1)
                            .isFalse();
                }
            }
            assertThat(parsed)
                    .as("TWO INDEPENDENTLY WRITTEN SCANNERS DISAGREE ABOUT %s. The cheapest way "
                            + "to escape this gate is to change three characters in a fence, and "
                            + "two scanners disagreeing is the only way to notice.", file)
                    .isEqualTo(naive);
            compared++;
        }
        assertThat(compared).as("VERIFY THE COUNT: the cross-check is a loop over the tracked "
                + "file list, and an empty list compares nothing").isGreaterThan(5);

        List<String> odd = List.of(
                "<!-- snippet: pseudo reason=\"synthetic fixture proving the extractor sees odd fences\" -->",
                "```Java",
                "int a = 1;",
                "```",
                "<!-- snippet: pseudo reason=\"synthetic fixture proving the extractor sees odd fences\" -->",
                "~~~java",
                "int b = 2;",
                "~~~",
                "<!-- snippet: pseudo reason=\"synthetic fixture proving the extractor sees odd fences\" -->",
                "```java title=example.java",
                "int c = 3;",
                "```",
                "<!-- snippet: pseudo reason=\"synthetic fixture proving the extractor sees odd fences\" -->",
                "   ```java",
                "   int d = 4;",
                "   ```");
        assertThat(DocSnippetSource.parse("synthetic.md", odd, true))
                .as("```Java, ~~~java, an info-string attribute and an indented fence are all "
                        + "java blocks; each is a three-character change away from invisible")
                .hasSize(4);
    }

    // ------------------------------------------------------------------ 8. exact sets

    @Test
    @DisplayName("file exemptions and environments are exact sets")
    void exemptionsAndEnvironmentsAreExactSets() {
        assertThat(DocSnippetSource.exemptFiles())
                .as("SET EQUALITY, NOT CONTAINMENT. Containment lets a third exempt file in with "
                        + "a green build, and a file-level exemption is the widest hatch here.")
                .isEqualTo(Set.of(
                        "docs/audit/FINDINGS.md",
                        "src/main/java/io/github/pierce/converter/RESEARCH_README.md"));
        for (Map.Entry<String, String> e : DocSnippetSource.EXEMPT_FILES.entrySet()) {
            assertThat(e.getValue()).as("exempt file %s must state why", e.getKey())
                    .isNotBlank().hasSizeGreaterThan(60);
            assertThat(DocSnippetSource.moduleRoot().resolve(e.getKey()))
                    .as("an exemption for a file that no longer exists is dead weight that hides "
                            + "the next one").exists();
        }

        Map<String, Integer> used = new LinkedHashMap<>();
        for (Snippet s : DocSnippetSource.blocks()) {
            if (s.env() != null) {
                used.merge(s.env(), 1, Integer::sum);
            }
        }
        assertThat(used.keySet())
                .as("every env a block references must be declared")
                .isSubsetOf(SnippetEnvironments.names());
        assertThat(new TreeSet<>(used.keySet()))
                .as("A DECLARED-BUT-UNUSED TEMPLATE is the residue of somebody widening the hatch "
                        + "for one snippet and leaving the hole open. Declared: %s. Used: %s.",
                        SnippetEnvironments.names(), used)
                .isEqualTo(new TreeSet<>(SnippetEnvironments.names()));
    }
}
