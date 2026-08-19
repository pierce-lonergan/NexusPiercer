package io.github.pierce.gates;

import io.github.pierce.MapFlattener;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A default-deny gate on the one sentence that outlived its code for over a year.
 *
 * <h2>What went stale, and how</h2>
 *
 * <p>The {@code MapFlattener} class javadoc documented {@code {data_name, data_value}} for
 * {@code {"data":[[{"name":"A"}],"text"]}} since before 2.0.0. The code has never produced it.
 * Commit 6bb66d1 rewrote the javadoc to describe the measured output, which closed the discrepancy
 * IN THE JAVADOC and left the claim about the javadoc alive in two other places: the [BL-022]
 * backlog entry and the fixture's own {@code rationale} field. Six orchestration passes then
 * inherited the false premise from those two sites.</p>
 *
 * <h2>Two halves, and why both are needed</h2>
 *
 * <ol>
 *   <li>BEHAVIOUR. The emitted key set is measured here, in the same test, so the gate cannot be
 *       satisfied by editing prose after a behaviour change. Same default-deny shape as
 *       {@code DocumentedJavaSnippetsCompileTest}.</li>
 *   <li>PROSE. No git-tracked document and no fixture field may assert in the present tense that
 *       the javadoc documents {@code data_value}. Implemented as an exact-substring ban on the
 *       sentences that were false at HEAD rather than as a fuzzy search, because a fuzzy search
 *       over a corpus this size produces false positives and gets switched off.</li>
 * </ol>
 *
 * <p>{@code CHANGELOG.md} is exempt BY PATH. Its tables are a historical record of what was
 * believed and published at the time; rewriting history to match today is the opposite of the
 * correction this gate exists to hold.</p>
 */
@DisplayName("no published prose claims the MapFlattener javadoc documents data_value")
class SentinelKeyProseMatchesTheCodeTest {

    /**
     * The exact sentences that were false at HEAD before [BL-022] was resolved. Substrings, not
     * patterns: each one was copied out of the file it was found in.
     */
    private static final List<String> BANNED = List.of(
            "The `MapFlattener` class javadoc has documented `{data_name, data_value}`",
            "which claims the output is {data_name, data_value}",
            "what the class\njavadoc has always mis-documented");

    /** Historical records. Their whole job is to say what was believed then. */
    private static final Set<String> EXEMPT_BY_PATH = Set.of("CHANGELOG.md");

    // ------------------------------------------------------------------ half (i): behaviour

    @Test
    @DisplayName("the measured key set is exactly {data, data_name}")
    void theMeasuredKeySetIsExactlyDataAndDataName() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("data", List.of(List.of(Map.of("name", "A")), "text"));

        Map<String, Object> flat = MapFlattener.builder().build().flatten(source);

        assertEquals(Set.of("data", "data_name"), flat.keySet(),
                "this single fact is what all the prose below is about; if it ever changes, the "
                        + "prose ban has to be rewritten in the same commit rather than the "
                        + "behaviour being quietly documented after the fact");
    }

    // ------------------------------------------------------------------ half (ii): prose

    @Test
    @DisplayName("no git-tracked markdown asserts the javadoc documents data_value")
    void noGitTrackedMarkdownAssertsTheJavadocDocumentsDataValue() throws IOException {
        List<String> offences = new ArrayList<>();

        for (Path md : markdownFiles()) {
            String rel = repoRoot().relativize(md).toString().replace('\\', '/');
            if (EXEMPT_BY_PATH.contains(rel)) {
                continue;
            }
            String text = Files.readString(md, StandardCharsets.UTF_8).replace("\r\n", "\n");
            for (String banned : BANNED) {
                if (text.contains(banned)) {
                    offences.add(rel + " still asserts: \"" + oneLine(banned) + "\"");
                }
            }
        }

        assertTrue(offences.isEmpty(),
                "the MapFlattener class javadoc was corrected in 6bb66d1 and describes the "
                        + "measured output; these documents still say it does not:\n  "
                        + String.join("\n  ", offences));
    }

    @Test
    @DisplayName("no fixture rationale, cannotCatch or detail asserts it either")
    void noFixtureFieldAssertsTheJavadocDocumentsDataValue() throws IOException {
        List<String> offences = new ArrayList<>();
        Path corpus = repoRoot().resolve("src/test/resources/fidelity");

        try (Stream<Path> walk = Files.walk(corpus)) {
            for (Path fixture : walk.filter(p -> p.toString().endsWith(".json")).toList()) {
                String text = Files.readString(fixture, StandardCharsets.UTF_8).replace("\r\n", "\n");
                for (String banned : BANNED) {
                    if (text.contains(banned)) {
                        offences.add(corpus.relativize(fixture).toString().replace('\\', '/')
                                + " still asserts: \"" + oneLine(banned) + "\"");
                    }
                }
            }
        }

        assertTrue(offences.isEmpty(),
                "a corpus row whose stated value is that it contradicts shipped documentation "
                        + "must not itself contradict the shipped documentation:\n  "
                        + String.join("\n  ", offences));
    }

    // ------------------------------------------------------------------ the gate's own guard

    @Test
    @DisplayName("the ban list is non-empty and every entry is a real sentence, not a wildcard")
    void theBanListIsNonEmptyAndLiteral() {
        assertTrue(BANNED.size() >= 3, "a ban list that shrinks to nothing gates nothing");
        for (String banned : BANNED) {
            assertTrue(banned.toLowerCase(Locale.ROOT).contains("data_value")
                            || banned.contains("mis-documented"),
                    "every banned sentence must be about the data_value claim: " + banned);
            assertTrue(banned.length() > 30,
                    "short substrings match by accident and get switched off: " + banned);
        }
    }

    // ------------------------------------------------------------------ helpers

    private static String oneLine(String s) {
        return s.replace("\n", " ");
    }

    private static Path repoRoot() {
        Path here = Path.of("").toAbsolutePath();
        while (here != null && !Files.exists(here.resolve("pom.xml"))) {
            here = here.getParent();
        }
        return here == null ? Path.of("").toAbsolutePath() : here;
    }

    /**
     * Git-tracked markdown, approximated by walking the tree and skipping build output and every
     * dot-directory.
     *
     * <p>The dot-directory rule is not cosmetic. {@code .claude/worktrees/} can hold a FULL
     * checkout of this repository at another commit, and a checkout old enough to still contain
     * the banned sentences would have failed this gate on one developer's machine and passed on
     * everyone else's. A gate whose verdict depends on whether someone happens to have a worktree
     * open is worse than no gate: it gets muted the first time it fires for the wrong reason.</p>
     */
    private static List<Path> markdownFiles() throws IOException {
        Path root = repoRoot();
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".md"))
                    .filter(p -> {
                        String rel = root.relativize(p).toString().replace('\\', '/');
                        for (String segment : rel.split("/")) {
                            if (segment.startsWith(".") || "target".equals(segment)) {
                                return false;
                            }
                        }
                        return true;
                    })
                    .toList();
        }
    }
}
