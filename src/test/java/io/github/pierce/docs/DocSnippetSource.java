package io.github.pierce.docs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Finds every published Java snippet in every tracked markdown file, and refuses to be vague
 * about any of them.
 *
 * <h2>Why the file list comes from git and not from a constant</h2>
 *
 * <p>{@code PublishedSnippetsCompileTest} gates four snippets - the manifest's stack recipes. The
 * other 78 published {@code ```java} blocks in this repository were gated by nothing, and the
 * consequence is on record: {@code docs/SPARK_PIPELINE.md} published six calls to four
 * {@code NexusPiercerPatterns} methods that have never existed, and the commit that added a
 * warning banner about them simultaneously published a NEW non-compiling example in the README
 * and in the class javadoc. Hand-correction of unchecked prose has now failed twice in this
 * repository in the same place. Only a compiler is trustworthy here.</p>
 *
 * <p>The file set is therefore derived from {@code git ls-files '*.md'} rather than from a list.
 * A hardcoded list of four filenames is how document number five escapes: by existing.</p>
 *
 * <h2>Default deny</h2>
 *
 * <p>Every {@code java} fence must carry an HTML-comment directive on the line immediately above
 * it. A block with no directive is a BUILD FAILURE, not a skip. That single property is the gate;
 * everything else is bookkeeping around it. This class throws on every degenerate input - missing
 * directive, unknown kind, blank reason - and never returns {@code null} or an empty result for a
 * malformed one, for the reason {@code FidelitySnippetSource} states in its own javadoc: an
 * extractor that quietly returns nothing for input it does not understand evaporates the whole
 * gate while leaving the test names green.</p>
 *
 * <h2>The directives</h2>
 *
 * <pre>
 * &lt;!-- snippet: unit --&gt;                    compiled verbatim as a compilation unit
 * &lt;!-- snippet: body env=spark --&gt;          spliced into a template that predeclares locals
 * &lt;!-- snippet: members env=core --&gt;        spliced into a template's class body
 * &lt;!-- snippet: pseudo reason="..." --&gt;     NOT compiled; reason mandatory, and counted
 * </pre>
 */
final class DocSnippetSource {

    /**
     * Files whose java blocks are exempt wholesale, with the reason each is exempt.
     *
     * <p>Asserted by SET EQUALITY, never containment - containment would let a third file in
     * without a red build, and a file-level exemption is the widest hatch in this gate.</p>
     *
     * <p>THE DESIGN NOTE FOR THIS GATE SAID "EXACTLY ONE". Measured, there are two, and the
     * second is not a compromise. {@code docs/audit/FINDINGS.md} quotes code AS FOUND, including
     * code that never compiled - that is the entire point of an audit register.
     * {@code RESEARCH_README.md} is a design-research document: its thirteen blocks are
     * IMPLEMENTATION SKETCHES - a proposed body for {@code SchemaBasedMapConverter}, a proposed
     * {@code switch} over Iceberg type ids, a WRONG/CORRECT contrast on {@code CharSequence} -
     * arguing for an approach rather than showing a reader something to call. (The types they
     * name mostly DO exist; it is the bodies that are proposals. An earlier draft of this note
     * said the types did not exist, which was wrong and is corrected here rather than
     * overwritten.) Both files are the same category - code that is deliberately not this
     * library's callable surface - and in both cases per-block reasons would be N copies of one
     * sentence, which is noise that teaches a reader to stop reading reasons.</p>
     */
    static final Map<String, String> EXEMPT_FILES = Collections.unmodifiableMap(new LinkedHashMap<>(Map.of(
            "docs/audit/FINDINGS.md",
            "An audit register. It quotes code AS FOUND at 2026-08-09, including code that never "
                    + "compiled - which is the finding, not a defect in the document.",
            "src/main/java/io/github/pierce/converter/RESEARCH_README.md",
            "A design-research document. Its blocks are PROPOSED implementations naming types "
                    + "that deliberately do not exist (TypeConverter, NullValueException, the "
                    + "*Converter.INSTANCE family); they argue for a design rather than document "
                    + "an API a reader could call.")));

    /** A fence whose info string begins with "java", case-insensitively. */
    private static final Pattern FENCE_OPEN =
            Pattern.compile("^(\\s*)(`{3,}|~{3,})\\s*([Jj][Aa][Vv][Aa].*)?$");

    private static final Pattern DIRECTIVE =
            Pattern.compile("^\\s*<!--\\s*snippet:\\s*(.*?)\\s*-->\\s*$");

    private static final Pattern REASON = Pattern.compile("reason\\s*=\\s*\"([^\"]*)\"");
    private static final Pattern ENV = Pattern.compile("\\benv\\s*=\\s*([A-Za-z0-9_]+)");

    /** Minimum length of a pseudo reason. Short enough to write, long enough to require thought. */
    static final int MIN_REASON_CHARS = 30;

    private DocSnippetSource() {
    }

    /** One published block. */
    record Snippet(String file, int fenceLine, Kind kind, String env, String reason, String code) {

        String where() {
            return file + ":" + fenceLine;
        }
    }

    enum Kind { UNIT, BODY, MEMBERS, PSEUDO }

    // ------------------------------------------------------------------ repository plumbing

    static Path moduleRoot() {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null && !Files.exists(dir.resolve("pom.xml"))) {
            dir = dir.getParent();
        }
        if (dir == null) {
            throw new AssertionError("DOC SNIPPET GATE DID NOT RUN: no pom.xml above "
                    + Path.of("").toAbsolutePath());
        }
        return dir;
    }

    /**
     * Every tracked markdown file, from git.
     *
     * <p>Never returns an empty list. A gate parameterized over an empty file set runs zero
     * invocations and passes, which is the repository's signature pathology and the exact thing
     * this class exists to stop happening to published prose.</p>
     */
    static List<String> trackedMarkdown() {
        List<String> out = new ArrayList<>();
        try {
            Process p = new ProcessBuilder("git", "ls-files", "*.md")
                    .directory(moduleRoot().toFile())
                    .redirectErrorStream(true)
                    .start();
            try (var reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String t = line.trim();
                    if (t.endsWith(".md")) {
                        out.add(t.replace('\\', '/'));
                    }
                }
            }
            if (!p.waitFor(60, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                throw new AssertionError("DOC SNIPPET GATE DID NOT RUN: git ls-files timed out");
            }
        } catch (IOException | InterruptedException e) {
            throw new AssertionError("DOC SNIPPET GATE DID NOT RUN: could not run git ls-files. "
                    + "The file set must come from git so that a new document cannot escape by "
                    + "not being on a list.", e);
        }
        if (out.isEmpty()) {
            throw new AssertionError("DOC SNIPPET GATE DID NOT RUN: git ls-files '*.md' returned "
                    + "nothing. Zero files means zero parameterized invocations means a green "
                    + "build that checked nothing.");
        }
        Collections.sort(out);
        return out;
    }

    // ------------------------------------------------------------------ extraction

    /** Every java block in every tracked markdown file outside {@link #EXEMPT_FILES}. */
    static List<Snippet> blocks() {
        List<Snippet> all = new ArrayList<>();
        for (String file : trackedMarkdown()) {
            if (EXEMPT_FILES.containsKey(file)) {
                continue;
            }
            all.addAll(blocksIn(file));
        }
        if (all.isEmpty()) {
            throw new AssertionError("DOC SNIPPET GATE DID NOT RUN: no java blocks found in any "
                    + "tracked markdown file. Either every document lost its examples or the "
                    + "extractor stopped recognising a fence.");
        }
        return all;
    }

    /** Java blocks in one tracked file, exempt or not. Used by the count assertions. */
    static List<Snippet> blocksIn(String file) {
        Path path = moduleRoot().resolve(file);
        if (!Files.isRegularFile(path)) {
            throw new AssertionError("DOC SNIPPET GATE DID NOT RUN: git tracks " + file
                    + " but it is not a readable file at " + path);
        }
        List<String> lines;
        try {
            lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AssertionError("DOC SNIPPET GATE DID NOT RUN: cannot read " + file, e);
        }
        if (EXEMPT_FILES.containsKey(file)) {
            // An exempt file is COUNTED but never parsed for structure, because its blocks are
            // never compiled and its fences may not be well formed. docs/audit/FINDINGS.md
            // opens several of its fences mid-line ("**Evidence.** ```xml"), which is malformed
            // markdown: the opener is invisible to any line-anchored scanner, so the eventual
            // closing fence reads as an opener and every block after it shifts. Running the
            // structural parser over it would report 9 blocks where there are 20. The count
            // that matters for an exempt file is how many java blocks it holds, and the
            // permissive scanner answers exactly that.
            List<Snippet> counted = new ArrayList<>();
            for (int line : naiveJavaFenceLines(lines)) {
                counted.add(new Snippet(file, line, Kind.PSEUDO, null, EXEMPT_FILES.get(file), ""));
            }
            return counted;
        }
        return parse(file, lines, true);
    }

    /**
     * @param requireDirective false only for the exempt files, whose blocks are counted but never
     *                         asked for a directive.
     */
    static List<Snippet> parse(String file, List<String> lines, boolean requireDirective) {
        List<Snippet> out = new ArrayList<>();
        String openMarker = null;
        int openLine = -1;
        boolean openIsJava = false;
        StringBuilder body = new StringBuilder();

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (openMarker == null) {
                Matcher m = FENCE_OPEN.matcher(line);
                boolean anyFence = line.stripLeading().startsWith("```")
                        || line.stripLeading().startsWith("~~~");
                if (m.matches()) {
                    openMarker = m.group(2);
                    openLine = i + 1;
                    openIsJava = m.group(3) != null;
                    body.setLength(0);
                } else if (anyFence) {
                    // A non-java fence. Track it so a ```json block containing ```java text
                    // cannot be mistaken for a java block.
                    Matcher generic = Pattern.compile("^\\s*(`{3,}|~{3,})").matcher(line);
                    if (generic.find()) {
                        openMarker = generic.group(1);
                        openLine = i + 1;
                        openIsJava = false;
                        body.setLength(0);
                    }
                }
                continue;
            }
            String marker = openMarker.substring(0, 1);
            String stripped = line.strip();
            boolean closes = stripped.chars().allMatch(c -> c == marker.charAt(0))
                    && stripped.length() >= openMarker.length()
                    && !stripped.isEmpty();
            if (closes) {
                if (openIsJava) {
                    out.add(build(file, openLine, directiveAbove(lines, openLine),
                            body.toString(), requireDirective));
                }
                openMarker = null;
                openIsJava = false;
                continue;
            }
            body.append(line).append('\n');
        }
        if (openMarker != null && openIsJava) {
            throw new AssertionError(file + ":" + openLine + " opens a java fence that is never "
                    + "closed. An unterminated fence swallows the rest of the document, so every "
                    + "block after it silently disappears from this gate.");
        }
        return out;
    }

    private static String directiveAbove(List<String> lines, int fenceLine) {
        int idx = fenceLine - 2;
        if (idx < 0) {
            return null;
        }
        Matcher m = DIRECTIVE.matcher(lines.get(idx));
        return m.matches() ? m.group(1) : null;
    }

    private static Snippet build(String file, int fenceLine, String directive, String code,
                                 boolean requireDirective) {
        if (directive == null) {
            if (!requireDirective) {
                return new Snippet(file, fenceLine, Kind.PSEUDO, null,
                        EXEMPT_FILES.get(file), code);
            }
            throw new AssertionError(file + ":" + fenceLine + " publishes a ```java block with no "
                    + "<!-- snippet: ... --> directive on the line immediately above it.\n"
                    + "THIS IS A FAILURE, NOT A SKIP. Add exactly one of:\n"
                    + "  <!-- snippet: unit -->                 the block is a whole compilation unit\n"
                    + "  <!-- snippet: body env=NAME -->        statements, spliced into a template\n"
                    + "  <!-- snippet: members env=NAME -->     fields and methods, wrapped in a class\n"
                    + "  <!-- snippet: pseudo reason=\"...\" -->  not Java; >= " + MIN_REASON_CHARS
                    + " chars of reason, and it is counted\n"
                    + "A published snippet nobody compiles is a claim nobody checks.");
        }
        String head = directive.split("\\s+", 2)[0].toLowerCase(Locale.ROOT);
        String env = null;
        Matcher e = ENV.matcher(directive);
        if (e.find()) {
            env = e.group(1);
        }
        switch (head) {
            case "unit":
                return new Snippet(file, fenceLine, Kind.UNIT, env, null, code);
            case "body":
                return new Snippet(file, fenceLine, Kind.BODY, requireEnv(file, fenceLine, env, head), null, code);
            case "members":
                return new Snippet(file, fenceLine, Kind.MEMBERS, requireEnv(file, fenceLine, env, head), null, code);
            case "pseudo": {
                Matcher r = REASON.matcher(directive);
                if (!r.find()) {
                    throw new AssertionError(file + ":" + fenceLine + " is marked pseudo with no "
                            + "reason=\"...\". Pseudo is the ONLY way a published snippet escapes "
                            + "the compiler, so it is the only place this gate can be hollowed "
                            + "out. An unreasoned escape is an escape nobody has to defend.");
                }
                String reason = r.group(1).strip();
                if (reason.length() < MIN_REASON_CHARS) {
                    throw new AssertionError(file + ":" + fenceLine + " has a pseudo reason of "
                            + reason.length() + " characters (\"" + reason + "\"); at least "
                            + MIN_REASON_CHARS + " are required. \"n/a\" is not a reason.");
                }
                return new Snippet(file, fenceLine, Kind.PSEUDO, env, reason, code);
            }
            default:
                throw new AssertionError(file + ":" + fenceLine + " carries an unrecognised "
                        + "snippet directive \"" + directive + "\". Known kinds: unit, body, "
                        + "members, pseudo. An unknown kind must FAIL rather than be treated as "
                        + "an opt-out, or the opt-out is spelled however you like.");
        }
    }

    private static String requireEnv(String file, int fenceLine, String env, String kind) {
        if (env == null || env.isBlank()) {
            throw new AssertionError(file + ":" + fenceLine + " is marked '" + kind + "' with no "
                    + "env=NAME. A fragment needs a named environment to be spliced into.");
        }
        return env;
    }

    // ------------------------------------------------------------------ the independent scanner

    /**
     * A deliberately permissive, independently written second scanner.
     *
     * <p>The cheapest way to escape this gate is to change three characters in a fence:
     * {@code ```Java}, {@code ~~~java}, {@code ```java title=x}, or an indented fence. Two
     * scanners written from different rules disagreeing is the only way to notice.</p>
     */
    static List<Integer> naiveJavaFenceLines(List<String> lines) {
        Pattern naive = Pattern.compile("^\\s*(```|~~~)\\s*[Jj][Aa][Vv][Aa]");
        List<Integer> hits = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            if (naive.matcher(lines.get(i)).find()) {
                hits.add(i + 1);
            }
        }
        return hits;
    }

    static Set<String> exemptFiles() {
        return EXEMPT_FILES.keySet();
    }
}
