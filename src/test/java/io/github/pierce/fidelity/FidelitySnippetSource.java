package io.github.pierce.fidelity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Extracts a published snippet from {@link PublishedStackRecipes}'s SOURCE FILE on disk.
 *
 * <p>Reading the real {@code .java} is the only read that cannot drift. A copy on the test
 * classpath, or a resource file, could go stale against the source javac actually compiled, and
 * the gate would then be guarding a copy of the snippet rather than the snippet.</p>
 *
 * <p>{@link #extract} THROWS on every degenerate case. If it returned {@code ""} for a missing
 * marker, the identity assertion would compare nothing against a manifest field and the whole gate
 * would evaporate the moment someone renamed a marker.</p>
 *
 * <h2>Why marker text alone is not enough - MEASURED, not theorised</h2>
 *
 * <p>The first version of this class located its markers with {@code String.contains} and took the
 * FIRST hit, with no notion of a method body. Adversarial review drilled it: a byte-identical copy
 * of the MAP marker region pasted into a {@code &#47;* ... *&#47;} block comment ABOVE the real
 * method, with the real compiled body simultaneously changed to something else, left all five test
 * groups green - the identity check matched the comment while a different body executed. javac
 * accepts comments, so "the manifest string is a copy of a region javac accepted" was not the
 * property the class was enforcing. Two invariants close that:</p>
 * <ol>
 *   <li>a marker line must match EXACTLY once, comparing the stripped line to the whole marker -
 *       which also removes the {@code AVRO} / {@code AVRO_SCHEMA} prefix collision that
 *       {@code contains} had, where reordering the two methods silently retargeted the gate;</li>
 *   <li>the marker region must lie strictly inside the source range of the method the execution
 *       tests actually call, so the published text and the executed code are the same lines.</li>
 * </ol>
 */
final class FidelitySnippetSource {

    static final String RECIPES = "src/test/java/io/github/pierce/fidelity/PublishedStackRecipes.java";

    /**
     * Every recipe method on {@link PublishedStackRecipes}, keyed by its published stack name.
     *
     * <p>It lives here rather than in the test because it is now load-bearing for EXTRACTION and
     * not only for coverage: the extractor binds each marker region to the named method. Two copies
     * of this map would be two ideas of which code a snippet is, and they could disagree.</p>
     */
    static final Map<String, String> RECIPE_METHODS = Map.of(
            "MAP", "stackMap",
            "JSON", "stackJson",
            "AVRO", "stackAvroData",
            "AVRO_SCHEMA", "stackAvroSchema");

    private static final String BEGIN = "// SNIPPET-BEGIN ";
    private static final String END = "// SNIPPET-END ";
    private static final String CANNOT_RUN = "PUBLISHED SNIPPET GATE CANNOT RUN: ";

    private FidelitySnippetSource() {
    }

    static Path recipesFile() {
        return FidelityCorpus.moduleRoot().resolve(RECIPES);
    }

    /** The gate's entry point: extract, and bind the region to the method the tests execute. */
    static String extract(String stack) {
        return extract(recipesFile(), stack, methodFor(stack));
    }

    static String methodFor(String stack) {
        String method = RECIPE_METHODS.get(stack);
        if (method == null) {
            throw new AssertionError(CANNOT_RUN + "no compiled recipe method is declared for stack '"
                    + stack + "'. A stack the manifest publishes with no method behind it is the "
                    + "hole this gate exists to close, so it must not extract at all.");
        }
        return method;
    }

    /**
     * Extracts the region between the markers for {@code stack} and proves it is inside
     * {@code methodName}'s body.
     */
    static String extract(Path file, String stack, String methodName) {
        List<String> lines = read(file);
        int[] region = markerRegion(lines, stack, file);
        int[] body = methodBody(lines, methodName, file);
        if (region[0] <= body[0] || region[1] >= body[1]) {
            throw new AssertionError(CANNOT_RUN + "the SNIPPET markers for '" + stack + "' in " + file
                    + " span lines " + (region[0] + 1) + ".." + (region[1] + 1) + ", which is NOT "
                    + "strictly inside the body of " + methodName + " (lines " + (body[0] + 1)
                    + ".." + (body[1] + 1) + "). javac accepts a comment, so byte identity against "
                    + "the manifest proves nothing unless the region is the code the execution "
                    + "tests run. A commented-out copy of a recipe is exactly this failure.");
        }
        return render(lines, region, stack, file);
    }

    /**
     * Marker-only extraction with no method binding.
     *
     * <p>For the degenerate-input drills, which build synthetic files that contain markers and no
     * method at all. It is deliberately NOT what the identity assertion calls: an extractor that
     * only finds markers is the version the review broke.</p>
     */
    static String extractMarkerRegion(Path file, String stack) {
        List<String> lines = read(file);
        return render(lines, markerRegion(lines, stack, file), stack, file);
    }

    /** Every marker in the recipes file, so COVERAGE can be checked rather than assumed. */
    static Set<String> markers() {
        Set<String> out = new TreeSet<>();
        for (String line : read(recipesFile())) {
            String stripped = line.strip();
            if (stripped.startsWith(BEGIN)) {
                out.add(stripped.substring(BEGIN.length()).trim());
            }
        }
        return out;
    }

    private static int[] markerRegion(List<String> lines, String stack, Path file) {
        int begin = soleIndexOf(lines, BEGIN + stack, file);
        int end = soleIndexOf(lines, END + stack, file);
        if (end <= begin) {
            throw new AssertionError(CANNOT_RUN + "markers for '" + stack
                    + "' are out of order in " + file);
        }
        return new int[] {begin, end};
    }

    /**
     * EXACT match on the stripped line, and exactly one of them.
     *
     * <p>{@code contains} was two bugs at once. It matched a marker inside a block comment, and it
     * matched {@code // SNIPPET-BEGIN AVRO_SCHEMA} when asked for {@code AVRO} - so moving one
     * method above the other retargeted the gate at the wrong recipe.</p>
     */
    private static int soleIndexOf(List<String> lines, String marker, Path file) {
        int at = -1;
        int count = 0;
        for (int i = 0; i < lines.size(); i++) {
            if (marker.equals(lines.get(i).strip())) {
                count++;
                if (at < 0) {
                    at = i;
                }
            }
        }
        if (count == 0) {
            throw new AssertionError(CANNOT_RUN + "no '" + marker + "' marker in " + file);
        }
        if (count > 1) {
            throw new AssertionError(CANNOT_RUN + "'" + marker + "' appears " + count + " times in "
                    + file + ". The extractor would take the first, which need not be the compiled "
                    + "one - a duplicate marker is how a comment gets published as a recipe.");
        }
        return at;
    }

    /** First and last source line of {@code methodName}: its signature line and its closing brace. */
    private static int[] methodBody(List<String> lines, String methodName, Path file) {
        int signature = -1;
        int count = 0;
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.strip().startsWith("static ")
                    && line.contains(" " + methodName + "(")
                    && line.stripTrailing().endsWith("{")) {
                count++;
                if (signature < 0) {
                    signature = i;
                }
            }
        }
        if (count != 1) {
            throw new AssertionError(CANNOT_RUN + "expected exactly one 'static ... " + methodName
                    + "(...) {' declaration in " + file + " but found " + count + ". The gate binds "
                    + "each published region to the method the execution tests call, and it cannot "
                    + "do that against an ambiguous or absent declaration.");
        }
        String closer = " ".repeat(indent(lines.get(signature))) + "}";
        for (int i = signature + 1; i < lines.size(); i++) {
            if (closer.equals(lines.get(i).stripTrailing())) {
                return new int[] {signature, i};
            }
        }
        throw new AssertionError(CANNOT_RUN + "no closing brace at the declaration indent was found "
                + "for " + methodName + " in " + file);
    }

    private static String render(List<String> lines, int[] region, String stack, Path file) {
        List<String> body = new ArrayList<>(lines.subList(region[0] + 1, region[1]));
        int indent = Integer.MAX_VALUE;
        for (String line : body) {
            if (line.isBlank()) {
                continue;
            }
            indent = Math.min(indent, indent(line));
        }
        if (indent == Integer.MAX_VALUE) {
            indent = 0;
        }
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < body.size(); i++) {
            String line = body.get(i);
            String stripped = line.isBlank() ? "" : line.substring(indent);
            out.append(stripped.stripTrailing());
            if (i < body.size() - 1) {
                out.append('\n');
            }
        }
        String snippet = out.toString();
        if (snippet.isBlank()) {
            throw new AssertionError(CANNOT_RUN + "extracted snippet for '" + stack
                    + "' is blank in " + file);
        }
        if (body.size() < 2) {
            throw new AssertionError(CANNOT_RUN + "snippet for '" + stack + "' is a single line in "
                    + file + "; a one-line recipe is almost certainly a truncated extraction rather "
                    + "than a real recipe");
        }
        return snippet;
    }

    private static int indent(String line) {
        int i = 0;
        while (i < line.length() && line.charAt(i) == ' ') {
            i++;
        }
        return i;
    }

    private static List<String> read(Path file) {
        if (!Files.isRegularFile(file)) {
            throw new AssertionError(CANNOT_RUN + file + " does not exist. The gate reads the "
                    + "recipe class's SOURCE from disk, so it cannot run from a shaded jar with no "
                    + "source tree.");
        }
        try {
            return List.of(Files.readString(file, StandardCharsets.UTF_8)
                    .replace("\r\n", "\n").split("\n", -1));
        } catch (IOException e) {
            throw new AssertionError(CANNOT_RUN + "unreadable " + file, e);
        }
    }
}
