package io.github.pierce.fidelity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
 */
final class FidelitySnippetSource {

    static final String RECIPES = "src/test/java/io/github/pierce/fidelity/PublishedStackRecipes.java";
    private static final String BEGIN = "// SNIPPET-BEGIN ";
    private static final String END = "// SNIPPET-END ";

    private FidelitySnippetSource() {
    }

    static Path recipesFile() {
        return FidelityCorpus.moduleRoot().resolve(RECIPES);
    }

    static String extract(String stack) {
        return extract(recipesFile(), stack);
    }

    static String extract(Path file, String stack) {
        List<String> lines = read(file);
        int begin = indexOf(lines, BEGIN + stack);
        int end = indexOf(lines, END + stack);
        if (begin < 0) {
            throw new AssertionError("PUBLISHED SNIPPET GATE CANNOT RUN: no '" + BEGIN + stack
                    + "' marker in " + file);
        }
        if (end < 0) {
            throw new AssertionError("PUBLISHED SNIPPET GATE CANNOT RUN: no '" + END + stack
                    + "' marker in " + file);
        }
        if (end <= begin) {
            throw new AssertionError("PUBLISHED SNIPPET GATE CANNOT RUN: markers for '" + stack
                    + "' are out of order in " + file);
        }
        List<String> region = new ArrayList<>(lines.subList(begin + 1, end));
        int indent = Integer.MAX_VALUE;
        for (String line : region) {
            if (line.isBlank()) {
                continue;
            }
            int i = 0;
            while (i < line.length() && line.charAt(i) == ' ') {
                i++;
            }
            indent = Math.min(indent, i);
        }
        if (indent == Integer.MAX_VALUE) {
            indent = 0;
        }
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < region.size(); i++) {
            String line = region.get(i);
            String stripped = line.isBlank() ? "" : line.substring(indent);
            out.append(stripped.stripTrailing());
            if (i < region.size() - 1) {
                out.append('\n');
            }
        }
        String snippet = out.toString();
        if (snippet.isBlank()) {
            throw new AssertionError("PUBLISHED SNIPPET GATE CANNOT RUN: extracted snippet for '"
                    + stack + "' is blank in " + file);
        }
        if (region.size() < 2) {
            throw new AssertionError("PUBLISHED SNIPPET GATE CANNOT RUN: snippet for '" + stack
                    + "' is a single line in " + file + "; a one-line recipe is almost certainly a "
                    + "truncated extraction rather than a real recipe");
        }
        return snippet;
    }

    /** Every marker in the recipes file, so COVERAGE can be checked rather than assumed. */
    static Set<String> markers() {
        Set<String> out = new TreeSet<>();
        for (String line : read(recipesFile())) {
            int at = line.indexOf(BEGIN);
            if (at >= 0) {
                out.add(line.substring(at + BEGIN.length()).trim());
            }
        }
        return out;
    }

    private static List<String> read(Path file) {
        if (!Files.isRegularFile(file)) {
            throw new AssertionError("PUBLISHED SNIPPET GATE CANNOT RUN: " + file + " does not "
                    + "exist. The gate reads the recipe class's SOURCE from disk, so it cannot run "
                    + "from a shaded jar with no source tree.");
        }
        try {
            return List.of(Files.readString(file, StandardCharsets.UTF_8)
                    .replace("\r\n", "\n").split("\n", -1));
        } catch (IOException e) {
            throw new AssertionError("PUBLISHED SNIPPET GATE CANNOT RUN: unreadable " + file, e);
        }
    }

    private static int indexOf(List<String> lines, String marker) {
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).contains(marker)) {
                return i;
            }
        }
        return -1;
    }
}
