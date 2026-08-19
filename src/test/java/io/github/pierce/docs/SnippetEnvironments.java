package io.github.pierce.docs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The registry of wrapper templates the documented-snippet gate splices fragments into.
 *
 * <h2>Why the templates are marker regions inside real compiled files</h2>
 *
 * <p>The templates are the gate's oracle. If one rots, the gate reports green against a fiction -
 * which is the failure the gate exists to prevent, one level up. Holding them as
 * {@code // SNIPPET-BEGIN} regions of files javac compiles on every build means an import that
 * stops resolving, a type that was renamed or a local whose class was deleted turns the TEMPLATE
 * red before it can turn the GATE hollow.</p>
 *
 * <h2>VARIABLES AND IMPORTS ONLY. NO STUB METHODS. NO STUB TYPES.</h2>
 *
 * <p>Enforced by {@link #assertRegionDeclaresNoMethodOrType} and drilled adversarially by
 * {@code DocumentedJavaSnippetsCompileTest#aWrapperTemplateCannotMakeAPhantomMethodCompile}.</p>
 *
 * <p>A template that declares a helper makes the phantom TRUE. A snippet calling a method this
 * library does not have would compile against the stub, the gate would go green, and the document
 * would keep advertising an API nobody can call. That is the comment bypass which broke the
 * fidelity snippet gate, wearing a different costume, and it is the single most likely way this
 * whole mechanism ships broken. If that drill is ever dropped, drop the gate instead.</p>
 */
final class SnippetEnvironments {

    /** env name to the template file that carries its IMPORTS and LOCALS regions. */
    private static final Map<String, String> TEMPLATE_FILES;

    static {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("core", "src/test/java/io/github/pierce/docs/SnippetEnvCore.java");
        m.put("spark", "src/test/java/io/github/pierce/docs/SnippetEnvSpark.java");
        m.put("converter", "src/test/java/io/github/pierce/docs/SnippetEnvConverter.java");
        TEMPLATE_FILES = java.util.Collections.unmodifiableMap(m);
    }

    /** Declaration of a method or of a type - neither may appear inside a template region. */
    private static final Pattern METHOD_OR_TYPE = Pattern.compile(
            "(^|\\s)(class|interface|enum|record|@interface)\\s+\\w+"
                    + "|\\b[A-Za-z_$][\\w$<>\\[\\],.\\s]*\\s+\\w+\\s*\\([^)]*\\)\\s*(throws[^{;]+)?\\{");

    private SnippetEnvironments() {
    }

    static Set<String> names() {
        return TEMPLATE_FILES.keySet();
    }

    static Path fileFor(String env) {
        String rel = TEMPLATE_FILES.get(env);
        if (rel == null) {
            throw new AssertionError("no snippet environment named '" + env + "'. Declared: "
                    + TEMPLATE_FILES.keySet() + ". An undefined env must fail loudly; treating it "
                    + "as 'no wrapper' would let a fragment opt out of compilation by typo.");
        }
        return DocSnippetSource.moduleRoot().resolve(rel);
    }

    static String imports(String env) {
        return region(fileFor(env), "IMPORTS " + env);
    }

    static String locals(String env) {
        return region(fileFor(env), "LOCALS " + env);
    }

    /**
     * Extracts one {@code // SNIPPET-BEGIN &lt;marker&gt;} region.
     *
     * <p>Throws on every degenerate case rather than returning {@code ""}. An extractor that
     * returns the empty string for a marker it cannot find silently converts every template into
     * "no imports, no locals", which fails every snippet at once - and the repair a hurried
     * reader reaches for is to soften the gate, not to fix the extractor.</p>
     */
    static String region(Path file, String marker) {
        if (!Files.isRegularFile(file)) {
            throw new AssertionError("template file does not exist: " + file);
        }
        List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AssertionError("cannot read template file " + file, e);
        }
        String begin = "// SNIPPET-BEGIN " + marker;
        String end = "// SNIPPET-END " + marker;
        int b = -1;
        int e = -1;
        int beginCount = 0;
        for (int i = 0; i < lines.size(); i++) {
            String t = lines.get(i).strip();
            if (t.equals(begin)) {
                beginCount++;
                if (b < 0) {
                    b = i;
                }
            } else if (t.equals(end) && e < 0) {
                e = i;
            }
        }
        if (beginCount == 0) {
            throw new AssertionError("marker '" + begin + "' does not appear in " + file
                    + ". A missing marker must fail, never yield an empty template.");
        }
        if (beginCount > 1) {
            throw new AssertionError("marker '" + begin + "' appears " + beginCount + " times in "
                    + file + "; taking the first is how a commented copy gets used as the real one.");
        }
        if (e < 0) {
            throw new AssertionError("marker '" + end + "' does not appear in " + file);
        }
        if (e < b) {
            throw new AssertionError("markers for '" + marker + "' are out of order in " + file);
        }
        List<String> body = new ArrayList<>(lines.subList(b + 1, e));
        String text = String.join("\n", body).strip();
        if (text.isEmpty()) {
            throw new AssertionError("region '" + marker + "' in " + file + " is blank");
        }
        return text;
    }

    /**
     * The no-stubs rule, as a check rather than as a comment.
     *
     * @throws AssertionError if the region declares a method, a class, an interface, an enum or a
     *                        record. Either would let a published snippet call something the
     *                        library does not have and still compile.
     */
    static void assertRegionDeclaresNoMethodOrType(String env, String kind, String region) {
        var m = METHOD_OR_TYPE.matcher(region);
        if (m.find()) {
            throw new AssertionError("snippet environment '" + env + "' declares a "
                    + "method or type in its " + kind + " region:\n    " + m.group().strip()
                    + "\nVARIABLES AND IMPORTS ONLY. A stubbed helper makes a phantom API compile: "
                    + "the gate goes green while the document advertises a method nobody can call. "
                    + "That is exactly the defect this gate was built to catch, relocated into the "
                    + "gate itself.");
        }
    }
}
