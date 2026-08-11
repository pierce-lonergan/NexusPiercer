package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Re-measures every fixture in the corpus and rewrites its recorded behaviour in place.
 *
 * <p>This is a tool, not a test: the class name deliberately does not end in {@code Test}, so
 * surefire never runs it and it can never masquerade as coverage. Run it by hand, and only after
 * a <em>deliberate</em> behaviour change:</p>
 *
 * <pre>
 *   mvn -o test-compile dependency:build-classpath -Dmdep.outputFile=target/test-cp.txt
 *   java -cp "target/classes;target/test-classes;$(cat target/test-cp.txt)" \
 *        io.github.pierce.fidelity.FidelityCorpusRecorder src/test/resources/fidelity
 * </pre>
 *
 * <p>The fixture files are their own source of truth: each one already carries its input
 * document, its configuration and its metadata, so this tool only replaces the {@code expected}
 * block and the {@code measuredLossless} flag. Everything a human wrote is left untouched.</p>
 *
 * <p>It records MEASURED behaviour and nothing else. It never edits {@code manifest.json} and it
 * never decides a classification: it prints every fixture whose measured losslessness disagrees
 * with the manifest, and stops there. Turning a measurement into ACCEPTED_LOSS or DEFECT is a
 * human judgement, and the whole point of the manifest is that the judgement is written down by
 * a person rather than derived by a program.</p>
 */
public final class FidelityCorpusRecorder {

    private static final ObjectMapper JSON = new ObjectMapper();

    private FidelityCorpusRecorder() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException(
                    "usage: FidelityCorpusRecorder <fidelity-resources-dir>");
        }
        Path root = Paths.get(args[0]);
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("not a directory: " + root);
        }

        List<Path> files = fixtureFiles(root);
        if (files.isEmpty()) {
            throw new IllegalStateException("no fixture files found under " + root
                    + " - refusing to report success on an empty corpus");
        }

        JsonNode manifest = Files.isRegularFile(root.resolve("manifest.json"))
                ? JSON.readTree(Files.readString(root.resolve("manifest.json"), StandardCharsets.UTF_8))
                : null;

        int lossless = 0;
        int lossy = 0;
        int changed = 0;
        List<String> disagreements = new ArrayList<>();
        List<String> recipeVerdicts = new ArrayList<>();

        for (Path file : files) {
            String before = Files.readString(file, StandardCharsets.UTF_8);
            ObjectNode node = (ObjectNode) JSON.readTree(before);
            FidelityFixture fx = FidelityFixture.from(node);
            FidelityRunner.Measurement m = FidelityRunner.run(fx);

            boolean isLossless = measuredLossless(m, fx.id());
            node.set("expected", JSON.valueToTree(m.recorded));
            node.put("measuredLossless", isLossless);

            String after = JSON.writerWithDefaultPrettyPrinter().writeValueAsString(node)
                    + System.lineSeparator();
            if (!after.equals(before)) {
                Files.writeString(file, after, StandardCharsets.UTF_8);
                changed++;
            }

            if (isLossless) {
                lossless++;
            } else {
                lossy++;
            }

            // The published-recipe verdict is DERIVED here rather than guessed into the manifest by
            // hand. It has to be computed AFTER the expected block is refreshed, because it
            // compares the recipe's output against this row's own recording.
            FidelityFixture refreshed = FidelityFixture.from((ObjectNode) JSON.readTree(after));
            recipeVerdicts.add(fx.id() + "	" + FidelityRecipe.verdict(refreshed));

            String declared = declaredClassification(manifest, fx.id());
            if (declared != null && "LOSSLESS".equals(declared) != isLossless) {
                disagreements.add(fx.id() + ": manifest says " + declared
                        + " but measurement says lossless=" + isLossless + "  [" + perStack(m) + "]");
            }
        }

        System.out.println("---- holdsUnderPublishedRecipe (copy into manifest.json by hand) ----");
        recipeVerdicts.forEach(v -> System.out.println("RECIPE	" + v));
        System.out.println("re-measured " + files.size() + " fixtures under " + root);
        System.out.println("  lossless=" + lossless + "  lossy=" + lossy + "  files rewritten=" + changed);
        if (disagreements.isEmpty()) {
            System.out.println("  no fixture disagrees with the manifest");
        } else {
            System.out.println("  " + disagreements.size() + " FIXTURE(S) DISAGREE WITH THE MANIFEST"
                    + " - classify each one deliberately and edit manifest.json by hand:");
            disagreements.forEach(d -> System.out.println("    " + d));
        }
    }

    private static List<Path> fixtureFiles(Path root) throws IOException {
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".json"))
                    .filter(p -> !"manifest.json".equals(p.getFileName().toString()))
                    .sorted()
                    .toList();
        }
    }

    private static String declaredClassification(JsonNode manifest, String id) {
        if (manifest == null) {
            return null;
        }
        for (JsonNode e : manifest.path("fixtures")) {
            if (id.equals(e.path("id").asText())) {
                return e.path("classification").asText();
            }
        }
        return null;
    }

    private static boolean measuredLossless(FidelityRunner.Measurement m, String id) {
        boolean any = false;
        boolean all = true;
        for (String k : new String[] {"losslessMap", "losslessJson", "losslessAvro"}) {
            Object v = m.recorded.get(k);
            if (v instanceof Boolean b) {
                any = true;
                all &= b;
            }
        }
        if (!any) {
            throw new IllegalStateException("fixture " + id + " ran no stack, so it produced no "
                    + "verdict - a fixture that measures nothing cannot fail and must not exist");
        }
        return all;
    }

    private static String perStack(FidelityRunner.Measurement m) {
        Map<String, Object> flags = new LinkedHashMap<>();
        for (String k : new String[] {"losslessMap", "losslessJson", "losslessAvro"}) {
            if (m.recorded.containsKey(k)) {
                flags.put(k, m.recorded.get(k));
            }
        }
        return flags.toString();
    }
}
