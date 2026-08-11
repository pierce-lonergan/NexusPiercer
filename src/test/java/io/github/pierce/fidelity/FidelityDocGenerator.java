package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Renders {@code docs/ROUND_TRIP_FIDELITY.md} from {@code manifest.json}.
 *
 * <p>The published document is <b>generated, never written</b>. A hand-maintained table of this
 * many rows would go stale the first time a fixture was reclassified, and a consumer would then be
 * reading a promise the corpus no longer makes - which is the exact pathology this repository has
 * found a dozen times and would be self-parody to ship inside the fidelity guarantee itself.</p>
 *
 * <p>{@link #render(JsonNode)} is a pure function of the manifest. {@code RoundTripFidelityDocTest}
 * asserts that the committed document is byte-identical to what this renders from the manifest as
 * it stands right now, so the two cannot diverge: change the manifest without regenerating and the
 * build goes red.</p>
 *
 * <p>Everything factual in the output comes from the manifest. The only prose that lives in this
 * class is the operator runbook - how to run the corpus, how to re-record it - which describes the
 * build rather than the corpus, and so has no manifest field it could contradict.</p>
 *
 * <p>Regenerate with:</p>
 * <pre>
 *   mvn -o test-compile dependency:build-classpath -Dmdep.outputFile=target/test-cp.txt
 *   java -cp "target/classes;target/test-classes;$(cat target/test-cp.txt)" \
 *        io.github.pierce.fidelity.FidelityDocGenerator \
 *        src/test/resources/fidelity/manifest.json docs/ROUND_TRIP_FIDELITY.md
 * </pre>
 */
public final class FidelityDocGenerator {

    static final String DOC_PATH = "docs/ROUND_TRIP_FIDELITY.md";
    static final String MANIFEST_PATH = "src/test/resources/fidelity/manifest.json";

    private static final String NL = "\n";

    private FidelityDocGenerator() {
    }

    public static void main(String[] args) throws Exception {
        Path manifest = Paths.get(args.length > 0 ? args[0] : MANIFEST_PATH);
        Path out = Paths.get(args.length > 1 ? args[1] : DOC_PATH);
        String text = render(new ObjectMapper().readTree(
                Files.readString(manifest, StandardCharsets.UTF_8)));
        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.writeString(out, text, StandardCharsets.UTF_8);
        System.out.println("wrote " + out + " (" + text.length() + " chars) from " + manifest);
    }

    // ------------------------------------------------------------------ the document

    /**
     * @throws IllegalArgumentException if the manifest is absent, empty, or missing any block the
     *     document is built from. Rendering a plausible-looking page from a gutted manifest would
     *     turn "the corpus vanished" into a silent pass, so every read is checked.
     */
    public static String render(JsonNode m) {
        require(m != null && m.isObject() && m.size() > 0, "manifest is empty or not an object");
        JsonNode fixtures = req(m, "fixtures");
        require(fixtures.isArray() && !fixtures.isEmpty(),
                "manifest declares no fixtures - refusing to publish a guarantee over nothing");
        JsonNode counts = req(m, "counts");
        JsonNode known = req(m, "knownLossy");
        require(known.isArray() && !known.isEmpty(),
                "manifest declares no knownLossy items - refusing to publish a fidelity document "
                        + "whose up-front warning list is empty");

        StringBuilder b = new StringBuilder(200_000);

        header(b, m, counts);
        readThisFirst(b, known);
        whatItMeans(b, m);
        theStacks(b, m);
        classifications(b, m, counts);
        configurationCaveat(b, m, counts);
        fixtureTables(b, m, counts);
        pairs(b, m);
        overrides(b, m);
        runbook(b, counts);

        return b.toString();
    }

    private static void header(StringBuilder b, JsonNode m, JsonNode counts) {
        line(b, "<!-- GENERATED FILE - DO NOT EDIT BY HAND. -->");
        line(b, "<!-- Source of truth: " + MANIFEST_PATH + " -->");
        line(b, "<!-- Regenerate with FidelityDocGenerator; see the last section. -->");
        line(b, "<!-- RoundTripFidelityDocTest fails the build when this file and the manifest "
                + "disagree. -->");
        blank(b);
        line(b, "# " + text(m, "title"));
        blank(b);
        line(b, "**What a document loses, and does not lose, when NexusPiercer flattens and "
                + "reconstructs it.**");
        blank(b);
        line(b, "Measured, not asserted. Every number and every row below is generated from the "
                + "corpus manifest");
        line(b, "recorded on " + text(m, "recorded") + ", and the corpus is executed on every "
                + "build.");
        blank(b);
        line(b, "| | count |");
        line(b, "| --- | ---: |");
        line(b, "| documents in the corpus | " + counts.path("total").asInt() + " |");
        line(b, "| reproduce the source exactly (`LOSSLESS`) | " + counts.path("lossless").asInt() + " |");
        line(b, "| lose something, by accepted design (`ACCEPTED_LOSS`) | "
                + counts.path("acceptedLoss").asInt() + " |");
        line(b, "| lose something, wrongly (`DEFECT`) | " + counts.path("defect").asInt() + " |");
        line(b, "| measured under non-default configuration | "
                + counts.path("nonDefaultConfig").asInt() + " |");
        line(b, "| `LOSSLESS` rows that do **not** hold through the default reconstruction entry "
                + "point | " + counts.path("losslessNotUnderDefaultReconstruction").asInt() + " |");
        line(b, "| `LOSSLESS` rows the **published recipe on this page** cannot reproduce | "
                + counts.path("losslessNotUnderPublishedRecipe").asInt() + " |");
        line(b, "| rows for which no published recipe exists (schema-only paths) | "
                + counts.path("publishedRecipeNotApplicable").asInt() + " |");
        line(b, "| rows the **published recipe** cannot reproduce at all | "
                + counts.path("holdsUnderPublishedRecipeNo").asInt() + " |");
        blank(b);
        line(b, "**" + counts.path("acceptedLoss").asInt() + " + " + counts.path("defect").asInt()
                + " = " + (counts.path("acceptedLoss").asInt() + counts.path("defect").asInt())
                + " of " + counts.path("total").asInt() + " documents do not survive a round "
                + "trip.** That ratio is the headline fact about");
        line(b, "this library. It is high because the corpus was built adversarially - it hunts "
                + "for the shapes that");
        line(b, "break rather than sampling shapes at random - but every one of those rows is a "
                + "shape real data has.");
        blank(b);
    }

    private static void readThisFirst(StringBuilder b, JsonNode known) {
        h2(b, "1. Read this before you depend on the library");
        line(b, "Stated without softening, because the point of the list is that you meet these "
                + "here rather than in");
        line(b, "production. Each item names the fixtures that hold it in place; if the "
                + "behaviour is ever repaired,");
        line(b, "those fixtures fail and the item has to be withdrawn from this page.");
        blank(b);
        int n = 0;
        for (JsonNode k : known) {
            n++;
            String tag = "INERT_CONTROL".equals(k.path("kind").asText("LOSS"))
                    ? " *(a setting that does nothing, not a data loss)*" : "";
            line(b, "**" + n + ". " + k.path("headline").asText() + "**" + tag);
            blank(b);
            line(b, k.path("statement").asText());
            blank(b);
            List<String> ids = strings(k.path("fixtures"));
            line(b, "<sub>Pinned by: " + join(ids, ", ", "`") + "</sub>");
            blank(b);
        }
    }

    private static void whatItMeans(StringBuilder b, JsonNode m) {
        h2(b, "2. What round-trip fidelity means here");
        for (String para : text(m, "guarantee").split("\n\n")) {
            line(b, para.replace("\n", " ").trim());
            blank(b);
        }
        line(b, "Concretely, two documents count as equal only when every scalar has the same "
                + "runtime type, every");
        line(b, "list has the same length and order, every key set matches, and an absent key is "
                + "distinct from a");
        line(b, "present null. `1`, `1L` and `\"1\"` are three different values; `-0.0` is "
                + "distinct from `0.0`;");
        line(b, "`37.7740` is distinct from `37.774`.");
        blank(b);
    }

    private static void theStacks(StringBuilder b, JsonNode m) {
        JsonNode stacks = req(m, "stacks");
        h2(b, "3. The stacks this applies to");
        line(b, "A fixture declared `BOTH` is measured on Stack A and Stack B independently and "
                + "must satisfy both.");
        blank(b);
        Iterator<String> names = stacks.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            JsonNode s = stacks.get(name);
            h3(b, s.path("title").asText() + "  <sub>`" + name + "`</sub>");
            line(b, "```java");
            for (String l : s.path("code").asText().split("\n")) {
                line(b, l);
            }
            line(b, "```");
            blank(b);
            line(b, s.path("note").asText());
            blank(b);
        }
    }

    private static void classifications(StringBuilder b, JsonNode m, JsonNode counts) {
        JsonNode cls = req(m, "classifications");
        JsonNode repair = req(m, "repairPolicy");
        h2(b, "4. What the three classifications mean");
        for (String name : new String[] {"LOSSLESS", "ACCEPTED_LOSS", "DEFECT"}) {
            require(cls.has(name), "manifest classifications is missing " + name);
            require(repair.has(name), "manifest repairPolicy is missing " + name);
            h3(b, "`" + name + "` - " + countFor(counts, name) + " documents");
            line(b, cls.get(name).asText());
            blank(b);
            line(b, "> **Repair status.** " + repair.get(name).asText());
            blank(b);
        }
    }

    private static void configurationCaveat(StringBuilder b, JsonNode m, JsonNode counts) {
        h2(b, "5. Which rows are only true under a non-default configuration");
        line(b, "Every row in the table below was measured under a stated configuration. "
                + counts.path("nonDefaultConfig").asInt() + " of "
                + counts.path("total").asInt() + " rows");
        line(b, "turn some knob away from its default, and the `config` column says which.");
        blank(b);
        line(b, "The sharper question is whether a row still describes what happens through the "
                + "library's *default*");
        line(b, "reconstruction entry point - `JsonReconstructor.quickReconstruct`, "
                + "`quickReconstructToJson`, or a");
        line(b, "default-built `AvroReconstructor`. The harness reconstructs a second time that "
                + "way and compares, and the");
        line(b, "`defaults` column publishes the answer. `NOT_APPLICABLE` means the row "
                + "reconstructs no data at all -");
        line(b, "the Avro schema-only rows.");
        blank(b);
        line(b, "The sharpest question of the three is the `recipe` column: does the row still "
                + "hold when you run the");
        line(b, "code block published in section 3 verbatim? That is strictly stronger than the "
                + "`defaults` column, which");
        line(b, "only re-reconstructs a map the row's own flattener already produced and is "
                + "therefore blind to any");
        line(b, "divergence the FLATTENER creates. Every recipe on this page is a compiled, "
                + "executed method body - a");
        line(b, "test asserts the text is byte-identical to source that javac accepted and that "
                + "running it reproduces the");
        line(b, "recorded answer.");
        blank(b);
        int recipeNo = 0;
        for (JsonNode e : req(m, "fixtures")) {
            if ("NO".equals(e.path("holdsUnderPublishedRecipe").asText())) {
                recipeNo++;
            }
        }
        line(b, "**" + recipeNo + " rows are not reproducible by the published recipe at all.**");
        blank(b);

        List<JsonNode> diverging = new ArrayList<>();
        for (JsonNode e : req(m, "fixtures")) {
            if ("NO".equals(e.path("holdsUnderDefaultReconstruction").asText())) {
                diverging.add(e);
            }
        }
        line(b, "**" + diverging.size() + " rows behave differently through the default entry "
                + "point, " + counts.path("losslessNotUnderDefaultReconstruction").asInt()
                + " of them `LOSSLESS` ones.**");
        line(b, "If you use the defaults, these rows do not describe what you will get:");
        blank(b);
        line(b, "| fixture | classification | required configuration |");
        line(b, "| --- | --- | --- |");
        for (JsonNode e : diverging) {
            line(b, "| `" + e.path("id").asText() + "` | `" + e.path("classification").asText()
                    + "` | " + cell(e.path("configDescription").asText()) + " |");
        }
        blank(b);
    }

    private static void fixtureTables(StringBuilder b, JsonNode m, JsonNode counts) {
        h2(b, "6. Every fixture");
        line(b, "`covers` is what the document is; `what happens` is precisely what the round "
                + "trip does to it.");
        line(b, "`defaults` is `YES` when the row also holds through the default reconstruction "
                + "entry point.");
        blank(b);

        Map<String, List<JsonNode>> byFamily = new LinkedHashMap<>();
        Iterator<String> fams = req(counts, "families").fieldNames();
        while (fams.hasNext()) {
            byFamily.put(fams.next(), new ArrayList<>());
        }
        for (JsonNode e : req(m, "fixtures")) {
            String family = e.path("family").asText();
            require(byFamily.containsKey(family),
                    "fixture " + e.path("id").asText() + " is in family '" + family
                            + "' but counts.families does not declare it");
            byFamily.get(family).add(e);
        }

        for (Map.Entry<String, List<JsonNode>> en : byFamily.entrySet()) {
            JsonNode fc = req(counts, "families").get(en.getKey());
            h3(b, "`" + en.getKey() + "` - " + fc.path("total").asInt() + " documents ("
                    + fc.path("LOSSLESS").asInt() + " lossless, "
                    + fc.path("ACCEPTED_LOSS").asInt() + " accepted loss, "
                    + fc.path("DEFECT").asInt() + " defect)");
            line(b, "| id | stack | covers | class | what happens | config | defaults | recipe "
                    + "| issue |");
            line(b, "| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
            for (JsonNode e : en.getValue()) {
                String issue = e.path("referenceIssue").asText("");
                line(b, "| `" + e.path("id").asText() + "` "
                        + "| " + e.path("stack").asText() + " "
                        + "| " + cell(e.path("title").asText()) + " "
                        + "| `" + e.path("classification").asText() + "` "
                        + "| " + cell(e.path("detail").asText()) + " "
                        + "| " + cell(e.path("configDescription").asText()) + " "
                        + "| " + e.path("holdsUnderDefaultReconstruction").asText() + " "
                        + "| " + e.path("holdsUnderPublishedRecipe").asText() + " "
                        + "| " + (issue.isEmpty() ? "-" : issue) + " |");
            }
            blank(b);
        }
    }

    private static void pairs(StringBuilder b, JsonNode m) {
        JsonNode pairs = req(m, "pairs");
        require(pairs.isArray() && !pairs.isEmpty(), "manifest declares no pair invariants");
        h2(b, "7. Cross-fixture invariants");
        line(b, "Some findings are not visible from any single document - each fixture "
                + "round-trips or fails on its own");
        line(b, "terms, and only the relationship between two of them is the defect. These are "
                + "asserted separately.");
        blank(b);
        line(b, "| invariant | kind | holds over | statement |");
        line(b, "| --- | --- | --- | --- |");
        for (JsonNode p : pairs) {
            String over = p.has("right")
                    ? "`" + p.path("left").asText() + "` + `" + p.path("right").asText() + "`"
                    : "`" + p.path("left").asText() + "` at `" + p.path("path").asText() + "`";
            line(b, "| `" + p.path("id").asText() + "` | " + p.path("kind").asText() + " | "
                    + over + " | " + cell(p.path("detail").asText()) + " |");
        }
        blank(b);
    }

    private static void overrides(StringBuilder b, JsonNode m) {
        JsonNode overrides = req(m, "classificationOverrides");
        h2(b, "8. Where measurement contradicted the designer");
        line(b, "Each fixture carries a hand-traced prediction made before it was run. These are "
                + "the rows where the");
        line(b, "prediction was wrong, kept visible rather than quietly corrected. Every "
                + "disagreement between a");
        line(b, "prediction and a published classification must appear here, and every entry "
                + "here must correspond to a");
        line(b, "real disagreement - the corpus asserts that in both directions.");
        blank(b);
        if (overrides.isEmpty()) {
            line(b, "*No fixture currently disagrees with its prediction.*");
            blank(b);
            return;
        }
        for (JsonNode o : overrides) {
            line(b, "**`" + o.path("id").asText() + "` -> `" + o.path("classification").asText()
                    + "`**");
            blank(b);
            line(b, o.path("reason").asText());
            blank(b);
        }
    }

    private static void runbook(StringBuilder b, JsonNode counts) {
        h2(b, "9. Running the corpus against your own data");
        line(b, "The corpus is not a fixed list you have to accept - it is a harness you can "
                + "point at your own");
        line(b, "documents, which is the only way to find out what *your* data loses.");
        blank(b);
        h3(b, "Run the published corpus");
        line(b, "```bash");
        line(b, "./mvnw -o test -Dtest=RoundTripFidelityCorpusTest");
        line(b, "```");
        blank(b);
        line(b, "All " + counts.path("total").asInt() + " documents, on every declared stack, "
                + "plus the cross-fixture invariants and the");
        line(b, "control probes. A green run means the library still behaves exactly as this "
                + "page says - including still");
        line(b, "being broken in exactly the ways it says.");
        blank(b);
        h3(b, "Add one of your own documents");
        line(b, "1. Drop a fixture at `src/test/resources/fidelity/<family>/<your-id>.json`. "
                + "Copy the nearest existing");
        line(b, "   file and replace `input`, `title`, `rationale`, `catchesBugClass`, "
                + "`cannotCatch` and `predicted`.");
        line(b, "   Write `predicted` **before** you run anything - a prediction made after the "
                + "fact measures nothing.");
        line(b, "2. Add a matching entry to `" + MANIFEST_PATH + "` and bump the counts. The "
                + "corpus fails if a fixture");
        line(b, "   file exists that no manifest entry claims, or the reverse, so this step "
                + "cannot be skipped.");
        line(b, "3. Record the measured behaviour:");
        line(b, "```bash");
        line(b, "./mvnw -o test-compile dependency:build-classpath -Dmdep.outputFile=target/test-cp.txt");
        line(b, "java -cp \"target/classes;target/test-classes;$(cat target/test-cp.txt)\" \\");
        line(b, "     io.github.pierce.fidelity.FidelityCorpusRecorder src/test/resources/fidelity");
        line(b, "```");
        line(b, "   The recorder writes only the `expected` block. It never edits the manifest "
                + "and never decides a");
        line(b, "   classification: it prints the fixtures whose measurement disagrees with the "
                + "manifest and stops.");
        line(b, "   Whether a measured loss is `ACCEPTED_LOSS` or `DEFECT` is a judgement a "
                + "person makes and signs.");
        line(b, "4. Compare your prediction with the recording. Where they differ, add a "
                + "`classificationOverrides`");
        line(b, "   entry saying so; the corpus refuses to let a disagreement go unexplained.");
        blank(b);
        h3(b, "Just check whether one document survives");
        line(b, "If you only want a yes/no answer for a document, you do not need a fixture:");
        blank(b);
        line(b, "```java");
        line(b, "Map<String, Object> src  = new ObjectMapper().readValue(json, new TypeReference<>() { });");
        line(b, "Map<String, Object> flat = MapFlattener.builder().build().flatten(src);");
        line(b, "Map<String, Object> back = JsonReconstructor.quickReconstruct(flat);");
        line(b, "boolean survives = back.equals(src);   // Map.equals, not JsonReconstructor.verify");
        line(b, "```");
        blank(b);
        line(b, "Use `Map.equals`, not `JsonReconstructor.verify()`. `verify()` treats String and "
                + "Number as a compatible");
        line(b, "type pair and compares doubles with an absolute tolerance of `1e-6`; wired to "
                + "that oracle this corpus");
        line(b, "reports the 30-digit-integer row and the decimal-precision row as perfect while "
                + "the data is demonstrably");
        line(b, "changed. It is not a fidelity check.");
        blank(b);

        h2(b, "10. Regenerating this document");
        line(b, "This page is generated. Editing it by hand is pointless - "
                + "`RoundTripFidelityDocTest` compares it");
        line(b, "against a fresh render of the manifest and fails on any difference. Change "
                + "`" + MANIFEST_PATH + "`,");
        line(b, "then:");
        blank(b);
        line(b, "```bash");
        line(b, "./mvnw -o test-compile dependency:build-classpath -Dmdep.outputFile=target/test-cp.txt");
        line(b, "java -cp \"target/classes;target/test-classes;$(cat target/test-cp.txt)\" \\");
        line(b, "     io.github.pierce.fidelity.FidelityDocGenerator \\");
        line(b, "     " + MANIFEST_PATH + " " + DOC_PATH);
        line(b, "```");
    }

    // ------------------------------------------------------------------ helpers

    private static int countFor(JsonNode counts, String classification) {
        return switch (classification) {
            case "LOSSLESS" -> counts.path("lossless").asInt();
            case "ACCEPTED_LOSS" -> counts.path("acceptedLoss").asInt();
            default -> counts.path("defect").asInt();
        };
    }

    /** Markdown table cells cannot contain a raw pipe or a newline. */
    private static String cell(String raw) {
        return raw.replace("\r", " ").replace("\n", " ").replace("|", "\\|")
                .replaceAll("\\s+", " ").trim();
    }

    private static List<String> strings(JsonNode array) {
        List<String> out = new ArrayList<>();
        for (JsonNode n : array) {
            out.add(n.asText());
        }
        return out;
    }

    private static String join(List<String> items, String sep, String wrap) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) {
                sb.append(sep);
            }
            sb.append(wrap).append(items.get(i)).append(wrap);
        }
        return sb.toString();
    }

    private static JsonNode req(JsonNode m, String field) {
        JsonNode n = m.get(field);
        require(n != null && !n.isNull(), "manifest is missing required block '" + field + "'");
        return n;
    }

    private static String text(JsonNode m, String field) {
        String v = req(m, field).asText("");
        require(!v.isBlank(), "manifest field '" + field + "' is blank");
        return v;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException("CANNOT GENERATE THE FIDELITY DOCUMENT: " + message);
        }
    }

    private static void h2(StringBuilder b, String s) {
        line(b, "## " + s);
        blank(b);
    }

    private static void h3(StringBuilder b, String s) {
        line(b, "### " + s);
        blank(b);
    }

    private static void line(StringBuilder b, String s) {
        b.append(s).append(NL);
    }

    private static void blank(StringBuilder b) {
        b.append(NL);
    }
}
