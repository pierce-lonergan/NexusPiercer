package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The round-trip fidelity guarantee, enforced.
 *
 * <h2>What this test is for</h2>
 *
 * <p>{@code src/test/resources/fidelity/manifest.json} is a contract published to consumers: for
 * every document in the corpus it states exactly what survives a flatten/reconstruct round trip
 * and what does not. This class is the mechanism that stops the repository changing any of it
 * quietly. The corpus size is deliberately not written out here - it is derived and asserted in
 * {@link #discoveredFixtureCountEqualsManifestCount()}, and a number typed into prose is exactly
 * the drift the tally gates below were added to catch.</p>
 *
 * <p>Each fixture is checked three ways, and the three are separate test methods so the executed
 * count is visible rather than inferred:</p>
 * <ol>
 *   <li>the flattened intermediate matches its recording - which localises blame, because on many
 *       fixtures flattening is correct and the entire loss is in {@code JsonReconstructor};</li>
 *   <li>the reconstructed document matches its recording, exactly, including runtime types;</li>
 *   <li>the manifest's classification matches the measured losslessness, per declared stack.</li>
 * </ol>
 *
 * <h2>The property that matters: fixing a bug must break this test</h2>
 *
 * <p>A {@code DEFECT} fixture does not assert "the round trip failed". It asserts the exact wrong
 * document that comes out today. The moment someone repairs the defect, the reconstruction stops
 * matching its recording and this test goes red - <b>on purpose</b>. The build cannot be made
 * green again by improving the library alone; the fix must land together with an updated
 * manifest, because the guarantee published to consumers changes at that moment. The same holds
 * for {@code ACCEPTED_LOSS}: the deal cannot silently get worse, and it cannot silently get
 * better either.</p>
 *
 * <p>The corollary is the rule this repository keeps having to relearn: <b>a red result here is
 * never fixed by editing the fixture.</b> It is fixed by deciding, in writing, what the new
 * guarantee is.</p>
 *
 * <h2>Why the oracle is not JsonReconstructor.verify()</h2>
 *
 * <p>{@code verify()} declares String-vs-Number a compatible type pair and compares doubles with
 * an absolute tolerance of 1e-6. Wired to that, this corpus would report the 30-digit-integer
 * fixture and the decimal-precision fixture as PERFECT while the data is demonstrably changed -
 * a gate that appears present and does nothing, which is precisely the pathology this repository
 * has found a dozen times. {@link FidelityRender} is used instead: strict recursive equality with
 * runtime types attached, {@code Double.toString} so {@code -0.0} stays distinct from
 * {@code 0.0}, scale-sensitive {@code BigDecimal}, and absent-key distinct from present-null.</p>
 *
 * <h2>Failing loudly rather than vacuously</h2>
 *
 * <p>A missing manifest, an unparseable manifest, a manifest entry whose fixture file is absent,
 * a fixture file on disk that no manifest entry claims, a declared count that disagrees with the
 * declared fixtures, or a fixture that runs no stack at all - every one of these fails the build
 * with a named error. None of them can present as "0 fixtures executed, all good".</p>
 */
@DisplayName("Round-trip fidelity corpus: the published guarantee")
class RoundTripFidelityCorpusTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** The three per-stack verdict keys. A fixture must record at least one of them. */
    private static final String[] STACK_FLAGS = {"losslessMap", "losslessJson", "losslessAvro"};

    // ------------------------------------------------------------------ loading (fails loudly)
    //
    // Delegated to FidelityCorpus so the published-snippet gate reads the SAME corpus this class
    // asserts. Two loaders would be two ideas of what the corpus is, and they could disagree about
    // which fixtures exist - the failure mode where a gate runs over a smaller set than it appears
    // to. Every failure message below still names the problem and refuses to present as an empty
    // pass; see FidelityCorpus.

    private static Path corpusRoot() {
        return FidelityCorpus.corpusRoot();
    }

    private static JsonNode manifest() {
        return FidelityCorpus.manifest();
    }

    private static List<JsonNode> manifestEntries() {
        return FidelityCorpus.manifestEntries();
    }

    static Stream<String> manifestFixtureIds() {
        return FidelityCorpus.manifestFixtureIds();
    }

    static Stream<String> fixtureIdsWithProbes() {
        return manifestFixtureIds().filter(id -> fixture(id).probe() != null);
    }

    static Stream<JsonNode> manifestPairs() {
        JsonNode pairs = manifest().get("pairs");
        if (pairs == null || !pairs.isArray() || pairs.isEmpty()) {
            throw new AssertionError("FIDELITY CORPUS DID NOT RUN: the manifest declares no pair "
                    + "invariants. The cross-fixture collisions are only observable as pairs, so "
                    + "an empty list silently deletes them.");
        }
        List<JsonNode> out = new ArrayList<>();
        pairs.forEach(out::add);
        return out.stream();
    }

    private static JsonNode entry(String id) {
        return FidelityCorpus.entry(id);
    }

    private static FidelityFixture fixture(String id) {
        return FidelityCorpus.fixture(id);
    }

    private static FidelityRunner.Measurement measure(String id) {
        return FidelityCorpus.measure(id);
    }

    // ------------------------------------------------------------------ corpus integrity

    @Test
    @DisplayName("the manifest exists, parses, and declares a non-empty corpus")
    void manifestIsPresentParseableAndNonEmpty() {
        JsonNode m = manifest();
        assertThat(m.path("schemaVersion").asInt()).as("manifest schemaVersion").isEqualTo(1);
        assertThat(m.path("guarantee").asText()).as("manifest must state the guarantee").isNotBlank();
        assertThat(manifestEntries()).as("declared fixtures").isNotEmpty();
        for (JsonNode e : manifestEntries()) {
            String id = e.path("id").asText();
            assertThat(id).as("every entry needs an id").isNotBlank();
            assertThat(e.path("file").asText()).as("entry " + id + " needs a file").isNotBlank();
            assertThat(e.path("detail").asText()).as("entry " + id + " needs a detail").isNotBlank();
            assertThat(e.path("classification").asText())
                    .as("entry " + id + " classification")
                    .isIn("LOSSLESS", "ACCEPTED_LOSS", "DEFECT");
        }
    }

    @Test
    @DisplayName("the fixture files on disk are exactly the fixtures the manifest declares")
    void discoveredFixtureCountEqualsManifestCount() throws IOException {
        Path root = corpusRoot();
        Set<String> onDisk = new TreeSet<>();
        try (Stream<Path> walk = Files.walk(root)) {
            walk.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".json"))
                    .filter(p -> !p.getFileName().toString().equals("manifest.json"))
                    .forEach(p -> onDisk.add(root.relativize(p).toString().replace('\\', '/')));
        }
        Set<String> declared = new TreeSet<>();
        for (JsonNode e : manifestEntries()) {
            declared.add(e.get("file").asText());
        }

        assertThat(onDisk).as("a fixture file exists that no manifest entry claims - "
                + "the corpus would run it against no contract").isEqualTo(declared);
        assertThat(onDisk).as("discovered fixture files").hasSize(declared.size());
        assertThat(onDisk.size()).as("VERIFY THE COUNT: discovered fixture files must equal the "
                + "manifest's declared total; a corpus that silently shrank is the failure mode "
                + "this repository keeps shipping")
                .isEqualTo(manifest().path("counts").path("total").asInt(-1));
    }

    @Test
    @DisplayName("the declared counts add up and match the declared classifications")
    void declaredCountsMatchTheDeclaredFixtures() {
        JsonNode counts = manifest().get("counts");
        assertThat(counts).as("manifest must publish counts").isNotNull();
        int lossless = 0;
        int accepted = 0;
        int defect = 0;
        for (JsonNode e : manifestEntries()) {
            switch (e.get("classification").asText()) {
                case "LOSSLESS" -> lossless++;
                case "ACCEPTED_LOSS" -> accepted++;
                default -> defect++;
            }
        }
        assertThat(lossless).as("lossless count").isEqualTo(counts.path("lossless").asInt(-1));
        assertThat(accepted).as("acceptedLoss count").isEqualTo(counts.path("acceptedLoss").asInt(-1));
        assertThat(defect).as("defect count").isEqualTo(counts.path("defect").asInt(-1));
        assertThat(lossless + accepted + defect).as("total count")
                .isEqualTo(counts.path("total").asInt(-1));
        assertThat(manifestEntries()).as("declared fixtures").hasSize(lossless + accepted + defect);
    }

    // ------------------------------------------------------------------ the guarantee itself

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("manifestFixtureIds")
    @DisplayName("flattened intermediate matches its recording")
    void flattenedIntermediateMatchesTheRecording(String id) {
        FidelityFixture fx = fixture(id);
        FidelityRunner.Measurement m = measure(id);
        String recorded = fx.expected().path("flat").asText(null);
        assertThat(recorded)
                .as("fixture %s records no flattened intermediate - it would assert nothing", id)
                .isNotNull();
        assertThat(m.recorded.get("flat"))
                .as("%s: the FLATTENED FORM changed. This is the flattener's output, before "
                        + "reconstruction; on many fixtures it is correct and the whole loss is "
                        + "downstream, so a change here points at MapFlattener specifically. "
                        + "Manifest says: %s", id, entry(id).get("detail").asText())
                .isEqualTo(recorded);

        // A BOTH fixture runs TWO flatteners. Until this assertion existed the JSON one's output
        // was computed and thrown away, so 29 of the 32 LOSSLESS rows localised blame for the MAP
        // stack only and a JsonFlattener-specific divergence could reach the reconstruction step
        // before anything noticed.
        if (!"BOTH".equals(fx.stack())) {
            assertThat(fx.expected().has("flatJson"))
                    .as("%s declares stack %s, so there is no second flattener to record",
                            id, fx.stack())
                    .isFalse();
            return;
        }
        String recordedJson = fx.expected().path("flatJson").asText(null);
        assertThat(recordedJson)
                .as("%s declares stack BOTH but records no 'flatJson'. The JSON stack's flattened "
                        + "intermediate would then be measured and discarded - a control that "
                        + "appears present and does nothing. Re-record the corpus.", id)
                .isNotNull();
        assertThat(m.recorded.get("flatJson"))
                .as("%s: JsonFlattener's FLATTENED FORM changed while MapFlattener's did not, or "
                        + "changed differently. The two parse with different mappers, so this "
                        + "blames the JSON flattening step specifically. Manifest says: %s",
                        id, entry(id).get("detail").asText())
                .isEqualTo(recordedJson);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("manifestFixtureIds")
    @DisplayName("reconstruction matches its recording exactly")
    void reconstructionMatchesTheRecording(String id) {
        FidelityFixture fx = fixture(id);
        FidelityRunner.Measurement m = measure(id);
        JsonNode recorded = fx.expected();
        assertThat(recorded.isObject() && recorded.size() > 0)
                .as("fixture %s has no recorded expectations at all", id).isTrue();

        // The loop below can only compare keys the RECORDING carries, so a key quietly deleted
        // from a fixture file would stop being asserted while the fixture still looked complete -
        // the same shape of hole that left the JSON stack's flattened form unchecked. Pinning the
        // key SET makes a deletion a failure instead of a silent narrowing.
        Set<String> recordedKeys = new TreeSet<>();
        recorded.fieldNames().forEachRemaining(recordedKeys::add);
        assertThat(recordedKeys)
                .as("%s: the set of recorded renderings must be exactly the set the harness "
                        + "produces. A missing key is an assertion that quietly stopped running; "
                        + "an extra key is a recording of something no longer measured. "
                        + "Re-record the corpus.", id)
                .isEqualTo(new TreeSet<>(m.recorded.keySet()));

        int compared = 0;
        Iterator<String> names = recorded.fieldNames();
        while (names.hasNext()) {
            String key = names.next();
            if ("flat".equals(key) || "flatJson".equals(key)) {
                continue;
            }
            Object measured = m.recorded.get(key);
            assertThat(measured)
                    .as("%s: the harness no longer produces '%s' at all, so its recording is "
                            + "unverified", id, key)
                    .isNotNull();
            JsonNode measuredNode = JSON.valueToTree(measured);
            assertThat(measuredNode)
                    .as("%s: '%s' changed. If a defect was just fixed this failure is CORRECT - "
                            + "update the manifest and the recording deliberately, do not weaken "
                            + "the fixture. Manifest says: %s", id, key, entry(id).get("detail").asText())
                    .isEqualTo(recorded.get(key));
            compared++;
        }
        assertThat(compared).as("%s must compare at least the baseline and the reconstructed "
                + "document; a fixture that compares nothing is a disabled test", id)
                .isGreaterThanOrEqualTo(2);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("manifestFixtureIds")
    @DisplayName("the manifest classification holds against measured behaviour")
    void theDeclaredClassificationHolds(String id) {
        JsonNode e = entry(id);
        FidelityFixture fx = fixture(id);
        FidelityRunner.Measurement m = measure(id);
        String classification = e.get("classification").asText();

        List<String> ran = new ArrayList<>();
        boolean allLossless = true;
        for (String flag : STACK_FLAGS) {
            Object v = m.recorded.get(flag);
            if (v instanceof Boolean b) {
                ran.add(flag);
                allLossless &= b;
            }
        }
        assertThat(ran).as("%s ran no stack at all - it measures nothing and cannot fail", id)
                .isNotEmpty();
        assertThat(ran).as("%s declares stack %s, so the stacks actually executed must match",
                        id, fx.stack())
                .containsExactlyInAnyOrderElementsOf(expectedFlags(fx.stack()));

        if ("LOSSLESS".equals(classification)) {
            assertThat(allLossless)
                    .as("%s is published as LOSSLESS but the round trip no longer reproduces the "
                            + "source. Either a regression landed, or the guarantee is wrong. "
                            + "Manifest says: %s", id, e.get("detail").asText())
                    .isTrue();
        } else {
            assertThat(allLossless)
                    .as("%s is published as %s - a loss that is expected, bounded and recorded. "
                            + "The round trip is now LOSSLESS, so the loss is gone. That is good "
                            + "news and a deliberate manifest update, not something to paper "
                            + "over. Manifest says: %s", id, classification, e.get("detail").asText())
                    .isFalse();
        }
    }

    // ------------------------------------------------------------------ disclosure gates

    /**
     * A contract row that is only true under a non-default configuration has to say so <em>in the
     * contract</em>.
     *
     * <p>Adversarial verification drove the brief's Stack A verbatim - {@code MapFlattener.flatten}
     * then {@code JsonReconstructor.quickReconstruct} - across the LOSSLESS rows and found three
     * that do not hold that way: {@code nested-array-of-objects-explicit-hints},
     * {@code boundary-separator-on-round-trip} and
     * {@code multichar-separator-with-single-separator-char-in-name}. Every one of them states its
     * precondition in its own fixture file, and none of them stated it in {@code manifest.json},
     * which is the file the guarantee tells consumers to rely on.</p>
     *
     * <p>So the disclosure is now measured rather than written down: the harness reconstructs a
     * second time through the default entry point and this gate asserts the manifest's published
     * answer against that measurement. A row cannot claim to survive defaults unless it does.</p>
     */
    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("manifestFixtureIds")
    @DisplayName("the manifest discloses the configuration each row was measured under")
    void theConfigurationDisclosureIsPresentAndTrue(String id) {
        JsonNode e = entry(id);
        FidelityFixture fx = fixture(id);

        assertThat(e.path("configDescription").asText(""))
                .as("entry %s must republish the fixture's configDescription. A row whose "
                        + "configuration is recorded only in the fixture file is a caveat the "
                        + "consumer reading the contract never sees.", id)
                .isNotBlank()
                .isEqualTo(fx.configDescription());

        assertThat(e.has("requiresNonDefaultConfig"))
                .as("entry %s must publish requiresNonDefaultConfig", id).isTrue();
        assertThat(e.path("requiresNonDefaultConfig").asBoolean())
                .as("entry %s: requiresNonDefaultConfig must equal whether the fixture actually "
                        + "tunes MapFlattener, JsonReconstructor, AvroReconstructor or "
                        + "AvroSchemaFlattener. Declared config: %s", id, fx.config())
                .isEqualTo(declaresTuning(fx));

        String declared = e.path("holdsUnderDefaultReconstruction").asText("");
        assertThat(declared)
                .as("entry %s must publish holdsUnderDefaultReconstruction", id)
                .isIn(FidelityRunner.DEFAULTS_HOLD, FidelityRunner.DEFAULTS_DIVERGE,
                        FidelityRunner.DEFAULTS_NA);
        assertThat(declared)
                .as("%s: the manifest publishes holdsUnderDefaultReconstruction=%s, but "
                        + "re-running this fixture's flattened output through the library's "
                        + "DEFAULT entry point (JsonReconstructor.quickReconstruct / "
                        + "quickReconstructToJson / AvroReconstructor.builder().build()) "
                        + "measured the opposite. Either the row now behaves differently under "
                        + "defaults, or the disclosure is wrong. Manifest says: %s",
                        id, declared, e.get("detail").asText())
                .isEqualTo(FidelityRunner.defaultsVerdict(measure(id)));

        // The published RECIPE is a stronger claim than default reconstruction: the defaults arm
        // only re-reconstructs an already-flattened map, so it is blind to a divergence the
        // FLATTENER creates. A row measured under maxDepth(2) can hold under defaults and still be
        // unreproducible by anyone following the page verbatim. The verdict itself is measured in
        // PublishedSnippetsCompileTest; this gate is what makes publishing it non-optional.
        assertThat(e.path("holdsUnderPublishedRecipe").asText(""))
                .as("entry %s must publish holdsUnderPublishedRecipe", id)
                .isIn(FidelityRunner.DEFAULTS_HOLD, FidelityRunner.DEFAULTS_DIVERGE,
                        FidelityRunner.DEFAULTS_NA);
    }

    /** True when the fixture turns any knob the library would otherwise leave at its default. */
    private static boolean declaresTuning(FidelityFixture fx) {
        JsonNode c = fx.config();
        if (c == null || !c.isObject()) {
            return false;
        }
        // The Avro WRITER'S SCHEMA is data, not configuration - an Avro fixture cannot exist
        // without one, so counting it would make every Avro row read as "non-default" and the
        // flag would stop discriminating.
        // avro.enriched is FlattenOptions - separator, collisionPolicy, inheritDoc, injections -
        // and a row that sets it is measuring a tuned flattener. avro.enrichedCompare is
        // deliberately a SIBLING key rather than a member of this block: it names the comparator,
        // not a knob, and tuned() is a size>0 test, so a selector living inside the block would
        // flip every enriched row to "non-default" and the flag would stop discriminating.
        return tuned(c.path("mapFlattener"))
                || tuned(c.path("reconstructor"))
                || tuned(c.path("avro").path("reconstructor"))
                || tuned(c.path("avro").path("schemaFlattener"))
                || tuned(c.path("avro").path("enriched"));
    }

    private static boolean tuned(JsonNode node) {
        return node != null && node.isObject() && node.size() > 0;
    }

    @Test
    @DisplayName("every classification override names a real disagreement, and every disagreement has one")
    void classificationOverridesAreLiveAndComplete() {
        JsonNode overrides = manifest().get("classificationOverrides");
        assertThat(overrides).as("the manifest must carry a classificationOverrides array").isNotNull();
        assertThat(overrides.isArray()).as("classificationOverrides must be an array").isTrue();

        Set<String> declared = new TreeSet<>();
        for (JsonNode o : overrides) {
            String id = o.path("id").asText();
            assertThat(id).as("every override needs an id").isNotBlank();
            assertThat(declared.add(id)).as("duplicate override for %s", id).isTrue();
            assertThat(o.path("reason").asText()).as("override %s must state WHY", id).isNotBlank();
            JsonNode e = entry(id);
            assertThat(o.path("classification").asText())
                    .as("override %s records a classification that is no longer what the manifest "
                            + "publishes for that fixture", id)
                    .isEqualTo(e.get("classification").asText());
            assertThat(fixture(id).predicted().path("classification").asText())
                    .as("override %s says measurement corrected the prediction, but the fixture "
                            + "now predicts exactly what the manifest publishes. The override is "
                            + "stale and is documenting a disagreement that no longer exists.", id)
                    .isNotEqualTo(e.get("classification").asText());
        }

        Set<String> disagreements = new TreeSet<>();
        for (JsonNode e : manifestEntries()) {
            String id = e.get("id").asText();
            String predicted = fixture(id).predicted().path("classification").asText("");
            assertThat(predicted)
                    .as("%s must record the designer's predicted classification, so that "
                            + "prediction and measurement can be compared at all", id)
                    .isNotBlank();
            if (!predicted.equals(e.get("classification").asText())) {
                disagreements.add(id);
            }
        }

        assertThat(declared)
                .as("classificationOverrides must be exactly the set of fixtures whose PREDICTED "
                        + "classification differs from the PUBLISHED one. An unexplained "
                        + "disagreement is a silent reclassification; an override with no "
                        + "disagreement behind it is decoration.")
                .isEqualTo(disagreements);
    }

    @Test
    @DisplayName("the per-family counts match the declared fixtures")
    void perFamilyCountsMatchTheDeclaredFixtures() {
        JsonNode families = manifest().path("counts").path("families");
        assertThat(families.isObject() && families.size() > 0)
                .as("the manifest publishes a per-family breakdown; an empty one would let the "
                        + "corpus lose a whole family without the totals noticing")
                .isTrue();

        Map<String, int[]> tally = new java.util.TreeMap<>();
        for (JsonNode e : manifestEntries()) {
            int[] t = tally.computeIfAbsent(e.get("family").asText(), k -> new int[4]);
            t[0]++;
            switch (e.get("classification").asText()) {
                case "LOSSLESS" -> t[1]++;
                case "ACCEPTED_LOSS" -> t[2]++;
                default -> t[3]++;
            }
        }

        Set<String> published = new TreeSet<>();
        families.fieldNames().forEachRemaining(published::add);
        assertThat(published)
                .as("the families the manifest publishes counts for must be exactly the families "
                        + "its fixtures belong to")
                .isEqualTo(new TreeSet<>(tally.keySet()));

        for (Map.Entry<String, int[]> en : tally.entrySet()) {
            JsonNode f = families.get(en.getKey());
            int[] t = en.getValue();
            assertThat(f.path("total").asInt(-1)).as("%s total", en.getKey()).isEqualTo(t[0]);
            assertThat(f.path("LOSSLESS").asInt(-1)).as("%s LOSSLESS", en.getKey()).isEqualTo(t[1]);
            assertThat(f.path("ACCEPTED_LOSS").asInt(-1)).as("%s ACCEPTED_LOSS", en.getKey()).isEqualTo(t[2]);
            assertThat(f.path("DEFECT").asInt(-1)).as("%s DEFECT", en.getKey()).isEqualTo(t[3]);
        }
    }

    /** The five published tallies, recomputed from a fixture list. Order matches TALLY_NAMES. */
    private static final List<String> TALLY_NAMES = List.of(
            "nonDefaultConfig", "losslessNotUnderDefaultReconstruction",
            "losslessNotUnderPublishedRecipe", "publishedRecipeNotApplicable",
            "holdsUnderPublishedRecipeNo");

    private static int[] tallies(List<JsonNode> entries) {
        int[] t = new int[TALLY_NAMES.size()];
        for (JsonNode e : entries) {
            boolean lossless = "LOSSLESS".equals(e.path("classification").asText());
            String recipe = e.path("holdsUnderPublishedRecipe").asText("");
            if (e.path("requiresNonDefaultConfig").asBoolean()) {
                t[0]++;
            }
            if (lossless && FidelityRunner.DEFAULTS_DIVERGE.equals(
                    e.path("holdsUnderDefaultReconstruction").asText())) {
                t[1]++;
            }
            if (lossless && FidelityRunner.DEFAULTS_DIVERGE.equals(recipe)) {
                t[2]++;
            }
            if (FidelityRunner.DEFAULTS_NA.equals(recipe)) {
                t[3]++;
            }
            if (FidelityRunner.DEFAULTS_DIVERGE.equals(recipe)) {
                t[4]++;
            }
        }
        return t;
    }

    @Test
    @DisplayName("the configuration tallies the contract publishes are true")
    void configurationTalliesMatchTheDeclaredFixtures() {
        JsonNode counts = manifest().path("counts");
        int[] t = tallies(manifestEntries());
        int nonDefault = t[0];
        int losslessNotUnderDefaults = t[1];
        int losslessNotUnderRecipe = t[2];
        int recipeNotApplicable = t[3];
        int recipeNo = t[4];
        assertThat(counts.path("nonDefaultConfig").asInt(-1))
                .as("the headline count of rows measured under non-default configuration")
                .isEqualTo(nonDefault);
        assertThat(counts.path("losslessNotUnderDefaultReconstruction").asInt(-1))
                .as("the headline count of LOSSLESS rows that do NOT hold through the default "
                        + "reconstruction entry point. This is the number a consumer who reads "
                        + "only the summary most needs.")
                .isEqualTo(losslessNotUnderDefaults);
        assertThat(counts.path("losslessNotUnderPublishedRecipe").asInt(-1))
                .as("the headline count of LOSSLESS rows a consumer following the published recipe "
                        + "verbatim cannot reproduce")
                .isEqualTo(losslessNotUnderRecipe);
        // Load-bearing and easy to omit: without it, a bug that stopped the recipe measurement
        // running would turn every row NOT_APPLICABLE and the next person to update the manifest
        // would bless that as the new truth. Pinning the NA population makes a mass-NA regression
        // a count failure - and FidelityCorpusRecorder never edits manifest.json, which is exactly
        // why the count belongs here.
        assertThat(counts.path("publishedRecipeNotApplicable").asInt(-1))
                .as("the count of rows for which no published recipe exists (the Avro schema-path "
                        + "and enriched-schema-path modes reconstruct no data)")
                .isEqualTo(recipeNotApplicable);
        // Added because the number DID drift. The known-lossy warning carried this population as
        // prose and said 25 while the generated line four sections later said 24 - both on the
        // published page, neither gated, because every neighbouring number was derived and this
        // one was typed. The prose no longer states it and this assertion pins it.
        assertThat(counts.path("holdsUnderPublishedRecipeNo").asInt(-1))
                .as("the count of rows a consumer following the published recipe verbatim cannot "
                        + "reproduce at all. This is the headline number of the recipe column and "
                        + "the one most likely to be restated in prose that nothing checks.")
                .isEqualTo(recipeNo);
    }

    /**
     * The tally gate above, drilled the other two ways.
     *
     * <p>Good input passing is what the sibling test measures. On its own that is a comparison
     * between a number in a file and a number derived from the same file's neighbours, and it would
     * look identical if the derivation had stopped discriminating. So: a synthetic violation must
     * move every tally it touches, and an empty corpus must not reproduce the published numbers.
     * This exists because the population it guards is the one that DID drift - the known-lossy
     * warning said 25 while the generated line said 24, on the same published page.</p>
     */
    @Test
    @DisplayName("the tally derivation moves when a row changes, and collapses when rows vanish")
    void theTallyDerivationIsLiveNotConstant() {
        JsonNode counts = manifest().path("counts");
        int[] real = tallies(manifestEntries());
        assertThat(real).as("VERIFY THE COUNT: five tallies are published and five are derived")
                .hasSize(TALLY_NAMES.size());

        for (int i = 0; i < TALLY_NAMES.size(); i++) {
            assertThat(counts.path(TALLY_NAMES.get(i)).asInt(-1))
                    .as("%s must be published and must equal the derivation", TALLY_NAMES.get(i))
                    .isEqualTo(real[i]);
        }

        // MISSING/EMPTY INPUT BLOCKS. Every published tally here is non-zero, so a derivation over
        // zero rows must disagree with all five. If it did not, the tally would be a constant.
        int[] empty = tallies(List.of());
        for (int i = 0; i < TALLY_NAMES.size(); i++) {
            assertThat(real[i]).as("%s is zero, so an empty corpus would reproduce it and the gate "
                    + "could not tell a vanished corpus from a correct one", TALLY_NAMES.get(i))
                    .isGreaterThan(0);
            assertThat(empty[i]).as("%s derived over zero rows must be zero", TALLY_NAMES.get(i))
                    .isZero();
        }

        // SYNTHETIC VIOLATION BLOCKS. Flip one row's recipe verdict from NO to YES: the NO
        // population and nothing else must move, and the published number must stop matching.
        List<JsonNode> mutated = new ArrayList<>();
        boolean flipped = false;
        for (JsonNode e : manifestEntries()) {
            ObjectNode copy = e.deepCopy();
            if (!flipped && FidelityRunner.DEFAULTS_DIVERGE.equals(
                    e.path("holdsUnderPublishedRecipe").asText())) {
                copy.put("holdsUnderPublishedRecipe", FidelityRunner.DEFAULTS_HOLD);
                flipped = true;
            }
            mutated.add(copy);
        }
        assertThat(flipped).as("no row publishes a NO verdict, so the mutation below would be a "
                + "no-op and this drill would pass without drilling anything").isTrue();
        int[] after = tallies(mutated);
        assertThat(after[4]).as("flipping one NO verdict to YES must reduce the NO tally by one; "
                + "if it does not, the derivation is not reading the field it claims to read")
                .isEqualTo(real[4] - 1);
        assertThat(counts.path("holdsUnderPublishedRecipeNo").asInt(-1))
                .as("and the published number must then disagree with the derivation - which is "
                        + "the failure a stale manifest edit has to produce")
                .isNotEqualTo(after[4]);
        assertThat(after[0]).as("nonDefaultConfig must be unaffected by a recipe-verdict flip")
                .isEqualTo(real[0]);
        assertThat(after[3]).as("the NOT_APPLICABLE tally must be unaffected by a NO->YES flip")
                .isEqualTo(real[3]);
    }

    /**
     * The {@code ENRICHED_STREAM} comparator, drilled so it cannot become a gate that only passes.
     *
     * <p>Both rows that use it now agree, because {@code flatten()} and {@code stream()} agree -
     * that agreement is the repaired behaviour. But a comparator whose two rows can only ever
     * report "equal" proves nothing about the comparator, and no fixture can restore the missing
     * arm: after the repair there is no schema and no configuration under which the two entry
     * points disagree. So the discrimination is asserted here instead. Configuring an injection
     * must genuinely change what {@code stream()} produces.</p>
     *
     * <p>Before {@code stream()} honoured injections this assertion failed, because the two calls
     * returned byte-identical lists. That failure WAS the defect, expressed at harness level.</p>
     *
     * <p>An injection may now legitimately REFUSE rather than emit — an injected name equal to a
     * source column's is a collision, and the guard that catches it lives on the output rather
     * than in the traversal. That is discrimination of the strongest kind and is accepted as such,
     * but it is also a way for every injecting row to stop exercising the size arm below, so the
     * count of rows that actually emit is asserted rather than assumed.</p>
     */
    @Test
    @DisplayName("the ENRICHED_STREAM comparator still discriminates: an injection changes stream()")
    void enrichedStreamComparatorStillDiscriminates() {
        List<String> injecting = new ArrayList<>();
        for (JsonNode e : manifestEntries()) {
            String id = e.get("id").asText();
            JsonNode inject = fixture(id).config().path("avro").path("enriched").path("inject");
            if (inject.isArray() && !inject.isEmpty()) {
                injecting.add(id);
            }
        }
        assertThat(injecting)
                .as("no fixture configures avro.enriched.inject, so the comparison below would be "
                        + "between two identical configurations and would drill nothing")
                .isNotEmpty();

        int emitting = 0;
        int refusing = 0;
        for (String id : injecting) {
            JsonNode avro = fixture(id).config().path("avro");
            org.apache.avro.Schema schema =
                    new org.apache.avro.Schema.Parser().parse(avro.path("avsc").toString());
            int injections = avro.path("enriched").path("inject").size();

            // The un-injected arm must always succeed: a schema that refuses on its own would make
            // any comparison below meaningless.
            List<String> withoutInjection = FidelityEnriched.streamNames(
                    io.github.pierce.schema.FlattenOptions.defaults(), schema);

            List<String> withInjection;
            try {
                withInjection =
                        FidelityEnriched.streamNames(FidelityEnriched.buildOptions(id, avro), schema);
            } catch (io.github.pierce.schema.SchemaFlattenException refused) {
                refusing++;
                assertThat(refused.getMessage())
                        .as("%s: the injection turned %d clean columns into a refusal, which is a "
                                + "reaction - but a blank diagnostic would leave the caller unable "
                                + "to tell which two columns collided", id, withoutInjection.size())
                        .isNotBlank();
                continue;
            }
            emitting++;

            assertThat(withInjection)
                    .as("%s: stream() must react to injectField. If these agree, the "
                            + "ENRICHED_STREAM rows pass for a reason unrelated to what they "
                            + "claim to measure, and the comparator can never report unequal "
                            + "again.", id)
                    .isNotEqualTo(withoutInjection)
                    .hasSize(withoutInjection.size() + injections);
        }

        assertThat(emitting)
                .as("every injecting fixture refused, so the size arm above never ran and this "
                        + "gate silently narrowed to 'an injection throws'")
                .isGreaterThanOrEqualTo(1);
        assertThat(refusing)
                .as("no injecting fixture reaches the output-side collision guard, so nothing in "
                        + "the corpus sees an injected column being checked at all - which is the "
                        + "gap that let injectField() bypass NameCollisionPolicy.FAIL")
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("the known-lossy headline is backed by fixtures that actually demonstrate the loss")
    void theKnownLossyHeadlineIsBackedByFixtures() {
        JsonNode known = manifest().get("knownLossy");
        long lossyRows = manifestEntries().stream()
                .filter(e -> !"LOSSLESS".equals(e.get("classification").asText())).count();
        assertThat(known != null && known.isArray() && !known.isEmpty())
                .as("the manifest must carry the up-front known-lossy list that the published "
                        + "document leads with. An empty list publishes a document implying "
                        + "nothing is lost, which is false %d times over. (Derived, not typed: "
                        + "this sentence said 103 while the corpus held 104, which is the drift "
                        + "the tally gates exist to catch.)", lossyRows)
                .isTrue();

        Set<String> citedFamilies = new TreeSet<>();
        Set<String> ids = new TreeSet<>();
        for (JsonNode k : known) {
            String kid = k.path("id").asText();
            assertThat(kid).as("every known-lossy item needs an id").isNotBlank();
            assertThat(ids.add(kid)).as("duplicate known-lossy item %s", kid).isTrue();
            assertThat(k.path("headline").asText()).as("%s needs a headline", kid).isNotBlank();
            assertThat(k.path("statement").asText()).as("%s needs a plain statement", kid).isNotBlank();
            String kind = k.path("kind").asText("LOSS");
            assertThat(kind).as("%s kind", kid).isIn("LOSS", "INERT_CONTROL");

            JsonNode cites = k.path("fixtures");
            assertThat(cites.isArray() && !cites.isEmpty())
                    .as("known-lossy item '%s' cites no fixture, so nothing in the corpus "
                            + "enforces it and it can outlive the behaviour it describes", kid)
                    .isTrue();
            for (JsonNode f : cites) {
                String fid = f.asText();
                JsonNode e = entry(fid);
                citedFamilies.add(e.get("family").asText());
                if ("INERT_CONTROL".equals(kind)) {
                    assertThat(fixture(fid).probe())
                            .as("known-lossy item '%s' is an INERT_CONTROL warning citing %s, so "
                                    + "that fixture must carry the probe that PROVES the control "
                                    + "is inert", kid, fid)
                            .isNotNull();
                    assertThat(fixture(fid).probe().path("expect").asText())
                            .as("%s cites %s as an inert control, but that fixture's probe expects "
                                    + "the two configurations to DIFFER - which would mean the "
                                    + "control works", kid, fid)
                            .isEqualTo("EQUAL");
                } else {
                    assertThat(e.get("classification").asText())
                            .as("known-lossy item '%s' cites %s, which the manifest publishes as "
                                    + "LOSSLESS. A headline warning backed by a fixture that "
                                    + "loses nothing is a false warning.", kid, fid)
                            .isIn("ACCEPTED_LOSS", "DEFECT");
                }
            }
        }

        Set<String> lossyFamilies = new TreeSet<>();
        for (JsonNode e : manifestEntries()) {
            if (!"LOSSLESS".equals(e.get("classification").asText())) {
                lossyFamilies.add(e.get("family").asText());
            }
        }
        assertThat(citedFamilies)
                .as("every family that contains a known loss must be represented in the up-front "
                        + "warning list. A family whose losses are only discoverable by reading "
                        + "every row of the published table has not been disclosed.")
                .containsAll(lossyFamilies);
    }

    /**
     * Every mechanism the harness offers is used by at least one fixture, verified by COUNT.
     *
     * <p>An assert mode, a comparator selector, a probe kind or an input hatch that no fixture
     * exercises is a control that appears present and does nothing - the failure this repository
     * has shipped a dozen times, four of them inside this very harness. The declared sets below
     * are the CONTRACT: an unused mechanism fails because the used set is smaller, and an
     * undeclared one fails because the used set is larger, so adding a mode without a fixture and
     * adding a fixture on an unlisted mode are both build failures.</p>
     */
    @Test
    @DisplayName("every harness mechanism is exercised by at least one fixture")
    void everyHarnessMechanismIsExercisedByAFixture() {
        Set<String> declaredModes = new TreeSet<>(List.of(
                "DATA", "DATUM", "KEYSET", "SCHEMA", "SCHEMA_ARG_IGNORED", "SCHEMA_CACHED",
                "ENRICHED_KEYSET", "ENRICHED_METADATA"));
        Set<String> declaredComparators = new TreeSet<>(List.of(
                "MAP_FLATTENER", "LEGACY_AVRO_SCHEMA_FLATTENER", "GAVRO_SCHEMA_FLATTENER",
                "ENRICHED_STREAM", "PROPERTY_SET", "DECLARED_DOC", "DECODED_PATH"));
        Set<String> declaredProbes = new TreeSet<>(List.of(
                "RECONSTRUCT_CONFIG_COMPARE", "FLATTEN_CONFIG_COMPARE", "ENRICHED_CONFIG_COMPARE"));
        Set<String> declaredPairKinds = new TreeSet<>(List.of("FLAT_EQUAL", "RECON_TYPE_AT_PATH"));

        Set<String> usedModes = new TreeSet<>();
        Set<String> usedComparators = new TreeSet<>();
        Set<String> usedProbes = new TreeSet<>();
        int javaInputRows = 0;
        for (JsonNode e : manifestEntries()) {
            FidelityFixture fx = fixture(e.get("id").asText());
            if ("AVRO".equals(fx.stack())) {
                usedModes.add(fx.config().path("avro").path("assert").asText("DATA"));
                String cmp = fx.config().path("avro").path("enrichedCompare").asText("");
                if (!cmp.isEmpty()) {
                    usedComparators.add(cmp);
                }
            }
            if (fx.probe() != null) {
                usedProbes.add(fx.probe().path("kind").asText());
            }
            if (fx.javaInput() != null) {
                javaInputRows++;
            }
        }
        Set<String> usedPairKinds = new TreeSet<>();
        manifestPairs().forEach(p -> usedPairKinds.add(p.get("kind").asText()));

        assertThat(usedModes).as("every Avro assert mode the runner dispatches on must have a "
                + "fixture, and no fixture may declare a mode that is not in this list")
                .isEqualTo(declaredModes);
        assertThat(usedComparators).as("every enrichedCompare selector must have a fixture")
                .isEqualTo(declaredComparators);
        assertThat(usedProbes).as("every probe kind must have a fixture")
                .isEqualTo(declaredProbes);
        assertThat(usedPairKinds).as("every pair-invariant kind must have a pair")
                .isEqualTo(declaredPairKinds);
        assertThat(javaInputRows).as("the typed javaInput hatch must be exercised; it is the only "
                + "way to reach the Java value domain and the only way to build a cycle, so a "
                + "corpus with zero javaInput rows means the hatch measures nothing")
                .isGreaterThanOrEqualTo(9);
        assertThat(declaredModes.size() + declaredComparators.size() + declaredProbes.size())
                .as("VERIFY THE COUNT of mechanisms under test, so gutting a list above is a "
                        + "failure rather than a silently smaller gate")
                .isEqualTo(18);
    }

    private static List<String> expectedFlags(String stack) {
        return switch (stack) {
            case "MAP" -> List.of("losslessMap");
            case "JSON" -> List.of("losslessJson");
            case "BOTH" -> List.of("losslessMap", "losslessJson");
            case "AVRO" -> List.of("losslessAvro");
            default -> throw new AssertionError("unknown stack: " + stack);
        };
    }

    // ------------------------------------------------------------------ probes

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("fixtureIdsWithProbes")
    @DisplayName("probe holds: a control is proven live, or proven inert")
    void theProbeHolds(String id) {
        FidelityFixture fx = fixture(id);
        FidelityRunner.Measurement m = measure(id);
        @SuppressWarnings("unchecked")
        Map<String, Object> probe = (Map<String, Object>) m.recorded.get("probe");
        assertThat(probe).as("%s declares a probe but the runner produced none", id).isNotNull();

        String expect = String.valueOf(probe.get("expect"));
        boolean equal = Boolean.TRUE.equals(probe.get("equal"));
        String note = fx.probe().path("note").asText("");
        if ("EQUAL".equals(expect)) {
            assertThat(equal).as("%s: the two configurations were expected to produce IDENTICAL "
                    + "output, which is what proves the control is inert. They now differ, so "
                    + "the control has been wired up. %s", id, note).isTrue();
        } else if ("DIFFERENT".equals(expect)) {
            assertThat(equal).as("%s: the two configurations were expected to produce DIFFERENT "
                    + "output, which is what proves the control is live. They now agree, so the "
                    + "control has become a no-op. %s", id, note).isFalse();
        } else {
            fail("%s declares an unknown probe expectation '%s'", id, expect);
        }
    }

    // ------------------------------------------------------------------ pair invariants

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("manifestPairs")
    @DisplayName("pair invariant holds")
    void thePairInvariantHolds(JsonNode pair) {
        String pid = pair.get("id").asText();
        String kind = pair.get("kind").asText();
        String detail = pair.path("detail").asText("");
        switch (kind) {
            case "FLAT_EQUAL" -> {
                String left = pair.get("left").asText();
                String right = pair.get("right").asText();
                assertThat(measure(left).recorded.get("flat"))
                        .as("%s: '%s' and '%s' must flatten to byte-identical output. That "
                                + "collision is the finding; it cannot be observed from either "
                                + "fixture alone, because each one round-trips or fails on its "
                                + "own terms. %s", pid, left, right, detail)
                        .isEqualTo(measure(right).recorded.get("flat"));
            }
            case "RECON_TYPE_AT_PATH" -> {
                String left = pair.get("left").asText();
                String path = pair.get("path").asText();
                String expected = pair.get("expectedType").asText();
                assertThat(typeAtPath(measure(left).mapDocObject, path))
                        .as("%s: the reconstructed container type at '%s' in fixture '%s'. %s",
                                pid, path, left, detail)
                        .isEqualTo(expected);
            }
            default -> fail("unknown pair kind '%s' in manifest entry %s", kind, pid);
        }
    }

    private static String typeAtPath(Object document, String dottedPath) {
        Object current = document;
        for (String segment : dottedPath.split("\\.")) {
            if (!(current instanceof Map<?, ?> map)) {
                return "ABSENT";
            }
            if (!map.containsKey(segment)) {
                return "ABSENT";
            }
            current = map.get(segment);
        }
        if (current == null) {
            return "NULL";
        }
        if (current instanceof List<?>) {
            return "LIST";
        }
        if (current instanceof Map<?, ?>) {
            return "MAP";
        }
        return "SCALAR";
    }

    // ------------------------------------------------------------------ self-check

    @Test
    @DisplayName("every fixture declares the metadata a reader needs to act on it")
    void everyFixtureIsSelfDescribing() {
        Set<String> seen = new HashSet<>();
        int checked = 0;
        for (JsonNode e : manifestEntries()) {
            String id = e.get("id").asText();
            boolean fresh = seen.add(id);
            assertThat(fresh).as("duplicate fixture id in manifest: %s", id).isTrue();
            FidelityFixture fx = fixture(id);
            assertThat(fx.id()).as("fixture file id must match its manifest entry").isEqualTo(id);
            assertThat(fx.family()).as("%s family", id).isEqualTo(e.get("family").asText());
            assertThat(fx.stack()).as("%s stack", id).isEqualTo(e.get("stack").asText());
            assertThat(fx.rationale()).as("%s must say why it exists", id).isNotBlank();
            assertThat(fx.catchesBugClass()).as("%s must say what it catches", id).isNotBlank();
            assertThat(fx.cannotCatch()).as("%s must state its limits honestly", id).isNotBlank();
            // SOURCE XOR. Exactly one of input (JSON text) and javaInput (a typed-constructor
            // spec) must carry the document. Relaxing this to isNotNull() would accept a fixture
            // with no source at all, which is the one thing this assertion exists to prevent.
            boolean hasInput = fx.input() != null && !fx.input().isBlank();
            boolean hasJava = fx.javaInput() != null;
            assertThat(hasInput ^ hasJava)
                    .as("%s must carry exactly one source document: 'input' as JSON text OR "
                            + "'javaInput' as a typed spec. Neither is a fixture that measures "
                            + "nothing; both is a field that reads like the source and is not.", id)
                    .isTrue();
            if (hasJava) {
                assertThat(fx.stack())
                        .as("%s uses javaInput, which only the MAP stack can accept - the JSON, "
                                + "BOTH and AVRO arms all need parseable source text", id)
                        .isEqualTo("MAP");
            }
            checked++;
        }
        assertThat(checked).as("fixtures checked").isEqualTo(manifestEntries().size());
    }
}
