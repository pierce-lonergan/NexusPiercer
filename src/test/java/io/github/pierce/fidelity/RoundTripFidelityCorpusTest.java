package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The round-trip fidelity guarantee, enforced.
 *
 * <h2>What this test is for</h2>
 *
 * <p>{@code src/test/resources/fidelity/manifest.json} is a contract published to consumers: for
 * each of 108 documents it states exactly what survives a flatten/reconstruct round trip and what
 * does not. This class is the mechanism that stops the repository changing any of it quietly.</p>
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

    private static final String MANIFEST_RESOURCE = "/fidelity/manifest.json";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Map<String, FidelityRunner.Measurement> MEASUREMENTS = new ConcurrentHashMap<>();
    private static final Map<String, FidelityFixture> FIXTURES = new ConcurrentHashMap<>();

    /** The three per-stack verdict keys. A fixture must record at least one of them. */
    private static final String[] STACK_FLAGS = {"losslessMap", "losslessJson", "losslessAvro"};

    // ------------------------------------------------------------------ loading (fails loudly)

    private static Path corpusRoot() {
        URL url = RoundTripFidelityCorpusTest.class.getResource(MANIFEST_RESOURCE);
        if (url == null) {
            throw new AssertionError("FIDELITY CORPUS DID NOT RUN: " + MANIFEST_RESOURCE
                    + " is not on the test classpath. The guarantee is unverified - this is a "
                    + "failure, not an empty pass.");
        }
        try {
            return Path.of(url.toURI()).getParent();
        } catch (URISyntaxException e) {
            throw new AssertionError("FIDELITY CORPUS DID NOT RUN: manifest URL is not a file path: " + url, e);
        }
    }

    private static final Map<String, JsonNode> MANIFEST_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, JsonNode> ENTRY_CACHE = new ConcurrentHashMap<>();

    /**
     * Parses once and caches, but ONLY on success: every failure path re-reads and re-throws, so
     * a broken manifest can never be cached into silence.
     */
    private static JsonNode manifest() {
        JsonNode cached = MANIFEST_CACHE.get("m");
        if (cached != null) {
            return cached;
        }
        JsonNode parsed = readManifest();
        MANIFEST_CACHE.put("m", parsed);
        return parsed;
    }

    private static JsonNode readManifest() {
        Path file = corpusRoot().resolve("manifest.json");
        String text;
        try {
            text = Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AssertionError("FIDELITY CORPUS DID NOT RUN: manifest is unreadable at " + file, e);
        }
        if (text.isBlank()) {
            throw new AssertionError("FIDELITY CORPUS DID NOT RUN: manifest at " + file
                    + " is empty. An empty contract is not a satisfied contract.");
        }
        JsonNode node;
        try {
            node = JSON.readTree(text);
        } catch (IOException e) {
            throw new AssertionError("FIDELITY CORPUS DID NOT RUN: manifest at " + file
                    + " is not parseable JSON", e);
        }
        JsonNode fixtures = node.get("fixtures");
        if (fixtures == null || !fixtures.isArray() || fixtures.isEmpty()) {
            throw new AssertionError("FIDELITY CORPUS DID NOT RUN: manifest at " + file
                    + " declares no fixtures. Zero fixtures is a failure, never a pass.");
        }
        return node;
    }

    private static List<JsonNode> manifestEntries() {
        List<JsonNode> out = new ArrayList<>();
        manifest().get("fixtures").forEach(out::add);
        return out;
    }

    static Stream<String> manifestFixtureIds() {
        return manifestEntries().stream().map(e -> e.get("id").asText());
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
        return ENTRY_CACHE.computeIfAbsent(id, k -> {
            for (JsonNode e : manifestEntries()) {
                if (k.equals(e.get("id").asText())) {
                    return e;
                }
            }
            throw new AssertionError("no manifest entry for fixture id " + k);
        });
    }

    private static FidelityFixture fixture(String id) {
        return FIXTURES.computeIfAbsent(id, k -> {
            JsonNode e = entry(k);
            Path file = corpusRoot().resolve(e.get("file").asText());
            if (!Files.isRegularFile(file)) {
                throw new AssertionError("MANIFEST REFERENCES A MISSING FIXTURE: " + e.get("file").asText()
                        + " is declared in the manifest but no such file exists under "
                        + corpusRoot() + ". The guarantee for '" + k + "' is unverifiable.");
            }
            try {
                return FidelityFixture.from(JSON.readTree(Files.readString(file, StandardCharsets.UTF_8)));
            } catch (IOException ex) {
                throw new AssertionError("fixture file is unreadable or unparseable: " + file, ex);
            }
        });
    }

    private static FidelityRunner.Measurement measure(String id) {
        return MEASUREMENTS.computeIfAbsent(id, k -> FidelityRunner.run(fixture(k)));
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

        int compared = 0;
        Iterator<String> names = recorded.fieldNames();
        while (names.hasNext()) {
            String key = names.next();
            if ("flat".equals(key)) {
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
            assertThat(fx.input()).as("%s must carry its input document", id).isNotBlank();
            checked++;
        }
        assertThat(checked).as("fixtures checked").isEqualTo(manifestEntries().size());
    }
}
