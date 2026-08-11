package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The published stack recipes compile, are byte-identical to what the manifest publishes, and
 * produce the answers the contract records.
 *
 * <h2>The gap this closes</h2>
 *
 * <p>{@code manifest.stacks[*].code} was free text. Nothing compiled it, nothing ran it, and
 * nothing compared it to anything. One snippet could not compile at all - {@code JsonFlattener}'s
 * constructor is private and every factory returns a {@code FluentOperation}, so no consumer could
 * ever have declared the variable the recipe declared. Another compiled and was wrong: it named
 * the statically cached {@code getFlattenedSchema(Schema)}, which none of the corpus's
 * measurements had ever executed.</p>
 *
 * <h2>Why there are five groups and not one</h2>
 *
 * <p>The identity test is parameterized over {@code manifest.stacks}. Gut that block and it runs
 * zero invocations and passes - the repository's signature pathology, sitting inside the fix for
 * it. So coverage is asserted by COUNT against three independently derived sets, and invocation is
 * asserted by count too, because a recipe can be declared, marker-delimited, published,
 * byte-compared and never once called.</p>
 */
@DisplayName("The published stack recipes compile, match the manifest, and run")
class PublishedSnippetsCompileTest {

    private static final Set<String> INVOKED = ConcurrentHashMap.newKeySet();
    private static final com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>> MAPS =
            new com.fasterxml.jackson.core.type.TypeReference<>() { };

    /** Every recipe method on {@link PublishedStackRecipes}, keyed by its published stack name. */
    private static final Map<String, String> RECIPE_METHODS = Map.of(
            "MAP", "stackMap",
            "JSON", "stackJson",
            "AVRO", "stackAvroData",
            "AVRO_SCHEMA", "stackAvroSchema");

    static Stream<String> publishedStacks() {
        JsonNode stacks = FidelityCorpus.manifest().get("stacks");
        if (stacks == null || !stacks.isObject() || stacks.isEmpty()) {
            throw new AssertionError("PUBLISHED SNIPPET GATE DID NOT RUN: manifest.stacks is empty, "
                    + "so the identity check would run zero times and pass");
        }
        Set<String> names = new TreeSet<>();
        stacks.fieldNames().forEachRemaining(names::add);
        return names.stream();
    }

    // ------------------------------------------------------------------ 1. identity

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("publishedStacks")
    @DisplayName("every published snippet is byte-identical to a compiled method body")
    void publishedSnippetIsACompiledMethodBody(String stack) {
        assertThat(FidelityCorpus.manifest().get("stacks").get(stack).path("code").asText())
                .as("manifest.stacks.%s.code is not the text between the SNIPPET markers in %s. "
                        + "A published recipe that nothing compiles is a recipe that can be wrong "
                        + "forever; the manifest string must be a copy of a region javac accepted.",
                        stack, FidelitySnippetSource.RECIPES)
                .isEqualTo(FidelitySnippetSource.extract(stack));
    }

    // ------------------------------------------------------------------ 2. coverage, by count

    @Test
    @DisplayName("markers, published stacks and compiled recipe methods are the same non-empty set")
    void everyStackHasAMarkerAndAMethod() {
        Set<String> markers = FidelitySnippetSource.markers();
        Set<String> published = new TreeSet<>();
        FidelityCorpus.manifest().get("stacks").fieldNames().forEachRemaining(published::add);
        Set<String> methods = new TreeSet<>();
        for (Method method : PublishedStackRecipes.class.getDeclaredMethods()) {
            if (Modifier.isStatic(method.getModifiers()) && !Modifier.isPrivate(method.getModifiers())) {
                for (Map.Entry<String, String> e : RECIPE_METHODS.entrySet()) {
                    if (e.getValue().equals(method.getName())) {
                        methods.add(e.getKey());
                    }
                }
            }
        }
        assertThat(markers).as("snippet markers must not be empty; the identity test draws its "
                + "parameters from the manifest and would pass by running zero times").isNotEmpty();
        assertThat(markers).as("the set of SNIPPET markers must equal the set of published stacks")
                .isEqualTo(published);
        assertThat(methods).as("every published stack must have a compiled recipe method behind it")
                .isEqualTo(published);
        assertThat(markers).as("VERIFY THE COUNT: four measured paths, four recipes - MAP, JSON, "
                + "AVRO data and AVRO schema inverse. The manifest used to publish three for four.")
                .hasSize(4);
    }

    // ------------------------------------------------------------------ 3. execution

    @Test
    @DisplayName("Stack A executed reproduces the recorded reconstruction")
    void stackMapMatchesRecordedMapDoc() throws Exception {
        FidelityFixture fx = FidelityCorpus.fixture("object-chain-depth-8");
        Map<String, Object> src = FidelityRunner.LENIENT.readValue(fx.input(), MAPS);
        INVOKED.add("MAP");
        assertThat(FidelityRender.text(FidelityRender.java(PublishedStackRecipes.stackMap(src))))
                .as("the published Stack A recipe no longer reproduces what the contract records "
                        + "for object-chain-depth-8")
                .isEqualTo(fx.expected().get("mapDoc").asText());
    }

    @Test
    @DisplayName("Stack B executed reproduces the exact money loss the contract records")
    void stackJsonMatchesRecordedJsonDoc() throws Exception {
        // Pinned against a DEFECT row on purpose: a DEFECT recording is an exact WRONG value, so
        // the assertion has something specific to be wrong about. A LOSSLESS recording can be
        // reproduced by accident. Read back with EXACT - a default mapper collapses both sides to
        // the same Double and the money loss disappears.
        FidelityFixture fx = FidelityCorpus.fixture("vd-decimal-scale-and-precision");
        INVOKED.add("JSON");
        String back = PublishedStackRecipes.stackJson(fx.input());
        assertThat(FidelityRender.text(FidelityRender.json(FidelityRunner.EXACT.readTree(back))))
                .as("the published Stack B recipe no longer reproduces what the contract records "
                        + "for vd-decimal-scale-and-precision. This is the snippet that could not "
                        + "even compile before, so it is the one most likely to reach a different "
                        + "entry point than the corpus measures.")
                .isEqualTo(fx.expected().get("jsonDoc").asText());
    }

    @Test
    @DisplayName("Stack C data executed reproduces the recorded Avro reconstruction")
    void stackAvroDataMatchesRecordedAvroDoc() throws Exception {
        FidelityFixture fx = FidelityCorpus.fixture("avro-nullable-scalars-and-logical-types-control");
        Schema schema = new Schema.Parser()
                .parse(fx.config().path("avro").path("avsc").toString());
        Map<String, Object> datum = FidelityRunner.LENIENT.readValue(fx.input(), MAPS);
        INVOKED.add("AVRO");
        assertThat(FidelityRender.text(FidelityRender.java(
                PublishedStackRecipes.stackAvroData(schema, datum))))
                .as("the published Stack C data recipe no longer reproduces the recorded Avro "
                        + "reconstruction")
                .isEqualTo(fx.expected().get("avroDoc").asText());
    }

    @Test
    @DisplayName("Stack C schema executed reproduces the recorded schema inverse")
    void stackAvroSchemaMatchesRecordedInverse() {
        // The inverse REPLAYS definitions captured during the forward pass rather than reading the
        // flattened schema. This assertion pins its output; it is not evidence that the flattened
        // schema retained the information - avro-reconstruct-original-schema-ignores-argument owns
        // that finding and this row must not be cited as independent confirmation of it.
        FidelityFixture fx = FidelityCorpus.fixture("avro-flattened-name-collision-guard-fires");
        Schema schema = new Schema.Parser()
                .parse(fx.config().path("avro").path("avsc").toString());
        INVOKED.add("AVRO_SCHEMA");
        String measured;
        try {
            measured = PublishedStackRecipes.stackAvroSchema(schema).toString();
        } catch (Throwable t) {
            measured = FidelityRender.thrown(t);
        }
        assertThat(measured)
                .as("the newly published Stack C schema recipe no longer reproduces the recorded "
                        + "schema inverse")
                .isEqualTo(fx.expected().get("avroDoc").asText());
    }

    // ------------------------------------------------------------------ 4. invocation coverage

    @AfterAll
    static void everyRecipeMethodWasActuallyInvoked() {
        assertThat(INVOKED).as("NO RECIPE WAS EXECUTED AT ALL. Compilation and text identity are "
                + "the cheap half of this gate; a recipe that is compile-checked, marker-delimited, "
                + "published and never called is the half-built version of this very fix.")
                .isNotEmpty();
        assertThat(new TreeSet<>(INVOKED))
                .as("every compiled recipe must be executed by at least one test")
                .isEqualTo(new TreeSet<>(RECIPE_METHODS.keySet()));
    }

    // ------------------------------------------------------------------ 5. the three-way drill

    @Test
    @DisplayName("extraction refuses a missing file, a missing marker and a blank region")
    void theExtractorRefusesEveryDegenerateInput() throws Exception {
        Path missing = FidelityCorpus.moduleRoot().resolve("target/no-such-recipes.java");
        assertThatThrownBy(() -> FidelitySnippetSource.extract(missing, "MAP"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("does not");

        Path real = FidelitySnippetSource.recipesFile();
        assertThatThrownBy(() -> FidelitySnippetSource.extract(real, "NO_SUCH_STACK"))
                .as("a renamed marker must fail loudly; returning \"\" would disarm every identity "
                        + "assertion at once while leaving the test names green")
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("NO_SUCH_STACK");

        Path blank = FidelityCorpus.moduleRoot().resolve("target/blank-recipes.java");
        java.nio.file.Files.createDirectories(blank.getParent());
        java.nio.file.Files.writeString(blank,
                "// SNIPPET-BEGIN MAP\n// SNIPPET-END MAP\n", java.nio.charset.StandardCharsets.UTF_8);
        assertThatThrownBy(() -> FidelitySnippetSource.extract(blank, "MAP"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("blank");

        Path swapped = FidelityCorpus.moduleRoot().resolve("target/swapped-recipes.java");
        java.nio.file.Files.writeString(swapped,
                "// SNIPPET-END MAP\nx\ny\n// SNIPPET-BEGIN MAP\n", java.nio.charset.StandardCharsets.UTF_8);
        assertThatThrownBy(() -> FidelitySnippetSource.extract(swapped, "MAP"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("out of order");

        String published = FidelityCorpus.manifest().get("stacks").get("MAP").path("code").asText();
        assertThat(published.replace("MapFlattener", "MapFlattenerX"))
                .as("a one-character manifest edit must break identity")
                .isNotEqualTo(FidelitySnippetSource.extract("MAP"));
    }

    // ------------------------------------------------------------------ per-row recipe verdict

    static Stream<String> allFixtureIds() {
        return FidelityCorpus.manifestFixtureIds();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("allFixtureIds")
    @DisplayName("the manifest's published-recipe verdict is true for every row")
    void thePublishedRecipeVerdictHolds(String id) {
        JsonNode e = FidelityCorpus.entry(id);
        assertThat(FidelityRecipe.verdict(FidelityCorpus.fixture(id)))
                .as("%s publishes holdsUnderPublishedRecipe=%s, but running the recipe on this "
                        + "row's own source document and comparing to its recorded reconstruction "
                        + "measured the opposite. A consumer following the published page verbatim "
                        + "does not get the answer this row states. Manifest says: %s",
                        id, e.path("holdsUnderPublishedRecipe").asText(), e.get("detail").asText())
                .isEqualTo(e.path("holdsUnderPublishedRecipe").asText());
    }

    /** Test-visible so a drill can assert the invocation set is a real set and not a constant. */
    static Set<String> invoked() {
        return new LinkedHashSet<>(INVOKED);
    }
}
