package io.github.pierce.fidelity;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.pierce.AvroSchemaFlattener;
import org.apache.avro.Schema;

import java.util.Map;

/**
 * Runs the PUBLISHED recipe for a row's stack against that row's own source document and folds the
 * comparison into the tri-state the manifest publishes as {@code holdsUnderPublishedRecipe}.
 *
 * <h2>Why this is a stronger claim than {@code holdsUnderDefaultReconstruction}</h2>
 *
 * <p>The defaults arm re-reconstructs a map the FIXTURE'S flattener already produced, so it is
 * structurally blind to any divergence the flattener creates. A row measured under
 * {@code maxDepth(2)} reconstructs identically through the default reconstructor - defaults say
 * YES - and is still not reproducible by anyone who follows the published page verbatim, because
 * the page's flattener is at its own default depth. That gap is what this measures.</p>
 *
 * <p>Shared by {@code PublishedSnippetsCompileTest}, which asserts the published value, and by
 * {@code FidelityCorpusRecorder}, which prints it so the column is DERIVED rather than guessed.
 * One implementation, so the tool that tells you what to publish and the gate that enforces it
 * cannot disagree.</p>
 */
final class FidelityRecipe {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() { };

    private FidelityRecipe() {
    }

    /**
     * MEASURED, NOT ASSUMED: the first version of this method ran the recipe OUTSIDE the fixture's
     * declared environment pin, and {@code vd-java-date-is-not-a-function-of-its-input} then
     * answered NO under a bare JVM and YES under surefire (which the pom starts with
     * {@code -Duser.timezone=UTC}). That is precisely the machine-dependence the pin exists to
     * remove, reintroduced by the gate that checks the pinned row. Every measurement of a pinned
     * fixture has to run inside the pin, not just the primary one.
     */
    static String verdict(FidelityFixture fx) {
        try (FidelityJavaInput.Env env = FidelityJavaInput.environment(fx.javaInput())) {
            assert env != null;
            return pinnedVerdict(fx);
        }
    }

    private static String pinnedVerdict(FidelityFixture fx) {
        JsonNode recorded = fx.expected();
        try {
            if ("AVRO".equals(fx.stack()) && !isDataMode(fx)) {
                // KEYSET / SCHEMA / SCHEMA_ARG_IGNORED / SCHEMA_CACHED / ENRICHED_* answer a schema
                // question and reconstruct no data. DATUM is NOT_APPLICABLE too, and for a sharper
                // reason: no published recipe calls reconstruct(Map,Schema) at all, and because the
                // oracle renders a GenericRecord and a LinkedHashMap identically, running the
                // reconstructToMap recipe against a DATUM row's recording would report YES and
                // read as "the recipe reproduces this row" when the recipe never goes near the
                // entry point the row is about.
                return FidelityRunner.DEFAULTS_NA;
            }
            boolean ok = true;
            if ("MAP".equals(fx.stack()) || "BOTH".equals(fx.stack())) {
                Object src = fx.javaInput() != null
                        ? FidelityJavaInput.build(fx.javaInput(), fx.id())
                        : FidelityRunner.LENIENT.readValue(fx.input(), MAP_TYPE);
                ok &= mapArm(src, recorded);
            }
            if ("JSON".equals(fx.stack()) || "BOTH".equals(fx.stack())) {
                String back = PublishedStackRecipes.stackJson(fx.input());
                ok &= FidelityRender.text(FidelityRender.json(FidelityRunner.EXACT.readTree(back)))
                        .equals(recorded.path("jsonDoc").asText());
            }
            if ("AVRO".equals(fx.stack())) {
                Schema schema = new Schema.Parser()
                        .parse(fx.config().path("avro").path("avsc").toString());
                Map<String, Object> datum = FidelityRunner.LENIENT.readValue(fx.input(), MAP_TYPE);
                ok &= FidelityRender.text(FidelityRender.java(
                                PublishedStackRecipes.stackAvroData(schema, datum)))
                        .equals(recorded.path("avroDoc").asText());
            }
            return ok ? FidelityRunner.DEFAULTS_HOLD : FidelityRunner.DEFAULTS_DIVERGE;
        } catch (Throwable t) {
            // A recipe that throws reproduces the row only if the row itself recorded that throw.
            String thrown = FidelityRender.thrown(t);
            boolean matches = thrown.equals(recorded.path("mapDoc").asText(""))
                    || thrown.equals(recorded.path("avroDoc").asText(""))
                    || thrown.equals(recorded.path("jsonDoc").asText(""));
            return matches ? FidelityRunner.DEFAULTS_HOLD : FidelityRunner.DEFAULTS_DIVERGE;
        } finally {
            AvroSchemaFlattener.clearCache();
        }
    }

    private static boolean isDataMode(FidelityFixture fx) {
        return "DATA".equals(fx.config().path("avro").path("assert").asText("DATA"));
    }

    @SuppressWarnings("unchecked")
    private static boolean mapArm(Object src, JsonNode recorded) {
        return FidelityRender.text(FidelityRender.java(
                        PublishedStackRecipes.stackMap((Map<String, Object>) src)))
                .equals(recorded.path("mapDoc").asText());
    }
}
