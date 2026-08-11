package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pierce.schema.FlattenOptions;
import io.github.pierce.schema.NameCollisionPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The corpus's hatch onto {@link FlattenOptions}'s named factories, drilled three ways.
 *
 * <h2>Why this exists</h2>
 *
 * <p>{@link FidelityEnriched#buildOptions} builds options knob by knob and, until this change,
 * had no way to reach a NAMED factory at all. Two fixtures carried {@code gAvroParity} in their
 * titles while measuring {@code FlattenOptions.builder().build()}, so the factory whose name makes
 * the claim was unobservable from the corpus and its claim could rot unnoticed - which it did.</p>
 *
 * <p>Spelling the parity knobs out by hand in a fixture would not fix that: it would pin a
 * hand-written configuration rather than the factory, which is the same pathology in a third
 * costume. Hence a {@code preset} hatch, and hence this class, because a hatch that silently falls
 * through to the defaults on a typo would let a fixture declare a preset, measure something else
 * entirely, and still pass.</p>
 */
@DisplayName("FidelityEnriched.buildOptions: the preset hatch")
class FidelityEnrichedOptionsTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static JsonNode avro(String enrichedJson) throws IOException {
        return JSON.readTree("{\"enriched\":" + enrichedJson + "}");
    }

    // ------------------------------------------------------------------ 1. good input passes

    @Test
    @DisplayName("preset GAVRO_PARITY returns exactly what FlattenOptions.gAvroParity() returns")
    void gAvroParityPresetIsTheFactory() throws IOException {
        FlattenOptions built = FidelityEnriched.buildOptions("fx", avro("{\"preset\":\"GAVRO_PARITY\"}"));

        assertThat(FidelityEnriched.renderOptions(built))
                .as("the hatch must reach the FACTORY; a hand-spelled equivalent would pin a "
                        + "configuration rather than the claim the factory's name makes")
                .isEqualTo(FidelityEnriched.renderOptions(FlattenOptions.gAvroParity()));
    }

    @Test
    @DisplayName("preset DEFAULTS returns the library defaults, and an absent block does too")
    void defaultsPresetAndAbsentBlockAgree() throws IOException {
        String expected = FidelityEnriched.renderOptions(FlattenOptions.defaults());

        assertThat(FidelityEnriched.renderOptions(
                FidelityEnriched.buildOptions("fx", avro("{\"preset\":\"DEFAULTS\"}"))))
                .isEqualTo(expected);
        assertThat(FidelityEnriched.renderOptions(
                FidelityEnriched.buildOptions("fx", avro("{}"))))
                .isEqualTo(expected);
        assertThat(FidelityEnriched.renderOptions(
                FidelityEnriched.buildOptions("fx", JSON.createObjectNode())))
                .isEqualTo(expected);
    }

    @Test
    @DisplayName("the two presets are genuinely different, so the selector is not a constant")
    void thePresetsDiffer() throws IOException {
        assertThat(FidelityEnriched.renderOptions(
                FidelityEnriched.buildOptions("fx", avro("{\"preset\":\"GAVRO_PARITY\"}"))))
                .as("if DEFAULTS and GAVRO_PARITY rendered alike, a fixture declaring either would "
                        + "measure the same thing and the hatch would prove nothing")
                .isNotEqualTo(FidelityEnriched.renderOptions(
                        FidelityEnriched.buildOptions("fx", avro("{\"preset\":\"DEFAULTS\"}"))));

        assertThat(FidelityEnriched.buildOptions("fx", avro("{\"preset\":\"GAVRO_PARITY\"}"))
                .collisionPolicy()).isEqualTo(NameCollisionPolicy.ESCAPE);
    }

    @Test
    @DisplayName("explicit knobs still work when no preset is named")
    void explicitKnobsStillWork() throws IOException {
        FlattenOptions o = FidelityEnriched.buildOptions("fx",
                avro("{\"collisionPolicy\":\"ESCAPE\",\"separator\":\".\"}"));
        assertThat(o.collisionPolicy()).isEqualTo(NameCollisionPolicy.ESCAPE);
        assertThat(o.separator()).isEqualTo(".");
    }

    // ------------------------------------------------------------------ 2. violations block

    @Test
    @DisplayName("an unknown preset name is refused, naming the fixture, not silently ignored")
    void unknownPresetIsRefused() throws IOException {
        assertThatThrownBy(() ->
                FidelityEnriched.buildOptions("some-fixture", avro("{\"preset\":\"NOT_A_PRESET\"}")))
                .as("a typo must not fall through to the defaults: the fixture would then declare "
                        + "one configuration, measure another, and still pass")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("some-fixture")
                .hasMessageContaining("NOT_A_PRESET");
    }

    @Test
    @DisplayName("a preset combined with explicit knobs is refused rather than silently merged")
    void presetPlusKnobsIsRefused() throws IOException {
        assertThatThrownBy(() -> FidelityEnriched.buildOptions("some-fixture",
                avro("{\"preset\":\"GAVRO_PARITY\",\"separator\":\".\"}")))
                .as("a preset that a neighbouring key can partially override is a preset whose "
                        + "recorded name no longer describes what was measured")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("some-fixture")
                .hasMessageContaining("preset");
    }

    // ------------------------------------------------------------------ 3. missing/empty blocks

    @Test
    @DisplayName("an empty or blank preset name is refused, not treated as absent")
    void emptyPresetIsRefused() throws IOException {
        assertThatThrownBy(() -> FidelityEnriched.buildOptions("fx", avro("{\"preset\":\"\"}")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("preset");
        assertThatThrownBy(() -> FidelityEnriched.buildOptions("fx", avro("{\"preset\":\"   \"}")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("preset");
    }
}
