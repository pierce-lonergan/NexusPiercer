package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The drift guard on {@code docs/ROUND_TRIP_FIDELITY.md}.
 *
 * <p>That document is the artifact a consumer reads before depending on this library, and it
 * restates every row of a contract that lives somewhere else. A hand-maintained copy of a contract
 * is a copy that goes stale, and a stale fidelity guarantee is worse than none: it is a promise
 * the corpus has stopped making. So the document is generated from {@code manifest.json} and this
 * class asserts that the committed bytes are exactly what the generator produces from the manifest
 * as it stands right now.</p>
 *
 * <p>The gate is drilled here rather than only in a reviewer's shell, because a comparison that
 * can only ever succeed proves nothing:</p>
 * <ol>
 *   <li><b>good input passes</b> - the committed document matches the current manifest;</li>
 *   <li><b>a synthetic violation blocks</b> - mutating a classification, a detail, a count and a
 *       known-lossy headline each produce a document that no longer matches the committed one, so
 *       an un-regenerated manifest edit cannot slip through;</li>
 *   <li><b>missing or empty input blocks</b> - an absent document, an empty document, and a
 *       manifest with its blocks removed all fail loudly rather than producing an empty pass.</li>
 * </ol>
 */
@DisplayName("The published fidelity document cannot drift from the manifest")
class RoundTripFidelityDocTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * Delegated to {@link FidelityCorpus} rather than duplicated. A second definition of "where is
     * the repository" is a second thing that can drift, and the published-snippet gate reads a
     * source file relative to the same root.
     */
    private static Path moduleRoot() {
        return FidelityCorpus.moduleRoot();
    }

    private static Path docFile() {
        return moduleRoot().resolve(FidelityDocGenerator.DOC_PATH);
    }

    private static JsonNode manifest() throws IOException {
        Path f = moduleRoot().resolve(FidelityDocGenerator.MANIFEST_PATH);
        assertThat(Files.isRegularFile(f)).as("manifest missing at " + f).isTrue();
        return JSON.readTree(Files.readString(f, StandardCharsets.UTF_8));
    }

    /** Git may check the document out with CRLF; the comparison is about content, not bytes. */
    private static String normalise(String s) {
        return s.replace("\r\n", "\n");
    }

    private static String committed() throws IOException {
        Path doc = docFile();
        assertThat(Files.isRegularFile(doc))
                .as("THE PUBLISHED GUARANTEE IS MISSING: " + doc + " does not exist. The corpus "
                        + "may be green and the document a consumer actually reads still absent.")
                .isTrue();
        String text = normalise(Files.readString(doc, StandardCharsets.UTF_8));
        assertThat(text.isBlank())
                .as("THE PUBLISHED GUARANTEE IS EMPTY: " + doc + " is blank. An empty contract is "
                        + "not a satisfied contract.")
                .isFalse();
        return text;
    }

    // ------------------------------------------------------------------ 1. good input passes

    @Test
    @DisplayName("the committed document is exactly what the generator renders from the manifest")
    void committedDocumentMatchesTheManifest() throws IOException {
        assertThat(committed())
                .as("docs/ROUND_TRIP_FIDELITY.md no longer matches manifest.json. The manifest "
                        + "changed and the document was not regenerated, or the document was "
                        + "hand-edited. Either way a consumer is now reading a guarantee the "
                        + "corpus does not make. Regenerate with FidelityDocGenerator - do not "
                        + "edit the markdown.")
                .isEqualTo(FidelityDocGenerator.render(manifest()));
    }

    @Test
    @DisplayName("the document actually carries every fixture, not a truncated sample")
    void theDocumentCarriesEveryFixture() throws IOException {
        JsonNode m = manifest();
        String doc = committed();
        int checked = 0;
        for (JsonNode e : m.get("fixtures")) {
            assertThat(doc).as("fixture %s is missing from the published table",
                    e.get("id").asText()).contains("`" + e.get("id").asText() + "`");
            checked++;
        }
        assertThat(checked).as("fixtures checked against the document")
                .isEqualTo(m.path("counts").path("total").asInt());
        assertThat(checked).as("a document generated from zero fixtures would trivially pass "
                + "every containment check above").isGreaterThan(0);

        for (JsonNode k : m.get("knownLossy")) {
            assertThat(doc).as("known-lossy warning '%s' is missing from the published document",
                    k.path("id").asText()).contains(k.path("headline").asText());
        }
    }

    // ------------------------------------------------------------------ 2. violations block

    @Test
    @DisplayName("an un-regenerated manifest edit is caught, whichever field was edited")
    void mutatingTheManifestBreaksTheMatch() throws IOException {
        String committed = committed();

        ObjectNode reclassified = (ObjectNode) manifest();
        ((ObjectNode) reclassified.get("fixtures").get(0)).put("classification", "DEFECT");
        assertThat(FidelityDocGenerator.render(reclassified))
                .as("reclassifying a fixture must change the published document, otherwise the "
                        + "document is not really derived from the classification column")
                .isNotEqualTo(committed);

        ObjectNode redetailed = (ObjectNode) manifest();
        ((ObjectNode) redetailed.get("fixtures").get(0))
                .put("detail", "SYNTHETIC VIOLATION: this loss statement was never measured.");
        assertThat(FidelityDocGenerator.render(redetailed))
                .as("changing what a fixture claims to lose must change the document")
                .isNotEqualTo(committed);

        ObjectNode recounted = (ObjectNode) manifest();
        ((ObjectNode) recounted.get("counts")).put("defect", 999);
        assertThat(FidelityDocGenerator.render(recounted))
                .as("changing a headline count must change the document")
                .isNotEqualTo(committed);

        ObjectNode rewarned = (ObjectNode) manifest();
        ((ObjectNode) rewarned.get("knownLossy").get(0))
                .put("headline", "SYNTHETIC VIOLATION: nothing is ever lost.");
        assertThat(FidelityDocGenerator.render(rewarned))
                .as("changing the up-front warning list must change the document - this is the "
                        + "part of the page a consumer is most likely to read and least likely "
                        + "to re-derive")
                .isNotEqualTo(committed);

        ObjectNode dropped = (ObjectNode) manifest();
        ((ArrayNode) dropped.get("fixtures")).remove(0);
        assertThat(FidelityDocGenerator.render(dropped))
                .as("deleting a fixture must change the document; a corpus that silently shrank "
                        + "is the failure mode this repository keeps shipping")
                .isNotEqualTo(committed);
    }

    // ------------------------------------------------------------------ 3. missing/empty blocks

    @Test
    @DisplayName("a gutted manifest refuses to render rather than publishing a plausible page")
    void agGuttedManifestRefusesToRender() throws IOException {
        assertThatThrownBy(() -> FidelityDocGenerator.render(JSON.createObjectNode()))
                .as("an empty manifest must not render")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CANNOT GENERATE");

        assertThatThrownBy(() -> FidelityDocGenerator.render(null))
                .isInstanceOf(IllegalArgumentException.class);

        ObjectNode noFixtures = (ObjectNode) manifest();
        noFixtures.set("fixtures", JSON.createArrayNode());
        assertThatThrownBy(() -> FidelityDocGenerator.render(noFixtures))
                .as("zero fixtures is a failure, never a page that says everything is fine")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no fixtures");

        ObjectNode noWarnings = (ObjectNode) manifest();
        noWarnings.set("knownLossy", JSON.createArrayNode());
        assertThatThrownBy(() -> FidelityDocGenerator.render(noWarnings))
                .as("a fidelity page whose warning list is empty is the single most dangerous "
                        + "document this repository could publish")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("knownLossy");

        for (String block : new String[] {"counts", "pairs", "stacks", "repairPolicy",
                "classifications", "classificationOverrides"}) {
            ObjectNode gutted = (ObjectNode) manifest();
            gutted.remove(block);
            assertThatThrownBy(() -> FidelityDocGenerator.render(gutted))
                    .as("removing the '%s' block must stop the render, not silently omit a "
                            + "section", block)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(block);
        }
    }
}
