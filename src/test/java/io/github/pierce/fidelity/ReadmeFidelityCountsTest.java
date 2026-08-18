package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The drift guard on the FRONT PAGE, which is the one that was missing.
 *
 * <p>ADVERSARIAL REVIEW, and the point is sharper than "another doc test". A mechanism already
 * existed to catch fidelity-count drift - {@link RoundTripFidelityDocTest} - but it covers
 * {@code docs/ROUND_TRIP_FIDELITY.md}, which is GENERATED from the manifest and therefore the one
 * file that could never have gone stale by hand. {@code README.md} hand-carries the same four
 * numbers, is the first thing a prospective consumer reads, and had no gate at all. Measured:
 * {@code grep -rn README src/test/ .github/} matched only an issue template. Had the previous pass
 * moved the corpus and forgotten the README, the build would have stayed green.</p>
 *
 * <p>The numbers are taken from {@code manifest.json}. The manifest is the contract; the README is
 * a restatement of it, and a restatement that disagrees with its source is worse than no
 * restatement, because it is read as a measurement.</p>
 */
@DisplayName("README.md's fidelity counts cannot drift from the manifest")
class ReadmeFidelityCountsTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static Path readmeFile() {
        return FidelityCorpus.moduleRoot().resolve("README.md");
    }

    private static JsonNode counts() throws IOException {
        Path f = FidelityCorpus.moduleRoot().resolve(FidelityDocGenerator.MANIFEST_PATH);
        assertThat(Files.isRegularFile(f)).as("manifest missing at " + f).isTrue();
        return JSON.readTree(Files.readString(f, StandardCharsets.UTF_8)).path("counts");
    }

    private static String readme() throws IOException {
        Path f = readmeFile();
        assertThat(Files.isRegularFile(f)).as("README.md missing at " + f).isTrue();
        String text = Files.readString(f, StandardCharsets.UTF_8).replace("\r\n", "\n");
        assertThat(text.isBlank()).as("README.md is blank").isFalse();
        return text;
    }

    /**
     * The count the README publishes for one classification row of its table.
     *
     * <p>Anchored on the backticked classification name followed by a pipe, which is the table
     * row and not the prose that discusses it, so a sentence mentioning {@code DEFECT} elsewhere
     * cannot satisfy the assertion. Returns -1 when the row is absent, so a DELETED row fails
     * loudly instead of quietly matching nothing.</p>
     */
    private static int publishedRow(String text, String classification) {
        Matcher m = Pattern.compile("\\| `" + classification + "` \\| (\\d+) \\|").matcher(text);
        return m.find() ? Integer.parseInt(m.group(1)) : -1;
    }

    private static int publishedTotal(String text) {
        Matcher m = Pattern.compile("corpus of \\*\\*(\\d+) fixtures\\*\\*").matcher(text);
        return m.find() ? Integer.parseInt(m.group(1)) : -1;
    }

    @Test
    @DisplayName("the front-page classification table matches manifest counts exactly")
    void frontPageClassificationTableMatchesTheManifest() throws IOException {
        JsonNode counts = counts();
        String text = readme();
        String source = " Take the number from src/test/resources/fidelity/manifest.json, which "
                + "is the contract; the README only restates it.";

        assertThat(publishedTotal(text))
                .as("README.md's \"corpus of **N fixtures**\" disagrees with counts.total, or the "
                        + "phrase was reworded so this gate stopped measuring anything." + source)
                .isEqualTo(counts.path("total").asInt());

        assertThat(publishedRow(text, "LOSSLESS"))
                .as("README.md's LOSSLESS row disagrees with counts.lossless." + source)
                .isEqualTo(counts.path("lossless").asInt());

        assertThat(publishedRow(text, "ACCEPTED_LOSS"))
                .as("README.md's ACCEPTED_LOSS row disagrees with counts.acceptedLoss." + source)
                .isEqualTo(counts.path("acceptedLoss").asInt());

        assertThat(publishedRow(text, "DEFECT"))
                .as("README.md's DEFECT row disagrees with counts.defect." + source)
                .isEqualTo(counts.path("defect").asInt());
    }

    @Test
    @DisplayName("the three published rows add up to the published total")
    void theThreePublishedRowsAddUpToThePublishedTotal() throws IOException {
        String text = readme();
        int sum = publishedRow(text, "LOSSLESS")
                + publishedRow(text, "ACCEPTED_LOSS")
                + publishedRow(text, "DEFECT");
        assertThat(sum)
                .as("the README's own three rows do not sum to its own total, which is what a "
                        + "half-finished hand edit looks like - one row updated, the headline "
                        + "left behind")
                .isEqualTo(publishedTotal(text));
    }

    @Test
    @DisplayName("DRILL: the anchors really do bind, so a wrong number cannot pass")
    void theAnchorsReallyBindSoAWrongNumberCannotPass() throws IOException {
        // A gate that reads its numbers out of prose is worth exactly as much as its regexes, and
        // a regex that silently matches nothing turns every assertion above into "-1 == -1" only
        // if the manifest also said -1 - which it never will, so a broken anchor fails. This
        // proves the other direction: that the anchors are not matching some unrelated digit.
        String text = readme();
        assertThat(publishedTotal(text)).as("the total anchor found nothing").isNotEqualTo(-1);
        assertThat(publishedRow(text, "LOSSLESS")).as("the LOSSLESS anchor found nothing")
                .isNotEqualTo(-1);
        assertThat(publishedRow(text, "ACCEPTED_LOSS")).as("the ACCEPTED_LOSS anchor found nothing")
                .isNotEqualTo(-1);
        assertThat(publishedRow(text, "DEFECT")).as("the DEFECT anchor found nothing")
                .isNotEqualTo(-1);

        String mutated = text.replaceFirst("\\| `DEFECT` \\| \\d+ \\|", "| `DEFECT` | 9999 |");
        assertThat(mutated).as("the mutation must actually apply, or this drill proves nothing")
                .isNotEqualTo(text);
        assertThat(publishedRow(mutated, "DEFECT"))
                .as("a hand-edited DEFECT count must be visible to this gate")
                .isEqualTo(9999);

        String reworded = text.replaceFirst("corpus of \\*\\*\\d+ fixtures\\*\\*",
                "corpus of many fixtures");
        assertThat(publishedTotal(reworded))
                .as("rewording the headline out of existence must FAIL the gate, not silently "
                        + "excuse it - -1 never equals a manifest count")
                .isEqualTo(-1);
    }
}
