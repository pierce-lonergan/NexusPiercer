package io.github.pierce.gates;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every benchmark figure {@code docs/PERFORMANCE.md} publishes, checked against
 * {@code benchmarks/results/baseline.json} - the file the gate actually reads.
 *
 * <h2>Why this exists</h2>
 *
 * <p>The 2026-08-19 pass published {@code consolidate_deepNarrow} as <b>13,504</b> B/op in its
 * results table. 13,504 is the measurement from iteration 5, which that same document reports as
 * REVERTED. The shipped figure is 13,632, and it was written into
 * {@code benchmarks/results/baseline.json} <em>in the same commit</em>. Every other cell in the
 * table matched the baseline to the digit, which is what ruled out a protocol difference and
 * isolated it to a single slip - and nothing anywhere bound the document to the file, so nothing
 * caught it. The table credited the pass with 128 bytes that no code in the tree saves.</p>
 *
 * <p>This is the same technique as {@link PublishedProjectFactsMatchTheSourceTest}, pointed at
 * the other document that restates a machine-readable source of truth.</p>
 *
 * <h2>What it can and cannot catch</h2>
 *
 * <p>It catches a published number drifting from the recorded one. It cannot catch the recorded
 * one being wrong - that is what the harness-freshness check and the natural control group are
 * for, one level down. A baseline recorded against a stale artifact would be internally
 * consistent and this gate would pass it happily.</p>
 */
@DisplayName("published benchmark numbers match the recorded baseline")
class PublishedBenchmarkNumbersMatchTheBaselineTest {

    private static final Path PERFORMANCE = Paths.get("docs/PERFORMANCE.md");
    private static final Path BASELINE = Paths.get("benchmarks/results/baseline.json");

    /**
     * The pass table is a re-measurement of the same bytecode the baseline records, taken on a
     * different day in a different fork, so it is checked within a band rather than exactly. The
     * band is measured, not chosen for comfort: the largest fork-to-fork difference observed
     * anywhere in this suite is 0.28% ({@code consolidate_mixedProduction} avgt, 840 B in 300 K),
     * and the drift this gate exists to catch was 0.94% (13,504 against 13,632). 0.5% separates
     * them.
     */
    private static final double REPRODUCTION_BAND_PERCENT = 0.5;

    private static final Pattern ROW = Pattern.compile(
            "^\\|\\s*(?:\\*\\*control\\*\\*\\s*)?`([A-Za-z0-9_]+)"
                    + "(?:\\s*\\[([^\\]]*)\\])?`[^|]*\\|(.*)$");

    // ------------------------------------------------------------------ 1. the current results

    @Test
    @DisplayName("every row of the Current results table is the baseline's own figure")
    void currentResultsTableMatchesTheBaseline() {
        Map<String, JsonNode> baseline = baseline();
        List<String[]> rows = tableRows("| MB alloc/op |");

        assertThat(rows)
                .as("docs/PERFORMANCE.md no longer carries a results table ending in "
                        + "'| MB alloc/op |'. THE ANCHOR MUST BIND: rewording the header out of "
                        + "existence would make this gate silently stop measuring, which is the "
                        + "failure it was written to prevent one level up.")
                .hasSizeGreaterThan(20);

        List<String> wrong = new ArrayList<>();
        for (String[] row : rows) {
            String key = key(row[0], "avgt", row[1]);
            JsonNode entry = baseline.get(key);
            if (entry == null) {
                wrong.add(row[0] + row[1] + ": no avgt entry in the baseline at all");
                continue;
            }
            double score = entry.get("primaryMetric").get("score").asDouble();
            double error = entry.get("primaryMetric").get("scoreError").asDouble();
            double alloc = alloc(entry);
            check(wrong, row[0] + row[1] + " us/op", cell(row, 2), round(score, 1));
            check(wrong, row[0] + row[1] + " error", cell(row, 3), round(error, 1));
            check(wrong, row[0] + row[1] + " MB/op", cell(row, 4), round(alloc / 1e6, 3));
        }

        assertThat(wrong)
                .as("docs/PERFORMANCE.md publishes figures that benchmarks/results/baseline.json "
                        + "does not record. The baseline file is what compare.py reads, so a "
                        + "document that disagrees with it is describing a run nobody can "
                        + "reproduce - which is how a REVERTED iteration's measurement came to be "
                        + "published as a shipped result. Take the number from the baseline, or "
                        + "re-record the baseline; do not edit one of the two.")
                .isEmpty();
    }

    @Test
    @DisplayName("no baseline row is missing from the published table")
    void everyBaselineAvgtRowIsPublished() {
        Map<String, JsonNode> baseline = baseline();
        List<String> published = new ArrayList<>();
        for (String[] row : tableRows("| MB alloc/op |")) {
            published.add(key(row[0], "avgt", row[1]));
        }

        List<String> missing = new ArrayList<>();
        for (String key : new TreeMap<>(baseline).keySet()) {
            if (key.contains("|avgt|") && !published.contains(key)) {
                missing.add(key);
            }
        }

        assertThat(missing)
                .as("benchmarks/results/baseline.json records these avgt rows and "
                        + "docs/PERFORMANCE.md does not publish them. A results table that "
                        + "quietly drops a benchmark is how a regression goes unnoticed in the "
                        + "document a reader trusts, and dropping the row is the easiest way to "
                        + "make a disagreement with the baseline go away.")
                .isEmpty();
    }

    @Test
    @DisplayName("the throughput-only batch figure is the baseline's own figure")
    void theBatchSentenceMatchesTheBaseline() {
        Matcher m = Pattern.compile("\\*\\*([\\d,.]+) ops/s,\\s*([\\d,]+) B/op\\*\\*")
                .matcher(read(PERFORMANCE));
        assertThat(m.find())
                .as("docs/PERFORMANCE.md no longer publishes consolidate_batch1000 in the form "
                        + "'**N ops/s, M B/op**'. THE ANCHOR MUST BIND - it is the "
                        + "externally-quoted number and the only benchmark with no us/op row, so "
                        + "it is checked nowhere else.")
                .isTrue();

        JsonNode entry = baseline().get(key("consolidate_batch1000", "thrpt", ""));
        assertThat(entry).as("no consolidate_batch1000 thrpt entry in the baseline").isNotNull();

        assertThat(number(m.group(1)))
                .as("docs/PERFORMANCE.md publishes %s ops/s for consolidate_batch1000; the "
                        + "baseline records %s", m.group(1),
                        entry.get("primaryMetric").get("score").asDouble())
                .isEqualTo(round(entry.get("primaryMetric").get("score").asDouble(), 2));
        assertThat(number(m.group(2)))
                .as("docs/PERFORMANCE.md publishes %s B/op for consolidate_batch1000; the "
                        + "baseline records %s", m.group(2), alloc(entry))
                .isEqualTo((double) Math.round(alloc(entry)));
    }

    // ------------------------------------------------------------------ 2. the pass A/B table

    @Test
    @DisplayName("the pass table's after column reproduces the baseline within measurement error")
    void theAfterColumnReproducesTheBaseline() {
        Map<String, JsonNode> baseline = baseline();
        List<String[]> rows = tableRows("| before B/op | after B/op |");

        assertThat(rows)
                .as("docs/PERFORMANCE.md no longer carries a before/after table with a "
                        + "'| before B/op | after B/op |' header. THE ANCHOR MUST BIND: that "
                        + "table is where a REVERTED iteration's number was published as a "
                        + "shipped result, and it is the only reason this test exists.")
                .hasSizeGreaterThan(5);

        List<String> wrong = new ArrayList<>();
        for (String[] row : rows) {
            JsonNode entry = baseline.get(key(row[0], "avgt", row[1]));
            if (entry == null) {
                entry = baseline.get(key(row[0], "thrpt", row[1]));
            }
            if (entry == null) {
                wrong.add(row[0] + ": no baseline entry in either mode");
                continue;
            }
            double published = cell(row, 3);
            double recorded = alloc(entry);
            double drift = Math.abs(published - recorded) / recorded * 100.0;
            if (drift > REPRODUCTION_BAND_PERCENT) {
                wrong.add(String.format("%s: published %,.0f, baseline %,.0f (%.2f%% apart)",
                        row[0], published, recorded, drift));
            }
        }

        assertThat(wrong)
                .as("the 'after' column of the pass table is more than %s%% from the figure "
                        + "benchmarks/results/baseline.json records for the same bytecode. This "
                        + "is the exact defect the table shipped with: consolidate_deepNarrow was "
                        + "published as 13,504 - iteration 5's measurement, and iteration 5 was "
                        + "REVERTED - against a recorded 13,632, a gap of 0.94%%. Fork-to-fork "
                        + "reproduction on this suite is within 0.28%%, so anything past this "
                        + "band is a transcription error, not a measurement.",
                        REPRODUCTION_BAND_PERCENT)
                .isEmpty();
    }

    // ------------------------------------------------------------------ helpers

    /** Baseline entries keyed as {@code name|mode|params}. */
    private static Map<String, JsonNode> baseline() {
        JsonNode root;
        try {
            root = new ObjectMapper().readTree(Files.readString(BASELINE, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new AssertionError("cannot read " + BASELINE, e);
        }
        Map<String, JsonNode> out = new LinkedHashMap<>();
        for (JsonNode entry : root) {
            String full = entry.get("benchmark").asText();
            String name = full.substring(full.lastIndexOf('.') + 1);
            StringBuilder params = new StringBuilder();
            JsonNode p = entry.get("params");
            if (p != null) {
                for (var it = p.fields(); it.hasNext();) {
                    var field = it.next();
                    params.append(field.getKey()).append('=').append(field.getValue().asText());
                }
            }
            out.put(key(name, entry.get("mode").asText(), params.toString()), entry);
        }
        assertThat(out).as("benchmarks/results/baseline.json parsed to nothing").isNotEmpty();
        return out;
    }

    /**
     * The document writes params as {@code [distinctSchemas=1000]} and JMH records them as a
     * JSON object, so the brackets are stripped rather than being carried into the key.
     */
    private static String key(String name, String mode, String params) {
        return name + "|" + mode + "|"
                + params.replace(" ", "").replace("[", "").replace("]", "");
    }

    /**
     * The rows of the markdown table whose header line contains {@code headerFragment}, each as
     * {@code [name, params, cell1, cell2, ...]}.
     *
     * <p>Scoped to one table on purpose. A document-wide row regex also matches the neighbouring
     * "stale published baseline" table, whose second column is a figure this gate would then
     * compare against the wrong thing.</p>
     */
    private static List<String[]> tableRows(String headerFragment) {
        List<String[]> rows = new ArrayList<>();
        boolean inTable = false;
        for (String line : read(PERFORMANCE).split("\\R")) {
            if (!inTable) {
                inTable = line.contains(headerFragment);
                continue;
            }
            if (!line.startsWith("|")) {
                break;
            }
            Matcher m = ROW.matcher(line);
            if (!m.matches()) {
                continue;
            }
            List<String> cells = new ArrayList<>();
            cells.add(m.group(1));
            cells.add(m.group(2) == null ? "" : "[" + m.group(2) + "]");
            for (String cell : m.group(3).split("\\|")) {
                cells.add(cell.trim());
            }
            rows.add(cells.toArray(new String[0]));
        }
        return rows;
    }

    /** Cell {@code i} of a row as a number, commas and bold markers removed. */
    private static double cell(String[] row, int i) {
        assertThat(row.length)
                .as("row %s has %d cells; cell %d was asked for, so the table's shape changed "
                        + "and this gate is reading the wrong column", row[0], row.length, i)
                .isGreaterThan(i);
        return number(row[i]);
    }

    private static double number(String text) {
        String cleaned = text.replace(",", "").replace("*", "").trim();
        return Double.parseDouble(cleaned);
    }

    private static double alloc(JsonNode entry) {
        JsonNode secondary = entry.get("secondaryMetrics");
        assertThat(secondary).as("no secondaryMetrics on a baseline entry").isNotNull();
        JsonNode norm = secondary.get("gc.alloc.rate.norm");
        assertThat(norm)
                .as("a baseline entry has no gc.alloc.rate.norm, so it was recorded WITHOUT "
                        + "-prof gc and the only blocking gate tier has nothing to read")
                .isNotNull();
        return norm.get("score").asDouble();
    }

    private static void check(List<String> wrong, String what, double published, double recorded) {
        if (Math.abs(published - recorded) > 1e-9) {
            wrong.add(String.format("%s: document says %s, baseline records %s",
                    what, published, recorded));
        }
    }

    private static double round(double value, int decimals) {
        double factor = Math.pow(10, decimals);
        return Math.round(value * factor) / factor;
    }

    private static String read(Path p) {
        try {
            return Files.readString(p, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AssertionError("cannot read " + p, e);
        }
    }
}
