package io.github.pierce.FlattenConsolidatorTests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pierce.JsonFlattenerConsolidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Byte-identity gate for {@link JsonFlattenerConsolidator#flattenAndConsolidateJson(String)}.
 *
 * <p><b>Why this class exists.</b> The 166-fixture round-trip fidelity corpus is the repository's
 * stated instrument for "output must not change". It does not cover this class at all: grepping
 * {@code src/test/java/io/github/pierce/fidelity/} for {@code JsonFlattenerConsolidator} returns
 * nothing, because {@code FidelityRunner} drives {@code MapFlattener}, {@code JsonFlattener},
 * {@code AvroSchemaFlattener} and {@code GAvroSchemaFlattener} only. For any change to the
 * consolidator, "the corpus stayed byte-identical" is a gate that appears present and does
 * nothing — which is the exact pathology this project has removed from three other gates.</p>
 *
 * <p>This test closes that hole. It pins the <em>emitted JSON string</em>, not a parsed map, so
 * key order is part of the contract: a change that reorders columns fails here even though
 * {@code Map.equals} would call the two results equal.</p>
 *
 * <p><b>The recording is the assertion.</b> {@code consolidator-golden.txt} was recorded from the
 * tree as it stood before the 2026-08-19 performance pass and must not be regenerated to
 * accommodate a change. A performance change that moves a single byte here is not an
 * optimization, it is a behaviour change, and it belongs in its own deliberate commit with the
 * recording updated on purpose and the reason written down.</p>
 *
 * <p>Regenerate ONLY when the change is intentional:
 * {@code ./mvnw test -Dtest=ConsolidatorByteIdentityGoldenTest -Dconsolidator.golden.record=true}
 * then read the diff line by line before committing it.</p>
 *
 * <h2>What the input corpus covers</h2>
 * <ol>
 *   <li>All 166 fidelity fixture inputs — real adversarial documents that the consolidator has
 *       never been run against, reused here for the first time.</li>
 *   <li>Hand-built keys that probe the exact edges where a hand-rolled array-index scan can
 *       disagree with the regex it replaces: a key that <em>starts</em> with a bracket, a bracket
 *       preceded by a line terminator, non-ASCII digits inside brackets, unterminated and
 *       doubled brackets.</li>
 *   <li>Deterministic re-creations of the four benchmark corpus shapes, so the documents the
 *       performance work actually targets are the documents the gate actually checks.</li>
 * </ol>
 *
 * <p>Each document is run under four configurations, because the consolidator's behaviour forks
 * on both {@code gatherStatistics} and {@code consolidateWithMatrixDenotorsInValue} and a change
 * that preserves one branch can break the other.</p>
 */
@DisplayName("Consolidator output is byte-identical to its recording")
class ConsolidatorByteIdentityGoldenTest {

    private static final String GOLDEN_RESOURCE = "/consolidator-golden.txt";
    private static final String RECORD_PROPERTY = "consolidator.golden.record";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * The four configurations under test. Named so a golden diff says which branch moved.
     *
     * <p>{@code stats}/{@code nostats} selects {@code gatherStatistics}; {@code matrix} selects
     * {@code consolidateWithMatrixDenotorsInValue}. The pipe delimiter variant exists because
     * {@code processGroupedValues} tests {@code value.contains(arrayDelimiter)} and a
     * single-character comma is the case most likely to hide a delimiter bug.</p>
     */
    private static List<Map.Entry<String, JsonFlattenerConsolidator>> configurations() {
        List<Map.Entry<String, JsonFlattenerConsolidator>> configs = new ArrayList<>();
        configs.add(Map.entry("comma-stats",
                new JsonFlattenerConsolidator(",", null, 100, 10_000, false, true)));
        configs.add(Map.entry("comma-nostats",
                new JsonFlattenerConsolidator(",", null, 100, 10_000, false, false)));
        configs.add(Map.entry("comma-matrix-stats",
                new JsonFlattenerConsolidator(",", null, 100, 10_000, true, true)));
        configs.add(Map.entry("pipe-stats-nullph",
                new JsonFlattenerConsolidator("|", "NULL", 100, 10_000, false, true)));
        return configs;
    }

    @Test
    @DisplayName("every recorded (document, configuration) pair still emits the same bytes")
    void outputIsByteIdentical() throws IOException {
        List<Map.Entry<String, String>> documents = inputCorpus();
        assertThat(documents)
                .as("input corpus must not silently shrink; a golden over nothing always passes")
                .hasSizeGreaterThanOrEqualTo(176);

        Map<String, String> actual = new LinkedHashMap<>();
        for (Map.Entry<String, JsonFlattenerConsolidator> config : configurations()) {
            for (Map.Entry<String, String> doc : documents) {
                String caseId = config.getKey() + " :: " + doc.getKey();
                actual.put(caseId, config.getValue().flattenAndConsolidateJson(doc.getValue()));
            }
        }

        if (Boolean.getBoolean(RECORD_PROPERTY)) {
            record(actual);
            fail("Golden recording rewritten with " + actual.size() + " cases. "
                    + "This is never a passing state: review the diff, then re-run without -D"
                    + RECORD_PROPERTY + " to prove the recording is what the code emits.");
        }

        Map<String, String> expected = readGolden();
        assertThat(expected)
                .as("recorded golden must cover exactly the cases the test generates")
                .hasSameSizeAs(actual);

        List<String> drifted = new ArrayList<>();
        for (Map.Entry<String, String> entry : actual.entrySet()) {
            String recorded = expected.get(entry.getKey());
            if (recorded == null) {
                drifted.add("NEW CASE (not in recording): " + entry.getKey());
            } else if (!recorded.equals(entry.getValue())) {
                drifted.add("CHANGED: " + entry.getKey()
                        + "\n    recorded: " + recorded
                        + "\n    emitted : " + entry.getValue());
            }
        }
        for (String key : expected.keySet()) {
            if (!actual.containsKey(key)) {
                drifted.add("MISSING CASE (recorded but no longer generated): " + key);
            }
        }

        assertThat(drifted)
                .as("Consolidator output changed. Under a performance pass this is a REVERT, not "
                        + "a re-record: a faster flattener that emits different bytes is a "
                        + "regression wearing a stopwatch. If the change is a deliberate "
                        + "correctness fix, it belongs in its own commit with this recording "
                        + "updated on purpose.")
                .isEmpty();
    }

    // ------------------------------------------------------------------ input corpus

    private static List<Map.Entry<String, String>> inputCorpus() throws IOException {
        List<Map.Entry<String, String>> docs = new ArrayList<>();
        docs.addAll(fidelityInputs());
        docs.addAll(adversarialKeyDocuments());
        docs.addAll(corpusShapes());
        return docs;
    }

    /**
     * The 166 fidelity fixture inputs, sorted by fixture id so the golden is stable across
     * filesystems that enumerate directories in different orders.
     */
    private static List<Map.Entry<String, String>> fidelityInputs() throws IOException {
        Path root = Paths.get("src", "test", "resources", "fidelity");
        assertThat(Files.isDirectory(root))
                .as("fidelity fixture tree must be present at %s", root.toAbsolutePath())
                .isTrue();

        List<Map.Entry<String, String>> inputs = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(root)) {
            walk.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .filter(p -> !"manifest.json".equals(p.getFileName().toString()))
                    .sorted()
                    .forEach(p -> {
                        try {
                            JsonNode fixture = MAPPER.readTree(Files.readString(p, StandardCharsets.UTF_8));
                            JsonNode input = fixture.get("input");
                            JsonNode id = fixture.get("id");
                            if (input != null && input.isTextual() && id != null && id.isTextual()) {
                                inputs.add(Map.entry("fidelity/" + id.asText(), input.asText()));
                            }
                        } catch (IOException e) {
                            throw new UncheckedIOException("unreadable fidelity fixture " + p, e);
                        }
                    });
        }
        assertThat(inputs)
                .as("157 of the 166 fidelity fixtures carry a textual JSON input; the other 9 are "
                        + "Java-native map fixtures (circular references, non-string map keys, "
                        + "BigDecimal, Date) that have no JSON text form and cannot reach this "
                        + "class. A count below 157 means the walk is wrong, not that the corpus "
                        + "shrank.")
                .hasSizeGreaterThanOrEqualTo(157);
        return inputs;
    }

    /**
     * Documents whose <em>keys</em> sit on the edges of the array-index predicate.
     *
     * <p>None of these shapes appears in any benchmark corpus or any fidelity fixture, and each
     * one distinguishes the regex {@code (.+?)\[(\d+)\](.*)} from a naive character scan:</p>
     * <ul>
     *   <li>{@code "[0]"} — the regex requires at least one character before the bracket, so this
     *       is NOT array-indexed and is emitted as a plain column.</li>
     *   <li>{@code "a\n[0]"} — {@code .} excludes line terminators, so the bracket is not
     *       preceded by a matchable character and this is NOT array-indexed either.</li>
     *   <li>{@code "a[०]"} — Devanagari zero. {@code \d} without UNICODE_CHARACTER_CLASS is
     *       ASCII-only, so this is NOT an index; a scan using {@code Character.isDigit} would
     *       wrongly strip it and silently merge two columns.</li>
     *   <li>{@code "a[[0]"} — the strip is left-to-right non-overlapping, so it yields {@code a[}.
     *       A scan that skips past a failed bracket instead of advancing one character gets this
     *       wrong.</li>
     * </ul>
     */
    private static List<Map.Entry<String, String>> adversarialKeyDocuments() {
        List<Map.Entry<String, String>> docs = new ArrayList<>();
        docs.add(Map.entry("adv/leading-bracket-key", "{\"[0]\":1,\"[1]\":2}"));
        docs.add(Map.entry("adv/user-supplied-index-key", "{\"a[0]\":1,\"a[1]\":2,\"a\":3}"));
        docs.add(Map.entry("adv/newline-before-bracket", "{\"a\\n[0]\":1,\"a\\r[0]\":2,\"a [0]\":3}"));
        docs.add(Map.entry("adv/unicode-digit-index", "{\"a[\\u0966]\":1,\"a[\\u06f0]\":2,\"a[\\uff10]\":3}"));
        docs.add(Map.entry("adv/doubled-open-bracket", "{\"a[[0]\":1,\"a[0][1]\":2,\"a[00]\":3}"));
        docs.add(Map.entry("adv/unterminated-bracket", "{\"a[0\":1,\"a[x]\":2,\"a[0x]\":3,\"a[]\":4}"));
        docs.add(Map.entry("adv/huge-index-digits", "{\"a[99999999999999999999999]\":1}"));
        docs.add(Map.entry("adv/dot-and-underscore-keys",
                "{\"a.b\":1,\"a_b\":2,\"a\":{\"b\":3}}"));
        docs.add(Map.entry("adv/no-underscore-array-names",
                "{\"items\":[{\"sku\":\"a\"},{\"sku\":\"b\"}],\"tags\":[\"x\",\"y\"]}"));
        docs.add(Map.entry("adv/single-element-array-with-delimiter",
                "{\"items\":[\"a,b,c\"],\"other\":[\"p|q\"]}"));
        docs.add(Map.entry("adv/nested-array-of-arrays",
                "{\"m\":[[1,2],[3,4]],\"n\":[[{\"k\":1}]]}"));
        docs.add(Map.entry("adv/empty-array-and-object",
                "{\"e\":[],\"o\":{},\"n\":null,\"z\":[null,null]}"));
        docs.add(Map.entry("adv/numeric-looking-strings",
                "{\"v\":[\"1\",\"2.5\",\"+3\",\"-0\",\"1e10\",\"0x1p3\",\"1d\",\"NaN\",\"Infinity\"]}"));
        docs.add(Map.entry("adv/boolean-and-mixed-arrays",
                "{\"b\":[\"true\",\"FALSE\"],\"m\":[\"true\",\"1\"],\"s\":[\"\",\"  \"]}"));
        docs.add(Map.entry("adv/duplicate-and-empty-values",
                "{\"d\":[\"x\",\"x\",\"\",\"yy\"],\"c\":[\",\",\",,\"]}"));
        return docs;
    }

    /**
     * Deterministic stand-ins for the benchmark corpus shapes.
     *
     * <p>Sized down so the golden file stays reviewable, but structurally identical to what
     * {@code benchmarks/.../Corpus.java} generates: snake_case names containing the separator,
     * scalar arrays, arrays of records, and a deep chain. Seeded from a constant for the same
     * reason the benchmark corpus is — a golden over a varying input is not a golden.</p>
     */
    private static List<Map.Entry<String, String>> corpusShapes() {
        List<Map.Entry<String, String>> docs = new ArrayList<>();
        Random rnd = new Random(0x4E455855_53504943L);

        StringBuilder wide = new StringBuilder("{");
        for (int i = 0; i < 40; i++) {
            if (i > 0) {
                wide.append(',');
            }
            wide.append("\"field_str_").append(i).append("\":\"")
                    .append(token(rnd, 8)).append('"');
        }
        wide.append('}');
        docs.add(Map.entry("shape/wideFlat40", wide.toString()));

        StringBuilder deep = new StringBuilder();
        for (int i = 0; i < 23; i++) {
            deep.append("{\"level_").append(i).append("\":");
        }
        deep.append("{\"leaf_value\":\"terminal\"}");
        deep.append("}".repeat(23));
        docs.add(Map.entry("shape/deepNarrow24", deep.toString()));

        StringBuilder arrays = new StringBuilder("{");
        for (int a = 0; a < 3; a++) {
            if (a > 0) {
                arrays.append(',');
            }
            arrays.append("\"numeric_array_").append(a).append("\":[");
            for (int i = 0; i < 12; i++) {
                arrays.append(i > 0 ? "," : "").append('"').append(rnd.nextInt(100000)).append('"');
            }
            arrays.append(']');
        }
        for (int a = 0; a < 3; a++) {
            arrays.append(",\"string_array_").append(a).append("\":[");
            for (int i = 0; i < 12; i++) {
                arrays.append(i > 0 ? "," : "").append('"').append(token(rnd, 8)).append('"');
            }
            arrays.append(']');
        }
        arrays.append(",\"record_array\":[");
        for (int i = 0; i < 5; i++) {
            arrays.append(i > 0 ? "," : "")
                    .append("{\"nested_field_0\":\"").append(token(rnd, 6))
                    .append("\",\"nested_field_1\":").append(rnd.nextInt(1000)).append('}');
        }
        arrays.append("]}");
        docs.add(Map.entry("shape/arrayHeavySmall", arrays.toString()));

        docs.add(Map.entry("shape/mixedProduction",
                "{\"order_id\":\"ord-1\",\"created_at\":\"2026-08-19\",\"order_total\":12.5,"
                        + "\"user\":{\"user_id\":7,\"user_name\":\"n\",\"is_active\":true},"
                        + "\"line_items\":[{\"sku_code\":\"a\",\"quantity_ordered\":1,\"unit_price\":2.0},"
                        + "{\"sku_code\":\"b\",\"quantity_ordered\":3,\"unit_price\":4.5}],"
                        + "\"attribute_0_value\":null,\"attribute_1_value\":9,"
                        + "\"attribute_2_value\":\"s\"}"));
        return docs;
    }

    private static String token(Random rnd, int len) {
        StringBuilder sb = new StringBuilder(len);
        String alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
        for (int i = 0; i < len; i++) {
            sb.append(alphabet.charAt(rnd.nextInt(alphabet.length())));
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------ golden i/o

    /**
     * Golden format: one case per pair of lines, {@code caseId} then the emitted JSON. Jackson
     * escapes every control character it writes, so an emitted document is always a single line
     * and the format cannot be confused by a newline inside a key or value.
     */
    private static Map<String, String> readGolden() throws IOException {
        try (InputStream in = ConsolidatorByteIdentityGoldenTest.class
                .getResourceAsStream(GOLDEN_RESOURCE)) {
            assertThat(in)
                    .as("golden recording %s is missing from the test classpath", GOLDEN_RESOURCE)
                    .isNotNull();
            String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            Map<String, String> out = new LinkedHashMap<>();
            String[] lines = body.split("\n", -1);
            for (int i = 0; i + 1 < lines.length; i += 2) {
                String header = lines[i];
                if (header.isEmpty()) {
                    break;
                }
                assertThat(header)
                        .as("malformed golden at line %d", i + 1)
                        .startsWith("# ");
                out.put(header.substring(2), lines[i + 1]);
            }
            return out;
        }
    }

    private static void record(Map<String, String> cases) throws IOException {
        Path target = Paths.get("src", "test", "resources", "consolidator-golden.txt");
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : cases.entrySet()) {
            sb.append("# ").append(e.getKey()).append('\n').append(e.getValue()).append('\n');
        }
        Files.writeString(target, sb.toString(), StandardCharsets.UTF_8);
    }
}
