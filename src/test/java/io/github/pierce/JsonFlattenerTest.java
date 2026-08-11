package io.github.pierce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Comprehensive test suite for JsonFlattener.
 *
 * <p>Ported from src/test/groovy/JsonFlattenerTest.groovy. The Groovy original contained
 * 62 {@code @Test} methods across 11 {@code @Nested} classes; this port contains the same
 * 62 methods in the same 11 nested classes, with the same display names.
 *
 * @author Pierce
 */
@DisplayName("JsonFlattener Tests")
class JsonFlattenerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;

    // ========================= BASIC FLATTENING TESTS =========================

    @Nested
    @DisplayName("Basic Flattening")
    class BasicFlatteningTests {

        @Test
        @DisplayName("Should flatten simple nested JSON")
        void shouldFlattenSimpleNestedJson() {
            String json = "{\"user\": {\"name\": \"John\", \"age\": 30}}";

            Map<String, Object> result = JsonFlattener.create()
                    .from(json)
                    .toMap();

            assertThat(result).hasSize(2);
            assertThat(result.get("user_name")).isEqualTo("John");
            assertThat(result.get("user_age")).isEqualTo(30);
        }

        @Test
        @DisplayName("Should flatten deeply nested JSON")
        void shouldFlattenDeeplyNestedJson() {
            String json = "{\"a\": {\"b\": {\"c\": {\"d\": {\"e\": \"deep\"}}}}}";

            Map<String, Object> result = JsonFlattener.create()
                    .from(json)
                    .toMap();

            assertThat(result).hasSize(1);
            assertThat(result.get("a_b_c_d_e")).isEqualTo("deep");
        }

        @Test
        @DisplayName("Should flatten JSON with arrays")
        void shouldFlattenJsonWithArrays() {
            String json = "{\"users\": [{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]}";

            Map<String, Object> result = JsonFlattener.create()
                    .from(json)
                    .toMap();

            assertThat(result.get("users_name")).isNotNull();
            assertThat(result.get("users_name").toString()).contains("Alice");
            assertThat(result.get("users_name").toString()).contains("Bob");
        }

        @Test
        @DisplayName("Should flatten JSON with mixed types")
        void shouldFlattenJsonWithMixedTypes() {
            String json = "{\"string\": \"value\", \"number\": 42, \"boolean\": true, \"null\": null}";

            Map<String, Object> result = JsonFlattener.create()
                    .from(json)
                    .toMap();

            assertThat(result.get("string")).isEqualTo("value");
            assertThat(result.get("number")).isEqualTo(42);
            assertThat(result.get("boolean")).isEqualTo(true);
            assertThat(result.get("null")).isNull();
        }

        @Test
        @DisplayName("Should handle empty JSON object")
        void shouldHandleEmptyJsonObject() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{}")
                    .toMap();

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("Should handle flat JSON without nesting")
        void shouldHandleFlatJson() {
            String json = "{\"a\": 1, \"b\": 2, \"c\": 3}";

            Map<String, Object> result = JsonFlattener.create()
                    .from(json)
                    .toMap();

            assertThat(result).hasSize(3);
            assertThat(result.get("a")).isEqualTo(1);
            assertThat(result.get("b")).isEqualTo(2);
            assertThat(result.get("c")).isEqualTo(3);
        }

        @Test
        @DisplayName("Should preserve key order")
        void shouldPreserveKeyOrder() {
            String json = "{\"z\": 1, \"a\": 2, \"m\": 3}";

            Map<String, Object> result = JsonFlattener.create()
                    .from(json)
                    .toMap();

            List<String> keys = new ArrayList<>(result.keySet());
            assertThat(keys).containsExactly("z", "a", "m");
        }
    }

    // ========================= INPUT SOURCE TESTS =========================

    @Nested
    @DisplayName("Input Sources")
    class InputSourceTests {

        private final String testJson = "{\"key\": \"value\", \"nested\": {\"inner\": 123}}";

        @Test
        @DisplayName("Should read from String")
        void shouldReadFromString() {
            Map<String, Object> result = JsonFlattener.create()
                    .from(testJson)
                    .toMap();

            assertThat(result.get("key")).isEqualTo("value");
            assertThat(result.get("nested_inner")).isEqualTo(123);
        }

        @Test
        @DisplayName("Should read from Map")
        void shouldReadFromMap() {
            Map<String, Object> input = new LinkedHashMap<>();
            input.put("name", "Alice");
            input.put("details", Map.of("age", 25));

            Map<String, Object> result = JsonFlattener.create()
                    .from(input)
                    .toMap();

            assertThat(result.get("name")).isEqualTo("Alice");
            assertThat(result.get("details_age")).isEqualTo(25);
        }

        @Test
        @DisplayName("Should read from byte array")
        void shouldReadFromByteArray() {
            byte[] bytes = testJson.getBytes(StandardCharsets.UTF_8);

            Map<String, Object> result = JsonFlattener.create()
                    .from(bytes)
                    .toMap();

            assertThat(result.get("key")).isEqualTo("value");
        }

        @Test
        @DisplayName("Should read from byte array with charset")
        void shouldReadFromByteArrayWithCharset() {
            byte[] bytes = testJson.getBytes(StandardCharsets.UTF_16);

            Map<String, Object> result = JsonFlattener.create()
                    .from(bytes, StandardCharsets.UTF_16)
                    .toMap();

            assertThat(result.get("key")).isEqualTo("value");
        }

        @Test
        @DisplayName("Should read from InputStream")
        void shouldReadFromInputStream() {
            InputStream is = new ByteArrayInputStream(testJson.getBytes());

            Map<String, Object> result = JsonFlattener.create()
                    .from(is)
                    .toMap();

            assertThat(result.get("key")).isEqualTo("value");
        }

        @Test
        @DisplayName("Should read from Reader")
        void shouldReadFromReader() {
            Reader reader = new StringReader(testJson);

            Map<String, Object> result = JsonFlattener.create()
                    .from(reader)
                    .toMap();

            assertThat(result.get("key")).isEqualTo("value");
        }

        @Test
        @DisplayName("Should read from File")
        void shouldReadFromFile() throws Exception {
            Path file = tempDir.resolve("test.json");
            Files.writeString(file, testJson);

            Map<String, Object> result = JsonFlattener.create()
                    .from(file.toFile())
                    .toMap();

            assertThat(result.get("key")).isEqualTo("value");
        }

        @Test
        @DisplayName("Should read from Path")
        void shouldReadFromPath() throws Exception {
            Path file = tempDir.resolve("test.json");
            Files.writeString(file, testJson);

            Map<String, Object> result = JsonFlattener.create()
                    .from(file)
                    .toMap();

            assertThat(result.get("key")).isEqualTo("value");
        }

        @Test
        @DisplayName("Should read from GZIP file")
        void shouldReadFromGzipFile() throws Exception {
            Path file = tempDir.resolve("test.json.gz");
            try (OutputStream os = new GZIPOutputStream(Files.newOutputStream(file))) {
                os.write(testJson.getBytes());
            }

            Map<String, Object> result = JsonFlattener.create()
                    .from(file, JsonFlattener.InputOptions.gzipped())
                    .toMap();

            assertThat(result.get("key")).isEqualTo("value");
        }

        @Test
        @DisplayName("Should read from JsonNode")
        void shouldReadFromJsonNode() throws Exception {
            JsonNode node = MAPPER.readTree(testJson);

            Map<String, Object> result = JsonFlattener.create()
                    .from(node)
                    .toMap();

            assertThat(result.get("key")).isEqualTo("value");
        }

        @Test
        @DisplayName("Should throw on invalid JSON")
        void shouldThrowOnInvalidJson() {
            assertThatThrownBy(() ->
                    JsonFlattener.create()
                            .from("not valid json")
                            .toMap()
            ).isInstanceOf(JsonFlattener.JsonFlattenException.class);
        }

        @Test
        @DisplayName("Should throw on missing file")
        void shouldThrowOnMissingFile() {
            Path nonExistent = tempDir.resolve("does-not-exist.json");

            assertThatThrownBy(() ->
                    JsonFlattener.create()
                            .from(nonExistent)
                            .toMap()
            ).isInstanceOf(JsonFlattener.JsonFlattenException.class);
        }
    }

    // ========================= OUTPUT TARGET TESTS =========================

    @Nested
    @DisplayName("Output Targets")
    class OutputTargetTests {

        private final String testJson = "{\"a\": 1, \"b\": {\"c\": 2}}";

        @Test
        @DisplayName("Should output to JSON string")
        void shouldOutputToJsonString() {
            String result = JsonFlattener.create()
                    .from(testJson)
                    .toJson();

            assertThat(result).isNotNull();
            assertThat(result.contains("\"a\":1") || result.contains("\"a\": 1"))
                    .as("output should contain a compact or spaced \"a\" entry: %s", result)
                    .isTrue();
            assertThat(result.contains("\"b_c\":2") || result.contains("\"b_c\": 2"))
                    .as("output should contain a compact or spaced \"b_c\" entry: %s", result)
                    .isTrue();
        }

        @Test
        @DisplayName("Should output to pretty JSON string")
        void shouldOutputToPrettyJsonString() {
            String result = JsonFlattener.create()
                    .from(testJson)
                    .toPrettyJson();

            assertThat(result).contains("\n");
            assertThat(result).contains("  ");
        }

        @Test
        @DisplayName("Should output to Map")
        void shouldOutputToMap() {
            Map<String, Object> result = JsonFlattener.create()
                    .from(testJson)
                    .toMap();

            assertThat(result).hasSize(2);
            assertThat(result.get("a")).isEqualTo(1);
            assertThat(result.get("b_c")).isEqualTo(2);
        }

        @Test
        @DisplayName("Should output to bytes")
        void shouldOutputToBytes() {
            byte[] result = JsonFlattener.create()
                    .from(testJson)
                    .toBytes();

            assertThat(result).isNotNull();
            assertThat(result.length > 0).isTrue();

            String asString = new String(result, StandardCharsets.UTF_8);
            assertThat(asString).contains("a");
        }

        @Test
        @DisplayName("Should output to JsonNode")
        void shouldOutputToJsonNode() {
            JsonNode result = JsonFlattener.create()
                    .from(testJson)
                    .toJsonNode();

            assertThat(result).isNotNull();
            assertThat(result.has("a")).isTrue();
            assertThat(result.has("b_c")).isTrue();
        }

        @Test
        @DisplayName("Should output to File")
        void shouldOutputToFile() throws Exception {
            Path file = tempDir.resolve("output.json");

            JsonFlattener.create()
                    .from(testJson)
                    .toFile(file.toFile());

            assertThat(Files.exists(file)).isTrue();
            String content = Files.readString(file);
            assertThat(content).contains("a");
        }

        @Test
        @DisplayName("Should output to Path")
        void shouldOutputToPath() throws Exception {
            Path file = tempDir.resolve("output.json");

            JsonFlattener.create()
                    .from(testJson)
                    .toFile(file);

            assertThat(Files.exists(file)).isTrue();
        }

        @Test
        @DisplayName("Should output to OutputStream")
        void shouldOutputToOutputStream() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            JsonFlattener.create()
                    .from(testJson)
                    .toStream(baos);

            assertThat(baos.size() > 0).isTrue();
            assertThat(baos.toString()).contains("a");
        }

        @Test
        @DisplayName("Should output to Writer")
        void shouldOutputToWriter() {
            StringWriter writer = new StringWriter();

            JsonFlattener.create()
                    .from(testJson)
                    .toWriter(writer);

            assertThat(writer.toString()).contains("a");
        }

        @Test
        @DisplayName("Should output to GZIP file")
        void shouldOutputToGzipFile() throws Exception {
            Path file = tempDir.resolve("output.json.gz");

            JsonFlattener.create()
                    .from(testJson)
                    .toFile(file, JsonFlattener.OutputOptions.gzipped());

            assertThat(Files.exists(file)).isTrue();

            // Verify it's actually gzipped
            try (GZIPInputStream gis = new GZIPInputStream(Files.newInputStream(file))) {
                String content = new String(gis.readAllBytes());
                assertThat(content).contains("a");
            }
        }

        @Test
        @DisplayName("Should output with sorted keys")
        void shouldOutputWithSortedKeys() {
            String json = "{\"z\": 1, \"a\": 2, \"m\": 3}";

            String result = JsonFlattener.create()
                    .from(json)
                    .toJson(JsonFlattener.OutputOptions.builder().sortKeys(true).build());

            int aPos = result.indexOf("\"a\"");
            int mPos = result.indexOf("\"m\"");
            int zPos = result.indexOf("\"z\"");

            assertThat(aPos < mPos).isTrue();
            assertThat(mPos < zPos).isTrue();
        }

        @Test
        @DisplayName("Should output without nulls when specified")
        void shouldOutputWithoutNulls() {
            String json = "{\"a\": 1, \"b\": null}";

            String result = JsonFlattener.create()
                    .from(json)
                    .toJson(JsonFlattener.OutputOptions.builder().includeNulls(false).build());

            assertThat(result.contains("\"b\"")).isFalse();
        }
    }

    // ========================= TRANSFORMATION TESTS =========================

    @Nested
    @DisplayName("Transformations")
    class TransformationTests {

        @Test
        @DisplayName("Should add field")
        void shouldAddField() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"a\": 1}")
                    .addField("b", 2)
                    .toMap();

            assertThat(result).hasSize(2);
            assertThat(result.get("a")).isEqualTo(1);
            assertThat(result.get("b")).isEqualTo(2);
        }

        @Test
        @DisplayName("Should remove field")
        void shouldRemoveField() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"a\": 1, \"b\": 2}")
                    .removeField("a")
                    .toMap();

            assertThat(result).hasSize(1);
            assertThat(result.containsKey("a")).isFalse();
            assertThat(result.containsKey("b")).isTrue();
        }

        @Test
        @DisplayName("Should rename field")
        void shouldRenameField() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"oldName\": \"value\"}")
                    .renameField("oldName", "newName")
                    .toMap();

            assertThat(result.containsKey("oldName")).isFalse();
            assertThat(result.get("newName")).isEqualTo("value");
        }

        @Test
        @DisplayName("Should include only specified fields")
        void shouldIncludeOnlySpecifiedFields() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"a\": 1, \"b\": 2, \"c\": 3}")
                    .includeOnly("a", "c")
                    .toMap();

            assertThat(result).hasSize(2);
            assertThat(result.containsKey("a")).isTrue();
            assertThat(result.containsKey("c")).isTrue();
            assertThat(result.containsKey("b")).isFalse();
        }

        @Test
        @DisplayName("Should prefix all keys")
        void shouldPrefixAllKeys() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"a\": 1, \"b\": 2}")
                    .prefixKeys("data_")
                    .toMap();

            assertThat(result.containsKey("data_a")).isTrue();
            assertThat(result.containsKey("data_b")).isTrue();
            assertThat(result.containsKey("a")).isFalse();
        }

        @Test
        @DisplayName("Should apply custom transformation")
        void shouldApplyCustomTransformation() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"value\": 10}")
                    .transform(map -> {
                        int val = (int) map.get("value");
                        map.put("value", val * 2);
                        map.put("computed", val * val);
                        return map;
                    })
                    .toMap();

            assertThat(result.get("value")).isEqualTo(20);
            assertThat(result.get("computed")).isEqualTo(100);
        }
    }

    // ========================= VALIDATION TESTS =========================

    @Nested
    @DisplayName("Validation")
    class ValidationTests {

        @Test
        @DisplayName("Should pass validation with required fields present")
        void shouldPassValidationWithRequiredFields() {
            JsonFlattener.ValidationRules rules = JsonFlattener.ValidationRules.builder()
                    .requireFields("id", "name")
                    .build();

            assertThatCode(() ->
                    JsonFlattener.create()
                            .from("{\"id\": 1, \"name\": \"test\"}")
                            .validate(rules)
                            .toMap()
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should fail validation with missing required fields")
        void shouldFailValidationWithMissingRequiredFields() {
            JsonFlattener.ValidationRules rules = JsonFlattener.ValidationRules.builder()
                    .requireFields("id", "name")
                    .build();

            JsonFlattener.JsonValidationException ex = catchThrowableOfType(() ->
                    JsonFlattener.create()
                            .from("{\"id\": 1}")
                            .validate(rules)
                            .toMap(),
                    JsonFlattener.JsonValidationException.class);

            assertThat(ex).isNotNull();
            assertThat(ex.getViolations().get(0).contains("name")).isTrue();
        }

        @Test
        @DisplayName("Should fail validation with too many fields")
        void shouldFailValidationWithTooManyFields() {
            JsonFlattener.ValidationRules rules = JsonFlattener.ValidationRules.builder()
                    .maxFields(2)
                    .build();

            assertThatThrownBy(() ->
                    JsonFlattener.create()
                            .from("{\"a\": 1, \"b\": 2, \"c\": 3}")
                            .validate(rules)
                            .toMap()
            ).isInstanceOf(JsonFlattener.JsonValidationException.class);
        }

        @Test
        @DisplayName("Should filter with predicate")
        void shouldFilterWithPredicate() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"value\": 10}")
                    .filter(map -> (int) map.get("value") > 5)
                    .toMap();

            assertThat(result).isNotEmpty();

            Map<String, Object> filtered = JsonFlattener.create()
                    .from("{\"value\": 3}")
                    .filter(map -> (int) map.get("value") > 5)
                    .toMap();

            assertThat(filtered).isEmpty();
        }
    }

    // ========================= BATCH PROCESSING TESTS =========================

    @Nested
    @DisplayName("Batch Processing")
    class BatchProcessingTests {

        @Test
        @DisplayName("Should process batch of JSON strings")
        void shouldProcessBatchOfJsonStrings() {
            List<String> inputs = Arrays.asList(
                    "{\"id\": 1}",
                    "{\"id\": 2}",
                    "{\"id\": 3}"
            );

            JsonFlattener.BatchResult result = JsonFlattener.create()
                    .batch()
                    .fromStrings(inputs);

            assertThat(result.getSuccessCount()).isEqualTo(3);
            assertThat(result.getErrorCount()).isEqualTo(0);
            assertThat(result.isSuccess()).isTrue();
        }

        @Test
        @DisplayName("Should process in parallel")
        void shouldProcessInParallel() {
            List<String> inputs = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                inputs.add("{\"id\": " + i + ", \"data\": {\"value\": " + (i * 2) + "}}");
            }

            JsonFlattener.BatchResult result = JsonFlattener.create()
                    .batch()
                    .parallel(4)
                    .fromStrings(inputs);

            assertThat(result.getSuccessCount()).isEqualTo(100);

            // Verify order is preserved
            List<Map<String, Object>> maps = result.toMaps();
            for (int i = 0; i < 100; i++) {
                assertThat(maps.get(i).get("id")).isEqualTo(i);
            }
        }

        @Test
        @DisplayName("Should handle errors with failFast false")
        void shouldHandleErrorsWithFailFastFalse() {
            List<String> inputs = Arrays.asList(
                    "{\"valid\": 1}",
                    "invalid json",
                    "{\"valid\": 2}"
            );

            JsonFlattener.BatchResult result = JsonFlattener.create()
                    .batch()
                    .failFast(false)
                    .fromStrings(inputs);

            assertThat(result.getSuccessCount()).isEqualTo(2);
            assertThat(result.getErrorCount()).isEqualTo(1);
            assertThat(result.isSuccess()).isFalse();

            JsonFlattener.BatchError error = result.getErrors().get(0);
            assertThat(error.getIndex()).isEqualTo(1);
        }

        @Test
        @DisplayName("Should throw with failFast true")
        void shouldThrowWithFailFastTrue() {
            List<String> inputs = Arrays.asList("{\"valid\": 1}", "invalid");

            assertThatThrownBy(() ->
                    JsonFlattener.create()
                            .batch()
                            .failFast(true)
                            .fromStrings(inputs)
            ).isInstanceOf(JsonFlattener.JsonFlattenException.class);
        }

        @Test
        @DisplayName("Should write batch to NDJSON file")
        void shouldWriteBatchToNdjsonFile() throws Exception {
            List<String> inputs = Arrays.asList("{\"a\":1}", "{\"a\":2}", "{\"a\":3}");
            Path outputFile = tempDir.resolve("batch-output.ndjson");

            JsonFlattener.create()
                    .batch()
                    .fromStrings(inputs)
                    .toNdjsonFile(outputFile);

            List<String> lines = Files.readAllLines(outputFile);
            assertThat(lines).hasSize(3);
        }
    }

    // ========================= STREAMING TESTS =========================

    @Nested
    @DisplayName("Streaming")
    class StreamingTests {

        @Test
        @DisplayName("Should stream from NDJSON file")
        void shouldStreamFromNdjsonFile() throws Exception {
            Path ndjsonFile = tempDir.resolve("test.ndjson");
            Files.write(ndjsonFile, Arrays.asList(
                    "{\"id\": 1, \"data\": {\"value\": \"a\"}}",
                    "{\"id\": 2, \"data\": {\"value\": \"b\"}}",
                    "{\"id\": 3, \"data\": {\"value\": \"c\"}}"
            ));

            AtomicInteger count = new AtomicInteger(0);
            JsonFlattener.create()
                    .stream()
                    .fromNdjsonFile(ndjsonFile)
                    .forEach(map -> count.incrementAndGet());

            assertThat(count.get()).isEqualTo(3);
        }

        @Test
        @DisplayName("Should stream with filter")
        void shouldStreamWithFilter() throws Exception {
            Path ndjsonFile = tempDir.resolve("test.ndjson");
            Files.write(ndjsonFile, Arrays.asList(
                    "{\"value\": 5}",
                    "{\"value\": 15}",
                    "{\"value\": 25}"
            ));

            long count = JsonFlattener.create()
                    .stream()
                    .filter(map -> ((Number) map.get("value")).intValue() > 10)
                    .fromNdjsonFile(ndjsonFile)
                    .count();

            assertThat(count).isEqualTo(2L);
        }

        @Test
        @DisplayName("Should skip errors when configured")
        void shouldSkipErrorsWhenConfigured() throws Exception {
            Path ndjsonFile = tempDir.resolve("test.ndjson");
            Files.write(ndjsonFile, Arrays.asList(
                    "{\"valid\": 1}",
                    "invalid json line",
                    "{\"valid\": 2}"
            ));

            AtomicInteger errorCount = new AtomicInteger(0);
            List<Map<String, Object>> results = JsonFlattener.create()
                    .stream()
                    .skipErrors(true)
                    .onError(err -> errorCount.incrementAndGet())
                    .fromNdjsonFile(ndjsonFile)
                    .toList();

            assertThat(results).hasSize(2);
            assertThat(errorCount.get()).isEqualTo(1);
        }

        @Test
        @DisplayName("Should throw on error when skipErrors is false")
        void shouldThrowOnErrorWhenSkipErrorsIsFalse() throws Exception {
            Path ndjsonFile = tempDir.resolve("test.ndjson");
            Files.write(ndjsonFile, Arrays.asList(
                    "{\"valid\": 1}",
                    "invalid"
            ));

            assertThatThrownBy(() ->
                    JsonFlattener.create()
                            .stream()
                            .skipErrors(false)
                            .fromNdjsonFile(ndjsonFile)
                            .count()
            ).isInstanceOf(JsonFlattener.JsonFlattenException.class);
        }

        @Test
        @DisplayName("Should stream from GZIP file")
        void shouldStreamFromGzipFile() throws Exception {
            Path gzFile = tempDir.resolve("test.ndjson.gz");
            try (GZIPOutputStream gos = new GZIPOutputStream(Files.newOutputStream(gzFile))) {
                gos.write("{\"id\": 1}\n{\"id\": 2}\n".getBytes());
            }

            long count = JsonFlattener.create()
                    .stream()
                    .fromNdjsonFile(gzFile, JsonFlattener.InputOptions.gzipped())
                    .count();

            assertThat(count).isEqualTo(2L);
        }
    }

    // ========================= BUILDER TESTS =========================

    @Nested
    @DisplayName("Builder")
    class BuilderTests {

        @Test
        @DisplayName("Should build with custom MapFlattener settings")
        void shouldBuildWithCustomMapFlattenerSettings() {
            String result = JsonFlattener.builder()
                    .maxDepth(10)
                    .arrayFormat(MapFlattener.ArraySerializationFormat.JSON)
                    .namingStrategy(MapFlattener.FieldNamingStrategy.LOWER_CASE)
                    .build()
                    .from("{\"TestKey\": 1}")
                    .toJson();

            assertThat(result).contains("testkey");
        }

        @Test
        @DisplayName("Should build with pretty print enabled")
        void shouldBuildWithPrettyPrintEnabled() {
            String result = JsonFlattener.builder()
                    .prettyPrint(true)
                    .build()
                    .from("{\"a\": 1}")
                    .toJson();

            assertThat(result).contains("\n");
        }

        @Test
        @DisplayName("Should build with custom MapFlattener")
        void shouldBuildWithCustomMapFlattener() {
            MapFlattener custom = MapFlattener.builder()
                    .useArrayBoundarySeparator(true)
                    .build();

            Map<String, Object> result = JsonFlattener.builder()
                    .mapFlattener(custom)
                    .build()
                    .from("{\"a\": {\"b\": 1}}")
                    .toMap();

            assertThat(result.containsKey("a__b")).isTrue();
        }
    }

    // ========================= QUICK STATIC METHOD TESTS =========================

    @Nested
    @DisplayName("Quick Static Methods")
    class QuickStaticMethodTests {

        @Test
        @DisplayName("Should quick flatten to Map")
        void shouldQuickFlattenToMap() {
            Map<String, Object> result = JsonFlattener.quickFlatten("{\"a\": {\"b\": 1}}");
            assertThat(result.get("a_b")).isEqualTo(1);
        }

        @Test
        @DisplayName("Should quick flatten to JSON")
        void shouldQuickFlattenToJson() {
            String result = JsonFlattener.quickFlattenToJson("{\"a\": {\"b\": 1}}");
            assertThat(result).contains("a_b");
        }

        @Test
        @DisplayName("Should quick flatten to pretty JSON")
        void shouldQuickFlattenToPrettyJson() {
            String result = JsonFlattener.quickFlattenToPrettyJson("{\"a\": 1}");
            assertThat(result).contains("\n");
        }

        @Test
        @DisplayName("Should quick flatten Map to JSON")
        void shouldQuickFlattenMapToJson() {
            Map<String, Object> input = Map.<String, Object>of("nested", Map.of("value", 42));
            String result = JsonFlattener.quickFlattenMapToJson(input);
            assertThat(result).contains("nested_value");
        }
    }

    // ========================= EDGE CASES TESTS =========================

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCaseTests {

        @Test
        @DisplayName("Should handle Unicode characters")
        void shouldHandleUnicodeCharacters() {
            String json = "{\"greeting\": \"こんにちは\", \"emoji\": \"🎉\"}";

            Map<String, Object> result = JsonFlattener.create()
                    .from(json)
                    .toMap();

            assertThat(result.get("greeting")).isEqualTo("こんにちは");
            assertThat(result.get("emoji")).isEqualTo("🎉");
        }

        @Test
        @DisplayName("Should throw when calling toMap without input")
        void shouldThrowWhenCallingToMapWithoutInput() {
            assertThatThrownBy(() ->
                    JsonFlattener.create().toMap()
            ).isInstanceOf(IllegalStateException.class);
        }

        @Test
        @DisplayName("Should handle null transformer gracefully")
        void shouldHandleNullTransformerGracefully() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"a\": 1}")
                    .transform(null)
                    .toMap();

            assertThat(result.get("a")).isEqualTo(1);
        }
    }

    // ========================= THREAD SAFETY TESTS =========================

    @Nested
    @DisplayName("Thread Safety")
    class ThreadSafetyTests {

        @Test
        @DisplayName("Should be thread-safe for concurrent operations")
        void shouldBeThreadSafeForConcurrentOperations() throws Exception {
            int numThreads = 10;
            int operationsPerThread = 100;

            List<Thread> threads = new ArrayList<>();
            List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                Thread thread = new Thread(() -> {
                    try {
                        for (int i = 0; i < operationsPerThread; i++) {
                            String json = "{\"thread\": " + threadId + ", \"op\": " + i + "}";
                            Map<String, Object> result = JsonFlattener.create()
                                    .from(json)
                                    .toMap();

                            assertThat(result.get("thread")).isEqualTo(threadId);
                            assertThat(result.get("op")).isEqualTo(i);
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                });
                threads.add(thread);
            }

            threads.forEach(Thread::start);
            for (Thread thread : threads) {
                thread.join();
            }

            assertThat(exceptions).as("Exceptions occurred: " + exceptions).isEmpty();
        }
    }
}
