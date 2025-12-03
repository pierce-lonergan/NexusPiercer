package io.github.pierce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for JsonFlattener.
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

            assertEquals(2, result.size());
            assertEquals("John", result.get("user_name"));
            assertEquals(30, result.get("user_age"));
        }

        @Test
        @DisplayName("Should flatten deeply nested JSON")
        void shouldFlattenDeeplyNestedJson() {
            String json = "{\"a\": {\"b\": {\"c\": {\"d\": {\"e\": \"deep\"}}}}}";

            Map<String, Object> result = JsonFlattener.create()
                    .from(json)
                    .toMap();

            assertEquals(1, result.size());
            assertEquals("deep", result.get("a_b_c_d_e"));
        }

        @Test
        @DisplayName("Should flatten JSON with arrays")
        void shouldFlattenJsonWithArrays() {
            String json = "{\"users\": [{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]}";

            Map<String, Object> result = JsonFlattener.create()
                    .from(json)
                    .toMap();

            assertNotNull(result.get("users_name"));
            assertTrue(result.get("users_name").toString().contains("Alice"));
            assertTrue(result.get("users_name").toString().contains("Bob"));
        }

        @Test
        @DisplayName("Should flatten JSON with mixed types")
        void shouldFlattenJsonWithMixedTypes() {
            String json = "{\"string\": \"value\", \"number\": 42, \"boolean\": true, \"null\": null}";

            Map<String, Object> result = JsonFlattener.create()
                    .from(json)
                    .toMap();

            assertEquals("value", result.get("string"));
            assertEquals(42, result.get("number"));
            assertEquals(true, result.get("boolean"));
            assertNull(result.get("null"));
        }

        @Test
        @DisplayName("Should handle empty JSON object")
        void shouldHandleEmptyJsonObject() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{}")
                    .toMap();

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should handle flat JSON without nesting")
        void shouldHandleFlatJson() {
            String json = "{\"a\": 1, \"b\": 2, \"c\": 3}";

            Map<String, Object> result = JsonFlattener.create()
                    .from(json)
                    .toMap();

            assertEquals(3, result.size());
            assertEquals(1, result.get("a"));
            assertEquals(2, result.get("b"));
            assertEquals(3, result.get("c"));
        }



        @Test
        @DisplayName("Should preserve key order")
        void shouldPreserveKeyOrder() {
            String json = "{\"z\": 1, \"a\": 2, \"m\": 3}";

            Map<String, Object> result = JsonFlattener.create()
                    .from(json)
                    .toMap();

            List<String> keys = new ArrayList<>(result.keySet());
            assertEquals(Arrays.asList("z", "a", "m"), keys);
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

            assertEquals("value", result.get("key"));
            assertEquals(123, result.get("nested_inner"));
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

            assertEquals("Alice", result.get("name"));
            assertEquals(25, result.get("details_age"));
        }

        @Test
        @DisplayName("Should read from byte array")
        void shouldReadFromByteArray() {
            byte[] bytes = testJson.getBytes(StandardCharsets.UTF_8);

            Map<String, Object> result = JsonFlattener.create()
                    .from(bytes)
                    .toMap();

            assertEquals("value", result.get("key"));
        }

        @Test
        @DisplayName("Should read from byte array with charset")
        void shouldReadFromByteArrayWithCharset() {
            byte[] bytes = testJson.getBytes(StandardCharsets.UTF_16);

            Map<String, Object> result = JsonFlattener.create()
                    .from(bytes, StandardCharsets.UTF_16)
                    .toMap();

            assertEquals("value", result.get("key"));
        }

        @Test
        @DisplayName("Should read from InputStream")
        void shouldReadFromInputStream() {
            InputStream is = new ByteArrayInputStream(testJson.getBytes());

            Map<String, Object> result = JsonFlattener.create()
                    .from(is)
                    .toMap();

            assertEquals("value", result.get("key"));
        }

        @Test
        @DisplayName("Should read from Reader")
        void shouldReadFromReader() {
            Reader reader = new StringReader(testJson);

            Map<String, Object> result = JsonFlattener.create()
                    .from(reader)
                    .toMap();

            assertEquals("value", result.get("key"));
        }

        @Test
        @DisplayName("Should read from File")
        void shouldReadFromFile() throws Exception {
            Path file = tempDir.resolve("test.json");
            Files.writeString(file, testJson);

            Map<String, Object> result = JsonFlattener.create()
                    .from(file.toFile())
                    .toMap();

            assertEquals("value", result.get("key"));
        }

        @Test
        @DisplayName("Should read from Path")
        void shouldReadFromPath() throws Exception {
            Path file = tempDir.resolve("test.json");
            Files.writeString(file, testJson);

            Map<String, Object> result = JsonFlattener.create()
                    .from(file)
                    .toMap();

            assertEquals("value", result.get("key"));
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

            assertEquals("value", result.get("key"));
        }

        @Test
        @DisplayName("Should read from JsonNode")
        void shouldReadFromJsonNode() throws Exception {
            JsonNode node = MAPPER.readTree(testJson);

            Map<String, Object> result = JsonFlattener.create()
                    .from(node)
                    .toMap();

            assertEquals("value", result.get("key"));
        }

        @Test
        @DisplayName("Should throw on invalid JSON")
        void shouldThrowOnInvalidJson() {
            assertThrows(JsonFlattener.JsonFlattenException.class, () ->
                    JsonFlattener.create()
                            .from("not valid json")
                            .toMap()
            );
        }

        @Test
        @DisplayName("Should throw on missing file")
        void shouldThrowOnMissingFile() {
            Path nonExistent = tempDir.resolve("does-not-exist.json");

            assertThrows(JsonFlattener.JsonFlattenException.class, () ->
                    JsonFlattener.create()
                            .from(nonExistent)
                            .toMap()
            );
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

            assertNotNull(result);
            assertTrue(result.contains("\"a\":1") || result.contains("\"a\": 1"));
            assertTrue(result.contains("\"b_c\":2") || result.contains("\"b_c\": 2"));
        }

        @Test
        @DisplayName("Should output to pretty JSON string")
        void shouldOutputToPrettyJsonString() {
            String result = JsonFlattener.create()
                    .from(testJson)
                    .toPrettyJson();

            assertTrue(result.contains("\n"));
            assertTrue(result.contains("  "));
        }

        @Test
        @DisplayName("Should output to Map")
        void shouldOutputToMap() {
            Map<String, Object> result = JsonFlattener.create()
                    .from(testJson)
                    .toMap();

            assertEquals(2, result.size());
            assertEquals(1, result.get("a"));
            assertEquals(2, result.get("b_c"));
        }

        @Test
        @DisplayName("Should output to bytes")
        void shouldOutputToBytes() {
            byte[] result = JsonFlattener.create()
                    .from(testJson)
                    .toBytes();

            assertNotNull(result);
            assertTrue(result.length > 0);

            String asString = new String(result, StandardCharsets.UTF_8);
            assertTrue(asString.contains("a"));
        }

        @Test
        @DisplayName("Should output to JsonNode")
        void shouldOutputToJsonNode() {
            JsonNode result = JsonFlattener.create()
                    .from(testJson)
                    .toJsonNode();

            assertNotNull(result);
            assertTrue(result.has("a"));
            assertTrue(result.has("b_c"));
        }

        @Test
        @DisplayName("Should output to File")
        void shouldOutputToFile() throws Exception {
            Path file = tempDir.resolve("output.json");

            JsonFlattener.create()
                    .from(testJson)
                    .toFile(file.toFile());

            assertTrue(Files.exists(file));
            String content = Files.readString(file);
            assertTrue(content.contains("a"));
        }

        @Test
        @DisplayName("Should output to Path")
        void shouldOutputToPath() throws Exception {
            Path file = tempDir.resolve("output.json");

            JsonFlattener.create()
                    .from(testJson)
                    .toFile(file);

            assertTrue(Files.exists(file));
        }

        @Test
        @DisplayName("Should output to OutputStream")
        void shouldOutputToOutputStream() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            JsonFlattener.create()
                    .from(testJson)
                    .toStream(baos);

            assertTrue(baos.size() > 0);
            assertTrue(baos.toString().contains("a"));
        }

        @Test
        @DisplayName("Should output to Writer")
        void shouldOutputToWriter() {
            StringWriter writer = new StringWriter();

            JsonFlattener.create()
                    .from(testJson)
                    .toWriter(writer);

            assertTrue(writer.toString().contains("a"));
        }

        @Test
        @DisplayName("Should output to GZIP file")
        void shouldOutputToGzipFile() throws Exception {
            Path file = tempDir.resolve("output.json.gz");

            JsonFlattener.create()
                    .from(testJson)
                    .toFile(file, JsonFlattener.OutputOptions.gzipped());

            assertTrue(Files.exists(file));

            // Verify it's actually gzipped
            try (GZIPInputStream gis = new GZIPInputStream(Files.newInputStream(file))) {
                String content = new String(gis.readAllBytes());
                assertTrue(content.contains("a"));
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

            assertTrue(aPos < mPos);
            assertTrue(mPos < zPos);
        }

        @Test
        @DisplayName("Should output without nulls when specified")
        void shouldOutputWithoutNulls() {
            String json = "{\"a\": 1, \"b\": null}";

            String result = JsonFlattener.create()
                    .from(json)
                    .toJson(JsonFlattener.OutputOptions.builder().includeNulls(false).build());

            assertFalse(result.contains("\"b\""));
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

            assertEquals(2, result.size());
            assertEquals(1, result.get("a"));
            assertEquals(2, result.get("b"));
        }

        @Test
        @DisplayName("Should remove field")
        void shouldRemoveField() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"a\": 1, \"b\": 2}")
                    .removeField("a")
                    .toMap();

            assertEquals(1, result.size());
            assertFalse(result.containsKey("a"));
            assertTrue(result.containsKey("b"));
        }

        @Test
        @DisplayName("Should rename field")
        void shouldRenameField() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"oldName\": \"value\"}")
                    .renameField("oldName", "newName")
                    .toMap();

            assertFalse(result.containsKey("oldName"));
            assertEquals("value", result.get("newName"));
        }

        @Test
        @DisplayName("Should include only specified fields")
        void shouldIncludeOnlySpecifiedFields() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"a\": 1, \"b\": 2, \"c\": 3}")
                    .includeOnly("a", "c")
                    .toMap();

            assertEquals(2, result.size());
            assertTrue(result.containsKey("a"));
            assertTrue(result.containsKey("c"));
            assertFalse(result.containsKey("b"));
        }

        @Test
        @DisplayName("Should prefix all keys")
        void shouldPrefixAllKeys() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"a\": 1, \"b\": 2}")
                    .prefixKeys("data_")
                    .toMap();

            assertTrue(result.containsKey("data_a"));
            assertTrue(result.containsKey("data_b"));
            assertFalse(result.containsKey("a"));
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

            assertEquals(20, result.get("value"));
            assertEquals(100, result.get("computed"));
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

            assertDoesNotThrow({
                JsonFlattener.create()
                        .from("{\"id\": 1, \"name\": \"test\"}")
                        .validate(rules)
                        .toMap()
            } as org.junit.jupiter.api.function.Executable);
        }

        @Test
        @DisplayName("Should fail validation with missing required fields")
        void shouldFailValidationWithMissingRequiredFields() {
            JsonFlattener.ValidationRules rules = JsonFlattener.ValidationRules.builder()
                    .requireFields("id", "name")
                    .build();

            JsonFlattener.JsonValidationException ex = assertThrows(
                    JsonFlattener.JsonValidationException.class, () ->
                    JsonFlattener.create()
                            .from("{\"id\": 1}")
                            .validate(rules)
                            .toMap()
            );

            assertTrue(ex.getViolations().get(0).contains("name"));
        }

        @Test
        @DisplayName("Should fail validation with too many fields")
        void shouldFailValidationWithTooManyFields() {
            JsonFlattener.ValidationRules rules = JsonFlattener.ValidationRules.builder()
                    .maxFields(2)
                    .build();

            assertThrows(JsonFlattener.JsonValidationException.class, () ->
                    JsonFlattener.create()
                            .from("{\"a\": 1, \"b\": 2, \"c\": 3}")
                            .validate(rules)
                            .toMap()
            );
        }

        @Test
        @DisplayName("Should filter with predicate")
        void shouldFilterWithPredicate() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"value\": 10}")
                    .filter(map -> (int) map.get("value") > 5)
                    .toMap();

            assertFalse(result.isEmpty());

            Map<String, Object> filtered = JsonFlattener.create()
                    .from("{\"value\": 3}")
                    .filter(map -> (int) map.get("value") > 5)
                    .toMap();

            assertTrue(filtered.isEmpty());
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

            assertEquals(3, result.getSuccessCount());
            assertEquals(0, result.getErrorCount());
            assertTrue(result.isSuccess());
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

            assertEquals(100, result.getSuccessCount());

            // Verify order is preserved
            List<Map<String, Object>> maps = result.toMaps();
            for (int i = 0; i < 100; i++) {
                assertEquals(i, maps.get(i).get("id"));
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

            assertEquals(2, result.getSuccessCount());
            assertEquals(1, result.getErrorCount());
            assertFalse(result.isSuccess());

            JsonFlattener.BatchError error = result.getErrors().get(0);
            assertEquals(1, error.getIndex());
        }

        @Test
        @DisplayName("Should throw with failFast true")
        void shouldThrowWithFailFastTrue() {
            List<String> inputs = Arrays.asList("{\"valid\": 1}", "invalid");

            assertThrows(JsonFlattener.JsonFlattenException.class, () ->
                    JsonFlattener.create()
                            .batch()
                            .failFast(true)
                            .fromStrings(inputs)
            );
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
            assertEquals(3, lines.size());
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

            assertEquals(3, count.get());
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

            assertEquals(2, count);
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

            assertEquals(2, results.size());
            assertEquals(1, errorCount.get());
        }

        @Test
        @DisplayName("Should throw on error when skipErrors is false")
        void shouldThrowOnErrorWhenSkipErrorsIsFalse() throws Exception {
            Path ndjsonFile = tempDir.resolve("test.ndjson");
            Files.write(ndjsonFile, Arrays.asList(
                    "{\"valid\": 1}",
                    "invalid"
            ));

            assertThrows(JsonFlattener.JsonFlattenException.class, () ->
                    JsonFlattener.create()
                            .stream()
                            .skipErrors(false)
                            .fromNdjsonFile(ndjsonFile)
                            .count()
            );
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

            assertEquals(2, count);
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

            assertTrue(result.contains("testkey"));
        }

        @Test
        @DisplayName("Should build with pretty print enabled")
        void shouldBuildWithPrettyPrintEnabled() {
            String result = JsonFlattener.builder()
                    .prettyPrint(true)
                    .build()
                    .from("{\"a\": 1}")
                    .toJson();

            assertTrue(result.contains("\n"));
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

            assertTrue(result.containsKey("a__b"));
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
            assertEquals(1, result.get("a_b"));
        }

        @Test
        @DisplayName("Should quick flatten to JSON")
        void shouldQuickFlattenToJson() {
            String result = JsonFlattener.quickFlattenToJson("{\"a\": {\"b\": 1}}");
            assertTrue(result.contains("a_b"));
        }

        @Test
        @DisplayName("Should quick flatten to pretty JSON")
        void shouldQuickFlattenToPrettyJson() {
            String result = JsonFlattener.quickFlattenToPrettyJson("{\"a\": 1}");
            assertTrue(result.contains("\n"));
        }

        @Test
        @DisplayName("Should quick flatten Map to JSON")
        void shouldQuickFlattenMapToJson() {
            Map<String, Object> input = Map.of("nested", Map.of("value", 42));
            String result = JsonFlattener.quickFlattenMapToJson(input);
            assertTrue(result.contains("nested_value"));
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

            assertEquals("こんにちは", result.get("greeting"));
            assertEquals("🎉", result.get("emoji"));
        }

        @Test
        @DisplayName("Should throw when calling toMap without input")
        void shouldThrowWhenCallingToMapWithoutInput() {
            assertThrows(IllegalStateException.class, () ->
                    JsonFlattener.create().toMap()
            );
        }

        @Test
        @DisplayName("Should handle null transformer gracefully")
        void shouldHandleNullTransformerGracefully() {
            Map<String, Object> result = JsonFlattener.create()
                    .from("{\"a\": 1}")
                    .transform(null)
                    .toMap();

            assertEquals(1, result.get("a"));
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

                            assertEquals(threadId, result.get("thread"));
                            assertEquals(i, result.get("op"));
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

            assertTrue(exceptions.isEmpty(), "Exceptions occurred: " + exceptions);
        }
    }
}