//package io.github.pierce;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.junit.jupiter.api.*;
//import org.junit.jupiter.api.io.TempDir;
//
//import java.io.*;
//import java.nio.file.*;
//import java.util.*;
//
//import static org.junit.jupiter.api.Assertions.*;
//
///**
// * Comprehensive test suite for JsonReconstructor.
// *
// * @author Pierce
// */
//@DisplayName("JsonReconstructor Tests")
//class JsonReconstructorTest {
//
//    private static final ObjectMapper MAPPER = new ObjectMapper();
//
//    @TempDir
//    Path tempDir;
//
//    // ========================= BASIC RECONSTRUCTION TESTS =========================
//
//    @Nested
//    @DisplayName("Basic Reconstruction")
//    class BasicReconstructionTests {
//
//        @Test
//        @DisplayName("Should reconstruct simple nested structure")
//        void shouldReconstructSimpleNestedStructure() {
//            // Flattened: {user_name: "John", user_age: 30}
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("user_name", "John");
//            flattened.put("user_age", 30);
//
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(flattened);
//
//            assertNotNull(result.get("user"));
//            assertTrue(result.get("user") instanceof Map);
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> user = (Map<String, Object>) result.get("user");
//            assertEquals("John", user.get("name"));
//            assertEquals(30, user.get("age"));
//        }
//
//        @Test
//        @DisplayName("Should reconstruct deeply nested structure")
//        void shouldReconstructDeeplyNestedStructure() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("a_b_c_d_e", "deep");
//
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(flattened);
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> a = (Map<String, Object>) result.get("a");
//            @SuppressWarnings("unchecked")
//            Map<String, Object> b = (Map<String, Object>) a.get("b");
//            @SuppressWarnings("unchecked")
//            Map<String, Object> c = (Map<String, Object>) b.get("c");
//            @SuppressWarnings("unchecked")
//            Map<String, Object> d = (Map<String, Object>) c.get("d");
//
//            assertEquals("deep", d.get("e"));
//        }
//
//        @Test
//        @DisplayName("Should handle flat structure without nesting")
//        void shouldHandleFlatStructure() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("a", 1);
//            flattened.put("b", 2);
//            flattened.put("c", 3);
//
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(flattened);
//
//            assertEquals(3, result.size());
//            assertEquals(1, result.get("a"));
//            assertEquals(2, result.get("b"));
//            assertEquals(3, result.get("c"));
//        }
//
//        @Test
//        @DisplayName("Should handle empty map")
//        void shouldHandleEmptyMap() {
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(new LinkedHashMap<>());
//            assertTrue(result.isEmpty());
//        }
//
//        @Test
//        @DisplayName("Should handle null map")
//        void shouldHandleNullMap() {
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(null);
//            assertTrue(result.isEmpty());
//        }
//
//        @Test
//        @DisplayName("Should preserve null values")
//        void shouldPreserveNullValues() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("user_name", "John");
//            flattened.put("user_email", null);
//
//            Map<String, Object> result = JsonReconstructor.builder()
//                    .preserveNulls(true)
//                    .build()
//                    .reconstruct(flattened);
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> user = (Map<String, Object>) result.get("user");
//            assertEquals("John", user.get("name"));
//            assertTrue(user.containsKey("email"));
//            assertNull(user.get("email"));
//        }
//
//        @Test
//        @DisplayName("Should handle mixed types")
//        void shouldHandleMixedTypes() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("data_string", "value");
//            flattened.put("data_number", 42);
//            flattened.put("data_boolean", true);
//            flattened.put("data_decimal", 3.14);
//
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(flattened);
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> data = (Map<String, Object>) result.get("data");
//            assertEquals("value", data.get("string"));
//            assertEquals(42, data.get("number"));
//            assertEquals(true, data.get("boolean"));
//            assertEquals(3.14, data.get("decimal"));
//        }
//    }
//
//    // ========================= ARRAY RECONSTRUCTION TESTS =========================
//
//    @Nested
//    @DisplayName("Array Reconstruction")
//    class ArrayReconstructionTests {
//
//        @Test
//        @DisplayName("Should reconstruct array from JSON serialized values")
//        void shouldReconstructArrayFromJsonValues() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("users_name", "[\"Alice\",\"Bob\",\"Charlie\"]");
//            flattened.put("users_age", "[30,25,35]");
//
//            JsonReconstructor reconstructor = JsonReconstructor.builder()
//                    .arrayPaths("users")
//                    .arrayFormat(JsonReconstructor.ArraySerializationFormat.JSON)
//                    .build();
//
//            Map<String, Object> result = reconstructor.reconstruct(flattened);
//
//            assertTrue(result.get("users") instanceof List);
//            @SuppressWarnings("unchecked")
//            List<Map<String, Object>> users = (List<Map<String, Object>>) result.get("users");
//
//            assertEquals(3, users.size());
//            assertEquals("Alice", users.get(0).get("name"));
//            assertEquals(30, users.get(0).get("age"));
//            assertEquals("Bob", users.get(1).get("name"));
//            assertEquals(25, users.get(1).get("age"));
//            assertEquals("Charlie", users.get(2).get("name"));
//            assertEquals(35, users.get(2).get("age"));
//        }
//
//        @Test
//        @DisplayName("Should reconstruct array from comma separated values")
//        void shouldReconstructArrayFromCommaSeparatedValues() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("tags_value", "java,groovy,kotlin");
//
//            JsonReconstructor reconstructor = JsonReconstructor.builder()
//                    .arrayPaths("tags")
//                    .arrayFormat(JsonReconstructor.ArraySerializationFormat.COMMA_SEPARATED)
//                    .build();
//
//            Map<String, Object> result = reconstructor.reconstruct(flattened);
//
//            assertTrue(result.get("tags") instanceof List);
//            @SuppressWarnings("unchecked")
//            List<Map<String, Object>> tags = (List<Map<String, Object>>) result.get("tags");
//
//            assertEquals(3, tags.size());
//        }
//
//        @Test
//        @DisplayName("Should reconstruct array from pipe separated values")
//        void shouldReconstructArrayFromPipeSeparatedValues() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("items_id", "1|2|3");
//            flattened.put("items_name", "A|B|C");
//
//            JsonReconstructor reconstructor = JsonReconstructor.builder()
//                    .arrayPaths("items")
//                    .arrayFormat(JsonReconstructor.ArraySerializationFormat.PIPE_SEPARATED)
//                    .build();
//
//            Map<String, Object> result = reconstructor.reconstruct(flattened);
//
//            assertTrue(result.get("items") instanceof List);
//            @SuppressWarnings("unchecked")
//            List<Map<String, Object>> items = (List<Map<String, Object>>) result.get("items");
//
//            assertEquals(3, items.size());
//        }
//
//        @Test
//        @DisplayName("Should auto-detect arrays from values")
//        void shouldAutoDetectArraysFromValues() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("scores", "[100,200,300]");
//
//            JsonReconstructor reconstructor = JsonReconstructor.builder()
//                    .inferArraysFromValues(true)
//                    .build();
//
//            Map<String, Object> result = reconstructor.reconstruct(flattened);
//
//            // When inferred as primitive array, value should be parsed
//            Object scores = result.get("scores");
//            assertTrue(scores instanceof List || scores instanceof String);
//        }
//    }
//
//    // ========================= SEPARATOR TESTS =========================
//
//    @Nested
//    @DisplayName("Separator Handling")
//    class SeparatorTests {
//
//        @Test
//        @DisplayName("Should use default underscore separator")
//        void shouldUseDefaultUnderscoreSeparator() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("user_profile_name", "John");
//
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(flattened);
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> user = (Map<String, Object>) result.get("user");
//            @SuppressWarnings("unchecked")
//            Map<String, Object> profile = (Map<String, Object>) user.get("profile");
//            assertEquals("John", profile.get("name"));
//        }
//
//        @Test
//        @DisplayName("Should use double underscore separator")
//        void shouldUseDoubleUnderscoreSeparator() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("user__profile__name", "John");
//
//            Map<String, Object> result = JsonReconstructor.builder()
//                    .separator("__")
//                    .build()
//                    .reconstruct(flattened);
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> user = (Map<String, Object>) result.get("user");
//            @SuppressWarnings("unchecked")
//            Map<String, Object> profile = (Map<String, Object>) user.get("profile");
//            assertEquals("John", profile.get("name"));
//        }
//
//        @Test
//        @DisplayName("Should use array boundary separator mode")
//        void shouldUseArrayBoundarySeparatorMode() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("a__b__c", "value");
//
//            Map<String, Object> result = JsonReconstructor.withArrayBoundarySeparator()
//                    .from(flattened)
//                    .toMap();
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> a = (Map<String, Object>) result.get("a");
//            @SuppressWarnings("unchecked")
//            Map<String, Object> b = (Map<String, Object>) a.get("b");
//            assertEquals("value", b.get("c"));
//        }
//
//        @Test
//        @DisplayName("Should handle custom separator")
//        void shouldHandleCustomSeparator() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("user.profile.name", "John");
//
//            Map<String, Object> result = JsonReconstructor.builder()
//                    .separator(".")
//                    .build()
//                    .reconstruct(flattened);
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> user = (Map<String, Object>) result.get("user");
//            @SuppressWarnings("unchecked")
//            Map<String, Object> profile = (Map<String, Object>) user.get("profile");
//            assertEquals("John", profile.get("name"));
//        }
//    }
//
//    // ========================= FLUENT API TESTS =========================
//
//    @Nested
//    @DisplayName("Fluent API")
//    class FluentApiTests {
//
//        @Test
//        @DisplayName("Should use fluent API with from()")
//        void shouldUseFluentApiWithFrom() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("user_name", "Alice");
//
//            Map<String, Object> result = JsonReconstructor.create()
//                    .from(flattened)
//                    .toMap();
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> user = (Map<String, Object>) result.get("user");
//            assertEquals("Alice", user.get("name"));
//        }
//
//        @Test
//        @DisplayName("Should use fluent API with fromJson()")
//        void shouldUseFluentApiWithFromJson() {
//            String flattenedJson = "{\"user_name\":\"Bob\",\"user_age\":25}";
//
//            Map<String, Object> result = JsonReconstructor.create()
//                    .fromJson(flattenedJson)
//                    .toMap();
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> user = (Map<String, Object>) result.get("user");
//            assertEquals("Bob", user.get("name"));
//            assertEquals(25, user.get("age"));
//        }
//
//        @Test
//        @DisplayName("Should apply transformation")
//        void shouldApplyTransformation() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("value", 10);
//
//            Map<String, Object> result = JsonReconstructor.create()
//                    .from(flattened)
//                    .transform(map -> {
//                        map.put("reconstructed", true);
//                        return map;
//                    })
//                    .toMap();
//
//            assertEquals(10, result.get("value"));
//            assertEquals(true, result.get("reconstructed"));
//        }
//
//        @Test
//        @DisplayName("Should output to JSON")
//        void shouldOutputToJson() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("user_name", "Test");
//
//            String json = JsonReconstructor.create()
//                    .from(flattened)
//                    .toJson();
//
//            assertTrue(json.contains("user"));
//            assertTrue(json.contains("name"));
//            assertTrue(json.contains("Test"));
//        }
//
//        @Test
//        @DisplayName("Should output to pretty JSON")
//        void shouldOutputToPrettyJson() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("user_name", "Test");
//
//            String json = JsonReconstructor.create()
//                    .from(flattened)
//                    .toPrettyJson();
//
//            assertTrue(json.contains("\n"));
//            assertTrue(json.contains("user"));
//        }
//
//        @Test
//        @DisplayName("Should throw when calling toMap without input")
//        void shouldThrowWhenCallingToMapWithoutInput() {
//            assertThrows(IllegalStateException.class, () ->
//                    JsonReconstructor.create().toMap()
//            );
//        }
//    }
//
//    // ========================= FILE I/O TESTS =========================
//
//    @Nested
//    @DisplayName("File I/O")
//    class FileIoTests {
//
//        @Test
//        @DisplayName("Should read from file")
//        void shouldReadFromFile() throws Exception {
//            Path inputFile = tempDir.resolve("input.json");
//            Files.writeString(inputFile, "{\"user_name\":\"FileUser\"}");
//
//            Map<String, Object> result = JsonReconstructor.create()
//                    .fromFile(inputFile)
//                    .toMap();
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> user = (Map<String, Object>) result.get("user");
//            assertEquals("FileUser", user.get("name"));
//        }
//
//        @Test
//        @DisplayName("Should write to file")
//        void shouldWriteToFile() throws Exception {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("user_name", "OutputUser");
//
//            Path outputFile = tempDir.resolve("output.json");
//
//            JsonReconstructor.create()
//                    .from(flattened)
//                    .toFile(outputFile);
//
//            String content = Files.readString(outputFile);
//            assertTrue(content.contains("user"));
//            assertTrue(content.contains("OutputUser"));
//        }
//    }
//
//    // ========================= VERIFICATION TESTS =========================
//
//    @Nested
//    @DisplayName("Verification")
//    class VerificationTests {
//
//        @Test
//        @DisplayName("Should verify perfect reconstruction")
//        void shouldVerifyPerfectReconstruction() {
//            Map<String, Object> original = new LinkedHashMap<>();
//            Map<String, Object> user = new LinkedHashMap<>();
//            user.put("name", "John");
//            user.put("age", 30);
//            original.put("user", user);
//
//            Map<String, Object> reconstructed = new LinkedHashMap<>();
//            Map<String, Object> user2 = new LinkedHashMap<>();
//            user2.put("name", "John");
//            user2.put("age", 30);
//            reconstructed.put("user", user2);
//
//            JsonReconstructor reconstructor = JsonReconstructor.builder().build();
//            JsonReconstructor.ReconstructionVerification verification =
//                    reconstructor.verify(original, reconstructed);
//
//            assertTrue(verification.isPerfect());
//            assertTrue(verification.getDifferences().isEmpty());
//        }
//
//        @Test
//        @DisplayName("Should detect differences")
//        void shouldDetectDifferences() {
//            Map<String, Object> original = new LinkedHashMap<>();
//            original.put("value", 100);
//
//            Map<String, Object> reconstructed = new LinkedHashMap<>();
//            reconstructed.put("value", 200);
//
//            JsonReconstructor reconstructor = JsonReconstructor.builder().build();
//            JsonReconstructor.ReconstructionVerification verification =
//                    reconstructor.verify(original, reconstructed);
//
//            assertFalse(verification.isPerfect());
//            assertFalse(verification.getDifferences().isEmpty());
//        }
//
//        @Test
//        @DisplayName("Should detect missing fields")
//        void shouldDetectMissingFields() {
//            Map<String, Object> original = new LinkedHashMap<>();
//            original.put("a", 1);
//            original.put("b", 2);
//
//            Map<String, Object> reconstructed = new LinkedHashMap<>();
//            reconstructed.put("a", 1);
//
//            JsonReconstructor reconstructor = JsonReconstructor.builder().build();
//            JsonReconstructor.ReconstructionVerification verification =
//                    reconstructor.verify(original, reconstructed);
//
//            assertFalse(verification.isPerfect());
//            assertTrue(verification.getDifferences().stream()
//                    .anyMatch(d -> d.contains("b")));
//        }
//
//        @Test
//        @DisplayName("Should generate verification report")
//        void shouldGenerateVerificationReport() {
//            Map<String, Object> original = new LinkedHashMap<>();
//            original.put("test", "value");
//
//            JsonReconstructor reconstructor = JsonReconstructor.builder().build();
//            JsonReconstructor.ReconstructionVerification verification =
//                    reconstructor.verify(original, original);
//
//            String report = verification.getReport();
//            assertNotNull(report);
//            assertTrue(report.contains("Verification"));
//            assertTrue(report.contains("PERFECT"));
//        }
//    }
//
//    // ========================= ROUND TRIP TESTS =========================
//
//    @Nested
//    @DisplayName("Round Trip (Flatten -> Reconstruct)")
//    class RoundTripTests {
//
//        @Test
//        @DisplayName("Should round trip simple nested object")
//        void shouldRoundTripSimpleNestedObject() {
//            Map<String, Object> original = new LinkedHashMap<>();
//            Map<String, Object> user = new LinkedHashMap<>();
//            user.put("name", "John");
//            user.put("age", 30);
//            original.put("user", user);
//
//            MapFlattener flattener = MapFlattener.builder().build();
//            JsonReconstructor reconstructor = JsonReconstructor.builder().build();
//
//            Map<String, Object> flattened = flattener.flatten(original);
//            Map<String, Object> reconstructed = reconstructor.reconstruct(flattened);
//
//            JsonReconstructor.ReconstructionVerification verification =
//                    reconstructor.verify(original, reconstructed);
//
//            assertTrue(verification.isPerfect(), "Round trip failed: " + verification.getReport());
//        }
//
//        @Test
//        @DisplayName("Should round trip deeply nested object")
//        void shouldRoundTripDeeplyNestedObject() {
//            Map<String, Object> original = new LinkedHashMap<>();
//            Map<String, Object> level1 = new LinkedHashMap<>();
//            Map<String, Object> level2 = new LinkedHashMap<>();
//            Map<String, Object> level3 = new LinkedHashMap<>();
//            level3.put("deep", "value");
//            level2.put("c", level3);
//            level1.put("b", level2);
//            original.put("a", level1);
//
//            MapFlattener flattener = MapFlattener.builder().build();
//            JsonReconstructor reconstructor = JsonReconstructor.builder().build();
//
//            Map<String, Object> flattened = flattener.flatten(original);
//            Map<String, Object> reconstructed = reconstructor.reconstruct(flattened);
//
//            JsonReconstructor.ReconstructionVerification verification =
//                    reconstructor.verify(original, reconstructed);
//
//            assertTrue(verification.isPerfect(), "Round trip failed: " + verification.getReport());
//        }
//
//        @Test
//        @DisplayName("Should round trip with array boundary separator")
//        void shouldRoundTripWithArrayBoundarySeparator() {
//            Map<String, Object> original = new LinkedHashMap<>();
//            Map<String, Object> data = new LinkedHashMap<>();
//            data.put("value", 42);
//            original.put("nested", data);
//
//            MapFlattener flattener = MapFlattener.builder()
//                    .useArrayBoundarySeparator(true)
//                    .build();
//            JsonReconstructor reconstructor = JsonReconstructor.builder()
//                    .useArrayBoundarySeparator(true)
//                    .build();
//
//            Map<String, Object> flattened = flattener.flatten(original);
//            Map<String, Object> reconstructed = reconstructor.reconstruct(flattened);
//
//            JsonReconstructor.ReconstructionVerification verification =
//                    reconstructor.verify(original, reconstructed);
//
//            assertTrue(verification.isPerfect(), "Round trip failed: " + verification.getReport());
//        }
//    }
//
//    // ========================= EDGE CASES TESTS =========================
//
//    @Nested
//    @DisplayName("Edge Cases")
//    class EdgeCaseTests {
//
//        @Test
//        @DisplayName("Should handle Unicode characters")
//        void shouldHandleUnicodeCharacters() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("greeting_japanese", "こんにちは");
//            flattened.put("greeting_emoji", "🎉");
//
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(flattened);
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> greeting = (Map<String, Object>) result.get("greeting");
//            assertEquals("こんにちは", greeting.get("japanese"));
//            assertEquals("🎉", greeting.get("emoji"));
//        }
//
//        @Test
//        @DisplayName("Should handle keys with numbers")
//        void shouldHandleKeysWithNumbers() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("item1_value", "first");
//            flattened.put("item2_value", "second");
//
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(flattened);
//
//            assertNotNull(result.get("item1"));
//            assertNotNull(result.get("item2"));
//        }
//
//        @Test
//        @DisplayName("Should handle empty string values")
//        void shouldHandleEmptyStringValues() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("user_name", "");
//
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(flattened);
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> user = (Map<String, Object>) result.get("user");
//            assertEquals("", user.get("name"));
//        }
//
//        @Test
//        @DisplayName("Should handle special characters in values")
//        void shouldHandleSpecialCharactersInValues() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("data_query", "SELECT * FROM users WHERE name = 'John'");
//
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(flattened);
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> data = (Map<String, Object>) result.get("data");
//            assertEquals("SELECT * FROM users WHERE name = 'John'", data.get("query"));
//        }
//
//        @Test
//        @DisplayName("Should handle numeric string keys")
//        void shouldHandleNumericStringKeys() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("data_123", "numeric key");
//
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(flattened);
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> data = (Map<String, Object>) result.get("data");
//            assertEquals("numeric key", data.get("123"));
//        }
//    }
//
//    // ========================= QUICK METHODS TESTS =========================
//
//    @Nested
//    @DisplayName("Quick Static Methods")
//    class QuickMethodsTests {
//
//        @Test
//        @DisplayName("Should quick reconstruct")
//        void shouldQuickReconstruct() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("a_b", 1);
//
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(flattened);
//
//            assertNotNull(result.get("a"));
//        }
//
//        @Test
//        @DisplayName("Should quick reconstruct with separator")
//        void shouldQuickReconstructWithSeparator() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("a__b", 1);
//
//            Map<String, Object> result = JsonReconstructor.quickReconstruct(flattened, "__");
//
//            @SuppressWarnings("unchecked")
//            Map<String, Object> a = (Map<String, Object>) result.get("a");
//            assertEquals(1, a.get("b"));
//        }
//
//        @Test
//        @DisplayName("Should quick reconstruct to JSON")
//        void shouldQuickReconstructToJson() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("test", "value");
//
//            String json = JsonReconstructor.quickReconstructToJson(flattened);
//
//            assertNotNull(json);
//            assertTrue(json.contains("test"));
//            assertTrue(json.contains("value"));
//        }
//
//        @Test
//        @DisplayName("Should quick reconstruct to pretty JSON")
//        void shouldQuickReconstructToPrettyJson() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("test", "value");
//
//            String json = JsonReconstructor.quickReconstructToPrettyJson(flattened);
//
//            assertNotNull(json);
//            // Pretty JSON typically contains newlines
//            assertTrue(json.contains("test"));
//        }
//    }
//
//    // ========================= BUILDER TESTS =========================
//
//    @Nested
//    @DisplayName("Builder Configuration")
//    class BuilderTests {
//
//        @Test
//        @DisplayName("Should build with all options")
//        void shouldBuildWithAllOptions() {
//            JsonReconstructor reconstructor = JsonReconstructor.builder()
//                    .separator("__")
//                    .arrayFormat(JsonReconstructor.ArraySerializationFormat.JSON)
//                    .inferArraysFromValues(true)
//                    .preserveNulls(true)
//                    .maxDepth(50)
//                    .arrayPaths("items", "users")
//                    .build();
//
//            assertNotNull(reconstructor);
//        }
//
//        @Test
//        @DisplayName("Should add array paths incrementally")
//        void shouldAddArrayPathsIncrementally() {
//            JsonReconstructor reconstructor = JsonReconstructor.builder()
//                    .addArrayPath("items")
//                    .addArrayPath("users")
//                    .addArrayPath("orders")
//                    .build();
//
//            assertNotNull(reconstructor);
//        }
//
//        @Test
//        @DisplayName("Should throw on invalid max depth")
//        void shouldThrowOnInvalidMaxDepth() {
//            assertThrows(IllegalArgumentException.class, () ->
//                    JsonReconstructor.builder().maxDepth(0).build()
//            );
//        }
//
//        @Test
//        @DisplayName("Should build fluent operation")
//        void shouldBuildFluentOperation() {
//            Map<String, Object> flattened = new LinkedHashMap<>();
//            flattened.put("test", "value");
//
//            Map<String, Object> result = JsonReconstructor.builder()
//                    .buildFluent()
//                    .from(flattened)
//                    .toMap();
//
//            assertEquals("value", result.get("test"));
//        }
//    }
//
//    // ========================= ERROR HANDLING TESTS =========================
//
//    @Nested
//    @DisplayName("Error Handling")
//    class ErrorHandlingTests {
//
//        @Test
//        @DisplayName("Should throw on invalid JSON input")
//        void shouldThrowOnInvalidJsonInput() {
//            assertThrows(JsonReconstructor.ReconstructionException.class, () ->
//                    JsonReconstructor.create().fromJson("invalid json")
//            );
//        }
//
//        @Test
//        @DisplayName("Should throw on non-existent file")
//        void shouldThrowOnNonExistentFile() {
//            Path nonExistent = tempDir.resolve("does_not_exist.json");
//
//            assertThrows(JsonReconstructor.ReconstructionException.class, () ->
//                    JsonReconstructor.create().fromFile(nonExistent)
//            );
//        }
//    }
//}