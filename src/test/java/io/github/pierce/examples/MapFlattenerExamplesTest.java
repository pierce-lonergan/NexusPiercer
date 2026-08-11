package io.github.pierce.examples;

import io.github.pierce.MapFlattener;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ported from {@code src/test/groovy/MapFlattenerExamples.groovy}, which was a
 * {@code public static void main} demo with four example methods, zero assertions, and no
 * entry point that anything ever called — it was compiled on every build and executed never.
 *
 * <p>The four examples are worth keeping: between them they are the only executable record of
 * four distinct builder configurations (PIPE_SEPARATED for Athena, JSON for nested arrays,
 * SNAKE_CASE naming, and path exclusion). So each demo became a test with assertions on the
 * output the original only printed. The class is named {@code ...ExamplesTest} rather than
 * {@code ...Examples} for one reason: surefire's include patterns are {@code **}{@code /*Test.java},
 * {@code *Tests.java} and {@code *TestCase.java}, so a class called {@code MapFlattenerExamples}
 * would be compiled and skipped exactly as the Groovy one was.</p>
 *
 * <p>The demo bodies are otherwise unchanged, including the {@code System.out} output.</p>
 */
class MapFlattenerExamplesTest {

    @Test
    @DisplayName("Basic example: a nested map flattens to underscore-joined keys")
    void basicExample() {
        System.out.println("=== Basic Example ===");

        MapFlattener flattener = new MapFlattener();

        Map<String, Object> data = new HashMap<>();
        data.put("userId", 123);
        data.put("name", "Alice");

        Map<String, Object> address = new HashMap<>();
        address.put("street", "Main St");
        address.put("city", "Boston");
        data.put("address", address);

        Map<String, Object> result = flattener.flatten(data);
        result.forEach((key, value) ->
                System.out.println(key + " = " + value));

        assertThat(result)
                .as("the nested address map becomes two underscore-joined leaf columns")
                .containsEntry("userId", 123)
                .containsEntry("name", "Alice")
                .containsEntry("address_street", "Main St")
                .containsEntry("address_city", "Boston")
                .doesNotContainKey("address");
    }

    @Test
    @DisplayName("AWS Athena example: PIPE_SEPARATED joins a primitive array with '|'")
    void athenaExample() {
        System.out.println("\n=== AWS Athena Example ===");

        MapFlattener flattener = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.PIPE_SEPARATED)
                .maxArraySize(10000)
                .strictKeyValidation(true)
                .build();

        Map<String, Object> data = new HashMap<>();
        data.put("tags", Arrays.asList("java", "database", "analytics"));

        Map<String, Object> result = flattener.flatten(data);
        System.out.println("tags = " + result.get("tags"));

        // The original carried this as a comment: "Output: tags = java|database|analytics".
        // A commented-out expectation is not an expectation, so it is asserted instead.
        assertThat(result.get("tags")).isEqualTo("java|database|analytics");
    }

    @Test
    @DisplayName("Nested array example: JSON format emits one JSON array column per record field")
    void nestedArrayExample() {
        System.out.println("\n=== Nested Array Example ===");

        MapFlattener flattener = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.JSON)
                .build();

        Map<String, Object> data = new HashMap<>();

        List<Map<String, Object>> users = new ArrayList<>();
        Map<String, Object> user1 = new HashMap<>();
        user1.put("name", "Alice");
        user1.put("age", 30);
        users.add(user1);

        Map<String, Object> user2 = new HashMap<>();
        user2.put("name", "Bob");
        user2.put("age", 25);
        users.add(user2);

        data.put("users", users);

        Map<String, Object> result = flattener.flatten(data);
        result.forEach((key, value) ->
                System.out.println(key + " = " + value));

        // Again, the original's expected output lived only in a trailing comment.
        assertThat(result)
                .as("an array of records becomes one column per field, each a JSON array")
                .containsEntry("users_name", "[\"Alice\",\"Bob\"]")
                .containsEntry("users_age", "[30,25]");
    }

    @Test
    @DisplayName("Custom configuration example: SNAKE_CASE renames keys; excludePaths does not match here")
    void customConfigExample() {
        System.out.println("\n=== Custom Configuration Example ===");

        MapFlattener flattener = MapFlattener.builder()
                .maxDepth(10)
                .maxArraySize(500)
                .useArrayBoundarySeparator(true) // Use __ instead of _
                .namingStrategy(MapFlattener.FieldNamingStrategy.SNAKE_CASE)
                .detectCircularReferences(true)
                .preserveBigDecimalPrecision(true)
                .excludePaths("internal.*", "*.password")
                .build();

        Map<String, Object> data = new HashMap<>();
        data.put("userName", "Alice"); // Becomes user_name
        data.put("internalDebug", "secret"); // The original comment claimed "Will be excluded"

        Map<String, Object> result = flattener.flatten(data);
        result.forEach((key, value) ->
                System.out.println(key + " = " + value));

        assertThat(result)
                .as("SNAKE_CASE rewrites camelCase keys after flattening")
                .containsEntry("user_name", "Alice");

        // The original comment says internalDebug "Will be excluded". It is not.
        // MapFlattener.matchesPattern splits an exclude pattern on '*' and Pattern.quote()s the
        // literal parts, so "internal.*" compiles to \Qinternal.\E.* and requires a LITERAL DOT
        // after "internal". Flattened paths are joined with '_' (or '__'), never '.', so neither
        // "internal.*" nor "*.password" can ever match any key this flattener produces.
        // Asserted rather than commented, so that fixing the pattern semantics is a visible,
        // deliberate change rather than a silent one.
        assertThat(result)
                .as("excludePaths(\"internal.*\") does NOT exclude internalDebug - the '.' is literal")
                .containsEntry("internal_debug", "secret");
    }
}
