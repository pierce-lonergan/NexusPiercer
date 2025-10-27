package io.github.pierce.examples

import io.github.pierce.MapFlattener;

import java.util.*;

public class MapFlattenerExamples {

    public static void main(String[] args) {
        // Example 1: Basic usage
        basicExample();

        // Example 2: AWS Athena configuration
        athenaExample();

        // Example 3: Nested arrays
        nestedArrayExample();

        // Example 4: Custom configuration
        customConfigExample();
    }

    private static void basicExample() {
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
    }

    private static void athenaExample() {
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
        // Output: tags = java|database|analytics
    }

    private static void nestedArrayExample() {
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
        // Output:
        // users_name = ["Alice","Bob"]
        // users_age = [30,25]
    }

    private static void customConfigExample() {
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
        data.put("userName", "Alice"); // Will become user_name
        data.put("internalDebug", "secret"); // Will be excluded

        Map<String, Object> result = flattener.flatten(data);
        result.forEach((key, value) ->
                System.out.println(key + " = " + value));
    }
}