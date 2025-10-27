package io.github.pierce;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import io.github.pierce.AvroReconstructor
import io.github.pierce.MapFlattener
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge case tests for realistic but complex scenarios
 */
public class EdgeCaseAvroReconstructionTest {

    @Test
    @DisplayName("Deep nesting - 5 levels with mixed records and arrays")
    public void testDeepNesting() {
        // Level 5 (deepest)
        Schema level5Schema = SchemaBuilder.record("Level5")
                .fields()
                .requiredString("deepValue")
                .requiredInt("depth")
                .endRecord();

        // Level 4 - array of level 5
        Schema level4Schema = SchemaBuilder.record("Level4")
                .fields()
                .name("items").type().array().items(level5Schema).noDefault()
                .requiredString("level4Name")
                .endRecord();

        // Level 3 - nested record
        Schema level3Schema = SchemaBuilder.record("Level3")
                .fields()
                .name("data").type(level4Schema).noDefault()
                .requiredString("level3Name")
                .endRecord();

        // Level 2 - array of level 3
        Schema level2Schema = SchemaBuilder.record("Level2")
                .fields()
                .name("containers").type().array().items(level3Schema).noDefault()
                .endRecord();

        // Level 1 (root)
        Schema schema = SchemaBuilder.record("Level1")
                .fields()
                .name("root").type(level2Schema).noDefault()
                .requiredString("rootId")
                .endRecord();

        // Create deeply nested data
        Map<String, Object> level5_1 = new LinkedHashMap<>();
        level5_1.put("deepValue", "I am at level 5!");
        level5_1.put("depth", 5);

        Map<String, Object> level5_2 = new LinkedHashMap<>();
        level5_2.put("deepValue", "Me too!");
        level5_2.put("depth", 5);

        Map<String, Object> level4 = new LinkedHashMap<>();
        level4.put("items", Arrays.asList(level5_1, level5_2));
        level4.put("level4Name", "Fourth Level");

        Map<String, Object> level3 = new LinkedHashMap<>();
        level3.put("data", level4);
        level3.put("level3Name", "Third Level");

        Map<String, Object> level2 = new LinkedHashMap<>();
        level2.put("containers", Collections.singletonList(level3));

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("root", level2);
        original.put("rootId", "ROOT-001");

        System.out.println("=== DEEP NESTING TEST (5 levels) ===");

        // Flatten and reconstruct
        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("Flattened keys:");
        for (String key : flattened.keySet()) {
            System.out.println("  " + key + ": " + flattened.get(key));
        }

        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());
        assertTrue(verification.isPerfect());
    }

    @Test
    @DisplayName("Multiple arrays at same level with nested records")
    public void testMultipleArraysSameLevel() {
        Schema tagSchema = SchemaBuilder.record("Tag")
                .fields()
                .requiredString("key")
                .requiredString("value")
                .endRecord();

        Schema metricSchema = SchemaBuilder.record("Metric")
                .fields()
                .requiredString("name")
                .requiredDouble("value")
                .requiredString("unit")
                .endRecord();

        Schema eventSchema = SchemaBuilder.record("Event")
                .fields()
                .requiredLong("timestamp")
                .requiredString("type")
                .endRecord();

        Schema schema = SchemaBuilder.record("MultiArrayTest")
                .fields()
                .requiredString("id")
                .name("tags").type().array().items(tagSchema).noDefault()
                .name("metrics").type().array().items(metricSchema).noDefault()
                .name("events").type().array().items(eventSchema).noDefault()
                .endRecord();

        Map<String, Object> original = new LinkedHashMap<>();
        original.put("id", "test-123");

        List<Map<String, Object>> tags = new ArrayList<>();
        Map<String, Object> tag1 = new LinkedHashMap<>();
        tag1.put("key", "env");
        tag1.put("value", "production");
        tags.add(tag1);

        Map<String, Object> tag2 = new LinkedHashMap<>();
        tag2.put("key", "region");
        tag2.put("value", "us-west-2");
        tags.add(tag2);
        original.put("tags", tags);

        List<Map<String, Object>> metrics = new ArrayList<>();
        Map<String, Object> metric1 = new LinkedHashMap<>();
        metric1.put("name", "cpu_usage");
        metric1.put("value", 45.2);
        metric1.put("unit", "percent");
        metrics.add(metric1);

        Map<String, Object> metric2 = new LinkedHashMap<>();
        metric2.put("name", "memory_usage");
        metric2.put("value", 2048.5);
        metric2.put("unit", "MB");
        metrics.add(metric2);
        original.put("metrics", metrics);

        List<Map<String, Object>> events = new ArrayList<>();
        Map<String, Object> event1 = new LinkedHashMap<>();
        event1.put("timestamp", 1705334400000L);
        event1.put("type", "START");
        events.add(event1);

        Map<String, Object> event2 = new LinkedHashMap<>();
        event2.put("timestamp", 1705338000000L);
        event2.put("type", "STOP");
        events.add(event2);
        original.put("events", events);

        System.out.println("=== MULTIPLE ARRAYS AT SAME LEVEL ===");

        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("Flattened:");
        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());
        assertTrue(verification.isPerfect());
    }

    @Test
    @DisplayName("Arrays with nullable primitive fields")
    public void testArraysWithNullablePrimitives() {
        Schema itemSchema = SchemaBuilder.record("Item")
                .fields()
                .requiredString("id")
                .optionalString("name")
                .optionalInt("quantity")
                .optionalDouble("price")
                .endRecord();

        Schema schema = SchemaBuilder.record("NullableTest")
                .fields()
                .name("items").type().array().items(itemSchema).noDefault()
                .endRecord();

        Map<String, Object> original = new LinkedHashMap<>();

        List<Map<String, Object>> items = new ArrayList<>();

        // Item with all fields
        Map<String, Object> item1 = new LinkedHashMap<>();
        item1.put("id", "item-1");
        item1.put("name", "First Item");
        item1.put("quantity", 10);
        item1.put("price", 99.99);
        items.add(item1);

        // Item with nulls
        Map<String, Object> item2 = new LinkedHashMap<>();
        item2.put("id", "item-2");
        item2.put("name", null);
        item2.put("quantity", null);
        item2.put("price", null);
        items.add(item2);

        // Item with some nulls
        Map<String, Object> item3 = new LinkedHashMap<>();
        item3.put("id", "item-3");
        item3.put("name", "Third Item");
        item3.put("quantity", 5);
        item3.put("price", null);
        items.add(item3);

        original.put("items", items);

        System.out.println("=== NULLABLE PRIMITIVE FIELDS ===");

        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("Flattened:");
        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());
        assertTrue(verification.isPerfect());
    }

    @Test
    @DisplayName("Nested records with nullable fields in arrays")
    public void testNestedRecordsWithNullableFields() {
        Schema addressSchema = SchemaBuilder.record("Address")
                .fields()
                .requiredString("street")
                .optionalString("city")
                .optionalString("state")
                .endRecord();

        Schema personSchema = SchemaBuilder.record("Person")
                .fields()
                .requiredString("name")
                .name("address").type(addressSchema).noDefault()
                .endRecord();

        Schema schema = SchemaBuilder.record("NestedTest")
                .fields()
                .name("people").type().array().items(personSchema).noDefault()
                .endRecord();

        Map<String, Object> original = new LinkedHashMap<>();
        List<Map<String, Object>> people = new ArrayList<>();

        // Person with complete address
        Map<String, Object> person1 = new LinkedHashMap<>();
        person1.put("name", "Alice");
        Map<String, Object> address1 = new LinkedHashMap<>();
        address1.put("street", "123 Main St");
        address1.put("city", "Boston");
        address1.put("state", "MA");
        person1.put("address", address1);
        people.add(person1);

        // Person with partial address (null city and state)
        Map<String, Object> person2 = new LinkedHashMap<>();
        person2.put("name", "Bob");
        Map<String, Object> address2 = new LinkedHashMap<>();
        address2.put("street", "456 Oak Ave");
        address2.put("city", null);
        address2.put("state", null);
        person2.put("address", address2);
        people.add(person2);

        // Person with only required fields
        Map<String, Object> person3 = new LinkedHashMap<>();
        person3.put("name", "Charlie");
        Map<String, Object> address3 = new LinkedHashMap<>();
        address3.put("street", "789 Pine Rd");
        address3.put("city", "Seattle");
        address3.put("state", null);
        person3.put("address", address3);
        people.add(person3);

        original.put("people", people);

        System.out.println("=== NESTED RECORDS WITH NULLABLE FIELDS ===");

        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("Flattened:");
        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());
        assertTrue(verification.isPerfect());
    }

    @Test
    @DisplayName("Large array with 100 records")
    public void testLargeArray() {
        Schema itemSchema = SchemaBuilder.record("Item")
                .fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredDouble("value")
                .endRecord();

        Schema schema = SchemaBuilder.record("LargeArray")
                .fields()
                .name("items").type().array().items(itemSchema).noDefault()
                .endRecord();

        Map<String, Object> original = new LinkedHashMap<>();
        List<Map<String, Object>> items = new ArrayList<>();

        // Create 100 items
        for (int i = 0; i < 100; i++) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", i);
            item.put("name", "Item-" + i);
            item.put("value", i * 1.5);
            items.add(item);
        }

        original.put("items", items);

        System.out.println("=== LARGE ARRAY TEST (100 records) ===");

        MapFlattener flattener = new MapFlattener();
        long flattenStart = System.currentTimeMillis();
        Map<String, Object> flattened = flattener.flatten(original);
        long flattenTime = System.currentTimeMillis() - flattenStart;

        System.out.println("Flatten time: " + flattenTime + "ms");
        System.out.println("Flattened fields: " + flattened.size());

        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        long reconstructStart = System.currentTimeMillis();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);
        long reconstructTime = System.currentTimeMillis() - reconstructStart;

        System.out.println("Reconstruct time: " + reconstructTime + "ms");

        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());
        assertTrue(verification.isPerfect());
    }

    @Test
    @DisplayName("Arrays with different sized nested arrays")
    public void testVariableSizeNestedArrays() {
        Schema itemSchema = SchemaBuilder.record("Item")
                .fields()
                .requiredString("name")
                .name("tags").type().array().items().stringType().noDefault()
                .endRecord();

        Schema schema = SchemaBuilder.record("VariableArrays")
                .fields()
                .name("items").type().array().items(itemSchema).noDefault()
                .endRecord();

        Map<String, Object> original = new LinkedHashMap<>();
        List<Map<String, Object>> items = new ArrayList<>();

        // Item with 3 tags
        Map<String, Object> item1 = new LinkedHashMap<>();
        item1.put("name", "Item1");
        item1.put("tags", Arrays.asList("tag1", "tag2", "tag3"));
        items.add(item1);

        // Item with 1 tag
        Map<String, Object> item2 = new LinkedHashMap<>();
        item2.put("name", "Item2");
        item2.put("tags", Collections.singletonList("single"));
        items.add(item2);

        // Item with 5 tags
        Map<String, Object> item3 = new LinkedHashMap<>();
        item3.put("name", "Item3");
        item3.put("tags", Arrays.asList("a", "b", "c", "d", "e"));
        items.add(item3);

        // Item with empty array
        Map<String, Object> item4 = new LinkedHashMap<>();
        item4.put("name", "Item4");
        item4.put("tags", Collections.emptyList());
        items.add(item4);

        original.put("items", items);

        System.out.println("=== VARIABLE SIZE NESTED ARRAYS ===");

        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("Flattened:");
        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());
        assertTrue(verification.isPerfect());
    }

    @Test
    @DisplayName("All primitive types in arrays")
    public void testAllPrimitiveTypesInArrays() {
        Schema recordSchema = SchemaBuilder.record("AllTypes")
                .fields()
                .requiredString("stringField")
                .requiredInt("intField")
                .requiredLong("longField")
                .requiredFloat("floatField")
                .requiredDouble("doubleField")
                .requiredBoolean("booleanField")
                .endRecord();

        Schema schema = SchemaBuilder.record("PrimitiveTest")
                .fields()
                .name("records").type().array().items(recordSchema).noDefault()
                .endRecord();

        Map<String, Object> original = new LinkedHashMap<>();
        List<Map<String, Object>> records = new ArrayList<>();

        Map<String, Object> record1 = new LinkedHashMap<>();
        record1.put("stringField", "hello");
        record1.put("intField", 42);
        record1.put("longField", 9876543210L);
        record1.put("floatField", 3.14f);
        record1.put("doubleField", 2.718281828);
        record1.put("booleanField", true);
        records.add(record1);

        Map<String, Object> record2 = new LinkedHashMap<>();
        record2.put("stringField", "world");
        record2.put("intField", -100);
        record2.put("longField", 0L);
        record2.put("floatField", -99.9f);
        record2.put("doubleField", 1.23456789);
        record2.put("booleanField", false);
        records.add(record2);

        Map<String, Object> record3 = new LinkedHashMap<>();
        record3.put("stringField", "test with spaces and special chars: !@#\$%");
        record3.put("intField", 2147483647); // Max int
        record3.put("longField", 9223372036854775807L); // Max long
        record3.put("floatField", Float.MAX_VALUE);
        record3.put("doubleField", Double.MAX_VALUE);
        record3.put("booleanField", true);
        records.add(record3);

        original.put("records", records);

        System.out.println("=== ALL PRIMITIVE TYPES IN ARRAYS ===");

        MapFlattener flattener = new MapFlattener();
        Map<String, Object> flattened = flattener.flatten(original);

        System.out.println("Flattened:");
        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        AvroReconstructor reconstructor = AvroReconstructor.builder().build();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);

        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());
        assertTrue(verification.isPerfect());
    }
}