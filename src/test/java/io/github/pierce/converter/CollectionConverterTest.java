package io.github.pierce.converter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for collection type converters: List, Map, Struct.
 */
class CollectionConverterTest {

    private ConversionConfig defaultConfig;

    @BeforeEach
    void setUp() {
        defaultConfig = ConversionConfig.defaults();
    }

    // ==================== List Converter Tests ====================

    @Nested
    @DisplayName("ListConverter")
    class ListConverterTests {

        @Test
        @DisplayName("converts List of integers")
        void convertsListOfIntegers() {
            ListConverter converter = new ListConverter(defaultConfig, new IntegerConverter(defaultConfig));
            List<Object> result = converter.convert(List.of(1, 2, 3, 4, 5));
            assertThat(result).containsExactly(1, 2, 3, 4, 5);
        }

        @Test
        @DisplayName("converts List with type coercion")
        void convertsListWithCoercion() {
            ListConverter converter = new ListConverter(defaultConfig, new LongConverter(defaultConfig));
            // Integers should be promoted to Longs
            List<Object> result = converter.convert(List.of(1, 2, 3));
            assertThat(result).containsExactly(1L, 2L, 3L);
        }

        @Test
        @DisplayName("converts ArrayList")
        void convertsArrayList() {
            ListConverter converter = new ListConverter(defaultConfig, new StringConverter(defaultConfig));
            List<Object> result = converter.convert(new ArrayList<>(List.of("a", "b", "c")));
            assertThat(result).containsExactly("a", "b", "c");
        }

        @Test
        @DisplayName("converts Set to List")
        void convertsSetToList() {
            ListConverter converter = new ListConverter(defaultConfig, new IntegerConverter(defaultConfig));
            Set<Integer> input = new LinkedHashSet<>(List.of(1, 2, 3));
            List<Object> result = converter.convert(input);
            assertThat(result).containsExactlyInAnyOrder(1, 2, 3);
        }

        @Test
        @DisplayName("converts primitive array")
        void convertsPrimitiveArray() {
            ListConverter converter = new ListConverter(defaultConfig, new IntegerConverter(defaultConfig));
            int[] input = {1, 2, 3, 4, 5};
            List<Object> result = converter.convert(input);
            assertThat(result).containsExactly(1, 2, 3, 4, 5);
        }

        @Test
        @DisplayName("converts Object array")
        void convertsObjectArray() {
            ListConverter converter = new ListConverter(defaultConfig, new StringConverter(defaultConfig));
            String[] input = {"a", "b", "c"};
            List<Object> result = converter.convert(input);
            assertThat(result).containsExactly("a", "b", "c");
        }

        @Test
        @DisplayName("returns null for null input")
        void returnsNullForNull() {
            ListConverter converter = new ListConverter(defaultConfig, new IntegerConverter(defaultConfig));
            assertThat(converter.convert(null)).isNull();
        }

        @Test
        @DisplayName("converts empty list")
        void convertsEmptyList() {
            ListConverter converter = new ListConverter(defaultConfig, new IntegerConverter(defaultConfig));
            List<Object> result = converter.convert(List.of());
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("handles null elements")
        void handlesNullElements() {
            ListConverter converter = new ListConverter(defaultConfig, new IntegerConverter(defaultConfig));
            List<Integer> input = new ArrayList<>();
            input.add(1);
            input.add(null);
            input.add(3);
            List<Object> result = converter.convert(input);
            assertThat(result).containsExactly(1, null, 3);
        }

        @Test
        @DisplayName("propagates element conversion errors with index")
        void propagatesErrorsWithIndex() {
            ListConverter converter = new ListConverter(defaultConfig, new IntegerConverter(defaultConfig));
            List<Object> input = List.of(1, 2, "not a number", 4);
            assertThatThrownBy(() -> converter.convert(input))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("[2]");
        }

        @Test
        @DisplayName("converts nested lists")
        void convertsNestedLists() {
            ListConverter innerConverter = new ListConverter(defaultConfig, new IntegerConverter(defaultConfig));
            ListConverter outerConverter = new ListConverter(defaultConfig, innerConverter);

            List<List<Integer>> input = List.of(
                    List.of(1, 2),
                    List.of(3, 4, 5),
                    List.of(6)
            );

            List<Object> result = outerConverter.convert(input);
            assertThat(result).hasSize(3);
            @SuppressWarnings("unchecked")
            List<Object> list0 = (List<Object>) result.get(0);
            @SuppressWarnings("unchecked")
            List<Object> list1 = (List<Object>) result.get(1);
            @SuppressWarnings("unchecked")
            List<Object> list2 = (List<Object>) result.get(2);
            assertThat(list0).containsExactly(1, 2);
            assertThat(list1).containsExactly(3, 4, 5);
            assertThat(list2).containsExactly(6);
        }
    }

    // ==================== Map Converter Tests ====================

    @Nested
    @DisplayName("MapConverter")
    class MapConverterTests {

        @Test
        @DisplayName("converts Map with String keys")
        void convertsMapWithStringKeys() {
            MapConverter converter = new MapConverter(
                    defaultConfig,
                    new StringConverter(defaultConfig),
                    new IntegerConverter(defaultConfig)
            );
            Map<String, Integer> input = Map.of("a", 1, "b", 2, "c", 3);
            Map<Object, Object> result = converter.convert(input);
            assertThat(result).containsEntry("a", 1).containsEntry("b", 2).containsEntry("c", 3);
        }

        @Test
        @DisplayName("converts keys and values")
        void convertsKeysAndValues() {
            MapConverter converter = new MapConverter(
                    defaultConfig,
                    new StringConverter(defaultConfig),
                    new LongConverter(defaultConfig)
            );
            // Integers as values should be promoted to Longs
            Map<String, Integer> input = Map.of("x", 100, "y", 200);
            Map<Object, Object> result = converter.convert(input);
            assertThat(result.get("x")).isEqualTo(100L);
            assertThat(result.get("y")).isEqualTo(200L);
        }

        @Test
        @DisplayName("preserves insertion order with LinkedHashMap")
        void preservesInsertionOrder() {
            MapConverter converter = new MapConverter(
                    defaultConfig,
                    new StringConverter(defaultConfig),
                    new IntegerConverter(defaultConfig)
            );
            Map<String, Integer> input = new LinkedHashMap<>();
            input.put("first", 1);
            input.put("second", 2);
            input.put("third", 3);

            Map<Object, Object> result = converter.convert(input);
            List<Object> keys = new ArrayList<>(result.keySet());
            assertThat(keys).containsExactly("first", "second", "third");
        }

        @Test
        @DisplayName("returns null for null input")
        void returnsNullForNull() {
            MapConverter converter = new MapConverter(
                    defaultConfig,
                    new StringConverter(defaultConfig),
                    new IntegerConverter(defaultConfig)
            );
            assertThat(converter.convert(null)).isNull();
        }

        @Test
        @DisplayName("converts empty map")
        void convertsEmptyMap() {
            MapConverter converter = new MapConverter(
                    defaultConfig,
                    new StringConverter(defaultConfig),
                    new IntegerConverter(defaultConfig)
            );
            Map<Object, Object> result = converter.convert(Map.of());
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("handles null values")
        void handlesNullValues() {
            MapConverter converter = new MapConverter(
                    defaultConfig,
                    new StringConverter(defaultConfig),
                    new IntegerConverter(defaultConfig)
            );
            Map<String, Integer> input = new HashMap<>();
            input.put("a", 1);
            input.put("b", null);
            input.put("c", 3);

            Map<Object, Object> result = converter.convert(input);
            assertThat(result).containsEntry("a", 1).containsEntry("b", null).containsEntry("c", 3);
        }

        @Test
        @DisplayName("propagates value conversion errors with key")
        void propagatesErrorsWithKey() {
            MapConverter converter = new MapConverter(
                    defaultConfig,
                    new StringConverter(defaultConfig),
                    new IntegerConverter(defaultConfig)
            );
            Map<String, Object> input = Map.of("good", 1, "bad", "not a number");
            assertThatThrownBy(() -> converter.convert(input))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("[bad]");
        }

        @Test
        @DisplayName("converts nested maps")
        void convertsNestedMaps() {
            MapConverter innerConverter = new MapConverter(
                    defaultConfig,
                    new StringConverter(defaultConfig),
                    new IntegerConverter(defaultConfig)
            );
            MapConverter outerConverter = new MapConverter(
                    defaultConfig,
                    new StringConverter(defaultConfig),
                    innerConverter
            );

            Map<String, Map<String, Integer>> input = Map.of(
                    "outer1", Map.of("a", 1, "b", 2),
                    "outer2", Map.of("x", 10)
            );

            Map<Object, Object> result = outerConverter.convert(input);
            assertThat(result).hasSize(2);
            @SuppressWarnings("unchecked")
            Map<Object, Object> outer1 = (Map<Object, Object>) result.get("outer1");
            @SuppressWarnings("unchecked")
            Map<Object, Object> outer2 = (Map<Object, Object>) result.get("outer2");
            assertThat(outer1).containsEntry("a", 1).containsEntry("b", 2);
            assertThat(outer2).containsEntry("x", 10);
        }
    }

    // ==================== Struct Converter Tests ====================

    @Nested
    @DisplayName("StructConverter")
    class StructConverterTests {

        @Test
        @DisplayName("converts simple struct")
        void convertsSimpleStruct() {
            StructConverter converter = new StructConverter.Builder(defaultConfig)
                    .addField("name", new StringConverter(defaultConfig), true)
                    .addField("age", new IntegerConverter(defaultConfig), true)
                    .build();

            Map<String, Object> input = Map.of("name", "Alice", "age", 30);
            Map<String, Object> result = converter.convert(input);

            assertThat(result).containsEntry("name", "Alice").containsEntry("age", 30);
        }

        @Test
        @DisplayName("handles optional fields")
        void handlesOptionalFields() {
            StructConverter converter = new StructConverter.Builder(defaultConfig)
                    .addField("required", new StringConverter(defaultConfig), true)
                    .addField("optional", new StringConverter(defaultConfig), false)
                    .build();

            // Missing optional field
            Map<String, Object> input = Map.of("required", "value");
            Map<String, Object> result = converter.convert(input);

            assertThat(result).containsEntry("required", "value");
            assertThat(result).containsEntry("optional", null);
        }

        @Test
        @DisplayName("throws on missing required field")
        void throwsOnMissingRequiredField() {
            StructConverter converter = new StructConverter.Builder(defaultConfig)
                    .addField("required", new StringConverter(defaultConfig), true)
                    .build();

            Map<String, Object> input = Map.of(); // Missing "required"
            assertThatThrownBy(() -> converter.convert(input))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("required");
        }

        @Test
        @DisplayName("throws on null required field")
        void throwsOnNullRequiredField() {
            StructConverter converter = new StructConverter.Builder(defaultConfig)
                    .addField("required", new StringConverter(defaultConfig), true)
                    .build();

            Map<String, Object> input = new HashMap<>();
            input.put("required", null);

            assertThatThrownBy(() -> converter.convert(input))
                    .isInstanceOf(TypeConversionException.class);
        }

        @Test
        @DisplayName("uses default values when configured")
        void usesDefaultValues() {
            ConversionConfig useDefaults = ConversionConfig.builder()
                    .useSchemaDefaults(true)
                    .build();

            StructConverter converter = new StructConverter.Builder(useDefaults)
                    .addField("name", new StringConverter(useDefaults), true, "Unknown")
                    .build();

            Map<String, Object> input = Map.of(); // Missing "name"
            Map<String, Object> result = converter.convert(input);

            assertThat(result).containsEntry("name", "Unknown");
        }

        @Test
        @DisplayName("rejects extra fields when configured")
        void rejectsExtraFields() {
            ConversionConfig noExtras = ConversionConfig.builder()
                    .allowExtraFields(false)
                    .build();

            StructConverter converter = new StructConverter.Builder(noExtras)
                    .addField("known", new StringConverter(noExtras), true)
                    .build();

            Map<String, Object> input = Map.of("known", "value", "extra", "not allowed");
            assertThatThrownBy(() -> converter.convert(input))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("Extra fields");
        }

        @Test
        @DisplayName("allows extra fields by default")
        void allowsExtraFieldsByDefault() {
            StructConverter converter = new StructConverter.Builder(defaultConfig)
                    .addField("known", new StringConverter(defaultConfig), true)
                    .build();

            Map<String, Object> input = Map.of("known", "value", "extra", "ignored");
            Map<String, Object> result = converter.convert(input);

            assertThat(result).containsEntry("known", "value");
            assertThat(result).doesNotContainKey("extra");
        }

        @Test
        @DisplayName("converts nested structs")
        void convertsNestedStructs() {
            StructConverter innerConverter = new StructConverter.Builder(defaultConfig)
                    .addField("x", new IntegerConverter(defaultConfig), true)
                    .addField("y", new IntegerConverter(defaultConfig), true)
                    .build();

            StructConverter outerConverter = new StructConverter.Builder(defaultConfig)
                    .addField("name", new StringConverter(defaultConfig), true)
                    .addField("position", innerConverter, true)
                    .build();

            Map<String, Object> input = Map.of(
                    "name", "Point A",
                    "position", Map.of("x", 10, "y", 20)
            );

            Map<String, Object> result = outerConverter.convert(input);
            assertThat(result).containsEntry("name", "Point A");
            @SuppressWarnings("unchecked")
            Map<String, Object> position = (Map<String, Object>) result.get("position");
            assertThat(position).containsEntry("x", 10).containsEntry("y", 20);
        }

        @Test
        @DisplayName("converts struct with list field")
        void convertsStructWithListField() {
            ListConverter tagsConverter = new ListConverter(defaultConfig, new StringConverter(defaultConfig));

            StructConverter converter = new StructConverter.Builder(defaultConfig)
                    .addField("id", new IntegerConverter(defaultConfig), true)
                    .addField("tags", tagsConverter, false)
                    .build();

            Map<String, Object> input = Map.of(
                    "id", 123,
                    "tags", List.of("java", "iceberg", "avro")
            );

            Map<String, Object> result = converter.convert(input);
            assertThat(result).containsEntry("id", 123);
            @SuppressWarnings("unchecked")
            List<Object> tags = (List<Object>) result.get("tags");
            assertThat(tags).containsExactly("java", "iceberg", "avro");
        }

        @Test
        @DisplayName("propagates field conversion errors with field name")
        void propagatesErrorsWithFieldName() {
            StructConverter converter = new StructConverter.Builder(defaultConfig)
                    .addField("number", new IntegerConverter(defaultConfig), true)
                    .build();

            Map<String, Object> input = Map.of("number", "not a number");
            assertThatThrownBy(() -> converter.convert(input))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("number");
        }

        @Test
        @DisplayName("returns null for null input")
        void returnsNullForNull() {
            StructConverter converter = new StructConverter.Builder(defaultConfig)
                    .addField("field", new StringConverter(defaultConfig), true)
                    .build();
            assertThat(converter.convert(null)).isNull();
        }
    }
}