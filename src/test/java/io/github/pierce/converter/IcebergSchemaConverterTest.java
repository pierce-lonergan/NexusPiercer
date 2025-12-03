package io.github.pierce.converter;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for IcebergSchemaConverter.
 */
class IcebergSchemaConverterTest {

    private ConversionConfig defaultConfig;

    @BeforeEach
    void setUp() {
        defaultConfig = ConversionConfig.defaults();
    }

    // ==================== Simple Schema Tests ====================

    @Nested
    @DisplayName("Simple Schemas")
    class SimpleSchemaTests {

        @Test
        @DisplayName("converts record with all primitive types")
        void convertsAllPrimitiveTypes() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "bool_field", Types.BooleanType.get()),
                    Types.NestedField.required(2, "int_field", Types.IntegerType.get()),
                    Types.NestedField.required(3, "long_field", Types.LongType.get()),
                    Types.NestedField.required(4, "float_field", Types.FloatType.get()),
                    Types.NestedField.required(5, "double_field", Types.DoubleType.get()),
                    Types.NestedField.required(6, "string_field", Types.StringType.get())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of(
                    "bool_field", true,
                    "int_field", 42,
                    "long_field", 123456789L,
                    "float_field", 3.14f,
                    "double_field", 2.71828,
                    "string_field", "hello"
            );

            GenericRecord result = converter.convert(input);

            assertThat(result.getField("bool_field")).isEqualTo(true);
            assertThat(result.getField("int_field")).isEqualTo(42);
            assertThat(result.getField("long_field")).isEqualTo(123456789L);
            assertThat(result.getField("float_field")).isEqualTo(3.14f);
            assertThat(result.getField("double_field")).isEqualTo(2.71828);
            assertThat(result.getField("string_field")).isEqualTo("hello");
        }

        @Test
        @DisplayName("converts record with temporal types")
        void convertsTemporalTypes() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "date_field", Types.DateType.get()),
                    Types.NestedField.required(2, "time_field", Types.TimeType.get()),
                    Types.NestedField.required(3, "timestamp_field", Types.TimestampType.withZone())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            LocalDate date = LocalDate.of(2024, 6, 15);
            LocalTime time = LocalTime.of(14, 30, 45);
            Instant timestamp = Instant.parse("2024-06-15T14:30:45Z");

            Map<String, Object> input = Map.of(
                    "date_field", date,
                    "time_field", time,
                    "timestamp_field", timestamp
            );

            GenericRecord result = converter.convert(input);

            // Date is stored as days since epoch
            assertThat(result.getField("date_field")).isInstanceOf(Integer.class);
            // Time is stored as microseconds since midnight
            assertThat(result.getField("time_field")).isInstanceOf(Long.class);
            // Timestamp is stored as microseconds since epoch
            assertThat(result.getField("timestamp_field")).isInstanceOf(Long.class);
        }

        @Test
        @DisplayName("converts record with binary and UUID")
        void convertsBinaryAndUUID() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "binary_field", Types.BinaryType.get()),
                    Types.NestedField.required(2, "uuid_field", Types.UUIDType.get()),
                    Types.NestedField.required(3, "fixed_field", Types.FixedType.ofLength(8))
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            UUID uuid = UUID.randomUUID();
            byte[] binary = {1, 2, 3, 4, 5};
            byte[] fixed = {1, 2, 3, 4, 5, 6, 7, 8};

            Map<String, Object> input = Map.of(
                    "binary_field", binary,
                    "uuid_field", uuid,
                    "fixed_field", fixed
            );

            GenericRecord result = converter.convert(input);

            assertThat(result.getField("binary_field")).isInstanceOf(ByteBuffer.class);
            assertThat(result.getField("uuid_field")).isEqualTo(uuid);
            assertThat(result.getField("fixed_field")).isInstanceOf(ByteBuffer.class);
        }

        @Test
        @DisplayName("converts record with decimal")
        void convertsDecimal() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "price", Types.DecimalType.of(10, 2)),
                    Types.NestedField.required(2, "quantity", Types.DecimalType.of(5, 0))
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of(
                    "price", new BigDecimal("99.99"),
                    "quantity", 100
            );

            GenericRecord result = converter.convert(input);

            assertThat(result.getField("price")).isEqualTo(new BigDecimal("99.99"));
            assertThat((BigDecimal) result.getField("quantity")).isEqualByComparingTo(new BigDecimal("100"));
        }
    }

    // ==================== Nested Schema Tests ====================

    @Nested
    @DisplayName("Nested Schemas")
    class NestedSchemaTests {

        @Test
        @DisplayName("converts record with nested struct")
        void convertsNestedStruct() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "address", Types.StructType.of(
                            Types.NestedField.required(3, "street", Types.StringType.get()),
                            Types.NestedField.required(4, "city", Types.StringType.get()),
                            Types.NestedField.optional(5, "zip", Types.StringType.get())
                    ))
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of(
                    "id", 123,
                    "address", Map.of(
                            "street", "123 Main St",
                            "city", "Springfield"
                    )
            );

            GenericRecord result = converter.convert(input);

            assertThat(result.getField("id")).isEqualTo(123);
            @SuppressWarnings("unchecked")
            Map<String, Object> address = (Map<String, Object>) result.getField("address");
            assertThat(address.get("street")).isEqualTo("123 Main St");
            assertThat(address.get("city")).isEqualTo("Springfield");
            assertThat(address.get("zip")).isNull();
        }

        @Test
        @DisplayName("converts record with list")
        void convertsListField() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "name", Types.StringType.get()),
                    Types.NestedField.required(2, "scores", Types.ListType.ofRequired(3, Types.IntegerType.get()))
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of(
                    "name", "Student A",
                    "scores", List.of(85, 90, 78, 92)
            );

            GenericRecord result = converter.convert(input);

            assertThat(result.getField("name")).isEqualTo("Student A");
            @SuppressWarnings("unchecked")
            List<Integer> scores = (List<Integer>) result.getField("scores");
            assertThat(scores).containsExactly(85, 90, 78, 92);
        }

        @Test
        @DisplayName("converts record with map")
        void convertsMapField() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "metadata", Types.MapType.ofRequired(
                            3, 4, Types.StringType.get(), Types.StringType.get()
                    ))
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of(
                    "id", 1,
                    "metadata", Map.of(
                            "author", "John",
                            "version", "1.0"
                    )
            );

            GenericRecord result = converter.convert(input);

            assertThat(result.getField("id")).isEqualTo(1);
            @SuppressWarnings("unchecked")
            Map<String, String> metadata = (Map<String, String>) result.getField("metadata");
            assertThat(metadata).containsEntry("author", "John").containsEntry("version", "1.0");
        }

        @Test
        @DisplayName("converts deeply nested structure")
        void convertsDeeplyNestedStructure() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "company", Types.StructType.of(
                            Types.NestedField.required(2, "name", Types.StringType.get()),
                            Types.NestedField.required(3, "departments", Types.ListType.ofRequired(4,
                                    Types.StructType.of(
                                            Types.NestedField.required(5, "name", Types.StringType.get()),
                                            Types.NestedField.required(6, "employees", Types.ListType.ofRequired(7,
                                                    Types.StructType.of(
                                                            Types.NestedField.required(8, "id", Types.IntegerType.get()),
                                                            Types.NestedField.required(9, "name", Types.StringType.get())
                                                    )
                                            ))
                                    )
                            ))
                    ))
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of(
                    "company", Map.of(
                            "name", "TechCorp",
                            "departments", List.of(
                                    Map.of(
                                            "name", "Engineering",
                                            "employees", List.of(
                                                    Map.of("id", 1, "name", "Alice"),
                                                    Map.of("id", 2, "name", "Bob")
                                            )
                                    ),
                                    Map.of(
                                            "name", "Marketing",
                                            "employees", List.of(
                                                    Map.of("id", 3, "name", "Charlie")
                                            )
                                    )
                            )
                    )
            );

            GenericRecord result = converter.convert(input);
            assertThat(result).isNotNull();
            assertThat(result.getField("company")).isNotNull();
        }
    }

    // ==================== Type Coercion Tests ====================

    @Nested
    @DisplayName("Type Coercion")
    class TypeCoercionTests {

        @Test
        @DisplayName("coerces Integer to Long")
        void coercesIntegerToLong() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "value", Types.LongType.get())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("value", 42); // Integer, not Long

            GenericRecord result = converter.convert(input);
            assertThat(result.getField("value")).isEqualTo(42L);
        }

        @Test
        @DisplayName("coerces String to Integer")
        void coercesStringToInteger() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "count", Types.IntegerType.get())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("count", "123");

            GenericRecord result = converter.convert(input);
            assertThat(result.getField("count")).isEqualTo(123);
        }

        @Test
        @DisplayName("coerces String to Boolean")
        void coercesStringToBoolean() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "active", Types.BooleanType.get())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("active", "true");

            GenericRecord result = converter.convert(input);
            assertThat(result.getField("active")).isEqualTo(true);
        }

        @Test
        @DisplayName("coerces String date to Date")
        void coercesStringToDate() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "date", Types.DateType.get())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("date", "2024-06-15");

            GenericRecord result = converter.convert(input);
            Integer days = (Integer) result.getField("date");
            assertThat(DateConverter.daysToLocalDate(days)).isEqualTo(LocalDate.of(2024, 6, 15));
        }

        @Test
        @DisplayName("coerces String UUID to UUID")
        void coercesStringToUUID() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.UUIDType.get())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            String uuidStr = "550e8400-e29b-41d4-a716-446655440000";
            Map<String, Object> input = Map.of("id", uuidStr);

            GenericRecord result = converter.convert(input);
            assertThat(result.getField("id")).isEqualTo(UUID.fromString(uuidStr));
        }
    }

    // ==================== Optional Fields Tests ====================

    @Nested
    @DisplayName("Optional Fields")
    class OptionalFieldsTests {

        @Test
        @DisplayName("handles missing optional field")
        void handlesMissingOptionalField() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "name", Types.StringType.get()),
                    Types.NestedField.optional(2, "nickname", Types.StringType.get())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("name", "Alice");

            GenericRecord result = converter.convert(input);
            assertThat(result.getField("name")).isEqualTo("Alice");
            assertThat(result.getField("nickname")).isNull();
        }

        @Test
        @DisplayName("handles null optional field")
        void handlesNullOptionalField() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "name", Types.StringType.get()),
                    Types.NestedField.optional(2, "nickname", Types.StringType.get())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = new HashMap<>();
            input.put("name", "Alice");
            input.put("nickname", null);

            GenericRecord result = converter.convert(input);
            assertThat(result.getField("nickname")).isNull();
        }

        @Test
        @DisplayName("throws on missing required field")
        void throwsOnMissingRequiredField() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "name", Types.StringType.get())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of();

            assertThatThrownBy(() -> converter.convert(input))
                    .isInstanceOf(NullValueException.class)
                    .hasMessageContaining("name");
        }

        @Test
        @DisplayName("throws on null required field")
        void throwsOnNullRequiredField() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "name", Types.StringType.get())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = new HashMap<>();
            input.put("name", null);

            assertThatThrownBy(() -> converter.convert(input))
                    .isInstanceOf(NullValueException.class);
        }
    }

    // ==================== Batch Conversion Tests ====================

    @Nested
    @DisplayName("Batch Conversion")
    class BatchConversionTests {

        @Test
        @DisplayName("converts batch of records")
        void convertsBatch() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "name", Types.StringType.get())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            List<Map<String, Object>> batch = List.of(
                    Map.of("id", 1, "name", "Alice"),
                    Map.of("id", 2, "name", "Bob"),
                    Map.of("id", 3, "name", "Charlie")
            );

            List<GenericRecord> results = converter.convertBatch(batch);

            assertThat(results).hasSize(3);
            assertThat(results.get(0).getField("name")).isEqualTo("Alice");
            assertThat(results.get(1).getField("name")).isEqualTo("Bob");
            assertThat(results.get(2).getField("name")).isEqualTo("Charlie");
        }

        @Test
        @DisplayName("converts empty batch")
        void convertsEmptyBatch() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            List<GenericRecord> results = converter.convertBatch(List.of());
            assertThat(results).isEmpty();
        }
    }

    // ==================== Error Handling Tests ====================

    @Nested
    @DisplayName("Error Handling")
    class ErrorHandlingTests {

        @Test
        @DisplayName("convertWithErrors collects errors")
        void convertWithErrorsCollectsErrors() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "number", Types.IntegerType.get())
            );

            ConversionConfig collectErrors = ConversionConfig.builder()
                    .errorHandlingMode(ConversionConfig.ErrorHandlingMode.COLLECT_ERRORS)
                    .build();

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema, collectErrors);

            Map<String, Object> input = Map.of("number", "not a number");

            ConversionResult<GenericRecord> result = converter.convertWithErrors(input);

            assertThat(result.hasErrors()).isTrue();
            assertThat(result.getErrors()).isNotEmpty();
        }

        @Test
        @DisplayName("cached converter returns same instance")
        void cachedConverterReturnsSameInstance() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get())
            );

            IcebergSchemaConverter converter1 = IcebergSchemaConverter.cached(schema);
            IcebergSchemaConverter converter2 = IcebergSchemaConverter.cached(schema);

            assertThat(converter1).isSameAs(converter2);
        }

        @Test
        @DisplayName("provides field path in error messages")
        void providesFieldPathInErrors() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "user", Types.StructType.of(
                            Types.NestedField.required(2, "age", Types.IntegerType.get())
                    ))
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of(
                    "user", Map.of("age", "not a number")
            );

            assertThatThrownBy(() -> converter.convert(input))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("user")
                    .hasMessageContaining("age");
        }
    }

    // ==================== ConvertToMap Tests ====================

    @Nested
    @DisplayName("ConvertToMap")
    class ConvertToMapTests {

        @Test
        @DisplayName("convertToMap returns Map instead of GenericRecord")
        void convertToMapReturnsMap() {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "name", Types.StringType.get())
            );

            IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("id", 1, "name", "Test");

            Map<String, Object> result = converter.convertToMap(input);

            assertThat(result).containsEntry("id", 1).containsEntry("name", "Test");
        }
    }
}