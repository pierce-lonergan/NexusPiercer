package io.github.pierce.converter;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
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
 * Tests for AvroSchemaConverter.
 */
class AvroSchemaConverterTest {

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
        @DisplayName("converts record with primitive types")
        void convertsPrimitiveTypes() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .requiredBoolean("bool_field")
                    .requiredInt("int_field")
                    .requiredLong("long_field")
                    .requiredFloat("float_field")
                    .requiredDouble("double_field")
                    .requiredString("string_field")
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Map<String, Object> input = Map.of(
                    "bool_field", true,
                    "int_field", 42,
                    "long_field", 123456789L,
                    "float_field", 3.14f,
                    "double_field", 2.71828,
                    "string_field", "hello"
            );

            org.apache.avro.generic.GenericRecord result = converter.convert(input);

            assertThat(result.get("bool_field")).isEqualTo(true);
            assertThat(result.get("int_field")).isEqualTo(42);
            assertThat(result.get("long_field")).isEqualTo(123456789L);
            assertThat(result.get("float_field")).isEqualTo(3.14f);
            assertThat(result.get("double_field")).isEqualTo(2.71828);
            // Avro uses Utf8 for strings
            assertThat(result.get("string_field").toString()).isEqualTo("hello");
        }

        @Test
        @DisplayName("converts bytes field")
        void convertsBytesField() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .requiredBytes("data")
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            byte[] bytes = {1, 2, 3, 4, 5};
            Map<String, Object> input = Map.of("data", bytes);

            org.apache.avro.generic.GenericRecord result = converter.convert(input);

            ByteBuffer buffer = (ByteBuffer) result.get("data");
            byte[] resultBytes = new byte[buffer.remaining()];
            buffer.get(resultBytes);
            assertThat(resultBytes).isEqualTo(bytes);
        }
    }

    // ==================== Logical Type Tests ====================

    @Nested
    @DisplayName("Logical Types")
    class LogicalTypeTests {

        @Test
        @DisplayName("converts date logical type")
        void convertsDateLogicalType() {
            Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .name("date_field").type(dateSchema).noDefault()
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            LocalDate date = LocalDate.of(2024, 6, 15);
            Map<String, Object> input = Map.of("date_field", date);

            org.apache.avro.generic.GenericRecord result = converter.convert(input);

            // Date is stored as days since epoch
            assertThat(result.get("date_field")).isInstanceOf(Integer.class);
        }

        @Test
        @DisplayName("converts time-micros logical type")
        void convertsTimeMicrosLogicalType() {
            Schema timeSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .name("time_field").type(timeSchema).noDefault()
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            LocalTime time = LocalTime.of(14, 30, 45);
            Map<String, Object> input = Map.of("time_field", time);

            org.apache.avro.generic.GenericRecord result = converter.convert(input);

            assertThat(result.get("time_field")).isInstanceOf(Long.class);
        }

        @Test
        @DisplayName("converts timestamp-micros logical type")
        void convertsTimestampMicrosLogicalType() {
            Schema tsSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .name("ts_field").type(tsSchema).noDefault()
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Instant instant = Instant.parse("2024-06-15T12:30:45Z");
            Map<String, Object> input = Map.of("ts_field", instant);

            org.apache.avro.generic.GenericRecord result = converter.convert(input);

            assertThat(result.get("ts_field")).isInstanceOf(Long.class);
        }

        @Test
        @DisplayName("converts UUID logical type")
        void convertsUUIDLogicalType() {
            Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .name("uuid_field").type(uuidSchema).noDefault()
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            UUID uuid = UUID.randomUUID();
            Map<String, Object> input = Map.of("uuid_field", uuid);

            org.apache.avro.generic.GenericRecord result = converter.convert(input);

            assertThat(result.get("uuid_field").toString()).isEqualTo(uuid.toString());
        }

        @Test
        @DisplayName("converts decimal logical type")
        void convertsDecimalLogicalType() {
            Schema decimalSchema = LogicalTypes.decimal(10, 2)
                    .addToSchema(Schema.create(Schema.Type.BYTES));
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .name("price").type(decimalSchema).noDefault()
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            BigDecimal price = new BigDecimal("99.99");
            Map<String, Object> input = Map.of("price", price);

            org.apache.avro.generic.GenericRecord result = converter.convert(input);

            assertThat(result.get("price")).isInstanceOf(BigDecimal.class);
            assertThat((BigDecimal) result.get("price")).isEqualByComparingTo(price);
        }
    }

    // ==================== Nested Schema Tests ====================

    @Nested
    @DisplayName("Nested Schemas")
    class NestedSchemaTests {

        @Test
        @DisplayName("converts nested record")
        void convertsNestedRecord() {
            Schema addressSchema = SchemaBuilder.record("Address")
                    .fields()
                    .requiredString("street")
                    .requiredString("city")
                    .optionalString("zip")
                    .endRecord();

            Schema schema = SchemaBuilder.record("Person")
                    .fields()
                    .requiredInt("id")
                    .name("address").type(addressSchema).noDefault()
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Map<String, Object> input = Map.of(
                    "id", 123,
                    "address", Map.of(
                            "street", "123 Main St",
                            "city", "Springfield"
                    )
            );

            org.apache.avro.generic.GenericRecord result = converter.convert(input);

            assertThat(result.get("id")).isEqualTo(123);
            // Nested records are returned as Maps
            @SuppressWarnings("unchecked")
            Map<String, Object> address = (Map<String, Object>) result.get("address");
            assertThat(address.get("street").toString()).isEqualTo("123 Main St");
            assertThat(address.get("city").toString()).isEqualTo("Springfield");
        }

        @Test
        @DisplayName("converts array field")
        void convertsArrayField() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .name("tags").type().array().items().stringType().noDefault()
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("tags", List.of("java", "avro", "iceberg"));

            org.apache.avro.generic.GenericRecord result = converter.convert(input);

            @SuppressWarnings("unchecked")
            List<Object> tags = (List<Object>) result.get("tags");
            assertThat(tags).hasSize(3);
        }

        @Test
        @DisplayName("converts map field")
        void convertsMapField() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .name("metadata").type().map().values().stringType().noDefault()
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Map<String, Object> input = Map.of(
                    "metadata", Map.of("key1", "value1", "key2", "value2")
            );

            org.apache.avro.generic.GenericRecord result = converter.convert(input);

            @SuppressWarnings("unchecked")
            Map<Object, Object> metadata = (Map<Object, Object>) result.get("metadata");
            assertThat(metadata).hasSize(2);
        }
    }

    // ==================== Union Type Tests ====================

    @Nested
    @DisplayName("Union Types")
    class UnionTypeTests {

        @Test
        @DisplayName("handles nullable string")
        void handlesNullableString() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .optionalString("name")
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            // With value
            Map<String, Object> withValue = Map.of("name", "Alice");
            org.apache.avro.generic.GenericRecord result1 = converter.convert(withValue);
            assertThat(result1.get("name").toString()).isEqualTo("Alice");

            // With null
            Map<String, Object> withNull = new HashMap<>();
            withNull.put("name", null);
            org.apache.avro.generic.GenericRecord result2 = converter.convert(withNull);
            assertThat(result2.get("name")).isNull();
        }

        @Test
        @DisplayName("handles nullable int")
        void handlesNullableInt() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .optionalInt("count")
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("count", 42);
            org.apache.avro.generic.GenericRecord result = converter.convert(input);
            assertThat(result.get("count")).isEqualTo(42);
        }
    }

    // ==================== Enum Type Tests ====================

    @Nested
    @DisplayName("Enum Types")
    class EnumTypeTests {

        @Test
        @DisplayName("converts enum field")
        void convertsEnumField() {
            Schema enumSchema = SchemaBuilder.enumeration("Status")
                    .symbols("PENDING", "ACTIVE", "COMPLETED");
            Schema schema = SchemaBuilder.record("Task")
                    .fields()
                    .name("status").type(enumSchema).noDefault()
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("status", "ACTIVE");
            org.apache.avro.generic.GenericRecord result = converter.convert(input);

            assertThat(result.get("status")).isInstanceOf(GenericData.EnumSymbol.class);
            assertThat(result.get("status").toString()).isEqualTo("ACTIVE");
        }

        @Test
        @DisplayName("rejects invalid enum value")
        void rejectsInvalidEnumValue() {
            Schema enumSchema = SchemaBuilder.enumeration("Status")
                    .symbols("PENDING", "ACTIVE", "COMPLETED");
            Schema schema = SchemaBuilder.record("Task")
                    .fields()
                    .name("status").type(enumSchema).noDefault()
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("status", "INVALID");
            assertThatThrownBy(() -> converter.convert(input))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("Invalid enum symbol");
        }
    }

    // ==================== Type Coercion Tests ====================

    @Nested
    @DisplayName("Type Coercion")
    class TypeCoercionTests {

        @Test
        @DisplayName("coerces Integer to Long")
        void coercesIntegerToLong() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .requiredLong("value")
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("value", 42);
            org.apache.avro.generic.GenericRecord result = converter.convert(input);
            assertThat(result.get("value")).isEqualTo(42L);
        }

        @Test
        @DisplayName("coerces String to Integer")
        void coercesStringToInteger() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .requiredInt("count")
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("count", "123");
            org.apache.avro.generic.GenericRecord result = converter.convert(input);
            assertThat(result.get("count")).isEqualTo(123);
        }

        @Test
        @DisplayName("handles Avro Utf8 strings")
        void handlesAvroUtf8() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .requiredString("name")
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("name", new Utf8("Avro String"));
            org.apache.avro.generic.GenericRecord result = converter.convert(input);
            assertThat(result.get("name").toString()).isEqualTo("Avro String");
        }
    }

    // ==================== Batch Conversion Tests ====================

    @Nested
    @DisplayName("Batch Conversion")
    class BatchConversionTests {

        @Test
        @DisplayName("converts batch of records")
        void convertsBatch() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .requiredInt("id")
                    .requiredString("name")
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            List<Map<String, Object>> batch = List.of(
                    Map.of("id", 1, "name", "Alice"),
                    Map.of("id", 2, "name", "Bob"),
                    Map.of("id", 3, "name", "Charlie")
            );

            List<org.apache.avro.generic.GenericRecord> results = converter.convertBatch(batch);

            assertThat(results).hasSize(3);
            assertThat(results.get(0).get("name").toString()).isEqualTo("Alice");
            assertThat(results.get(1).get("name").toString()).isEqualTo("Bob");
            assertThat(results.get(2).get("name").toString()).isEqualTo("Charlie");
        }
    }

    // ==================== Error Handling Tests ====================

    @Nested
    @DisplayName("Error Handling")
    class ErrorHandlingTests {

        @Test
        @DisplayName("throws on missing required field")
        void throwsOnMissingRequiredField() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .requiredString("name")
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Map<String, Object> input = Map.of();
            assertThatThrownBy(() -> converter.convert(input))
                    .isInstanceOf(NullValueException.class)
                    .hasMessageContaining("name");
        }

        @Test
        @DisplayName("convertWithErrors collects errors")
        void convertWithErrorsCollectsErrors() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .requiredInt("number")
                    .endRecord();

            ConversionConfig collectErrors = ConversionConfig.builder()
                    .errorHandlingMode(ConversionConfig.ErrorHandlingMode.COLLECT_ERRORS)
                    .build();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema, collectErrors);

            Map<String, Object> input = Map.of("number", "not a number");
            ConversionResult<org.apache.avro.generic.GenericRecord> result =
                    converter.convertWithErrors(input);

            assertThat(result.hasErrors()).isTrue();
        }

        @Test
        @DisplayName("provides field path in error messages")
        void providesFieldPathInErrors() {
            Schema addressSchema = SchemaBuilder.record("Address")
                    .fields()
                    .requiredInt("zip")
                    .endRecord();

            Schema schema = SchemaBuilder.record("Person")
                    .fields()
                    .name("address").type(addressSchema).noDefault()
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Map<String, Object> input = Map.of(
                    "address", Map.of("zip", "not a number")
            );

            assertThatThrownBy(() -> converter.convert(input))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("zip");
        }
    }

    // ==================== Fixed Type Tests ====================

    @Nested
    @DisplayName("Fixed Types")
    class FixedTypeTests {

        @Test
        @DisplayName("converts fixed-length bytes")
        void convertsFixedLengthBytes() {
            Schema fixedSchema = Schema.createFixed("Hash", null, null, 16);
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .name("hash").type(fixedSchema).noDefault()
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            byte[] hash = new byte[16];
            new Random().nextBytes(hash);
            Map<String, Object> input = Map.of("hash", hash);

            org.apache.avro.generic.GenericRecord result = converter.convert(input);

            // Fixed type is converted to ByteBuffer
            assertThat(result.get("hash")).isInstanceOf(ByteBuffer.class);
            ByteBuffer buffer = (ByteBuffer) result.get("hash");
            byte[] resultBytes = new byte[buffer.remaining()];
            buffer.get(resultBytes);
            assertThat(resultBytes).isEqualTo(hash);
        }
    }

    // ==================== ConvertToMap Tests ====================

    @Nested
    @DisplayName("ConvertToMap")
    class ConvertToMapTests {

        @Test
        @DisplayName("convertToMap returns Map")
        void convertToMapReturnsMap() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .requiredInt("id")
                    .requiredString("name")
                    .endRecord();

            AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

            Map<String, Object> input = Map.of("id", 1, "name", "Test");
            Map<String, Object> result = converter.convertToMap(input);

            assertThat(result).containsEntry("id", 1);
            assertThat(result.get("name").toString()).isEqualTo("Test");
        }
    }
}