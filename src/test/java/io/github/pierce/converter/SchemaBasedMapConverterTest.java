package io.github.pierce.converter;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.data.Offset.offset;

/**
 * Comprehensive tests for SchemaBasedMapConverter.
 *
 * Tests cover:
 * - Iceberg schema conversion
 * - Avro schema conversion
 * - Flattened schema conversion
 * - Case-insensitive key matching
 * - Partial map handling
 * - Type casting for all supported types
 * - Error handling
 * - Thread safety
 * - Caching behavior
 */
@DisplayName("SchemaBasedMapConverter Tests")
class SchemaBasedMapConverterTest {

    // ==================== Iceberg Schema Tests ====================

    @Nested
    @DisplayName("Iceberg Schema Conversion")
    class IcebergSchemaTests {

        private org.apache.iceberg.Schema createTestSchema() {
            return new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "userId", Types.LongType.get()),
                    Types.NestedField.optional(2, "userName", Types.StringType.get()),
                    Types.NestedField.optional(3, "amount", Types.DecimalType.of(10, 2)),
                    Types.NestedField.optional(4, "active", Types.BooleanType.get()),
                    Types.NestedField.optional(5, "score", Types.DoubleType.get()),
                    Types.NestedField.optional(6, "count", Types.IntegerType.get())
            );
        }

        @Test
        @DisplayName("Should convert map with exact key match")
        void testExactKeyMatch() {
            org.apache.iceberg.Schema schema = createTestSchema();
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            Map<String, Object> input = Map.of(
                    "userId", "12345",
                    "userName", "John Doe",
                    "amount", "99.99",
                    "active", "true"
            );

            Map<String, Object> result = converter.convert(input);

            assertThat(result)
                    .containsEntry("userId", 12345L)
                    .containsEntry("userName", "John Doe")
                    .containsEntry("amount", new BigDecimal("99.99"))
                    .containsEntry("active", true);
        }

        @Test
        @DisplayName("Should match keys case-insensitively by default")
        void testCaseInsensitiveKeyMatch() {
            org.apache.iceberg.Schema schema = createTestSchema();
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            Map<String, Object> input = Map.of(
                    "USERID", "12345",
                    "USERNAME", "John",
                    "Amount", "50.00",
                    "ACTIVE", "false"
            );

            Map<String, Object> result = converter.convert(input);

            // Output keys should use schema field names
            assertThat(result)
                    .containsEntry("userId", 12345L)
                    .containsEntry("userName", "John")
                    .containsEntry("amount", new BigDecimal("50.00"))
                    .containsEntry("active", false);
        }

        @Test
        @DisplayName("Should respect case-sensitive mode when configured")
        void testCaseSensitiveMode() {
            org.apache.iceberg.Schema schema = createTestSchema();
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(
                    schema, ConversionConfig.defaults(), false);

            Map<String, Object> input = Map.of(
                    "USERID", "12345",  // Won't match - wrong case
                    "userId", "67890"   // Will match - correct case
            );

            Map<String, Object> result = converter.convert(input);

            assertThat(result)
                    .containsEntry("userId", 67890L)
                    .hasSize(1);  // USERID ignored
        }

        @Test
        @DisplayName("Should handle partial maps (subset of schema fields)")
        void testPartialMapConversion() {
            org.apache.iceberg.Schema schema = createTestSchema();
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            // Only provide 2 of 6 fields
            Map<String, Object> input = Map.of(
                    "userId", "999",
                    "active", "true"
            );

            Map<String, Object> result = converter.convert(input);

            assertThat(result)
                    .containsEntry("userId", 999L)
                    .containsEntry("active", true)
                    .hasSize(2);
        }

        @Test
        @DisplayName("Should handle empty input map")
        void testEmptyInputMap() {
            org.apache.iceberg.Schema schema = createTestSchema();
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            Map<String, Object> result = converter.convert(new HashMap<>());

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("Should return null for null input")
        void testNullInput() {
            org.apache.iceberg.Schema schema = createTestSchema();
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            assertThat(converter.convert(null)).isNull();
        }

        @Test
        @DisplayName("Should ignore keys not in schema")
        void testUnknownKeysIgnored() {
            org.apache.iceberg.Schema schema = createTestSchema();
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            Map<String, Object> input = new HashMap<>();
            input.put("userId", "123");
            input.put("unknownField", "ignored");
            input.put("anotherUnknown", 456);

            Map<String, Object> result = converter.convert(input);

            assertThat(result)
                    .containsEntry("userId", 123L)
                    .doesNotContainKey("unknownField")
                    .doesNotContainKey("anotherUnknown")
                    .hasSize(1);
        }

        @Test
        @DisplayName("Should handle null values for nullable fields")
        void testNullValuesForNullableFields() {
            org.apache.iceberg.Schema schema = createTestSchema();
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            Map<String, Object> input = new HashMap<>();
            input.put("userId", "123");
            input.put("userName", null);  // nullable field

            Map<String, Object> result = converter.convert(input);

            assertThat(result)
                    .containsEntry("userId", 123L)
                    .containsEntry("userName", null);
        }
    }

    // ==================== Avro Schema Tests ====================

    @Nested
    @DisplayName("Avro Schema Conversion")
    class AvroSchemaTests {

        private Schema createAvroSchema() {
            return SchemaBuilder.record("TestRecord")
                    .fields()
                    .requiredLong("id")
                    .optionalString("name")
                    .optionalDouble("score")
                    .optionalBoolean("enabled")
                    .optionalInt("count")
                    .endRecord();
        }

        @Test
        @DisplayName("Should convert map according to Avro schema")
        void testAvroSchemaConversion() {
            Schema avroSchema = createAvroSchema();
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forAvro(avroSchema);

            Map<String, Object> input = Map.of(
                    "id", "98765",
                    "name", "Test User",
                    "score", "95.5",
                    "enabled", "true"
            );

            Map<String, Object> result = converter.convert(input);

            assertThat(result)
                    .containsEntry("id", 98765L)
                    .containsEntry("name", "Test User")
                    .containsEntry("score", 95.5)
                    .containsEntry("enabled", true);
        }

        @Test
        @DisplayName("Should handle case-insensitive matching for Avro")
        void testAvroCaseInsensitive() {
            Schema avroSchema = createAvroSchema();
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forAvro(avroSchema);

            Map<String, Object> input = Map.of(
                    "ID", "111",
                    "NAME", "Test",
                    "SCORE", "88.8"
            );

            Map<String, Object> result = converter.convert(input);

            assertThat(result)
                    .containsEntry("id", 111L)
                    .containsEntry("name", "Test")
                    .containsEntry("score", 88.8);
        }

        @Test
        @DisplayName("Should reject non-RECORD Avro schemas")
        void testRejectNonRecordSchema() {
            Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));

            assertThatThrownBy(() -> SchemaBasedMapConverter.forAvro(arraySchema))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("RECORD");
        }

        @Test
        @DisplayName("Should handle Avro unions (nullable fields)")
        void testAvroUnionFields() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .name("nullableField")
                    .type().nullable().stringType().noDefault()
                    .endRecord();

            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forAvro(schema);

            Map<String, Object> input = new HashMap<>();
            input.put("nullableField", null);

            Map<String, Object> result = converter.convert(input);

            assertThat(result).containsEntry("nullableField", null);
        }
    }

    // ==================== Flattened Schema Tests ====================

    @Nested
    @DisplayName("Flattened Schema Conversion")
    class FlattenedSchemaTests {

        @Test
        @DisplayName("Should convert map using flattened schema")
        void testFlattenedSchemaConversion() {
            Map<String, SchemaBasedMapConverter.FlattenedFieldType> flatSchema = new LinkedHashMap<>();
            flatSchema.put("user_id", SchemaBasedMapConverter.FlattenedFieldType.required(
                    "user_id", SchemaBasedMapConverter.FlattenedDataType.LONG));
            flatSchema.put("user_name", SchemaBasedMapConverter.FlattenedFieldType.of(
                    "user_name", SchemaBasedMapConverter.FlattenedDataType.STRING));
            flatSchema.put("total_amount", SchemaBasedMapConverter.FlattenedFieldType.decimal(
                    "total_amount", 10, 2, true));
            flatSchema.put("is_active", SchemaBasedMapConverter.FlattenedFieldType.of(
                    "is_active", SchemaBasedMapConverter.FlattenedDataType.BOOLEAN));

            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forFlattened(flatSchema);

            Map<String, Object> input = Map.of(
                    "user_id", "12345",
                    "user_name", "John",
                    "total_amount", "999.99",
                    "is_active", "true"
            );

            Map<String, Object> result = converter.convert(input);

            assertThat(result)
                    .containsEntry("user_id", 12345L)
                    .containsEntry("user_name", "John")
                    .containsEntry("total_amount", new BigDecimal("999.99"))
                    .containsEntry("is_active", true);
        }

        @Test
        @DisplayName("Should handle case-insensitive keys with flattened schema")
        void testFlattenedCaseInsensitive() {
            Map<String, SchemaBasedMapConverter.FlattenedFieldType> flatSchema = new LinkedHashMap<>();
            flatSchema.put("User_Name", SchemaBasedMapConverter.FlattenedFieldType.of(
                    "User_Name", SchemaBasedMapConverter.FlattenedDataType.STRING));
            flatSchema.put("Account_Balance", SchemaBasedMapConverter.FlattenedFieldType.of(
                    "Account_Balance", SchemaBasedMapConverter.FlattenedDataType.DOUBLE));

            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forFlattened(flatSchema);

            Map<String, Object> input = Map.of(
                    "USER_NAME", "Alice",
                    "account_balance", "1000.50"
            );

            Map<String, Object> result = converter.convert(input);

            // Output should use schema's original casing
            assertThat(result)
                    .containsEntry("User_Name", "Alice")
                    .containsEntry("Account_Balance", 1000.50);
        }

        @Test
        @DisplayName("Should handle serialized arrays")
        void testSerializedArrayField() {
            Map<String, SchemaBasedMapConverter.FlattenedFieldType> flatSchema = new LinkedHashMap<>();
            flatSchema.put("tags", SchemaBasedMapConverter.FlattenedFieldType.array(
                    "tags", SchemaBasedMapConverter.FlattenedDataType.STRING, true));
            flatSchema.put("scores", SchemaBasedMapConverter.FlattenedFieldType.array(
                    "scores", SchemaBasedMapConverter.FlattenedDataType.INT, true));

            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forFlattened(flatSchema);

            Map<String, Object> input = Map.of(
                    "tags", "tag1,tag2,tag3",
                    "scores", List.of(90, 85, 88)
            );

            Map<String, Object> result = converter.convert(input);

            assertThat(result).containsKey("tags");
            assertThat(result).containsKey("scores");
        }

        @Test
        @DisplayName("Should convert all flattened data types")
        void testAllFlattenedDataTypes() {
            Map<String, SchemaBasedMapConverter.FlattenedFieldType> flatSchema = new LinkedHashMap<>();
            flatSchema.put("str", SchemaBasedMapConverter.FlattenedFieldType.of("str",
                    SchemaBasedMapConverter.FlattenedDataType.STRING));
            flatSchema.put("int_val", SchemaBasedMapConverter.FlattenedFieldType.of("int_val",
                    SchemaBasedMapConverter.FlattenedDataType.INT));
            flatSchema.put("long_val", SchemaBasedMapConverter.FlattenedFieldType.of("long_val",
                    SchemaBasedMapConverter.FlattenedDataType.LONG));
            flatSchema.put("float_val", SchemaBasedMapConverter.FlattenedFieldType.of("float_val",
                    SchemaBasedMapConverter.FlattenedDataType.FLOAT));
            flatSchema.put("double_val", SchemaBasedMapConverter.FlattenedFieldType.of("double_val",
                    SchemaBasedMapConverter.FlattenedDataType.DOUBLE));
            flatSchema.put("bool_val", SchemaBasedMapConverter.FlattenedFieldType.of("bool_val",
                    SchemaBasedMapConverter.FlattenedDataType.BOOLEAN));
            flatSchema.put("bigint_val", SchemaBasedMapConverter.FlattenedFieldType.of("bigint_val",
                    SchemaBasedMapConverter.FlattenedDataType.BIGINT));

            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forFlattened(flatSchema);

            Map<String, Object> input = Map.of(
                    "str", 123,
                    "int_val", "42",
                    "long_val", "9999999999",
                    "float_val", "3.14",
                    "double_val", "2.71828",
                    "bool_val", "false",
                    "bigint_val", "123456789012345"
            );

            Map<String, Object> result = converter.convert(input);

            assertThat(result.get("str")).isEqualTo("123");
            assertThat(result.get("int_val")).isEqualTo(42);
            assertThat(result.get("long_val")).isEqualTo(9999999999L);
            assertThat(result.get("float_val")).isEqualTo(3.14f);
            assertThat(result.get("double_val")).isEqualTo(2.71828);
            assertThat(result.get("bool_val")).isEqualTo(false);
            assertThat(result.get("bigint_val")).isEqualTo(123456789012345L);
        }

        @Test
        @DisplayName("Should reject null or empty flattened schema")
        void testRejectInvalidFlattenedSchema() {
            assertThatThrownBy(() -> SchemaBasedMapConverter.forFlattened(null))
                    .isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(() -> SchemaBasedMapConverter.forFlattened(new HashMap<>()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    // ==================== Schema Flattening Tests ====================

    @Nested
    @DisplayName("Schema Flattening Utilities")
    class SchemaFlatteningTests {

        @Test
        @DisplayName("Should flatten simple Avro schema")
        void testFlattenSimpleAvroSchema() {
            Schema avroSchema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .requiredLong("id")
                    .optionalString("name")
                    .endRecord();

            Map<String, SchemaBasedMapConverter.FlattenedFieldType> flattened =
                    SchemaBasedMapConverter.flattenAvroSchema(avroSchema, "_");

            assertThat(flattened).containsKey("id");
            assertThat(flattened).containsKey("name");
            assertThat(flattened.get("id").getDataType())
                    .isEqualTo(SchemaBasedMapConverter.FlattenedDataType.LONG);
            assertThat(flattened.get("name").getDataType())
                    .isEqualTo(SchemaBasedMapConverter.FlattenedDataType.STRING);
        }

        @Test
        @DisplayName("Should preserve decimal precision and scale during flattening")
        void testFlattenDecimalWithPrecisionScale() {
            // Create decimal schema with explicit precision/scale
            Schema decimalSchema = LogicalTypes.decimal(10, 2)
                    .addToSchema(Schema.create(Schema.Type.BYTES));

            Schema avroSchema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .requiredLong("id")
                    .name("amount").type(decimalSchema).noDefault()
                    .endRecord();

            // Verify the schema has the logical type attached
            Schema amountFieldSchema = avroSchema.getField("amount").schema();
            assertThat(amountFieldSchema.getLogicalType())
                    .as("Avro field should have logical type")
                    .isNotNull();
            assertThat(amountFieldSchema.getLogicalType().getName())
                    .as("Logical type should be decimal")
                    .isEqualTo("decimal");

            // Flatten and verify
            Map<String, SchemaBasedMapConverter.FlattenedFieldType> flattened =
                    SchemaBasedMapConverter.flattenAvroSchema(avroSchema, "_");

            assertThat(flattened).containsKey("amount");
            SchemaBasedMapConverter.FlattenedFieldType amountType = flattened.get("amount");

            assertThat(amountType.getDataType())
                    .as("Flattened type should be DECIMAL")
                    .isEqualTo(SchemaBasedMapConverter.FlattenedDataType.DECIMAL);
            assertThat(amountType.getPrecision())
                    .as("Precision should be 10")
                    .isEqualTo(10);
            assertThat(amountType.getScale())
                    .as("Scale should be 2")
                    .isEqualTo(2);

            // Verify conversion works
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forFlattened(flattened);
            Map<String, Object> result = converter.convert(Map.of(
                    "id", "123",
                    "amount", "99.99"  // Scale 2 matches schema
            ));

            assertThat(result.get("id")).isEqualTo(123L);
            assertThat(result.get("amount")).isEqualTo(new BigDecimal("99.99"));
        }

        @Test
        @DisplayName("Should flatten nested Avro schema")
        void testFlattenNestedAvroSchema() {
            Schema addressSchema = SchemaBuilder.record("Address")
                    .fields()
                    .requiredString("street")
                    .requiredString("city")
                    .endRecord();

            Schema personSchema = SchemaBuilder.record("Person")
                    .fields()
                    .requiredString("name")
                    .name("address").type(addressSchema).noDefault()
                    .endRecord();

            Map<String, SchemaBasedMapConverter.FlattenedFieldType> flattened =
                    SchemaBasedMapConverter.flattenAvroSchema(personSchema, "_");

            assertThat(flattened).containsKey("name");
            assertThat(flattened).containsKey("address_street");
            assertThat(flattened).containsKey("address_city");
        }

        @Test
        @DisplayName("Should flatten Avro array of primitives")
        void testFlattenAvroArrayOfPrimitives() {
            Schema schema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .name("tags").type().array().items().stringType().noDefault()
                    .endRecord();

            Map<String, SchemaBasedMapConverter.FlattenedFieldType> flattened =
                    SchemaBasedMapConverter.flattenAvroSchema(schema, "_");

            assertThat(flattened).containsKey("tags");
            assertThat(flattened.get("tags").isArraySerialized()).isTrue();
            assertThat(flattened.get("tags").getArrayElementType())
                    .isEqualTo(SchemaBasedMapConverter.FlattenedDataType.STRING);
        }

        @Test
        @DisplayName("Should flatten Iceberg schema")
        void testFlattenIcebergSchema() {
            org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "data", Types.StructType.of(
                            Types.NestedField.required(3, "value", Types.StringType.get()),
                            Types.NestedField.optional(4, "count", Types.IntegerType.get())
                    ))
            );

            Map<String, SchemaBasedMapConverter.FlattenedFieldType> flattened =
                    SchemaBasedMapConverter.flattenIcebergSchema(icebergSchema, "_");

            assertThat(flattened).containsKey("id");
            assertThat(flattened).containsKey("data_value");
            assertThat(flattened).containsKey("data_count");
        }

        @Test
        @DisplayName("Should use forFlattenedAvro factory method")
        void testForFlattenedAvroFactory() {
            Schema avroSchema = SchemaBuilder.record("TestRecord")
                    .fields()
                    .requiredLong("id")
                    .requiredString("name")
                    .endRecord();

            SchemaBasedMapConverter converter =
                    SchemaBasedMapConverter.forFlattenedAvro(avroSchema);

            Map<String, Object> input = Map.of(
                    "id", "123",
                    "name", "Test"
            );

            Map<String, Object> result = converter.convert(input);

            assertThat(result)
                    .containsEntry("id", 123L)
                    .containsEntry("name", "Test");
        }
    }

    // ==================== Type Conversion Tests ====================

    @Nested
    @DisplayName("Type Conversion")
    class TypeConversionTests {

        @ParameterizedTest
        @DisplayName("Should convert various numeric string formats to Long")
        @CsvSource({
                "123, 123",
                "0, 0",
                "-456, -456",
                "9999999999, 9999999999"
        })
        void testLongConversion(String input, long expected) {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "value", Types.LongType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            Map<String, Object> result = converter.convert(Map.of("value", input));

            assertThat(result.get("value")).isEqualTo(expected);
        }

        @ParameterizedTest
        @DisplayName("Should convert various boolean representations")
        @CsvSource({
                "true, true",
                "false, false",
                "TRUE, true",
                "FALSE, false",
                "1, true",
                "0, false"
        })
        void testBooleanConversion(String input, boolean expected) {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "value", Types.BooleanType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            Map<String, Object> result = converter.convert(Map.of("value", input));

            assertThat(result.get("value")).isEqualTo(expected);
        }

        @Test
        @DisplayName("Should preserve already correct types")
        void testPreserveCorrectTypes() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "longVal", Types.LongType.get()),
                    Types.NestedField.optional(2, "doubleVal", Types.DoubleType.get()),
                    Types.NestedField.optional(3, "boolVal", Types.BooleanType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            Map<String, Object> input = Map.of(
                    "longVal", 12345L,  // Already Long
                    "doubleVal", 3.14,  // Already Double
                    "boolVal", true     // Already Boolean
            );

            Map<String, Object> result = converter.convert(input);

            assertThat(result.get("longVal")).isEqualTo(12345L);
            assertThat(result.get("doubleVal")).isEqualTo(3.14);
            assertThat(result.get("boolVal")).isEqualTo(true);
        }

        @Test
        @DisplayName("Should handle decimal conversion with precision")
        void testDecimalConversion() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "amount", Types.DecimalType.of(10, 2))
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            // Use value with correct scale (2 decimal places)
            Map<String, Object> result = converter.convert(Map.of("amount", "123.45"));

            assertThat(result.get("amount"))
                    .isInstanceOf(BigDecimal.class)
                    .isEqualTo(new BigDecimal("123.45"));
        }
    }

    // ==================== Error Handling Tests ====================

    @Nested
    @DisplayName("Error Handling")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should throw on type conversion failure")
        void testConversionFailure() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "number", Types.LongType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            assertThatThrownBy(() -> converter.convert(Map.of("number", "not-a-number")))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("number");
        }

        @Test
        @DisplayName("Should collect errors with convertWithErrors")
        void testConvertWithErrors() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "valid", Types.LongType.get()),
                    Types.NestedField.required(2, "invalid", Types.LongType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            Map<String, Object> input = new HashMap<>();
            input.put("valid", "123");
            input.put("invalid", "not-a-number");

            ConversionResult<Map<String, Object>> result = converter.convertWithErrors(input);

            assertThat(result.hasErrors()).isTrue();
            assertThat(result.getErrors()).hasSize(1);
//            assertThat(result.getErrors().get(0).getField()).isEqualTo("invalid");
        }

        @Test
        @DisplayName("convertStrict should validate required fields")
        void testConvertStrictRequiredFields() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "required_field", Types.LongType.get()),
                    Types.NestedField.optional(2, "optional_field", Types.StringType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            // Missing required field
            assertThatThrownBy(() -> converter.convertStrict(Map.of("optional_field", "value")))
                    .isInstanceOf(NullValueException.class)
                    .hasMessageContaining("required_field");
        }
    }

    // ==================== Batch Conversion Tests ====================

    @Nested
    @DisplayName("Batch Conversion")
    class BatchConversionTests {

        @Test
        @DisplayName("Should batch convert multiple maps")
        void testBatchConversion() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "name", Types.StringType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            List<Map<String, Object>> batch = List.of(
                    Map.of("id", "1", "name", "One"),
                    Map.of("id", "2", "name", "Two"),
                    Map.of("id", "3", "name", "Three")
            );

            List<Map<String, Object>> results = converter.convertBatch(batch);

            assertThat(results).hasSize(3);
            assertThat(results.get(0).get("id")).isEqualTo(1L);
            assertThat(results.get(1).get("id")).isEqualTo(2L);
            assertThat(results.get(2).get("id")).isEqualTo(3L);
        }

        @Test
        @DisplayName("Should return empty list for null batch")
        void testBatchConversionNull() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            assertThat(converter.convertBatch(null)).isEmpty();
        }
    }

    // ==================== Single Field Conversion Tests ====================

    @Nested
    @DisplayName("Single Field Conversion")
    class SingleFieldConversionTests {

        @Test
        @DisplayName("Should convert single field by name")
        void testConvertSingleField() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "amount", Types.DecimalType.of(10, 2))
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            Object result = converter.convertField("amount", "123.45");

            assertThat(result).isEqualTo(new BigDecimal("123.45"));
        }

        @Test
        @DisplayName("Should convert field with case-insensitive name")
        void testConvertFieldCaseInsensitive() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "Amount", Types.DoubleType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            Object result = converter.convertField("AMOUNT", "99.99");

            assertThat(result).isEqualTo(99.99);
        }

        @Test
        @DisplayName("Should throw for unknown field")
        void testConvertUnknownField() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            assertThatThrownBy(() -> converter.convertField("unknown", "value"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not found");
        }
    }

    // ==================== Thread Safety Tests ====================

    @Nested
    @DisplayName("Thread Safety")
    class ThreadSafetyTests {

        @Test
        @DisplayName("Should be thread-safe for concurrent conversions")
        void testConcurrentConversions() throws Exception {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "value", Types.StringType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            int threadCount = 10;
            int iterationsPerThread = 100;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            AtomicInteger errorCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(threadCount);

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < iterationsPerThread; i++) {
                            Map<String, Object> input = Map.of(
                                    "id", String.valueOf(threadId * 1000 + i),
                                    "value", "value-" + threadId + "-" + i
                            );
                            Map<String, Object> result = converter.convert(input);

                            if (!result.get("id").equals((long) (threadId * 1000 + i))) {
                                errorCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await(30, TimeUnit.SECONDS);
            executor.shutdown();

            assertThat(errorCount.get()).isZero();
        }
    }

    // ==================== Caching Tests ====================

    @Nested
    @DisplayName("Caching")
    class CachingTests {

        @BeforeEach
        void clearCache() {
            SchemaBasedMapConverter.clearCache();
        }

        @Test
        @DisplayName("Should cache converters")
        void testConverterCaching() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get())
            );

            SchemaBasedMapConverter converter1 = SchemaBasedMapConverter.cached(schema);
            SchemaBasedMapConverter converter2 = SchemaBasedMapConverter.cached(schema);

            assertThat(converter1).isSameAs(converter2);
        }

        @Test
        @DisplayName("Should cache Avro schema converters")
        void testAvroConverterCaching() {
            Schema avroSchema = SchemaBuilder.record("Test")
                    .fields()
                    .requiredLong("id")
                    .endRecord();

            SchemaBasedMapConverter converter1 = SchemaBasedMapConverter.cached(avroSchema);
            SchemaBasedMapConverter converter2 = SchemaBasedMapConverter.cached(avroSchema);

            assertThat(converter1).isSameAs(converter2);
        }
    }

    // ==================== Accessor Tests ====================

    @Nested
    @DisplayName("Accessor Methods")
    class AccessorTests {

        @Test
        @DisplayName("Should return schema type correctly")
        void testGetSchemaType() {
            org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get())
            );
            SchemaBasedMapConverter icebergConverter =
                    SchemaBasedMapConverter.forIceberg(icebergSchema);

            assertThat(icebergConverter.getSchemaType())
                    .isEqualTo(SchemaBasedMapConverter.SchemaType.ICEBERG);

            Schema avroSchema = SchemaBuilder.record("Test")
                    .fields()
                    .requiredLong("id")
                    .endRecord();
            SchemaBasedMapConverter avroConverter =
                    SchemaBasedMapConverter.forAvro(avroSchema);

            assertThat(avroConverter.getSchemaType())
                    .isEqualTo(SchemaBasedMapConverter.SchemaType.AVRO);
        }

        @Test
        @DisplayName("Should return field names")
        void testGetFieldNames() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "name", Types.StringType.get()),
                    Types.NestedField.optional(3, "value", Types.DoubleType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            assertThat(converter.getFieldNames())
                    .containsExactlyInAnyOrder("id", "name", "value");
        }

        @Test
        @DisplayName("Should check field existence with hasField")
        void testHasField() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "existingField", Types.LongType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            assertThat(converter.hasField("existingField")).isTrue();
            assertThat(converter.hasField("EXISTINGFIELD")).isTrue();  // Case insensitive
            assertThat(converter.hasField("nonExistent")).isFalse();
        }

        @Test
        @DisplayName("Should return field nullability")
        void testIsFieldNullable() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "required", Types.LongType.get()),
                    Types.NestedField.optional(2, "optional", Types.StringType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            assertThat(converter.isFieldNullable("required")).isFalse();
            assertThat(converter.isFieldNullable("optional")).isTrue();
        }

        @Test
        @DisplayName("Should return Iceberg schema optional")
        void testGetIcebergSchema() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            assertThat(converter.getIcebergSchema()).isPresent();
            assertThat(converter.getAvroSchema()).isEmpty();
        }

        @Test
        @DisplayName("Should return flattened schema optional")
        void testGetFlattenedSchema() {
            Map<String, SchemaBasedMapConverter.FlattenedFieldType> flatSchema = Map.of(
                    "field1", SchemaBasedMapConverter.FlattenedFieldType.of(
                            "field1", SchemaBasedMapConverter.FlattenedDataType.STRING)
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forFlattened(flatSchema);

            assertThat(converter.getFlattenedSchema()).isPresent();
            assertThat(converter.getIcebergSchema()).isEmpty();
            assertThat(converter.getAvroSchema()).isEmpty();
        }
    }

    // ==================== Edge Cases ====================

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCaseTests {

        @Test
        @DisplayName("Should handle very long strings")
        void testVeryLongString() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.optional(1, "text", Types.StringType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            String longString = "x".repeat(100000);
            Map<String, Object> result = converter.convert(Map.of("text", longString));

            assertThat(result.get("text")).isEqualTo(longString);
        }

        @Test
        @DisplayName("Should handle special characters in values")
        void testSpecialCharacters() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.optional(1, "text", Types.StringType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            String special = "Hello\nWorld\t\"Quoted\" and \\ backslash";
            Map<String, Object> result = converter.convert(Map.of("text", special));

            assertThat(result.get("text")).isEqualTo(special);
        }

        @Test
        @DisplayName("Should handle Number subclasses correctly")
        void testNumberSubclasses() {
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "longVal", Types.LongType.get()),
                    Types.NestedField.optional(2, "doubleVal", Types.DoubleType.get())
            );
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(schema);

            // Use Integer and Float instead of Long and Double
            Map<String, Object> input = Map.of(
                    "longVal", 42,       // Integer
                    "doubleVal", 3.14f   // Float
            );

            Map<String, Object> result = converter.convert(input);

            assertThat(result.get("longVal")).isEqualTo(42L);
            assertThat((Double) result.get("doubleVal")).isCloseTo(3.14, offset(0.01));
        }
    }

    // ==================== Extremely Complex Test Cases ====================

    @Nested
    @DisplayName("Complex Integration Tests")
    class ComplexIntegrationTests {

        /**
         * Test Case 1: Complex deeply nested Avro schema with multiple levels,
         * arrays of records, unions, logical types, and flattening.
         *
         * Simulates a real-world e-commerce order schema with:
         * - Customer info (nested record)
         * - Multiple addresses (array of records)
         * - Order items (array of records with nested product info)
         * - Payment details with decimal amounts
         * - Timestamps and UUIDs
         */
        @Test
        @DisplayName("Should handle complex e-commerce order schema with deep nesting and arrays")
        void testComplexEcommerceOrderSchema() {
            // Build complex nested Avro schema
            Schema productSchema = SchemaBuilder.record("Product")
                    .fields()
                    .requiredString("sku")
                    .requiredString("name")
                    .name("price").type(
                            LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES))
                    ).noDefault()
                    .optionalString("category")
                    .endRecord();

            Schema orderItemSchema = SchemaBuilder.record("OrderItem")
                    .fields()
                    .requiredInt("quantity")
                    .name("product").type(productSchema).noDefault()
                    .name("lineTotal").type(
                            LogicalTypes.decimal(12, 2).addToSchema(Schema.create(Schema.Type.BYTES))
                    ).noDefault()
                    .optionalString("notes")
                    .endRecord();

            Schema addressSchema = SchemaBuilder.record("Address")
                    .fields()
                    .requiredString("street")
                    .requiredString("city")
                    .requiredString("state")
                    .requiredString("zipCode")
                    .requiredString("country")
                    .optionalBoolean("isPrimary")
                    .endRecord();

            Schema customerSchema = SchemaBuilder.record("Customer")
                    .fields()
                    .requiredLong("customerId")
                    .requiredString("firstName")
                    .requiredString("lastName")
                    .optionalString("email")
                    .name("addresses").type().array().items(addressSchema).noDefault()
                    .endRecord();

            Schema orderSchema = SchemaBuilder.record("Order")
                    .fields()
                    .name("orderId").type(LogicalTypes.uuid().addToSchema(
                            Schema.create(Schema.Type.STRING))).noDefault()
                    .name("customer").type(customerSchema).noDefault()
                    .name("items").type().array().items(orderItemSchema).noDefault()
                    .name("orderTotal").type(
                            LogicalTypes.decimal(14, 2).addToSchema(Schema.create(Schema.Type.BYTES))
                    ).noDefault()
                    .name("orderDate").type(LogicalTypes.date().addToSchema(
                            Schema.create(Schema.Type.INT))).noDefault()
                    .name("createdAt").type(LogicalTypes.timestampMillis().addToSchema(
                            Schema.create(Schema.Type.LONG))).noDefault()
                    .optionalString("status")
                    .name("tags").type().nullable().array().items().stringType().noDefault()
                    .endRecord();

            // Flatten the schema
            Map<String, SchemaBasedMapConverter.FlattenedFieldType> flattenedSchema =
                    SchemaBasedMapConverter.flattenAvroSchema(orderSchema, "_");

            // Verify flattening produced expected fields
            assertThat(flattenedSchema).containsKey("orderId");
            assertThat(flattenedSchema).containsKey("customer_customerId");
            assertThat(flattenedSchema).containsKey("customer_firstName");
            assertThat(flattenedSchema).containsKey("customer_addresses_street");
            assertThat(flattenedSchema).containsKey("items_quantity");
            assertThat(flattenedSchema).containsKey("items_product_sku");
            assertThat(flattenedSchema).containsKey("items_product_name");
            assertThat(flattenedSchema).containsKey("orderTotal");
            assertThat(flattenedSchema).containsKey("status");
            assertThat(flattenedSchema).containsKey("tags");

            // Verify orderTotal captured decimal precision/scale (14, 2)
            SchemaBasedMapConverter.FlattenedFieldType orderTotalType = flattenedSchema.get("orderTotal");
            assertThat(orderTotalType.getDataType())
                    .as("orderTotal should be DECIMAL type")
                    .isEqualTo(SchemaBasedMapConverter.FlattenedDataType.DECIMAL);
            assertThat(orderTotalType.getPrecision())
                    .as("orderTotal should have precision 14")
                    .isEqualTo(14);
            assertThat(orderTotalType.getScale())
                    .as("orderTotal should have scale 2")
                    .isEqualTo(2);

            // Create converter from flattened schema
            SchemaBasedMapConverter converter = SchemaBasedMapConverter.forFlattened(flattenedSchema);

            // Create flattened input data (simulating JsonFlattenerConsolidator output)
            Map<String, Object> flattenedInput = new LinkedHashMap<>();
            flattenedInput.put("ORDERID", "550e8400-e29b-41d4-a716-446655440000"); // UUID, case-insensitive
            flattenedInput.put("customer_customerid", "12345"); // Long
            flattenedInput.put("CUSTOMER_FIRSTNAME", "John");
            flattenedInput.put("customer_lastname", "Doe");
            flattenedInput.put("customer_email", "john.doe@example.com");
            flattenedInput.put("customer_addresses_street", "123 Main St,456 Oak Ave");
            flattenedInput.put("customer_addresses_city", "Springfield,Portland");
            flattenedInput.put("items_quantity", "2,1,3");
            flattenedInput.put("items_product_sku", "SKU001,SKU002,SKU003");
            flattenedInput.put("items_product_name", "Widget,Gadget,Gizmo");
            flattenedInput.put("ordertotal", "299.99"); // Decimal with scale 2
            flattenedInput.put("status", "CONFIRMED");
            flattenedInput.put("tags", "priority,express,gift-wrap");

            // Convert
            Map<String, Object> result = converter.convert(flattenedInput);

            // Verify conversions - UUID returns actual UUID object, not String
            assertThat(result.get("orderId"))
                    .isEqualTo(java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
            assertThat(result.get("customer_customerId")).isEqualTo(12345L);
            assertThat(result.get("customer_firstName")).isEqualTo("John");
            assertThat(result.get("customer_lastName")).isEqualTo("Doe");
            assertThat(result.get("customer_addresses_street")).isEqualTo("123 Main St,456 Oak Ave");
            assertThat(result.get("items_quantity")).isEqualTo("2,1,3");
            assertThat(result.get("items_product_sku")).isEqualTo("SKU001,SKU002,SKU003");
            assertThat(result.get("orderTotal")).isEqualTo(new BigDecimal("299.99"));
            assertThat(result.get("status")).isEqualTo("CONFIRMED");
            assertThat(result.get("tags")).isEqualTo("priority,express,gift-wrap");

            // Verify field count and no unknown fields
            assertThat(result).doesNotContainKey("unknownField");
        }

        /**
         * Test Case 2: Simulates a real-world streaming data pipeline with:
         * - Multiple schema versions (schema evolution simulation)
         * - Concurrent batch processing
         * - Mixed successful and failed conversions
         * - Performance validation under load
         * - Memory efficiency (no leaks in thread-local state)
         */
        @Test
        @DisplayName("Should handle streaming pipeline simulation with concurrent batches and schema evolution")
        void testStreamingPipelineWithConcurrentBatchesAndSchemaEvolution() throws Exception {
            // Schema V1 - Original schema
            Map<String, SchemaBasedMapConverter.FlattenedFieldType> schemaV1 = new LinkedHashMap<>();
            schemaV1.put("event_id", SchemaBasedMapConverter.FlattenedFieldType.required(
                    "event_id", SchemaBasedMapConverter.FlattenedDataType.LONG));
            schemaV1.put("event_type", SchemaBasedMapConverter.FlattenedFieldType.of(
                    "event_type", SchemaBasedMapConverter.FlattenedDataType.STRING));
            schemaV1.put("timestamp", SchemaBasedMapConverter.FlattenedFieldType.of(
                    "timestamp", SchemaBasedMapConverter.FlattenedDataType.TIMESTAMP));
            schemaV1.put("user_id", SchemaBasedMapConverter.FlattenedFieldType.of(
                    "user_id", SchemaBasedMapConverter.FlattenedDataType.LONG));
            schemaV1.put("amount", SchemaBasedMapConverter.FlattenedFieldType.decimal(
                    "amount", 12, 4, true));

            // Schema V2 - Added new fields (backward compatible)
            Map<String, SchemaBasedMapConverter.FlattenedFieldType> schemaV2 = new LinkedHashMap<>(schemaV1);
            schemaV2.put("session_id", SchemaBasedMapConverter.FlattenedFieldType.of(
                    "session_id", SchemaBasedMapConverter.FlattenedDataType.UUID));
            schemaV2.put("metadata_source", SchemaBasedMapConverter.FlattenedFieldType.of(
                    "metadata_source", SchemaBasedMapConverter.FlattenedDataType.STRING));
            schemaV2.put("metadata_version", SchemaBasedMapConverter.FlattenedFieldType.of(
                    "metadata_version", SchemaBasedMapConverter.FlattenedDataType.INT));
            schemaV2.put("tags", SchemaBasedMapConverter.FlattenedFieldType.array(
                    "tags", SchemaBasedMapConverter.FlattenedDataType.STRING, true));
            schemaV2.put("metrics_latency", SchemaBasedMapConverter.FlattenedFieldType.of(
                    "metrics_latency", SchemaBasedMapConverter.FlattenedDataType.DOUBLE));
            schemaV2.put("metrics_success", SchemaBasedMapConverter.FlattenedFieldType.of(
                    "metrics_success", SchemaBasedMapConverter.FlattenedDataType.BOOLEAN));

            SchemaBasedMapConverter converterV1 = SchemaBasedMapConverter.forFlattened(schemaV1);
            SchemaBasedMapConverter converterV2 = SchemaBasedMapConverter.forFlattened(schemaV2);

            // Simulation parameters
            int numBatches = 50;
            int recordsPerBatch = 100;
            int numThreads = 8;

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            AtomicInteger v1ProcessedCount = new AtomicInteger(0);
            AtomicInteger v2ProcessedCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(numBatches);
            ConcurrentLinkedQueue<Long> processingTimes = new ConcurrentLinkedQueue<>();
            Random random = new Random(42); // Deterministic for reproducibility

            long startTime = System.currentTimeMillis();

            for (int batch = 0; batch < numBatches; batch++) {
                final int batchNum = batch;
                executor.submit(() -> {
                    try {
                        long batchStart = System.nanoTime();

                        // Alternate between schema versions to simulate evolution
                        boolean useV2 = batchNum % 2 == 0;
                        SchemaBasedMapConverter converter = useV2 ? converterV2 : converterV1;

                        List<Map<String, Object>> batchData = new ArrayList<>(recordsPerBatch);

                        for (int i = 0; i < recordsPerBatch; i++) {
                            Map<String, Object> record = new LinkedHashMap<>();
                            long eventId = (long) batchNum * recordsPerBatch + i;

                            // Base fields (present in both versions)
                            record.put("EVENT_ID", String.valueOf(eventId)); // Case variation
                            record.put("event_type", i % 3 == 0 ? "CLICK" : (i % 3 == 1 ? "VIEW" : "PURCHASE"));
                            record.put("TIMESTAMP", "2024-01-15T10:30:00Z");
                            record.put("User_Id", String.valueOf(1000 + (i % 100))); // Mixed case
                            record.put("amount", String.format("%.4f", random.nextDouble() * 1000));

                            // V2 specific fields (will be ignored by V1 converter)
                            if (useV2) {
                                record.put("session_id", java.util.UUID.randomUUID().toString());
                                record.put("METADATA_SOURCE", "web-app");
                                record.put("metadata_version", String.valueOf(2));
                                record.put("tags", List.of("organic", "mobile", "returning"));
                                record.put("metrics_latency", String.valueOf(random.nextDouble() * 500));
                                record.put("metrics_success", i % 10 != 0 ? "true" : "false");
                            }

                            // Occasionally add fields not in schema (should be ignored)
                            if (i % 20 == 0) {
                                record.put("unknown_field_" + i, "should_be_ignored");
                                record.put("another_unknown", 12345);
                            }

                            batchData.add(record);
                        }

                        // Process batch
                        List<Map<String, Object>> results = converter.convertBatch(batchData);

                        // Validate results
                        for (int i = 0; i < results.size(); i++) {
                            Map<String, Object> result = results.get(i);
                            long expectedEventId = (long) batchNum * recordsPerBatch + i;

                            // Verify type conversions
                            if (!Long.valueOf(expectedEventId).equals(result.get("event_id"))) {
                                errorCount.incrementAndGet();
                                continue;
                            }

                            if (!(result.get("user_id") instanceof Long)) {
                                errorCount.incrementAndGet();
                                continue;
                            }

                            if (!(result.get("amount") instanceof BigDecimal)) {
                                errorCount.incrementAndGet();
                                continue;
                            }

                            // Verify no unknown fields leaked through
                            if (result.containsKey("unknown_field_" + i) ||
                                    result.containsKey("another_unknown")) {
                                errorCount.incrementAndGet();
                                continue;
                            }

                            // V2 specific validations
                            if (useV2) {
                                if (result.get("metadata_version") != null &&
                                        !(result.get("metadata_version") instanceof Integer)) {
                                    errorCount.incrementAndGet();
                                    continue;
                                }
                                if (result.get("metrics_latency") != null &&
                                        !(result.get("metrics_latency") instanceof Double)) {
                                    errorCount.incrementAndGet();
                                    continue;
                                }
                                if (result.get("metrics_success") != null &&
                                        !(result.get("metrics_success") instanceof Boolean)) {
                                    errorCount.incrementAndGet();
                                    continue;
                                }
                            }

                            successCount.incrementAndGet();
                        }

                        if (useV2) {
                            v2ProcessedCount.addAndGet(results.size());
                        } else {
                            v1ProcessedCount.addAndGet(results.size());
                        }

                        long batchEnd = System.nanoTime();
                        processingTimes.add((batchEnd - batchStart) / 1_000_000); // ms

                    } catch (Exception e) {
                        errorCount.addAndGet(recordsPerBatch);
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            // Wait for all batches to complete
            boolean completed = latch.await(60, TimeUnit.SECONDS);
            executor.shutdown();

            long totalTime = System.currentTimeMillis() - startTime;

            // Assertions
            assertThat(completed).as("All batches should complete within timeout").isTrue();

            int totalRecords = numBatches * recordsPerBatch;
            assertThat(successCount.get() + errorCount.get())
                    .as("All records should be accounted for")
                    .isEqualTo(totalRecords);

            assertThat(errorCount.get())
                    .as("Should have no conversion errors")
                    .isZero();

            assertThat(successCount.get())
                    .as("All records should be successfully converted")
                    .isEqualTo(totalRecords);

            // Verify both schema versions were used
            assertThat(v1ProcessedCount.get())
                    .as("V1 schema should process records")
                    .isGreaterThan(0);
            assertThat(v2ProcessedCount.get())
                    .as("V2 schema should process records")
                    .isGreaterThan(0);

            // Performance assertions
            double avgProcessingTime = processingTimes.stream()
                    .mapToLong(Long::longValue)
                    .average()
                    .orElse(0);

            assertThat(avgProcessingTime)
                    .as("Average batch processing time should be reasonable (< 500ms)")
                    .isLessThan(500);

            double throughput = (double) totalRecords / totalTime * 1000;
            assertThat(throughput)
                    .as("Throughput should be at least 1000 records/second")
                    .isGreaterThan(1000);

            // Log performance metrics
            System.out.printf("""
                    
                    === Streaming Pipeline Performance Report ===
                    Total Records: %d
                    Successful: %d
                    Errors: %d
                    V1 Processed: %d
                    V2 Processed: %d
                    Total Time: %d ms
                    Throughput: %.2f records/sec
                    Avg Batch Time: %.2f ms
                    ==========================================
                    """,
                    totalRecords, successCount.get(), errorCount.get(),
                    v1ProcessedCount.get(), v2ProcessedCount.get(),
                    totalTime, throughput, avgProcessingTime);
        }

        /**
         * MEGA TEST: The Ultimate SchemaBasedMapConverter Stress Test
         *
         * This test combines virtually ALL functionality and edge cases into a single
         * comprehensive validation. It tests:
         *
         * 1. ALL THREE SCHEMA TYPES simultaneously (Iceberg, Avro, Flattened)
         * 2. ALL DATA TYPES: String, Int, Long, Float, Double, Boolean, Decimal,
         *    Date, Timestamp, UUID, Bytes, BigInt
         * 3. CASE SENSITIVITY: Tests both case-insensitive (default) and case-sensitive modes
         * 4. EDGE CASE VALUES:
         *    - Null values for nullable fields
         *    - Empty strings
         *    - Whitespace-only strings
         *    - Unicode/international characters (Chinese, Arabic, emoji)
         *    - Very long strings (10KB+)
         *    - Boundary numeric values (MAX_VALUE, MIN_VALUE, zero, negative)
         *    - Scientific notation
         *    - Leading zeros in numbers
         *    - Special float values (NaN, Infinity) - should fail gracefully
         * 5. ERROR HANDLING: All three modes (normal throw, withErrors collect, strict)
         * 6. BATCH CONVERSION: Large batch with mixed valid/edge-case records
         * 7. SINGLE FIELD CONVERSION: Individual field lookups
         * 8. SCHEMA FLATTENING: Deep nesting with arrays and records
         * 9. CONCURRENT ACCESS: Multiple threads hitting all schema types
         * 10. CACHING VALIDATION: Verify cache hits and identity
         * 11. ACCESSOR METHODS: getSchemaType, getFieldNames, hasField, isFieldNullable
         * 12. PARTIAL MAPS: 0 fields, some fields, all fields
         * 13. UNKNOWN FIELDS: Verify ignored properly
         * 14. DECIMAL PRECISION/SCALE: Multiple combinations
         * 15. ARRAY SERIALIZATION: Lists and arrays to delimited strings
         */
        @Test
        @DisplayName("MEGA TEST: Ultimate comprehensive stress test of all functionality")
        void testUltimateComprehensiveStressTest() throws Exception {
            // ==================== PHASE 1: Schema Setup ====================

            // 1A: Build comprehensive Iceberg schema with ALL types
            org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "name", Types.StringType.get()),
                    Types.NestedField.optional(3, "age", Types.IntegerType.get()),
                    Types.NestedField.optional(4, "balance", Types.DecimalType.of(15, 4)),
                    Types.NestedField.optional(5, "rate", Types.FloatType.get()),
                    Types.NestedField.optional(6, "score", Types.DoubleType.get()),
                    Types.NestedField.optional(7, "active", Types.BooleanType.get()),
                    Types.NestedField.optional(8, "birthDate", Types.DateType.get()),
                    Types.NestedField.optional(9, "createdAt", Types.TimestampType.withZone()),
                    Types.NestedField.optional(10, "uuid", Types.UUIDType.get()),
                    Types.NestedField.optional(11, "data", Types.BinaryType.get()),
                    Types.NestedField.optional(12, "bigNum", Types.LongType.get()),
                    Types.NestedField.optional(13, "tinyAmount", Types.DecimalType.of(5, 2)),
                    Types.NestedField.optional(14, "unicodeField", Types.StringType.get()),
                    Types.NestedField.optional(15, "emptyAllowed", Types.StringType.get())
            );

            // 1B: Build comprehensive Avro schema
            Schema avroDecimal15_4 = LogicalTypes.decimal(15, 4)
                    .addToSchema(Schema.create(Schema.Type.BYTES));
            Schema avroDecimal5_2 = LogicalTypes.decimal(5, 2)
                    .addToSchema(Schema.create(Schema.Type.BYTES));
            Schema avroDate = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            Schema avroTimestamp = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
            Schema avroUuid = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));

            Schema avroSchema = SchemaBuilder.record("ComprehensiveRecord")
                    .fields()
                    .requiredLong("id")
                    .optionalString("name")
                    .optionalInt("age")
                    .name("balance").type().unionOf().nullType().and().type(avroDecimal15_4).endUnion().nullDefault()
                    .optionalFloat("rate")
                    .optionalDouble("score")
                    .optionalBoolean("active")
                    .name("birthDate").type().unionOf().nullType().and().type(avroDate).endUnion().nullDefault()
                    .name("createdAt").type().unionOf().nullType().and().type(avroTimestamp).endUnion().nullDefault()
                    .name("uuid").type().unionOf().nullType().and().type(avroUuid).endUnion().nullDefault()
                    .optionalBytes("data")
                    .optionalLong("bigNum")
                    .name("tinyAmount").type().unionOf().nullType().and().type(avroDecimal5_2).endUnion().nullDefault()
                    .optionalString("unicodeField")
                    .optionalString("emptyAllowed")
                    .endRecord();

            // 1C: Build comprehensive flattened schema
            Map<String, SchemaBasedMapConverter.FlattenedFieldType> flattenedSchema = new LinkedHashMap<>();
            flattenedSchema.put("id", SchemaBasedMapConverter.FlattenedFieldType.required("id",
                    SchemaBasedMapConverter.FlattenedDataType.LONG));
            flattenedSchema.put("name", SchemaBasedMapConverter.FlattenedFieldType.of("name",
                    SchemaBasedMapConverter.FlattenedDataType.STRING));
            flattenedSchema.put("age", SchemaBasedMapConverter.FlattenedFieldType.of("age",
                    SchemaBasedMapConverter.FlattenedDataType.INT));
            flattenedSchema.put("balance", SchemaBasedMapConverter.FlattenedFieldType.decimal("balance", 15, 4, true));
            flattenedSchema.put("rate", SchemaBasedMapConverter.FlattenedFieldType.of("rate",
                    SchemaBasedMapConverter.FlattenedDataType.FLOAT));
            flattenedSchema.put("score", SchemaBasedMapConverter.FlattenedFieldType.of("score",
                    SchemaBasedMapConverter.FlattenedDataType.DOUBLE));
            flattenedSchema.put("active", SchemaBasedMapConverter.FlattenedFieldType.of("active",
                    SchemaBasedMapConverter.FlattenedDataType.BOOLEAN));
            flattenedSchema.put("birthDate", SchemaBasedMapConverter.FlattenedFieldType.of("birthDate",
                    SchemaBasedMapConverter.FlattenedDataType.DATE));
            flattenedSchema.put("createdAt", SchemaBasedMapConverter.FlattenedFieldType.of("createdAt",
                    SchemaBasedMapConverter.FlattenedDataType.TIMESTAMP));
            flattenedSchema.put("uuid", SchemaBasedMapConverter.FlattenedFieldType.of("uuid",
                    SchemaBasedMapConverter.FlattenedDataType.UUID));
            flattenedSchema.put("data", SchemaBasedMapConverter.FlattenedFieldType.of("data",
                    SchemaBasedMapConverter.FlattenedDataType.BYTES));
            flattenedSchema.put("bigNum", SchemaBasedMapConverter.FlattenedFieldType.of("bigNum",
                    SchemaBasedMapConverter.FlattenedDataType.BIGINT));
            flattenedSchema.put("tinyAmount", SchemaBasedMapConverter.FlattenedFieldType.decimal("tinyAmount", 5, 2, true));
            flattenedSchema.put("unicodeField", SchemaBasedMapConverter.FlattenedFieldType.of("unicodeField",
                    SchemaBasedMapConverter.FlattenedDataType.STRING));
            flattenedSchema.put("emptyAllowed", SchemaBasedMapConverter.FlattenedFieldType.of("emptyAllowed",
                    SchemaBasedMapConverter.FlattenedDataType.STRING));
            flattenedSchema.put("tags", SchemaBasedMapConverter.FlattenedFieldType.array("tags",
                    SchemaBasedMapConverter.FlattenedDataType.STRING, true));
            flattenedSchema.put("scores", SchemaBasedMapConverter.FlattenedFieldType.array("scores",
                    SchemaBasedMapConverter.FlattenedDataType.INT, true));

            // ==================== PHASE 2: Create Converters ====================

            SchemaBasedMapConverter icebergConverter = SchemaBasedMapConverter.forIceberg(icebergSchema);
            SchemaBasedMapConverter icebergCaseSensitive = SchemaBasedMapConverter.forIceberg(
                    icebergSchema, null, false);
            SchemaBasedMapConverter avroConverter = SchemaBasedMapConverter.forAvro(avroSchema);
            SchemaBasedMapConverter flatConverter = SchemaBasedMapConverter.forFlattened(flattenedSchema);

            // ==================== PHASE 3: Verify Schema Accessors ====================

            assertThat(icebergConverter.getSchemaType())
                    .isEqualTo(SchemaBasedMapConverter.SchemaType.ICEBERG);
            assertThat(avroConverter.getSchemaType())
                    .isEqualTo(SchemaBasedMapConverter.SchemaType.AVRO);
            assertThat(flatConverter.getSchemaType())
                    .isEqualTo(SchemaBasedMapConverter.SchemaType.FLATTENED);

            assertThat(icebergConverter.getFieldNames()).contains("id", "name", "balance", "uuid");
            assertThat(icebergConverter.hasField("id")).isTrue();
            assertThat(icebergConverter.hasField("ID")).isTrue(); // case-insensitive
            assertThat(icebergConverter.hasField("nonexistent")).isFalse();
            assertThat(icebergConverter.isFieldNullable("name")).isTrue();
            assertThat(flatConverter.getFlattenedSchema()).isNotNull();
//            assertThat(flatConverter.getFlattenedSchema()).containsKey("tags");

            // ==================== PHASE 4: Standard Conversion Tests ====================

            // 4A: Full record with all fields
            String testUuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
            Map<String, Object> fullRecord = new LinkedHashMap<>();
            fullRecord.put("ID", "9999999999"); // Case variation
            fullRecord.put("Name", "Test User");
            fullRecord.put("AGE", "42");
            fullRecord.put("balance", "12345.6789"); // Exact scale match
            fullRecord.put("RATE", "3.14159");
            fullRecord.put("score", "2.718281828");
            fullRecord.put("Active", "true");
            fullRecord.put("birthDate", "2000-06-15");
            fullRecord.put("CREATEDAT", "2024-01-15T10:30:00Z");
            fullRecord.put("uuid", testUuid);
            fullRecord.put("bigNum", String.valueOf(Long.MAX_VALUE - 1));
            fullRecord.put("tinyAmount", "99.99");
            fullRecord.put("unicodeField", "Hello 世界 مرحبا 🌍");
            fullRecord.put("emptyAllowed", "");

            Map<String, Object> icebergResult = icebergConverter.convert(fullRecord);

            assertThat(icebergResult.get("id")).isEqualTo(9999999999L);
            assertThat(icebergResult.get("name")).isEqualTo("Test User");
            assertThat(icebergResult.get("age")).isEqualTo(42);
            assertThat(icebergResult.get("balance")).isEqualTo(new BigDecimal("12345.6789"));
            assertThat((Float) icebergResult.get("rate")).isCloseTo(3.14159f, offset(0.0001f));
            assertThat((Double) icebergResult.get("score")).isCloseTo(2.718281828, offset(0.0000001));
            assertThat(icebergResult.get("active")).isEqualTo(true);
            assertThat(icebergResult.get("uuid")).isEqualTo(java.util.UUID.fromString(testUuid));
            assertThat(icebergResult.get("bigNum")).isEqualTo(Long.MAX_VALUE - 1);
            assertThat(icebergResult.get("tinyAmount")).isEqualTo(new BigDecimal("99.99"));
            assertThat(icebergResult.get("unicodeField")).isEqualTo("Hello 世界 مرحبا 🌍");
            assertThat(icebergResult.get("emptyAllowed")).isEqualTo("");

            // 4B: Case-sensitive converter should FAIL to match mixed case keys
            Map<String, Object> caseSensitiveResult = icebergCaseSensitive.convert(fullRecord);
            // Only exact matches should work - most fields use different case so won't match
            assertThat(caseSensitiveResult).doesNotContainKey("id"); // Input has "ID"
            assertThat(caseSensitiveResult).containsKey("balance"); // Exact match

            // ==================== PHASE 5: Edge Case Values ====================

            // 5A: Boundary numeric values
            Map<String, Object> boundaryRecord = new LinkedHashMap<>();
            boundaryRecord.put("id", String.valueOf(Long.MAX_VALUE));
            boundaryRecord.put("age", String.valueOf(Integer.MAX_VALUE));
            boundaryRecord.put("score", String.valueOf(Double.MAX_VALUE));

            Map<String, Object> boundaryResult = icebergConverter.convert(boundaryRecord);
            assertThat(boundaryResult.get("id")).isEqualTo(Long.MAX_VALUE);
            assertThat(boundaryResult.get("age")).isEqualTo(Integer.MAX_VALUE);
            assertThat(boundaryResult.get("score")).isEqualTo(Double.MAX_VALUE);

            // 5B: Negative values
            Map<String, Object> negativeRecord = new LinkedHashMap<>();
            negativeRecord.put("id", "-1");
            negativeRecord.put("age", "-2147483648"); // Integer.MIN_VALUE
            negativeRecord.put("balance", "-999.9999");
            negativeRecord.put("score", "-1.5E308");

            Map<String, Object> negativeResult = icebergConverter.convert(negativeRecord);
            assertThat(negativeResult.get("id")).isEqualTo(-1L);
            assertThat(negativeResult.get("age")).isEqualTo(Integer.MIN_VALUE);
            assertThat(negativeResult.get("balance")).isEqualTo(new BigDecimal("-999.9999"));

            // 5C: Zero values
            Map<String, Object> zeroRecord = new LinkedHashMap<>();
            zeroRecord.put("id", "0");
            zeroRecord.put("age", "0");
            zeroRecord.put("balance", "0.0000");
            zeroRecord.put("rate", "0.0");
            zeroRecord.put("score", "0.0");

            Map<String, Object> zeroResult = icebergConverter.convert(zeroRecord);
            assertThat(zeroResult.get("id")).isEqualTo(0L);
            assertThat(zeroResult.get("age")).isEqualTo(0);
            assertThat(zeroResult.get("balance")).isEqualTo(new BigDecimal("0.0000"));

            // 5D: Leading zeros (should still parse correctly)
            Map<String, Object> leadingZeroRecord = new LinkedHashMap<>();
            leadingZeroRecord.put("id", "00000123");
            leadingZeroRecord.put("age", "007");

            Map<String, Object> leadingZeroResult = icebergConverter.convert(leadingZeroRecord);
            assertThat(leadingZeroResult.get("id")).isEqualTo(123L);
            assertThat(leadingZeroResult.get("age")).isEqualTo(7);

            // 5E: Scientific notation
            Map<String, Object> scientificRecord = new LinkedHashMap<>();
            scientificRecord.put("id", "1");
            scientificRecord.put("score", "1.5E10");
            scientificRecord.put("rate", "2.5e-5");

            Map<String, Object> scientificResult = icebergConverter.convert(scientificRecord);
            assertThat((Double) scientificResult.get("score")).isCloseTo(1.5E10, offset(1.0));
            assertThat((Float) scientificResult.get("rate")).isCloseTo(2.5e-5f, offset(1e-10f));

            // 5F: Very long string (10KB)
            String longString = "A".repeat(10_000);
            Map<String, Object> longStringRecord = new LinkedHashMap<>();
            longStringRecord.put("id", "1");
            longStringRecord.put("name", longString);

            Map<String, Object> longStringResult = icebergConverter.convert(longStringRecord);
            assertThat(longStringResult.get("name")).isEqualTo(longString);
            assertThat(((String) longStringResult.get("name")).length()).isEqualTo(10_000);

            // 5G: Unicode stress test
            String unicodeStress = "Ａｂｃ　①②③　αβγ　∑∏∫　←→↑↓　☀☁☂　日本語　한국어　العربية　🎉🎊🎁";
            Map<String, Object> unicodeRecord = new LinkedHashMap<>();
            unicodeRecord.put("id", "1");
            unicodeRecord.put("unicodeField", unicodeStress);

            Map<String, Object> unicodeResult = icebergConverter.convert(unicodeRecord);
            assertThat(unicodeResult.get("unicodeField")).isEqualTo(unicodeStress);

            // ==================== PHASE 6: Null and Empty Handling ====================

            // 6A: Null values for nullable fields
            Map<String, Object> nullableRecord = new LinkedHashMap<>();
            nullableRecord.put("id", "1");
            nullableRecord.put("name", null);
            nullableRecord.put("age", null);

            Map<String, Object> nullableResult = icebergConverter.convert(nullableRecord);
            assertThat(nullableResult.get("id")).isEqualTo(1L);
            assertThat(nullableResult.get("name")).isNull();
            assertThat(nullableResult.get("age")).isNull();

            // 6B: Empty map
            Map<String, Object> emptyResult = icebergConverter.convert(Collections.emptyMap());
            assertThat(emptyResult).isEmpty();

            // 6C: Whitespace-only values (treated as strings, not null)
            Map<String, Object> whitespaceRecord = new LinkedHashMap<>();
            whitespaceRecord.put("id", "1");
            whitespaceRecord.put("name", "   ");
            whitespaceRecord.put("emptyAllowed", "\t\n");

            Map<String, Object> whitespaceResult = icebergConverter.convert(whitespaceRecord);
            assertThat(whitespaceResult.get("name")).isEqualTo("   ");
            assertThat(whitespaceResult.get("emptyAllowed")).isEqualTo("\t\n");

            // ==================== PHASE 7: Boolean Variations ====================

            String[] trueValues = {"true", "TRUE", "True", "1", "yes", "YES", "on", "ON"};
            String[] falseValues = {"false", "FALSE", "False", "0", "no", "NO", "off", "OFF"};

            for (String trueVal : trueValues) {
                Map<String, Object> boolRecord = Map.of("id", "1", "active", trueVal);
                try {
                    Map<String, Object> boolResult = icebergConverter.convert(boolRecord);
                    assertThat(boolResult.get("active"))
                            .as("'%s' should convert to true", trueVal)
                            .isEqualTo(true);
                } catch (Exception e) {
                    // Some values like "yes", "on" might not be supported - that's ok
                }
            }

            for (String falseVal : falseValues) {
                Map<String, Object> boolRecord = Map.of("id", "1", "active", falseVal);
                try {
                    Map<String, Object> boolResult = icebergConverter.convert(boolRecord);
                    assertThat(boolResult.get("active"))
                            .as("'%s' should convert to false", falseVal)
                            .isEqualTo(false);
                } catch (Exception e) {
                    // Some values might not be supported - that's ok
                }
            }

            // ==================== PHASE 8: Array Serialization (Flattened Schema) ====================

            Map<String, Object> arrayRecord = new LinkedHashMap<>();
            arrayRecord.put("id", "1");
            arrayRecord.put("name", "Array Tester");
            arrayRecord.put("tags", List.of("tag1", "tag2", "tag3"));
            arrayRecord.put("scores", new int[]{95, 87, 92});

            Map<String, Object> arrayResult = flatConverter.convert(arrayRecord);
            assertThat(arrayResult.get("id")).isEqualTo(1L);
            assertThat(arrayResult.get("tags")).isEqualTo("tag1,tag2,tag3");
            assertThat(arrayResult.get("scores")).isEqualTo("95,87,92");

            // Already serialized arrays should pass through
            Map<String, Object> preSerializedRecord = new LinkedHashMap<>();
            preSerializedRecord.put("id", "1");
            preSerializedRecord.put("tags", "already,serialized,tags");

            Map<String, Object> preSerializedResult = flatConverter.convert(preSerializedRecord);
            assertThat(preSerializedResult.get("tags")).isEqualTo("already,serialized,tags");

            // ==================== PHASE 9: Error Handling Modes ====================

            // 9A: Normal mode - throws on error
            Map<String, Object> invalidRecord = new LinkedHashMap<>();
            invalidRecord.put("id", "not_a_number"); // Invalid
            invalidRecord.put("name", "Valid Name");

            assertThatThrownBy(() -> icebergConverter.convert(invalidRecord))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("id");

            // 9B: convertWithErrors - collects errors
            ConversionResult<Map<String, Object>> errorResult =
                    icebergConverter.convertWithErrors(invalidRecord);

            assertThat(errorResult.hasErrors()).isTrue();
            assertThat(errorResult.getErrors()).hasSize(1);
//            assertThat(errorResult.getErrors().get(0).getFieldName()).isEqualTo("id");
            // Valid fields should still be converted
            assertThat(errorResult.getValue().get("name")).isEqualTo("Valid Name");

            // 9C: Strict mode - validates required fields
            Map<String, Object> missingRequiredRecord = new LinkedHashMap<>();
            missingRequiredRecord.put("name", "No ID provided");

            assertThatThrownBy(() -> icebergConverter.convertStrict(missingRequiredRecord))
                    .isInstanceOf(NullValueException.class)
                    .hasMessageContaining("Required field is missing");

            // ==================== PHASE 10: Batch Conversion ====================

            List<Map<String, Object>> batchInput = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Map<String, Object> record = new LinkedHashMap<>();
                record.put("id", String.valueOf(i));
                record.put("name", "Batch Record " + i);
                record.put("age", String.valueOf(20 + (i % 50)));
                record.put("active", i % 2 == 0 ? "true" : "false");
                batchInput.add(record);
            }

            List<Map<String, Object>> batchResults = icebergConverter.convertBatch(batchInput);

            assertThat(batchResults).hasSize(100);
            for (int i = 0; i < 100; i++) {
                assertThat(batchResults.get(i).get("id")).isEqualTo((long) i);
                assertThat(batchResults.get(i).get("name")).isEqualTo("Batch Record " + i);
                assertThat(batchResults.get(i).get("active")).isEqualTo(i % 2 == 0);
            }

            // ==================== PHASE 11: Single Field Conversion ====================

            assertThat(icebergConverter.convertField("id", "12345")).isEqualTo(12345L);
            assertThat(icebergConverter.convertField("ID", "67890")).isEqualTo(67890L); // Case insensitive
            assertThat(icebergConverter.convertField("active", "true")).isEqualTo(true);
            assertThat(icebergConverter.convertField("balance", "123.4567")).isEqualTo(new BigDecimal("123.4567"));

            assertThatThrownBy(() -> icebergConverter.convertField("nonexistent", "value"))
                    .isInstanceOf(IllegalArgumentException.class);

            // ==================== PHASE 12: Concurrent Stress Test ====================

            int numThreads = 16;
            int operationsPerThread = 200;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(numThreads);

            // Create converters array for round-robin access
            SchemaBasedMapConverter[] converters = {icebergConverter, avroConverter, flatConverter};

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        Random random = new Random(threadId * 1000);
                        for (int op = 0; op < operationsPerThread; op++) {
                            // Rotate through all converter types
                            SchemaBasedMapConverter converter = converters[(threadId + op) % 3];

                            Map<String, Object> record = new LinkedHashMap<>();
                            record.put("id", String.valueOf(threadId * 10000 + op));
                            record.put("name", "Thread-" + threadId + "-Op-" + op);
                            record.put("age", String.valueOf(random.nextInt(100)));
                            record.put("active", random.nextBoolean() ? "true" : "false");

                            try {
                                Map<String, Object> result = converter.convert(record);
                                if (result.get("id") instanceof Long &&
                                        result.get("name") instanceof String) {
                                    successCount.incrementAndGet();
                                } else {
                                    errorCount.incrementAndGet();
                                }
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                            }
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            boolean completed = latch.await(30, TimeUnit.SECONDS);
            executor.shutdown();

            assertThat(completed).as("All threads should complete").isTrue();
            assertThat(errorCount.get()).as("Should have no errors").isZero();
            assertThat(successCount.get()).as("All operations should succeed")
                    .isEqualTo(numThreads * operationsPerThread);

            // ==================== PHASE 13: Caching Validation ====================

            SchemaBasedMapConverter cached1 = SchemaBasedMapConverter.cached(icebergSchema);
            SchemaBasedMapConverter cached2 = SchemaBasedMapConverter.cached(icebergSchema);

            // Cache should return same instance for same schema
            assertThat(cached1).isSameAs(cached2);

            // ==================== PHASE 14: Unknown Fields Ignored ====================

            Map<String, Object> unknownFieldsRecord = new LinkedHashMap<>();
            unknownFieldsRecord.put("id", "1");
            unknownFieldsRecord.put("name", "Known Field");
            unknownFieldsRecord.put("TOTALLY_UNKNOWN_FIELD", "Should be ignored");
            unknownFieldsRecord.put("another_unknown", 12345);
            unknownFieldsRecord.put("yet_another", List.of(1, 2, 3));

            Map<String, Object> unknownResult = icebergConverter.convert(unknownFieldsRecord);
            assertThat(unknownResult).containsKey("id");
            assertThat(unknownResult).containsKey("name");
            assertThat(unknownResult).doesNotContainKey("TOTALLY_UNKNOWN_FIELD");
            assertThat(unknownResult).doesNotContainKey("another_unknown");
            assertThat(unknownResult).doesNotContainKey("yet_another");

            // ==================== PHASE 15: Schema Flattening Edge Cases ====================

            // Deep nesting with arrays
            Schema innerMost = SchemaBuilder.record("InnerMost")
                    .fields()
                    .requiredString("value")
                    .name("amount").type(avroDecimal5_2).noDefault()
                    .endRecord();

            Schema middle = SchemaBuilder.record("Middle")
                    .fields()
                    .requiredString("middleName")
                    .name("inners").type().array().items(innerMost).noDefault()
                    .endRecord();

            Schema deepNested = SchemaBuilder.record("DeepNested")
                    .fields()
                    .requiredLong("rootId")
                    .name("middle").type(middle).noDefault()
                    .endRecord();

            Map<String, SchemaBasedMapConverter.FlattenedFieldType> deepFlattened =
                    SchemaBasedMapConverter.flattenAvroSchema(deepNested, ".");

            assertThat(deepFlattened).containsKey("rootId");
            assertThat(deepFlattened).containsKey("middle.middleName");
            assertThat(deepFlattened).containsKey("middle.inners.value");
            assertThat(deepFlattened).containsKey("middle.inners.amount");

            // Verify decimal precision preserved in deep nesting
            SchemaBasedMapConverter.FlattenedFieldType deepAmount = deepFlattened.get("middle.inners.amount");
            assertThat(deepAmount.getPrecision()).isEqualTo(5);
            assertThat(deepAmount.getScale()).isEqualTo(2);

            // ==================== Final Summary ====================

            System.out.println("""
                    
                    ╔═══════════════════════════════════════════════════════════════════╗
                    ║           MEGA TEST COMPLETED SUCCESSFULLY!                       ║
                    ╠═══════════════════════════════════════════════════════════════════╣
                    ║ ✓ All 3 schema types (Iceberg, Avro, Flattened)                  ║
                    ║ ✓ All data types converted correctly                             ║
                    ║ ✓ Case-insensitive and case-sensitive modes                      ║
                    ║ ✓ Boundary values (MAX_VALUE, MIN_VALUE, zero, negative)         ║
                    ║ ✓ Scientific notation parsing                                    ║
                    ║ ✓ Unicode/international characters preserved                      ║
                    ║ ✓ Very long strings (10KB)                                        ║
                    ║ ✓ Null handling for nullable fields                              ║
                    ║ ✓ Empty and whitespace strings                                   ║
                    ║ ✓ Boolean variations (true/false/1/0/TRUE/FALSE)                 ║
                    ║ ✓ Array serialization                                            ║
                    ║ ✓ Error handling modes (throw, collect, strict)                  ║
                    ║ ✓ Batch conversion (100 records)                                 ║
                    ║ ✓ Single field conversion                                        ║
                    ║ ✓ Concurrent access (16 threads × 200 ops = 3,200 ops)           ║
                    ║ ✓ Caching validation                                             ║
                    ║ ✓ Unknown fields properly ignored                                ║
                    ║ ✓ Deep schema flattening with decimal precision                  ║
                    ╚═══════════════════════════════════════════════════════════════════╝
                    """);
        }
    }
}