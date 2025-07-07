package io.github.pierce;



import org.apache.avro.Schema;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CreateSparkStructFromAvroSchemaTest {

    @BeforeEach
    void setUp() {
        CreateSparkStructFromAvroSchema.clearCache();
    }

    @Test
    void testSimpleSchemaConversion() {
        String avroSchemaJson = """
            {
              "type": "record",
              "name": "Simple",
              "namespace": "com.test",
              "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "active", "type": "boolean"},
                {"name": "score", "type": "double"},
                {"name": "count", "type": "int"},
                {"name": "ratio", "type": "float"}
              ]
            }
            """;

        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        StructType sparkSchema = CreateSparkStructFromAvroSchema.convertNestedAvroSchemaToSparkSchema(avroSchema);

        assertThat(sparkSchema.fields()).hasSize(6);


        assertThat(sparkSchema.fieldIndex("id")).isEqualTo(0);
        assertThat(sparkSchema.fields()[0].dataType()).isEqualTo(DataTypes.LongType);

        assertThat(sparkSchema.fieldIndex("name")).isEqualTo(1);
        assertThat(sparkSchema.fields()[1].dataType()).isEqualTo(DataTypes.StringType);

        assertThat(sparkSchema.fieldIndex("active")).isEqualTo(2);
        assertThat(sparkSchema.fields()[2].dataType()).isEqualTo(DataTypes.BooleanType);

        assertThat(sparkSchema.fieldIndex("score")).isEqualTo(3);
        assertThat(sparkSchema.fields()[3].dataType()).isEqualTo(DataTypes.DoubleType);

        assertThat(sparkSchema.fieldIndex("count")).isEqualTo(4);
        assertThat(sparkSchema.fields()[4].dataType()).isEqualTo(DataTypes.IntegerType);

        assertThat(sparkSchema.fieldIndex("ratio")).isEqualTo(5);
        assertThat(sparkSchema.fields()[5].dataType()).isEqualTo(DataTypes.FloatType);
    }

    @Test
    void testNullableFieldConversion() {
        String avroSchemaJson = """
            {
              "type": "record",
              "name": "Nullable",
              "namespace": "com.test",
              "fields": [
                {"name": "required", "type": "string"},
                {"name": "optional", "type": ["null", "string"], "default": null},
                {"name": "optionalInt", "type": ["null", "int"], "default": null},
                {"name": "optionalLong", "type": ["null", "long"], "default": null}
              ]
            }
            """;

        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        StructType sparkSchema = CreateSparkStructFromAvroSchema.convertNestedAvroSchemaToSparkSchema(avroSchema);


        for (StructField field : sparkSchema.fields()) {
            assertThat(field.nullable()).isTrue();
        }


        assertThat(sparkSchema.fields()[0].dataType()).isEqualTo(DataTypes.StringType);
        assertThat(sparkSchema.fields()[1].dataType()).isEqualTo(DataTypes.StringType);
        assertThat(sparkSchema.fields()[2].dataType()).isEqualTo(DataTypes.IntegerType);
        assertThat(sparkSchema.fields()[3].dataType()).isEqualTo(DataTypes.LongType);
    }

    @Test
    void testArrayFieldConversion() {
        String avroSchemaJson = """
            {
              "type": "record",
              "name": "ArrayTest",
              "namespace": "com.test",
              "fields": [
                {"name": "stringArray", "type": {"type": "array", "items": "string"}},
                {"name": "intArray", "type": {"type": "array", "items": "int"}},
                {"name": "nullableArray", "type": ["null", {"type": "array", "items": "string"}]}
              ]
            }
            """;

        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        StructType sparkSchema = CreateSparkStructFromAvroSchema.convertNestedAvroSchemaToSparkSchema(avroSchema);


        StructField stringArrayField = sparkSchema.fields()[0];
        assertThat(stringArrayField.dataType()).isInstanceOf(ArrayType.class);
        ArrayType stringArrayType = (ArrayType) stringArrayField.dataType();
        assertThat(stringArrayType.elementType()).isEqualTo(DataTypes.StringType);
        assertThat(stringArrayType.containsNull()).isTrue();

        StructField intArrayField = sparkSchema.fields()[1];
        assertThat(intArrayField.dataType()).isInstanceOf(ArrayType.class);
        ArrayType intArrayType = (ArrayType) intArrayField.dataType();
        assertThat(intArrayType.elementType()).isEqualTo(DataTypes.IntegerType);


        StructField nullableArrayField = sparkSchema.fields()[2];
        assertThat(nullableArrayField.dataType()).isInstanceOf(ArrayType.class);
    }

    @Test
    void testMapFieldConversion() {
        String avroSchemaJson = """
            {
              "type": "record",
              "name": "MapTest",
              "namespace": "com.test",
              "fields": [
                {"name": "stringMap", "type": {"type": "map", "values": "string"}},
                {"name": "intMap", "type": {"type": "map", "values": "int"}}
              ]
            }
            """;

        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        StructType sparkSchema = CreateSparkStructFromAvroSchema.convertNestedAvroSchemaToSparkSchema(avroSchema);


        StructField stringMapField = sparkSchema.fields()[0];
        assertThat(stringMapField.dataType()).isInstanceOf(MapType.class);
        MapType stringMapType = (MapType) stringMapField.dataType();
        assertThat(stringMapType.keyType()).isEqualTo(DataTypes.StringType);
        assertThat(stringMapType.valueType()).isEqualTo(DataTypes.StringType);
        assertThat(stringMapType.valueContainsNull()).isTrue();

        StructField intMapField = sparkSchema.fields()[1];
        assertThat(intMapField.dataType()).isInstanceOf(MapType.class);
        MapType intMapType = (MapType) intMapField.dataType();
        assertThat(intMapType.keyType()).isEqualTo(DataTypes.StringType);
        assertThat(intMapType.valueType()).isEqualTo(DataTypes.IntegerType);
    }

    @Test
    void testEnumAndFixedConversion() {
        String avroSchemaJson = """
            {
              "type": "record",
              "name": "Special",
              "namespace": "com.test",
              "fields": [
                {
                  "name": "status",
                  "type": {
                    "type": "enum",
                    "name": "Status",
                    "symbols": ["ACTIVE", "INACTIVE"]
                  }
                },
                {
                  "name": "hash",
                  "type": {
                    "type": "fixed",
                    "name": "MD5",
                    "size": 16
                  }
                },
                {"name": "data", "type": "bytes"}
              ]
            }
            """;

        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        StructType sparkSchema = CreateSparkStructFromAvroSchema.convertNestedAvroSchemaToSparkSchema(avroSchema);


        assertThat(sparkSchema.fields()[0].dataType()).isEqualTo(DataTypes.StringType);


        assertThat(sparkSchema.fields()[1].dataType()).isEqualTo(DataTypes.BinaryType);


        assertThat(sparkSchema.fields()[2].dataType()).isEqualTo(DataTypes.BinaryType);
    }

    @Test
    void testFlattenedSchemaConversion() {

        String complexSchemaJson = """
            {
              "type": "record",
              "name": "Complex",
              "namespace": "com.test",
              "fields": [
                {"name": "id", "type": "string"},
                {
                  "name": "user",
                  "type": {
                    "type": "record",
                    "name": "User",
                    "fields": [
                      {"name": "name", "type": "string"},
                      {"name": "age", "type": "int"}
                    ]
                  }
                },
                {"name": "tags", "type": {"type": "array", "items": "string"}}
              ]
            }
            """;

        Schema complexSchema = new Schema.Parser().parse(complexSchemaJson);
        AvroSchemaFlattener flattener = new AvroSchemaFlattener(true);
        Schema flattenedSchema = flattener.getFlattenedSchema(complexSchema);


        StructType sparkSchema = CreateSparkStructFromAvroSchema.convertNestedAvroSchemaToSparkSchema(flattenedSchema);


        assertThat(sparkSchema.fieldNames()).contains(
                "id",
                "user_name",
                "user_age",
                "tags",
                "tags_count",
                "tags_distinct_count",
                "tags_min_length",
                "tags_max_length",
                "tags_avg_length",
                "tags_type"
        );

        // Verify types
        assertThat(sparkSchema.fields()[sparkSchema.fieldIndex("user_name")].dataType())
                .isEqualTo(DataTypes.StringType);
        assertThat(sparkSchema.fields()[sparkSchema.fieldIndex("user_age")].dataType())
                .isEqualTo(DataTypes.IntegerType);
        assertThat(sparkSchema.fields()[sparkSchema.fieldIndex("tags_count")].dataType())
                .isEqualTo(DataTypes.LongType);
        assertThat(sparkSchema.fields()[sparkSchema.fieldIndex("tags_avg_length")].dataType())
                .isEqualTo(DataTypes.DoubleType);
    }

    @Test
    void testMetadataPreservation() {
        String avroSchemaJson = """
            {
              "type": "record",
              "name": "Documented",
              "namespace": "com.test",
              "fields": [
                {"name": "id", "type": "string", "doc": "Unique identifier"},
                {"name": "value", "type": "int", "doc": "Some value", "default": 0}
              ]
            }
            """;

        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        StructType sparkSchema = CreateSparkStructFromAvroSchema.convertNestedAvroSchemaToSparkSchema(avroSchema);

        // Check metadata
        StructField idField = sparkSchema.fields()[0];
        Metadata idMetadata = idField.metadata();
        assertThat(idMetadata.contains("comment")).isTrue();
        assertThat(idMetadata.getString("comment")).isEqualTo("Unique identifier");
        assertThat(idMetadata.getString("avro.field.name")).isEqualTo("id");

        StructField valueField = sparkSchema.fields()[1];
        Metadata valueMetadata = valueField.metadata();
        assertThat(valueMetadata.getString("comment")).isEqualTo("Some value");
        assertThat(valueMetadata.contains("avro.field.default")).isTrue();
    }

    @Test
    void testCachingBehavior() {
        String avroSchemaJson = """
            {
              "type": "record",
              "name": "CacheTest",
              "namespace": "com.test",
              "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"}
              ]
            }
            """;

        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);

        // First conversion
        long start = System.nanoTime();
        StructType schema1 = CreateSparkStructFromAvroSchema.convertNestedAvroSchemaToSparkSchema(avroSchema);
        long firstTime = System.nanoTime() - start;

        // Second conversion (should use cache)
        start = System.nanoTime();
        StructType schema2 = CreateSparkStructFromAvroSchema.convertNestedAvroSchemaToSparkSchema(avroSchema);
        long secondTime = System.nanoTime() - start;

        // Should be the same instance
        assertThat(schema1).isSameAs(schema2);

        // Second call should be much faster
        assertThat(secondTime).isLessThan(firstTime / 10);
    }

    @ParameterizedTest
    @MethodSource("provideAvroToSparkTypeMappings")
    void testTypeConversions(String avroType, DataType expectedSparkType) {
        String schemaJson = String.format("""
            {
              "type": "record",
              "name": "TypeTest",
              "namespace": "com.test",
              "fields": [
                {"name": "field", "type": %s}
              ]
            }
            """, avroType);

        Schema avroSchema = new Schema.Parser().parse(schemaJson);
        StructType sparkSchema = CreateSparkStructFromAvroSchema.convertNestedAvroSchemaToSparkSchema(avroSchema);

        assertThat(sparkSchema.fields()[0].dataType()).isEqualTo(expectedSparkType);
    }

    private static Stream<Arguments> provideAvroToSparkTypeMappings() {
        return Stream.of(
                Arguments.of("\"string\"", DataTypes.StringType),
                Arguments.of("\"int\"", DataTypes.IntegerType),
                Arguments.of("\"long\"", DataTypes.LongType),
                Arguments.of("\"float\"", DataTypes.FloatType),
                Arguments.of("\"double\"", DataTypes.DoubleType),
                Arguments.of("\"boolean\"", DataTypes.BooleanType),
                Arguments.of("\"bytes\"", DataTypes.BinaryType),
                Arguments.of("\"null\"", DataTypes.NullType)
        );
    }

    @Test
    void testComplexUnionHandling() {
        String avroSchemaJson = """
            {
              "type": "record",
              "name": "UnionTest",
              "namespace": "com.test",
              "fields": [
                {"name": "nullOnly", "type": ["null"]},
                {"name": "stringOrInt", "type": ["string", "int"]},
                {"name": "nullOrStringOrInt", "type": ["null", "string", "int"]}
              ]
            }
            """;

        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        StructType sparkSchema = CreateSparkStructFromAvroSchema.convertNestedAvroSchemaToSparkSchema(avroSchema);

        // Null only should default to StringType
        assertThat(sparkSchema.fields()[0].dataType()).isEqualTo(DataTypes.StringType);

        // Non-nullable union should take first type
        assertThat(sparkSchema.fields()[1].dataType()).isEqualTo(DataTypes.StringType);

        // Nullable union should take first non-null type
        assertThat(sparkSchema.fields()[2].dataType()).isEqualTo(DataTypes.StringType);
        assertThat(sparkSchema.fields()[2].nullable()).isTrue();
    }
}