package io.github.pierce.avroTesting;



import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import io.github.pierce.AvroSchemaFlattener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class AvroSchemaFlattenerTest {

    private AvroSchemaFlattener flattener;
    private AvroSchemaFlattener flattenerWithStats;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        AvroSchemaFlattener.clearCache();
        flattener = new AvroSchemaFlattener(false);
        flattenerWithStats = new AvroSchemaFlattener(true);
    }

    @Test
    void testSimpleSchema() {
        String schemaJson = """
            {
              "type": "record",
              "name": "SimpleRecord",
              "namespace": "com.test",
              "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "active", "type": "boolean"}
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattener.getFlattenedSchema(schema);

        assertThat(flattened.getName()).isEqualTo("FlattenedSimpleRecord");
        assertThat(flattened.getNamespace()).isEqualTo("com.test.flattened");
        assertThat(flattened.getFields()).hasSize(3);


        Set<String> fieldNames = flattened.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toSet());

        assertThat(fieldNames).containsExactlyInAnyOrder("id", "name", "active");
    }

    @Test
    void testNestedRecordSchema() {
        String schemaJson = """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.test",
              "fields": [
                {"name": "name", "type": "string"},
                {
                  "name": "address",
                  "type": {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                      {"name": "street", "type": "string"},
                      {"name": "city", "type": "string"},
                      {
                        "name": "location",
                        "type": {
                          "type": "record",
                          "name": "Location",
                          "fields": [
                            {"name": "lat", "type": "double"},
                            {"name": "lon", "type": "double"}
                          ]
                        }
                      }
                    ]
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattener.getFlattenedSchema(schema);

        assertThat(flattened.getFields()).hasSize(5);

        Set<String> fieldNames = flattened.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toSet());


        assertThat(fieldNames).containsExactlyInAnyOrder(
                "name",
                "address_street",
                "address_city",
                "address_location_lat",
                "address_location_lon"
        );
    }

    @Test
    void testArrayOfPrimitivesSchema() {
        String schemaJson = """
            {
              "type": "record",
              "name": "ArrayRecord",
              "namespace": "com.test",
              "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "scores", "type": {"type": "array", "items": "int"}}
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattenedNoStats = flattener.getFlattenedSchema(schema);
        Schema flattenedWithStats = flattenerWithStats.getFlattenedSchema(schema);


        assertThat(flattenedNoStats.getFields()).hasSize(2);
        assertThat(flattenedNoStats.getField("tags").schema())
                .matches(this::isNullableString);


        assertThat(flattenedWithStats.getFields()).hasSize(14);

        Set<String> fieldNames = flattenedWithStats.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toSet());

        assertThat(fieldNames).contains(
                "tags", "tags_count", "tags_distinct_count",
                "tags_min_length", "tags_max_length", "tags_avg_length", "tags_type",
                "scores", "scores_count", "scores_distinct_count",
                "scores_min_length", "scores_max_length", "scores_avg_length", "scores_type"
        );
    }

    @Test
    void testArrayOfRecordsSchema() {
        String schemaJson = """
            {
              "type": "record",
              "name": "OrderRecord",
              "namespace": "com.test",
              "fields": [
                {
                  "name": "items",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Item",
                      "fields": [
                        {"name": "product", "type": "string"},
                        {"name": "quantity", "type": "int"},
                        {"name": "price", "type": "double"}
                      ]
                    }
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattenerWithStats.getFlattenedSchema(schema);

        Set<String> fieldNames = flattened.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toSet());


        assertThat(fieldNames).contains(
                "items",
                "items_count",
                "items_distinct_count",
                "items_min_length",
                "items_max_length",
                "items_avg_length",
                "items_type",
                "items_product",
                "items_quantity",
                "items_price"
        );
    }

    @Test
    void testNullableFieldsSchema() {
        String schemaJson = """
            {
              "type": "record",
              "name": "NullableRecord",
              "namespace": "com.test",
              "fields": [
                {"name": "required", "type": "string"},
                {"name": "optional", "type": ["null", "string"], "default": null},
                {"name": "optionalInt", "type": ["null", "int"], "default": null},
                {
                  "name": "optionalRecord",
                  "type": ["null", {
                    "type": "record",
                    "name": "SubRecord",
                    "fields": [
                      {"name": "value", "type": "string"}
                    ]
                  }],
                  "default": null
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattener.getFlattenedSchema(schema);

        assertThat(flattened.getFields()).hasSize(4);


        for (Field field : flattened.getFields()) {
            assertThat(isNullable(field.schema())).isTrue();
        }


        assertThat(flattened.getField("required")).isNotNull();
        assertThat(flattened.getField("optional")).isNotNull();
        assertThat(flattened.getField("optionalInt")).isNotNull();
        assertThat(flattened.getField("optionalRecord_value")).isNotNull();
    }

    @Test
    void testMapSchema() {
        String schemaJson = """
            {
              "type": "record",
              "name": "MapRecord",
              "namespace": "com.test",
              "fields": [
                {"name": "properties", "type": {"type": "map", "values": "string"}},
                {
                  "name": "complexMap",
                  "type": {
                    "type": "map",
                    "values": {
                      "type": "record",
                      "name": "Value",
                      "fields": [
                        {"name": "data", "type": "string"}
                      ]
                    }
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattener.getFlattenedSchema(schema);


        assertThat(flattened.getFields()).hasSize(2);
        assertThat(flattened.getField("properties").schema())
                .matches(this::isNullableString);
        assertThat(flattened.getField("complexMap").schema())
                .matches(this::isNullableString);
    }



    @Test
    void testComplexRealWorldSchema() throws IOException {
        String schemaJson = """
        {
          "type": "record",
          "name": "Transaction",
          "namespace": "com.jpmc.example",
          "fields": [
            {"name": "transactionId", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {
              "name": "customer",
              "type": {
                "type": "record",
                "name": "Customer",
                "fields": [
                  {"name": "id", "type": "string"},
                  {"name": "name", "type": "string"},
                  {
                    "name": "accounts",
                    "type": {
                      "type": "array",
                      "items": {
                        "type": "record",
                        "name": "Account",
                        "fields": [
                          {"name": "accountNumber", "type": "string"},
                          {"name": "type", "type": "string"},
                          {"name": "balance", "type": "double"}
                        ]
                      }
                    }
                  }
                ]
              }
            },
            {
              "name": "items",
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "LineItem",
                  "fields": [
                    {"name": "product", "type": "string"},
                    {"name": "quantity", "type": "int"},
                    {"name": "price", "type": "double"},
                    {"name": "tags", "type": {"type": "array", "items": "string"}}
                  ]
                }
              }
            },
            {"name": "metadata", "type": {"type": "map", "values": "string"}}
          ]
        }
        """;


        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattenerWithStats.getFlattenedSchema(schema);

        Set<String> fieldNames = flattened.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toSet());


        assertThat(fieldNames).contains(
                "transactionId",
                "timestamp",
                "customer_id",
                "customer_name",
                "customer_accounts",
                "customer_accounts_accountNumber",
                "customer_accounts_type",
                "customer_accounts_balance",
                "items",
                "items_product",
                "items_quantity",
                "items_price",
                "items_tags",
                "metadata"
        );

        // Verify statistics fields for arrays, including the nested one
        assertThat(fieldNames).contains(
                "customer_accounts_count",
                "items_count",
                "items_tags_count" // This will now be found
        );
    }

    @Test
    void testSchemaCompatibilityWithJsonFlattener() {
        // This test ensures field names match what JsonFlattenerConsolidator produces
        String schemaJson = """
            {
              "type": "record",
              "name": "TestRecord",
              "namespace": "com.test",
              "fields": [
                {
                  "name": "nested",
                  "type": {
                    "type": "record",
                    "name": "Nested",
                    "fields": [
                      {"name": "field", "type": "string"}
                    ]
                  }
                },
                {"name": "array", "type": {"type": "array", "items": "string"}}
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattenerWithStats.getFlattenedSchema(schema);

        // JsonFlattenerConsolidator converts dots to underscores
        assertThat(flattened.getField("nested_field")).isNotNull();
        assertThat(flattened.getField("array")).isNotNull();
        assertThat(flattened.getField("array_count")).isNotNull();
    }

    @Test
    void testCachingPerformance() {
        String schemaJson = """
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

        Schema schema = new Schema.Parser().parse(schemaJson);

        // First call - should process
        long start = System.nanoTime();
        Schema flattened1 = flattener.getFlattenedSchema(schema);
        long firstCallTime = System.nanoTime() - start;

        // Second call - should use cache
        start = System.nanoTime();
        Schema flattened2 = flattener.getFlattenedSchema(schema);
        long secondCallTime = System.nanoTime() - start;

        // Verify same result
        assertThat(flattened1).isSameAs(flattened2);

        // Second call should be significantly faster (at least 10x)
//        assertThat(secondCallTime).isLessThan(firstCallTime / 10);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "STRING", "INT", "LONG", "FLOAT", "DOUBLE", "BOOLEAN", "BYTES"
    })
    void testAllPrimitiveTypes(String typeName) {
        String schemaJson = String.format("""
            {
              "type": "record",
              "name": "PrimitiveTest",
              "namespace": "com.test",
              "fields": [
                {"name": "field", "type": "%s"}
              ]
            }
            """, typeName.toLowerCase());

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattener.getFlattenedSchema(schema);

        assertThat(flattened.getFields()).hasSize(1);
        assertThat(flattened.getField("field")).isNotNull();
    }

    @Test
    void testEnumAndFixedTypes() {
        String schemaJson = """
            {
              "type": "record",
              "name": "SpecialTypes",
              "namespace": "com.test",
              "fields": [
                {
                  "name": "status",
                  "type": {
                    "type": "enum",
                    "name": "Status",
                    "symbols": ["ACTIVE", "INACTIVE", "PENDING"]
                  }
                },
                {
                  "name": "hash",
                  "type": {
                    "type": "fixed",
                    "name": "MD5",
                    "size": 16
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattener.getFlattenedSchema(schema);

        assertThat(flattened.getFields()).hasSize(2);

        // Enum becomes string
        Field statusField = flattened.getField("status");
        assertThat(getNonNullType(statusField.schema()).getType()).isEqualTo(Type.STRING);

        // Fixed becomes bytes
        Field hashField = flattened.getField("hash");
        assertThat(getNonNullType(hashField.schema()).getType()).isEqualTo(Type.BYTES);
    }

    // Helper methods
    private boolean isNullable(Schema schema) {
        if (schema.getType() == Type.UNION) {
            return schema.getTypes().stream()
                    .anyMatch(s -> s.getType() == Type.NULL);
        }
        return false;
    }

    private boolean isNullableString(Schema schema) {
        if (schema.getType() == Type.UNION) {
            boolean hasNull = false;
            boolean hasString = false;
            for (Schema s : schema.getTypes()) {
                if (s.getType() == Type.NULL) hasNull = true;
                if (s.getType() == Type.STRING) hasString = true;
            }
            return hasNull && hasString;
        }
        return schema.getType() == Type.STRING;
    }

    private Schema getNonNullType(Schema schema) {
        if (schema.getType() == Type.UNION) {
            return schema.getTypes().stream()
                    .filter(s -> s.getType() != Type.NULL)
                    .findFirst()
                    .orElse(null);
        }
        return schema;
    }
}
