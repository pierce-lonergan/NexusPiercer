package io.github.pierce.avroTesting;


import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import io.github.pierce.AvroSchemaFlattener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test cases specifically for verifying field ordering in the flattened schema
 */
class AvroSchemaFlattenerOrderingTest {

    private AvroSchemaFlattener flattener;
    private AvroSchemaFlattener flattenerWithStats;

    @BeforeEach
    void setUp() {
        AvroSchemaFlattener.clearCache();
        flattener = new AvroSchemaFlattener(false);
        flattenerWithStats = new AvroSchemaFlattener(true);
    }

    @Test
    void testFieldOrderingForComplexSchema() {
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
        Schema flattened = flattener.getFlattenedSchema(schema);

        // Get the field names in order
        List<String> fieldNames = flattened.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList());

        // Verify the exact order
        assertThat(fieldNames).containsExactly(
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
    }

    @Test
    void testFieldOrderingWithStatistics() {
        String schemaJson = """
        {
          "type": "record",
          "name": "Order",
          "namespace": "com.test",
          "fields": [
            {"name": "orderId", "type": "string"},
            {
              "name": "items",
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "Item",
                  "fields": [
                    {"name": "sku", "type": "string"},
                    {"name": "tags", "type": {"type": "array", "items": "string"}}
                  ]
                }
              }
            },
            {"name": "total", "type": "double"}
          ]
        }
        """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattenerWithStats.getFlattenedSchema(schema);

        List<String> fieldNames = flattened.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList());

        // With statistics, array fields should be followed immediately by their stats
        assertThat(fieldNames).containsExactly(
                "orderId",
                "items",
                "items_count",
                "items_distinct_count",
                "items_min_length",
                "items_max_length",
                "items_avg_length",
                "items_type",
                "items_sku",
                "items_tags",
                "items_tags_count",
                "items_tags_distinct_count",
                "items_tags_min_length",
                "items_tags_max_length",
                "items_tags_avg_length",
                "items_tags_type",
                "total"
        );
    }

    @Test
    void testDeeplyNestedFieldOrdering() {
        String schemaJson = """
        {
          "type": "record",
          "name": "Root",
          "namespace": "com.test",
          "fields": [
            {"name": "field1", "type": "string"},
            {
              "name": "level1",
              "type": {
                "type": "record",
                "name": "Level1",
                "fields": [
                  {"name": "field2", "type": "int"},
                  {
                    "name": "level2",
                    "type": {
                      "type": "record",
                      "name": "Level2",
                      "fields": [
                        {"name": "field3", "type": "boolean"},
                        {
                          "name": "level3",
                          "type": {
                            "type": "record",
                            "name": "Level3",
                            "fields": [
                              {"name": "field4", "type": "double"}
                            ]
                          }
                        },
                        {"name": "field5", "type": "string"}
                      ]
                    }
                  },
                  {"name": "field6", "type": "long"}
                ]
              }
            },
            {"name": "field7", "type": "string"}
          ]
        }
        """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattener.getFlattenedSchema(schema);

        List<String> fieldNames = flattened.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList());

        // Fields should appear in depth-first order
        assertThat(fieldNames).containsExactly(
                "field1",
                "level1_field2",
                "level1_level2_field3",
                "level1_level2_level3_field4",
                "level1_level2_field5",
                "level1_field6",
                "field7"
        );
    }

    @Test
    void testMixedTypesFieldOrdering() {
        String schemaJson = """
        {
          "type": "record",
          "name": "MixedTypes",
          "namespace": "com.test",
          "fields": [
            {"name": "id", "type": "string"},
            {
              "name": "data",
              "type": {
                "type": "record",
                "name": "Data",
                "fields": [
                  {"name": "values", "type": {"type": "array", "items": "int"}},
                  {"name": "properties", "type": {"type": "map", "values": "string"}},
                  {
                    "name": "nested",
                    "type": {
                      "type": "record",
                      "name": "Nested",
                      "fields": [
                        {"name": "flag", "type": "boolean"}
                      ]
                    }
                  }
                ]
              }
            },
            {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["ACTIVE", "INACTIVE"]}}
          ]
        }
        """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattener.getFlattenedSchema(schema);

        List<String> fieldNames = flattened.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList());

        assertThat(fieldNames).containsExactly(
                "id",
                "data_values",
                "data_properties",
                "data_nested_flag",
                "status"
        );
    }

    @Test
    void testNullableFieldsPreserveOrdering() {
        String schemaJson = """
        {
          "type": "record",
          "name": "NullableTest",
          "namespace": "com.test",
          "fields": [
            {"name": "required", "type": "string"},
            {"name": "optional", "type": ["null", "string"], "default": null},
            {
              "name": "optionalRecord",
              "type": ["null", {
                "type": "record",
                "name": "SubRecord",
                "fields": [
                  {"name": "value1", "type": "string"},
                  {"name": "value2", "type": ["null", "int"], "default": null}
                ]
              }],
              "default": null
            },
            {"name": "finalField", "type": "long"}
          ]
        }
        """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattener.getFlattenedSchema(schema);

        List<String> fieldNames = flattened.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList());

        assertThat(fieldNames).containsExactly(
                "required",
                "optional",
                "optionalRecord_value1",
                "optionalRecord_value2",
                "finalField"
        );
    }

    @Test
    void testArrayOfArraysOrdering() {
        String schemaJson = """
        {
          "type": "record",
          "name": "ArrayOfArrays",
          "namespace": "com.test",
          "fields": [
            {"name": "start", "type": "string"},
            {
              "name": "matrix",
              "type": {
                "type": "array",
                "items": {
                  "type": "array",
                  "items": "int"
                }
              }
            },
            {"name": "end", "type": "string"}
          ]
        }
        """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattenerWithStats.getFlattenedSchema(schema);

        List<String> fieldNames = flattened.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList());

        // The outer array should appear with its stats before the end field
        assertThat(fieldNames.get(0)).isEqualTo("start");
        assertThat(fieldNames.get(1)).isEqualTo("matrix");
        // Stats fields for matrix
        assertThat(fieldNames.subList(2, 8)).containsExactly(
                "matrix_count",
                "matrix_distinct_count",
                "matrix_min_length",
                "matrix_max_length",
                "matrix_avg_length",
                "matrix_type"
        );
        assertThat(fieldNames.get(8)).isEqualTo("end");
    }

    @Test
    void testQueueBasedApproachWouldFailThisOrdering() {
        // This test demonstrates a case where a queue-based (FIFO) approach would fail
        String schemaJson = """
        {
          "type": "record",
          "name": "OrderTest",
          "namespace": "com.test",
          "fields": [
            {
              "name": "first",
              "type": {
                "type": "record",
                "name": "First",
                "fields": [
                  {"name": "a", "type": "string"},
                  {"name": "b", "type": "string"}
                ]
              }
            },
            {"name": "middle", "type": "string"},
            {
              "name": "last",
              "type": {
                "type": "record",
                "name": "Last",
                "fields": [
                  {"name": "x", "type": "string"},
                  {"name": "y", "type": "string"}
                ]
              }
            }
          ]
        }
        """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattener.getFlattenedSchema(schema);

        List<String> fieldNames = flattened.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList());

        // Correct depth-first order
        assertThat(fieldNames).containsExactly(
                "first_a",
                "first_b",
                "middle",
                "last_x",
                "last_y"
        );

        // A queue-based approach would incorrectly produce:
        // "middle", "first_a", "first_b", "last_x", "last_y"
        // because it would process all top-level fields first
        assertThat(fieldNames).doesNotContainSequence("middle", "first_a");
    }
}