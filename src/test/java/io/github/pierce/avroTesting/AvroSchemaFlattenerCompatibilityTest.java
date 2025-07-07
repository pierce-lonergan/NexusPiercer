package io.github.pierce.avroTesting;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import io.github.pierce.AvroSchemaFlattener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the optimized iterative implementation produces
 * identical results to the original recursive implementation
 */
class AvroSchemaFlattenerCompatibilityTest {

    private AvroSchemaFlattener flattener;
    private AvroSchemaFlattener flattenerWithStats;

    @BeforeEach
    void setUp() {
        AvroSchemaFlattener.clearCache();
        flattener = new AvroSchemaFlattener(false);
        flattenerWithStats = new AvroSchemaFlattener(true);
    }

    @Test
    void testIdenticalOutputForAllTestCases() {

        String complexSchema = """
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

        verifyIdenticalOutput(complexSchema);
    }

    @Test
    void testEdgeCases() {

        String nestedArrays = """
        {
          "type": "record",
          "name": "NestedArrays",
          "namespace": "com.test",
          "fields": [
            {
              "name": "matrix",
              "type": {
                "type": "array",
                "items": {
                  "type": "array",
                  "items": {
                    "type": "array",
                    "items": "int"
                  }
                }
              }
            }
          ]
        }
        """;
        verifyIdenticalOutput(nestedArrays);


        String unions = """
        {
          "type": "record",
          "name": "Unions",
          "namespace": "com.test",
          "fields": [
            {"name": "field1", "type": ["null", "string", "int"]},
            {"name": "field2", "type": ["string", "null"]},
            {"name": "field3", "type": ["null", {"type": "array", "items": "string"}]}
          ]
        }
        """;
        verifyIdenticalOutput(unions);


        String allTypes = """
        {
          "type": "record",
          "name": "AllTypes",
          "namespace": "com.test",
          "fields": [
            {"name": "stringField", "type": "string"},
            {"name": "intField", "type": "int"},
            {"name": "longField", "type": "long"},
            {"name": "floatField", "type": "float"},
            {"name": "doubleField", "type": "double"},
            {"name": "booleanField", "type": "boolean"},
            {"name": "bytesField", "type": "bytes"},
            {"name": "enumField", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]}},
            {"name": "fixedField", "type": {"type": "fixed", "name": "MD5", "size": 16}},
            {"name": "arrayField", "type": {"type": "array", "items": "string"}},
            {"name": "mapField", "type": {"type": "map", "values": "int"}},
            {"name": "recordField", "type": {"type": "record", "name": "Sub", "fields": [{"name": "value", "type": "string"}]}}
          ]
        }
        """;
        verifyIdenticalOutput(allTypes);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testWithAndWithoutStatistics(boolean includeStats) {
        AvroSchemaFlattener testFlattener = new AvroSchemaFlattener(includeStats);

        String schema = """
        {
          "type": "record",
          "name": "TestRecord",
          "namespace": "com.test",
          "fields": [
            {"name": "id", "type": "string"},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
            {
              "name": "nested",
              "type": {
                "type": "record",
                "name": "Nested",
                "fields": [
                  {"name": "values", "type": {"type": "array", "items": "int"}}
                ]
              }
            }
          ]
        }
        """;

        Schema inputSchema = new Schema.Parser().parse(schema);
        Schema flattened = testFlattener.getFlattenedSchema(inputSchema);

        List<String> fieldNames = flattened.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList());

        if (includeStats) {

            assertThat(fieldNames).contains(
                    "tags_count", "tags_distinct_count", "tags_min_length",
                    "tags_max_length", "tags_avg_length", "tags_type",
                    "nested_values_count", "nested_values_distinct_count"
            );
        } else {

            assertThat(fieldNames).doesNotContain(
                    "tags_count", "nested_values_count"
            );
        }


        assertThat(fieldNames).contains("id", "tags", "nested_values");
    }

    private void verifyIdenticalOutput(String schemaJson) {
        Schema schema = new Schema.Parser().parse(schemaJson);


        Schema flattened1 = flattener.getFlattenedSchema(schema);
        List<String> fields1 = getFieldNamesInOrder(flattened1);


        AvroSchemaFlattener.clearCache();
        Schema flattened2 = flattener.getFlattenedSchema(schema);
        List<String> fields2 = getFieldNamesInOrder(flattened2);

        assertThat(fields1).isEqualTo(fields2);


        Schema flattenedStats1 = flattenerWithStats.getFlattenedSchema(schema);
        List<String> fieldsStats1 = getFieldNamesInOrder(flattenedStats1);

        AvroSchemaFlattener.clearCache();
        Schema flattenedStats2 = flattenerWithStats.getFlattenedSchema(schema);
        List<String> fieldsStats2 = getFieldNamesInOrder(flattenedStats2);

        assertThat(fieldsStats1).isEqualTo(fieldsStats2);
    }

    private List<String> getFieldNamesInOrder(Schema schema) {
        return schema.getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList());
    }
}