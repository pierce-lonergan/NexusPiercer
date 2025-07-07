package io.github.pierce.avroTesting;


import org.apache.avro.Schema;
import io.github.pierce.AvroSchemaFlattener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Demonstrates the enhanced features of AvroSchemaFlattener while ensuring
 * backward compatibility with existing functionality.
 */
class AvroSchemaFlattenerEnhancedTest {

    private AvroSchemaFlattener flattener;
    private AvroSchemaFlattener flattenerWithStats;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        AvroSchemaFlattener.clearCache();
        flattener = new AvroSchemaFlattener();
        flattenerWithStats = new AvroSchemaFlattener(true);
    }

    @Test
    void testNoArgConstructorDefaultsToNoStatistics() {
        String schemaJson = """
            {
              "type": "record",
              "name": "TestRecord",
              "namespace": "com.test",
              "fields": [
                {"name": "id", "type": "string"},
                {"name": "tags", "type": {"type": "array", "items": "string"}}
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = flattener.getFlattenedSchema(schema);


        assertThat(flattened.getFields()).hasSize(2);
        assertThat(flattened.getField("tags_count")).isNull();
    }

    @Test
    void testFieldsWithinArraysTracking() {
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
                        {"name": "product", "type": "string"},
                        {"name": "quantity", "type": "int"},
                        {"name": "price", "type": "double"},
                        {
                          "name": "attributes",
                          "type": {
                            "type": "record",
                            "name": "Attributes",
                            "fields": [
                              {"name": "color", "type": "string"},
                              {"name": "size", "type": "string"}
                            ]
                          }
                        }
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


        Set<String> fieldsWithinArrays = flattenerWithStats.getFieldsWithinArrays();


        assertThat(fieldsWithinArrays).containsExactlyInAnyOrder(
                "items_product",
                "items_quantity",
                "items_price",
                "items_attributes_color",
                "items_attributes_size"
        );


        Map<String, AvroSchemaFlattener.TypeTransformation> transformations =
                flattenerWithStats.getTypeTransformations();

        assertThat(transformations).containsKey("items_quantity");
        assertThat(transformations.get("items_quantity").originalType)
                .isEqualTo(Schema.Type.INT);
        assertThat(transformations.get("items_quantity").transformedType)
                .isEqualTo(Schema.Type.STRING);
    }

    @Test
    void testComprehensiveMetadataCollection() {
        String schemaJson = """
            {
              "type": "record",
              "name": "ComplexRecord",
              "namespace": "com.test",
              "fields": [
                {"name": "id", "type": "string", "doc": "Unique identifier"},
                {
                  "name": "metadata",
                  "type": {
                    "type": "record",
                    "name": "Metadata",
                    "fields": [
                      {"name": "version", "type": "int"},
                      {"name": "tags", "type": {"type": "array", "items": "string"}}
                    ]
                  }
                },
                {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["ACTIVE", "INACTIVE"]}},
                {"name": "optional", "type": ["null", "string"], "default": null}
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        flattenerWithStats.getFlattenedSchema(schema);


        List<AvroSchemaFlattener.FieldMetadata> metadata = flattenerWithStats.getFieldMetadata();


        AvroSchemaFlattener.FieldMetadata idField = metadata.stream()
                .filter(f -> f.flattenedName.equals("id"))
                .findFirst()
                .orElseThrow();

        assertThat(idField.originalPath).isEqualTo("id");
        assertThat(idField.nestingDepth).isEqualTo(0);
        assertThat(idField.documentation).isEqualTo("Unique identifier");
        assertThat(idField.isNullable).isFalse();


        AvroSchemaFlattener.FieldMetadata versionField = metadata.stream()
                .filter(f -> f.flattenedName.equals("metadata_version"))
                .findFirst()
                .orElseThrow();

        assertThat(versionField.originalPath).isEqualTo("metadata.version");
        assertThat(versionField.nestingDepth).isEqualTo(1);


        AvroSchemaFlattener.FieldMetadata tagsField = metadata.stream()
                .filter(f -> f.flattenedName.equals("metadata_tags"))
                .findFirst()
                .orElseThrow();

        assertThat(tagsField.isArray).isTrue();
        assertThat(tagsField.originalType).isEqualTo(Schema.Type.ARRAY);
        assertThat(tagsField.flattenedType).isEqualTo(Schema.Type.STRING);
    }

    @Test
    void testSchemaStatistics() {
        String schemaJson = """
            {
              "type": "record",
              "name": "StatisticsTest",
              "namespace": "com.example",
              "fields": [
                {"name": "field1", "type": "string"},
                {
                  "name": "nested1",
                  "type": {
                    "type": "record",
                    "name": "Nested1",
                    "fields": [
                      {"name": "field2", "type": "int"},
                      {
                        "name": "nested2",
                        "type": {
                          "type": "record",
                          "name": "Nested2",
                          "fields": [
                            {"name": "field3", "type": "boolean"},
                            {"name": "array1", "type": {"type": "array", "items": "string"}}
                          ]
                        }
                      }
                    ]
                  }
                },
                {"name": "map1", "type": {"type": "map", "values": "int"}},
                {"name": "enum1", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN"]}}
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        flattenerWithStats.getFlattenedSchema(schema);

        AvroSchemaFlattener.SchemaStatistics stats = flattenerWithStats.getSchemaStatistics();

        assertThat(stats.originalSchemaName).isEqualTo("com.example.StatisticsTest");
        assertThat(stats.originalFieldCount).isEqualTo(4);
        assertThat(stats.maxNestingDepth).isEqualTo(2);
        assertThat(stats.arrayFieldCount).isEqualTo(1);
        assertThat(stats.recordFieldCount).isEqualTo(2);
        assertThat(stats.mapFieldCount).isEqualTo(1);
        assertThat(stats.enumFieldCount).isEqualTo(1);
    }

    @Test
    void testExcelExport() throws IOException {
        String schemaJson = """
            {
              "type": "record",
              "name": "ExcelExportTest",
              "namespace": "com.test",
              "fields": [
                {"name": "id", "type": "string"},
                {
                  "name": "customer",
                  "type": {
                    "type": "record",
                    "name": "Customer",
                    "fields": [
                      {"name": "name", "type": "string"},
                      {"name": "orders", "type": {"type": "array", "items": {
                        "type": "record",
                        "name": "Order",
                        "fields": [
                          {"name": "orderId", "type": "string"},
                          {"name": "amount", "type": "double"}
                        ]
                      }}}
                    ]
                  }
                },
                {"name": "tags", "type": {"type": "array", "items": "string"}}
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        flattenerWithStats.getFlattenedSchema(schema);


        Path excelPath = tempDir.resolve("schema_analysis.xlsx");
        flattenerWithStats.exportToExcel(excelPath.toString());


        assertThat(excelPath).exists();
        assertThat(excelPath).isRegularFile();
        assertThat(excelPath.toFile().length()).isGreaterThan(0);
    }

    @Test
    void testBackwardCompatibility() {

        String schemaJson = """
            {
              "type": "record",
              "name": "BackwardCompatTest",
              "namespace": "com.test",
              "fields": [
                {"name": "name", "type": "string"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {
                  "name": "address",
                  "type": {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                      {"name": "street", "type": "string"},
                      {"name": "city", "type": "string"}
                    ]
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);


        Schema flattenedNoStats = flattener.getFlattenedSchema(schema);
        assertThat(flattenedNoStats.getFields()).hasSize(4);

        // Test with statistics enabled
        Schema flattenedWithStats = flattenerWithStats.getFlattenedSchema(schema);
        assertThat(flattenedWithStats.getFields()).hasSize(10); // 4 base + 6 statistics

        // Verify getArrayFieldNames() still works
        Set<String> arrayFields = flattenerWithStats.getArrayFieldNames();
        assertThat(arrayFields).containsExactly("tags");
    }

    @Test
    void testNestedArraysMetadata() {
        String schemaJson = """
            {
              "type": "record",
              "name": "NestedArrayTest",
              "namespace": "com.test",
              "fields": [
                {
                  "name": "departments",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Department",
                      "fields": [
                        {"name": "name", "type": "string"},
                        {
                          "name": "employees",
                          "type": {
                            "type": "array",
                            "items": {
                              "type": "record",
                              "name": "Employee",
                              "fields": [
                                {"name": "id", "type": "int"},
                                {"name": "name", "type": "string"}
                              ]
                            }
                          }
                        }
                      ]
                    }
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        flattenerWithStats.getFlattenedSchema(schema);

        // Check fields within arrays
        Set<String> fieldsWithinArrays = flattenerWithStats.getFieldsWithinArrays();
        assertThat(fieldsWithinArrays).contains(
                "departments_name",
                "departments_employees_id",
                "departments_employees_name"
        );

        // Check type transformations
        Map<String, AvroSchemaFlattener.TypeTransformation> transformations =
                flattenerWithStats.getTypeTransformations();

        // The int field within the nested array should be transformed to STRING
        assertThat(transformations).containsKey("departments_employees_id");
        assertThat(transformations.get("departments_employees_id").transformedType)
                .isEqualTo(Schema.Type.STRING);
    }
}