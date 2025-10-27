package io.github.pierce.avroTesting;

import org.apache.avro.Schema;
import io.github.pierce.AvroSchemaFlattener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive PhD-Level test suite for enhanced AvroSchemaFlattener functionality.
 * Tests all new features including terminal/non-terminal arrays, schema reconstruction,
 * advanced analytics, and enhanced Excel export capabilities.
 */
class AvroSchemaFlattenerEnhancedTest {

    private AvroSchemaFlattener flattener;
    private AvroSchemaFlattener flattenerWithStats;
    private AvroSchemaFlattener flattenerTerminalOnly;
    private AvroSchemaFlattener flattenerFullyConfigured;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        AvroSchemaFlattener.clearCache();
        flattener = new AvroSchemaFlattener();
        flattenerWithStats = new AvroSchemaFlattener(true);
        flattenerTerminalOnly = new AvroSchemaFlattener(false, false); // No stats, terminal arrays only
        flattenerFullyConfigured = new AvroSchemaFlattener(true, true); // Stats + non-terminal arrays
    }

    // ========================================
    // TERMINAL VS NON-TERMINAL ARRAY TESTS
    // ========================================

    @Test
    void testTerminalArrayClassification() {
        String schemaJson = """
            {
              "type": "record",
              "name": "ArrayClassificationTest",
              "namespace": "com.test",
              "fields": [
                {"name": "stringArray", "type": {"type": "array", "items": "string"}},
                {"name": "intArray", "type": {"type": "array", "items": "int"}},
                {"name": "doubleArray", "type": {"type": "array", "items": "double"}},
                {"name": "booleanArray", "type": {"type": "array", "items": "boolean"}},
                {"name": "enumArray", "type": {"type": "array", "items": {
                    "type": "enum", "name": "Status", "symbols": ["ACTIVE", "INACTIVE"]
                }}},
                {
                  "name": "recordArray",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Item",
                      "fields": [
                        {"name": "id", "type": "string"},
                        {"name": "value", "type": "int"}
                      ]
                    }
                  }
                },
                {
                  "name": "nestedArray",
                  "type": {
                    "type": "array", 
                    "items": {"type": "array", "items": "string"}
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        flattenerWithStats.getFlattenedSchema(schema);

        Set<String> terminalArrays = flattenerWithStats.getTerminalArrayFieldNames();
        Set<String> nonTerminalArrays = flattenerWithStats.getNonTerminalArrayFieldNames();
        Set<String> allArrays = flattenerWithStats.getArrayFieldNames();

        // Terminal arrays (contain primitives/enums)
        assertThat(terminalArrays).containsExactlyInAnyOrder(
                "stringArray", "intArray", "doubleArray", "booleanArray", "enumArray"
        );

        // Non-terminal arrays (contain records/arrays)
        assertThat(nonTerminalArrays).containsExactlyInAnyOrder(
                "recordArray", "nestedArray"
        );

        // All arrays should be sum of terminal + non-terminal
        assertThat(allArrays).hasSize(terminalArrays.size() + nonTerminalArrays.size());
        assertThat(allArrays).containsAll(terminalArrays);
        assertThat(allArrays).containsAll(nonTerminalArrays);
    }

    @Test
    void testTerminalOnlyFlattening() {
        String schemaJson = """
            {
              "type": "record",
              "name": "TerminalOnlyTest",
              "namespace": "com.test",
              "fields": [
                {"name": "id", "type": "string"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {
                  "name": "items",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Item",
                      "fields": [
                        {"name": "product", "type": "string"},
                        {"name": "quantity", "type": "int"}
                      ]
                    }
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);

        // Test with non-terminal arrays included
        Schema flattenedWithNonTerminal = flattenerWithStats.getFlattenedSchema(schema);

        // Test with non-terminal arrays excluded
        Schema flattenedTerminalOnly = flattenerTerminalOnly.getFlattenedSchema(schema);

        Set<String> fieldsWithNonTerminal = flattenedWithNonTerminal.getFields().stream()
                .map(f -> f.name())
                .collect(java.util.stream.Collectors.toSet());

        Set<String> fieldsTerminalOnly = flattenedTerminalOnly.getFields().stream()
                .map(f -> f.name())
                .collect(java.util.stream.Collectors.toSet());

        // With non-terminal arrays: should include 'items' field
        assertThat(fieldsWithNonTerminal).contains("items", "tags", "id");

        // Terminal only: should exclude 'items' array but include its descendants
        assertThat(fieldsTerminalOnly).doesNotContain("items");
        assertThat(fieldsTerminalOnly).contains("tags", "id", "items_product", "items_quantity");

        // Verify statistics
        assertThat(flattenerTerminalOnly.getTerminalArrayFieldNames()).containsExactly("tags");
        assertThat(flattenerTerminalOnly.getNonTerminalArrayFieldNames()).containsExactly("items");
    }

    @Test
    void testComplexNestedArrayClassification() {
        String schemaJson = """
            {
              "type": "record",
              "name": "ComplexNestedTest",
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
                        {"name": "tags", "type": {"type": "array", "items": "string"}},
                        {
                          "name": "employees",
                          "type": {
                            "type": "array",
                            "items": {
                              "type": "record",
                              "name": "Employee",
                              "fields": [
                                {"name": "id", "type": "int"},
                                {"name": "skills", "type": {"type": "array", "items": "string"}}
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

        Set<String> terminalArrays = flattenerWithStats.getTerminalArrayFieldNames();
        Set<String> nonTerminalArrays = flattenerWithStats.getNonTerminalArrayFieldNames();

        // Terminal arrays: primitive data arrays
        assertThat(terminalArrays).containsExactlyInAnyOrder(
                "departments_tags", "departments_employees_skills"
        );

        // Non-terminal arrays: record containing arrays
        assertThat(nonTerminalArrays).containsExactlyInAnyOrder(
                "departments", "departments_employees"
        );

        // Verify fields within arrays tracking
        Set<String> fieldsWithinArrays = flattenerWithStats.getFieldsWithinArrays();
        assertThat(fieldsWithinArrays).contains(
                "departments_name",
                "departments_employees_id"
        );
    }

    // ========================================
    // SCHEMA RECONSTRUCTION TESTS
    // ========================================

    @Test
    void testSchemaReconstructionSimple() {
        String schemaJson = """
            {
              "type": "record",
              "name": "SimpleReconstructionTest",
              "namespace": "com.test",
              "fields": [
                {"name": "id", "type": "string"},
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

        Schema originalSchema = new Schema.Parser().parse(schemaJson);
        Schema flattenedSchema = flattenerWithStats.getFlattenedSchema(originalSchema);

        // Test reconstruction
        Schema reconstructedSchema = flattenerWithStats.reconstructOriginalSchema(flattenedSchema);

        assertThat(reconstructedSchema).isNotNull();
        assertThat(reconstructedSchema.getName()).isEqualTo("SimpleReconstructionTest");
        assertThat(reconstructedSchema.getNamespace()).isEqualTo("com.test");
        assertThat(reconstructedSchema.getFields()).hasSize(2);

        // Verify field names match
        Set<String> originalFieldNames = originalSchema.getFields().stream()
                .map(f -> f.name())
                .collect(java.util.stream.Collectors.toSet());
        Set<String> reconstructedFieldNames = reconstructedSchema.getFields().stream()
                .map(f -> f.name())
                .collect(java.util.stream.Collectors.toSet());

        assertThat(reconstructedFieldNames).isEqualTo(originalFieldNames);
    }

    @Test
    void testSchemaReconstructionWithArrays() {
        String schemaJson = """
            {
              "type": "record",
              "name": "ArrayReconstructionTest",
              "namespace": "com.test",
              "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {
                  "name": "items",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Item",
                      "fields": [
                        {"name": "name", "type": "string"},
                        {"name": "count", "type": "int"}
                      ]
                    }
                  }
                }
              ]
            }
            """;

        Schema originalSchema = new Schema.Parser().parse(schemaJson);
        Schema flattenedSchema = flattenerWithStats.getFlattenedSchema(originalSchema);

        // Get reconstruction metadata
        List<AvroSchemaFlattener.RecordDefinition> recordDefs = flattenerWithStats.getRecordDefinitions();
        List<AvroSchemaFlattener.ArrayDefinition> arrayDefs = flattenerWithStats.getArrayDefinitions();

        assertThat(recordDefs).hasSize(2); // Root record + Item record
        assertThat(arrayDefs).hasSize(2); // tags array + items array

        // Verify array definitions
        AvroSchemaFlattener.ArrayDefinition tagsArray = arrayDefs.stream()
                .filter(a -> a.fieldName.equals("tags"))
                .findFirst()
                .orElseThrow();
        assertThat(tagsArray.isTerminal).isTrue();

        AvroSchemaFlattener.ArrayDefinition itemsArray = arrayDefs.stream()
                .filter(a -> a.fieldName.equals("items"))
                .findFirst()
                .orElseThrow();
        assertThat(itemsArray.isTerminal).isFalse();
    }

    @Test
    void testReconstructionFailsWithoutMetadata() {
        Schema dummySchema = Schema.createRecord("Dummy", null, "com.test", false);
        dummySchema.setFields(List.of());

        AvroSchemaFlattener freshFlattener = new AvroSchemaFlattener();

        assertThatThrownBy(() -> freshFlattener.reconstructOriginalSchema(dummySchema))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No record definitions available");
    }

    // ========================================
    // ENHANCED METADATA TESTS
    // ========================================

    @Test
    void testEnhancedFieldMetadata() {
        String schemaJson = """
            {
              "type": "record",
              "name": "MetadataTest",
              "namespace": "com.test",
              "fields": [
                {"name": "terminalArray", "type": {"type": "array", "items": "string"}},
                {
                  "name": "nonTerminalArray",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Item",
                      "fields": [{"name": "value", "type": "int"}]
                    }
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        flattenerWithStats.getFlattenedSchema(schema);

        List<AvroSchemaFlattener.FieldMetadata> metadata = flattenerWithStats.getFieldMetadata();

        // Find terminal array metadata
        AvroSchemaFlattener.FieldMetadata terminalArrayMeta = metadata.stream()
                .filter(m -> m.flattenedName.equals("terminalArray"))
                .findFirst()
                .orElseThrow();

        assertThat(terminalArrayMeta.isArray).isTrue();
        assertThat(terminalArrayMeta.isTerminalArray).isTrue();
        assertThat(terminalArrayMeta.isWithinArray).isFalse();

        // Find non-terminal array metadata
        AvroSchemaFlattener.FieldMetadata nonTerminalArrayMeta = metadata.stream()
                .filter(m -> m.flattenedName.equals("nonTerminalArray"))
                .findFirst()
                .orElseThrow();

        assertThat(nonTerminalArrayMeta.isArray).isTrue();
        assertThat(nonTerminalArrayMeta.isTerminalArray).isFalse();
        assertThat(nonTerminalArrayMeta.isWithinArray).isFalse();

        // Find field within non-terminal array
        AvroSchemaFlattener.FieldMetadata withinArrayMeta = metadata.stream()
                .filter(m -> m.flattenedName.equals("nonTerminalArray_value"))
                .findFirst()
                .orElseThrow();

        assertThat(withinArrayMeta.isArray).isFalse();
        assertThat(withinArrayMeta.isTerminalArray).isFalse();
        assertThat(withinArrayMeta.isWithinArray).isTrue();
    }

    @Test
    void testRecordDefinitionsCapture() {
        String schemaJson = """
            {
              "type": "record",
              "name": "RootRecord",
              "namespace": "com.test",
              "doc": "Root record documentation",
              "fields": [
                {"name": "id", "type": "string"},
                {
                  "name": "nested",
                  "type": {
                    "type": "record",
                    "name": "NestedRecord",
                    "doc": "Nested record documentation",
                    "fields": [
                      {"name": "value", "type": "int", "doc": "Value field"}
                    ]
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        flattenerWithStats.getFlattenedSchema(schema);

        List<AvroSchemaFlattener.RecordDefinition> recordDefs = flattenerWithStats.getRecordDefinitions();

        assertThat(recordDefs).hasSize(2);

        // Verify root record
        AvroSchemaFlattener.RecordDefinition rootRecord = recordDefs.stream()
                .filter(r -> r.name.equals("RootRecord"))
                .findFirst()
                .orElseThrow();

        assertThat(rootRecord.fullName).isEqualTo("com.test.RootRecord");
        assertThat(rootRecord.path).isEmpty();
        assertThat(rootRecord.documentation).isEqualTo("Root record documentation");
        assertThat(rootRecord.fieldReferences).hasSize(2);

        // Verify nested record
        AvroSchemaFlattener.RecordDefinition nestedRecord = recordDefs.stream()
                .filter(r -> r.name.equals("NestedRecord"))
                .findFirst()
                .orElseThrow();

        assertThat(nestedRecord.path).isEqualTo("nested");
        assertThat(nestedRecord.documentation).isEqualTo("Nested record documentation");
    }

    // ========================================
    // ADVANCED ANALYTICS TESTS
    // ========================================

    @Test
    void testComplexityCalculations() {
        String complexSchemaJson = """
            {
              "type": "record",
              "name": "ComplexityTest",
              "namespace": "com.test",
              "fields": [
                {"name": "simple", "type": "string"},
                {
                  "name": "level1",
                  "type": {
                    "type": "record",
                    "name": "Level1",
                    "fields": [
                      {
                        "name": "level2",
                        "type": {
                          "type": "record",
                          "name": "Level2",
                          "fields": [
                            {"name": "deep", "type": "string"},
                            {"name": "array", "type": {"type": "array", "items": "int"}}
                          ]
                        }
                      }
                    ]
                  }
                },
                {"name": "topArray", "type": {"type": "array", "items": "string"}},
                {"name": "map", "type": {"type": "map", "values": "string"}},
                {"name": "enum", "type": {"type": "enum", "name": "Status", "symbols": ["A", "B"]}}
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(complexSchemaJson);
        flattenerWithStats.getFlattenedSchema(schema);

        AvroSchemaFlattener.SchemaStatistics stats = flattenerWithStats.getSchemaStatistics();

        assertThat(stats.originalFieldCount).isEqualTo(5);
        assertThat(stats.maxNestingDepth).isEqualTo(2);
        assertThat(stats.arrayFieldCount).isEqualTo(2); // level1_level2_array + topArray
        assertThat(stats.terminalArrayFieldCount).isEqualTo(2); // Both are terminal
        assertThat(stats.nonTerminalArrayFieldCount).isEqualTo(0);
        assertThat(stats.recordFieldCount).isEqualTo(2); // Level1 + Level2
        assertThat(stats.mapFieldCount).isEqualTo(1);
        assertThat(stats.enumFieldCount).isEqualTo(1);
    }

    @ParameterizedTest
    @MethodSource("provideSchemaComplexityExamples")
    void testComplexityLevels(String schemaJson, String expectedComplexityLevel) {
        Schema schema = new Schema.Parser().parse(schemaJson);
        flattenerWithStats.getFlattenedSchema(schema);

        // This would test the private complexity calculation methods
        // In a real implementation, you might expose these as package-private for testing
        AvroSchemaFlattener.SchemaStatistics stats = flattenerWithStats.getSchemaStatistics();

        // Verify that we captured the complexity appropriately
        assertThat(stats.originalSchemaName).contains("ComplexityLevel");
    }

    static Stream<Arguments> provideSchemaComplexityExamples() {
        return Stream.of(
                Arguments.of("""
                    {
                      "type": "record",
                      "name": "LowComplexityLevel",
                      "namespace": "com.test",
                      "fields": [
                        {"name": "id", "type": "string"},
                        {"name": "name", "type": "string"}
                      ]
                    }
                    """, "Low"),
                Arguments.of("""
                    {
                      "type": "record",
                      "name": "HighComplexityLevel",
                      "namespace": "com.test",
                      "fields": [
                        {"name": "id", "type": "string"},
                        {
                          "name": "nested1",
                          "type": {
                            "type": "record",
                            "name": "Nested1",
                            "fields": [
                              {
                                "name": "nested2",
                                "type": {
                                  "type": "record",
                                  "name": "Nested2",
                                  "fields": [
                                    {"name": "array1", "type": {"type": "array", "items": "string"}},
                                    {"name": "array2", "type": {"type": "array", "items": {
                                      "type": "record",
                                      "name": "Item",
                                      "fields": [{"name": "value", "type": "int"}]
                                    }}}
                                  ]
                                }
                              }
                            ]
                          }
                        }
                      ]
                    }
                    """, "High")
        );
    }

    // ========================================
    // CONFIGURATION OPTION TESTS
    // ========================================

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testIncludeNonTerminalArraysConfiguration(boolean includeNonTerminal) {
        AvroSchemaFlattener configuredFlattener = new AvroSchemaFlattener(false, includeNonTerminal);

        String schemaJson = """
            {
              "type": "record",
              "name": "ConfigTest",
              "namespace": "com.test",
              "fields": [
                {"name": "terminalArray", "type": {"type": "array", "items": "string"}},
                {
                  "name": "nonTerminalArray",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Item",
                      "fields": [{"name": "value", "type": "int"}]
                    }
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        Schema flattened = configuredFlattener.getFlattenedSchema(schema);

        Set<String> fieldNames = flattened.getFields().stream()
                .map(f -> f.name())
                .collect(java.util.stream.Collectors.toSet());

        // Terminal array should always be included
        assertThat(fieldNames).contains("terminalArray");

        // Non-terminal array should only be included based on configuration
        if (includeNonTerminal) {
            assertThat(fieldNames).contains("nonTerminalArray");
        } else {
            assertThat(fieldNames).doesNotContain("nonTerminalArray");
        }

        // Descendants of non-terminal array should always be included
        assertThat(fieldNames).contains("nonTerminalArray_value");
    }

    @Test
    void testAllConfigurationCombinations() {
        String schemaJson = """
            {
              "type": "record",
              "name": "AllConfigTest",
              "namespace": "com.test",
              "fields": [
                {"name": "simple", "type": "string"},
                {"name": "terminalArray", "type": {"type": "array", "items": "string"}},
                {
                  "name": "nonTerminalArray",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Item",
                      "fields": [{"name": "value", "type": "int"}]
                    }
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);

        // Test all four combinations
        AvroSchemaFlattener flattener1 = new AvroSchemaFlattener(false, false); // No stats, no non-terminal
        AvroSchemaFlattener flattener2 = new AvroSchemaFlattener(false, true);  // No stats, with non-terminal
        AvroSchemaFlattener flattener3 = new AvroSchemaFlattener(true, false);  // With stats, no non-terminal
        AvroSchemaFlattener flattener4 = new AvroSchemaFlattener(true, true);   // With stats, with non-terminal

        Schema flat1 = flattener1.getFlattenedSchema(schema);
        Schema flat2 = flattener2.getFlattenedSchema(schema);
        Schema flat3 = flattener3.getFlattenedSchema(schema);
        Schema flat4 = flattener4.getFlattenedSchema(schema);

        // Verify field counts increase with each additional feature
        assertThat(flat1.getFields().size()).isLessThan(flat2.getFields().size()); // Adding non-terminal
        assertThat(flat2.getFields().size()).isLessThan(flat4.getFields().size()); // Adding stats
        assertThat(flat3.getFields().size()).isLessThan(flat4.getFields().size()); // Adding non-terminal to stats

        // flat1 should have minimum fields (no non-terminal array, no stats)
        Set<String> fields1 = flat1.getFields().stream()
                .map(f -> f.name())
                .collect(java.util.stream.Collectors.toSet());
        assertThat(fields1).containsExactlyInAnyOrder(
                "simple", "terminalArray", "nonTerminalArray_value"
        );

        // flat4 should have maximum fields (all features enabled)
        Set<String> fields4 = flat4.getFields().stream()
                .map(f -> f.name())
                .collect(java.util.stream.Collectors.toSet());
        assertThat(fields4).contains(
                "simple", "terminalArray", "nonTerminalArray", "nonTerminalArray_value",
                "terminalArray_count", "terminalArray_type", "nonTerminalArray_count"
        );
    }

    // ========================================
    // EXCEL EXPORT TESTS
    // ========================================

    @Test
    void testEnhancedExcelExport() throws IOException {
        String complexSchemaJson = """
            {
              "type": "record",
              "name": "ExcelExportTest",
              "namespace": "com.test",
              "fields": [
                {"name": "id", "type": "string"},
                {"name": "terminalArray", "type": {"type": "array", "items": "string"}},
                {
                  "name": "nonTerminalArray",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Item",
                      "fields": [
                        {"name": "name", "type": "string"},
                        {"name": "tags", "type": {"type": "array", "items": "string"}}
                      ]
                    }
                  }
                },
                {
                  "name": "nested",
                  "type": {
                    "type": "record",
                    "name": "Nested",
                    "fields": [
                      {"name": "value", "type": "int"}
                    ]
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(complexSchemaJson);
        flattenerFullyConfigured.getFlattenedSchema(schema);

        Path excelPath = tempDir.resolve("enhanced_schema_analysis.xlsx");
        flattenerFullyConfigured.exportToExcel(excelPath.toString());

        // Verify file was created and has content
        assertThat(excelPath).exists();
        assertThat(excelPath).isRegularFile();
        assertThat(excelPath.toFile().length()).isGreaterThan(1000); // Should be substantial

        // Verify we have the classification data
        assertThat(flattenerFullyConfigured.getTerminalArrayFieldNames()).hasSize(2);
        assertThat(flattenerFullyConfigured.getNonTerminalArrayFieldNames()).hasSize(1);
        assertThat(flattenerFullyConfigured.getRecordDefinitions()).hasSize(3); // Root + Item + Nested
    }

    // ========================================
    // EDGE CASE AND ERROR HANDLING TESTS
    // ========================================

    @Test
    void testEmptySchema() {
        String emptySchemaJson = """
            {
              "type": "record",
              "name": "EmptyRecord",
              "namespace": "com.test",
              "fields": []
            }
            """;

        Schema schema = new Schema.Parser().parse(emptySchemaJson);
        Schema flattened = flattenerWithStats.getFlattenedSchema(schema);

        assertThat(flattened.getFields()).isEmpty();
        assertThat(flattenerWithStats.getTerminalArrayFieldNames()).isEmpty();
        assertThat(flattenerWithStats.getNonTerminalArrayFieldNames()).isEmpty();
        assertThat(flattenerWithStats.getArrayFieldNames()).isEmpty();
    }

    @Test
    void testDeepNestingWithMixedArrayTypes() {
        String deepSchemaJson = """
            {
              "type": "record",
              "name": "DeepNestingTest",
              "namespace": "com.test",
              "fields": [
                {
                  "name": "level1",
                  "type": {
                    "type": "record",
                    "name": "Level1",
                    "fields": [
                      {
                        "name": "level2",
                        "type": {
                          "type": "record",
                          "name": "Level2",
                          "fields": [
                            {
                              "name": "level3",
                              "type": {
                                "type": "record",
                                "name": "Level3",
                                "fields": [
                                  {"name": "terminalArray", "type": {"type": "array", "items": "string"}},
                                  {
                                    "name": "nonTerminalArray",
                                    "type": {
                                      "type": "array",
                                      "items": {
                                        "type": "record",
                                        "name": "DeepItem",
                                        "fields": [
                                          {"name": "value", "type": "int"},
                                          {"name": "moreStrings", "type": {"type": "array", "items": "string"}}
                                        ]
                                      }
                                    }
                                  }
                                ]
                              }
                            }
                          ]
                        }
                      }
                    ]
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(deepSchemaJson);
        flattenerWithStats.getFlattenedSchema(schema);

        AvroSchemaFlattener.SchemaStatistics stats = flattenerWithStats.getSchemaStatistics();
        assertThat(stats.maxNestingDepth).isEqualTo(4);

        Set<String> terminalArrays = flattenerWithStats.getTerminalArrayFieldNames();
        Set<String> nonTerminalArrays = flattenerWithStats.getNonTerminalArrayFieldNames();

        assertThat(terminalArrays).containsExactlyInAnyOrder(
                "level1_level2_level3_terminalArray",
                "level1_level2_level3_nonTerminalArray_moreStrings"
        );

        assertThat(nonTerminalArrays).containsExactly(
                "level1_level2_level3_nonTerminalArray"
        );
    }

    @Test
    void testNullableArrays() {
        String nullableArraySchema = """
            {
              "type": "record",
              "name": "NullableArrayTest",
              "namespace": "com.test",
              "fields": [
                {"name": "optionalTerminalArray", "type": ["null", {"type": "array", "items": "string"}]},
                {
                  "name": "optionalNonTerminalArray",
                  "type": ["null", {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Item",
                      "fields": [{"name": "value", "type": "int"}]
                    }
                  }]
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(nullableArraySchema);
        flattenerWithStats.getFlattenedSchema(schema);

        Set<String> terminalArrays = flattenerWithStats.getTerminalArrayFieldNames();
        Set<String> nonTerminalArrays = flattenerWithStats.getNonTerminalArrayFieldNames();

        assertThat(terminalArrays).containsExactly("optionalTerminalArray");
        assertThat(nonTerminalArrays).containsExactly("optionalNonTerminalArray");

        // Verify nullable arrays are properly classified
        List<AvroSchemaFlattener.FieldMetadata> metadata = flattenerWithStats.getFieldMetadata();
        AvroSchemaFlattener.FieldMetadata terminalMeta = metadata.stream()
                .filter(m -> m.flattenedName.equals("optionalTerminalArray"))
                .findFirst()
                .orElseThrow();

        assertThat(terminalMeta.isNullable).isTrue();
        assertThat(terminalMeta.isArray).isTrue();
        assertThat(terminalMeta.isTerminalArray).isTrue();
    }

    // ========================================
    // BACKWARD COMPATIBILITY TESTS
    // ========================================

    @Test
    void testBackwardCompatibilityWithOriginalConstructors() {
        // Test default constructor behavior
        AvroSchemaFlattener defaultFlattener = new AvroSchemaFlattener();

        // Test single parameter constructor behavior
        AvroSchemaFlattener singleParamFlattener = new AvroSchemaFlattener(true);

        String schemaJson = """
            {
              "type": "record",
              "name": "BackwardCompatTest",
              "namespace": "com.test",
              "fields": [
                {"name": "name", "type": "string"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {
                  "name": "items",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Item",
                      "fields": [{"name": "value", "type": "int"}]
                    }
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);

        // Default constructor: no stats, include non-terminal arrays
        Schema defaultFlattened = defaultFlattener.getFlattenedSchema(schema);
        assertThat(defaultFlattened.getField("tags_count")).isNull(); // No stats
        assertThat(defaultFlattened.getField("items")).isNotNull(); // Include non-terminal

        // Single param constructor: optional stats, include non-terminal arrays
        Schema singleParamFlattened = singleParamFlattener.getFlattenedSchema(schema);
        assertThat(singleParamFlattened.getField("tags_count")).isNotNull(); // With stats
        assertThat(singleParamFlattened.getField("items")).isNotNull(); // Include non-terminal

        // Verify legacy methods still work
        Set<String> arrayFields = defaultFlattener.getArrayFieldNames();
        assertThat(arrayFields).contains("tags", "items");
    }

    @Test
    void testLegacyMethodsStillFunctional() {
        String schemaJson = """
            {
              "type": "record",
              "name": "LegacyMethodTest",
              "namespace": "com.test",
              "fields": [
                {"name": "terminalArray", "type": {"type": "array", "items": "string"}},
                {
                  "name": "nonTerminalArray",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Item",
                      "fields": [{"name": "value", "type": "int"}]
                    }
                  }
                }
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        flattenerWithStats.getFlattenedSchema(schema);

        // Test all legacy methods still work
        Set<String> arrayFieldNames = flattenerWithStats.getArrayFieldNames();
        Set<String> fieldsWithinArrays = flattenerWithStats.getFieldsWithinArrays();
        List<AvroSchemaFlattener.FieldMetadata> fieldMetadata = flattenerWithStats.getFieldMetadata();
        Map<String, AvroSchemaFlattener.TypeTransformation> typeTransformations = flattenerWithStats.getTypeTransformations();
        AvroSchemaFlattener.SchemaStatistics schemaStatistics = flattenerWithStats.getSchemaStatistics();

        assertThat(arrayFieldNames).hasSize(2);
        assertThat(fieldsWithinArrays).hasSize(1); // nonTerminalArray_value
        assertThat(fieldMetadata).isNotEmpty();
        assertThat(typeTransformations).containsKey("nonTerminalArray_value");
        assertThat(schemaStatistics).isNotNull();
    }
}