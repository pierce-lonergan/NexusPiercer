package io.github.pierce.avroTesting;


import org.apache.avro.Schema;
import org.apache.spark.sql.types.StructType;
import io.github.pierce.AvroSchemaLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive test and usage examples for the enhanced AvroSchemaLoader
 */
class AvroSchemaLoaderTest {

    @TempDir
    Path tempDir;

    private String testSchema;

    @BeforeEach
    void setUp() throws IOException {
        AvroSchemaLoader.clearCaches();

        // Create test schema
        testSchema = """
            {
              "type": "record",
              "name": "TestRecord",
              "namespace": "com.test",
              "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"},
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
                },
                {"name": "tags", "type": {"type": "array", "items": "string"}}
              ]
            }
            """;

        // Create test schemas in temp directory
        createTestSchemas();
    }

    private void createTestSchemas() throws IOException {
        // Create schemas directory
        File schemasDir = tempDir.resolve("schemas").toFile();
        schemasDir.mkdirs();

        // Write test schemas
        writeSchema(schemasDir, "TestRecord.avsc", testSchema);
        writeSchema(schemasDir, "User.avsc", createUserSchema());
        writeSchema(schemasDir, "Order.avsc", createOrderSchema());

        // Create another directory for testing search paths
        File configDir = tempDir.resolve("config/schemas").toFile();
        configDir.mkdirs();
        writeSchema(configDir, "ConfigSchema.avsc", createConfigSchema());
    }

    @Test
    void testBasicSchemaLoading() throws IOException {
        // Create loader with custom search path
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .addSearchPath(tempDir.resolve("schemas").toString())
                .build();

        // Load single schema
        Schema schema = loader.loadAvroSchema("TestRecord.avsc");
        assertThat(schema).isNotNull();
        assertThat(schema.getName()).isEqualTo("TestRecord");
        assertThat(schema.getFields()).hasSize(4);
    }

    @Test
    void testFlattenedSchemaLoading() throws IOException {
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .addSearchPath(tempDir.resolve("schemas").toString())
                .withArrayStatistics(true)
                .build();

        // Load flattened schema
        StructType sparkSchema = loader.loadFlattenedSchema("TestRecord");
        assertThat(sparkSchema).isNotNull();

        // Check that nested fields are flattened
        assertThat(sparkSchema.fieldNames()).contains(
                "id", "name", "address_street", "address_city", "tags"
        );

        // With array statistics enabled, should have additional fields
        assertThat(sparkSchema.fieldNames()).contains(
                "tags_count", "tags_distinct_count"
        );
    }

    @Test
    void testTargetDirectoryPriority() throws IOException {
        // Create same schema in two locations with different content
        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();

        String modifiedSchema = testSchema.replace("TestRecord", "ModifiedTestRecord");
        writeSchema(targetDir, "TestRecord.avsc", modifiedSchema);

        // Loader with target directory should find the modified version first
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .withTargetDirectory(targetDir.getAbsolutePath())
                .addSearchPath(tempDir.resolve("schemas").toString())
                .build();

        Schema schema = loader.loadAvroSchema("TestRecord");
        assertThat(schema.getName()).isEqualTo("ModifiedTestRecord");
    }

    @Test
    void testBatchLoading() {
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .addSearchPath(tempDir.resolve("schemas").toString())
                .build();

        // Load multiple schemas
        AvroSchemaLoader.SchemaLoadResult result = loader.loadFlattenedSchemas(
                "TestRecord", "User", "Order", "NonExistent"
        );

        // Check results
        assertThat(result.getSuccessCount()).isEqualTo(3);
        assertThat(result.getFailureCount()).isEqualTo(1);
        assertThat(result.hasFailures()).isTrue();

        Map<String, StructType> successful = result.getSuccessfulSchemas();
        assertThat(successful).containsKeys("TestRecord", "User", "Order");
        assertThat(successful).doesNotContainKey("NonExistent");

        Map<String, Exception> failed = result.getFailedSchemas();
        assertThat(failed).containsKey("NonExistent");

        // Log summary
        result.logSummary();
    }

    @Test
    void testSchemaDiscovery() {
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .addSearchPath(tempDir.resolve("schemas").toString())
                .addSearchPath(tempDir.resolve("config/schemas").toString())
                .build();

        // Discover all available schemas
        List<String> availableSchemas = loader.discoverAvailableSchemas();

        assertThat(availableSchemas).contains(
                "TestRecord.avsc", "User.avsc", "Order.avsc", "ConfigSchema.avsc"
        );
    }

    @Test
    void testSchemaLocationTracking() throws IOException {
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .addSearchPath(tempDir.resolve("schemas").toString())
                .addSearchPath(tempDir.resolve("config/schemas").toString())
                .build();

        // Get location information
        Map<String, AvroSchemaLoader.SchemaLocation> locations =
                loader.getSchemaLocations(List.of("TestRecord.avsc", "ConfigSchema.avsc"));

        assertThat(locations).hasSize(2);

        AvroSchemaLoader.SchemaLocation testRecordLocation = locations.get("TestRecord.avsc");
        assertThat(testRecordLocation.getLocationType())
                .isEqualTo(AvroSchemaLoader.SchemaLocation.LocationType.LOCAL_FILE);
        assertThat(testRecordLocation.getBasePath())
                .contains("schemas");

        AvroSchemaLoader.SchemaLocation configLocation = locations.get("ConfigSchema.avsc");
        assertThat(configLocation.getBasePath())
                .contains("config/schemas");
    }

    @Test
    void testCachingBehavior() throws IOException {
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .addSearchPath(tempDir.resolve("schemas").toString())
                .withCaching(true)
                .build();

        // First load
        long start = System.nanoTime();
        StructType schema1 = loader.loadFlattenedSchema("TestRecord");
        long firstLoadTime = System.nanoTime() - start;

        // Second load (should be cached)
        start = System.nanoTime();
        StructType schema2 = loader.loadFlattenedSchema("TestRecord");
        long secondLoadTime = System.nanoTime() - start;

        // Verify same instance (cached)
        assertThat(schema1).isSameAs(schema2);

        // Second load should be significantly faster
        assertThat(secondLoadTime).isLessThan(firstLoadTime / 2);
    }

    @Test
    void testNoCachingBehavior() throws IOException {
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .addSearchPath(tempDir.resolve("schemas").toString())
                .withCaching(false)
                .build();

        // Load same schema twice
        StructType schema1 = loader.loadFlattenedSchema("TestRecord");
        StructType schema2 = loader.loadFlattenedSchema("TestRecord");

        // Should be different instances (not cached)
        assertThat(schema1).isNotSameAs(schema2);

        // But should be equivalent
        assertThat(schema1.fieldNames()).isEqualTo(schema2.fieldNames());
    }



    @Test
    void testSchemaNotFound() {
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .addSearchPath(tempDir.resolve("schemas").toString())
                .build();

        assertThatThrownBy(() -> loader.loadAvroSchema("NonExistent.avsc"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Schema not found: NonExistent.avsc");
    }

    @Test
    void testProductionScenario() {
        // Simulate production environment with multiple search paths
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .withTargetDirectory("/app/schemas/current")  // Production target
                .addSearchPath("/app/schemas/legacy")         // Legacy schemas
                .addSearchPath("hdfs://namenode:9000/schemas") // HDFS location
                .addSearchPath(tempDir.resolve("schemas").toString()) // Fallback
                .withArrayStatistics(false)  // Disabled in production
                .withCaching(true)           // Enable caching for performance
                .build();

        // In production, this would load from the first available location
        AvroSchemaLoader.SchemaLoadResult result = loader.loadFlattenedSchemas(
                "TestRecord", "User", "Order"
        );

        // Even if some paths don't exist, it should find schemas in available paths
        assertThat(result.getSuccessCount()).isGreaterThanOrEqualTo(3);
    }

    @Test
    void testDevelopmentScenario() throws IOException {
        // Simulate development environment
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .withTargetDirectory("src/test/resources/schemas")
                .addSearchPath("src/main/resources/schemas")
                .addSearchPath(tempDir.resolve("schemas").toString())
                .withArrayStatistics(true)   // Enable for testing
                .withCaching(false)          // Disable for development
                .build();

        // Should find schemas in temp directory (simulating test resources)
        Schema schema = loader.loadAvroSchema("TestRecord");
        assertThat(schema).isNotNull();
    }

    // Helper methods to create test schemas

    private void writeSchema(File dir, String filename, String content) throws IOException {
        File schemaFile = new File(dir, filename);
        try (FileWriter writer = new FileWriter(schemaFile)) {
            writer.write(content);
        }
    }

    private String createUserSchema() {
        return """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.test",
              "fields": [
                {"name": "userId", "type": "long"},
                {"name": "username", "type": "string"},
                {"name": "email", "type": ["null", "string"], "default": null},
                {"name": "createdAt", "type": "long"}
              ]
            }
            """;
    }

    private String createOrderSchema() {
        return """
            {
              "type": "record",
              "name": "Order",
              "namespace": "com.test",
              "fields": [
                {"name": "orderId", "type": "string"},
                {"name": "userId", "type": "long"},
                {"name": "amount", "type": "double"},
                {"name": "status", "type": {"type": "enum", "name": "OrderStatus", 
                  "symbols": ["PENDING", "COMPLETED", "CANCELLED"]}}
              ]
            }
            """;
    }

    private String createConfigSchema() {
        return """
            {
              "type": "record",
              "name": "ConfigSchema",
              "namespace": "com.config",
              "fields": [
                {"name": "key", "type": "string"},
                {"name": "value", "type": "string"},
                {"name": "version", "type": "int"}
              ]
            }
            """;
    }
}
