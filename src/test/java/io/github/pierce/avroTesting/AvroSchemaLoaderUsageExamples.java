package io.github.pierce.avroTesting;


import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.types.StructType;
import io.github.pierce.AvroSchemaFlattener;
import io.github.pierce.AvroSchemaLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Practical usage examples for AvroSchemaLoader in different scenarios
 */
public class AvroSchemaLoaderUsageExamples {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaLoaderUsageExamples.class);

    /**
     * Example 1: Basic usage with default settings
     */
    public static void basicUsageExample() throws IOException {
        LOG.info("=== Basic Usage Example ===");

        // Create a default loader
        AvroSchemaLoader loader = AvroSchemaLoader.createDefault();

        // Load a single schema
        StructType userSchema = loader.loadFlattenedSchema("user.avsc");
        LOG.info("Loaded schema with {} fields", userSchema.fields().length);

        // Print field names
        for (String fieldName : userSchema.fieldNames()) {
            LOG.info("  Field: {}", fieldName);
        }
    }

    /**
     * Example 2: Production configuration with multiple search paths
     */
    public static void productionConfigExample() throws IOException {
        LOG.info("=== Production Configuration Example ===");

        // Configure for production environment
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                // Primary location for current schemas
                .withTargetDirectory("/data/schemas/v2")
                // Add multiple fallback locations
                .addSearchPath("/data/schemas/v1")          // Previous version
                .addSearchPath("hdfs://namenode:9000/schemas/current")
                .addSearchPath("s3://my-bucket/schemas")
                // Production settings
                .withArrayStatistics(false)  // Disable for performance
                .withCaching(true)          // Enable caching
                .withHadoopConfiguration(createHadoopConfig())
                .build();

        // Load multiple schemas
        List<String> requiredSchemas = List.of(
                "customer", "order", "product", "transaction"
        );

        AvroSchemaLoader.SchemaLoadResult result =
                loader.loadFlattenedSchemas(requiredSchemas);

        // Handle results
        if (result.isCompleteSuccess()) {
            LOG.info("All {} schemas loaded successfully", result.getSuccessCount());

            // Use the schemas
            Map<String, StructType> schemas = result.getSuccessfulSchemas();
            StructType customerSchema = schemas.get("customer");
            // ... use schemas for Spark operations

        } else {
            LOG.error("Failed to load some schemas");
            result.logSummary();

            // Decide whether to continue or fail
            if (result.getFailureCount() > 2) {
                throw new RuntimeException("Too many schema failures");
            }
        }
    }

    /**
     * Example 3: Development/Testing configuration
     */
    public static void developmentConfigExample() throws IOException {
        LOG.info("=== Development Configuration Example ===");

        // Configure for development/testing
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .withTargetDirectory("src/test/resources/test-schemas")
                .addSearchPath("src/main/resources/schemas")
                .addSearchPath("schemas")  // Project root
                .withArrayStatistics(true)  // Enable for analysis
                .withCaching(false)        // Disable to see changes immediately
                .build();

        // Discover available schemas
        List<String> availableSchemas = loader.discoverAvailableSchemas();
        LOG.info("Found {} schemas:", availableSchemas.size());
        availableSchemas.forEach(s -> LOG.info("  - {}", s));

        // Get location information
        Map<String, AvroSchemaLoader.SchemaLocation> locations =
                loader.getSchemaLocations(availableSchemas);

        LOG.info("\nSchema locations:");
        locations.forEach((name, location) ->
                LOG.info("  {} -> {} [{}]",
                        name, location.getFullPath(), location.getLocationType())
        );
    }

    /**
     * Example 4: Schema analysis and documentation
     */
    public static void schemaAnalysisExample() throws IOException {
        LOG.info("=== Schema Analysis Example ===");

        // Configure for analysis
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .addSearchPath("schemas")
                .withArrayStatistics(true)
                .build();

        // Load schema for analysis
        String schemaName = "complex_transaction.avsc";
        Schema avroSchema = loader.loadAvroSchema(schemaName);

        // Create flattener for detailed analysis
        AvroSchemaFlattener flattener = new AvroSchemaFlattener(true);
        Schema flattenedSchema = flattener.getFlattenedSchema(avroSchema);

        // Get comprehensive metadata
        LOG.info("Schema Analysis for: {}", schemaName);
        LOG.info("Original fields: {}", avroSchema.getFields().size());
        LOG.info("Flattened fields: {}", flattenedSchema.getFields().size());

        // Get detailed statistics
        AvroSchemaFlattener.SchemaStatistics stats = flattener.getSchemaStatistics();
        LOG.info("Max nesting depth: {}", stats.maxNestingDepth);
        LOG.info("Array fields: {}", stats.arrayFieldCount);
        LOG.info("Fields within arrays: {}", stats.fieldsWithinArraysCount);

        // Export to Excel for documentation
        String reportPath = "schema_analysis_" + schemaName.replace(".avsc", ".xlsx");
        flattener.exportToExcel(reportPath);
        LOG.info("Detailed analysis exported to: {}", reportPath);
    }

    /**
     * Example 5: Error handling and recovery
     */
    public static void errorHandlingExample() {
        LOG.info("=== Error Handling Example ===");

        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .addSearchPath("schemas")
                .build();

        List<String> schemasToLoad = List.of(
                "schema1", "schema2", "missing_schema", "schema3"
        );

        AvroSchemaLoader.SchemaLoadResult result =
                loader.loadFlattenedSchemas(schemasToLoad);

        // Handle partial failures
        if (result.hasFailures()) {
            LOG.warn("Some schemas failed to load:");

            result.getFailedSchemas().forEach((name, exception) -> {
                LOG.error("Failed: {} - {}", name, exception.getMessage());

                // Try alternative loading strategy
                try {
                    LOG.info("Attempting to load {} from backup location...", name);
                    AvroSchemaLoader backupLoader = new AvroSchemaLoader.Builder()
                            .addSearchPath("/backup/schemas")
                            .build();

                    StructType schema = backupLoader.loadFlattenedSchema(name);
                    LOG.info("Successfully loaded {} from backup", name);

                } catch (IOException e) {
                    LOG.error("Backup loading also failed for {}", name);
                }
            });
        }

        // Process successful schemas
        Map<String, StructType> successfulSchemas = result.getSuccessfulSchemas();
        LOG.info("Successfully loaded {} schemas", successfulSchemas.size());
    }

    /**
     * Example 6: Integration with Spark operations
     */
    public static void sparkIntegrationExample() throws IOException {
        LOG.info("=== Spark Integration Example ===");

        // Load schemas for Spark job
        AvroSchemaLoader loader = new AvroSchemaLoader.Builder()
                .withTargetDirectory(System.getenv("SCHEMA_DIR"))
                .addSearchPath("hdfs://namenode:9000/schemas")
                .withCaching(true)
                .build();

        // Load schemas needed for the job
        StructType inputSchema = loader.loadFlattenedSchema("raw_events");
        StructType outputSchema = loader.loadFlattenedSchema("processed_events");

        // Use in Spark (pseudo-code)
        /*
        Dataset<Row> rawData = spark.read()
            .schema(inputSchema)
            .parquet("hdfs://path/to/raw/data");

        Dataset<Row> processedData = rawData
            .transform(...)
            .select(outputSchema.fieldNames());

        processedData.write()
            .mode(SaveMode.Append)
            .parquet("hdfs://path/to/processed/data");
        */

        LOG.info("Schemas loaded and ready for Spark processing");
    }

    /**
     * Helper method to create Hadoop configuration
     */
    private static Configuration createHadoopConfig() {
        Configuration conf = new Configuration();

        // Add any custom Hadoop configuration
        conf.set("fs.defaultFS", "hdfs://namenode:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        // S3 configuration if needed
        conf.set("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY"));
        conf.set("fs.s3a.secret.key", System.getenv("AWS_SECRET_KEY"));

        return conf;
    }

    /**
     * Main method to run examples
     */
    public static void main(String[] args) {
        try {
            // Run examples based on command line argument
            if (args.length > 0) {
                switch (args[0]) {
                    case "basic":
                        basicUsageExample();
                        break;
                    case "production":
                        productionConfigExample();
                        break;
                    case "development":
                        developmentConfigExample();
                        break;
                    case "analysis":
                        schemaAnalysisExample();
                        break;
                    case "error":
                        errorHandlingExample();
                        break;
                    case "spark":
                        sparkIntegrationExample();
                        break;
                    default:
                        LOG.info("Unknown example: {}", args[0]);
                        printUsage();
                }
            } else {
                // Run all examples
                basicUsageExample();
                LOG.info("\n");
                developmentConfigExample();
                LOG.info("\n");
                errorHandlingExample();
            }
        } catch (Exception e) {
            LOG.error("Error running example", e);
        }
    }

    private static void printUsage() {
        LOG.info("Usage: java AvroSchemaLoaderUsageExamples [example]");
        LOG.info("Examples: basic, production, development, analysis, error, spark");
    }
}