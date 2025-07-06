package io.github.pierce.spark;

import io.github.pierce.AvroSchemaFlattener;
import io.github.pierce.CreateSparkStructFromAvroSchema;
import io.github.pierce.JsonFlattenerConsolidator;
import io.github.pierce.files.FileFinder;
import org.apache.avro.Schema;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

// ... (class javadoc and other methods remain the same) ...
public class NexusPiercerSparkPipeline implements Serializable {

    // ... fields, enums, inner classes ...
    private static final Logger LOG = LoggerFactory.getLogger(NexusPiercerSparkPipeline.class);
    private static final long serialVersionUID = 1L;

    // Cache for schemas to improve performance
    private static final Map<String, CachedSchema> SCHEMA_CACHE = new ConcurrentHashMap<>();

    private final SparkSession spark;
    private final PipelineMode mode;
    private final PipelineConfig config;

    // Cached objects
    private JsonFlattenerConsolidator flattener;
    private AvroSchemaFlattener schemaFlattener;

    /**
     * Pipeline execution mode
     */
    public enum PipelineMode {
        BATCH,
        STREAMING
    }

    /**
     * Error handling strategies
     */
    public enum ErrorHandling {
        FAIL_FAST,      // Fail on first error
        SKIP_MALFORMED, // Skip malformed records
        QUARANTINE,     // Move bad records to error dataset
        PERMISSIVE      // Try to parse what's possible
    }

    /**
     * Disables quarantining for schema validation errors.
     * Records with fields that fail to parse against the schema (e.g., wrong data type)
     * will be kept in the main dataset with the invalid fields set to null, instead
     * of being moved to the error dataset. Syntactically malformed JSON will still be
     * quarantined. This is a more lenient parsing mode.
     *
     * @return The pipeline for further configuration.
     */
    public NexusPiercerSparkPipeline allowSchemaErrors() {
        this.config.quarantineSchemaErrors = false;
        return this;
    }

// You could also add an explicit "enable" method for clarity if you wish:
    /**
     * Enables quarantining for schema validation errors (default behavior).
     * Records with fields that fail to parse will be moved to the error dataset.
     *
     * @return The pipeline for further configuration.
     */
    public NexusPiercerSparkPipeline quarantineSchemaErrors() {
        this.config.quarantineSchemaErrors = true;
        return this;
    }
    /**
     * Pipeline configuration
     */
    public static class PipelineConfig implements Serializable {
        private static final long serialVersionUID = 1L;
        // Schema configuration
        private String schemaPath;
        private Schema avroSchema;
        private boolean includeArrayStatistics = true;
        private boolean quarantineSchemaErrors = true; // Default to the strict behavior

        // JSON flattening configuration
        private String arrayDelimiter = ",";
        private String nullPlaceholder = null;
        private int maxNestingDepth = 50;
        private int maxArraySize = 1000;
        private boolean consolidateWithMatrixDenotors = false;

        // Array explosion configuration
        private final Set<String> explosionPaths = new HashSet<>();

        // Processing configuration
        private ErrorHandling errorHandling = ErrorHandling.SKIP_MALFORMED;
        private boolean enableMetrics = true;
        private boolean cacheSchemas = true;
        private int repartitionCount = -1; // -1 means no repartitioning

        // Column naming
        private boolean preserveDots = false; // false = dots become underscores

        // Output configuration
        private boolean includeMetadata = false; // Add _processing_time, _source_file, etc.
        private boolean includeRawJson = false;  // Keep original JSON in _raw column
    }

    /**
     * Cached schema information
     */
    private static class CachedSchema implements Serializable {
        private static final long serialVersionUID = 1L;
        final Schema originalSchema;
        final Schema flattenedSchema;
        final StructType sparkSchema;
        final Set<String> arrayFields;
        final long cachedAt;

        CachedSchema(Schema original, Schema flattened, StructType spark, Set<String> arrays) {
            this.originalSchema = original;
            this.flattenedSchema = flattened;
            this.sparkSchema = spark;
            this.arrayFields = arrays;
            this.cachedAt = System.currentTimeMillis();
        }
    }

    /**
     * Processing result with metrics
     */
    public static class ProcessingResult {
        private final Dataset<Row> dataset;
        private final Dataset<Row> errorDataset;
        private final ProcessingMetrics metrics;

        ProcessingResult(Dataset<Row> dataset, Dataset<Row> errorDataset, ProcessingMetrics metrics) {
            this.dataset = dataset;
            this.errorDataset = errorDataset;
            this.metrics = metrics;
        }

        public Dataset<Row> getDataset() { return dataset; }
        public Dataset<Row> getErrorDataset() { return errorDataset; }
        public ProcessingMetrics getMetrics() { return metrics; }

        /**
         * Write the result with builder pattern
         */
        public DataFrameWriter<Row> write() {
            return dataset.write();
        }

        /**
         * For streaming results
         */
        public DataStreamWriter<Row> writeStream() {
            if (dataset.isStreaming()) {
                return dataset.writeStream();
            }
            throw new IllegalStateException("writeStream() can only be called on streaming datasets");
        }
    }

    /**
     * Processing metrics
     */
    public static class ProcessingMetrics implements Serializable {
        private static final long serialVersionUID = 1L;
        private long totalRecords;
        private long successfulRecords;
        private long malformedRecords;
        private long schemaValidationFailures;
        private long explosionRecords;
        private long processingTimeMs;
        private Map<String, Long> customMetrics = new HashMap<>();

        // Getters
        public long getTotalRecords() { return totalRecords; }
        public long getSuccessfulRecords() { return successfulRecords; }
        public long getMalformedRecords() { return malformedRecords; }
        public long getSchemaValidationFailures() { return schemaValidationFailures; }
        public long getExplosionRecords() { return explosionRecords; }
        public long getProcessingTimeMs() { return processingTimeMs; }
        public Map<String, Long> getCustomMetrics() { return customMetrics; }

        public double getSuccessRate() {
            return totalRecords > 0 ? (double) successfulRecords / totalRecords : 0.0;
        }

        @Override
        public String toString() {
            return String.format(
                    "ProcessingMetrics[total=%d, success=%d (%.2f%%), malformed=%d, schemaFail=%d, exploded=%d, time=%dms]",
                    totalRecords, successfulRecords, getSuccessRate() * 100,
                    malformedRecords, schemaValidationFailures, explosionRecords, processingTimeMs
            );
        }
    }

    // ===== CONSTRUCTORS AND FACTORY METHODS =====

    private NexusPiercerSparkPipeline(SparkSession spark, PipelineMode mode) {
        this.spark = spark;
        this.mode = mode;
        this.config = new PipelineConfig();
    }

    /**
     * Create a pipeline for batch processing
     */
    public static NexusPiercerSparkPipeline forBatch(SparkSession spark) {
        return new NexusPiercerSparkPipeline(spark, PipelineMode.BATCH);
    }

    /**
     * Create a pipeline for streaming processing
     */
    public static NexusPiercerSparkPipeline forStreaming(SparkSession spark) {
        return new NexusPiercerSparkPipeline(spark, PipelineMode.STREAMING);
    }

    // ===== CONFIGURATION METHODS (FLUENT API) =====
    public NexusPiercerSparkPipeline withSchema(String schemaPath) {
        this.config.schemaPath = schemaPath;
        this.config.avroSchema = null; // Clear any previously set schema object
        return this;
    }

    public NexusPiercerSparkPipeline withSchema(Schema schema) {
        this.config.avroSchema = schema;
        this.config.schemaPath = null; // Clear any previously set schema path
        return this;
    }

    public NexusPiercerSparkPipeline withArrayDelimiter(String delimiter) {
        this.config.arrayDelimiter = delimiter;
        return this;
    }

    public NexusPiercerSparkPipeline withNullPlaceholder(String placeholder) {
        this.config.nullPlaceholder = placeholder;
        return this;
    }

    public NexusPiercerSparkPipeline withMaxNestingDepth(int depth) {
        this.config.maxNestingDepth = depth;
        return this;
    }

    public NexusPiercerSparkPipeline withMaxArraySize(int size) {
        this.config.maxArraySize = size;
        return this;
    }

    public NexusPiercerSparkPipeline enableArrayStatistics() {
        this.config.includeArrayStatistics = true;
        return this;
    }

    public NexusPiercerSparkPipeline disableArrayStatistics() {
        this.config.includeArrayStatistics = false;
        return this;
    }

    public NexusPiercerSparkPipeline enableMatrixDenotors() {
        this.config.consolidateWithMatrixDenotors = true;
        return this;
    }

    public NexusPiercerSparkPipeline withErrorHandling(ErrorHandling strategy) {
        this.config.errorHandling = strategy;
        return this;
    }

    public NexusPiercerSparkPipeline explodeArrays(String... paths) {
        this.config.explosionPaths.addAll(Arrays.asList(paths));
        return this;
    }

    public NexusPiercerSparkPipeline enableMetrics() {
        this.config.enableMetrics = true;
        return this;
    }

    public NexusPiercerSparkPipeline disableSchemaCache() {
        this.config.cacheSchemas = false;
        return this;
    }

    public NexusPiercerSparkPipeline withRepartition(int partitions) {
        this.config.repartitionCount = partitions;
        return this;
    }

    public NexusPiercerSparkPipeline preserveDots() {
        this.config.preserveDots = true;
        return this;
    }

    public NexusPiercerSparkPipeline includeMetadata() {
        this.config.includeMetadata = true;
        return this;
    }

    public NexusPiercerSparkPipeline includeRawJson() {
        this.config.includeRawJson = true;
        return this;
    }
    // ...

    // ===== PROCESSING METHODS =====

    /**
     * Process batch data from file paths
     */
    public ProcessingResult process(String... inputPaths) {
        if (mode != PipelineMode.BATCH) {
            throw new IllegalStateException("process() can only be called in BATCH mode");
        }
        Dataset<String> jsonDs = spark.read().text(inputPaths).as(Encoders.STRING());
        return this.processDataset(jsonDs);
    }

    /**
     * Process batch or streaming data from an existing dataset.
     * The behavior (batch/streaming) is determined by the pipeline's mode.
     * This method standardizes the input column to be named 'value'.
     */
    public ProcessingResult processDataset(Dataset<String> jsonDataset) {
        long startTime = System.currentTimeMillis();
        ProcessingMetrics metrics = new ProcessingMetrics();

        try {
            CachedSchema cachedSchema = loadSchema();

            // --- FIX IS HERE ---
            // Standardize the input column name to 'value' for internal processing.
            String originalColumn = jsonDataset.columns()[0];
            Dataset<String> standardizedDataset = jsonDataset.withColumnRenamed(originalColumn, "value").as(Encoders.STRING());
            // --- END FIX ---

            return processDataset(standardizedDataset, cachedSchema, metrics, startTime);
        } catch (Exception e) {
            LOG.error("Failed to process dataset", e);
            throw new RuntimeException("Pipeline processing failed", e);
        }
    }


    /**
     * Process streaming data
     */
    public ProcessingResult processStream(String source, Map<String, String> options) {
        if (mode != PipelineMode.STREAMING) {
            throw new IllegalStateException("processStream() can only be called in STREAMING mode");
        }

        // Create streaming dataset, ensuring the string column is named 'value'
        Dataset<String> jsonDs = spark.readStream()
                .format(source)
                .options(options)
                .load()
                .selectExpr("CAST(value AS STRING) as value")
                .filter(col("value").isNotNull())
                .as(Encoders.STRING());

        // Use the common processDataset method
        return this.processDataset(jsonDs);
    }

    // ===== CORE PROCESSING LOGIC =====

    /**
     * Core processing logic for both batch and streaming (private helper)
     */
    // In NexusPiercerSparkPipeline.java

    /**
     * Core processing logic for both batch and streaming (private helper)
     */
    // In NexusPiercerSparkPipeline.java

    private ProcessingResult processDataset(Dataset<String> jsonDs, CachedSchema cachedSchema,
                                            ProcessingMetrics metrics, long startTime) {
        initializeProcessors();

        if (config.repartitionCount > 0) {
            jsonDs = jsonDs.repartition(config.repartitionCount);
        }

        Dataset<Row> allProcessedRecords;
        if (config.explosionPaths.isEmpty()) {
            allProcessedRecords = processFlattenedMode(jsonDs, cachedSchema);
        } else {
            allProcessedRecords = processExplosionMode(jsonDs, cachedSchema);
        }

        allProcessedRecords.cache();

        Dataset<Row> successDataset;
        Dataset<Row> errorDataset = null;

        // Define filters for different error types
        Column malformedSyntaxError = col("_error").equalTo("Malformed JSON string");
        Column schemaValidationError = col("_error").equalTo("Schema validation failed");

        // Handle errors based on strategy
        if (config.errorHandling == ErrorHandling.QUARANTINE) {
            // The error dataset ALWAYS contains all records that have an error message.
            errorDataset = allProcessedRecords.filter(col("_error").isNotNull());

            if (config.quarantineSchemaErrors) {
                // STRICT MODE: Success is only records with no error at all.
                successDataset = allProcessedRecords.filter(col("_error").isNull());
            } else {
                // LENIENT MODE (allowSchemaErrors):
                // Success is records with EITHER no error OR just a schema validation error.
                successDataset = allProcessedRecords.filter(col("_error").isNull().or(schemaValidationError));
            }

        } else if (config.errorHandling == ErrorHandling.SKIP_MALFORMED) {
            // In SKIP_MALFORMED, we simply filter out all errors and don't produce an error dataset.
            successDataset = allProcessedRecords.filter(col("_error").isNull());
            errorDataset = null;
        } else {
            // For PERMISSIVE or FAIL_FAST, all processed records are considered "successful" from this POV.
            successDataset = allProcessedRecords;
        }


        // The _error column should never be in the final success dataset.
        if (Arrays.asList(successDataset.columns()).contains("_error")) {
            successDataset = successDataset.drop("_error");
        }

        if (config.includeMetadata) {
            successDataset = addMetadataColumns(successDataset);
        }

        // Metrics should be collected on the initial, combined set of records.
        if (config.enableMetrics && mode == PipelineMode.BATCH) {
            collectMetrics(allProcessedRecords, metrics); // Pass the unified dataset
        }

        metrics.processingTimeMs = System.currentTimeMillis() - startTime;
        allProcessedRecords.unpersist();

        return new ProcessingResult(successDataset, errorDataset, metrics);
    }

    // In NexusPiercerSparkPipeline.java

    private Dataset<Row> processFlattenedMode(Dataset<String> jsonDs, CachedSchema cachedSchema) {
        // UDF for custom flattening
        UserDefinedFunction flattenUdf = udf(
                (String json) -> {
                    if (json == null || json.trim().isEmpty()) {
                        return null;
                    }
                    try {
                        return flattener.flattenAndConsolidateJson(json);
                    } catch (Exception e) {
                        return "{}";
                    }
                },
                DataTypes.StringType
        );

        // Stage 1: Identify syntactically malformed JSON.
        Dataset<Row> syntaxChecked = jsonDs
                .withColumn("is_malformed_syntax",
                        from_json(col("value"), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)).isNull()
                );

        // Stage 2: Apply flattener.
        Dataset<Row> flattened = syntaxChecked
                .withColumn("flattened_json",
                        when(not(col("is_malformed_syntax")), flattenUdf.apply(col("value")))
                                .otherwise(null)
                );

        // Stage 3: Parse data.
        Dataset<Row> parsed = flattened
                .withColumn("data", from_json(col("flattened_json"), cachedSchema.sparkSchema));

        // Stage 4: ALWAYS build the full error column, regardless of config.
        // This is the key fix. This method's only job is to identify and label everything correctly.
        if (config.errorHandling == ErrorHandling.QUARANTINE ||
                config.errorHandling == ErrorHandling.SKIP_MALFORMED) {

            // --- The reliable Detector Logic ---
            Column schemaErrorCondition = lit(false);
            for (StructField field : cachedSchema.sparkSchema.fields()) {
                String fieldName = field.name();
                DataType dataType = field.dataType();

                if (dataType instanceof org.apache.spark.sql.types.NumericType ||
                        dataType.equals(DataTypes.BooleanType) ||
                        dataType.equals(DataTypes.TimestampType) ||
                        dataType.equals(DataTypes.DateType)) {

                    Column fieldIsCorrupt = col("data." + fieldName).isNull()
                            .and(get_json_object(col("flattened_json"), "$." + fieldName).isNotNull());
                    schemaErrorCondition = schemaErrorCondition.or(fieldIsCorrupt);
                }
            }

            // Create the error column with ALL possible errors.
            parsed = parsed.withColumn("_error",
                    when(col("is_malformed_syntax"), lit("Malformed JSON string"))
                            .when(schemaErrorCondition, lit("Schema validation failed"))
                            .otherwise(lit(null).cast(DataTypes.StringType))
            );
        }

        // Stage 5: Select final columns.
        List<Column> finalCols = new ArrayList<>();
        finalCols.add(col("data.*"));

        if (config.includeRawJson || config.errorHandling == ErrorHandling.QUARANTINE) {
            finalCols.add(col("value").as("_raw_json"));
        }

        if (Arrays.asList(parsed.columns()).contains("_error")) {
            finalCols.add(col("_error"));
        }

        return parsed.select(finalCols.toArray(new Column[0]));
    }

    private Dataset<Row> processExplosionMode(Dataset<String> jsonDs, CachedSchema cachedSchema) {
        // Create flattener with explosion paths
        JsonFlattenerConsolidator explosionFlattener = new JsonFlattenerConsolidator(
                config.arrayDelimiter,
                config.nullPlaceholder,
                config.maxNestingDepth,
                config.maxArraySize,
                config.consolidateWithMatrixDenotors,
                config.includeArrayStatistics,
                config.explosionPaths.toArray(new String[0])
        );

        // Create UDF for explosion
        UserDefinedFunction explodeUdf = udf(
                (String json) -> {
                    if (json == null || json.trim().isEmpty()) {
                        return null;
                    }
                    try {
                        List<String> exploded = explosionFlattener.flattenAndExplodeJson(json);
                        return JavaConverters.asScalaBuffer(exploded).toSeq();
                    } catch (Exception e) {
                        if (config.errorHandling == ErrorHandling.FAIL_FAST) {
                            throw new RuntimeException("Failed to explode JSON", e);
                        }
                        return JavaConverters.asScalaBuffer(
                                Collections.singletonList((String) null)
                        ).toSeq();
                    }
                },
                DataTypes.createArrayType(DataTypes.StringType)
        );

        // Apply explosion
        Dataset<Row> exploded = jsonDs
                .withColumn("exploded_json_array", explodeUdf.apply(col("value")))
                .withColumn("exploded_json", explode(col("exploded_json_array")))
                .filter(col("exploded_json").isNotNull());

        // Parse with schema
        Dataset<Row> parsed = exploded
                .select(
                        from_json(col("exploded_json"), cachedSchema.sparkSchema).as("data"),
                        col("value").as("json_string")
                );

        // Add error column for tracking
        if (config.errorHandling == ErrorHandling.QUARANTINE ||
                config.errorHandling == ErrorHandling.SKIP_MALFORMED) {
            parsed = parsed.withColumn("_error",
                    when(col("data").isNull(), lit("Failed to parse exploded JSON"))
                            .otherwise(lit(null).cast(DataTypes.StringType))
            );
        }

        // Explode the data struct and select final columns
        List<Column> finalCols = new ArrayList<>();
        finalCols.add(col("data.*"));

        if (config.includeRawJson) {
            finalCols.add(col("json_string").as("_raw_json"));
        }

        if (Arrays.asList(parsed.columns()).contains("_error")) {
            finalCols.add(col("_error"));
        }

        Dataset<Row> finalDf = parsed.select(finalCols.toArray(new Column[0]));

        // Add explosion index if present
        if (config.explosionPaths.size() > 0) {
            String firstPath = config.explosionPaths.iterator().next();
            String explosionIndexCol = firstPath.replace(".", "_") + "_explosion_index";
            if (Arrays.asList(finalDf.columns()).contains(explosionIndexCol)) {
                finalDf = finalDf.withColumn("_explosion_index", col(explosionIndexCol))
                        .drop(explosionIndexCol);
            }
        }

        return finalDf;
    }
    private void initializeProcessors() {
        if (flattener == null) {
            flattener = new JsonFlattenerConsolidator(
                    config.arrayDelimiter,
                    config.nullPlaceholder,
                    config.maxNestingDepth,
                    config.maxArraySize,
                    config.consolidateWithMatrixDenotors,
                    config.includeArrayStatistics
            );
        }

        if (schemaFlattener == null) {
            schemaFlattener = new AvroSchemaFlattener(config.includeArrayStatistics);
        }
    }
    private CachedSchema loadSchema() throws IOException {
        // Check if we have a schema configured
        if (config.avroSchema == null && config.schemaPath == null) {
            throw new IllegalStateException("No schema configured. Use withSchema() to set a schema.");
        }

        // Generate cache key
        String cacheKey = config.avroSchema != null ?
                config.avroSchema.getFullName() + ":" + config.avroSchema.hashCode() :
                config.schemaPath + ":" + config.includeArrayStatistics;

        // Check cache if enabled
        if (config.cacheSchemas && SCHEMA_CACHE.containsKey(cacheKey)) {
            LOG.debug("Using cached schema for: {}", cacheKey);
            return SCHEMA_CACHE.get(cacheKey);
        }

        // Load schema
        Schema originalSchema;
        if (config.avroSchema != null) {
            originalSchema = config.avroSchema;
        } else {
            LOG.info("Loading schema from: {}", config.schemaPath);
            String schemaJson = FileFinder.Util.readAsString(config.schemaPath);
            originalSchema = new Schema.Parser().parse(schemaJson);
        }

        // Flatten schema
        initializeProcessors();
        Schema flattenedSchema = schemaFlattener.getFlattenedSchema(originalSchema);

        // Convert to Spark schema
        StructType sparkSchema = CreateSparkStructFromAvroSchema
                .convertNestedAvroSchemaToSparkSchema(flattenedSchema);

        // Get array fields
        Set<String> arrayFields = schemaFlattener.getArrayFieldNames();

        // Create cached schema
        CachedSchema cached = new CachedSchema(originalSchema, flattenedSchema, sparkSchema, arrayFields);

        // Cache if enabled
        if (config.cacheSchemas) {
            SCHEMA_CACHE.put(cacheKey, cached);
            LOG.info("Cached schema: {} with {} fields", cacheKey, sparkSchema.fields().length);
        }

        return cached;
    }
    private Dataset<Row> addMetadataColumns(Dataset<Row> df) {
        df = df.withColumn("_processing_time", current_timestamp());

        if (mode == PipelineMode.BATCH) {
            df = df.withColumn("_input_file", input_file_name());
        }

        return df;
    }
    // In NexusPiercerSparkPipeline.java

    // Note the new signature. It only needs the combined dataset before splitting.
    private void collectMetrics(Dataset<Row> allProcessedRecords, ProcessingMetrics metrics) {
        if (allProcessedRecords != null && !allProcessedRecords.isStreaming()) {
            // This is a more robust way to calculate metrics based on the error column.
            long total = allProcessedRecords.count();
            long malformed = allProcessedRecords.filter(col("_error").equalTo("Malformed JSON string")).count();
            long schemaFailures = allProcessedRecords.filter(col("_error").equalTo("Schema validation failed")).count();

            metrics.totalRecords = total;
            metrics.malformedRecords = malformed;
            metrics.schemaValidationFailures = schemaFailures;

            // Successful records are those without any error flag.
            metrics.successfulRecords = allProcessedRecords.filter(col("_error").isNull()).count();

            // Check for explosion index to count exploded records
            if (Arrays.asList(allProcessedRecords.columns()).contains("_explosion_index")) {
                metrics.explosionRecords = allProcessedRecords.select("_explosion_index").distinct().count();
            }
        }
    }
    public static void clearSchemaCache() {
        SCHEMA_CACHE.clear();
        LOG.info("Schema cache cleared");
    }

    public static int getSchemaCacheSize() {
        return SCHEMA_CACHE.size();
    }

    public void validateConfiguration() {
        if (config.avroSchema == null && config.schemaPath == null) {
            throw new IllegalStateException("No schema configured. Use withSchema() to set a schema.");
        }

        if (config.maxNestingDepth < 1) {
            throw new IllegalArgumentException("Max nesting depth must be at least 1");
        }

        if (config.maxArraySize < 1) {
            throw new IllegalArgumentException("Max array size must be at least 1");
        }

        if (config.arrayDelimiter == null || config.arrayDelimiter.isEmpty()) {
            throw new IllegalArgumentException("Array delimiter cannot be null or empty");
        }
    }
    public PipelineConfig getConfig() {
        return config;
    }
}