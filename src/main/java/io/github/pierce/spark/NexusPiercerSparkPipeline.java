package io.github.pierce.spark;

import io.github.pierce.AvroSchemaFlattener;
import io.github.pierce.CreateSparkStructFromAvroSchema;
import io.github.pierce.JsonFlattenerConsolidator;
import io.github.pierce.files.FileFinder;
import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;


public class NexusPiercerSparkPipeline implements Serializable {


    private static final Logger LOG = LoggerFactory.getLogger(NexusPiercerSparkPipeline.class);
    private static final long serialVersionUID = 1L;


    private static final Map<String, CachedSchema> SCHEMA_CACHE = new ConcurrentHashMap<>();

    private final SparkSession spark;
    private final PipelineMode mode;
    private final PipelineConfig config;


    private JsonFlattenerConsolidator flattener;
    private AvroSchemaFlattener schemaFlattener;
    private UserDefinedFunction flattenUdf;

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
        FAIL_FAST,
        SKIP_MALFORMED,
        QUARANTINE,
        PERMISSIVE
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

    private static class CachedSchema implements Serializable {
        private static final long serialVersionUID = 1L;
        final Schema originalSchema;
        final Schema flattenedSchema;
        final StructType sparkSchema;
        final Set<String> arrayFields;
        final Set<String> terminalArrayFields;    // NEW: Arrays containing primitives (keep these)
        final Set<String> nonTerminalArrayFields; // NEW: Arrays containing records (drop these)
        final Set<String> mapFieldPaths;  // ADD THIS

        final long cachedAt;

        CachedSchema(Schema original, Schema flattened, StructType spark, Set<String> arrays,
                     Set<String> terminalArrays, Set<String> nonTerminalArrays,  Set<String> mapPaths) {
            this.originalSchema = original;
            this.flattenedSchema = flattened;
            this.sparkSchema = spark;
            this.arrayFields = arrays;
            this.terminalArrayFields = terminalArrays;
            this.nonTerminalArrayFields = nonTerminalArrays;
            this.mapFieldPaths = mapPaths;  // ADD THIS
            this.cachedAt = System.currentTimeMillis();
        }
    }

    /**
     * Pipeline configuration
     */
    public static class PipelineConfig implements Serializable {
        private static final long serialVersionUID = 1L;

        private String schemaPath;
        private Schema avroSchema;
        private boolean includeArrayStatistics = true;
        private boolean quarantineSchemaErrors = true;
        private boolean includeNonTerminalArrays = false;

        private String arrayDelimiter = ",";
        private String nullPlaceholder = null;
        private int maxNestingDepth = 50;
        private int maxArraySize = 1000;
        private boolean consolidateWithMatrixDenotors = false;

        private final Set<String> explosionPaths = new HashSet<>();

        private ErrorHandling errorHandling = ErrorHandling.SKIP_MALFORMED;
        private boolean enableMetrics = true;
        private boolean cacheSchemas = true;
        private int repartitionCount = -1;

        private boolean preserveDots = false;

        private boolean includeMetadata = false;
        private boolean includeRawJson = false;
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


    public NexusPiercerSparkPipeline withSchema(String schemaPath) {
        this.config.schemaPath = schemaPath;
        this.config.avroSchema = null;
        return this;
    }

    public NexusPiercerSparkPipeline withSchema(Schema schema) {
        this.config.avroSchema = schema;
        this.config.schemaPath = null;
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

    // NEW: Configuration method for terminal arrays only
    public NexusPiercerSparkPipeline includeNonTerminalArrays() {
        this.config.includeNonTerminalArrays = true;
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



            String originalColumn = jsonDataset.columns()[0];
            Dataset<String> standardizedDataset = jsonDataset.withColumnRenamed(originalColumn, "value").as(Encoders.STRING());


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


        Dataset<String> jsonDs = spark.readStream()
                .format(source)
                .options(options)
                .load()
                .selectExpr("CAST(value AS STRING) as value")
                .filter(col("value").isNotNull())
                .as(Encoders.STRING());


        return this.processDataset(jsonDs);
    }



    /**
     * Core processing logic for both batch and streaming (private helper)
     */
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


        Column malformedSyntaxError = col("_error").equalTo("Malformed JSON string");
        Column schemaValidationError = col("_error").equalTo("Schema validation failed");


        if (config.errorHandling == ErrorHandling.QUARANTINE) {

            errorDataset = allProcessedRecords.filter(col("_error").isNotNull());

            if (config.quarantineSchemaErrors) {

                successDataset = allProcessedRecords.filter(col("_error").isNull());
            } else {


                successDataset = allProcessedRecords.filter(col("_error").isNull().or(schemaValidationError));
            }

        } else if (config.errorHandling == ErrorHandling.SKIP_MALFORMED) {

            successDataset = allProcessedRecords.filter(col("_error").isNull());
            errorDataset = null;
        } else {

            successDataset = allProcessedRecords;
        }



        if (Arrays.asList(successDataset.columns()).contains("_error")) {
            successDataset = successDataset.drop("_error");
        }

        if (config.includeMetadata) {
            successDataset = addMetadataColumns(successDataset);
        }


        if (config.enableMetrics && mode == PipelineMode.BATCH) {
            collectMetrics(allProcessedRecords, metrics);
        }

        metrics.processingTimeMs = System.currentTimeMillis() - startTime;
        allProcessedRecords.unpersist();

        return new ProcessingResult(successDataset, errorDataset, metrics);
    }



    private Dataset<Row> processFlattenedMode(Dataset<String> jsonDs, CachedSchema cachedSchema) {

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


        Dataset<Row> syntaxChecked = jsonDs
                .withColumn("is_malformed_syntax",
                        from_json(col("value"), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)).isNull()
                );


        Dataset<Row> flattened = syntaxChecked
                .withColumn("flattened_json",
                        when(not(col("is_malformed_syntax")), flattenUdf.apply(col("value")))
                                .otherwise(null)
                );


        Dataset<Row> parsed = flattened
                .withColumn("data", from_json(col("flattened_json"), cachedSchema.sparkSchema));



        if (config.errorHandling == ErrorHandling.QUARANTINE ||
                config.errorHandling == ErrorHandling.SKIP_MALFORMED) {


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


            parsed = parsed.withColumn("_error",
                    when(col("is_malformed_syntax"), lit("Malformed JSON string"))
                            .when(schemaErrorCondition, lit("Schema validation failed"))
                            .otherwise(lit(null).cast(DataTypes.StringType))
            );
        }


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


    public ProcessingResult processJsonColumn(Dataset<Row> sourceDf, String jsonColumnName) {
        if (!Arrays.asList(sourceDf.columns()).contains(jsonColumnName)) {
            throw new IllegalArgumentException("Source DataFrame does not contain specified JSON column: " + jsonColumnName);
        }

        long startTime = System.currentTimeMillis();
        ProcessingMetrics metrics = new ProcessingMetrics();
        Broadcast<StructType> broadcastSchema = null;

        try {
            final CachedSchema cachedSchema = loadSchema();
            initializeProcessors();
            broadcastSchema = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(cachedSchema.sparkSchema);

            // --- 1. Define the combined UDF that returns a Struct ---
            StructType combinedUdfReturnType = new StructType()
                    .add("flattened_json", DataTypes.StringType, true)
                    .add("schema_error", DataTypes.BooleanType, false);

            final JsonFlattenerConsolidator flattener = this.flattener;
            Broadcast<StructType> finalBroadcastSchema = broadcastSchema;

            UserDefinedFunction flattenAndValidateUdf = udf(
                    (String rawJson) -> {
                        if (rawJson == null || rawJson.trim().isEmpty()) {
                            return RowFactory.create(null, false);
                        }

                        try {
                            String flattenedJson = flattener.flattenAndConsolidateJson(rawJson);
                            boolean schemaError = false;

                            StructType sparkSchema = finalBroadcastSchema.getValue();
                            JSONObject jsonObject = new JSONObject(flattenedJson);

                            for (StructField field : sparkSchema.fields()) {
                                String fieldName = field.name();
                                if (jsonObject.has(fieldName) && !jsonObject.isNull(fieldName)) {
                                    Object value = jsonObject.get(fieldName);
                                    DataType dataType = field.dataType();
                                    if (!isTypeCompatible(value, dataType)) {
                                        schemaError = true;
                                        break;
                                    }
                                }
                            }
                            return RowFactory.create(flattenedJson, schemaError);
                        } catch (Exception e) {
                            return RowFactory.create("{}", true);
                        }
                    },
                    combinedUdfReturnType
            );

            // --- 2. Apply the single, combined UDF ---
            Dataset<Row> processedDf = sourceDf
                    .withColumn("_is_malformed_syntax", from_json(col(jsonColumnName), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)).isNull())
                    .withColumn("_processing_output", when(not(col("_is_malformed_syntax")), flattenAndValidateUdf.apply(col(jsonColumnName))));

            // --- 3. Create the final error and data columns from the UDF's output struct ---
            processedDf = processedDf
                    .withColumn("_error",
                            when(col("_is_malformed_syntax"), lit("Malformed JSON string"))
                                    .when(col("_processing_output.schema_error"), lit("Schema validation failed"))
                                    .otherwise(lit(null).cast(DataTypes.StringType))
                    )
                    .withColumn("_parsed_data", from_json(col("_processing_output.flattened_json"), cachedSchema.sparkSchema));

            // --- 4. Split into Success and Error DataFrames ---
            Dataset<Row> errorDf = processedDf.filter(col("_error").isNotNull());
            Dataset<Row> successDf = processedDf.filter(col("_error").isNull());

            // --- 5. FIXED: Finalize the Success DataFrame schema - Only drop NON-TERMINAL arrays ---
            List<String> headerNames = Arrays.stream(sourceDf.columns()).filter(c -> !c.equals(jsonColumnName)).toList();
            List<Column> finalSuccessCols = headerNames.stream().map(functions::col).collect(Collectors.toList());

            // CRITICAL FIX: Only drop non-terminal arrays, keep terminal arrays
            Set<String> nonTerminalArraysToDrop = cachedSchema.nonTerminalArrayFields;
            LOG.info("Dropping non-terminal arrays: {}", nonTerminalArraysToDrop);
            LOG.info("Keeping terminal arrays: {}", cachedSchema.terminalArrayFields);

            List<Column> flattenedCols = Arrays.stream(cachedSchema.sparkSchema.fields()).map(StructField::name)
                    .filter(name -> !nonTerminalArraysToDrop.contains(name)) // Only filter out non-terminal arrays
                    .map(name -> col("_parsed_data." + name).as(name))
                    .toList();

            finalSuccessCols.addAll(flattenedCols);
            if (config.includeRawJson) {
                finalSuccessCols.add(col(jsonColumnName).as("_raw_json"));
            }
            successDf = successDf.select(finalSuccessCols.toArray(new Column[0]));

            // --- 6. Finalize the Error DataFrame schema ---
            List<Column> finalErrorCols = headerNames.stream().map(functions::col).collect(Collectors.toList());
            if (config.includeRawJson) {
                finalErrorCols.add(col(jsonColumnName).as("_raw_json"));
            }
            finalErrorCols.add(col("_error"));
            errorDf = errorDf.select(finalErrorCols.toArray(new Column[0]));

            // --- 7. Collect Metrics and Return ---
            if (config.enableMetrics && mode == PipelineMode.BATCH) {
                processedDf.cache();
                collectMetrics(processedDf, metrics);
                processedDf.unpersist();
            }
            metrics.processingTimeMs = System.currentTimeMillis() - startTime;

            return new ProcessingResult(successDf, errorDf, metrics);

        } catch (Exception e) {
            LOG.error("Failed to process JSON column", e);
            throw new RuntimeException("Pipeline processing failed for JSON column", e);
        }
    }
    /**
     * A private helper method to check if a Java object from a JSON parser is compatible
     * with a target Spark DataType. This mimics the type coercion behavior of `from_json`.
     */
    private static boolean isTypeCompatible(Object value, DataType dataType) {
        if (value == null) return true;
        if (dataType instanceof org.apache.spark.sql.types.StringType) return true;

        if (dataType instanceof org.apache.spark.sql.types.NumericType) {
            if (value instanceof Number) return true;
            try {
                Double.parseDouble(value.toString());
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        if (dataType instanceof org.apache.spark.sql.types.BooleanType) {
            if (value instanceof Boolean) return true;
            String lowerValue = value.toString().toLowerCase();
            return "true".equals(lowerValue) || "false".equals(lowerValue);
        }

        return true;
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
    /**
     * UPDATED: Initialize processors with new configuration
     */
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
            // UPDATED: Initialize with both array statistics and non-terminal array configuration
            schemaFlattener = new AvroSchemaFlattener(config.includeArrayStatistics, config.includeNonTerminalArrays);
        }

        if (flattenUdf == null) {
            flattenUdf = udf(
                    (String json) -> {
                        if (json == null || json.trim().isEmpty()) return null;
                        try {
                            return flattener.flattenAndConsolidateJson(json);
                        } catch (Exception e) {
                            return "{}";
                        }
                    },
                    DataTypes.StringType
            );
        }
    }
    /**
     * UPDATED: Load schema with terminal/non-terminal array distinction
     */
    private CachedSchema loadSchema() throws IOException {
        if (config.avroSchema == null && config.schemaPath == null) {
            throw new IllegalStateException("No schema configured. Use withSchema() to set a schema.");
        }

        // Generate cache key including new configuration
        String cacheKey = config.avroSchema != null ?
                config.avroSchema.getFullName() + ":" + config.avroSchema.hashCode() + ":" + config.includeArrayStatistics + ":" + config.includeNonTerminalArrays :
                config.schemaPath + ":" + config.includeArrayStatistics + ":" + config.includeNonTerminalArrays;

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

        // UPDATED: Initialize processors with new configuration
        initializeProcessors();
        Schema flattenedSchema = schemaFlattener.getFlattenedSchema(originalSchema);

        // Convert to Spark schema
        StructType sparkSchema = CreateSparkStructFromAvroSchema
                .convertNestedAvroSchemaToSparkSchema(flattenedSchema);

        // UPDATED: Get both terminal and non-terminal array fields
        Set<String> allArrayFields = schemaFlattener.getArrayFieldNames();
        Set<String> terminalArrayFields = schemaFlattener.getTerminalArrayFieldNames();
        Set<String> nonTerminalArrayFields = schemaFlattener.getNonTerminalArrayFieldNames();
        Set<String> mapFieldPaths = schemaFlattener.getMapFieldPaths();

        LOG.info("Schema analysis - Total arrays: {}, Terminal: {}, Non-terminal: {}",
                allArrayFields.size(), terminalArrayFields.size(), nonTerminalArrayFields.size());
        LOG.debug("Terminal arrays: {}", terminalArrayFields);
        LOG.debug("Non-terminal arrays: {}", nonTerminalArrayFields);

        // UPDATED: Create cached schema with terminal/non-terminal distinction
        CachedSchema cached = new CachedSchema(originalSchema, flattenedSchema, sparkSchema,
                allArrayFields, terminalArrayFields, nonTerminalArrayFields, mapFieldPaths);

        if (config.cacheSchemas) {
            SCHEMA_CACHE.put(cacheKey, cached);
            LOG.info("Cached schema: {} with {} fields ({} terminal arrays, {} non-terminal arrays)",
                    cacheKey, sparkSchema.fields().length, terminalArrayFields.size(), nonTerminalArrayFields.size());
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

    /**
     * Collects processing metrics in a single pass over the data for efficiency.
     *
     * @param allProcessedRecords The DataFrame containing the _error column before splitting.
     * @param metrics The metrics object to populate.
     */
    private void collectMetrics(Dataset<Row> allProcessedRecords, ProcessingMetrics metrics) {
        if (allProcessedRecords == null || allProcessedRecords.isStreaming()) {
            return;
        }

        // OPTIMIZATION: Calculate all counts in a single pass using groupBy.
        // This is vastly more efficient than multiple .count() actions.
        Dataset<Row> countsByError = allProcessedRecords
                .withColumn("_error_type",
                        when(col("_error").isNull(), lit("success"))
                                .otherwise(col("_error"))
                )
                .groupBy("_error_type")
                .count();

        // Collect the small result set to the driver
        Map<String, Long> errorCounts = new HashMap<>();
        for (Row r : countsByError.collectAsList()) {
            errorCounts.put(r.getString(0), r.getLong(1));
        }

        metrics.successfulRecords = errorCounts.getOrDefault("success", 0L);
        metrics.malformedRecords = errorCounts.getOrDefault("Malformed JSON string", 0L);
        metrics.schemaValidationFailures = errorCounts.getOrDefault("Schema validation failed", 0L);

        // Summing up the parts is safer and avoids another count.
        metrics.totalRecords = metrics.successfulRecords + metrics.malformedRecords + metrics.schemaValidationFailures;

        if (Arrays.asList(allProcessedRecords.columns()).contains("_explosion_index")) {
            // This count is on a smaller, distinct set of a single column, so it's less expensive.
            metrics.explosionRecords = allProcessedRecords.select("_explosion_index").distinct().count();
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