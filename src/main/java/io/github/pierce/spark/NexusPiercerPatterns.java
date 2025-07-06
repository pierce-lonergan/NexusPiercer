package io.github.pierce.spark;

import io.github.pierce.JsonFlattenerConsolidator;
import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.github.pierce.spark.NexusPiercerFunctions.*;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * NexusPiercerPatterns - Common pipeline patterns and recipes.
 *
 * This class provides pre-configured pipelines for common use cases, making it
 * even easier to get started with JSON processing.
 *
 * Example usage:
 * <pre>
 * // ETL from JSON files to Delta Lake
 * NexusPiercerPatterns.jsonToDelta(spark,
 *     "product_schema.avsc",
 *     "s3://bucket/input/products/*.json",
 *     "s3://bucket/delta/products"
 * );
 *
 * // Streaming from Kafka to Parquet with checkpointing
 * StreamingQuery query = NexusPiercerPatterns.kafkaToParquetStream(spark,
 *     "events_schema.avsc",
 *     "kafka-broker:9092",
 *     "events-topic",
 *     "/output/events",
 *     "/checkpoints/events"
 * );
 * </pre>
 */
public class NexusPiercerPatterns {

    // ===== BATCH PATTERNS =====

    /**
     * JSON files to Parquet with schema validation
     */
    public static void jsonToParquet(SparkSession spark, String schemaPath,
                                     String inputPath, String outputPath) {
        jsonToParquet(spark, schemaPath, inputPath, outputPath, SaveMode.Overwrite, null);
    }

    public static void jsonToParquet(SparkSession spark, String schemaPath,
                                     String inputPath, String outputPath,
                                     SaveMode saveMode, String partitionBy) {
        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(schemaPath)
                .enableArrayStatistics()
                .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.QUARANTINE)
                .includeMetadata()
                .process(inputPath);

        if (partitionBy != null) {
            result.getDataset().write().mode(saveMode).partitionBy(partitionBy).parquet(outputPath);
        } else {
            result.getDataset().write().mode(saveMode).parquet(outputPath);
        }

        if (result.getErrorDataset() != null && result.getErrorDataset().count() > 0) {
            result.getErrorDataset().write().mode(saveMode).parquet(outputPath + "_errors");
        }

        System.out.println("Processing completed: " + result.getMetrics());
    }

    /**
     * JSON files to Delta Lake
     */
    public static void jsonToDelta(SparkSession spark, String schemaPath,
                                   String inputPath, String deltaPath) {
        jsonToDelta(spark, schemaPath, inputPath, deltaPath, null, false);
    }

    public static void jsonToDelta(SparkSession spark, String schemaPath,
                                   String inputPath, String deltaPath,
                                   String mergeKey, boolean enableMerge) {
        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(schemaPath)
                .enableArrayStatistics()
                .includeMetadata()
                .process(inputPath);

        if (enableMerge && mergeKey != null && result.getDataset().count() > 0) {
            result.getDataset().createOrReplaceTempView("updates");
            spark.sql(String.format("""
                MERGE INTO delta.`%s` target
                USING updates source
                ON target.%s = source.%s
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """, deltaPath, mergeKey, mergeKey));
        } else {
            result.getDataset().write().format("delta").mode(enableMerge ? SaveMode.Append : SaveMode.Overwrite).save(deltaPath);
        }
    }

    // In NexusPiercerPatterns.java

    public static Map<String, Dataset<Row>> jsonToNormalizedTables(
            SparkSession spark, String schemaPath, String inputPath,
            String primaryArrayPath, String... secondaryArrayPaths) {

        Map<String, Dataset<Row>> tables = new HashMap<>();

        // Process the data ONCE for the main table.
        // The key is to use the regular pipeline which flattens everything.
        Dataset<Row> initialData = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(schemaPath)
                .process(inputPath)
                .getDataset();
        initialData.cache(); // Cache for reuse

        // --- FIX: Robustly drop all columns related to the arrays ---
        List<String> columnsToDrop = new ArrayList<>();
        List<String> allArrayPaths = new ArrayList<>();
        allArrayPaths.add(primaryArrayPath);
        allArrayPaths.addAll(Arrays.asList(secondaryArrayPaths));

        for (String arrayPath : allArrayPaths) {
            // The simple path name (e.g., "items") might exist as a complex column.
            String simpleName = arrayPath.contains(".") ? arrayPath.substring(arrayPath.lastIndexOf('.') + 1) : arrayPath;
            // The flattened prefix (e.g., "items_")
            String prefix = arrayPath.replace(".", "_") + "_";

            // Add both the simple name and any prefixed columns to the drop list.
            columnsToDrop.add(simpleName);
            for (String colName : initialData.columns()) {
                if (colName.startsWith(prefix)) {
                    columnsToDrop.add(colName);
                }
            }
        }

        Dataset<Row> mainTable = initialData.drop(columnsToDrop.toArray(new String[0]));
        tables.put("main", mainTable);
        initialData.unpersist(); // Release cache

        // For explosion, re-process the raw data with the explode config.
        // This part of your logic is correct.
        NexusPiercerSparkPipeline.ProcessingResult primaryResult =
                NexusPiercerSparkPipeline.forBatch(spark)
                        .withSchema(schemaPath)
                        .explodeArrays(primaryArrayPath)
                        .process(inputPath);
        tables.put(primaryArrayPath.replace(".", "_"), primaryResult.getDataset());

        for (String arrayPath : secondaryArrayPaths) {
            NexusPiercerSparkPipeline.ProcessingResult arrayResult =
                    NexusPiercerSparkPipeline.forBatch(spark)
                            .withSchema(schemaPath)
                            .explodeArrays(arrayPath)
                            .process(inputPath);
            tables.put(arrayPath.replace(".", "_"), arrayResult.getDataset());
        }
        return tables;
    }

    /**
     * JSON data quality report
     */
    public static Dataset<Row> generateDataQualityReport(
            SparkSession spark, String schemaPath, String inputPath) {


        // Read raw JSON
        Dataset<Row> rawData = spark.read()
                .textFile(inputPath)
                .selectExpr("value as json");

        // Apply validation and analysis
        Dataset<Row> qualityReport = rawData
                .withColumn("is_valid", isValid(col("json")))
                .withColumn("error_message", jsonError(col("json")))
                .withColumn("json_length", length(col("json")))
                .withColumn("is_empty", col("json").equalTo("{}"))
                .groupBy("is_valid", "is_empty")
                .agg(
                        count("*").as("record_count"),
                        avg("json_length").as("avg_json_size"),
                        max("json_length").as("max_json_size"),
                        min("json_length").as("min_json_size"),
                        collect_set("error_message").as("unique_errors")
                );

        // Try to parse with schema
        NexusPiercerSparkPipeline.ProcessingResult schemaResult =
                NexusPiercerSparkPipeline.forBatch(spark)
                        .withSchema(schemaPath)
                        .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.QUARANTINE)
                        .enableMetrics()
                        .process(inputPath);

        // Add schema validation metrics
        long totalRecords = schemaResult.getMetrics().getTotalRecords();
        long successfulRecords = schemaResult.getMetrics().getSuccessfulRecords();
        double successRate = schemaResult.getMetrics().getSuccessRate();

        qualityReport = qualityReport
                .withColumn("total_records", lit(totalRecords))
                .withColumn("schema_valid_records", lit(successfulRecords))
                .withColumn("schema_success_rate", lit(successRate));

        return qualityReport;
    }

    // ===== STREAMING PATTERNS =====

    /**
     * Kafka to Parquet streaming pipeline
     */
    public static StreamingQuery kafkaToParquetStream(
            SparkSession spark, String schemaPath,
            String kafkaBootstrapServers, String topic,
            String outputPath, String checkpointPath) throws TimeoutException {

        Map<String, String> kafkaOptions = new HashMap<>();
        kafkaOptions.put("kafka.bootstrap.servers", kafkaBootstrapServers);
        kafkaOptions.put("subscribe", topic);
        kafkaOptions.put("startingOffsets", "latest");

        return kafkaToParquetStream(spark, schemaPath, kafkaOptions,
                outputPath, checkpointPath,
                "1 minute", null);
    }

    public static StreamingQuery kafkaToParquetStream(
            SparkSession spark, String schemaPath,
            Map<String, String> kafkaOptions,
            String outputPath, String checkpointPath,
            String triggerInterval, String partitionBy) throws TimeoutException {

        NexusPiercerSparkPipeline.ProcessingResult result =
                NexusPiercerSparkPipeline.forStreaming(spark)
                        .withSchema(schemaPath)
                        .enableArrayStatistics()
                        .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.SKIP_MALFORMED)
                        .includeMetadata()
                        .processStream("kafka", kafkaOptions);

        return result.writeStream()
                .outputMode(OutputMode.Append())
                .format("parquet")
                .option("path", outputPath)
                .option("checkpointLocation", checkpointPath)
                .partitionBy(partitionBy != null ? partitionBy : "_processing_time")
                .trigger(Trigger.ProcessingTime(triggerInterval))
                .start();
    }

    /**
     * Kafka to Delta streaming with merge
     */
    public static StreamingQuery kafkaToDeltaStream(
            SparkSession spark, String schemaPath,
            String kafkaBootstrapServers, String topic,
            String deltaPath, String checkpointPath,
            String mergeKey) throws TimeoutException {

        Map<String, String> kafkaOptions = new HashMap<>();
        kafkaOptions.put("kafka.bootstrap.servers", kafkaBootstrapServers);
        kafkaOptions.put("subscribe", topic);
        kafkaOptions.put("startingOffsets", "latest");

        NexusPiercerSparkPipeline.ProcessingResult result =
                NexusPiercerSparkPipeline.forStreaming(spark)
                        .withSchema(schemaPath)
                        .enableArrayStatistics()
                        .includeMetadata()
                        .processStream("kafka", kafkaOptions);

        // Define merge logic in foreachBatch
        return result.writeStream()
                .foreachBatch((Dataset<Row> batchDF, Long batchId) -> {
                    batchDF.createOrReplaceTempView("updates");

                    spark.sql(String.format("""
                    MERGE INTO delta.`%s` target
                    USING updates source
                    ON target.%s = source.%s
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                    """, deltaPath, mergeKey, mergeKey));
                })
                .outputMode(OutputMode.Update())
                .option("checkpointLocation", checkpointPath)
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();
    }

    /**
     * Multi-stream join pattern
     */
    public static StreamingQuery joinedStreamsToSink(
            SparkSession spark,
            String leftSchemaPath, Map<String, String> leftStreamOptions,
            String rightSchemaPath, Map<String, String> rightStreamOptions,
            String joinKey, String joinType,
            String outputFormat, Map<String, String> outputOptions,
            String checkpointPath) throws TimeoutException {

        // Process left stream
        Dataset<Row> leftStream = NexusPiercerSparkPipeline.forStreaming(spark)
                .withSchema(leftSchemaPath)
                .processStream("kafka", leftStreamOptions)
                .getDataset()
                .withWatermark("_processing_time", "5 minutes");

        // Process right stream
        Dataset<Row> rightStream = NexusPiercerSparkPipeline.forStreaming(spark)
                .withSchema(rightSchemaPath)
                .processStream("kafka", rightStreamOptions)
                .getDataset()
                .withWatermark("_processing_time", "5 minutes");

        // Join streams
        Dataset<Row> joined = leftStream
                .join(rightStream,
                        leftStream.col(joinKey).equalTo(rightStream.col(joinKey)),
                        joinType)
                .drop(rightStream.col(joinKey)); // Remove duplicate join key

        // Write to sink
        return joined.writeStream()
                .outputMode(OutputMode.Append())
                .format(outputFormat)
                .options(outputOptions)
                .option("checkpointLocation", checkpointPath)
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .start();
    }

    // ===== UTILITY PATTERNS =====

    /**
     * Schema evolution check
     */
    // In NexusPiercerPatterns.java

    public static boolean checkSchemaCompatibility(
            SparkSession spark,
            String oldSchemaPath, String newSchemaPath,
            String sampleDataPath) {
        try {
            NexusPiercerSparkPipeline.ProcessingResult oldResult =
                    NexusPiercerSparkPipeline.forBatch(spark)
                            .withSchema(oldSchemaPath)
                            // Use allowSchemaErrors so we can count successful records accurately
                            .allowSchemaErrors()
                            .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.QUARANTINE)
                            .process(sampleDataPath);

            NexusPiercerSparkPipeline.ProcessingResult newResult =
                    NexusPiercerSparkPipeline.forBatch(spark)
                            .withSchema(newSchemaPath)
                            .allowSchemaErrors()
                            .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.QUARANTINE)
                            .process(sampleDataPath);

            // FIX: A schema is compatible if the number of successfully parsed rows
            // does not decrease. This correctly handles data type changes that cause nulls.
            long oldSuccessCount = oldResult.getDataset().count();
            long newSuccessCount = newResult.getDataset().count();

            // Adding a new optional field is compatible (counts are equal).
            // Changing a type to an incompatible one is not (newSuccessCount will be lower).
            return newSuccessCount >= oldSuccessCount;

        } catch (Exception e) {
            // Any exception during processing means incompatibility.
            return false;
        }
    }

    /**
     * Incremental processing pattern
     */
    public static void processIncremental(
            SparkSession spark, String schemaPath,
            String inputBasePath, String outputPath,
            String lastProcessedTimestamp) {

        // Build path pattern for incremental load
        String incrementalPath;
        if (lastProcessedTimestamp != null) {
            // Assuming date-partitioned input like /data/yyyy/MM/dd/
            incrementalPath = inputBasePath + "/{" +
                    buildDatePattern(lastProcessedTimestamp) + "}/*.json";
        } else {
            incrementalPath = inputBasePath + "/*/*.json";
        }

        // Process incremental data
        NexusPiercerSparkPipeline.ProcessingResult result =
                NexusPiercerSparkPipeline.forBatch(spark)
                        .withSchema(schemaPath)
                        .enableArrayStatistics()
                        .includeMetadata()
                        .process(incrementalPath);

        // Append to output
        result.write()
                .mode(SaveMode.Append)
                .partitionBy("_processing_time")
                .parquet(outputPath);

        // Return max timestamp for next run
        Row maxTimestamp = result.getDataset()
                .agg(max("_processing_time"))
                .first();

        System.out.println("Processed up to: " + maxTimestamp.get(0));
    }

    /**
     * Profile JSON data structure
     */
    // In NexusPiercerPatterns.java

    // In NexusPiercerPatterns.java

    // In NexusPiercerPatterns.java

    public static Dataset<Row> profileJsonStructure(
            SparkSession spark, String inputPath, int sampleSize) {

        Dataset<String> sample = spark.read()
                .textFile(inputPath)
                .limit(sampleSize)
                .as(org.apache.spark.sql.Encoders.STRING());

        // Filter out malformed JSON
        Dataset<String> validSample = sample.filter(isValid(col("value")));

        // Use the pipeline's consistent flattener
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                ",", null, 50, 1000, false, true
        );

        UserDefinedFunction profileFlattenerUdf = udf(
                (String json) -> {
                    if (json == null) return null;
                    return flattener.flattenAndConsolidateJson(json);
                }, DataTypes.StringType
        );

        MapType schema = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);

        Dataset<Row> flattened = validSample
                .withColumn("flattened", profileFlattenerUdf.apply(col("value")))
                .select(from_json(col("flattened"), schema).as("fields"))
                // --- FIX: Use selectExpr to correctly name the output of explode ---
                .selectExpr("explode(fields) as (key, value)");

        // Get a set of all field names that are array statistics
        Set<String> statFields = new HashSet<>(
                flattened.filter(col("key").rlike(".*_count$|.*_type$|.*_distinct_count$|.*_min_length$|.*_max_length$|.*_avg_length$"))
                        .select("key").as(org.apache.spark.sql.Encoders.STRING()).collectAsList()
        );

        // From the stat fields, derive the base array field names
        Set<String> arrayBaseFields = new HashSet<>();
        for (String statField : statFields) {
            arrayBaseFields.add(statField.replaceAll("_(count|type|distinct_count|min_length|max_length|avg_length)$", ""));
        }

        // Now do the main aggregation
        Dataset<Row> profiled = flattened
                .groupBy("key")
                .agg(
                        count("*").as("occurrences"),
                        countDistinct("value").as("distinct_values"),
                        first("value").as("sample_value")
                );

        // Join the derived information back to classify the fields
        return profiled
                .withColumn("field_type",
                        when(col("key").rlike(".*_count$"), "array_count")
                                .when(col("key").rlike(".*_type$"), "array_type")
                                .when(col("key").rlike(".*_distinct_count$|.*_min_length$|.*_max_length$|.*_avg_length$"), "array_stat")
                                .otherwise("field")
                )
                .withColumn("likely_array",
                        // A field is an array if its name is in our derived set of base names
                        col("key").isin(arrayBaseFields.toArray())
                )
                .withColumnRenamed("key", "field")
                .orderBy("field");
    }


    // ===== HELPER METHODS =====

    private static String buildDatePattern(String lastTimestamp) {
        // Parse timestamp and generate path pattern for dates after it
        // This is a simplified example - adjust based on your date format
        try {
            java.time.LocalDateTime lastDate = java.time.LocalDateTime.parse(lastTimestamp);
            java.time.LocalDateTime now = java.time.LocalDateTime.now();

            StringBuilder pattern = new StringBuilder();
            java.time.LocalDateTime current = lastDate.plusDays(1);

            while (current.isBefore(now) || current.isEqual(now)) {
                if (pattern.length() > 0) pattern.append(",");
                pattern.append(current.format(
                        java.time.format.DateTimeFormatter.ofPattern("yyyy/MM/dd")
                ));
                current = current.plusDays(1);
            }

            return pattern.toString();

        } catch (Exception e) {
            return "*/*/*/*"; // Fallback pattern
        }
    }
}