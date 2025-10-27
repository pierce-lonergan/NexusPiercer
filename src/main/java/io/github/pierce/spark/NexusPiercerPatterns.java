package io.github.pierce.spark;

import io.github.pierce.JsonFlattenerConsolidator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;

import java.util.*;

import static io.github.pierce.spark.NexusPiercerFunctions.*;
import static org.apache.spark.sql.functions.*;

/**
 * NexusPiercerPatterns - Common pipeline patterns and recipes.
 *
 * This class provides pre-configured pipelines for common use cases, making it
 * even easier to get started with JSON processing.
 *
 * Example usage:
 * <pre>
 *
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

    /**
     * JSON data quality report
     */
    public static Dataset<Row> generateDataQualityReport(
            SparkSession spark, String schemaPath, String inputPath) {



        Dataset<Row> rawData = spark.read()
                .textFile(inputPath)
                .selectExpr("value as json");


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


        NexusPiercerSparkPipeline.ProcessingResult schemaResult =
                NexusPiercerSparkPipeline.forBatch(spark)
                        .withSchema(schemaPath)
                        .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.QUARANTINE)
                        .enableMetrics()
                        .process(inputPath);


        long totalRecords = schemaResult.getMetrics().getTotalRecords();
        long successfulRecords = schemaResult.getMetrics().getSuccessfulRecords();
        double successRate = schemaResult.getMetrics().getSuccessRate();

        qualityReport = qualityReport
                .withColumn("total_records", lit(totalRecords))
                .withColumn("schema_valid_records", lit(successfulRecords))
                .withColumn("schema_success_rate", lit(successRate));

        return qualityReport;
    }



    public static Dataset<Row> profileJsonStructure(
            SparkSession spark, String inputPath, int sampleSize) {

        Dataset<String> sample = spark.read()
                .textFile(inputPath)
                .limit(sampleSize)
                .as(org.apache.spark.sql.Encoders.STRING());


        Dataset<String> validSample = sample.filter(isValid(col("value")));


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

                .selectExpr("explode(fields) as (key, value)");


        Set<String> statFields = new HashSet<>(
                flattened.filter(col("key").rlike(".*_count$|.*_type$|.*_distinct_count$|.*_min_length$|.*_max_length$|.*_avg_length$"))
                        .select("key").as(org.apache.spark.sql.Encoders.STRING()).collectAsList()
        );


        Set<String> arrayBaseFields = new HashSet<>();
        for (String statField : statFields) {
            arrayBaseFields.add(statField.replaceAll("_(count|type|distinct_count|min_length|max_length|avg_length)$", ""));
        }


        Dataset<Row> profiled = flattened
                .groupBy("key")
                .agg(
                        count("*").as("occurrences"),
                        countDistinct("value").as("distinct_values"),
                        first("value").as("sample_value")
                );


        return profiled
                .withColumn("field_type",
                        when(col("key").rlike(".*_count$"), "array_count")
                                .when(col("key").rlike(".*_type$"), "array_type")
                                .when(col("key").rlike(".*_distinct_count$|.*_min_length$|.*_max_length$|.*_avg_length$"), "array_stat")
                                .otherwise("field")
                )
                .withColumn("likely_array",

                        col("key").isin(arrayBaseFields.toArray())
                )
                .withColumnRenamed("key", "field")
                .orderBy("field");
    }

}