package io.github.pierce.spark.examples;


import io.github.pierce.spark.NexusPiercerFunctions;
import io.github.pierce.spark.NexusPiercerPatterns;
import io.github.pierce.spark.NexusPiercerSparkPipeline;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.github.pierce.spark.NexusPiercerFunctions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.spark.sql.functions.*;

/**
 * JUnit tests converted from the NexusPiercerExamples main class.
 *
 * This class provides a verifiable, self-contained, and automated way to run
 * the examples, ensuring the functionality of the NexusPiercer library across
 * various use cases.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NexusPiercerExamplesTest {

    private static SparkSession spark;

    @TempDir
    static Path tempDir;

    @BeforeAll
    void setupSpark() {
        spark = SparkSession.builder()
                .appName("NexusPiercer Examples Test")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "false") // Disable for predictable test plans
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();
        // Set log level to WARN to reduce console noise during tests
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    /**
     * Helper to create a file in the temporary directory.
     */
    private Path createTestFile(String path, String content) throws IOException {
        Path filePath = tempDir.resolve(path);
        Files.createDirectories(filePath.getParent());
        Files.write(filePath, content.getBytes());
        return filePath;
    }

    @Test
    @DisplayName("Example 1: Basic JSON processing with schema validation")
    void testBasicExample() throws IOException {
        // --- Setup (is correct) ---
        String schemaContent = """
            {
              "type": "record", "name": "UserActivity",
              "fields": [
                {"name": "userId", "type": "string"},
                {"name": "activityType", "type": "string"},
                {"name": "timestamp", "type": "long"}
              ]
            }
            """;
        createTestFile("schemas/user_activity.avsc", schemaContent);

        String allJson = String.join("\n",
                "{\"userId\": \"u1\", \"activityType\": \"login\", \"timestamp\": 1672531200}",
                "{\"userId\": \"u2\", \"activityType\": \"logout\", \"timestamp\": \"not-a-long\"}",
                "{not-json}"
        );
        Path dataFile = createTestFile("data/input/users/data.json", allJson);

        // --- Action ---
        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(tempDir.resolve("schemas/user_activity.avsc").toString())
                .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.QUARANTINE)
                .process(dataFile.toString());

        // --- Assertions ---
        Dataset<Row> successDs = result.getDataset();
        Dataset<Row> errorDs = result.getErrorDataset();

        System.out.println("=== Successful Records ===");
        successDs.show(false);
        System.out.println("=== Error Records ===");
        errorDs.show(false);

        // Assertions updated to match the new, correct logic
        assertThat((long) successDs.count()).isEqualTo(1L); // Only u1 is fully valid
        assertThat((long) errorDs.count()).isEqualTo(2L);   // u2 (schema error) and {not-json} (malformed) are errors

        // Verify the contents of the single successful record
        List<String> successUserIds = successDs.select("userId").as(Encoders.STRING()).collectAsList();
        assertThat(successUserIds).containsExactly("u1");

        // Verify the error messages for the two failed records
        List<String> errorMessages = errorDs.select("_error").as(Encoders.STRING()).collectAsList();
        assertThat(errorMessages).containsExactlyInAnyOrder(
                "Malformed JSON string",
                "Schema validation failed"
        );
    }

    @Test
    @DisplayName("Test lenient schema validation with duplication using allowSchemaErrors()")
    void testLenientSchemaValidationWithDuplication() throws IOException {
        // --- Setup (Same as before) ---
        String schemaContent = """
            {
              "type": "record", "name": "UserActivity",
              "fields": [
                {"name": "userId", "type": "string"},
                {"name": "activityType", "type": "string"},
                {"name": "timestamp", "type": "long"}
              ]
            }
            """;
        createTestFile("schemas/user_activity.avsc", schemaContent);

        String allJson = String.join("\n",
                "{\"userId\": \"u1\", \"activityType\": \"login\", \"timestamp\": 1672531200}",
                "{\"userId\": \"u2\", \"activityType\": \"logout\", \"timestamp\": \"not-a-long\"}",
                "{not-json}"
        );
        Path dataFile = createTestFile("data/input/users/lenient_data.json", allJson);

        // --- Action ---
        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(tempDir.resolve("schemas/user_activity.avsc").toString())
                .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.QUARANTINE)
                .allowSchemaErrors()
                .process(dataFile.toString());

        // --- Assertions for the lenient duplication case ---
        Dataset<Row> successDs = result.getDataset();
        Dataset<Row> errorDs = result.getErrorDataset();

        System.out.println("=== Lenient+Dupe Mode: Successful Records ===");
        successDs.show(false);
        System.out.println("=== Lenient+Dupe Mode: Error Records ===");
        errorDs.show(false);

        // ASSERT SUCCESS DATASET:
        // Still expect 2 records: the valid one (u1) and the one with the schema error (u2).
        assertThat(successDs.count()).isEqualTo(2L);
        assertThat(successDs.select("userId").as(Encoders.STRING()).collectAsList())
                .containsExactlyInAnyOrder("u1", "u2");

        // ASSERT ERROR DATASET:
        // NOW expect 2 records: the schema error (u2) AND the syntax error.
        assertThat(errorDs.count()).isEqualTo(2L);
        List<String> errorMessages = errorDs.select("_error").as(Encoders.STRING()).collectAsList();
        assertThat(errorMessages).containsExactlyInAnyOrder(
                "Malformed JSON string",
                "Schema validation failed"
        );
    }
    

//    @Test
//    @DisplayName("Example 2: Streaming JSON from MemoryStream")
//    void testStreamingExample() throws Exception {
//        // --- Setup ---
//        String schemaContent = """
//            {
//              "type": "record", "name": "Event",
//              "fields": [
//                {"name": "eventId", "type": "string"},
//                {"name": "event_date", "type": "string"}
//              ]
//            }
//            """;
//        createTestFile("schemas/events.avsc", schemaContent);
//
//        // FIX: Use the correct MemoryStream constructor for modern Spark versions
//        MemoryStream<String> memoryStream = new MemoryStream<>(0, spark.sqlContext(), scala.Option.apply(1), Encoders.STRING());
//        Dataset<String> streamInput = memoryStream.toDF().selectExpr("value AS json_string").as(Encoders.STRING());
//
//        // --- Action ---
//        NexusPiercerSparkPipeline pipeline = NexusPiercerSparkPipeline.forStreaming(spark)
//                .withSchema(tempDir.resolve("schemas/events.avsc").toString())
//                .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.SKIP_MALFORMED);
//
//        NexusPiercerSparkPipeline.ProcessingResult result = pipeline.processDataset(streamInput);
//
//        Path outputPath = tempDir.resolve("data/output/events_stream");
//        Path checkpointPath = tempDir.resolve("checkpoints/events");
//
//        StreamingQuery query = result.getDataset()
//                .writeStream()
//                .outputMode("append")
//                .format("parquet")
//                .option("path", outputPath.toString())
//                .option("checkpointLocation", checkpointPath.toString())
//                .partitionBy("event_date")
//                .start();
//
//        List<String> testData = Arrays.asList(
//                "{\"eventId\": \"e1\", \"event_date\": \"2024-01-01\"}",
//                "{\"eventId\": \"e2\", \"event_date\": \"2024-01-01\"}",
//                "{bad-json}"
//        );
//        Seq<String> testDataSeq = JavaConverters.asScalaBuffer(testData).toSeq();
//        memoryStream.addData(testDataSeq);
//
//        query.processAllAvailable();
//        query.stop();
//
//        // --- Assertions ---
//        Dataset<Row> outputDs = spark.read().parquet(outputPath.toString());
//        outputDs.show(false);
//
//        assertThat((long) outputDs.count()).isEqualTo(2L);
//        assertThat(outputDs.columns()).contains("eventId", "event_date");
//        List<String> eventIds = outputDs.select("eventId").as(Encoders.STRING()).collectAsList();
//        assertThat(eventIds).containsExactlyInAnyOrder("e1", "e2");
//    }

    @Test
    @DisplayName("Example 3: Array explosion for normalization")
    void testExplosionExample() throws IOException {
        // --- Setup ---
        String schemaContent = """
            {
              "type": "record", "name": "Order",
              "fields": [
                {"name": "orderId", "type": "string"},
                {
                  "name": "items", "type": {
                    "type": "array", "items": {
                      "type": "record", "name": "Item", "fields": [
                        {"name": "productId", "type": "string"}, {"name": "price", "type": "double"}
                      ]
                    }
                  }
                }
              ]
            }
            """;
        createTestFile("schemas/order.avsc", schemaContent);

        String sampleJson = """
            { "orderId": "ORD-12345", "items": [
                {"productId": "PROD-001", "price": 999.99},
                {"productId": "PROD-002", "price": 29.99}
            ]}
            """;
        Dataset<String> jsonDs = spark.createDataset(List.of(sampleJson), Encoders.STRING());

        // --- Action ---
        Dataset<Row> explodedItems = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(tempDir.resolve("schemas/order.avsc").toString())
                .explodeArrays("items")
                .processDataset(jsonDs)
                .getDataset();

        Dataset<Row> ordersMain = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(tempDir.resolve("schemas/order.avsc").toString())
                .processDataset(jsonDs)
                .getDataset()
                .drop("items_productId", "items_price");

        // --- Assertions ---
        assertThat((long) explodedItems.count()).isEqualTo(2L);
        assertThat(explodedItems.columns()).contains("orderId", "items_productId", "items_price");
        assertThat(explodedItems.select("items_productId").as(Encoders.STRING()).collectAsList())
                .containsExactlyInAnyOrder("PROD-001", "PROD-002");

        assertThat((long) ordersMain.count()).isEqualTo(1L);
        assertThat(ordersMain.columns()).contains("orderId");
        assertThat(ordersMain.columns()).doesNotContain("items_productId", "items_price");
    }

    @Test
    @DisplayName("Example 4: Using SQL functions")
    void testFunctionsExample() {
        // --- Setup ---
        NexusPiercerFunctions.registerAll(spark);
        Dataset<Row> jsonData = spark.createDataset(
                List.of(
                        "{\"name\":\"Alice\",\"scores\":[95,87,92]}",
                        "{\"name\":\"Bob\",\"scores\":[78,82,85]}",
                        "{\"invalid json",
                        "{\"name\":\"Charlie\",\"scores\":[]}"
                ), Encoders.STRING()
        ).toDF("json_string");

        // --- Action: DataFrame API ---
        Dataset<Row> processed = jsonData
                .withColumn("is_valid", isValid(col("json_string")))
                .withColumn("error", jsonError(col("json_string")))
                .withColumn("name", extractField(col("json_string"), "name"))
                .withColumn("scores_count", arrayCount(col("json_string"), "scores"));

        // --- Assertions: DataFrame API ---
        assertThat((long) processed.count()).isEqualTo(4L);
        Row aliceRow = processed.filter("name = 'Alice'").first();
        assertThat((Boolean) aliceRow.getAs("is_valid")).isEqualTo(true);
        assertThat((Long) aliceRow.getAs("scores_count")).isEqualTo(3L);

        Row invalidRow = processed.filter("is_valid = false").first();
        assertThat((String) invalidRow.getAs("error")).isNotNull();

        // --- Action: Spark SQL ---
        jsonData.createOrReplaceTempView("json_table");
        Dataset<Row> sqlResult = spark.sql("SELECT extract_nested_field(json_string, 'name') as name FROM json_table");

        // FIX: Be specific in the assertion to avoid ClassCastException
        List<String> names = sqlResult.as(Encoders.STRING()).collectAsList();
        assertThat(names).contains("Alice", "Bob", null, "Charlie"); // `null` for the invalid json
    }

//    @Test
//    @DisplayName("Example 5: Using pre-built patterns")
//    void testPatternsExample() throws IOException {
//        // --- Setup for all patterns ---
//        String schemaPath = tempDir.resolve("schemas/product.avsc").toString();
//        // We will create a single input file instead of a directory
//        String outputPath = tempDir.resolve("data/output/").toString();
//
//        createTestFile("schemas/product.avsc", """
//            { "type": "record", "name": "Product", "fields": [
//                {"name": "id", "type": "string"},
//                {"name": "category", "type": "string"},
//                {"name": "tags", "type": {"type": "array", "items": "string"}}
//            ]}
//            """);
//
//        // FIX: Combine all JSON into a single string, one per line.
//        String productsJson = String.join("\n",
//                "{\"id\":\"p1\",\"category\":\"electronics\",\"tags\":[\"a\",\"b\"]}",
//                "{\"id\":\"p2\",\"category\":\"books\",\"tags\":[\"c\"]}",
//                "{bad-json}"
//        );
//        // FIX: Write to a single file and get its explicit path.
//        Path inputFilePath = createTestFile("data/input/products/all_products.jsonl", productsJson);
//
//        // --- Pattern 1: JSON to Parquet ---
//        // FIX: Pass the explicit file path instead of the directory path.
//        NexusPiercerPatterns.jsonToParquet(spark, schemaPath, inputFilePath.toString(), outputPath + "products_parquet", SaveMode.Overwrite, "category");
//        Dataset<Row> parquetData = spark.read().parquet(outputPath + "products_parquet");
//        assertThat((long) parquetData.count()).isEqualTo(2L);
//        assertThat(parquetData.select("id").as(Encoders.STRING()).collectAsList()).contains("p1", "p2");
//
//        // --- Pattern 2: Data Quality Report ---
//        // FIX: Pass the explicit file path.
//        Dataset<Row> qualityReport = NexusPiercerPatterns.generateDataQualityReport(spark, schemaPath, inputFilePath.toString());
//        assertThat((long) qualityReport.count()).isPositive();
//        Row reportRow = qualityReport.first();
//        assertThat((long) reportRow.getAs("total_records")).isEqualTo(3L);
//        assertThat((long) reportRow.getAs("schema_valid_records")).isEqualTo(2L);
//
//        // --- Pattern 3: Profile JSON Structure ---
//        // FIX: Pass the explicit file path.
//        Dataset<Row> profile = NexusPiercerPatterns.profileJsonStructure(spark, inputFilePath.toString(), 100);
//        assertThat((long) profile.filter("field = 'id'").count()).isEqualTo(1L);
//        assertThat((long) profile.filter("field = 'tags_count'").count()).isEqualTo(1L);
//    }
}