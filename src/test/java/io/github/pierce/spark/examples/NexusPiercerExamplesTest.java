package io.github.pierce.spark.examples;


import io.github.pierce.spark.NexusPiercerFunctions;
import io.github.pierce.spark.NexusPiercerPatterns;
import io.github.pierce.spark.NexusPiercerSparkPipeline;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import scala.Function1;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

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
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();

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


        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(tempDir.resolve("schemas/user_activity.avsc").toString())
                .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.QUARANTINE)
                .process(dataFile.toString());


        Dataset<Row> successDs = result.getDataset();
        Dataset<Row> errorDs = result.getErrorDataset();

        System.out.println("=== Successful Records ===");
        successDs.show(false);
        System.out.println("=== Error Records ===");
        errorDs.show(false);


        assertThat((long) successDs.count()).isEqualTo(1L);
        assertThat((long) errorDs.count()).isEqualTo(2L);

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

    @Test
    @DisplayName("Example 5: Enhanced test for processing a JSON column within a complex DataFrame")
    void testEnhancedProcessingOfJsonColumn() throws IOException {
        // --- 1. Arrange: Define a more complex schema ---
        String complexSchemaContent = """
        {
          "type": "record", "name": "ShipmentNotification",
          "fields": [
            {"name": "shipmentId", "type": "string"},
            {"name": "customerId", "type": "long"},
            {"name": "isExpedited", "type": "boolean"},
            {"name": "status", "type": {"type": "enum", "name": "StatusType", "symbols": ["PENDING", "IN_TRANSIT", "DELIVERED", "EXCEPTION"]}},
            {"name": "eventTimestamp", "type": "long"},
            {"name": "origin", "type": {
              "type": "record", "name": "Address", "fields": [
                {"name": "street", "type": "string"},
                {"name": "city", "type": "string"},
                {"name": "zip", "type": "string"}
              ]}
            },
            {"name": "destination", "type": "Address"},
            {"name": "trackingEvents", "type": {"type": "array", "items": "string"}},
            {"name": "lineItems", "type": {"type": "array", "items": {
              "type": "record", "name": "Item", "fields": [
                {"name": "sku", "type": "string"},
                {"name": "quantity", "type": "int"},
                {"name": "price", "type": "double"}
              ]}
            }},
            {"name": "notes", "type": ["null", "string"], "default": null}
          ]
        }
        """;
        Path schemaFile = createTestFile("schemas/complex_shipment.avsc", complexSchemaContent);

        // --- 2. Arrange: Create diverse and complex test data ---

        // Three valid records that conform to the schema
        String validRecord1 = "{\"shipmentId\":\"SH-VALID-001\",\"customerId\":101,\"isExpedited\":true,\"status\":\"IN_TRANSIT\",\"eventTimestamp\":1678886400,\"origin\":{\"street\":\"100 Main St\",\"city\":\"Metropolis\",\"zip\":\"12345\"},\"destination\":{\"street\":\"500 End Rd\",\"city\":\"Gotham\",\"zip\":\"67890\"},\"trackingEvents\":[\"Created\",\"Picked Up\",\"In Transit\"],\"lineItems\":[{\"sku\":\"SKU-A\",\"quantity\":2,\"price\":19.99},{\"sku\":\"SKU-B\",\"quantity\":1,\"price\":50.0}],\"notes\":\"Leave at front door.\"}";
        String validRecord2 = "{\"shipmentId\":\"SH-VALID-002\",\"customerId\":202,\"isExpedited\":false,\"status\":\"PENDING\",\"eventTimestamp\":1678890000,\"origin\":{\"street\":\"200 Start Blvd\",\"city\":\"Star City\",\"zip\":\"54321\"},\"destination\":{\"street\":\"800 Last Ave\",\"city\":\"Central City\",\"zip\":\"09876\"},\"trackingEvents\":[\"Created\"],\"lineItems\":[{\"sku\":\"SKU-C\",\"quantity\":10,\"price\":5.25}]}"; // Note: 'notes' field is omitted, testing default null
        String validRecord3 = "{\"shipmentId\":\"SH-VALID-003\",\"customerId\":303,\"isExpedited\":false,\"status\":\"DELIVERED\",\"eventTimestamp\":1678990000,\"origin\":{\"street\":\"300 Place\",\"city\":\"Coast City\",\"zip\":\"11111\"},\"destination\":{\"street\":\"900 Way\",\"city\":\"National City\",\"zip\":\"22222\"},\"trackingEvents\":[],\"lineItems\":[]}"; // Empty arrays

        // Two invalid records: one with a schema validation error, one with a syntax error
        String recordWithSchemaError = "{\"shipmentId\":\"SH-SCHEMA-ERR\",\"customerId\":\"not-a-long\",\"isExpedited\":\"true\",\"status\":\"UNKNOWN_STATUS\",\"eventTimestamp\":1678886400,\"origin\":{\"street\":\"1 Error Ln\",\"city\":\"Corruptville\",\"zip\":\"00000\"},\"destination\":{\"street\":\"1 Bad Rd\",\"city\":\"Failburg\",\"zip\":\"99999\"},\"trackingEvents\":[],\"lineItems\":[]}"; // customerId is string, isExpedited is string, status is invalid enum
        String recordWithSyntaxError = "{\"shipmentId\":\"SH-SYNTAX-ERR\",\"customerId\":999, ... // Malformed JSON";

        // Create the source DataFrame with "header" columns and the JSON "payload" column
        StructType initialSchema = new StructType()
                .add("topic", DataTypes.StringType, false)
                .add("kafka_key", DataTypes.StringType, false)
                .add("ingestion_time", DataTypes.TimestampType, false)
                .add("payload", DataTypes.StringType, false);

        List<Row> initialData = Arrays.asList(
                RowFactory.create("shipments", "key-1", java.sql.Timestamp.valueOf("2023-03-15 12:00:00"), validRecord1),
                RowFactory.create("shipments", "key-2", java.sql.Timestamp.valueOf("2023-03-15 12:01:00"), validRecord2),
                RowFactory.create("shipments", "key-schema-err", java.sql.Timestamp.valueOf("2023-03-15 12:02:00"), recordWithSchemaError),
                RowFactory.create("shipments", "key-3", java.sql.Timestamp.valueOf("2023-03-15 12:03:00"), validRecord3),
                RowFactory.create("shipments", "key-syntax-err", java.sql.Timestamp.valueOf("2023-03-15 12:04:00"), recordWithSyntaxError)
        );
        Dataset<Row> sourceDf = spark.createDataFrame(initialData, initialSchema);

        // --- 3. Act: Process the 'payload' column using the pipeline ---
        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(schemaFile.toString())
                .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.QUARANTINE)
                .disableArrayStatistics() // Simplify output for assertions
                .includeRawJson() // Important for debugging and verifying error records
                .processJsonColumn(sourceDf, "payload");

        Dataset<Row> successDf = result.getDataset();
        Dataset<Row> errorDf = result.getErrorDataset();

        System.out.println("=== Source DataFrame ===");
        sourceDf.show(false);
        System.out.println("=== Final Success DataFrame (Headers + Flattened Payload) ===");
        successDf.show(false);
        System.out.println("=== Final Error DataFrame (Headers + Raw Payload + Error) ===");
        errorDf.show(false);

        // --- 4. Assert ---

        // Assert counts
        assertThat(successDf.count()).as("Count of successfully processed records").isEqualTo(3);
        assertThat(errorDf.count()).as("Count of quarantined error records").isEqualTo(2);

        // Assert success DataFrame schema and content
        assertThat(successDf.columns()).as("Success DF columns")
                .startsWith("topic", "kafka_key", "ingestion_time")
                .contains("shipmentId", "customerId", "isExpedited", "origin_city", "destination_zip", "lineItems_sku", "notes")
                .doesNotContain("payload", "lineItems_count"); // Original payload removed, array stats disabled

        // Deep dive into a specific successful record to verify the join was correct
        Row successRow1 = successDf.filter(col("kafka_key").equalTo("key-1")).first();
        assertThat(successRow1.getString(successRow1.fieldIndex("topic"))).isEqualTo("shipments");
        assertThat(successRow1.getString(successRow1.fieldIndex("shipmentId"))).isEqualTo("SH-VALID-001");
        assertThat(successRow1.getBoolean(successRow1.fieldIndex("isExpedited"))).isTrue();
        assertThat(successRow1.getString(successRow1.fieldIndex("origin_city"))).isEqualTo("Metropolis");
        assertThat(successRow1.getString(successRow1.fieldIndex("lineItems_sku"))).isEqualTo("SKU-A,SKU-B");
        assertThat(successRow1.getString(successRow1.fieldIndex("notes"))).isEqualTo("Leave at front door.");

        // Assert error DataFrame schema and content
        assertThat(errorDf.columns()).as("Error DF columns")
                .startsWith("topic", "kafka_key", "ingestion_time")
                .contains("_raw_json", "_error")
                .doesNotContain("payload", "shipmentId"); // No flattened columns in error DF

        // Check the specific error messages and their associated headers
        Row schemaErrorRow = errorDf.filter(col("kafka_key").equalTo("key-schema-err")).first();
        assertThat(schemaErrorRow.getString(schemaErrorRow.fieldIndex("topic"))).isEqualTo("shipments");
        assertThat(schemaErrorRow.getString(schemaErrorRow.fieldIndex("_error"))).isEqualTo("Schema validation failed");
        assertThat(schemaErrorRow.getString(schemaErrorRow.fieldIndex("_raw_json"))).isEqualTo(recordWithSchemaError);

        Row syntaxErrorRow = errorDf.filter(col("kafka_key").equalTo("key-syntax-err")).first();
        assertThat(syntaxErrorRow.getString(syntaxErrorRow.fieldIndex("topic"))).isEqualTo("shipments");
        assertThat(syntaxErrorRow.getString(syntaxErrorRow.fieldIndex("_error"))).isEqualTo("Malformed JSON string");
        assertThat(syntaxErrorRow.getString(syntaxErrorRow.fieldIndex("_raw_json"))).isEqualTo(recordWithSyntaxError);
    }


    @Test
    @DisplayName("Example 5: Process JSON column without _raw_json")
    void testProcessingJsonColumn_WithoutRawJson() throws IOException {
        // --- 1. Arrange: Define a more complex schema ---
        String complexSchemaContent = """
        {
          "type": "record", "name": "ShipmentNotification",
          "fields": [
            {"name": "shipmentId", "type": "string"},
            {"name": "customerId", "type": "long"},
            {"name": "isExpedited", "type": "boolean"},
            {"name": "status", "type": {"type": "enum", "name": "StatusType", "symbols": ["PENDING", "IN_TRANSIT", "DELIVERED", "EXCEPTION"]}},
            {"name": "eventTimestamp", "type": "long"},
            {"name": "origin", "type": {
              "type": "record", "name": "Address", "fields": [
                {"name": "street", "type": "string"},
                {"name": "city", "type": "string"},
                {"name": "zip", "type": "string"}
              ]}
            },
            {"name": "destination", "type": "Address"},
            {"name": "trackingEvents", "type": {"type": "array", "items": "string"}},
            {"name": "lineItems", "type": {"type": "array", "items": {
              "type": "record", "name": "Item", "fields": [
                {"name": "sku", "type": "string"},
                {"name": "quantity", "type": "int"},
                {"name": "price", "type": "double"}
              ]}
            }},
            {"name": "notes", "type": ["null", "string"], "default": null}
          ]
        }
        """;
        Path schemaFile = createTestFile("schemas/complex_shipment.avsc", complexSchemaContent);

        // --- 2. Arrange: Create diverse and complex test data ---

        // Three valid records that conform to the schema
        String validRecord1 = "{\"shipmentId\":\"SH-VALID-001\",\"customerId\":101,\"isExpedited\":true,\"status\":\"IN_TRANSIT\",\"eventTimestamp\":1678886400,\"origin\":{\"street\":\"100 Main St\",\"city\":\"Metropolis\",\"zip\":\"12345\"},\"destination\":{\"street\":\"500 End Rd\",\"city\":\"Gotham\",\"zip\":\"67890\"},\"trackingEvents\":[\"Created\",\"Picked Up\",\"In Transit\"],\"lineItems\":[{\"sku\":\"SKU-A\",\"quantity\":2,\"price\":19.99},{\"sku\":\"SKU-B\",\"quantity\":1,\"price\":50.0}],\"notes\":\"Leave at front door.\"}";
        String validRecord2 = "{\"shipmentId\":\"SH-VALID-002\",\"customerId\":202,\"isExpedited\":false,\"status\":\"PENDING\",\"eventTimestamp\":1678890000,\"origin\":{\"street\":\"200 Start Blvd\",\"city\":\"Star City\",\"zip\":\"54321\"},\"destination\":{\"street\":\"800 Last Ave\",\"city\":\"Central City\",\"zip\":\"09876\"},\"trackingEvents\":[\"Created\"],\"lineItems\":[{\"sku\":\"SKU-C\",\"quantity\":10,\"price\":5.25}]}"; // Note: 'notes' field is omitted, testing default null
        String validRecord3 = "{\"shipmentId\":\"SH-VALID-003\",\"customerId\":303,\"isExpedited\":false,\"status\":\"DELIVERED\",\"eventTimestamp\":1678990000,\"origin\":{\"street\":\"300 Place\",\"city\":\"Coast City\",\"zip\":\"11111\"},\"destination\":{\"street\":\"900 Way\",\"city\":\"National City\",\"zip\":\"22222\"},\"trackingEvents\":[],\"lineItems\":[]}"; // Empty arrays

        // Two invalid records: one with a schema validation error, one with a syntax error
        String recordWithSchemaError = "{\"shipmentId\":\"SH-SCHEMA-ERR\",\"customerId\":\"not-a-long\",\"isExpedited\":\"true\",\"status\":\"UNKNOWN_STATUS\",\"eventTimestamp\":1678886400,\"origin\":{\"street\":\"1 Error Ln\",\"city\":\"Corruptville\",\"zip\":\"00000\"},\"destination\":{\"street\":\"1 Bad Rd\",\"city\":\"Failburg\",\"zip\":\"99999\"},\"trackingEvents\":[],\"lineItems\":[]}"; // customerId is string, isExpedited is string, status is invalid enum
        String recordWithSyntaxError = "{\"shipmentId\":\"SH-SYNTAX-ERR\",\"customerId\":999, ... // Malformed JSON";

        // Create the source DataFrame with "header" columns and the JSON "payload" column
        StructType initialSchema = new StructType()
                .add("topic", DataTypes.StringType, false)
                .add("kafka_key", DataTypes.StringType, false)
                .add("ingestion_time", DataTypes.TimestampType, false)
                .add("payload", DataTypes.StringType, false);

        List<Row> initialData = Arrays.asList(
                RowFactory.create("shipments", "key-1", java.sql.Timestamp.valueOf("2023-03-15 12:00:00"), validRecord1),
                RowFactory.create("shipments", "key-2", java.sql.Timestamp.valueOf("2023-03-15 12:01:00"), validRecord2),
                RowFactory.create("shipments", "key-schema-err", java.sql.Timestamp.valueOf("2023-03-15 12:02:00"), recordWithSchemaError),
                RowFactory.create("shipments", "key-3", java.sql.Timestamp.valueOf("2023-03-15 12:03:00"), validRecord3),
                RowFactory.create("shipments", "key-syntax-err", java.sql.Timestamp.valueOf("2023-03-15 12:04:00"), recordWithSyntaxError)
        );
        Dataset<Row> sourceDf = spark.createDataFrame(initialData, initialSchema);

        // --- Act ---
        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(schemaFile.toString())
                .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.QUARANTINE)
                .disableArrayStatistics()
                // .includeRawJson() // <--- DO NOT CALL THIS METHOD
                .processJsonColumn(sourceDf, "payload");

        Dataset<Row> successDf = result.getDataset();
        Dataset<Row> errorDf = result.getErrorDataset();

        System.out.println("=== Success DF (No Raw JSON) ===");
        successDf.show(false);
        System.out.println("=== Error DF (No Raw JSON) ===");
        errorDf.show(false);

        // --- Assert ---
        // Assert that _raw_json IS NOT present
        assertThat(successDf.columns()).doesNotContain("_raw_json");
        // Assert that the error DF has the fallback column
        assertThat(errorDf.columns()).doesNotContain("_raw_json");
        assertThat(errorDf.columns()).contains("_source_payload", "_error");

        // Verify counts are still correct
        assertThat(successDf.count()).isEqualTo(3);
        assertThat(errorDf.count()).isEqualTo(2);
    }
}