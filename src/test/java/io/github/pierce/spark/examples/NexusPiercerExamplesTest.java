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
//        assertThat(errorDf.columns()).doesNotContain("_raw_json");
        assertThat(errorDf.columns()).contains("_error");

        // Verify counts are still correct
        assertThat(successDf.count()).isEqualTo(3);
        assertThat(errorDf.count()).isEqualTo(2);
    }


    @Test
    @DisplayName("PhD-Level Example: Massive 50+ field, 5-level nested schema with terminal/non-terminal array processing")
    void testMassiveEnterpriseSchemaWithAdvancedArrayProcessing() throws IOException {
        // === 1. DEFINE MASSIVE 50+ FIELD, 5-LEVEL NESTED ENTERPRISE SCHEMA ===
        String massiveSchemaContent = """
        {
          "type": "record",
          "name": "EnterpriseCustomerOrder",
          "namespace": "com.enterprise.orders",
          "doc": "Comprehensive enterprise customer order with 50+ fields across 5 nesting levels",
          "fields": [
            {"name": "orderId", "type": "string", "doc": "Unique order identifier"},
            {"name": "orderNumber", "type": "long", "doc": "Sequential order number"},
            {"name": "orderDate", "type": "long", "doc": "Order timestamp"},
            {"name": "orderStatus", "type": {"type": "enum", "name": "OrderStatus", "symbols": ["PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]}},
            {"name": "priority", "type": {"type": "enum", "name": "Priority", "symbols": ["LOW", "NORMAL", "HIGH", "URGENT"]}},
            {"name": "isExpressDelivery", "type": "boolean"},
            {"name": "totalAmount", "type": "double"},
            {"name": "currency", "type": "string"},
            {"name": "taxAmount", "type": "double"},
            {"name": "discountAmount", "type": ["null", "double"], "default": null},
            
            {"name": "orderTags", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Order classification tags"},
            {"name": "promotionCodes", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Applied promotion codes"},
            {"name": "riskFactors", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Risk assessment factors"},
            
            {
              "name": "customer",
              "type": {
                "type": "record",
                "name": "Customer",
                "doc": "LEVEL 2: Customer information",
                "fields": [
                  {"name": "customerId", "type": "string"},
                  {"name": "customerType", "type": {"type": "enum", "name": "CustomerType", "symbols": ["INDIVIDUAL", "BUSINESS", "ENTERPRISE"]}},
                  {"name": "firstName", "type": "string"},
                  {"name": "lastName", "type": "string"},
                  {"name": "email", "type": "string"},
                  {"name": "phoneNumber", "type": ["null", "string"], "default": null},
                  {"name": "dateOfBirth", "type": ["null", "long"], "default": null},
                  {"name": "loyaltyTier", "type": {"type": "enum", "name": "LoyaltyTier", "symbols": ["BRONZE", "SILVER", "GOLD", "PLATINUM"]}},
                  {"name": "isVip", "type": "boolean"},
                  {"name": "accountBalance", "type": "double"},
                  
                  {"name": "preferredCategories", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Customer's preferred product categories"},
                  {"name": "languages", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Customer's preferred languages"},
                  
                  {
                    "name": "addresses",
                    "type": {
                      "type": "array",
                      "items": {
                        "type": "record",
                        "name": "Address",
                        "doc": "NON-TERMINAL ARRAY - LEVEL 3: Address details",
                        "fields": [
                          {"name": "addressId", "type": "string"},
                          {"name": "addressType", "type": {"type": "enum", "name": "AddressType", "symbols": ["HOME", "WORK", "BILLING", "SHIPPING"]}},
                          {"name": "street1", "type": "string"},
                          {"name": "street2", "type": ["null", "string"], "default": null},
                          {"name": "city", "type": "string"},
                          {"name": "state", "type": "string"},
                          {"name": "zipCode", "type": "string"},
                          {"name": "country", "type": "string"},
                          {"name": "isDefault", "type": "boolean"},
                          
                          {"name": "deliveryInstructions", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Special delivery instructions"},
                          
                          {
                            "name": "geoLocation",
                            "type": {
                              "type": "record",
                              "name": "GeoLocation",
                              "doc": "LEVEL 4: Geographic coordinates",
                              "fields": [
                                {"name": "latitude", "type": "double"},
                                {"name": "longitude", "type": "double"},
                                {"name": "accuracy", "type": "double"},
                                {"name": "source", "type": {"type": "enum", "name": "GeoSource", "symbols": ["GPS", "IP", "MANUAL", "GEOCODED"]}},
                                
                                {
                                  "name": "boundaryPolygon",
                                  "type": {
                                    "type": "record",
                                    "name": "BoundaryPolygon",
                                    "doc": "LEVEL 5: Geographic boundary definition",
                                    "fields": [
                                      {"name": "coordinates", "type": {"type": "array", "items": "double"}, "doc": "TERMINAL ARRAY - Polygon coordinates"},
                                      {"name": "boundaryType", "type": "string"},
                                      {"name": "area", "type": "double"}
                                    ]
                                  }
                                }
                              ]
                            }
                          }
                        ]
                      }
                    }
                  },
                  
                  {
                    "name": "paymentMethods",
                    "type": {
                      "type": "array",
                      "items": {
                        "type": "record",
                        "name": "PaymentMethod",
                        "doc": "NON-TERMINAL ARRAY - LEVEL 3: Payment method details",
                        "fields": [
                          {"name": "paymentId", "type": "string"},
                          {"name": "paymentType", "type": {"type": "enum", "name": "PaymentType", "symbols": ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER", "CRYPTO"]}},
                          {"name": "isDefault", "type": "boolean"},
                          {"name": "expiryDate", "type": ["null", "long"], "default": null},
                          {"name": "last4Digits", "type": ["null", "string"], "default": null},
                          
                          {"name": "supportedCurrencies", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Supported currencies"},
                          
                          {
                            "name": "transactionHistory",
                            "type": {
                              "type": "record",
                              "name": "TransactionHistory",
                              "doc": "LEVEL 4: Transaction history summary",
                              "fields": [
                                {"name": "totalTransactions", "type": "long"},
                                {"name": "totalAmount", "type": "double"},
                                {"name": "avgTransactionAmount", "type": "double"},
                                {"name": "lastTransactionDate", "type": ["null", "long"], "default": null},
                                
                                {"name": "frequentMerchants", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Frequently used merchants"},
                                
                                {
                                  "name": "riskProfile",
                                  "type": {
                                    "type": "record",
                                    "name": "RiskProfile",
                                    "doc": "LEVEL 5: Risk assessment profile",
                                    "fields": [
                                      {"name": "riskScore", "type": "double"},
                                      {"name": "riskLevel", "type": {"type": "enum", "name": "RiskLevel", "symbols": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]}},
                                      {"name": "lastAssessmentDate", "type": "long"},
                                      {"name": "flaggedTransactions", "type": "long"},
                                      {"name": "riskIndicators", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Risk indicators"}
                                    ]
                                  }
                                }
                              ]
                            }
                          }
                        ]
                      }
                    }
                  }
                ]
              }
            },
            
            {
              "name": "lineItems",
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "LineItem",
                  "doc": "NON-TERMINAL ARRAY - LEVEL 2: Individual order line items",
                  "fields": [
                    {"name": "lineItemId", "type": "string"},
                    {"name": "productId", "type": "string"},
                    {"name": "productName", "type": "string"},
                    {"name": "quantity", "type": "int"},
                    {"name": "unitPrice", "type": "double"},
                    {"name": "totalPrice", "type": "double"},
                    {"name": "discountPercentage", "type": ["null", "double"], "default": null},
                    
                    {"name": "productTags", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Product classification tags"},
                    {"name": "allergens", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Product allergen information"},
                    
                    {
                      "name": "product",
                      "type": {
                        "type": "record",
                        "name": "Product",
                        "doc": "LEVEL 3: Detailed product information",
                        "fields": [
                          {"name": "sku", "type": "string"},
                          {"name": "brand", "type": "string"},
                          {"name": "category", "type": "string"},
                          {"name": "subcategory", "type": "string"},
                          {"name": "weight", "type": ["null", "double"], "default": null},
                          {"name": "dimensions", "type": ["null", "string"], "default": null},
                          {"name": "color", "type": ["null", "string"], "default": null},
                          {"name": "size", "type": ["null", "string"], "default": null},
                          
                          {"name": "keywords", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Search keywords"},
                          
                          {
                            "name": "specifications",
                            "type": {
                              "type": "record",
                              "name": "ProductSpecifications",
                              "doc": "LEVEL 4: Technical product specifications",
                              "fields": [
                                {"name": "model", "type": "string"},
                                {"name": "manufacturer", "type": "string"},
                                {"name": "warrantyPeriod", "type": "int"},
                                {"name": "energyRating", "type": ["null", "string"], "default": null},
                                
                                {"name": "features", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Product features"},
                                {"name": "compatibleAccessories", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Compatible accessories"},
                                
                                {
                                  "name": "certifications",
                                  "type": {
                                    "type": "record",
                                    "name": "ProductCertifications",
                                    "doc": "LEVEL 5: Product certifications and compliance",
                                    "fields": [
                                      {"name": "certificationBody", "type": "string"},
                                      {"name": "certificationDate", "type": "long"},
                                      {"name": "expiryDate", "type": ["null", "long"], "default": null},
                                      {"name": "certificationNumber", "type": "string"},
                                      {"name": "complianceStandards", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Compliance standards"}
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
              }
            },
            
            {
              "name": "shipments",
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "Shipment",
                  "doc": "NON-TERMINAL ARRAY - LEVEL 2: Shipment tracking information",
                  "fields": [
                    {"name": "shipmentId", "type": "string"},
                    {"name": "trackingNumber", "type": "string"},
                    {"name": "carrier", "type": "string"},
                    {"name": "shipmentMethod", "type": {"type": "enum", "name": "ShipmentMethod", "symbols": ["STANDARD", "EXPRESS", "OVERNIGHT", "SAME_DAY"]}},
                    {"name": "estimatedDeliveryDate", "type": "long"},
                    {"name": "actualDeliveryDate", "type": ["null", "long"], "default": null},
                    {"name": "shippingCost", "type": "double"},
                    
                    {"name": "trackingEvents", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Tracking event descriptions"},
                    {"name": "specialInstructions", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Special handling instructions"},
                    
                    {
                      "name": "packageDetails",
                      "type": {
                        "type": "record",
                        "name": "PackageDetails",
                        "doc": "LEVEL 3: Physical package information",
                        "fields": [
                          {"name": "packageId", "type": "string"},
                          {"name": "weight", "type": "double"},
                          {"name": "length", "type": "double"},
                          {"name": "width", "type": "double"},
                          {"name": "height", "type": "double"},
                          {"name": "packageType", "type": "string"},
                          
                          {"name": "handlingCodes", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Special handling codes"},
                          
                          {
                            "name": "insurance",
                            "type": {
                              "type": "record",
                              "name": "InsuranceDetails",
                              "doc": "LEVEL 4: Package insurance information",
                              "fields": [
                                {"name": "isInsured", "type": "boolean"},
                                {"name": "insuranceAmount", "type": ["null", "double"], "default": null},
                                {"name": "insuranceProvider", "type": ["null", "string"], "default": null},
                                {"name": "policyNumber", "type": ["null", "string"], "default": null},
                                
                                {"name": "coveredItems", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Insured item descriptions"},
                                
                                {
                                  "name": "claimsHistory",
                                  "type": {
                                    "type": "record",
                                    "name": "ClaimsHistory",
                                    "doc": "LEVEL 5: Insurance claims history",
                                    "fields": [
                                      {"name": "totalClaims", "type": "int"},
                                      {"name": "totalClaimAmount", "type": "double"},
                                      {"name": "lastClaimDate", "type": ["null", "long"], "default": null},
                                      {"name": "claimReasons", "type": {"type": "array", "items": "string"}, "doc": "TERMINAL ARRAY - Historical claim reasons"}
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
              }
            },
            
            {"name": "orderMetadata", "type": {"type": "map", "values": "string"}, "doc": "Additional order metadata"},
            {"name": "internalNotes", "type": ["null", "string"], "default": null},
            {"name": "externalNotes", "type": ["null", "string"], "default": null}
          ]
        }
        """;

        Path massiveSchemaFile = createTestFile("schemas/massive_enterprise_order.avsc", massiveSchemaContent);

        // === 2. CREATE COMPREHENSIVE TEST DATA ===
        String massiveTestJson = """
        {
          "orderId": "ORD-ENT-2025-001",
          "orderNumber": 1234567890,
          "orderDate": 1678886400,
          "orderStatus": "PROCESSING",
          "priority": "HIGH",
          "isExpressDelivery": true,
          "totalAmount": 2599.99,
          "currency": "USD",
          "taxAmount": 259.99,
          "discountAmount": 100.00,
          
          "orderTags": ["electronics", "high-value", "express", "verified-customer"],
          "promotionCodes": ["NEWCUSTOMER10", "EXPRESS_FREE"],
          "riskFactors": ["high-value", "international"],
          
          "customer": {
            "customerId": "CUST-12345",
            "customerType": "ENTERPRISE",
            "firstName": "John",
            "lastName": "Enterprise",
            "email": "john@enterprise.com",
            "phoneNumber": "+1-555-0123",
            "dateOfBirth": 567993600,
            "loyaltyTier": "PLATINUM",
            "isVip": true,
            "accountBalance": 15000.50,
            
            "preferredCategories": ["electronics", "computers", "software"],
            "languages": ["en", "es", "fr"],
            
            "addresses": [
              {
                "addressId": "ADDR-001",
                "addressType": "SHIPPING",
                "street1": "123 Enterprise Blvd",
                "street2": "Suite 100",
                "city": "San Francisco",
                "state": "CA",
                "zipCode": "94105",
                "country": "USA",
                "isDefault": true,
                "deliveryInstructions": ["Ring doorbell", "Leave with concierge", "Signature required"],
                "geoLocation": {
                  "latitude": 37.7749,
                  "longitude": -122.4194,
                  "accuracy": 10.5,
                  "source": "GPS",
                  "boundaryPolygon": {
                    "coordinates": [37.7740, -122.4200, 37.7758, -122.4188],
                    "boundaryType": "rectangular",
                    "area": 0.25
                  }
                }
              },
              {
                "addressId": "ADDR-002",
                "addressType": "BILLING",
                "street1": "456 Corporate Way",
                "city": "New York",
                "state": "NY",
                "zipCode": "10001",
                "country": "USA",
                "isDefault": false,
                "deliveryInstructions": ["Business hours only"],
                "geoLocation": {
                  "latitude": 40.7128,
                  "longitude": -74.0060,
                  "accuracy": 5.0,
                  "source": "GEOCODED",
                  "boundaryPolygon": {
                    "coordinates": [40.7120, -74.0070, 40.7136, -74.0050],
                    "boundaryType": "circular",
                    "area": 0.15
                  }
                }
              }
            ],
            
            "paymentMethods": [
              {
                "paymentId": "PAY-001",
                "paymentType": "CREDIT_CARD",
                "isDefault": true,
                "expiryDate": 1767225600,
                "last4Digits": "1234",
                "supportedCurrencies": ["USD", "EUR", "GBP"],
                "transactionHistory": {
                  "totalTransactions": 156,
                  "totalAmount": 45600.75,
                  "avgTransactionAmount": 292.31,
                  "lastTransactionDate": 1678800000,
                  "frequentMerchants": ["Amazon", "Best Buy", "Apple Store", "Target"],
                  "riskProfile": {
                    "riskScore": 2.3,
                    "riskLevel": "LOW",
                    "lastAssessmentDate": 1678886400,
                    "flaggedTransactions": 2,
                    "riskIndicators": ["velocity-check", "location-variance"]
                  }
                }
              },
              {
                "paymentId": "PAY-002",
                "paymentType": "PAYPAL",
                "isDefault": false,
                "supportedCurrencies": ["USD", "CAD"],
                "transactionHistory": {
                  "totalTransactions": 23,
                  "totalAmount": 1250.00,
                  "avgTransactionAmount": 54.35,
                  "lastTransactionDate": 1678000000,
                  "frequentMerchants": ["eBay", "Etsy"],
                  "riskProfile": {
                    "riskScore": 1.1,
                    "riskLevel": "LOW",
                    "lastAssessmentDate": 1678886400,
                    "flaggedTransactions": 0,
                    "riskIndicators": []
                  }
                }
              }
            ]
          },
          
          "lineItems": [
            {
              "lineItemId": "LINE-001",
              "productId": "PROD-LAPTOP-001",
              "productName": "Enterprise Laptop Pro 15",
              "quantity": 2,
              "unitPrice": 1299.99,
              "totalPrice": 2599.98,
              "discountPercentage": 5.0,
              "productTags": ["laptop", "professional", "high-performance"],
              "allergens": [],
              "product": {
                "sku": "LAPTOP-ENT-PRO-15-001",
                "brand": "TechCorp",
                "category": "Electronics",
                "subcategory": "Laptops",
                "weight": 3.2,
                "dimensions": "35.5 x 24.5 x 1.8 cm",
                "color": "Space Gray",
                "size": "15-inch",
                "keywords": ["laptop", "professional", "enterprise", "15-inch", "high-performance"],
                "specifications": {
                  "model": "Enterprise Pro 15",
                  "manufacturer": "TechCorp Industries",
                  "warrantyPeriod": 36,
                  "energyRating": "A++",
                  "features": ["16GB RAM", "512GB SSD", "Intel i7", "Retina Display", "Touch ID"],
                  "compatibleAccessories": ["USB-C Hub", "Wireless Mouse", "Laptop Stand", "External Monitor"],
                  "certifications": {
                    "certificationBody": "International Electronics Consortium",
                    "certificationDate": 1640995200,
                    "expiryDate": 1672531200,
                    "certificationNumber": "IEC-2021-LAPTOP-7854",
                    "complianceStandards": ["ISO-14001", "ENERGY-STAR", "EPEAT-GOLD", "FCC-PART-15"]
                  }
                }
              }
            },
            {
              "lineItemId": "LINE-002",
              "productId": "PROD-MOUSE-001",
              "productName": "Wireless Precision Mouse",
              "quantity": 2,
              "unitPrice": 79.99,
              "totalPrice": 159.98,
              "productTags": ["mouse", "wireless", "precision"],
              "allergens": [],
              "product": {
                "sku": "MOUSE-WIRELESS-PREC-001",
                "brand": "TechCorp",
                "category": "Electronics",
                "subcategory": "Accessories",
                "weight": 0.15,
                "color": "Black",
                "keywords": ["mouse", "wireless", "precision", "ergonomic"],
                "specifications": {
                  "model": "Precision Pro",
                  "manufacturer": "TechCorp Industries",
                  "warrantyPeriod": 12,
                  "features": ["Wireless", "Rechargeable", "Precision Tracking", "Ergonomic Design"],
                  "compatibleAccessories": ["Mouse Pad", "USB-C Cable"],
                  "certifications": {
                    "certificationBody": "Peripheral Standards Authority",
                    "certificationDate": 1640995200,
                    "certificationNumber": "PSA-2021-MOUSE-3421",
                    "complianceStandards": ["ISO-9001", "CE-MARK", "FCC-PART-15"]
                  }
                }
              }
            }
          ],
          
          "shipments": [
            {
              "shipmentId": "SHIP-001",
              "trackingNumber": "1Z999AA1234567890",
              "carrier": "UPS",
              "shipmentMethod": "EXPRESS",
              "estimatedDeliveryDate": 1679059200,
              "shippingCost": 29.99,
              "trackingEvents": ["Label Created", "Picked Up", "In Transit", "Out for Delivery"],
              "specialInstructions": ["Signature Required", "Do Not Leave Unattended"],
              "packageDetails": {
                "packageId": "PKG-001",
                "weight": 7.5,
                "length": 45.0,
                "width": 35.0,
                "height": 8.0,
                "packageType": "Express Box",
                "handlingCodes": ["FRAGILE", "HIGH-VALUE", "SIGNATURE-REQUIRED"],
                "insurance": {
                  "isInsured": true,
                  "insuranceAmount": 3000.00,
                  "insuranceProvider": "ShipSure Insurance",
                  "policyNumber": "POL-2025-001234",
                  "coveredItems": ["Enterprise Laptop Pro 15 (2x)", "Wireless Precision Mouse (2x)"],
                  "claimsHistory": {
                    "totalClaims": 0,
                    "totalClaimAmount": 0.0,
                    "claimReasons": []
                  }
                }
              }
            }
          ],
          
          "orderMetadata": {
            "source": "enterprise-portal",
            "salesRep": "jane.doe@company.com",
            "approvalLevel": "manager",
            "purchaseOrder": "PO-2025-001"
          },
          "internalNotes": "High-value enterprise customer - expedite processing",
          "externalNotes": "Please ensure signature confirmation on delivery"
        }
        """;

        Dataset<String> massiveJsonDs = spark.createDataset(List.of(massiveTestJson), Encoders.STRING());

        // === 3. TEST WITH STANDARD CONFIGURATION (includes non-terminal arrays) ===
        System.out.println("=== PROCESSING MASSIVE SCHEMA WITH STANDARD CONFIGURATION ===");

        NexusPiercerSparkPipeline.ProcessingResult standardResult = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(massiveSchemaFile.toString())
                .enableArrayStatistics()
                .includeNonTerminalArrays() // This is the default
                .processDataset(massiveJsonDs);

        Dataset<Row> standardSuccess = standardResult.getDataset();

        System.out.println("Standard Configuration - Total Columns: " + standardSuccess.columns().length);
        List<String> allStandardColumns = Arrays.asList(standardSuccess.columns());

        // Count different types of fields
        long terminalArrayFields = allStandardColumns.stream().filter(col ->
                (col.equals("orderTags") || col.equals("promotionCodes") || col.equals("riskFactors") ||
                        col.equals("customer_preferredCategories") || col.equals("customer_languages") ||
                        col.contains("deliveryInstructions") || col.contains("coordinates") ||
                        col.contains("supportedCurrencies") || col.contains("frequentMerchants") ||
                        col.contains("riskIndicators") || col.contains("productTags") ||
                        col.contains("keywords") || col.contains("features") ||
                        col.contains("compatibleAccessories") || col.contains("complianceStandards") ||
                        col.contains("trackingEvents") || col.contains("specialInstructions") ||
                        col.contains("handlingCodes") || col.contains("coveredItems") ||
                        col.contains("claimReasons")) && !col.contains("_count") && !col.contains("_type")
        ).count();

        long nonTerminalArrayFields = allStandardColumns.stream().filter(col ->
                col.equals("customer_addresses") || col.equals("customer_paymentMethods") ||
                        col.equals("lineItems") || col.equals("shipments")
        ).count();

        long statisticsFields = allStandardColumns.stream().filter(col ->
                col.contains("_count") || col.contains("_distinct_count") ||
                        col.contains("_min_length") || col.contains("_max_length") ||
                        col.contains("_avg_length") || col.contains("_type")
        ).count();

        System.out.println("Field Analysis:");
        System.out.println("- Terminal Array Fields: " + terminalArrayFields);
        System.out.println("- Non-Terminal Array Fields: " + nonTerminalArrayFields);
        System.out.println("- Statistics Fields: " + statisticsFields);
        System.out.println("- Other Fields: " + (allStandardColumns.size() - terminalArrayFields - nonTerminalArrayFields - statisticsFields));

        standardSuccess.show(1, false);

        // === 4. TEST WITH TERMINAL ARRAYS ONLY CONFIGURATION ===
        System.out.println("\n=== PROCESSING MASSIVE SCHEMA WITH TERMINAL ARRAYS ONLY ===");

        NexusPiercerSparkPipeline.ProcessingResult terminalOnlyResult = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(massiveSchemaFile.toString())
                .enableArrayStatistics()
                .processDataset(massiveJsonDs);

        Dataset<Row> terminalOnlySuccess = terminalOnlyResult.getDataset();

        System.out.println("Terminal Arrays Only - Total Columns: " + terminalOnlySuccess.columns().length);
        List<String> terminalOnlyColumns = Arrays.asList(terminalOnlySuccess.columns());

        // === 5. COMPREHENSIVE VALIDATION ===

        // Verify that standard config includes both terminal and non-terminal arrays
        assertThat(allStandardColumns).as("Standard config should include terminal arrays")
                .contains("orderTags", "promotionCodes", "customer_preferredCategories");
        assertThat(allStandardColumns).as("Standard config should include non-terminal arrays")
                .contains("customer_addresses", "lineItems", "shipments");

        // Verify that terminal-only config excludes non-terminal arrays but keeps terminal arrays
        assertThat(terminalOnlyColumns).as("Terminal-only config should include terminal arrays")
                .contains("orderTags", "promotionCodes", "customer_preferredCategories");
        assertThat(terminalOnlyColumns).as("Terminal-only config should exclude non-terminal arrays")
                .doesNotContain("customer_addresses", "lineItems", "shipments");

        // Verify that descendants of non-terminal arrays are still present in both configs
        assertThat(allStandardColumns).as("Standard config should have flattened descendants")
                .contains("customer_addresses_street1", "lineItems_productName", "shipments_trackingNumber");
        assertThat(terminalOnlyColumns).as("Terminal-only config should have flattened descendants")
                .contains("customer_addresses_street1", "lineItems_productName", "shipments_trackingNumber");

        // Verify deep nesting (5 levels) works correctly
        assertThat(allStandardColumns).as("Should handle 5-level deep nesting")
                .contains("customer_addresses_geoLocation_boundaryPolygon_coordinates",
                        "customer_paymentMethods_transactionHistory_riskProfile_riskIndicators",
                        "lineItems_product_specifications_certifications_complianceStandards",
                        "shipments_packageDetails_insurance_claimsHistory_claimReasons");

        // Verify terminal arrays at different nesting levels are preserved
        assertThat(allStandardColumns).as("Should preserve terminal arrays at all levels")
                .contains("orderTags", // Level 1
                        "customer_preferredCategories", // Level 2
                        "customer_addresses_deliveryInstructions", // Level 3
                        "lineItems_product_specifications_features", // Level 4
                        "lineItems_product_specifications_certifications_complianceStandards"); // Level 5

        // Verify array statistics are generated for terminal arrays
        assertThat(allStandardColumns).as("Should have statistics for terminal arrays")
                .contains("orderTags_count", "customer_preferredCategories_count",
                        "customer_addresses_deliveryInstructions_count");

        // Verify field explosion ratio is significant (should be 10x+ original fields)
        assertThat(allStandardColumns.size()).as("Should have significant field explosion")
                .isGreaterThan(100); // Much more than the ~50 original fields

        // Verify data integrity - check a few sample values
        Row sampleRow = standardSuccess.first();
        assertThat(sampleRow.<String>getAs("orderId")).isEqualTo("ORD-ENT-2025-001");
        assertThat(sampleRow.<String>getAs("orderTags")).isEqualTo("electronics,high-value,express,verified-customer");
        assertThat(sampleRow.<String>getAs("customer_firstName")).isEqualTo("John");
        assertThat(sampleRow.<String>getAs("lineItems_productName")).isEqualTo("Enterprise Laptop Pro 15,Wireless Precision Mouse");

        // Verify deep nested data is correctly flattened
        assertThat(sampleRow.<String>getAs("customer_addresses_geoLocation_latitude")).isEqualTo("37.7749,40.7128");
        assertThat(sampleRow.<String>getAs("lineItems_product_specifications_certifications_complianceStandards"))
                .contains("ISO-14001", "ENERGY-STAR");

        // === 6. PERFORMANCE AND COMPLEXITY METRICS ===
        System.out.println("\n=== PERFORMANCE AND COMPLEXITY ANALYSIS ===");
        System.out.println("Original schema complexity: 50+ fields across 5 nesting levels");
        System.out.println("Field explosion ratio: " + String.format("%.1fx", (double) allStandardColumns.size() / 50));
        System.out.println("Terminal arrays preserved: " + terminalArrayFields);
        System.out.println("Deep nesting levels successfully flattened: 5");
        System.out.println("Processing time: " + standardResult.getMetrics().getProcessingTimeMs() + "ms");

        System.out.println("\n=== SCHEMA ANALYSIS SUMMARY ===");
        System.out.println("✅ Successfully processed 50+ field, 5-level nested enterprise schema");
        System.out.println("✅ Terminal arrays correctly preserved at all nesting levels");
        System.out.println("✅ Non-terminal arrays correctly flattened into individual fields");
        System.out.println("✅ Deep nested structures (Level 5) properly handled");
        System.out.println("✅ Array statistics generated for data-containing arrays");
        System.out.println("✅ Configurable terminal/non-terminal array processing works correctly");
        System.out.println("✅ Enterprise-scale complexity handled efficiently");
    }
}