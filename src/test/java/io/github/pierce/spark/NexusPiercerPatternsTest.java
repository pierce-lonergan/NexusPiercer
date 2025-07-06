package io.github.pierce.spark;


import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive JUnit tests for the NexusPiercerPatterns class.
 *
 * These tests cover all public methods, including batch processing to Parquet and Delta,
 * table normalization, data quality reporting, and utility patterns.
 *
 * NOTE: To run Delta Lake tests, ensure you have the `io.delta:delta-spark` dependency
 * in your pom.xml with a 'test' scope.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NexusPiercerPatternsTest {

    private SparkSession spark;

    @TempDir
    static Path tempDir;

    // --- Test Data and Schemas ---

    private final String productSchema = """
            {
              "type": "record", "name": "Product", "namespace": "com.example",
              "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": ["null", "string"], "default": null},
                {"name": "category", "type": "string"},
                {"name": "price", "type": "double"},
                {"name": "tags", "type": {"type": "array", "items": "string"}}
              ]
            }
            """;

    private final String productSchemaV2 = """
            {
              "type": "record", "name": "Product", "namespace": "com.example",
              "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": ["null", "string"], "default": null},
                {"name": "category", "type": "string"},
                {"name": "price", "type": "double"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "stock", "type": ["null", "int"], "default": null}
              ]
            }
            """;

    private final String productSchemaIncompatible = """
            {
              "type": "record", "name": "Product", "namespace": "com.example",
              "fields": [
                {"name": "id", "type": "string"},
                {"name": "price", "type": "boolean"}
              ]
            }
            """;

    private final String productJsonData = String.join("\n",
            "{\"id\": \"p1\", \"name\": \"Laptop\", \"category\": \"electronics\", \"price\": 1200.50, \"tags\": [\"pc\", \"work\"]}",
            "{\"id\": \"p2\", \"name\": \"Coffee Mug\", \"category\": \"kitchen\", \"price\": 15.00, \"tags\": [\"home\"]}",
            "{\"id\": \"p3\", \"category\": \"electronics\", \"price\": \"not-a-double\", \"tags\": []}", // Schema validation error
            "{malformed-json}" // Parse error
    );

    private final String orderSchema = """
            {
              "type": "record", "name": "Order", "namespace": "com.example",
              "fields": [
                {"name": "orderId", "type": "string"},
                {"name": "customer", "type": {"type": "record", "name": "Customer", "fields": [
                  {"name": "id", "type": "string"}, {"name": "email", "type": "string"}
                ]}},
                {"name": "items", "type": {"type": "array", "items": {"type": "record", "name": "Item", "fields": [
                  {"name": "productId", "type": "string"}, {"name": "quantity", "type": "int"}
                ]}}},
                {"name": "notes", "type": ["null", {"type": "array", "items": "string"}], "default": null}
              ]
            }
            """;

    private final String orderJsonData = String.join("\n",
            "{\"orderId\":\"o1\",\"customer\":{\"id\":\"c1\",\"email\":\"c1@test.com\"},\"items\":[{\"productId\":\"p1\",\"quantity\":1},{\"productId\":\"p2\",\"quantity\":3}],\"notes\":[\"gift wrap\"]}",
            "{\"orderId\":\"o2\",\"customer\":{\"id\":\"c2\",\"email\":\"c2@test.com\"},\"items\":[{\"productId\":\"p1\",\"quantity\":2}]}"
    );

    @BeforeAll
    void setupSpark() {
        // NOTE: For Delta tests to work, you need the delta-spark dependency and this config.
        spark = SparkSession.builder()
                .appName("NexusPiercerPatternsTest")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        NexusPiercerFunctions.registerAll(spark);
    }

    @AfterAll
    void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    private Path createTestFile(String path, String content) throws IOException {
        Path filePath = tempDir.resolve(path);
        Files.createDirectories(filePath.getParent());
        Files.write(filePath, content.getBytes());
        return filePath;
    }

    @Test
    @DisplayName("jsonToParquet should write valid records and quarantine errors")
    void testJsonToParquet() throws IOException {
        // Arrange
        Path schemaFile = createTestFile("schemas/product.avsc", productSchema);
        Path dataFile = createTestFile("data/products.json", productJsonData);
        Path outputPath = tempDir.resolve("output/products_parquet");
        Path errorsPath = tempDir.resolve("output/products_parquet_errors");

        // Act
        NexusPiercerPatterns.jsonToParquet(spark, schemaFile.toString(), dataFile.toString(), outputPath.toString(), SaveMode.Overwrite, "category");

        // Assert
        Dataset<Row> validData = spark.read().parquet(outputPath.toString());
        Dataset<Row> errorData = spark.read().parquet(errorsPath.toString());

        assertThat(validData.count()).isEqualTo(3L);
        assertThat(validData.columns()).contains("id", "name", "category", "price", "tags", "_processing_time", "_input_file");
        assertThat(validData.select("id").collectAsList().stream().map(r -> r.getString(0)))
                .containsExactlyInAnyOrder("p1", "p2", "p3");
        // Record p3 has a null price due to schema validation failure
        assertThat(validData.filter("id = 'p3'").first().isNullAt(validData.schema().fieldIndex("price"))).isTrue();

        assertThat(errorData.count()).isEqualTo(1L);
        assertThat(errorData.first().getString(errorData.schema().fieldIndex("_error"))).contains("Malformed JSON string");

        // Assert partitioning
        assertThat(Files.exists(outputPath.resolve("category=electronics"))).isTrue();
        assertThat(Files.exists(outputPath.resolve("category=kitchen"))).isTrue();
    }

    @Test
    @DisplayName("jsonToDelta should perform overwrite and merge operations correctly")
    void testJsonToDelta() throws IOException {
        // Arrange
        Path schemaFile = createTestFile("schemas/product.avsc", productSchema);
        Path initialDataFile = createTestFile("data/initial_products.json", "{\"id\": \"p1\", \"name\": \"Initial Laptop\", \"category\": \"electronics\", \"price\": 1000.0, \"tags\": [\"a\"]}");
        Path updatesDataFile = createTestFile("data/updates_products.json", "{\"id\": \"p1\", \"name\": \"Updated Laptop\", \"category\": \"electronics\", \"price\": 1100.0, \"tags\": [\"b\"]}\n{\"id\": \"p4\", \"name\": \"Mouse\", \"category\": \"peripherals\", \"price\": 50.0, \"tags\": [\"c\"]}");
        Path deltaPath = tempDir.resolve("output/delta_products");

        // --- Act 1: Initial Overwrite ---
        NexusPiercerPatterns.jsonToDelta(spark, schemaFile.toString(), initialDataFile.toString(), deltaPath.toString());

        // Assert 1
        DeltaTable initialTable = DeltaTable.forPath(spark, deltaPath.toString());
        assertThat(initialTable.toDF().count()).isEqualTo(1L);
        assertThat(initialTable.toDF().first().getString(initialTable.toDF().schema().fieldIndex("name"))).isEqualTo("Initial Laptop");

        // --- Act 2: Merge ---
        NexusPiercerPatterns.jsonToDelta(spark, schemaFile.toString(), updatesDataFile.toString(), deltaPath.toString(), "id", true);

        // Assert 2
        Dataset<Row> finalData = spark.read().format("delta").load(deltaPath.toString());
        finalData.show(false);
        assertThat(finalData.count()).isEqualTo(2L);

        Map<String, Row> finalMap = new java.util.HashMap<>();
        finalData.collectAsList().forEach(r -> finalMap.put(r.getString(r.schema().fieldIndex("id")), r));

        assertThat(finalMap.get("p1").getString(finalMap.get("p1").schema().fieldIndex("name"))).isEqualTo("Updated Laptop");
        assertThat(finalMap.get("p1").getDouble(finalMap.get("p1").schema().fieldIndex("price"))).isEqualTo(1100.0);
        assertThat(finalMap.get("p4").getString(finalMap.get("p4").schema().fieldIndex("name"))).isEqualTo("Mouse");
    }

    @Test
    @DisplayName("jsonToNormalizedTables should create main and exploded array tables")
    void testJsonToNormalizedTables() throws IOException {
        // Arrange
        Path schemaFile = createTestFile("schemas/order.avsc", orderSchema);
        Path dataFile = createTestFile("data/orders.json", orderJsonData);

        // Act
        Map<String, Dataset<Row>> tables = NexusPiercerPatterns.jsonToNormalizedTables(spark, schemaFile.toString(), dataFile.toString(), "items", "notes");

        // Assert
        assertThat(tables.keySet()).containsExactlyInAnyOrder("main", "items", "notes");

        // Assert main table
        Dataset<Row> mainTable = tables.get("main");
        assertThat(mainTable.count()).isEqualTo(2L);
        assertThat(mainTable.columns()).contains("orderId", "customer_id", "customer_email");
        assertThat(mainTable.columns()).doesNotContain("items", "notes", "items_productId", "items_quantity");

        // Assert primary exploded table
        Dataset<Row> itemsTable = tables.get("items");
        assertThat(itemsTable.count()).isEqualTo(3L); // 2 items in first order, 1 in second
        assertThat(itemsTable.columns()).contains("orderId", "items_productId", "items_quantity");
        assertThat(itemsTable.select("items_quantity").collectAsList().stream().mapToInt(r -> r.getInt(0)).sum()).isEqualTo(6);

        // Assert secondary exploded table
        Dataset<Row> notesTable = tables.get("notes");
        assertThat(notesTable.count()).isEqualTo(1L); // Only first order has notes
        assertThat(notesTable.columns()).contains("orderId", "notes");
        assertThat(notesTable.first().getString(notesTable.schema().fieldIndex("notes"))).isEqualTo("gift wrap");
    }

    @Test
    @DisplayName("generateDataQualityReport should produce correct metrics")
    void testGenerateDataQualityReport() throws IOException {
        // Arrange
        Path schemaFile = createTestFile("schemas/product.avsc", productSchema);
        Path dataFile = createTestFile("data/products.json", productJsonData);

        // Act
        Dataset<Row> report = NexusPiercerPatterns.generateDataQualityReport(spark, schemaFile.toString(), dataFile.toString());
        report.show(false);

        // Assert
        assertThat(report.count()).isGreaterThanOrEqualTo(1);
        Row validRow = report.filter("is_valid = true").first();

        assertThat(validRow.getLong(validRow.schema().fieldIndex("record_count"))).isEqualTo(3);
        assertThat(validRow.getLong(validRow.schema().fieldIndex("total_records"))).isEqualTo(4);
        assertThat(validRow.getLong(validRow.schema().fieldIndex("schema_valid_records"))).isEqualTo(2);

        Row invalidRow = report.filter("is_valid = false").first();
        assertThat(invalidRow.getLong(invalidRow.schema().fieldIndex("record_count"))).isEqualTo(1);
        assertThat((List<Object>)invalidRow.getList(invalidRow.schema().fieldIndex("unique_errors"))).isNotEmpty();
    }

    @Test
    @DisplayName("checkSchemaCompatibility should return true for compatible changes and false for incompatible")
    void testCheckSchemaCompatibility() throws IOException {
        // Arrange
        Path oldSchemaFile = createTestFile("schemas/product_v1.avsc", productSchema);
        Path newSchemaFile = createTestFile("schemas/product_v2.avsc", productSchemaV2);
        Path incompatibleSchemaFile = createTestFile("schemas/product_incompatible.avsc", productSchemaIncompatible);
        Path dataFile = createTestFile("data/products.json", productJsonData);

        // Act
        boolean isCompatible = NexusPiercerPatterns.checkSchemaCompatibility(spark, oldSchemaFile.toString(), newSchemaFile.toString(), dataFile.toString());
        boolean isNotCompatible = NexusPiercerPatterns.checkSchemaCompatibility(spark, oldSchemaFile.toString(), incompatibleSchemaFile.toString(), dataFile.toString());
        boolean isFailing = NexusPiercerPatterns.checkSchemaCompatibility(spark, "nonexistent.avsc", "nonexistent.avsc", dataFile.toString());

        // Assert
        assertThat(isCompatible).isTrue();
        assertThat(isNotCompatible).isFalse();
        assertThat(isFailing).isFalse();
    }

    @Test
    @DisplayName("profileJsonStructure should infer fields and types from raw JSON")
    void testProfileJsonStructure() throws IOException {
        // Arrange
        Path dataFile = createTestFile("data/products.json", productJsonData);

        // Act
        Dataset<Row> profile = NexusPiercerPatterns.profileJsonStructure(spark, dataFile.toString(), 100);
        profile.show(false);

        // Assert
        assertThat(profile.count()).isPositive();

        Map<String, Row> profileMap = new java.util.HashMap<>();
        profile.collectAsList().forEach(r -> profileMap.put(r.getString(r.schema().fieldIndex("field")), r));

        assertThat(profileMap.containsKey("id")).isTrue();
        assertThat(profileMap.get("id").getLong(profileMap.get("id").schema().fieldIndex("occurrences"))).isEqualTo(3);

        assertThat(profileMap.containsKey("tags")).isTrue();
        assertThat(profileMap.get("tags").getBoolean(profileMap.get("tags").schema().fieldIndex("likely_array"))).isTrue();

        assertThat(profileMap.containsKey("tags_count")).isTrue();
        assertThat(profileMap.get("tags_count").getString(profileMap.get("tags_count").schema().fieldIndex("field_type"))).isEqualTo("array_count");
    }
}
