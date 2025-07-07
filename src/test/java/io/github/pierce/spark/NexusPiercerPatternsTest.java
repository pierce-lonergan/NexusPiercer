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
            "{\"id\": \"p3\", \"category\": \"electronics\", \"price\": \"not-a-double\", \"tags\": []}",
            "{malformed-json}"
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

        System.setProperty("hadoop.home.dir", System.getProperty("user.dir"));

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
    @DisplayName("generateDataQualityReport should produce correct metrics")
    void testGenerateDataQualityReport() throws IOException {

        Path schemaFile = createTestFile("schemas/product.avsc", productSchema);
        Path dataFile = createTestFile("data/products.json", productJsonData);


        Dataset<Row> report = NexusPiercerPatterns.generateDataQualityReport(spark, schemaFile.toString(), dataFile.toString());
        report.show(false);


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
