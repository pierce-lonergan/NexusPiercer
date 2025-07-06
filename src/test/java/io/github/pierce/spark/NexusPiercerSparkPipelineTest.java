package io.github.pierce.spark;

import io.github.pierce.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.*;
import org.apache.avro.Schema;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.apache.spark.sql.functions.*;

/**
 * Unit tests for NexusPiercerSparkPipeline
 */
class NexusPiercerSparkPipelineTest extends SparkTestBase {

    private static final String TEST_SCHEMA_JSON = """
        {
          "type": "record",
          "name": "TestRecord",
          "namespace": "com.test",
          "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "value", "type": "double"},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
            {
              "name": "metadata",
              "type": {
                "type": "record",
                "name": "Metadata",
                "fields": [
                  {"name": "source", "type": "string"},
                  {"name": "timestamp", "type": "long"}
                ]
              }
            }
          ]
        }
        """;

    private Schema testSchema;

    @BeforeEach
    void setUp() {
        testSchema = new Schema.Parser().parse(TEST_SCHEMA_JSON);
        NexusPiercerSparkPipeline.clearSchemaCache();
    }

    @Test
    @DisplayName("Should process simple JSON with basic configuration")
    void testBasicProcessing() {
        // Create test data
        List<String> jsonData = Arrays.asList(
                """
                {
                  "id": "1",
                  "name": "Test Item 1",
                  "value": 99.99,
                  "tags": ["tag1", "tag2", "tag3"],
                  "metadata": {
                    "source": "test",
                    "timestamp": 1234567890
                  }
                }
                """,
                """
                {
                  "id": "2",
                  "name": "Test Item 2",
                  "value": 149.99,
                  "tags": ["tag2", "tag4"],
                  "metadata": {
                    "source": "test",
                    "timestamp": 1234567900
                  }
                }
                """
        );

        Dataset<String> jsonDs = spark.createDataset(jsonData, org.apache.spark.sql.Encoders.STRING());

        // Process with pipeline
        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(testSchema)
                .enableArrayStatistics()
                .processDataset(jsonDs);

        // Verify results
        Dataset<Row> processed = result.getDataset();
        assertThat(processed.count()).isEqualTo(2);

        // Check schema
        List<String> columns = Arrays.asList(processed.columns());
        assertThat(columns).contains("id", "name", "value", "tags", "metadata_source", "metadata_timestamp");
        assertThat(columns).contains("tags_count", "tags_distinct_count"); // Array statistics

        // Check data
        Row firstRow = processed.filter(col("id").equalTo("1")).first();
        assertThat(firstRow.<String>getAs("name")).isEqualTo("Test Item 1");
        assertThat(firstRow.<Double>getAs("value")).isEqualTo(99.99);
        assertThat(firstRow.<String>getAs("tags")).isEqualTo("tag1,tag2,tag3");
        assertThat(firstRow.<Long>getAs("tags_count")).isEqualTo(3L);
        assertThat(firstRow.<String>getAs("metadata_source")).isEqualTo("test");
    }

    @Test
    @DisplayName("Should handle malformed JSON based on error handling strategy")
    void testErrorHandling() {
        List<String> jsonData = Arrays.asList(
                "{\"id\": \"1\", \"name\": \"Valid\", \"value\": 10.0, \"tags\": [], \"metadata\": {\"source\": \"test\", \"timestamp\": 123}}",
                "{invalid json}",
                "{\"id\": \"3\"}" // Missing required fields but valid JSON
        );

        Dataset<String> jsonDs = spark.createDataset(jsonData, org.apache.spark.sql.Encoders.STRING());

        // Test SKIP_MALFORMED - should skip the malformed JSON
        NexusPiercerSparkPipeline.ProcessingResult skipResult = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(testSchema)
                .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.SKIP_MALFORMED)
                .processDataset(jsonDs);

        // Should have 1 or 2 records (depending on whether the incomplete JSON validates against schema)
        long skipCount = skipResult.getDataset().count();
        assertThat(skipCount).isGreaterThanOrEqualTo(1);
        assertThat(skipCount).isLessThanOrEqualTo(2);

        // Test QUARANTINE - should separate good and bad records
        NexusPiercerSparkPipeline.ProcessingResult quarantineResult = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(testSchema)
                .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.QUARANTINE)
                .includeRawJson()
                .processDataset(jsonDs);

        // Check the datasets
        Dataset<Row> goodDs = quarantineResult.getDataset();
        Dataset<Row> errorDs = quarantineResult.getErrorDataset();

        assertThat(errorDs).isNotNull();

        long goodCount = goodDs.count();
        long errorCount = errorDs.count();

        // Total should be 3
        assertThat(goodCount + errorCount).isEqualTo(3);
        // At least the malformed JSON should be in error dataset
        assertThat(errorCount).isGreaterThanOrEqualTo(1);
        // Verify the fully valid record is in the good dataset
        List<Row> goodRows = goodDs.collectAsList();
        boolean foundValidRecord = goodRows.stream()
                .anyMatch(row -> "1".equals(row.<String>getAs("id")) &&
                        "Valid".equals(row.<String>getAs("name")));
        assertThat(foundValidRecord).isTrue();

        // Verify the malformed JSON is in the error dataset
        List<Row> errorRows = errorDs.collectAsList();
        boolean foundMalformed = errorRows.stream()
                .anyMatch(row -> {
                    String rawJson = row.<String>getAs("_raw_json");
                    String error = row.<String>getAs("_error");
                    return "{invalid json}".equals(rawJson) && "Malformed JSON string".equals(error);
                });
        assertThat(foundMalformed).isTrue();
    }

    @Test
    @DisplayName("Should explode arrays correctly")
    void testArrayExplosion() {
        String jsonWithArray = """
            {
              "id": "1",
              "name": "Multi-tag item",
              "value": 50.0,
              "tags": ["electronics", "computers", "laptops"],
              "metadata": {
                "source": "catalog",
                "timestamp": 1234567890
              }
            }
            """;

        Dataset<String> jsonDs = spark.createDataset(
                Arrays.asList(jsonWithArray),
                org.apache.spark.sql.Encoders.STRING()
        );

        // Process with explosion
        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(testSchema)
                .explodeArrays("tags")
                .processDataset(jsonDs);

        Dataset<Row> exploded = result.getDataset();

        // Should have 3 rows (one per tag)
        assertThat(exploded.count()).isEqualTo(3);

        // Each row should have the same base data but different tag
        List<Row> rows = exploded.orderBy("tags").collectAsList();
        assertThat(rows.get(0).<String>getAs("tags")).isEqualTo("computers");
        assertThat(rows.get(1).<String>getAs("tags")).isEqualTo("electronics");
        assertThat(rows.get(2).<String>getAs("tags")).isEqualTo("laptops");

        // All rows should have same id
        assertThat(exploded.select("id").distinct().count()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should disable array statistics when configured")
    void testDisableArrayStatistics() {
        String json = """
            {
              "id": "1",
              "name": "Test",
              "value": 10.0,
              "tags": ["a", "b", "c"],
              "metadata": {"source": "test", "timestamp": 123}
            }
            """;

        Dataset<String> jsonDs = spark.createDataset(
                Arrays.asList(json),
                org.apache.spark.sql.Encoders.STRING()
        );

        // Process without statistics
        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(testSchema)
                .disableArrayStatistics()
                .processDataset(jsonDs);

        List<String> columns = Arrays.asList(result.getDataset().columns());
        assertThat(columns).contains("tags");
        assertThat(columns).doesNotContain("tags_count", "tags_distinct_count");
    }

    @Test
    @DisplayName("Should include metadata columns when configured")
    void testMetadataColumns() {
        String json = "{\"id\": \"1\", \"name\": \"Test\", \"value\": 10.0, \"tags\": [], \"metadata\": {\"source\": \"test\", \"timestamp\": 123}}";

        Dataset<String> jsonDs = spark.createDataset(
                Arrays.asList(json),
                org.apache.spark.sql.Encoders.STRING()
        );

        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(testSchema)
                .includeMetadata()
                .processDataset(jsonDs);

        List<String> columns = Arrays.asList(result.getDataset().columns());
        assertThat(columns).contains("_processing_time");
    }

    @Test
    @DisplayName("Should keep raw JSON when configured")
    void testIncludeRawJson() {
        String json = "{\"id\": \"1\", \"name\": \"Test\", \"value\": 10.0, \"tags\": [], \"metadata\": {\"source\": \"test\", \"timestamp\": 123}}";

        Dataset<String> jsonDs = spark.createDataset(
                Arrays.asList(json),
                org.apache.spark.sql.Encoders.STRING()
        );

        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(testSchema)
                .includeRawJson()
                .processDataset(jsonDs);

        List<String> columns = Arrays.asList(result.getDataset().columns());
        assertThat(columns).contains("_raw_json");

        Row row = result.getDataset().first();
        assertThat(row.<String>getAs("_raw_json")).contains("\"id\": \"1\"");
    }

    @Test
    @DisplayName("Should use custom array delimiter")
    void testCustomDelimiter() {
        String json = """
            {
              "id": "1",
              "name": "Test",
              "value": 10.0,
              "tags": ["tag1", "tag2", "tag3"],
              "metadata": {"source": "test", "timestamp": 123}
            }
            """;

        Dataset<String> jsonDs = spark.createDataset(
                Arrays.asList(json),
                org.apache.spark.sql.Encoders.STRING()
        );

        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(testSchema)
                .withArrayDelimiter("|")
                .processDataset(jsonDs);

        Row row = result.getDataset().first();
        assertThat(row.<String>getAs("tags")).isEqualTo("tag1|tag2|tag3");
    }

    @Test
    @DisplayName("Should validate configuration")
    void testConfigurationValidation() {
        NexusPiercerSparkPipeline pipeline = NexusPiercerSparkPipeline.forBatch(spark);

        // Should throw when no schema is set
        assertThatThrownBy(() -> pipeline.validateConfiguration())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No schema configured");

        // Should throw for invalid settings
        assertThatThrownBy(() ->
                pipeline.withSchema(testSchema)
                        .withMaxNestingDepth(0)
                        .validateConfiguration()
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should cache schemas for performance")
    void testSchemaCache() {
        String json = "{\"id\": \"1\", \"name\": \"Test\", \"value\": 10.0, \"tags\": [], \"metadata\": {\"source\": \"test\", \"timestamp\": 123}}";
        Dataset<String> jsonDs = spark.createDataset(
                Arrays.asList(json),
                org.apache.spark.sql.Encoders.STRING()
        );

        // First call should cache
        assertThat(NexusPiercerSparkPipeline.getSchemaCacheSize()).isEqualTo(0);

        NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(testSchema)
                .processDataset(jsonDs);

        assertThat(NexusPiercerSparkPipeline.getSchemaCacheSize()).isEqualTo(1);

        // Second call should use cache
        NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(testSchema)
                .processDataset(jsonDs);

        assertThat(NexusPiercerSparkPipeline.getSchemaCacheSize()).isEqualTo(1);

        // Clear cache
        NexusPiercerSparkPipeline.clearSchemaCache();
        assertThat(NexusPiercerSparkPipeline.getSchemaCacheSize()).isEqualTo(0);
    }

    @Test
    @DisplayName("Should collect metrics for batch processing")
    void testMetrics() {
        List<String> jsonData = Arrays.asList(
                "{\"id\": \"1\", \"name\": \"Test1\", \"value\": 10.0, \"tags\": [\"a\"], \"metadata\": {\"source\": \"test\", \"timestamp\": 123}}",
                "{\"id\": \"2\", \"name\": \"Test2\", \"value\": 20.0, \"tags\": [\"b\"], \"metadata\": {\"source\": \"test\", \"timestamp\": 124}}"
        );

        Dataset<String> jsonDs = spark.createDataset(jsonData, org.apache.spark.sql.Encoders.STRING());

        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(testSchema)
                .enableMetrics()
                .processDataset(jsonDs);

        NexusPiercerSparkPipeline.ProcessingMetrics metrics = result.getMetrics();
        assertThat(metrics.getTotalRecords()).isEqualTo(2);
        assertThat(metrics.getSuccessfulRecords()).isEqualTo(2);
        assertThat(metrics.getSuccessRate()).isEqualTo(1.0);
        assertThat(metrics.getProcessingTimeMs()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Should handle complex nested JSON")
    void testComplexNestedJson() {
        String complexJson = """
            {
              "id": "complex-1",
              "name": "Complex Item",
              "value": 299.99,
              "tags": ["electronics", "premium", "featured"],
              "metadata": {
                "source": "import",
                "timestamp": 1234567890
              },
              "details": {
                "manufacturer": "TechCorp",
                "model": "X2000",
                "specs": {
                  "weight": 2.5,
                  "dimensions": {
                    "length": 30,
                    "width": 20,
                    "height": 5
                  }
                }
              },
              "reviews": [
                {
                  "user": "user1",
                  "rating": 5,
                  "comment": "Excellent!"
                },
                {
                  "user": "user2",
                  "rating": 4,
                  "comment": "Good value"
                }
              ]
            }
            """;

        // Add the nested fields to schema
        String extendedSchemaJson = """
            {
              "type": "record",
              "name": "ExtendedRecord",
              "namespace": "com.test",
              "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "value", "type": "double"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {
                  "name": "metadata",
                  "type": {
                    "type": "record",
                    "name": "Metadata",
                    "fields": [
                      {"name": "source", "type": "string"},
                      {"name": "timestamp", "type": "long"}
                    ]
                  }
                },
                {
                  "name": "details",
                  "type": {
                    "type": "record",
                    "name": "Details",
                    "fields": [
                      {"name": "manufacturer", "type": "string"},
                      {"name": "model", "type": "string"},
                      {
                        "name": "specs",
                        "type": {
                          "type": "record",
                          "name": "Specs",
                          "fields": [
                            {"name": "weight", "type": "double"},
                            {
                              "name": "dimensions",
                              "type": {
                                "type": "record",
                                "name": "Dimensions",
                                "fields": [
                                  {"name": "length", "type": "int"},
                                  {"name": "width", "type": "int"},
                                  {"name": "height", "type": "int"}
                                ]
                              }
                            }
                          ]
                        }
                      }
                    ]
                  }
                },
                {
                  "name": "reviews",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Review",
                      "fields": [
                        {"name": "user", "type": "string"},
                        {"name": "rating", "type": "int"},
                        {"name": "comment", "type": "string"}
                      ]
                    }
                  }
                }
              ]
            }
            """;

        Schema extendedSchema = new Schema.Parser().parse(extendedSchemaJson);
        Dataset<String> jsonDs = spark.createDataset(
                Arrays.asList(complexJson),
                org.apache.spark.sql.Encoders.STRING()
        );

        NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
                .withSchema(extendedSchema)
                .enableArrayStatistics()
                .processDataset(jsonDs);

        Row row = result.getDataset().first();

        // Check flattened nested fields
        assertThat(row.<String>getAs("details_manufacturer")).isEqualTo("TechCorp");
        assertThat(row.<Double>getAs("details_specs_weight")).isEqualTo(2.5);
        assertThat(row.<Integer>getAs("details_specs_dimensions_length")).isEqualTo(30);

        // Check array consolidation
        assertThat(row.<String>getAs("reviews_user")).isEqualTo("user1,user2");
        assertThat(row.<String>getAs("reviews_rating")).isEqualTo("5,4");
    }
}