//package io.github.pierce.spark;
//
//import org.apache.spark.sql.AnalysisException;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.json.JSONObject;
//import org.junit.jupiter.api.*;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//
//import static io.github.pierce.spark.NexusPiercerFunctions.*;
//import static org.apache.spark.sql.functions.col;
//import static org.apache.spark.sql.functions.explode;
//import static org.apache.spark.sql.functions.lit;
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.assertj.core.api.Assertions.assertThatThrownBy;
//
//
//
//class NexusPiercerFunctionsTest {
//
//    private SparkSession spark;
//    private Dataset<Row> df;
//
//
//    private final String complexJson = "{\"id\":1,\"user\":{\"name\":\"Alice\",\"email\":\"alice@example.com\"},\"tags\":[\"dev\",\"java\"],\"scores\":{\"history\":[100,95,95],\"math\":98}}";
//    private final String simpleJson = "{\"a\":\"hello\",\"b\":123}";
//    private final String invalidJson = "{ a: 'hello' }";
//    private final String jsonWithNulls = "{\"id\":2,\"user\":null,\"tags\":[\"qa\",null]}";
//    private final String emptyJson = "{}";
//    private final String emptyArrayJson = "{\"id\":3,\"tags\":[]}";
//    private final String jsonArrayString = "[\"one\", \"two\"]";
//
//
//
//    @BeforeEach
//    void setup() {
//        spark = SparkSession.builder()
//                .appName("NexusPiercerFunctionsTest")
//                .master("local[*]")
//                .config("spark.sql.ui.enabled", "false")
//                .getOrCreate();
//        spark.sparkContext().setLogLevel("WARN");
//
//        List<String> jsonData = Arrays.asList(
//                complexJson,
//                simpleJson,
//                invalidJson,
//                jsonWithNulls,
//                emptyJson,
//                emptyArrayJson,
//                jsonArrayString,
//                null
//        );
//        df = spark.createDataset(jsonData, Encoders.STRING()).toDF("json");
//    }
//
//
//    @AfterEach
//    void tearDown() {
//        if (spark != null) {
//            spark.stop();
//        }
//    }
//
//    @Nested
//    @DisplayName("Flattening Functions")
//    class FlatteningFunctionsTest {
//        @Test
//        void flattenJson_shouldFlattenWithDefaultDelimiter() {
//            Dataset<Row> result = df.withColumn("flattened", flattenJson(col("json")));
//            String flattened = result.filter(col("json").equalTo(complexJson)).select("flattened").as(Encoders.STRING()).first();
//
//            JSONObject obj = new JSONObject(flattened);
//            assertThat(obj.getString("user_name")).isEqualTo("Alice");
//            assertThat(obj.getString("tags")).isEqualTo("dev,java");
//        }
//
//        @Test
//        void flattenJsonWithDelimiter_shouldFlattenWithCustomDelimiter() {
//            Dataset<Row> result = df.withColumn("flattened", flattenJson(col("json"), "|"));
//            String flattened = result.filter(col("json").equalTo(complexJson)).select("flattened").as(Encoders.STRING()).first();
//
//            JSONObject obj = new JSONObject(flattened);
//            assertThat(obj.getString("tags")).isEqualTo("dev|java");
//        }
//
//        @Test
//        void flattenJsonWithStatistics_shouldIncludeArrayStats() {
//            Dataset<Row> result = df.withColumn("flattened", flattenJsonWithStatistics(col("json")));
//            String flattened = result.filter(col("json").equalTo(complexJson)).select("flattened").as(Encoders.STRING()).first();
//
//            JSONObject obj = new JSONObject(flattened);
//            assertThat(obj.getLong("tags_count")).isEqualTo(2L);
//            assertThat(obj.getLong("scores_history_distinct_count")).isEqualTo(2L);
//        }
//    }
//
//    @Nested
//    @DisplayName("Array Functions")
//    class ArrayFunctionsTest {
//        @Test
//        void extractJsonArray_shouldExtractDelimitedString() {
//            Dataset<Row> result = df.withColumn("extracted", extractArray(col("json"), "tags"));
//            String extracted = result.filter(col("json").equalTo(complexJson)).select("extracted").as(Encoders.STRING()).first();
//            assertThat(extracted).isEqualTo("dev,java");
//        }
//
//        @Test
//        void jsonArrayCount_shouldReturnCorrectCount() {
//            Dataset<Row> result = df.withColumn("count", arrayCount(col("json"), "scores.history"));
//            Long count = result.filter(col("json").equalTo(complexJson)).select("count").as(Encoders.LONG()).first();
//            assertThat(count).isEqualTo(3L);
//        }
//
//        @Test
//        void jsonArrayCount_shouldReturnNullForNonArray() {
//            Dataset<Row> result = df.withColumn("count", arrayCount(col("json"), "user.name"));
//            Row row = result.filter(col("json").equalTo(complexJson)).select("count").first();
//            assertThat(row.isNullAt(0)).isTrue();
//        }
//
//        @Test
//        void jsonArrayDistinctCount_shouldReturnCorrectDistinctCount() {
//            Dataset<Row> result = df.withColumn("distinct_count", arrayDistinctCount(col("json"), "scores.history"));
//            Long count = result.filter(col("json").equalTo(complexJson)).select("distinct_count").as(Encoders.LONG()).first();
//            assertThat(count).isEqualTo(2L);
//        }
//
//
//        @Test
//        void explodeJsonArray_shouldCreateArrayForExplosion() {
//
//            Dataset<Row> result = df.filter(col("json").equalTo(complexJson))
//                    .withColumn("exploded_items", explode(explodeJsonArray.apply(col("json"), lit("tags"))));
//
//            assertThat(result.count()).isEqualTo(2L);
//            List<String> items = result.select("exploded_items").as(Encoders.STRING()).collectAsList();
//
//
//            assertThat(items).allMatch(s -> s.contains("\"user_name\":\"Alice\""));
//
//            assertThat(items.get(0)).contains("\"tags\":\"dev\"");
//            assertThat(items.get(1)).contains("\"tags\":\"java\"");
//        }
//    }
//
//    @Nested
//    @DisplayName("Validation Functions")
//    class ValidationFunctionsTest {
//
//        @Test
//        void isValid_shouldCorrectlyIdentifyValidity() {
//            Dataset<Row> result = df.withColumn("is_valid", isValid(col("json")));
//
//            Map<String, Boolean> validityMap = result.select("json", "is_valid")
//                    .filter(col("json").isNotNull())
//                    .as(Encoders.tuple(Encoders.STRING(), Encoders.BOOLEAN()))
//                    .collectAsList().stream()
//                    .collect(Collectors.toMap(t -> t._1(), t -> t._2()));
//
//            assertThat(validityMap.get(complexJson)).isTrue();
//            assertThat(validityMap.get(invalidJson)).isFalse();
//            assertThat(validityMap.get(emptyJson)).isTrue();
//            assertThat(validityMap.get(jsonArrayString)).isTrue();
//
//            Boolean nullIsValid = result.filter(col("json").isNull()).select("is_valid").as(Encoders.BOOLEAN()).first();
//            assertThat(nullIsValid).isFalse();
//        }
//
//
//
//        @Test
//        void jsonError_shouldReturnMessagesForInvalidJson() {
//            Dataset<Row> result = df.withColumn("error_msg", jsonError(col("json")));
//
//            // This assertion is correct and should remain
//            String validError = result.filter(col("json").equalTo(complexJson)).select("error_msg").as(Encoders.STRING()).first();
//            assertThat(validError).isEqualTo("");
//
//            String invalidError = result.filter(col("json").equalTo(invalidJson)).select("error_msg").as(Encoders.STRING()).first();
//
//            // --- FIX IS HERE ---
//            // Update the assertion to match the actual, more accurate error message from Jackson.
//            assertThat(invalidError).isNotNull().contains("was expecting double-quote to start field name");
//
//            // This assertion is also correct and should remain
//            String nullError = result.filter(col("json").isNull()).select("error_msg").as(Encoders.STRING()).first();
//            assertThat(nullError).isEqualTo("JSON is null");
//        }
//    }
//
//    @Nested
//    @DisplayName("Field Extraction Functions")
//    class FieldExtractionTest {
//        @Test
//        void extractField_shouldExtractNestedValue() {
//            Dataset<Row> result = df.withColumn("user_name", extractField(col("json"), "user.name"));
//
//            String name = result.filter(col("json").equalTo(complexJson)).select("user_name").as(Encoders.STRING()).first();
//            assertThat(name).isEqualTo("Alice");
//        }
//
//        @Test
//        void extractField_shouldReturnNullForMissingPath() {
//            Dataset<Row> result = df.withColumn("missing", extractField(col("json"), "user.nonexistent"));
//            Row row = result.filter(col("json").equalTo(complexJson)).select("missing").first();
//            assertThat(row.isNullAt(0)).isTrue();
//        }
//    }
//
//    @Nested
//    @DisplayName("SQL Registration")
//    class SqlRegistrationTest {
//
//        @BeforeEach
//        void setupView() {
//            df.createOrReplaceTempView("test_data");
//        }
//
//        @AfterEach
//        void teardownView() {
//            // The view is associated with the session, which is destroyed anyway,
//            // but this is good practice if the session were to be reused.
//            spark.catalog().dropTempView("test_data");
//        }
//
//        @Test
//        void registerAll_shouldMakeAllFunctionsAvailableInSql() {
//            registerAll(spark);
//
//            Dataset<Row> result = spark.sql("SELECT " +
//                    "flatten_json(json) as flat, " +
//                    "json_array_count(json, 'tags') as tag_count " +
//                    "FROM test_data WHERE json = '" + complexJson + "'");
//
//            assertThat(result.count()).isEqualTo(1);
//            Row row = result.first();
//            assertThat(row.getString(0)).contains("\"user_name\":\"Alice\"");
//            assertThat(row.getLong(1)).isEqualTo(2L);
//        }
//
//        // Inside NexusPiercerFunctionsTest.java -> SqlRegistrationTest class
//
//        @Test
//        void register_shouldMakeSpecificFunctionsAvailable() {
//            register(spark, "is_valid_json", "extract_nested_field");
//
//            Dataset<Row> result = spark.sql("SELECT " +
//                    "is_valid_json(json) as is_valid, " +
//                    "extract_nested_field(json, 'user.name') as name " +
//                    "FROM test_data WHERE json = '" + complexJson + "'");
//
//            assertThat(result.first().getBoolean(0)).isTrue();
//            assertThat(result.first().getString(1)).isEqualTo("Alice");
//
//            // --- FIX IS HERE ---
//            // Make the assertion more robust to changes in Spark error messages.
//            // Check for the error code and the function name.
//            assertThatThrownBy(() -> spark.sql("SELECT flatten_json(json) FROM test_data"))
//                    .isInstanceOf(AnalysisException.class)
//                    .hasMessageContaining("UNRESOLVED_ROUTINE")
//                    .hasMessageContaining("`flatten_json`"); // Using backticks as seen in the error
//        }
//    }
//}