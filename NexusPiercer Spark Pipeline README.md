# NexusPiercer Spark Pipeline

A powerful, intuitive abstraction layer for processing nested JSON data with Avro schemas in Apache Spark. This library combines the JSON flattening capabilities of NexusPiercer with Spark's distributed processing power.

## 🚀 Key Features

- **Unified API** for batch and streaming JSON processing
- **Automatic JSON flattening** with array consolidation
- **Avro schema validation** with automatic type conversion
- **Array explosion** for data normalization
- **Built-in error handling** strategies
- **Performance optimizations** with schema caching
- **SQL functions** for ad-hoc processing
- **Pre-built patterns** for common use cases

## 📋 Table of Contents

- [Quick Start](#quick-start)
- [Core Components](#core-components)
- [Usage Examples](#usage-examples)
- [Configuration Options](#configuration-options)
- [SQL Functions](#sql-functions)
- [Common Patterns](#common-patterns)
- [Best Practices](#best-practices)
- [Performance Tuning](#performance-tuning)

## Quick Start

### Basic Batch Processing

```java
import io.github.pierce.spark.NexusPiercerSparkPipeline;
import static io.github.pierce.spark.NexusPiercerSparkPipeline.*;

// Process JSON files with schema validation
ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
    .withSchema("user_schema.avsc")
    .enableArrayStatistics()
    .process("data/users/*.json");

// Write to Parquet
result.write()
    .mode(SaveMode.Overwrite)
    .partitionBy("date")
    .parquet("output/users");

// Check metrics
System.out.println(result.getMetrics());
```

### Streaming from Kafka

```java
Map<String, String> kafkaOptions = Map.of(
    "kafka.bootstrap.servers", "localhost:9092",
    "subscribe", "events-topic"
);

StreamingQuery query = NexusPiercerSparkPipeline.forStreaming(spark)
    .withSchema("event_schema.avsc")
    .processStream("kafka", kafkaOptions)
    .writeStream()
    .format("delta")
    .option("checkpointLocation", "/checkpoints/events")
    .start();
```

## Core Components

### 1. NexusPiercerSparkPipeline

The main entry point for all pipeline operations. Provides a fluent API for configuration and processing.

**Key Methods:**
- `forBatch(SparkSession)` - Create batch pipeline
- `forStreaming(SparkSession)` - Create streaming pipeline
- `withSchema(String/Schema)` - Set Avro schema
- `process(String...)` - Process input files
- `processStream(String, Map)` - Process streaming source

### 2. NexusPiercerFunctions

Spark SQL functions for JSON processing that can be used independently.

```java
import static io.github.pierce.spark.NexusPiercerFunctions.*;

// Register functions for SQL
NexusPiercerFunctions.registerAll(spark);

// Use in DataFrame API
df.withColumn("flattened", flattenJson(col("json")))
  .withColumn("tag_count", arrayCount(col("json"), "tags"));

// Use in SQL
spark.sql("""
    SELECT 
        flatten_json(json_data) as flattened,
        json_array_count(json_data, 'items') as item_count
    FROM json_table
""");
```

### 3. NexusPiercerPatterns

Pre-configured pipelines for common use cases.

```java
// JSON to Delta Lake
NexusPiercerPatterns.jsonToDelta(spark, 
    "schema.avsc", 
    "input/*.json", 
    "delta/table");

// Generate data quality report
Dataset<Row> report = NexusPiercerPatterns.generateDataQualityReport(
    spark, "schema.avsc", "input/*.json");
```

## Usage Examples

### Array Explosion for Normalization

```java
// Original JSON has nested arrays
// {"orderId": "123", "items": [{"sku": "A", "qty": 1}, {"sku": "B", "qty": 2}]}

ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
    .withSchema("order_schema.avsc")
    .explodeArrays("items")  // Creates one row per array element
    .process("orders/*.json");

// Result: Normalized table with one row per item
// orderId | items_sku | items_qty | _explosion_index
// 123     | A         | 1         | 0
// 123     | B         | 2         | 1
```

### Error Handling Strategies

```java
// Quarantine bad records
ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
    .withSchema("schema.avsc")
    .withErrorHandling(ErrorHandling.QUARANTINE)
    .process("input/*.json");

Dataset<Row> goodRecords = result.getDataset();
Dataset<Row> badRecords = result.getErrorDataset();

// Write both
goodRecords.write().parquet("output/processed");
badRecords.write().json("output/errors");
```

### Custom Configuration

```java
ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
    .withSchema("schema.avsc")
    .withArrayDelimiter("|")          // Custom delimiter
    .withNullPlaceholder("NULL")      // Custom null value
    .withMaxNestingDepth(10)          // Limit nesting
    .withMaxArraySize(500)            // Limit array size
    .enableMatrixDenotors()           // Include array indices
    .includeMetadata()                // Add processing metadata
    .includeRawJson()                 // Keep original JSON
    .withRepartition(20)              // Optimize partitions
    .process("input/*.json");
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `arrayDelimiter` | `","` | Delimiter for concatenated array values |
| `nullPlaceholder` | `null` | Replacement for null values |
| `maxNestingDepth` | `50` | Maximum JSON nesting depth |
| `maxArraySize` | `1000` | Maximum array elements to process |
| `includeArrayStatistics` | `true` | Generate array statistics (_count, _distinct_count, etc.) |
| `consolidateWithMatrixDenotors` | `false` | Include array indices in values |
| `errorHandling` | `SKIP_MALFORMED` | Strategy: FAIL_FAST, SKIP_MALFORMED, QUARANTINE, PERMISSIVE |
| `enableMetrics` | `true` | Collect processing metrics |
| `cacheSchemas` | `true` | Cache parsed schemas |
| `preserveDots` | `false` | Keep dots in field names (vs underscores) |
| `includeMetadata` | `false` | Add _processing_time, _input_file columns |
| `includeRawJson` | `false` | Keep original JSON in _raw_json column |

## SQL Functions

### Available Functions

| Function | Description | Example |
|----------|-------------|---------|
| `flatten_json(json)` | Flatten and consolidate JSON | `SELECT flatten_json(json_col) FROM table` |
| `flatten_json_with_delimiter(json, delim)` | Flatten with custom delimiter | `flatten_json_with_delimiter(json_col, '\|')` |
| `flatten_json_with_stats(json)` | Flatten with array statistics | `flatten_json_with_stats(json_col)` |
| `extract_json_array(json, path)` | Extract array as string | `extract_json_array(json_col, 'items.tags')` |
| `json_array_count(json, path)` | Count array elements | `json_array_count(json_col, 'items')` |
| `json_array_distinct_count(json, path)` | Count distinct elements | `json_array_distinct_count(json_col, 'tags')` |
| `explode_json_array(json, path)` | Explode array to rows | `explode_json_array(json_col, 'items')` |
| `is_valid_json(json)` | Check if JSON is valid | `WHERE is_valid_json(json_col)` |
| `get_json_error(json)` | Get parsing error message | `get_json_error(json_col)` |
| `extract_nested_field(json, path)` | Extract nested field value | `extract_nested_field(json_col, 'user.name')` |

### Registration

```java
// Register all functions
NexusPiercerFunctions.registerAll(spark);

// Register specific functions
NexusPiercerFunctions.register(spark, "flatten_json", "json_array_count");
```

## Common Patterns

### 1. ETL Pipeline

```java
// JSON → Validate → Transform → Parquet
NexusPiercerPatterns.jsonToParquet(spark,
    "schema.avsc",
    "s3://bucket/raw/*.json",
    "s3://bucket/processed/",
    SaveMode.Overwrite,
    "date"  // Partition column
);
```

### 2. Data Quality Check

```java
Dataset<Row> qualityReport = NexusPiercerPatterns.generateDataQualityReport(
    spark, "schema.avsc", "input/*.json");

qualityReport.show();
// +--------+--------+------------+-------------+
// |is_valid|is_empty|record_count|avg_json_size|
// +--------+--------+------------+-------------+
// |true    |false   |9500        |2048.5       |
// |false   |false   |500         |1024.2       |
// +--------+--------+------------+-------------+
```

### 3. Normalize to Multiple Tables

```java
Map<String, Dataset<Row>> tables = NexusPiercerPatterns.jsonToNormalizedTables(
    spark,
    "order_schema.avsc",
    "orders/*.json",
    "items",           // Primary array to explode
    "payments",        // Secondary arrays
    "shipping_events"
);

// Write each table
tables.get("main").write().parquet("orders");
tables.get("items").write().parquet("order_items");
tables.get("payments").write().parquet("order_payments");
```

### 4. Incremental Processing

```java
NexusPiercerPatterns.processIncremental(
    spark,
    "schema.avsc",
    "input/daily",
    "output/incremental",
    "2024-01-15T00:00:00"  // Last processed timestamp
);
```

## Best Practices

### 1. Schema Management

- Store schemas in version control
- Use schema registry for production
- Test schema evolution compatibility
- Document field meanings and constraints

### 2. Error Handling

- Use QUARANTINE mode for production pipelines
- Monitor error rates and patterns
- Set up alerts for schema validation failures
- Keep raw JSON for reprocessing

### 3. Performance

- Enable schema caching for repeated processing
- Use appropriate partitioning strategies
- Consider disabling statistics for large arrays
- Repartition data based on volume

### 4. Data Quality

- Validate JSON before processing
- Use data quality reports regularly
- Monitor array sizes and nesting depths
- Set appropriate limits for your data

## Performance Tuning

### Schema Caching

```java
// Caching is enabled by default
// Disable for one-time processing
.disableSchemaCache()

// Clear cache manually
NexusPiercerSparkPipeline.clearSchemaCache();
```

### Partitioning

```java
// Repartition for optimal parallelism
.withRepartition(spark.conf.get("spark.sql.shuffle.partitions"))

// Partition output by high-cardinality columns
result.write()
    .partitionBy("year", "month", "day")
    .parquet("output");
```

### Array Statistics

```java
// Disable statistics for better performance with large arrays
.disableArrayStatistics()

// Or selectively enable for specific processing
.enableArrayStatistics()  // Only when needed
```

### Batch Size (Streaming)

```java
// Configure micro-batch size
.writeStream()
.trigger(Trigger.ProcessingTime("30 seconds"))
.option("maxFilesPerTrigger", "100")
```

## Integration with Existing Pipelines

The library is designed to integrate seamlessly with existing Spark workflows:

```java
// Use with existing DataFrames
Dataset<Row> existingDf = spark.read().json("data.json");
Dataset<String> jsonStrings = existingDf.select("json_column").as(Encoders.STRING());

ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
    .withSchema("schema.avsc")
    .processDataset(jsonStrings);

// Combine with other transformations
Dataset<Row> enriched = result.getDataset()
    .join(dimensionTable, "key")
    .filter(col("status").equalTo("active"));
```

## Troubleshooting

### Common Issues

1. **Schema not found**: Ensure schema file is in classpath or provide absolute path
2. **Memory issues**: Reduce `maxArraySize` or disable array statistics
3. **Slow processing**: Enable adaptive query execution and check partitioning
4. **Type mismatches**: Check that Avro schema matches JSON structure

### Debug Mode

```java
// Enable detailed logging
spark.sparkContext().setLogLevel("DEBUG");

// Include raw JSON for debugging
.includeRawJson()
.includeMetadata()

// Check intermediate results
ProcessingResult result = pipeline.process("test.json");
result.getDataset().printSchema();
result.getDataset().show(false);
```

## Comprehensive Examples

```java
public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession.builder()
            .appName("NexusPiercer Examples")
            .master("local[*]") // <-- ADD THIS LINE
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate();

    // Choose which example to run
    String example = args.length > 0 ? args[0] : "basic";

    switch (example) {
        case "basic":
            runBasicExample(spark);
            break;
        case "streaming":
            runStreamingExample(spark);
            break;
        case "explosion":
            runExplosionExample(spark);
            break;
        case "functions":
            runFunctionsExample(spark);
            break;
        case "patterns":
            runPatternsExample(spark);
            break;
        case "advanced":
            runAdvancedExample(spark);
            break;
        default:
            System.out.println("Unknown example: " + example);
            System.out.println("Available: basic, streaming, explosion, functions, patterns, advanced");
    }

    spark.stop();
}

/**
 * Example 1: Basic JSON processing with schema validation
 */
public static void runBasicExample(SparkSession spark) {
    System.out.println("=== Basic JSON Processing Example ===");

    // Simple pipeline: JSON files → Flatten → Validate → Parquet
    NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forBatch(spark)
            .withSchema("schemas/user_activity.avsc")  // Avro schema file
            .withArrayDelimiter(",")                   // Array delimiter
            .enableArrayStatistics()                   // Include array statistics
            .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.QUARANTINE)
            .includeMetadata()                         // Add processing metadata
            .process("data/input/users/*.json");       // Input files

    // Show sample results
    System.out.println("Successfully processed records:");
    result.getDataset().show(10, false);

    // Show any errors
    if (result.getErrorDataset() != null) {
        System.out.println("\nRecords with errors:");
        result.getErrorDataset().show(5, false);
    }

    // Show metrics
    System.out.println("\nProcessing metrics:");
    System.out.println(result.getMetrics());

    // Write to Parquet
    result.write()
            .mode(SaveMode.Overwrite)
            .partitionBy("userId")
            .parquet("data/output/users_processed");
}

/**
 * Example 2: Streaming JSON from Kafka
 */
public static void runStreamingExample(SparkSession spark) throws Exception {
    System.out.println("=== Streaming JSON Processing Example ===");

    // Kafka configuration
    Map<String, String> kafkaOptions = new HashMap<>();
    kafkaOptions.put("kafka.bootstrap.servers", "localhost:9092");
    kafkaOptions.put("subscribe", "user-events");
    kafkaOptions.put("startingOffsets", "latest");
    kafkaOptions.put("failOnDataLoss", "false");

    // Streaming pipeline: Kafka → Flatten → Validate → Parquet
    NexusPiercerSparkPipeline.ProcessingResult result = NexusPiercerSparkPipeline.forStreaming(spark)
            .withSchema("schemas/events.avsc")
            .enableArrayStatistics()
            .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.SKIP_MALFORMED)
            .includeMetadata()
            .processStream("kafka", kafkaOptions);

    // Write stream to Parquet with checkpointing
    StreamingQuery query = result.writeStream()
            .outputMode("append")
            .format("parquet")
            .option("path", "data/output/events_stream")
            .option("checkpointLocation", "checkpoints/events")
            .partitionBy("event_date")
            .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("30 seconds"))
            .start();

    // Monitor for 2 minutes then stop
    Thread.sleep(120000);
    query.stop();

    System.out.println("Streaming query stopped. Last progress:");
    System.out.println(query.lastProgress());
}

/**
 * Example 3: Array explosion for normalization
 */
public static void runExplosionExample(SparkSession spark) {
    System.out.println("=== Array Explosion Example ===");

    // Sample e-commerce order data with nested arrays
    String sampleJson = """
            {
              "orderId": "ORD-12345",
              "customerId": "CUST-67890",
              "orderDate": "2024-01-15",
              "items": [
                {
                  "productId": "PROD-001",
                  "productName": "Laptop",
                  "quantity": 1,
                  "price": 999.99,
                  "categories": ["Electronics", "Computers", "Laptops"]
                },
                {
                  "productId": "PROD-002",
                  "productName": "Mouse",
                  "quantity": 2,
                  "price": 29.99,
                  "categories": ["Electronics", "Accessories"]
                }
              ],
              "shipping": {
                "address": "123 Main St",
                "city": "New York",
                "method": "EXPRESS"
              }
            }
            """;

    // Create test dataset
    Dataset<String> jsonDs = spark.createDataset(
            java.util.Arrays.asList(sampleJson),
            Encoders.STRING()
    );

    // Explode items array to create normalized order_items table
    NexusPiercerSparkPipeline.ProcessingResult itemsResult =
            NexusPiercerSparkPipeline.forBatch(spark)
                    .withSchema("schemas/order.avsc")
                    .explodeArrays("items")  // Explode items array
                    .disableArrayStatistics()
                    .processDataset(jsonDs);

    System.out.println("Exploded items (one row per item):");
    itemsResult.getDataset().show(false);

    // Create main orders table without arrays
    NexusPiercerSparkPipeline.ProcessingResult ordersResult =
            NexusPiercerSparkPipeline.forBatch(spark)
                    .withSchema("schemas/order.avsc")
                    .disableArrayStatistics()
                    .processDataset(jsonDs);

    // Remove array columns for main table
    Dataset<Row> ordersMain = ordersResult.getDataset()
            .drop("items", "items_productId", "items_productName",
                    "items_quantity", "items_price", "items_categories");

    System.out.println("\nMain orders table:");
    ordersMain.show(false);

    // Save as separate tables
    itemsResult.write().mode(SaveMode.Overwrite).parquet("data/output/order_items");
    ordersMain.write().mode(SaveMode.Overwrite).parquet("data/output/orders");
}

/**
 * Example 4: Using SQL functions
 */
public static void runFunctionsExample(SparkSession spark) {
    System.out.println("=== SQL Functions Example ===");

    // Register all functions
    NexusPiercerFunctions.registerAll(spark);

    // Create sample data
    Dataset<Row> jsonData = spark.createDataset(
            java.util.Arrays.asList(
                    "{\"name\":\"Alice\",\"scores\":[95,87,92],\"tags\":[\"student\",\"honor-roll\"]}",
                    "{\"name\":\"Bob\",\"scores\":[78,82,85],\"tags\":[\"student\"]}",
                    "{\"invalid json",
                    "{\"name\":\"Charlie\",\"scores\":[],\"tags\":null}"
            ),
            Encoders.STRING()
    ).toDF("json_string");

    // Use functions in DataFrame API
    Dataset<Row> processed = jsonData
            .withColumn("is_valid", isValid(col("json_string")))
            .withColumn("error", jsonError(col("json_string")))
            .withColumn("name", extractField(col("json_string"), "name"))
            .withColumn("scores_array", extractArray(col("json_string"), "scores"))
            .withColumn("scores_count", arrayCount(col("json_string"), "scores"))
            .withColumn("flattened", flattenJson(col("json_string")));

    System.out.println("Processed with functions:");
    processed.show(false);

    // Use in Spark SQL
    jsonData.createOrReplaceTempView("json_table");

    Dataset<Row> sqlResult = spark.sql("""
                SELECT
                    json_string,
                    is_valid_json(json_string) as is_valid,
                    extract_nested_field(json_string, 'name') as name,
                    json_array_count(json_string, 'scores') as score_count,
                    flatten_json(json_string) as flattened
                FROM json_table
                WHERE is_valid_json(json_string)
            """);

    System.out.println("\nSQL query results:");
    sqlResult.show(false);
}

/**
 * Example 5: Using pre-built patterns
 */
public static void runPatternsExample(SparkSession spark) {
    System.out.println("=== Pipeline Patterns Example ===");

    // Pattern 1: Simple JSON to Parquet ETL
    System.out.println("\n1. JSON to Parquet pattern:");
    NexusPiercerPatterns.jsonToParquet(
            spark,
            "schemas/product.avsc",
            "data/input/products/*.json",
            "data/output/products_parquet",
            SaveMode.Overwrite,
            "category"  // Partition by category
    );

    // Pattern 2: Generate data quality report
    System.out.println("\n2. Data quality report:");
    Dataset<Row> qualityReport = NexusPiercerPatterns.generateDataQualityReport(
            spark,
            "schemas/product.avsc",
            "data/input/products/*.json"
    );
    qualityReport.show(false);

    // Pattern 3: Profile JSON structure
    System.out.println("\n3. JSON structure profile:");
    Dataset<Row> profile = NexusPiercerPatterns.profileJsonStructure(
            spark,
            "data/input/products/*.json",
            100  // Sample size
    );
    profile.show(50, false);

    // Pattern 4: Normalize to multiple tables
    System.out.println("\n4. Normalization pattern:");
    Map<String, Dataset<Row>> tables = NexusPiercerPatterns.jsonToNormalizedTables(
            spark,
            "schemas/complex_order.avsc",
            "data/input/orders/*.json",
            "items",           // Primary array
            "payments",        // Secondary arrays
            "shipping_history"
    );

    tables.forEach((tableName, dataset) -> {
        System.out.println("\nTable: " + tableName);
        dataset.show(5, false);
    });
}

/**
 * Example 6: Advanced pipeline with custom processing
 */
public static void runAdvancedExample(SparkSession spark) {
    System.out.println("=== Advanced Pipeline Example ===");

    // Complex pipeline with multiple transformations
    NexusPiercerSparkPipeline pipeline = NexusPiercerSparkPipeline.forBatch(spark)
            .withSchema("schemas/transaction.avsc")
            .withArrayDelimiter("|")           // Custom delimiter
            .withNullPlaceholder("N/A")        // Custom null placeholder
            .enableArrayStatistics()
            .enableMatrixDenotors()            // Include array indices
            .withErrorHandling(NexusPiercerSparkPipeline.ErrorHandling.QUARANTINE)
            .includeMetadata()
            .includeRawJson()                  // Keep original JSON
            .withRepartition(10);              // Optimize partitions

    // Process with custom schema
    NexusPiercerSparkPipeline.ProcessingResult result = pipeline.process(
            "data/input/transactions/*.json.gz"  // Compressed files
    );

    // Apply custom transformations
    Dataset<Row> transformed = result.getDataset()
            // Add derived columns
            .withColumn("transaction_year", year(col("transaction_date")))
            .withColumn("transaction_month", month(col("transaction_date")))
            .withColumn("is_high_value", col("amount").gt(10000))

            // Clean up array fields
            .withColumn("merchant_categories_list",
                    split(col("merchant_categories"), "\\|"))
            .withColumn("primary_category",
                    element_at(col("merchant_categories_list"), 1))

            // Add data quality flags
            .withColumn("has_all_required_fields",
                    col("transaction_id").isNotNull()
                            .and(col("amount").isNotNull())
                            .and(col("merchant_id").isNotNull()))

            // Calculate statistics from array fields
            .withColumn("avg_risk_score",
                    when(col("risk_scores_count").gt(0),
                            col("risk_scores_avg_length")).otherwise(0));

    // Cache for multiple operations
    transformed.cache();

    // Generate multiple outputs

    // 1. Main transaction table
    transformed
            .filter(col("has_all_required_fields"))
            .drop("_raw_json", "_error")
            .write()
            .mode(SaveMode.Overwrite)
            .partitionBy("transaction_year", "transaction_month")
            .parquet("data/output/transactions");

    // 2. High-value transactions for fraud analysis
    transformed
            .filter(col("is_high_value"))
            .select(
                    col("transaction_id"),
                    col("amount"),
                    col("merchant_id"),
                    col("risk_scores"),
                    col("_raw_json").alias("original_json")
            )
            .write()
            .mode(SaveMode.Overwrite)
            .parquet("data/output/high_value_transactions");

    // 3. Data quality issues
    if (result.getErrorDataset() != null) {
        result.getErrorDataset()
                .union(
                        transformed
                                .filter(not(col("has_all_required_fields")))
                                .withColumn("_error", lit("Missing required fields"))
                )
                .write()
                .mode(SaveMode.Overwrite)
                .json("data/output/data_quality_issues");
    }

    // 4. Daily aggregates
    Dataset<Row> dailyStats = transformed
            // Use col() for all arguments to ensure they are all Column objects
            .groupBy(
                    col("transaction_year"),
                    col("transaction_month"),
                    date_format(col("transaction_date"), "yyyy-MM-dd").alias("date")
            )
            .agg(
                    count("*").alias("transaction_count"),
                    sum("amount").alias("total_amount"),
                    avg("amount").alias("avg_amount"),
                    max("amount").alias("max_amount"),
                    countDistinct("merchant_id").alias("unique_merchants"),
                    countDistinct("primary_category").alias("unique_categories"),
                    sum(when(col("is_high_value"), 1).otherwise(0)).alias("high_value_count")
            );

    dailyStats
            .write()
            .mode(SaveMode.Overwrite)
            .partitionBy("transaction_year", "transaction_month")
            .parquet("data/output/daily_statistics");

    // Show summary
    System.out.println("\nProcessing Summary:");
    System.out.println("Total records: " + result.getMetrics().getTotalRecords());
    System.out.println("Successful: " + result.getMetrics().getSuccessfulRecords());
    System.out.println("Errors: " + result.getMetrics().getMalformedRecords());
    System.out.println("Success rate: " +
            String.format("%.2f%%", result.getMetrics().getSuccessRate() * 100));

    System.out.println("\nDaily statistics sample:");
    dailyStats.orderBy(desc("date")).show(10, false);

    // Cleanup
    transformed.unpersist();
}
```

## License

Same as the NexusPiercer project - Apache License 2.0.