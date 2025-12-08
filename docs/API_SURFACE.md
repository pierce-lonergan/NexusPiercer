# API Surface — NexusPiercer
> External interfaces and public APIs
> Last Updated: 2025-12-08

## Primary APIs

### 1. NexusPiercerSparkPipeline (Fluent Builder API)

**Entry Points:**
```java
// Batch processing
NexusPiercerSparkPipeline.forBatch(SparkSession spark)

// Streaming processing
NexusPiercerSparkPipeline.forStreaming(SparkSession spark)
```

**Configuration Methods:**
| Method | Purpose | Default |
|--------|---------|---------|
| `withSchema(String path)` | Set Avro schema from file | Required |
| `withSchema(Schema schema)` | Set Avro schema object | Required |
| `withArrayDelimiter(String delim)` | Delimiter for array values | `","` |
| `withNullPlaceholder(String val)` | Null representation | `null` |
| `withMaxNestingDepth(int depth)` | Max JSON depth | 50 |
| `withMaxArraySize(int size)` | Max array elements | 1000 |
| `enableArrayStatistics()` | Generate _count, _distinct_count, etc. | disabled |
| `enableMatrixDenotors()` | Include array indices in values | disabled |
| `explodeArrays(String... paths)` | Arrays to normalize | none |
| `withErrorHandling(ErrorHandling)` | Error strategy | SKIP_MALFORMED |
| `allowSchemaErrors()` | Keep records with type mismatches | disabled |
| `quarantineSchemaErrors()` | Move type mismatches to error dataset | enabled |
| `includeMetadata()` | Add _processing_time, _input_file | disabled |
| `includeRawJson()` | Keep original in _raw_json | disabled |
| `withRepartition(int count)` | Control output partitions | auto |

**Processing Methods:**
```java
// Batch processing
ProcessingResult process(String... inputPaths)

// Streaming processing
DataStreamWriter processStream(String source, Map<String, String> options)
```

**ProcessingResult:**
```java
Dataset<Row> getDataset()        // Main results
Dataset<Row> getErrorDataset()   // Quarantined records (if QUARANTINE mode)
ProcessingMetrics getMetrics()   // Processing statistics
DataFrameWriter write()          // Convenience writer
```

---

### 2. NexusPiercerFunctions (Spark SQL UDFs)

**Registration:**
```java
NexusPiercerFunctions.registerAll(SparkSession spark)
NexusPiercerFunctions.register(SparkSession spark, String... functionNames)
```

**Available Functions:**

| Function | Signature | Description |
|----------|-----------|-------------|
| `flatten_json` | `(json: String) → String` | Flatten and consolidate JSON |
| `flatten_json_with_delimiter` | `(json: String, delim: String) → String` | Flatten with custom delimiter |
| `flatten_json_with_stats` | `(json: String) → String` | Flatten with array statistics |
| `extract_json_array` | `(json: String, path: String) → String` | Extract array as string |
| `json_array_count` | `(json: String, path: String) → Long` | Count array elements |
| `json_array_distinct_count` | `(json: String, path: String) → Long` | Count distinct elements |
| `explode_json_array` | `(json: String, path: String) → Array[String]` | Explode array to rows |
| `is_valid_json` | `(json: String) → Boolean` | Check JSON validity |
| `get_json_error` | `(json: String) → String` | Get parsing error message |
| `extract_nested_field` | `(json: String, path: String) → String` | Extract nested field value |

**DataFrame API Usage:**
```java
import static io.github.pierce.spark.NexusPiercerFunctions.*;

df.withColumn("flattened", flattenJson(col("json_string")))
  .withColumn("item_count", arrayCount(col("json_string"), "items"))
```

**SQL Usage:**
```sql
SELECT 
    flatten_json(json_data) as flattened,
    json_array_count(json_data, 'items') as item_count
FROM json_table
```

---

### 3. NexusPiercerPatterns (Pre-built Pipelines)

**Available Patterns:**

```java
// JSON to Parquet with partitioning
NexusPiercerPatterns.jsonToParquet(
    SparkSession spark,
    String schemaPath,
    String inputPath,
    String outputPath,
    SaveMode mode,
    String... partitionColumns
)

// JSON to Delta Lake
NexusPiercerPatterns.jsonToDelta(
    SparkSession spark,
    String schemaPath,
    String inputPath,
    String outputPath
)

// Generate data quality report
Dataset<Row> NexusPiercerPatterns.generateDataQualityReport(
    SparkSession spark,
    String schemaPath,
    String inputPath
)

// Profile JSON structure
Dataset<Row> NexusPiercerPatterns.profileJsonStructure(
    SparkSession spark,
    String inputPath,
    int sampleSize
)

// Normalize to multiple tables
Map<String, Dataset<Row>> NexusPiercerPatterns.jsonToNormalizedTables(
    SparkSession spark,
    String schemaPath,
    String inputPath,
    String... arrayPaths
)
```

---

### 4. JsonFlattenerConsolidator (Direct API)

**Construction:**
```java
// Full constructor
new JsonFlattenerConsolidator(
    String arrayDelimiter,       // "," 
    String nullPlaceholder,      // null or "NULL"
    int maxNestingDepth,         // 50
    int maxArraySize,            // 1000
    boolean matrixDenotors,      // false
    boolean gatherStatistics,    // true
    String... explosionPaths     // optional
)

// Convenience constructors
new JsonFlattenerConsolidator(delimiter, null, 50, 1000, false)
new JsonFlattenerConsolidator(delimiter, null, 50, 1000, false, true)
```

**Methods:**
```java
// Flatten and consolidate to single JSON string
String flattenAndConsolidateJson(String jsonString)

// Flatten and explode to multiple JSON strings
List<String> flattenAndExplodeJson(String jsonString)
```

---

### 5. MapFlattener (Builder API)

**Construction:**
```java
MapFlattener flattener = MapFlattener.builder()
    .maxDepth(50)
    .maxArraySize(1000)
    .maxMapSize(10000)
    .maxJsonStringLength(1000000)
    .useArrayBoundarySeparator(false)
    .namingStrategy(FieldNamingStrategy.SNAKE_CASE)
    .excludePaths("internal.*", "debug")
    .detectCircularReferences(true)
    .strictKeyValidation(false)
    .parseNestedJsonStrings(false)
    .preserveBigDecimalPrecision(true)
    .arrayFormat(ArraySerializationFormat.JSON)
    .build();
```

**Methods:**
```java
Map<String, Object> flatten(Map<?, ?> input)
```

**Array Serialization Formats:**
- `JSON` — `["Alice","Bob"]`
- `COMMA_SEPARATED` — `Alice,Bob`
- `PIPE_SEPARATED` — `Alice|Bob`
- `BRACKET_LIST` — `[Alice, Bob]`

---

### 6. IcebergSchemaConverter

**Construction:**
```java
// Create new converter
IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);
IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema, config);

// Get cached converter
IcebergSchemaConverter converter = IcebergSchemaConverter.cached(schema);
IcebergSchemaConverter converter = IcebergSchemaConverter.cached(schema, config);
```

**Methods:**
```java
GenericRecord convert(Map<String, Object> data)
static void clearCache()
```

---

### 7. AvroSchemaFlattener

**Construction:**
```java
new AvroSchemaFlattener()                                    // No stats, include non-terminal
new AvroSchemaFlattener(boolean includeArrayStatistics)       // Control stats
new AvroSchemaFlattener(boolean stats, boolean nonTerminal)   // Full control
```

**Methods:**
```java
Schema getFlattenedSchema(String schemaPath) throws IOException
Schema flattenSchema(Schema schema)

// Metadata accessors
Set<String> getArrayFieldNames()
Set<String> getTerminalArrayFieldNames()
Set<String> getNonTerminalArrayFieldNames()
Set<String> getMapFieldPaths()

// Excel export (if POI available)
void exportToExcel(String outputPath)
```

---

## Enumerations

### ErrorHandling
```java
public enum ErrorHandling {
    FAIL_FAST,       // Throw exception on first error
    SKIP_MALFORMED,  // Skip bad records silently
    QUARANTINE,      // Collect errors in separate dataset
    PERMISSIVE       // Keep records with null for failed fields
}
```

### FieldNamingStrategy (MapFlattener)
```java
public enum FieldNamingStrategy {
    AS_IS,       // Keep original names
    SNAKE_CASE,  // Convert camelCase to snake_case
    LOWER_CASE,  // Lowercase all
    UPPER_CASE   // Uppercase all
}
```

### ArraySerializationFormat (MapFlattener)
```java
public enum ArraySerializationFormat {
    JSON,             // ["a","b"]
    COMMA_SEPARATED,  // a,b
    PIPE_SEPARATED,   // a|b
    BRACKET_LIST      // [a, b]
}
```

---

## Maven Coordinates

```xml
<dependency>
    <groupId>io.github.pierce-lonergan</groupId>
    <artifactId>nexus-piercer</artifactId>
    <version>1.0.5</version>
</dependency>
```

---

## Compatibility Matrix

| Component | Minimum Version | Tested With |
|-----------|-----------------|-------------|
| Java | 17 | 17 |
| Spark | 3.x | 3.5.0 |
| Scala | 2.12.x | 2.12 |
| Avro | 1.11+ | 1.12.0 |
| Iceberg | 1.0+ | 1.7.1 |
| Hadoop | 3.x | 3.3.6 |
