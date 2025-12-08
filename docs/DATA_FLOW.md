# Data Flow — NexusPiercer
> How data moves through the system
> Last Updated: 2025-12-08

## High-Level Data Flow

```mermaid
graph LR
    subgraph "Input"
        A[Nested JSON] 
        B[Avro Schema]
        C[Map Objects]
    end
    
    subgraph "Processing"
        D[Flatten]
        E[Consolidate]
        F[Convert Types]
    end
    
    subgraph "Output"
        G[Flat Key-Value]
        H[Spark DataFrame]
        I[Iceberg Records]
    end
    
    A --> D --> E --> G
    B --> D
    G --> F --> H
    G --> F --> I
    C --> D --> G
```

---

## Flow 1: Batch JSON Processing

### Sequence

```mermaid
sequenceDiagram
    participant User
    participant Pipeline as NexusPiercerSparkPipeline
    participant SchemaFlat as AvroSchemaFlattener
    participant Spark as SparkSession
    participant JsonFlat as JsonFlattenerConsolidator
    
    User->>Pipeline: forBatch(spark)
    User->>Pipeline: withSchema("schema.avsc")
    Pipeline->>SchemaFlat: getFlattenedSchema()
    SchemaFlat-->>Pipeline: FlattenedSchema
    
    User->>Pipeline: process("input/*.json")
    Pipeline->>Spark: Read JSON files
    Spark-->>Pipeline: Dataset[Row]
    
    Pipeline->>Pipeline: Create flatten UDF
    
    loop Each Row
        Pipeline->>JsonFlat: flattenAndConsolidateJson(json)
        JsonFlat-->>Pipeline: Flat JSON string
    end
    
    Pipeline->>Spark: Apply schema via from_json
    Pipeline-->>User: ProcessingResult
```

### Data Transformation Example

```
Input JSON:
{
  "user": {
    "name": "Alice",
    "addresses": [
      {"city": "NYC", "zip": "10001"},
      {"city": "LA", "zip": "90001"}
    ]
  }
}

After Flattening:
{
  "user.name": "Alice",
  "user.addresses[0].city": "NYC",
  "user.addresses[0].zip": "10001",
  "user.addresses[1].city": "LA",
  "user.addresses[1].zip": "90001"
}

After Consolidation:
{
  "user_name": "Alice",
  "user_addresses_city": "NYC,LA",
  "user_addresses_zip": "10001,90001",
  "user_addresses_city_count": 2,
  "user_addresses_city_distinct_count": 2
}
```

---

## Flow 2: Streaming from Kafka

### Sequence

```mermaid
sequenceDiagram
    participant User
    participant Pipeline as NexusPiercerSparkPipeline
    participant Kafka
    participant Spark as SparkSession
    participant JsonFlat as JsonFlattenerConsolidator
    participant Sink as Output Sink
    
    User->>Pipeline: forStreaming(spark)
    User->>Pipeline: withSchema("schema.avsc")
    User->>Pipeline: processStream("kafka", options)
    
    Pipeline->>Kafka: Subscribe to topic
    
    loop Micro-batch
        Kafka-->>Pipeline: Batch of messages
        Pipeline->>JsonFlat: flattenAndConsolidateJson(each)
        JsonFlat-->>Pipeline: Flat JSON strings
        Pipeline->>Sink: Write batch
    end
```

---

## Flow 3: Array Explosion

### Sequence

```mermaid
sequenceDiagram
    participant User
    participant Pipeline as NexusPiercerSparkPipeline
    participant JsonFlat as JsonFlattenerConsolidator
    
    User->>Pipeline: explodeArrays("items")
    User->>Pipeline: process("orders/*.json")
    
    loop Each JSON Record
        Pipeline->>JsonFlat: flattenAndExplodeJson(json)
        JsonFlat-->>Pipeline: List[Flat JSON] (1 per array element)
    end
    
    Pipeline-->>User: Normalized rows
```

### Data Transformation Example

```
Input JSON:
{
  "orderId": "123",
  "items": [
    {"sku": "A", "qty": 1},
    {"sku": "B", "qty": 2}
  ]
}

After Explosion (2 rows):
Row 1: {"orderId": "123", "items_sku": "A", "items_qty": 1, "_explosion_index": 0}
Row 2: {"orderId": "123", "items_sku": "B", "items_qty": 2, "_explosion_index": 1}
```

---

## Flow 4: Map-to-Iceberg Conversion

### Sequence

```mermaid
sequenceDiagram
    participant User
    participant ISC as IcebergSchemaConverter
    participant TCR as TypeConverterRegistry
    participant TC as TypeConverters
    
    User->>ISC: create(icebergSchema)
    ISC->>TCR: Initialize converters
    TCR->>TC: Register type converters
    
    User->>ISC: convert(mapData)
    
    loop Each Field
        ISC->>TCR: getConverter(fieldType)
        TCR-->>ISC: TypeConverter
        ISC->>TC: convert(value)
        TC-->>ISC: Converted value
    end
    
    ISC-->>User: GenericRecord
```

---

## Flow 5: Schema Flattening

### Sequence

```mermaid
sequenceDiagram
    participant User
    participant ASF as AvroSchemaFlattener
    participant FF as FileFinder
    
    User->>ASF: getFlattenedSchema("schema.avsc")
    ASF->>ASF: Check cache
    
    alt Cache Miss
        ASF->>FF: findFile("schema.avsc")
        FF-->>ASF: InputStream
        ASF->>ASF: Parse Schema
        ASF->>ASF: Flatten recursively
        ASF->>ASF: Classify terminal/non-terminal arrays
        ASF->>ASF: Generate statistics fields
        ASF->>ASF: Cache result
    end
    
    ASF-->>User: Flattened Schema
```

---

## Data State Transitions

```
┌─────────────────────────────────────────────────────────────────────┐
│                         JSON String                                  │
│  {"user": {"name": "Alice", "scores": [90, 85, 92]}}               │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼ JsonFlattenerConsolidator
┌─────────────────────────────────────────────────────────────────────┐
│                     Flattened JSON String                           │
│  {"user_name": "Alice", "user_scores": "90,85,92",                  │
│   "user_scores_count": 3, "user_scores_avg": 89}                    │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼ Spark from_json + Schema
┌─────────────────────────────────────────────────────────────────────┐
│                       Spark Row                                      │
│  [user_name: String, user_scores: String, user_scores_count: Long]  │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼ IcebergSchemaConverter (optional)
┌─────────────────────────────────────────────────────────────────────┐
│                    Iceberg GenericRecord                            │
│  Record(user_name="Alice", user_scores="90,85,92", ...)            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Error Handling Flow

```mermaid
graph TD
    A[JSON Input] --> B{Valid JSON?}
    B -->|No| C[Error: Malformed]
    B -->|Yes| D{Schema Match?}
    D -->|No| E{ErrorHandling?}
    E -->|FAIL_FAST| F[Throw Exception]
    E -->|SKIP_MALFORMED| G[Skip Record]
    E -->|QUARANTINE| H[Add to Error Dataset]
    E -->|PERMISSIVE| I[Keep with nulls]
    D -->|Yes| J[Process Normally]
    
    G --> K[Continue Processing]
    H --> K
    I --> K
    J --> K
```

---

## Performance Considerations

### Caching Points
1. **Schema Cache:** `AvroSchemaFlattener.schemaCache` — ConcurrentHashMap
2. **Pipeline Schema Cache:** `NexusPiercerSparkPipeline.SCHEMA_CACHE` — ConcurrentHashMap
3. **Converter Cache:** `IcebergSchemaConverter.CONVERTER_CACHE` — ConcurrentHashMap

### Broadcast Variables
- Flattener configuration broadcast to Spark executors
- Schema broadcast for distributed processing

### Partitioning
- Configurable repartition count via `withRepartition(n)`
- Default follows Spark's automatic partitioning
