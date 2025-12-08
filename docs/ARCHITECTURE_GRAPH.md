# Architecture Graph — NexusPiercer
> Auto-generated from discovery. Updates as exploration progresses.
> Last Updated: 2025-12-08

## System Context (C4 Level 1)
*External systems and actors that interact with this system*

```mermaid
graph TB
    subgraph "System Context"
        NP[NexusPiercer Library]
    end
    
    DEV[Data Engineer] -->|"Uses API"| NP
    NP -->|"Reads"| JSON_SRC[JSON Sources]
    NP -->|"Reads"| AVRO_SRC[Avro Files/Schemas]
    NP -->|"Processes via"| SPARK[Apache Spark]
    NP -->|"Writes to"| PARQUET[Parquet/Delta Lake]
    NP -->|"Writes to"| ICEBERG[Apache Iceberg Tables]
    NP -->|"Streams from"| KAFKA[Apache Kafka]
```

## Container Diagram (C4 Level 2)
*Major deployable units / processes*

```mermaid
graph TB
    subgraph "NexusPiercer JAR"
        subgraph "Core Engine"
            FLAT[Flattening Engine]
            SCHEMA[Schema Processing]
        end
        
        subgraph "Spark Integration"
            PIPELINE[Spark Pipeline]
            UDF[SQL Functions]
        end
        
        subgraph "Converters"
            CONV[Type Converters]
        end
    end
    
    PIPELINE --> FLAT
    PIPELINE --> SCHEMA
    UDF --> FLAT
    SCHEMA --> CONV
```

## Component Diagram (C4 Level 3)
*Internal components and their relationships*

```mermaid
graph LR
    subgraph "Spark Layer"
        NPSP[NexusPiercerSparkPipeline]
        NPF[NexusPiercerFunctions]
        NPP[NexusPiercerPatterns]
    end
    
    subgraph "Flattening Layer"
        JFC[JsonFlattenerConsolidator]
        MF[MapFlattener]
        JF[JsonFlattener]
        JR[JsonReconstructor ⚠️ INACTIVE]
        AR[AvroReconstructor]
    end
    
    subgraph "Schema Layer"
        ASF[AvroSchemaFlattener]
        ASL[AvroSchemaLoader]
        GASF[GAvroSchemaFlattener]
        CSFS[CreateSparkStructFromAvroSchema]
    end
    
    subgraph "Converter Layer"
        ASC[AvroSchemaConverter]
        ISC[IcebergSchemaConverter]
        TCR[TypeConverterRegistry]
        CC[ConversionConfig]
    end
    
    subgraph "Type Converters"
        TC_PRIM[Primitive Converters]
        TC_COMPLEX[Complex Converters]
    end
    
    subgraph "Utilities"
        FF[FileFinder]
    end
    
    NPSP --> JFC
    NPSP --> ASF
    NPSP --> NPF
    NPP --> NPSP
    
    NPF --> JFC
    
    JF --> MF
    
    AR --> |"uses schema"| AVRO_SCHEMA[Avro Schema]
    
    ASF --> ASL
    ASF --> FF
    GASF --> ASF
    CSFS --> ASF
    
    ISC --> TCR
    ASC --> TCR
    TCR --> CC
    TCR --> TC_PRIM
    TCR --> TC_COMPLEX
```

## Detailed Dependency Graph
*Class/module level dependencies*

```mermaid
graph LR
    %% Spark Pipeline dependencies
    NPSP[NexusPiercerSparkPipeline] --> |"uses"| JFC[JsonFlattenerConsolidator]
    NPSP --> |"uses"| ASF[AvroSchemaFlattener]
    NPSP --> |"uses"| CSFS[CreateSparkStructFromAvroSchema]
    NPSP --> |"uses"| FF[FileFinder]
    NPSP --> |"uses"| NPF[NexusPiercerFunctions]
    
    %% Functions dependencies
    NPF --> |"uses"| JFC
    
    %% Schema converter dependencies
    ISC[IcebergSchemaConverter] --> |"uses"| TCR[TypeConverterRegistry]
    ISC --> |"uses"| CC[ConversionConfig]
    
    ASC[AvroSchemaConverter] --> |"uses"| TCR
    ASC --> |"uses"| CC
    
    TCR --> |"manages"| TC[TypeConverter]
    
    %% Type converters
    BOOL[BooleanConverter] --> |"implements"| TC
    STR[StringConverter] --> |"implements"| TC
    INT[IntegerConverter] --> |"implements"| TC
    LONG[LongConverter] --> |"implements"| TC
    DBL[DoubleConverter] --> |"implements"| TC
    FLT[FloatConverter] --> |"implements"| TC
    DEC[DecimalConverter] --> |"implements"| TC
    DATE[DateConverter] --> |"implements"| TC
    TIME[TimeConverter] --> |"implements"| TC
    TS[TimestampConverter] --> |"implements"| TC
    BIN[BinaryConverter] --> |"implements"| TC
    UUID[UUIDConverter] --> |"implements"| TC
    LIST[ListConverter] --> |"implements"| TC
    MAP[MapConverter] --> |"implements"| TC
    STRUCT[StructConverter] --> |"implements"| TC
```

## Layer Diagram
*Architectural layers and allowed dependencies*

```mermaid
graph TB
    subgraph "API Layer"
        API_SPARK[Spark Pipeline API]
        API_DIRECT[Direct Flattening API]
    end
    
    subgraph "Processing Layer"
        PROC_FLAT[Flattening Engine]
        PROC_SCHEMA[Schema Processing]
        PROC_CONV[Type Conversion]
    end
    
    subgraph "Infrastructure Layer"
        INFRA_FILE[File Operations]
        INFRA_CACHE[Schema Caching]
    end
    
    API_SPARK --> PROC_FLAT
    API_SPARK --> PROC_SCHEMA
    API_DIRECT --> PROC_FLAT
    
    PROC_SCHEMA --> PROC_CONV
    PROC_FLAT --> INFRA_CACHE
    PROC_SCHEMA --> INFRA_FILE
```

## Data Flow Diagram
*How data moves through the system*

```mermaid
sequenceDiagram
    participant User
    participant NPSP as NexusPiercerSparkPipeline
    participant JFC as JsonFlattenerConsolidator
    participant ASF as AvroSchemaFlattener
    participant Spark as SparkSession
    
    User->>NPSP: forBatch(spark).withSchema("schema.avsc")
    NPSP->>ASF: getFlattenedSchema(schemaPath)
    ASF-->>NPSP: Schema (flattened)
    
    User->>NPSP: process("input/*.json")
    NPSP->>Spark: Read JSON files
    Spark-->>NPSP: Dataset[Row]
    
    loop For each row
        NPSP->>JFC: flattenAndConsolidateJson(json)
        JFC-->>NPSP: Flat JSON string
    end
    
    NPSP->>Spark: Apply flattened schema
    NPSP-->>User: ProcessingResult
```

## Relationship Registry
*Machine-parseable relationship list*

| Source | Relationship | Target | Evidence |
|--------|--------------|--------|----------|
| NexusPiercerSparkPipeline | DEPENDS_ON | JsonFlattenerConsolidator | import statement |
| NexusPiercerSparkPipeline | DEPENDS_ON | AvroSchemaFlattener | import statement |
| NexusPiercerSparkPipeline | DEPENDS_ON | CreateSparkStructFromAvroSchema | import statement |
| NexusPiercerSparkPipeline | DEPENDS_ON | FileFinder | import statement |
| NexusPiercerSparkPipeline | DEPENDS_ON | SparkSession | constructor param |
| NexusPiercerFunctions | DEPENDS_ON | JsonFlattenerConsolidator | UDF implementation |
| JsonFlattener | DELEGATES_TO | MapFlattener | uses MapFlattener internally |
| AvroReconstructor | DEPENDS_ON | Schema (Avro) | reconstruction requires schema |
| AvroReconstructor | DEPENDS_ON | ObjectMapper | JSON serialization |
| AvroReconstructor | DEPENDS_ON | GenericRecord | output type |
| AvroSchemaConverter | DEPENDS_ON | TypeConverterRegistry | field |
| AvroSchemaConverter | DEPENDS_ON | ConversionConfig | field |
| IcebergSchemaConverter | DEPENDS_ON | TypeConverterRegistry | field |
| IcebergSchemaConverter | DEPENDS_ON | ConversionConfig | field |
| AvroSchemaFlattener | DEPENDS_ON | FileFinder | getFlattenedSchema() |
| AvroSchemaFlattener | DEPENDS_ON | POI | Excel export |
| MapFlattener | DEPENDS_ON | ObjectMapper | field |
| JsonFlattenerConsolidator | DEPENDS_ON | ObjectMapper (Jackson) | JSON processing |
| TypeConverterRegistry | CREATES | *Converter | factory pattern |
| BooleanConverter | IMPLEMENTS | TypeConverter | class declaration |
| StringConverter | IMPLEMENTS | TypeConverter | class declaration |
| IntegerConverter | IMPLEMENTS | TypeConverter | class declaration |
| LongConverter | IMPLEMENTS | TypeConverter | class declaration |
| DoubleConverter | IMPLEMENTS | TypeConverter | class declaration |
| FloatConverter | IMPLEMENTS | TypeConverter | class declaration |
| DecimalConverter | IMPLEMENTS | TypeConverter | class declaration |
| DateConverter | IMPLEMENTS | TypeConverter | class declaration |
| TimeConverter | IMPLEMENTS | TypeConverter | class declaration |
| TimestampConverter | IMPLEMENTS | TypeConverter | class declaration |
| TimestampNanoConverter | IMPLEMENTS | TypeConverter | class declaration |
| BinaryConverter | IMPLEMENTS | TypeConverter | class declaration |
| UUIDConverter | IMPLEMENTS | TypeConverter | class declaration |
| ListConverter | IMPLEMENTS | TypeConverter | class declaration |
| MapConverter | IMPLEMENTS | TypeConverter | class declaration |
| StructConverter | IMPLEMENTS | TypeConverter | class declaration |
