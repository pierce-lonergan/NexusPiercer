# Class Registry — NexusPiercer
> Complete inventory of all classes/types with responsibilities
> Last Updated: 2025-12-08

## Registry Format

Each class entry follows this structure:
```
### [ClassName]
- **File:** `path/to/file.ext`
- **Layer:** [DOMAIN | APPLICATION | INFRASTRUCTURE | PRESENTATION | SHARED]
- **Type:** [Entity | Value Object | Service | Repository | Controller | Utility | DTO | Event | Command | Query | Handler | Factory | Adapter | Port]
- **Responsibility:** [Single sentence: what does this class do?]
- **Collaborators:** [Classes it directly uses]
- **Used By:** [Classes that use this]
- **State:** [Stateless | Stateful — what state?]
- **Thread Safety:** [Yes | No | Unknown]
- **Concerns:** [Any issues noted]
```

---

## Application Layer (Spark Integration)

### NexusPiercerSparkPipeline
- **File:** `src/main/java/io/github/pierce/spark/NexusPiercerSparkPipeline.java`
- **Layer:** APPLICATION
- **Type:** Service (Builder Pattern)
- **Responsibility:** Main entry point for Spark batch/streaming JSON processing with schema validation
- **Collaborators:** JsonFlattenerConsolidator, AvroSchemaFlattener, CreateSparkStructFromAvroSchema, FileFinder, SparkSession
- **Used By:** Client applications, NexusPiercerPatterns
- **State:** Stateful — SparkSession, PipelineConfig, cached schemas
- **Thread Safety:** Yes — uses ConcurrentHashMap for schema caching
- **Lines:** 948
- **Concerns:** None identified yet

### NexusPiercerSparkPipeline.PipelineConfig
- **File:** `src/main/java/io/github/pierce/spark/NexusPiercerSparkPipeline.java` (inner class)
- **Layer:** APPLICATION
- **Type:** DTO / Configuration
- **Responsibility:** Holds pipeline configuration options (delimiters, error handling, etc.)
- **Collaborators:** None
- **Used By:** NexusPiercerSparkPipeline
- **State:** Stateful — all configuration fields
- **Thread Safety:** No — mutable

### NexusPiercerSparkPipeline.CachedSchema
- **File:** `src/main/java/io/github/pierce/spark/NexusPiercerSparkPipeline.java` (inner class)
- **Layer:** APPLICATION
- **Type:** Value Object
- **Responsibility:** Caches flattened schema with metadata for performance
- **Collaborators:** Schema, StructType
- **Used By:** NexusPiercerSparkPipeline
- **State:** Stateful — immutable after construction
- **Thread Safety:** Yes — immutable

### NexusPiercerFunctions
- **File:** `src/main/java/io/github/pierce/spark/NexusPiercerFunctions.java`
- **Layer:** APPLICATION
- **Type:** Utility (Static Methods)
- **Responsibility:** Provides Spark SQL UDFs for JSON processing (flatten_json, json_array_count, etc.)
- **Collaborators:** JsonFlattenerConsolidator, SparkSession
- **Used By:** Client SQL queries, NexusPiercerSparkPipeline
- **State:** Stateless
- **Thread Safety:** Yes
- **Lines:** ~200 estimated
- **Concerns:** None identified yet

### NexusPiercerPatterns
- **File:** `src/main/java/io/github/pierce/spark/NexusPiercerPatterns.java`
- **Layer:** APPLICATION
- **Type:** Utility (Static Methods)
- **Responsibility:** Pre-configured pipelines for common ETL patterns (jsonToParquet, jsonToDelta, etc.)
- **Collaborators:** NexusPiercerSparkPipeline, SparkSession
- **Used By:** Client applications
- **State:** Stateless
- **Thread Safety:** Yes
- **Lines:** ~150 estimated
- **Concerns:** None identified yet

---

## Domain Layer (Core Processing)

### JsonFlattenerConsolidator
- **File:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java`
- **Layer:** DOMAIN
- **Type:** Service
- **Responsibility:** Flattens nested JSON into flat key-value pairs with array consolidation, statistics, and explosion support
- **Collaborators:** Jackson ObjectMapper, JsonNode, ArrayNode, ObjectNode
- **Used By:** NexusPiercerSparkPipeline, NexusPiercerFunctions
- **State:** Stateful — configuration (immutable), arrayFields (cleared per run)
- **Thread Safety:** Partial — config immutable, arrayFields cleared per run
- **Lines:** 820
- **Key Methods:**
  - `flattenAndConsolidateJson(String)` — Main entry point for flattening
  - `flattenAndExplodeJson(String)` — Returns multiple records for explosion
  - `consolidateFlattened(Map)` — Groups and aggregates array values
- **Concerns:** None identified
- **NOTE:** Refactored to use Jackson (Apache 2.0) instead of org.json

### MapFlattener
- **File:** `src/main/groovy/io/github/pierce/MapFlattener.groovy`
- **Layer:** DOMAIN
- **Type:** Service (Builder Pattern)
- **Responsibility:** Production-hardened Map-to-flat-Map transformation with circular reference detection
- **Collaborators:** ObjectMapper
- **Used By:** Client applications
- **State:** Stateful — configuration; ThreadLocal for context
- **Thread Safety:** Yes — ThreadLocal for per-thread context
- **Lines:** 1300
- **Concerns:** None identified yet

### AvroSchemaFlattener
- **File:** `src/main/java/io/github/pierce/AvroSchemaFlattener.java`
- **Layer:** DOMAIN
- **Type:** Service
- **Responsibility:** Flattens complex Avro schemas with terminal/non-terminal array classification
- **Collaborators:** Schema, FileFinder, POI (for Excel export)
- **Used By:** NexusPiercerSparkPipeline
- **State:** Stateful — field tracking, metadata, statistics
- **Thread Safety:** Partial — uses ConcurrentHashMap for cache
- **Lines:** 1122
- **Concerns:** None identified yet

### JsonFlattener
- **File:** `src/main/groovy/io/github/pierce/JsonFlattener.groovy`
- **Layer:** DOMAIN
- **Type:** Service (Fluent API)
- **Responsibility:** Production-grade JSON flattening utility with fluent API, streaming, batch processing, and comprehensive I/O support
- **Collaborators:** MapFlattener, ObjectMapper
- **Used By:** Client applications needing fluent API for JSON processing
- **State:** Stateful — MapFlattener instance, configuration
- **Thread Safety:** Yes — uses MapFlattener which is thread-safe
- **Lines:** 2005
- **Key Features:**
  - Multiple input sources: String, File, InputStream, Reader, byte[], Path, URL
  - Multiple output targets: String, File, OutputStream, Writer, byte[], Path
  - Streaming support for NDJSON/JSON Lines
  - Batch processing with parallel execution
  - GZIP compression support
- **Concerns:** None identified
- **NOTE:** This is a fluent wrapper around MapFlattener, NOT a duplicate of JsonFlattenerConsolidator

### JsonReconstructor ⚠️ COMMENTED OUT
- **File:** `src/main/groovy/io/github/pierce/JsonReconstructor.groovy`
- **Layer:** DOMAIN
- **Type:** Service (INACTIVE)
- **Responsibility:** Schema-less reconstruction of hierarchical JSON from flattened data
- **Collaborators:** ObjectMapper
- **Used By:** NONE — entire class is commented out
- **State:** N/A — COMMENTED OUT
- **Thread Safety:** N/A — COMMENTED OUT
- **Lines:** ~1294 (all commented)
- **Key Features (if enabled):**
  - Schema-less reconstruction (infers structure from keys)
  - Support for all array serialization formats
  - Deep reconstruction of nested arrays and objects
  - Fluent API with builder pattern
  - Verification utilities
- **Concerns:** ⚠️ MAJOR — Entire class is commented out (~1294 lines). Potential dead code or incomplete refactor.
- **NOTE:** Unlike AvroReconstructor which is active, this is completely inactive

### AvroReconstructor
- **File:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy`
- **Layer:** DOMAIN
- **Type:** Service (Builder Pattern)
- **Responsibility:** Proof-of-concept reconstructor that perfectly rebuilds hierarchical Avro GenericRecords from flattened Maps using schema guidance
- **Collaborators:** Schema, GenericRecord, GenericData, ObjectMapper, LogicalTypes, Conversions
- **Used By:** Client applications needing bidirectional flatten/reconstruct capability
- **State:** Stateful — configuration, schema cache (ConcurrentHashMap)
- **Thread Safety:** Yes — ConcurrentHashMap for caching, immutable config
- **Lines:** 2980
- **Key Features:**
  - Perfect reconstruction verified by diff comparison
  - Full Avro logical type support (decimal, timestamp, UUID, etc.)
  - ReconstructionVerification utility class for validation
  - Schema path trie for efficient field lookup
  - Support for all array serialization formats
  - Memory-safe iterative algorithms (no stack overflow)
- **Key Methods:**
  - `reconstructToMap(Map, Schema)` — Main reconstruction entry point
  - `verifyReconstruction(original, reconstructed, schema)` — Validates perfect reconstruction
  - `genericRecordToMap(GenericRecord)` — Converts GenericRecord to Map
- **Concerns:** None identified — well-designed with comprehensive verification

### GAvroSchemaFlattener
- **File:** `src/main/groovy/io/github/pierce/GAvroSchemaFlattener.groovy`
- **Layer:** DOMAIN
- **Type:** Service
- **Responsibility:** Groovy wrapper/extension for AvroSchemaFlattener
- **Collaborators:** AvroSchemaFlattener
- **Used By:** Unknown — needs exploration
- **State:** Unknown
- **Thread Safety:** Unknown

---

## Infrastructure Layer (Converters)

### IcebergSchemaConverter
- **File:** `src/main/java/io/github/pierce/converter/IcebergSchemaConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts Map<String, Object> to Iceberg GenericRecord according to schema
- **Collaborators:** TypeConverterRegistry, ConversionConfig, Schema (Iceberg)
- **Used By:** Client applications working with Iceberg
- **State:** Stateful — schema, converters cache (static)
- **Thread Safety:** Yes — ConcurrentHashMap for cache
- **Lines:** 409
- **Concerns:** None identified yet

### AvroSchemaConverter
- **File:** `src/main/java/io/github/pierce/converter/AvroSchemaConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts Map<String, Object> to Avro GenericRecord with full support for Avro-specific types (Utf8, unions, logical types)
- **Collaborators:** TypeConverter implementations, ConversionConfig, Schema (Avro), GenericData
- **Used By:** Client applications working with Avro
- **State:** Stateful — schema, field converters (static cache)
- **Thread Safety:** Yes — ConcurrentHashMap for converter cache
- **Lines:** 615
- **Key Features:**
  - Handles Utf8 strings
  - Union types (nullable fields)
  - Logical types (decimal, date, timestamp-millis, timestamp-micros, uuid)
  - Complex types (record, array, map, enum, fixed)
- **Concerns:** None identified

### TypeConverterRegistry
- **File:** `src/main/java/io/github/pierce/converter/TypeConverterRegistry.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Registry
- **Responsibility:** Manages and provides type converters for schema conversion
- **Collaborators:** ConversionConfig, TypeConverter implementations
- **Used By:** IcebergSchemaConverter, AvroSchemaConverter
- **State:** Stateful — registered converters
- **Thread Safety:** Unknown
- **Lines:** ~150

### ConversionConfig
- **File:** `src/main/java/io/github/pierce/converter/ConversionConfig.java`
- **Layer:** INFRASTRUCTURE
- **Type:** DTO / Configuration
- **Responsibility:** Configuration options for type conversion behavior
- **Collaborators:** None
- **Used By:** IcebergSchemaConverter, AvroSchemaConverter, TypeConverterRegistry
- **State:** Stateful — configuration
- **Thread Safety:** Unknown
- **Lines:** ~300 (12K file)

### TypeConverter (Interface)
- **File:** `src/main/java/io/github/pierce/converter/TypeConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Port (Interface)
- **Responsibility:** Contract for type conversion implementations
- **Collaborators:** None
- **Used By:** All *Converter classes
- **Lines:** ~50

### AbstractTypeConverter
- **File:** `src/main/java/io/github/pierce/converter/AbstractTypeConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Base Class
- **Responsibility:** Common base for type converters with shared logic
- **Collaborators:** ConversionConfig
- **Used By:** All concrete converters
- **Lines:** ~120

---

## Infrastructure Layer (Type Converters)

### BooleanConverter
- **File:** `src/main/java/io/github/pierce/converter/BooleanConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to Boolean for Iceberg/Avro

### StringConverter
- **File:** `src/main/java/io/github/pierce/converter/StringConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to String for Iceberg/Avro

### IntegerConverter
- **File:** `src/main/java/io/github/pierce/converter/IntegerConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to Integer for Iceberg/Avro

### LongConverter
- **File:** `src/main/java/io/github/pierce/converter/LongConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to Long for Iceberg/Avro

### FloatConverter
- **File:** `src/main/java/io/github/pierce/converter/FloatConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to Float for Iceberg/Avro

### DoubleConverter
- **File:** `src/main/java/io/github/pierce/converter/DoubleConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to Double for Iceberg/Avro

### DecimalConverter
- **File:** `src/main/java/io/github/pierce/converter/DecimalConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to BigDecimal with precision handling

### DateConverter
- **File:** `src/main/java/io/github/pierce/converter/DateConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to Date types for Iceberg/Avro

### TimeConverter
- **File:** `src/main/java/io/github/pierce/converter/TimeConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to Time types for Iceberg/Avro

### TimestampConverter
- **File:** `src/main/java/io/github/pierce/converter/TimestampConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to Timestamp (microseconds) for Iceberg/Avro

### TimestampNanoConverter
- **File:** `src/main/java/io/github/pierce/converter/TimestampNanoConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to Timestamp (nanoseconds) for Iceberg

### BinaryConverter
- **File:** `src/main/java/io/github/pierce/converter/BinaryConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to Binary/ByteBuffer for Iceberg/Avro

### UUIDConverter
- **File:** `src/main/java/io/github/pierce/converter/UUIDConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to UUID for Iceberg/Avro

### ListConverter
- **File:** `src/main/java/io/github/pierce/converter/ListConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to List types with element conversion

### MapConverter
- **File:** `src/main/java/io/github/pierce/converter/MapConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to Map types with key/value conversion

### StructConverter
- **File:** `src/main/java/io/github/pierce/converter/StructConverter.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Adapter
- **Responsibility:** Converts values to Struct/Record types with field conversion

---

## Infrastructure Layer (Utilities)

### FileFinder
- **File:** `src/main/java/io/github/pierce/files/FileFinder.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Utility
- **Responsibility:** Finds and loads files from filesystem, classpath, or resources
- **Collaborators:** InputStream, File
- **Used By:** AvroSchemaFlattener, AvroSchemaLoader
- **State:** Stateless
- **Thread Safety:** Yes

### AvroSchemaLoader
- **File:** `src/main/java/io/github/pierce/AvroSchemaLoader.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Utility
- **Responsibility:** Loads Avro schemas from various sources
- **Collaborators:** FileFinder, Schema.Parser
- **Used By:** NexusPiercerSparkPipeline
- **State:** Unknown
- **Thread Safety:** Unknown

### CreateSparkStructFromAvroSchema
- **File:** `src/main/java/io/github/pierce/CreateSparkStructFromAvroSchema.java`
- **Layer:** INFRASTRUCTURE
- **Type:** Utility
- **Responsibility:** Generates Spark StructType from Avro Schema
- **Collaborators:** AvroSchemaFlattener, Schema
- **Used By:** NexusPiercerSparkPipeline
- **State:** Unknown
- **Thread Safety:** Unknown

---

## Shared (DTOs / Exceptions)

### GenericRecord (Custom)
- **File:** `src/main/java/io/github/pierce/converter/GenericRecord.java`
- **Layer:** SHARED
- **Type:** DTO
- **Responsibility:** Custom GenericRecord implementation for Iceberg
- **Lines:** ~100

### ConversionResult
- **File:** `src/main/java/io/github/pierce/converter/ConversionResult.java`
- **Layer:** SHARED
- **Type:** DTO
- **Responsibility:** Holds result of conversion with success/failure status
- **Lines:** ~100

### Notification
- **File:** `src/main/java/io/github/pierce/converter/Notification.java`
- **Layer:** SHARED
- **Type:** DTO
- **Responsibility:** Holds conversion notifications/warnings
- **Lines:** ~100

### SchemaConversionException
- **File:** `src/main/java/io/github/pierce/converter/SchemaConversionException.java`
- **Layer:** SHARED
- **Type:** Exception
- **Responsibility:** Exception for schema conversion failures

### TypeConversionException
- **File:** `src/main/java/io/github/pierce/converter/TypeConversionException.java`
- **Layer:** SHARED
- **Type:** Exception
- **Responsibility:** Exception for type conversion failures

### NullValueException
- **File:** `src/main/java/io/github/pierce/converter/NullValueException.java`
- **Layer:** SHARED
- **Type:** Exception
- **Responsibility:** Exception for unexpected null values

### SchemaConversionUtil
- **File:** `src/main/java/io/github/pierce/converter/SchemaConversionUtil.java`
- **Layer:** SHARED
- **Type:** Utility
- **Responsibility:** Static utility methods for schema conversion
- **Lines:** ~280

### AvroSchemaUtilWrapper
- **File:** `src/main/java/io/github/pierce/converter/AvroSchemaUtilWrapper.java`
- **Layer:** SHARED
- **Type:** Utility
- **Responsibility:** Wraps Avro schema utilities with additional functionality
- **Lines:** ~250
