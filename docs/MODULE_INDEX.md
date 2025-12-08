# Module Index — NexusPiercer
> Directory of all modules/packages discovered
> Last Updated: 2025-12-08

## Package Structure

```
io.github.pierce
├── spark/              # Spark integration layer
├── converter/          # Schema conversion utilities
├── files/              # File handling utilities
└── (root)              # Core flattening classes
```

---

## io.github.pierce (Root Package)

**Purpose:** Core flattening and consolidation engines

| Class | Purpose | Lines | Explored |
|-------|---------|-------|----------|
| JsonFlattenerConsolidator | JSON flattening with array consolidation | 923 | Partial |
| AvroSchemaFlattener | Avro schema flattening with analytics | 1122 | Partial |
| AvroSchemaLoader | Load Avro schemas from sources | ~200 | No |
| CreateSparkStructFromAvroSchema | Generate Spark StructType | ~200 | No |

**Groovy Classes:**
| Class | Purpose | Lines | Explored |
|-------|---------|-------|----------|
| JsonFlattener | Groovy JSON flattening implementation | ~500 | No |
| JsonReconstructor | Reconstruct JSON from flattened | ~400 | No |
| MapFlattener | Production Map flattening | 1300 | Partial |
| AvroReconstructor | Reconstruct Avro from flattened | ~400 | No |
| GAvroSchemaFlattener | Groovy Avro schema wrapper | ~200 | No |

---

## io.github.pierce.spark

**Purpose:** Apache Spark integration — pipelines, UDFs, patterns

| Class | Purpose | Lines | Explored |
|-------|---------|-------|----------|
| NexusPiercerSparkPipeline | Main pipeline API (batch/streaming) | 948 | Partial |
| NexusPiercerFunctions | Spark SQL UDFs (flatten_json, etc.) | ~200 | No |
| NexusPiercerPatterns | Pre-built ETL patterns | ~150 | No |

---

## io.github.pierce.converter

**Purpose:** Type-safe schema conversion between Avro, Iceberg, and Maps

### Core Classes
| Class | Purpose | Lines | Explored |
|-------|---------|-------|----------|
| IcebergSchemaConverter | Map → Iceberg GenericRecord | 409 | Partial |
| AvroSchemaConverter | Avro schema conversion | ~700 | No |
| TypeConverterRegistry | Manages type converters | ~150 | No |
| ConversionConfig | Conversion configuration | ~300 | No |
| AbstractTypeConverter | Base converter class | ~120 | No |
| TypeConverter | Converter interface | ~50 | No |

### Type Converters (Primitives)
| Class | Converts To | Lines |
|-------|-------------|-------|
| BooleanConverter | Boolean | ~60 |
| StringConverter | String | ~100 |
| IntegerConverter | Integer | ~100 |
| LongConverter | Long | ~90 |
| FloatConverter | Float | ~90 |
| DoubleConverter | Double | ~75 |

### Type Converters (Complex)
| Class | Converts To | Lines |
|-------|-------------|-------|
| DecimalConverter | BigDecimal | ~200 |
| DateConverter | Date | ~150 |
| TimeConverter | Time | ~130 |
| TimestampConverter | Timestamp (micros) | ~200 |
| TimestampNanoConverter | Timestamp (nanos) | ~200 |
| BinaryConverter | ByteBuffer | ~160 |
| UUIDConverter | UUID | ~120 |
| ListConverter | List | ~90 |
| MapConverter | Map | ~75 |
| StructConverter | Struct/Record | ~200 |

### DTOs and Exceptions
| Class | Purpose | Lines |
|-------|---------|-------|
| GenericRecord | Custom Iceberg record | ~100 |
| ConversionResult | Conversion outcome | ~100 |
| Notification | Warnings/info | ~100 |
| SchemaConversionException | Schema conversion errors | ~45 |
| TypeConversionException | Type conversion errors | ~75 |
| NullValueException | Null handling errors | ~15 |

### Utilities
| Class | Purpose | Lines |
|-------|---------|-------|
| SchemaConversionUtil | Static conversion utilities | ~280 |
| AvroSchemaUtilWrapper | Avro utility wrapper | ~250 |

---

## io.github.pierce.files

**Purpose:** File system and resource loading utilities

| Class | Purpose | Lines | Explored |
|-------|---------|-------|----------|
| FileFinder | Find files from classpath/filesystem | ~100 | No |

---

## Module Exploration Status

| Module | Classes | Explored | Coverage |
|--------|---------|----------|----------|
| Root (core) | 9 | 4 partial | 30% |
| spark | 3 | 1 partial | 20% |
| converter | 22 | 1 partial | 5% |
| files | 1 | 0 | 0% |
| **Total** | **35** | **6 partial** | **~15%** |
