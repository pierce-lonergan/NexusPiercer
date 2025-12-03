# Schema Forge Converter

A production-grade Java library for converting `Map<String, Object>` data according to Apache Iceberg and Avro schemas.

## Overview

This library provides type-safe conversion between untyped Java maps and strongly-typed schema formats. It handles all 23+ Iceberg/Avro types including nested structures (struct, list, map), logical types (decimal, date, timestamp, uuid), and Avro's internal representations (Utf8, ByteBuffer).

## Project Structure

```
schema-forge-converter/
├── pom.xml
└── src/
    ├── main/java/io/github/pierce/converter/
    │   ├── TypeConverter.java               # Core interface
    │   ├── AbstractTypeConverter.java       # Base implementation
    │   ├── ConversionConfig.java            # Configuration options
    │   ├── ConversionResult.java            # Result wrapper with errors
    │   ├── Notification.java                # Error accumulation pattern
    │   ├── SchemaConversionException.java   # Base exception
    │   ├── TypeConversionException.java     # Type conversion errors
    │   ├── NullValueException.java          # Null handling errors
    │   ├── GenericRecord.java               # Iceberg record wrapper
    │   ├── TypeConverterRegistry.java       # Cached converter registry
    │   │
    │   ├── IcebergSchemaConverter.java      # Main Iceberg converter
    │   ├── AvroSchemaConverter.java         # Main Avro converter
    │   ├── SchemaConversionUtil.java        # Schema conversion utilities
    │   ├── AvroSchemaUtilWrapper.java       # Wrapper around Iceberg's AvroSchemaUtil
    │   │
    │   ├── BooleanConverter.java            # Boolean type
    │   ├── IntegerConverter.java            # Int type (32-bit)
    │   ├── LongConverter.java               # Long type (64-bit)
    │   ├── FloatConverter.java              # Float type (32-bit)
    │   ├── DoubleConverter.java             # Double type (64-bit)
    │   ├── StringConverter.java             # String type
    │   ├── DecimalConverter.java            # Decimal with precision/scale
    │   ├── BinaryConverter.java             # Binary/Fixed types
    │   ├── UUIDConverter.java               # UUID type
    │   ├── DateConverter.java               # Date (days since epoch)
    │   ├── TimeConverter.java               # Time (microseconds since midnight)
    │   ├── TimestampConverter.java          # Timestamp (microseconds since epoch)
    │   ├── ListConverter.java               # List/Array types
    │   ├── MapConverter.java                # Map types
    │   └── StructConverter.java             # Struct/Record types
    │
    └── test/java/io/github/pierce/converter/
        └── TypeConverterProperties.java     # Property-based tests
```

## Key Features

- **Cached Converter Registry**: Pre-computes converters per schema for O(1) lookup
- **Thread-Safe**: Uses `ConcurrentHashMap` for thread-safe caching
- **Flexible Input Handling**: Accepts data from Avro GenericRecord, JSON, or any Map source
- **Robust Type Conversion**: Handles Avro's Utf8 strings, ByteBuffer binaries, logical types
- **Error Accumulation**: Notification pattern for collecting all errors before failing
- **Configurable Behavior**: Strict or lenient modes, precision handling, timezone settings

## Usage

### Iceberg Schema Conversion

```java
Schema schema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.optional(2, "name", Types.StringType.get()),
    Types.NestedField.optional(3, "amount", Types.DecimalType.of(10, 2))
);

IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);

Map<String, Object> data = Map.of(
    "id", 12345L,
    "name", "Test",
    "amount", new BigDecimal("99.99")
);

GenericRecord record = converter.convert(data);
```

### Avro Schema Conversion

```java
String schemaJson = """
    {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": ["null", "string"], "default": null}
        ]
    }
    """;

org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaJson);
AvroSchemaConverter converter = AvroSchemaConverter.create(schema);

Map<String, Object> data = Map.of("id", 12345L, "name", "John Doe");
org.apache.avro.generic.GenericRecord record = converter.convert(data);
```

### Configuration

```java
// Strict configuration - fails fast on any issues
ConversionConfig strict = ConversionConfig.strict();

// Lenient configuration - attempts to convert whenever possible
ConversionConfig lenient = ConversionConfig.lenient();

// Custom configuration
ConversionConfig custom = ConversionConfig.builder()
    .errorHandlingMode(ErrorHandlingMode.COLLECT_ERRORS)
    .strictTypeChecking(false)
    .allowPrecisionLoss(true)
    .coerceEmptyStringsToNull(true)
    .defaultTimezone(ZoneId.of("America/New_York"))
    .build();
```

## Dependencies

- Apache Iceberg 1.4.3+
- Apache Avro 1.11.3+
- Java 17+

## Building

```bash
mvn clean package
```

## Testing

```bash
mvn test
```

The project includes property-based tests using jqwik to validate converter behavior across a wide range of inputs.

## What Was Fixed/Created

### Created Classes (were missing from original files):

1. **TypeConverter** - Core interface for all converters
2. **AbstractTypeConverter** - Base class with common functionality
3. **SchemaConversionException** - Base exception class
4. **TypeConversionException** - Specific conversion error
5. **NullValueException** - Null value handling error
6. **ConversionResult** - Result wrapper with success/failure handling
7. **Notification** - Error accumulation pattern implementation
8. **TypeConverterRegistry** - Cached converter registry
9. **GenericRecord** - Wrapper for Iceberg records
10. **All primitive converters** (Boolean, Integer, Long, Float, Double, String)
11. **DecimalConverter** - BigDecimal with precision/scale
12. **UUIDConverter** - UUID handling
13. **DateConverter** - Date as days since epoch
14. **TimeConverter** - Time as microseconds since midnight
15. **TimestampConverter** - Timestamp as microseconds since epoch
16. **ListConverter** - List/array handling
17. **MapConverter** - Map handling
18. **AvroSchemaUtilWrapper** - Wrapper around Iceberg's AvroSchemaUtil

### Fixed Issues:

1. Fixed CRLF line endings in all uploaded files
2. Added missing import for `AvroSchemaUtil` in `SchemaConversionUtil.java`
3. Fixed pom.xml structure

## License

MIT License
