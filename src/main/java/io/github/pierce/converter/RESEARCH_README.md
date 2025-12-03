# Building a production-grade Java Map-to-Schema converter for Iceberg and Avro

A robust Map-to-Schema conversion utility requires handling **23 distinct type conversions**, managing Avro's quirky internal representations (Utf8 strings, ByteBuffer binaries), and supporting both flat and deeply nested structures. The key architectural decision is pre-computing type converters per schema using a cached registry pattern, which eliminates repeated type introspection and achieves **O(1) converter lookup** for each field. Iceberg's `GenericRecord.create(schema).copy(map)` pattern provides the foundation for record creation, while Avro's `Conversions` API handles logical type transformations like decimal and timestamp.

## Understanding the dual type systems

Both Iceberg and Avro define comprehensive type hierarchies, but they differ in internal representation and logical type handling. Iceberg stores timestamps as **microseconds** (long), dates as **days since epoch** (int), and times as **microseconds from midnight** (long). Avro logical types overlay semantic meaning onto primitive types—a `timestamp-millis` is stored as a long representing milliseconds, while `timestamp-micros` uses microseconds.

The critical difference lies in how data arrives from each source. Avro's `GenericRecord` returns `org.apache.avro.util.Utf8` for strings (not `java.lang.String`), `ByteBuffer` for bytes, and uses Utf8 keys in Maps. JSON deserialization through Jackson typically produces `String`, `Integer`, `Long`, `Double`, and `ArrayList` instances. Your converter must handle both input shapes transparently.

**Iceberg type mappings to Java:**

| Iceberg Type | Internal Java Type | Factory Method |
|-------------|-------------------|----------------|
| boolean | Boolean | `Types.BooleanType.get()` |
| int | Integer | `Types.IntegerType.get()` |
| long | Long | `Types.LongType.get()` |
| float | Float | `Types.FloatType.get()` |
| double | Double | `Types.DoubleType.get()` |
| decimal(P,S) | BigDecimal | `Types.DecimalType.of(precision, scale)` |
| date | Integer (days) | `Types.DateType.get()` |
| time | Long (microseconds) | `Types.TimeType.get()` |
| timestamp | Long (microseconds) | `Types.TimestampType.withoutZone()` |
| timestamptz | Long (UTC microseconds) | `Types.TimestampType.withZone()` |
| string | CharSequence/String | `Types.StringType.get()` |
| uuid | UUID | `Types.UUIDType.get()` |
| binary | ByteBuffer | `Types.BinaryType.get()` |
| fixed(L) | ByteBuffer | `Types.FixedType.ofLength(L)` |

## Core converter architecture with cached type handlers

The optimal design pre-computes a converter function for each schema field during initialization, then applies these cached converters during record conversion. This eliminates the overhead of type introspection on every record while maintaining thread safety through `ConcurrentHashMap.computeIfAbsent()`.

```java
public class SchemaBasedMapConverter {
    private final ConcurrentMap<Integer, Map<String, TypeConverter<?,?>>> schemaConverters 
        = new ConcurrentHashMap<>();
    
    public GenericRecord convert(Map<String, Object> input, Schema schema) {
        Map<String, TypeConverter<?,?>> converters = getOrCreateConverters(schema);
        GenericRecord record = GenericRecord.create(schema);
        
        for (Types.NestedField field : schema.columns()) {
            String name = field.name();
            Object value = input.get(name);
            
            if (value == null) {
                if (field.isOptional()) {
                    record.setField(name, null);
                    continue;
                }
                throw new NullValueException("Required field missing: " + name);
            }
            
            TypeConverter<Object, Object> converter = 
                (TypeConverter<Object, Object>) converters.get(name);
            record.setField(name, converter.convert(value));
        }
        return record;
    }
    
    private Map<String, TypeConverter<?,?>> getOrCreateConverters(Schema schema) {
        return schemaConverters.computeIfAbsent(
            System.identityHashCode(schema),
            k -> buildConvertersForSchema(schema)
        );
    }
}
```

For type dispatch, Java 17+ pattern matching with switch expressions delivers both readability and performance through `invokedynamic` bytecode optimization:

```java
private TypeConverter<?,?> createConverterForType(Type type) {
    return switch (type.typeId()) {
        case BOOLEAN -> BooleanConverter.INSTANCE;
        case INTEGER -> IntegerConverter.INSTANCE;
        case LONG -> LongConverter.INSTANCE;
        case FLOAT -> FloatConverter.INSTANCE;
        case DOUBLE -> DoubleConverter.INSTANCE;
        case STRING -> StringConverter.INSTANCE;
        case DECIMAL -> new DecimalConverter(
            ((Types.DecimalType) type).precision(),
            ((Types.DecimalType) type).scale()
        );
        case UUID -> UUIDConverter.INSTANCE;
        case BINARY, FIXED -> BinaryConverter.INSTANCE;
        case DATE -> DateConverter.INSTANCE;
        case TIME -> TimeConverter.INSTANCE;
        case TIMESTAMP -> new TimestampConverter(
            ((Types.TimestampType) type).shouldAdjustToUTC()
        );
        case LIST -> new ListConverter(
            createConverterForType(((Types.ListType) type).elementType())
        );
        case MAP -> new MapConverter(
            createConverterForType(((Types.MapType) type).keyType()),
            createConverterForType(((Types.MapType) type).valueType())
        );
        case STRUCT -> new StructConverter((Types.StructType) type);
    };
}
```

## Handling Avro's internal representations

When input maps originate from Avro `GenericRecord` conversions, you must handle Avro's non-standard internal types. The most common pitfall is casting to `String` when Avro returns `Utf8`:

```java
// WRONG - throws ClassCastException
String name = (String) avroRecord.get("name");

// CORRECT - handle CharSequence polymorphically
Object value = avroRecord.get("name");
String name = value != null ? value.toString() : null;
```

For Avro Maps, keys are also `Utf8`, requiring explicit conversion:

```java
public Map<String, Object> convertAvroMap(Map<?, ?> avroMap, Type valueType) {
    Map<String, Object> result = new LinkedHashMap<>(avroMap.size() * 4 / 3 + 1);
    TypeConverter<?,?> valueConverter = getConverterForType(valueType);
    
    for (Map.Entry<?, ?> entry : avroMap.entrySet()) {
        String key = entry.getKey().toString();  // Utf8 -> String
        Object val = valueConverter.convert(entry.getValue());
        result.put(key, val);
    }
    return result;
}
```

Avro's logical types require detection through `Schema.getLogicalType()`, but unions complicate this—you must search within union branches:

```java
private LogicalType extractLogicalType(org.apache.avro.Schema avroSchema) {
    if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
        return avroSchema.getTypes().stream()
            .map(s -> s.getLogicalType())
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }
    return avroSchema.getLogicalType();
}
```

## Timestamp precision alignment between systems

Timestamp handling requires careful attention to precision differences. Avro offers both **millisecond** and **microsecond** precision variants, while Iceberg standardizes on **microseconds** internally. JSON timestamps typically arrive as epoch milliseconds or ISO-8601 strings.

```java
public class TimestampConverter implements TypeConverter<Object, Long> {
    private final boolean adjustToUtc;
    
    @Override
    public Long convert(Object value) {
        if (value == null) return null;
        
        // Avro timestamp-millis (long epoch millis)
        if (value instanceof Long epochMillis) {
            return epochMillis * 1000L;  // Convert to microseconds
        }
        
        // Java Instant (preferred for timezone-aware timestamps)
        if (value instanceof Instant instant) {
            return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000;
        }
        
        // LocalDateTime (for timestamps without timezone)
        if (value instanceof LocalDateTime ldt) {
            ZoneOffset offset = adjustToUtc ? ZoneOffset.UTC : ZoneOffset.systemDefault().getRules()
                .getOffset(ldt);
            return ldt.toEpochSecond(offset) * 1_000_000L + ldt.getNano() / 1000;
        }
        
        // ISO-8601 String parsing
        if (value instanceof String str) {
            try {
                Instant instant = Instant.parse(str);
                return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000;
            } catch (DateTimeParseException e) {
                LocalDateTime ldt = LocalDateTime.parse(str);
                return ldt.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + ldt.getNano() / 1000;
            }
        }
        
        throw new TypeConversionException("Cannot convert to timestamp: " + value.getClass());
    }
}
```

## BigDecimal precision enforcement and edge cases

Decimal handling requires strict precision and scale enforcement to prevent data corruption. Iceberg's decimal types specify maximum precision (total digits) and scale (decimal places), and values exceeding these limits must be rejected or rounded according to your policy.

```java
public class DecimalConverter implements TypeConverter<Object, BigDecimal> {
    private final int maxPrecision;
    private final int scale;
    
    @Override
    public BigDecimal convert(Object value) {
        if (value == null) return null;
        
        BigDecimal bd;
        if (value instanceof BigDecimal) {
            bd = (BigDecimal) value;
        } else if (value instanceof Number) {
            // Avoid double constructor - use String for precision
            bd = new BigDecimal(value.toString());
        } else if (value instanceof String) {
            bd = new BigDecimal((String) value);
        } else if (value instanceof ByteBuffer) {
            // Avro decimal from bytes
            ByteBuffer bb = (ByteBuffer) value;
            byte[] bytes = new byte[bb.remaining()];
            bb.duplicate().get(bytes);
            bd = new BigDecimal(new BigInteger(bytes), scale);
        } else {
            throw new TypeConversionException("Cannot convert to decimal: " + value.getClass());
        }
        
        return enforceConstraints(bd);
    }
    
    private BigDecimal enforceConstraints(BigDecimal value) {
        // Set scale with rounding if necessary
        BigDecimal scaled = value.setScale(scale, RoundingMode.HALF_UP);
        
        // Check precision after scaling
        BigDecimal stripped = scaled.stripTrailingZeros();
        int actualPrecision = stripped.precision();
        int actualScale = stripped.scale();
        
        // Handle negative scale (e.g., 1000 has scale -3)
        if (actualScale < 0) {
            actualPrecision = actualPrecision - actualScale;
        }
        
        if (actualPrecision > maxPrecision) {
            throw new ArithmeticException(
                String.format("Value %s exceeds precision %d", value, maxPrecision));
        }
        
        return scaled;
    }
}
```

**Critical BigDecimal rules:**
- Always use the `String` constructor (`new BigDecimal("0.1")`)—the double constructor loses precision
- Use `compareTo()` for equality checks, not `equals()` (which is scale-sensitive)
- Call `stripTrailingZeros()` before precision validation
- Explicitly specify `RoundingMode` for `setScale()` to avoid `ArithmeticException`

## Schema evolution and field ID stability

Iceberg's schema evolution relies on **field IDs** that remain stable across column renames and reorderings. When converting data, you should support scenarios where the input map uses old column names while the schema uses new names.

```java
public class EvolvingSchemaConverter {
    private final Map<String, String> aliasMap;  // oldName -> newName
    
    public GenericRecord convert(Map<String, Object> input, Schema schema) {
        GenericRecord record = GenericRecord.create(schema);
        
        for (Types.NestedField field : schema.columns()) {
            String name = field.name();
            Object value = input.get(name);
            
            // Try alias if primary name not found
            if (value == null && aliasMap.containsValue(name)) {
                String oldName = aliasMap.entrySet().stream()
                    .filter(e -> e.getValue().equals(name))
                    .map(Map.Entry::getKey)
                    .findFirst().orElse(null);
                if (oldName != null) {
                    value = input.get(oldName);
                }
            }
            
            // Handle missing fields
            if (value == null) {
                if (field.isOptional()) {
                    record.setField(name, null);
                } else {
                    throw new SchemaEvolutionException(
                        "Required field " + name + " missing and no alias found");
                }
                continue;
            }
            
            record.setField(name, convertWithPromotion(value, field.type()));
        }
        return record;
    }
    
    private Object convertWithPromotion(Object value, Type targetType) {
        // Support type promotion: int -> long, float -> double
        if (value instanceof Integer && targetType.typeId() == Type.TypeID.LONG) {
            return ((Integer) value).longValue();
        }
        if (value instanceof Float && targetType.typeId() == Type.TypeID.DOUBLE) {
            return ((Float) value).doubleValue();
        }
        // Standard conversion
        return convert(value, targetType);
    }
}
```

## Robustness patterns for edge cases

Production converters must handle numerous edge cases gracefully. Here's a comprehensive approach to the most common issues:

```java
public class RobustTypeConverter {
    
    // Overflow detection for numeric narrowing
    public int longToIntSafe(long value) {
        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            throw new ArithmeticException("Integer overflow: " + value);
        }
        return (int) value;
    }
    
    // NaN and Infinity rejection
    public double validateDouble(double value) {
        if (Double.isNaN(value)) {
            throw new ArithmeticException("NaN values not supported");
        }
        if (Double.isInfinite(value)) {
            throw new ArithmeticException("Infinite values not supported");
        }
        return value;
    }
    
    // UUID format validation with helpful errors
    private static final Pattern UUID_PATTERN = Pattern.compile(
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    );
    
    public UUID parseUuid(String value) {
        if (value == null || value.isEmpty()) return null;
        if (!UUID_PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException("Invalid UUID format: " + value);
        }
        return UUID.fromString(value);
    }
    
    // Binary from various sources
    public ByteBuffer toBinary(Object value) {
        if (value instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) value);
        }
        if (value instanceof ByteBuffer) {
            return ((ByteBuffer) value).duplicate();  // Don't modify original position
        }
        if (value instanceof String) {
            // Base64 encoded binary
            return ByteBuffer.wrap(Base64.getDecoder().decode((String) value));
        }
        throw new TypeConversionException("Cannot convert to binary: " + value.getClass());
    }
}
```

## Error accumulation versus fail-fast strategies

For batch processing, collecting all conversion errors before failing often provides better operational experience than fail-fast. The **Notification pattern** accumulates errors with field paths:

```java
public class Notification {
    private final List<ConversionError> errors = new ArrayList<>();
    
    public void addError(String fieldPath, String message, Exception cause) {
        errors.add(new ConversionError(fieldPath, message, cause));
    }
    
    public boolean hasErrors() { return !errors.isEmpty(); }
    
    public String formatErrors() {
        return errors.stream()
            .map(e -> e.fieldPath() + ": " + e.message())
            .collect(Collectors.joining("; "));
    }
}

public record ConversionError(String fieldPath, String message, Exception cause) {}

// Usage in converter
public ConversionResult<GenericRecord> convertWithErrorCollection(
        Map<String, Object> map, Schema schema) {
    Notification notification = new Notification();
    GenericRecord record = GenericRecord.create(schema);
    
    for (Types.NestedField field : schema.columns()) {
        try {
            Object value = convertField(map.get(field.name()), field, notification);
            record.setField(field.name(), value);
        } catch (Exception e) {
            notification.addError(field.name(), e.getMessage(), e);
        }
    }
    
    return notification.hasErrors() 
        ? ConversionResult.failure(notification.getErrors())
        : ConversionResult.success(record);
}
```

## Integration with Spark and Kafka Connect

For Spark DataFrame integration, use Iceberg's `SparkSchemaUtil` for bidirectional schema conversion:

```java
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.types.StructType;

// Convert Iceberg Schema to Spark StructType
StructType sparkSchema = SparkSchemaUtil.convert(icebergSchema);

// Convert rows to DataFrames for Iceberg writes
List<Row> rows = maps.stream()
    .map(map -> mapToRow(map, sparkSchema))
    .collect(Collectors.toList());

Dataset<Row> df = spark.createDataFrame(rows, sparkSchema);
df.write().format("iceberg").mode("append").save(tableLocation);
```

For Kafka Connect with the Iceberg sink connector, configure automatic schema evolution:

```json
{
  "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
  "iceberg.tables.auto-create-enabled": "true",
  "iceberg.tables.evolve-schema-enabled": "true",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081"
}
```

## Testing strategies for comprehensive type coverage

Property-based testing with **jqwik** validates converter correctness across input ranges:

```java
@Property
boolean roundTripPreservesValue(@ForAll @IntRange(min = -10000, max = 10000) int value) {
    Object converted = converter.convert(value, Types.IntegerType.get());
    return value == (int) converted;
}

@Property
boolean timestampConversionPreservesPrecision(
        @ForAll @LongRange(min = 0, max = 4102444800000000L) long micros) {
    Instant original = Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000);
    Long converted = timestampConverter.convert(original);
    return micros == converted;
}
```

Parameterized tests cover the full type matrix efficiently:

```java
@ParameterizedTest
@MethodSource("typePromotionCases")
void testTypePromotion(Object input, Type targetType, Object expected) {
    Object result = converter.convertWithPromotion(input, targetType);
    assertEquals(expected, result);
}

static Stream<Arguments> typePromotionCases() {
    return Stream.of(
        Arguments.of(123, Types.LongType.get(), 123L),
        Arguments.of(1.5f, Types.DoubleType.get(), 1.5d),
        Arguments.of((short) 100, Types.IntegerType.get(), 100)
    );
}
```

## Conclusion

Building a production-grade Map-to-Schema converter requires understanding both Iceberg and Avro type systems, pre-computing converters for performance, and handling the numerous edge cases that arise from diverse data sources. The **cached converter registry pattern** with `ConcurrentHashMap.computeIfAbsent()` provides thread-safe initialization without locking on the hot path. Avro's internal representations (Utf8, ByteBuffer) require explicit handling, while timestamp precision alignment between millisecond and microsecond systems demands careful conversion logic.

For robustness, implement the **Notification pattern** for error accumulation in batch scenarios, validate numeric ranges using `Math.toIntExact()` and similar methods, and enforce BigDecimal precision/scale constraints explicitly. Leverage Iceberg's built-in `GenericRecord.create(schema).copy(map)` for record creation, `TypeUtil.SchemaVisitor` for complex schema traversal, and `AvroSchemaUtil` for bidirectional schema conversion. Property-based testing with jqwik provides confidence across the full input space that unit tests cannot practically cover.