package io.github.pierce.converter;


import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Converts Map&lt;String, Object&gt; to Iceberg GenericRecord according to an Iceberg Schema.
 *
 * <p>This class is thread-safe and caches converters per schema for optimal performance.
 * It handles all Iceberg types including nested structures (struct, list, map) and
 * properly converts values from various input formats (Avro GenericRecord output,
 * JSON deserialization, etc.).</p>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * Schema schema = new Schema(
 *     Types.NestedField.required(1, "id", Types.LongType.get()),
 *     Types.NestedField.optional(2, "name", Types.StringType.get()),
 *     Types.NestedField.optional(3, "amount", Types.DecimalType.of(10, 2))
 * );
 *
 * IcebergSchemaConverter converter = IcebergSchemaConverter.create(schema);
 *
 * Map<String, Object> data = Map.of(
 *     "id", 12345L,
 *     "name", "Test",
 *     "amount", new BigDecimal("99.99")
 * );
 *
 * GenericRecord record = converter.convert(data);
 * }</pre>
 */
public class IcebergSchemaConverter {

    private final Schema schema;
    private final ConversionConfig config;
    private final TypeConverterRegistry registry;
    private final Map<String, TypeConverter<Object, Object>> fieldConverters;
    private final Map<String, Types.NestedField> fieldsByName;

    // Cache for schema fingerprint -> converter instances (static, shared across instances)
    private static final ConcurrentHashMap<Integer, IcebergSchemaConverter> CONVERTER_CACHE =
            new ConcurrentHashMap<>();

    private IcebergSchemaConverter(Schema schema, ConversionConfig config) {
        this.schema = Objects.requireNonNull(schema, "Schema cannot be null");
        this.config = config != null ? config : ConversionConfig.defaults();
        this.registry = new TypeConverterRegistry(this.config);
        this.fieldConverters = new LinkedHashMap<>();
        this.fieldsByName = new LinkedHashMap<>();

        initializeConverters();
    }

    /**
     * Creates a new converter for the given schema with default configuration.
     */
    public static IcebergSchemaConverter create(Schema schema) {
        return create(schema, ConversionConfig.defaults());
    }

    /**
     * Creates a new converter for the given schema with custom configuration.
     */
    public static IcebergSchemaConverter create(Schema schema, ConversionConfig config) {
        return new IcebergSchemaConverter(schema, config);
    }

    /**
     * Gets a cached converter for the schema, creating one if necessary.
     * Uses the schema's identity hash code as the cache key.
     */
    public static IcebergSchemaConverter cached(Schema schema) {
        return cached(schema, ConversionConfig.defaults());
    }

    /**
     * Gets a cached converter for the schema with custom configuration.
     */
    public static IcebergSchemaConverter cached(Schema schema, ConversionConfig config) {
        int key = System.identityHashCode(schema);
        return CONVERTER_CACHE.computeIfAbsent(key, k -> create(schema, config));
    }

    /**
     * Clears the static converter cache.
     */
    public static void clearCache() {
        CONVERTER_CACHE.clear();
    }

    private void initializeConverters() {
        for (Types.NestedField field : schema.columns()) {
            String name = field.name();
            fieldsByName.put(name, field);
            TypeConverter<Object, Object> converter = createConverterForType(field.type());
            fieldConverters.put(name, converter);
        }
    }

    /**
     * Converts a Map to an Iceberg GenericRecord.
     *
     * @param data the input map
     * @return the converted GenericRecord
     * @throws SchemaConversionException if conversion fails
     */
    public GenericRecord convert(Map<String, Object> data) {
        if (data == null) {
            throw new NullValueException(null, "Input map cannot be null");
        }

        GenericRecord record = GenericRecord.create(schema);

        for (Types.NestedField field : schema.columns()) {
            String name = field.name();
            Object value = data.get(name);

            // Handle missing fields
            if (value == null && !data.containsKey(name)) {
                if (!field.isOptional()) {
                    throw new NullValueException(name, "Required field is missing");
                }
                record.setField(name, null);
                continue;
            }

            // Handle null values
            if (value == null) {
                if (!field.isOptional()) {
                    throw new NullValueException(name);
                }
                record.setField(name, null);
                continue;
            }

            // Convert the value
            try {
                TypeConverter<Object, Object> converter = fieldConverters.get(name);
                Object converted = converter.convert(value);
                record.setField(name, converted);
            } catch (SchemaConversionException e) {
                if (e.getFieldPath() == null || e.getFieldPath().isEmpty()) {
                    throw new TypeConversionException(name, value,
                            field.type().toString(), e.getMessage(), e);
                }
                throw new TypeConversionException(name + "." + e.getFieldPath(),
                        value, field.type().toString(), e.getMessage(), e);
            }
        }

        return record;
    }

    /**
     * Converts a Map to a plain Map with converted values.
     * Useful when you need the converted values without creating a GenericRecord.
     *
     * @param data the input map
     * @return a new map with converted values
     */
    public Map<String, Object> convertToMap(Map<String, Object> data) {
        if (data == null) {
            return null;
        }

        Map<String, Object> result = new LinkedHashMap<>();

        for (Types.NestedField field : schema.columns()) {
            String name = field.name();
            Object value = data.get(name);

            if (value == null && !data.containsKey(name)) {
                if (!field.isOptional()) {
                    throw new NullValueException(name, "Required field is missing");
                }
                continue;
            }

            if (value == null) {
                if (!field.isOptional()) {
                    throw new NullValueException(name);
                }
                result.put(name, null);
                continue;
            }

            TypeConverter<Object, Object> converter = fieldConverters.get(name);
            Object converted = converter.convert(value, name);
            result.put(name, converted);
        }

        return result;
    }

    /**
     * Converts with error collection instead of fail-fast.
     *
     * @param data the input map
     * @return a ConversionResult containing either the record or errors
     */
    public ConversionResult<GenericRecord> convertWithErrors(Map<String, Object> data) {
        if (data == null) {
            return ConversionResult.failure(
                    new ConversionResult.ConversionError(null, "Input map cannot be null"));
        }

        Notification notification = new Notification();
        GenericRecord record = GenericRecord.create(schema);

        for (Types.NestedField field : schema.columns()) {
            String name = field.name();

            try (var ctx = notification.pushField(name)) {
                Object value = data.get(name);

                if (value == null && !data.containsKey(name)) {
                    if (!field.isOptional()) {
                        notification.addError("Required field is missing");
                    }
                    continue;
                }

                if (value == null) {
                    if (!field.isOptional()) {
                        notification.addError("Required field is null");
                    }
                    record.setField(name, null);
                    continue;
                }

                try {
                    TypeConverter<Object, Object> converter = fieldConverters.get(name);
                    Object converted = converter.convert(value);
                    record.setField(name, converted);
                } catch (SchemaConversionException e) {
                    notification.addError(e.getMessage(), e);
                }
            }
        }

        return notification.toResult(record);
    }

    /**
     * Batch converts multiple maps.
     *
     * @param dataList list of input maps
     * @return list of converted records
     */
    public List<GenericRecord> convertBatch(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return Collections.emptyList();
        }

        List<GenericRecord> results = new ArrayList<>(dataList.size());
        for (Map<String, Object> data : dataList) {
            results.add(convert(data));
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    private TypeConverter<Object, Object> createConverterForType(Type type) {
        TypeConverter<?, ?> converter = switch (type.typeId()) {
            case BOOLEAN -> new BooleanConverter(config);
            case INTEGER -> new IntegerConverter(config);
            case LONG -> new LongConverter(config);
            case FLOAT -> new FloatConverter(config);
            case DOUBLE -> new DoubleConverter(config);
            case STRING -> new StringConverter(config);
            case DATE -> new DateConverter(config);
            case TIME -> new TimeConverter(config);
            case TIMESTAMP -> {
                Types.TimestampType ts = (Types.TimestampType) type;
                yield new TimestampConverter(config, ts.shouldAdjustToUTC());
            }
            case TIMESTAMP_NANO -> {
                Types.TimestampNanoType tsNano = (Types.TimestampNanoType) type;
                yield new TimestampNanoConverter(config, tsNano.shouldAdjustToUTC());
            }
            case UUID -> new UUIDConverter(config);
            case BINARY -> new BinaryConverter(config);
            case FIXED -> {
                Types.FixedType fixed = (Types.FixedType) type;
                yield BinaryConverter.forFixed(config, fixed.length());
            }
            case DECIMAL -> {
                Types.DecimalType decimal = (Types.DecimalType) type;
                yield new DecimalConverter(config, decimal.precision(), decimal.scale());
            }
            case LIST -> {
                Types.ListType listType = (Types.ListType) type;
                TypeConverter<Object, Object> elementConverter =
                        createConverterForType(listType.elementType());
                yield new ListConverter(config, elementConverter);
            }
            case MAP -> {
                Types.MapType mapType = (Types.MapType) type;
                TypeConverter<Object, Object> keyConverter =
                        createConverterForType(mapType.keyType());
                TypeConverter<Object, Object> valueConverter =
                        createConverterForType(mapType.valueType());
                yield new MapConverter(config, keyConverter, valueConverter);
            }
            case STRUCT -> {
                Types.StructType structType = (Types.StructType) type;
                yield createStructConverter(structType);
            }
        };

        return (TypeConverter<Object, Object>) converter;
    }

    private StructConverter createStructConverter(Types.StructType structType) {
        List<StructConverter.FieldInfo> fieldInfos = new ArrayList<>();

        for (Types.NestedField nestedField : structType.fields()) {
            TypeConverter<Object, Object> fieldConverter =
                    createConverterForType(nestedField.type());
            fieldInfos.add(StructConverter.FieldInfo.of(
                    nestedField.name(),
                    fieldConverter,
                    !nestedField.isOptional()
            ));
        }

        return new StructConverter(config, fieldInfos);
    }

    /**
     * Returns the schema this converter was created for.
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Returns the configuration used by this converter.
     */
    public ConversionConfig getConfig() {
        return config;
    }

    /**
     * Returns the converter for a specific field.
     */
    public TypeConverter<Object, Object> getFieldConverter(String fieldName) {
        return fieldConverters.get(fieldName);
    }

    /**
     * Returns all field names in schema order.
     */
    public Set<String> getFieldNames() {
        return Collections.unmodifiableSet(fieldsByName.keySet());
    }
}