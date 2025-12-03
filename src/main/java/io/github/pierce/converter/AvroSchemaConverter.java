package io.github.pierce.converter;



import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Converts Map&lt;String, Object&gt; according to an Apache Avro Schema.
 *
 * <p>This class handles Avro's unique type representations including:
 * <ul>
 *   <li>Utf8 strings</li>
 *   <li>Union types (nullable fields)</li>
 *   <li>Logical types (decimal, date, timestamp-millis, timestamp-micros, uuid)</li>
 *   <li>Complex types (record, array, map, enum, fixed)</li>
 * </ul>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * String schemaJson = """
 *     {
 *         "type": "record",
 *         "name": "User",
 *         "fields": [
 *             {"name": "id", "type": "long"},
 *             {"name": "name", "type": ["null", "string"], "default": null},
 *             {"name": "balance", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}
 *         ]
 *     }
 *     """;
 *
 * Schema schema = new Schema.Parser().parse(schemaJson);
 * AvroSchemaConverter converter = AvroSchemaConverter.create(schema);
 *
 * Map<String, Object> data = Map.of(
 *     "id", 12345L,
 *     "name", "John Doe",
 *     "balance", new BigDecimal("1000.50")
 * );
 *
 * GenericRecord record = converter.convert(data);
 * }</pre>
 */
public class AvroSchemaConverter {

    private final org.apache.avro.Schema schema;
    private final ConversionConfig config;
    private final Map<String, TypeConverter<Object, Object>> fieldConverters;
    private final Map<String, org.apache.avro.Schema.Field> fieldsByName;
    private final Map<String, Boolean> fieldNullability;

    private static final ConcurrentHashMap<Integer, AvroSchemaConverter> CONVERTER_CACHE =
            new ConcurrentHashMap<>();

    private AvroSchemaConverter(org.apache.avro.Schema schema, ConversionConfig config) {
        if (schema.getType() != org.apache.avro.Schema.Type.RECORD) {
            throw new IllegalArgumentException("Schema must be a RECORD type, got: " + schema.getType());
        }

        this.schema = Objects.requireNonNull(schema, "Schema cannot be null");
        this.config = config != null ? config : ConversionConfig.defaults();
        this.fieldConverters = new LinkedHashMap<>();
        this.fieldsByName = new LinkedHashMap<>();
        this.fieldNullability = new LinkedHashMap<>();

        initializeConverters();
    }

    /**
     * Creates a new converter for the given Avro schema.
     */
    public static AvroSchemaConverter create(org.apache.avro.Schema schema) {
        return create(schema, ConversionConfig.defaults());
    }

    /**
     * Creates a new converter with custom configuration.
     */
    public static AvroSchemaConverter create(org.apache.avro.Schema schema, ConversionConfig config) {
        return new AvroSchemaConverter(schema, config);
    }

    /**
     * Gets a cached converter, creating one if necessary.
     */
    public static AvroSchemaConverter cached(org.apache.avro.Schema schema) {
        return cached(schema, ConversionConfig.defaults());
    }

    /**
     * Gets a cached converter with custom configuration.
     */
    public static AvroSchemaConverter cached(org.apache.avro.Schema schema, ConversionConfig config) {
        int key = System.identityHashCode(schema);
        return CONVERTER_CACHE.computeIfAbsent(key, k -> create(schema, config));
    }

    /**
     * Clears the converter cache.
     */
    public static void clearCache() {
        CONVERTER_CACHE.clear();
    }

    private void initializeConverters() {
        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            String name = field.name();
            fieldsByName.put(name, field);

            org.apache.avro.Schema fieldSchema = field.schema();
            boolean nullable = isNullable(fieldSchema);
            fieldNullability.put(name, nullable);

            // Get the non-null schema for union types
            org.apache.avro.Schema effectiveSchema = unwrapNullable(fieldSchema);

            TypeConverter<Object, Object> converter = createConverterForSchema(effectiveSchema);
            fieldConverters.put(name, converter);
        }
    }

    /**
     * Converts a Map to an Avro GenericRecord.
     */
    public GenericRecord convert(Map<String, Object> data) {
        if (data == null) {
            throw new NullValueException(null, "Input map cannot be null");
        }

        GenericRecord record = new GenericData.Record(schema);

        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            String name = field.name();
            Object value = data.get(name);

            // Handle missing fields
            if (value == null && !data.containsKey(name)) {
                if (field.hasDefaultValue()) {
                    // Use Avro's default value handling
                    record.put(name, GenericData.get().getDefaultValue(field));
                    continue;
                }
                if (!fieldNullability.get(name)) {
                    throw new NullValueException(name, "Required field is missing");
                }
                record.put(name, null);
                continue;
            }

            // Handle null values
            if (value == null) {
                if (!fieldNullability.get(name)) {
                    throw new NullValueException(name);
                }
                record.put(name, null);
                continue;
            }

            // Convert the value
            try {
                TypeConverter<Object, Object> converter = fieldConverters.get(name);
                Object converted = converter.convert(value);
                record.put(name, converted);
            } catch (SchemaConversionException e) {
                throw new TypeConversionException(name, value,
                        field.schema().toString(), e.getMessage(), e);
            }
        }

        return record;
    }

    /**
     * Converts a Map to a plain Map with Avro-compatible values.
     */
    public Map<String, Object> convertToMap(Map<String, Object> data) {
        if (data == null) {
            return null;
        }

        Map<String, Object> result = new LinkedHashMap<>();

        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            String name = field.name();
            Object value = data.get(name);

            if (value == null && !data.containsKey(name)) {
                if (field.hasDefaultValue()) {
                    result.put(name, GenericData.get().getDefaultValue(field));
                    continue;
                }
                if (!fieldNullability.get(name)) {
                    throw new NullValueException(name, "Required field is missing");
                }
                continue;
            }

            if (value == null) {
                if (!fieldNullability.get(name)) {
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
     * Converts a Map based on the configured output format.
     * If {@link ConversionConfig.OutputFormat#MAP} is configured, returns a Map.
     * Otherwise returns a GenericRecord.
     *
     * @param data the input map
     * @return the converted result (GenericRecord or Map depending on config)
     * @throws SchemaConversionException if conversion fails
     */
    public Object convertAuto(Map<String, Object> data) {
        if (config.getOutputFormat() == ConversionConfig.OutputFormat.MAP) {
            return convertToMap(data);
        }
        return convert(data);
    }

    /**
     * Batch converts multiple Maps based on the configured output format.
     *
     * @param dataList the list of input maps
     * @return list of converted results (GenericRecords or Maps depending on config)
     */
    public List<?> convertBatchAuto(List<Map<String, Object>> dataList) {
        if (config.getOutputFormat() == ConversionConfig.OutputFormat.MAP) {
            return convertBatchToMap(dataList);
        }
        return convertBatch(dataList);
    }

    /**
     * Batch converts multiple Maps to plain Maps with converted values.
     *
     * @param dataList the list of input maps
     * @return list of converted maps
     */
    public List<Map<String, Object>> convertBatchToMap(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return null;
        }
        List<Map<String, Object>> results = new ArrayList<>(dataList.size());
        for (Map<String, Object> data : dataList) {
            results.add(convertToMap(data));
        }
        return results;
    }

    /**
     * Converts with error collection.
     */
    public ConversionResult<GenericRecord> convertWithErrors(Map<String, Object> data) {
        if (data == null) {
            return ConversionResult.failure(
                    new ConversionResult.ConversionError(null, "Input map cannot be null"));
        }

        Notification notification = new Notification();
        GenericRecord record = new GenericData.Record(schema);

        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            String name = field.name();

            try (var ctx = notification.pushField(name)) {
                Object value = data.get(name);

                if (value == null && !data.containsKey(name)) {
                    if (field.hasDefaultValue()) {
                        record.put(name, GenericData.get().getDefaultValue(field));
                        continue;
                    }
                    if (!fieldNullability.get(name)) {
                        notification.addError("Required field is missing");
                    }
                    continue;
                }

                if (value == null) {
                    if (!fieldNullability.get(name)) {
                        notification.addError("Required field is null");
                    }
                    record.put(name, null);
                    continue;
                }

                try {
                    TypeConverter<Object, Object> converter = fieldConverters.get(name);
                    Object converted = converter.convert(value);
                    record.put(name, converted);
                } catch (SchemaConversionException e) {
                    notification.addError(e.getMessage(), e);
                }
            }
        }

        return notification.toResult(record);
    }

    /**
     * Batch converts multiple maps.
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

    /**
     * Checks if a schema is a union containing null.
     */
    private boolean isNullable(org.apache.avro.Schema schema) {
        if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .anyMatch(s -> s.getType() == org.apache.avro.Schema.Type.NULL);
        }
        return false;
    }

    /**
     * Unwraps a union schema to get the non-null type.
     */
    private org.apache.avro.Schema unwrapNullable(org.apache.avro.Schema schema) {
        if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .filter(s -> s.getType() != org.apache.avro.Schema.Type.NULL)
                    .findFirst()
                    .orElse(schema);
        }
        return schema;
    }

    @SuppressWarnings("unchecked")
    private TypeConverter<Object, Object> createConverterForSchema(org.apache.avro.Schema avroSchema) {
        // Check for logical types first
        LogicalType logicalType = avroSchema.getLogicalType();
        if (logicalType != null) {
            TypeConverter<?, ?> logicalConverter = createLogicalTypeConverter(avroSchema, logicalType);
            if (logicalConverter != null) {
                return (TypeConverter<Object, Object>) logicalConverter;
            }
        }

        TypeConverter<?, ?> converter = switch (avroSchema.getType()) {
            case BOOLEAN -> new BooleanConverter(config);
            case INT -> new IntegerConverter(config);
            case LONG -> new LongConverter(config);
            case FLOAT -> new FloatConverter(config);
            case DOUBLE -> new DoubleConverter(config);
            case STRING -> new AvroStringConverter(config);
            case BYTES -> new BinaryConverter(config);
            case FIXED -> BinaryConverter.forFixed(config, avroSchema.getFixedSize());
            case ENUM -> new AvroEnumConverter(config, avroSchema);
            case ARRAY -> {
                TypeConverter<Object, Object> elementConverter =
                        createConverterForSchema(avroSchema.getElementType());
                yield new ListConverter(config, elementConverter);
            }
            case MAP -> {
                TypeConverter<Object, Object> valueConverter =
                        createConverterForSchema(avroSchema.getValueType());
                // Avro map keys are always strings
                yield new MapConverter(config, new AvroStringConverter(config), valueConverter);
            }
            case RECORD -> createNestedRecordConverter(avroSchema);
            case UNION -> {
                // For complex unions (not just [null, type]), create a union converter
                List<org.apache.avro.Schema> types = avroSchema.getTypes().stream()
                        .filter(s -> s.getType() != org.apache.avro.Schema.Type.NULL)
                        .toList();
                if (types.size() == 1) {
                    yield createConverterForSchema(types.get(0));
                }
                yield new AvroUnionConverter(config, avroSchema, this);
            }
            case NULL -> new NullConverter();
        };

        return (TypeConverter<Object, Object>) converter;
    }

    private TypeConverter<?, ?> createLogicalTypeConverter(
            org.apache.avro.Schema schema, LogicalType logicalType) {

        String logicalTypeName = logicalType.getName();

        return switch (logicalTypeName) {
            case "decimal" -> {
                LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
                yield new DecimalConverter(config, decimal.getPrecision(), decimal.getScale());
            }
            case "date" -> new DateConverter(config);
            case "time-millis" -> new AvroTimeMillisConverter(config);
            case "time-micros" -> new TimeConverter(config);
            case "timestamp-millis" -> new AvroTimestampMillisConverter(config);
            case "timestamp-micros" -> new TimestampConverter(config, true);
            case "uuid" -> new UUIDConverter(config);
            case "local-timestamp-millis" -> new AvroTimestampMillisConverter(config);
            case "local-timestamp-micros" -> new TimestampConverter(config, false);
            default -> null; // Fall back to base type handling
        };
    }

    private StructConverter createNestedRecordConverter(org.apache.avro.Schema recordSchema) {
        List<StructConverter.FieldInfo> fieldInfos = new ArrayList<>();

        for (org.apache.avro.Schema.Field field : recordSchema.getFields()) {
            org.apache.avro.Schema fieldSchema = field.schema();
            boolean nullable = isNullable(fieldSchema);
            org.apache.avro.Schema effectiveSchema = unwrapNullable(fieldSchema);

            TypeConverter<Object, Object> fieldConverter = createConverterForSchema(effectiveSchema);

            Object defaultValue = field.hasDefaultValue()
                    ? GenericData.get().getDefaultValue(field)
                    : null;

            fieldInfos.add(StructConverter.FieldInfo.of(
                    field.name(),
                    fieldConverter,
                    !nullable,
                    defaultValue
            ));
        }

        return new StructConverter(config, fieldInfos);
    }

    public org.apache.avro.Schema getSchema() {
        return schema;
    }

    public ConversionConfig getConfig() {
        return config;
    }

    public TypeConverter<Object, Object> getFieldConverter(String fieldName) {
        return fieldConverters.get(fieldName);
    }

    public Set<String> getFieldNames() {
        return Collections.unmodifiableSet(fieldsByName.keySet());
    }

    public boolean isFieldNullable(String fieldName) {
        return fieldNullability.getOrDefault(fieldName, false);
    }

    // Inner converter classes for Avro-specific types

    /**
     * Converter that produces Avro Utf8 strings.
     */
    private static class AvroStringConverter extends StringConverter {
        public AvroStringConverter(ConversionConfig config) {
            super(config);
        }

        @Override
        protected String doConvert(Object value) {
            // Return String - Avro will handle Utf8 conversion
            return super.doConvert(value);
        }
    }

    /**
     * Converter for Avro enum types.
     */
    private static class AvroEnumConverter extends AbstractTypeConverter<GenericData.EnumSymbol> {
        private final org.apache.avro.Schema enumSchema;
        private final Set<String> validSymbols;

        public AvroEnumConverter(ConversionConfig config, org.apache.avro.Schema enumSchema) {
            super(config, "enum<" + enumSchema.getName() + ">");
            this.enumSchema = enumSchema;
            this.validSymbols = new HashSet<>(enumSchema.getEnumSymbols());
        }

        @Override
        protected GenericData.EnumSymbol doConvert(Object value) {
            String symbol;

            if (value instanceof GenericData.EnumSymbol es) {
                symbol = es.toString();
            } else if (value instanceof Enum<?> e) {
                symbol = e.name();
            } else {
                symbol = charSequenceToString(value);
                if (symbol == null) {
                    throw unsupportedType(value);
                }
            }

            if (!validSymbols.contains(symbol)) {
                throw conversionError(value,
                        "Invalid enum symbol '" + symbol + "'. Valid symbols: " + validSymbols);
            }

            return new GenericData.EnumSymbol(enumSchema, symbol);
        }
    }

    /**
     * Converter for Avro union types (non-nullable unions).
     */
    private static class AvroUnionConverter extends AbstractTypeConverter<Object> {
        private final org.apache.avro.Schema unionSchema;
        private final List<TypeConverter<Object, Object>> branchConverters;
        private final List<org.apache.avro.Schema> nonNullTypes;

        @SuppressWarnings("unchecked")
        public AvroUnionConverter(ConversionConfig config, org.apache.avro.Schema unionSchema,
                                  AvroSchemaConverter parent) {
            super(config, "union");
            this.unionSchema = unionSchema;
            this.nonNullTypes = unionSchema.getTypes().stream()
                    .filter(s -> s.getType() != org.apache.avro.Schema.Type.NULL)
                    .toList();
            this.branchConverters = new ArrayList<>();

            for (org.apache.avro.Schema branchSchema : nonNullTypes) {
                branchConverters.add(parent.createConverterForSchema(branchSchema));
            }
        }

        @Override
        protected Object doConvert(Object value) {
            // Try each branch converter until one succeeds
            List<Exception> errors = new ArrayList<>();

            for (int i = 0; i < branchConverters.size(); i++) {
                try {
                    return branchConverters.get(i).convert(value);
                } catch (Exception e) {
                    errors.add(e);
                }
            }

            throw conversionError(value,
                    "Value does not match any union branch. Tried: " + nonNullTypes);
        }
    }

    /**
     * Converter for Avro time-millis (milliseconds since midnight).
     */
    private static class AvroTimeMillisConverter extends AbstractTypeConverter<Integer> {
        public AvroTimeMillisConverter(ConversionConfig config) {
            super(config, "time-millis");
        }

        @Override
        protected Integer doConvert(Object value) {
            TimeConverter timeConverter = new TimeConverter(config);
            Long micros = timeConverter.convert(value);
            if (micros == null) return null;
            return (int) (micros / 1000); // Convert micros to millis
        }
    }

    /**
     * Converter for Avro timestamp-millis.
     */
    private static class AvroTimestampMillisConverter extends AbstractTypeConverter<Long> {
        public AvroTimestampMillisConverter(ConversionConfig config) {
            super(config, "timestamp-millis");
        }

        @Override
        protected Long doConvert(Object value) {
            TimestampConverter tsConverter = new TimestampConverter(config, true);
            Long micros = tsConverter.convert(value);
            if (micros == null) return null;
            return micros / 1000; // Convert micros to millis
        }
    }

    /**
     * Converter that always returns null.
     */
    private static class NullConverter implements TypeConverter<Object, Object> {
        @Override
        public Object convert(Object value) {
            return null;
        }

        @Override
        public String getTargetTypeName() {
            return "null";
        }
    }
}