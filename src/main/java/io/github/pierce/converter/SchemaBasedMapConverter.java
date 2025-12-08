package io.github.pierce.converter;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Unified schema-based map converter that casts map values according to a schema.
 *
 * <p>This class provides a single entry point for converting Map values based on
 * Avro schemas, Iceberg schemas, or flattened schema definitions, with support for:
 * <ul>
 *   <li>Case-insensitive key matching</li>
 *   <li>Partial map conversion (not all schema fields required)</li>
 *   <li>Multiple schema types (Avro, Iceberg, Flattened)</li>
 *   <li>Flattened schema support for streaming/ETL pipelines</li>
 *   <li>Thread-safe operation with converter caching</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // With Iceberg Schema
 * Schema icebergSchema = new Schema(
 *     Types.NestedField.required(1, "userId", Types.LongType.get()),
 *     Types.NestedField.optional(2, "Amount", Types.DecimalType.of(10, 2)),
 *     Types.NestedField.optional(3, "active", Types.BooleanType.get())
 * );
 *
 * SchemaBasedMapConverter converter = SchemaBasedMapConverter.forIceberg(icebergSchema);
 *
 * // Keys are matched case-insensitively
 * Map<String, Object> input = Map.of(
 *     "USERID", "12345",      // String -> Long
 *     "amount", "99.99",      // String -> BigDecimal
 *     "ACTIVE", "true"        // String -> Boolean
 * );
 *
 * Map<String, Object> converted = converter.convert(input);
 * // Result: {userId=12345L, Amount=99.99, active=true}
 *
 * // With Avro Schema
 * org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schemaJson);
 * SchemaBasedMapConverter avroConverter = SchemaBasedMapConverter.forAvro(avroSchema);
 * Map<String, Object> result = avroConverter.convert(inputMap);
 * }</pre>
 *
 * <h2>Key Matching Behavior:</h2>
 * <ul>
 *   <li>Keys are matched case-insensitively by default</li>
 *   <li>Output keys use the schema's field names (preserving original case)</li>
 *   <li>Input keys not in schema are ignored</li>
 *   <li>Schema fields missing from input are not included in output</li>
 * </ul>
 */
public class SchemaBasedMapConverter {

    /**
     * Schema type enumeration.
     */
    public enum SchemaType {
        AVRO,
        ICEBERG,
        FLATTENED
    }

    /**
     * Data types for flattened schema fields.
     * Matches common types from Avro/Iceberg/Glue schemas.
     */
    public enum FlattenedDataType {
        STRING,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        BYTES,
        BIGINT,
        DECIMAL,
        TIMESTAMP,
        DATE,
        UUID
    }

    /**
     * Represents a flattened field with type information.
     * Used for pre-flattened schemas in streaming/ETL pipelines.
     */
    public static class FlattenedFieldType {
        private final String flattenedName;
        private final FlattenedDataType dataType;
        private final boolean isArraySerialized;
        private final FlattenedDataType arrayElementType;
        private final boolean nullable;
        private final Integer precision;  // For DECIMAL
        private final Integer scale;      // For DECIMAL

        public FlattenedFieldType(String flattenedName, FlattenedDataType dataType, boolean nullable) {
            this(flattenedName, dataType, false, null, nullable, null, null);
        }

        public FlattenedFieldType(String flattenedName, FlattenedDataType dataType,
                                  boolean isArraySerialized, FlattenedDataType arrayElementType,
                                  boolean nullable) {
            this(flattenedName, dataType, isArraySerialized, arrayElementType, nullable, null, null);
        }

        public FlattenedFieldType(String flattenedName, FlattenedDataType dataType,
                                  boolean isArraySerialized, FlattenedDataType arrayElementType,
                                  boolean nullable, Integer precision, Integer scale) {
            this.flattenedName = flattenedName;
            this.dataType = dataType;
            this.isArraySerialized = isArraySerialized;
            this.arrayElementType = arrayElementType;
            this.nullable = nullable;
            this.precision = precision;
            this.scale = scale;
        }

        public static FlattenedFieldType of(String name, FlattenedDataType type) {
            return new FlattenedFieldType(name, type, true);
        }

        public static FlattenedFieldType required(String name, FlattenedDataType type) {
            return new FlattenedFieldType(name, type, false);
        }

        public static FlattenedFieldType decimal(String name, int precision, int scale, boolean nullable) {
            return new FlattenedFieldType(name, FlattenedDataType.DECIMAL, false, null, nullable, precision, scale);
        }

        public static FlattenedFieldType array(String name, FlattenedDataType elementType, boolean nullable) {
            return new FlattenedFieldType(name, FlattenedDataType.STRING, true, elementType, nullable);
        }

        public String getFlattenedName() { return flattenedName; }
        public FlattenedDataType getDataType() { return dataType; }
        public boolean isArraySerialized() { return isArraySerialized; }
        public FlattenedDataType getArrayElementType() { return arrayElementType; }
        public boolean isNullable() { return nullable; }
        public Integer getPrecision() { return precision; }
        public Integer getScale() { return scale; }

        @Override
        public String toString() {
            if (isArraySerialized) {
                return String.format("%s (array<%s> as %s)%s",
                        flattenedName, arrayElementType, dataType, nullable ? " [nullable]" : "");
            }
            return String.format("%s (%s)%s", flattenedName, dataType, nullable ? " [nullable]" : "");
        }
    }

    private final SchemaType schemaType;
    private final Object schema; // Either org.apache.avro.Schema, org.apache.iceberg.Schema, or Map<String, FlattenedFieldType>
    private final ConversionConfig config;
    private final boolean caseInsensitive;

    // Field name -> converter mapping
    private final Map<String, TypeConverter<Object, Object>> fieldConverters;

    // Lowercase field name -> actual field name (for case-insensitive lookup)
    private final Map<String, String> fieldNameLookup;

    // Field name -> nullability
    private final Map<String, Boolean> fieldNullability;

    // For flattened schemas: field name -> FlattenedFieldType
    private final Map<String, FlattenedFieldType> flattenedSchemaFields;

    // Static converter cache
    private static final ConcurrentHashMap<String, SchemaBasedMapConverter> CONVERTER_CACHE =
            new ConcurrentHashMap<>();

    private SchemaBasedMapConverter(Object schema, SchemaType schemaType,
                                    ConversionConfig config, boolean caseInsensitive) {
        this.schema = Objects.requireNonNull(schema, "Schema cannot be null");
        this.schemaType = schemaType;
        this.config = config != null ? config : ConversionConfig.defaults();
        this.caseInsensitive = caseInsensitive;
        this.fieldConverters = new LinkedHashMap<>();
        this.fieldNameLookup = new LinkedHashMap<>();
        this.fieldNullability = new LinkedHashMap<>();
        this.flattenedSchemaFields = schemaType == SchemaType.FLATTENED
                ? new LinkedHashMap<>((Map<String, FlattenedFieldType>) schema)
                : Collections.emptyMap();

        initializeConverters();
    }

    // ==================== Factory Methods ====================

    /**
     * Creates a converter for an Iceberg schema with case-insensitive matching.
     */
    public static SchemaBasedMapConverter forIceberg(Schema schema) {
        return forIceberg(schema, ConversionConfig.defaults(), true);
    }

    /**
     * Creates a converter for an Iceberg schema with custom configuration.
     */
    public static SchemaBasedMapConverter forIceberg(Schema schema, ConversionConfig config) {
        return forIceberg(schema, config, true);
    }

    /**
     * Creates a converter for an Iceberg schema with full control.
     */
    public static SchemaBasedMapConverter forIceberg(Schema schema, ConversionConfig config,
                                                     boolean caseInsensitive) {
        return new SchemaBasedMapConverter(schema, SchemaType.ICEBERG, config, caseInsensitive);
    }

    /**
     * Creates a converter for an Avro schema with case-insensitive matching.
     */
    public static SchemaBasedMapConverter forAvro(org.apache.avro.Schema schema) {
        return forAvro(schema, ConversionConfig.defaults(), true);
    }

    /**
     * Creates a converter for an Avro schema with custom configuration.
     */
    public static SchemaBasedMapConverter forAvro(org.apache.avro.Schema schema,
                                                  ConversionConfig config) {
        return forAvro(schema, config, true);
    }

    /**
     * Creates a converter for an Avro schema with full control.
     */
    public static SchemaBasedMapConverter forAvro(org.apache.avro.Schema schema,
                                                  ConversionConfig config,
                                                  boolean caseInsensitive) {
        if (schema.getType() != org.apache.avro.Schema.Type.RECORD) {
            throw new IllegalArgumentException("Avro schema must be RECORD type, got: " + schema.getType());
        }
        return new SchemaBasedMapConverter(schema, SchemaType.AVRO, config, caseInsensitive);
    }

    // ==================== Flattened Schema Factory Methods ====================

    /**
     * Creates a converter for a flattened schema with case-insensitive matching.
     *
     * <p>Flattened schemas are useful for streaming/ETL pipelines where schemas
     * have been pre-flattened (e.g., by JsonFlattenerConsolidator or similar).</p>
     *
     * @param flattenedSchema map of field names to their type definitions
     */
    public static SchemaBasedMapConverter forFlattened(Map<String, FlattenedFieldType> flattenedSchema) {
        return forFlattened(flattenedSchema, ConversionConfig.defaults(), true);
    }

    /**
     * Creates a converter for a flattened schema with custom configuration.
     */
    public static SchemaBasedMapConverter forFlattened(Map<String, FlattenedFieldType> flattenedSchema,
                                                       ConversionConfig config) {
        return forFlattened(flattenedSchema, config, true);
    }

    /**
     * Creates a converter for a flattened schema with full control.
     */
    public static SchemaBasedMapConverter forFlattened(Map<String, FlattenedFieldType> flattenedSchema,
                                                       ConversionConfig config,
                                                       boolean caseInsensitive) {
        if (flattenedSchema == null || flattenedSchema.isEmpty()) {
            throw new IllegalArgumentException("Flattened schema cannot be null or empty");
        }
        return new SchemaBasedMapConverter(flattenedSchema, SchemaType.FLATTENED, config, caseInsensitive);
    }

    /**
     * Creates a flattened schema converter from an Avro schema by first flattening it.
     * This is a convenience method that combines schema flattening with conversion.
     */
    public static SchemaBasedMapConverter forFlattenedAvro(org.apache.avro.Schema avroSchema) {
        return forFlattenedAvro(avroSchema, "_", ConversionConfig.defaults(), true);
    }

    /**
     * Creates a flattened schema converter from an Avro schema with separator control.
     */
    public static SchemaBasedMapConverter forFlattenedAvro(org.apache.avro.Schema avroSchema,
                                                           String separator) {
        return forFlattenedAvro(avroSchema, separator, ConversionConfig.defaults(), true);
    }

    /**
     * Creates a flattened schema converter from an Avro schema with full control.
     */
    public static SchemaBasedMapConverter forFlattenedAvro(org.apache.avro.Schema avroSchema,
                                                           String separator,
                                                           ConversionConfig config,
                                                           boolean caseInsensitive) {
        Map<String, FlattenedFieldType> flattenedSchema = flattenAvroSchema(avroSchema, separator);
        return forFlattened(flattenedSchema, config, caseInsensitive);
    }

    /**
     * Creates a flattened schema converter from an Iceberg schema.
     */
    public static SchemaBasedMapConverter forFlattenedIceberg(Schema icebergSchema) {
        return forFlattenedIceberg(icebergSchema, "_", ConversionConfig.defaults(), true);
    }

    /**
     * Creates a flattened schema converter from an Iceberg schema with full control.
     */
    public static SchemaBasedMapConverter forFlattenedIceberg(Schema icebergSchema,
                                                              String separator,
                                                              ConversionConfig config,
                                                              boolean caseInsensitive) {
        Map<String, FlattenedFieldType> flattenedSchema = flattenIcebergSchema(icebergSchema, separator);
        return forFlattened(flattenedSchema, config, caseInsensitive);
    }

    /**
     * Gets a cached converter for the schema (using schema fingerprint as key).
     */
    public static SchemaBasedMapConverter cached(Schema icebergSchema) {
        String key = "iceberg:" + System.identityHashCode(icebergSchema);
        return CONVERTER_CACHE.computeIfAbsent(key,
                k -> forIceberg(icebergSchema));
    }

    /**
     * Gets a cached converter for the Avro schema.
     */
    public static SchemaBasedMapConverter cached(org.apache.avro.Schema avroSchema) {
        String key = "avro:" + System.identityHashCode(avroSchema);
        return CONVERTER_CACHE.computeIfAbsent(key,
                k -> forAvro(avroSchema));
    }

    /**
     * Gets a cached converter for a flattened schema.
     */
    public static SchemaBasedMapConverter cached(Map<String, FlattenedFieldType> flattenedSchema) {
        String key = "flattened:" + System.identityHashCode(flattenedSchema);
        return CONVERTER_CACHE.computeIfAbsent(key,
                k -> forFlattened(flattenedSchema));
    }

    /**
     * Clears the converter cache.
     */
    public static void clearCache() {
        CONVERTER_CACHE.clear();
    }

    // ==================== Initialization ====================

    private void initializeConverters() {
        if (schemaType == SchemaType.ICEBERG) {
            initializeIcebergConverters();
        } else if (schemaType == SchemaType.AVRO) {
            initializeAvroConverters();
        } else if (schemaType == SchemaType.FLATTENED) {
            initializeFlattenedConverters();
        }
    }

    private void initializeIcebergConverters() {
        Schema icebergSchema = (Schema) schema;

        for (Types.NestedField field : icebergSchema.columns()) {
            String fieldName = field.name();
            fieldNameLookup.put(fieldName.toLowerCase(), fieldName);
            fieldNullability.put(fieldName, field.isOptional());

            TypeConverter<Object, Object> converter = createIcebergConverter(field.type());
            fieldConverters.put(fieldName, converter);
        }
    }

    private void initializeAvroConverters() {
        org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) schema;

        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            String fieldName = field.name();
            fieldNameLookup.put(fieldName.toLowerCase(), fieldName);

            org.apache.avro.Schema fieldSchema = field.schema();
            boolean nullable = isAvroNullable(fieldSchema);
            fieldNullability.put(fieldName, nullable);

            org.apache.avro.Schema effectiveSchema = unwrapAvroNullable(fieldSchema);
            TypeConverter<Object, Object> converter = createAvroConverter(effectiveSchema);
            fieldConverters.put(fieldName, converter);
        }
    }

    @SuppressWarnings("unchecked")
    private void initializeFlattenedConverters() {
        Map<String, FlattenedFieldType> flatSchema = (Map<String, FlattenedFieldType>) schema;

        for (Map.Entry<String, FlattenedFieldType> entry : flatSchema.entrySet()) {
            String fieldName = entry.getKey();
            FlattenedFieldType fieldType = entry.getValue();

            fieldNameLookup.put(fieldName.toLowerCase(), fieldName);
            fieldNullability.put(fieldName, fieldType.isNullable());

            TypeConverter<Object, Object> converter = createFlattenedConverter(fieldType);
            fieldConverters.put(fieldName, converter);
        }
    }

    @SuppressWarnings("unchecked")
    private TypeConverter<Object, Object> createFlattenedConverter(FlattenedFieldType fieldType) {
        // If it's a serialized array, use a special converter (no cast needed - it implements TypeConverter<Object, Object>)
        if (fieldType.isArraySerialized()) {
            return new SerializedArrayConverter(config, fieldType.getArrayElementType());
        }

        TypeConverter<?, ?> converter = switch (fieldType.getDataType()) {
            case STRING -> new StringConverter(config);
            case INT -> new IntegerConverter(config);
            case LONG, BIGINT -> new LongConverter(config);
            case FLOAT -> new FloatConverter(config);
            case DOUBLE -> new DoubleConverter(config);
            case BOOLEAN -> new BooleanConverter(config);
            case DECIMAL -> new DecimalConverter(config,
                    fieldType.getPrecision() != null ? fieldType.getPrecision() : 38,
                    fieldType.getScale() != null ? fieldType.getScale() : 0);
            case DATE -> new DateConverter(config);
            case TIMESTAMP -> new TimestampConverter(config, true);
            case UUID -> new UUIDConverter(config);
            case BYTES -> new BinaryConverter(config);
        };

        return (TypeConverter<Object, Object>) converter;
    }

    // ==================== Conversion Methods ====================

    /**
     * Converts a map's values according to the schema.
     *
     * <p>Only keys that match schema fields (case-insensitively by default) are converted.
     * Output keys use the schema's original field names.</p>
     *
     * @param data the input map (can have zero or more keys from schema)
     * @return a new map with converted values using schema field names as keys
     */
    public Map<String, Object> convert(Map<String, Object> data) {
        if (data == null) {
            return null;
        }

        if (data.isEmpty()) {
            return new LinkedHashMap<>();
        }

        Map<String, Object> result = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String inputKey = entry.getKey();
            Object value = entry.getValue();

            // Find matching schema field
            String schemaFieldName = findMatchingField(inputKey);

            if (schemaFieldName == null) {
                // Key doesn't match any schema field - skip it
                continue;
            }

            // Handle null values
            if (value == null) {
                if (fieldNullability.getOrDefault(schemaFieldName, true)) {
                    result.put(schemaFieldName, null);
                }
                // If field is required and value is null, skip (don't throw - partial maps allowed)
                continue;
            }

            // Convert the value
            try {
                TypeConverter<Object, Object> converter = fieldConverters.get(schemaFieldName);
                Object converted = converter.convert(value);
                result.put(schemaFieldName, converted);
            } catch (SchemaConversionException e) {
                throw new TypeConversionException(schemaFieldName, value,
                        getFieldTypeName(schemaFieldName), e.getMessage(), e);
            }
        }

        return result;
    }

    /**
     * Converts a map and validates that all required fields are present.
     *
     * @param data the input map
     * @return a new map with converted values
     * @throws NullValueException if a required field is missing or null
     */
    public Map<String, Object> convertStrict(Map<String, Object> data) {
        if (data == null) {
            throw new NullValueException(null, "Input map cannot be null");
        }

        Map<String, Object> result = new LinkedHashMap<>();
        Set<String> processedFields = new HashSet<>();

        // Process input data
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String inputKey = entry.getKey();
            Object value = entry.getValue();

            String schemaFieldName = findMatchingField(inputKey);

            if (schemaFieldName == null) {
                continue;
            }

            processedFields.add(schemaFieldName);

            if (value == null) {
                if (!fieldNullability.getOrDefault(schemaFieldName, true)) {
                    throw new NullValueException(schemaFieldName);
                }
                result.put(schemaFieldName, null);
                continue;
            }

            TypeConverter<Object, Object> converter = fieldConverters.get(schemaFieldName);
            Object converted = converter.convert(value);
            result.put(schemaFieldName, converted);
        }

        // Check for missing required fields
        for (String fieldName : fieldConverters.keySet()) {
            if (!processedFields.contains(fieldName) &&
                    !fieldNullability.getOrDefault(fieldName, true)) {
                throw new NullValueException(fieldName, "Required field is missing");
            }
        }

        return result;
    }

    /**
     * Converts a map with error collection instead of fail-fast.
     *
     * @param data the input map
     * @return a ConversionResult containing either the result map or errors
     */
    public ConversionResult<Map<String, Object>> convertWithErrors(Map<String, Object> data) {
        if (data == null) {
            return ConversionResult.failure(
                    new ConversionResult.ConversionError(null, "Input map cannot be null"));
        }

        Notification notification = new Notification();
        Map<String, Object> result = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String inputKey = entry.getKey();
            Object value = entry.getValue();

            String schemaFieldName = findMatchingField(inputKey);

            if (schemaFieldName == null) {
                continue;
            }

            try (var ctx = notification.pushField(schemaFieldName)) {
                if (value == null) {
                    if (fieldNullability.getOrDefault(schemaFieldName, true)) {
                        result.put(schemaFieldName, null);
                    }
                    continue;
                }

                try {
                    TypeConverter<Object, Object> converter = fieldConverters.get(schemaFieldName);
                    Object converted = converter.convert(value);
                    result.put(schemaFieldName, converted);
                } catch (SchemaConversionException e) {
                    notification.addError(e.getMessage(), e);
                }
            }
        }

        return notification.toResult(result);
    }

    /**
     * Batch converts multiple maps.
     *
     * @param dataList list of input maps
     * @return list of converted maps
     */
    public List<Map<String, Object>> convertBatch(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> results = new ArrayList<>(dataList.size());
        for (Map<String, Object> data : dataList) {
            results.add(convert(data));
        }
        return results;
    }

    /**
     * Converts a single field value by field name.
     *
     * @param fieldName the schema field name (case-insensitive match)
     * @param value the value to convert
     * @return the converted value
     */
    public Object convertField(String fieldName, Object value) {
        String schemaFieldName = findMatchingField(fieldName);

        if (schemaFieldName == null) {
            throw new IllegalArgumentException("Field not found in schema: " + fieldName);
        }

        if (value == null) {
            return null;
        }

        TypeConverter<Object, Object> converter = fieldConverters.get(schemaFieldName);
        return converter.convert(value);
    }

    // ==================== Helper Methods ====================

    private String findMatchingField(String inputKey) {
        if (inputKey == null) {
            return null;
        }

        if (caseInsensitive) {
            return fieldNameLookup.get(inputKey.toLowerCase());
        } else {
            return fieldConverters.containsKey(inputKey) ? inputKey : null;
        }
    }

    private String getFieldTypeName(String fieldName) {
        if (schemaType == SchemaType.ICEBERG) {
            Schema icebergSchema = (Schema) schema;
            Types.NestedField field = icebergSchema.findField(fieldName);
            return field != null ? field.type().toString() : "unknown";
        } else if (schemaType == SchemaType.AVRO) {
            org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) schema;
            org.apache.avro.Schema.Field field = avroSchema.getField(fieldName);
            return field != null ? field.schema().toString() : "unknown";
        } else {
            // FLATTENED schema
            FlattenedFieldType fieldType = flattenedSchemaFields.get(fieldName);
            return fieldType != null ? fieldType.toString() : "unknown";
        }
    }

    // ==================== Iceberg Converter Creation ====================

    @SuppressWarnings("unchecked")
    private TypeConverter<Object, Object> createIcebergConverter(Type type) {
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
                TypeConverter<Object, Object> elementConverter = createIcebergConverter(listType.elementType());
                yield new ListConverter(config, elementConverter);
            }
            case MAP -> {
                Types.MapType mapType = (Types.MapType) type;
                TypeConverter<Object, Object> keyConverter = createIcebergConverter(mapType.keyType());
                TypeConverter<Object, Object> valueConverter = createIcebergConverter(mapType.valueType());
                yield new MapConverter(config, keyConverter, valueConverter);
            }
            case STRUCT -> {
                Types.StructType structType = (Types.StructType) type;
                yield createIcebergStructConverter(structType);
            }
        };

        return (TypeConverter<Object, Object>) converter;
    }

    private StructConverter createIcebergStructConverter(Types.StructType structType) {
        List<StructConverter.FieldInfo> fieldInfos = new ArrayList<>();

        for (Types.NestedField nestedField : structType.fields()) {
            TypeConverter<Object, Object> fieldConverter = createIcebergConverter(nestedField.type());
            fieldInfos.add(StructConverter.FieldInfo.of(
                    nestedField.name(),
                    fieldConverter,
                    !nestedField.isOptional()
            ));
        }

        return new StructConverter(config, fieldInfos);
    }

    // ==================== Avro Converter Creation ====================

    @SuppressWarnings("unchecked")
    private TypeConverter<Object, Object> createAvroConverter(org.apache.avro.Schema avroSchema) {
        // Check for logical type first
        LogicalType logicalType = avroSchema.getLogicalType();
        if (logicalType != null) {
            TypeConverter<?, ?> logicalConverter = createAvroLogicalTypeConverter(avroSchema, logicalType);
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
            case STRING -> new StringConverter(config);
            case BYTES -> new BinaryConverter(config);
            case FIXED -> BinaryConverter.forFixed(config, avroSchema.getFixedSize());
            case ARRAY -> {
                org.apache.avro.Schema elementSchema = avroSchema.getElementType();
                TypeConverter<Object, Object> elementConverter = createAvroConverter(elementSchema);
                yield new ListConverter(config, elementConverter);
            }
            case MAP -> {
                org.apache.avro.Schema valueSchema = avroSchema.getValueType();
                TypeConverter<Object, Object> valueConverter = createAvroConverter(valueSchema);
                yield new MapConverter(config, new StringConverter(config), valueConverter);
            }
            case RECORD -> createAvroRecordConverter(avroSchema);
            case ENUM -> new AvroEnumConverter(avroSchema);
            case UNION -> {
                List<org.apache.avro.Schema> types = avroSchema.getTypes().stream()
                        .filter(s -> s.getType() != org.apache.avro.Schema.Type.NULL)
                        .toList();
                if (types.size() == 1) {
                    yield createAvroConverter(types.get(0));
                }
                yield new AvroUnionConverter(avroSchema, this);
            }
            case NULL -> new NullConverter();
        };

        return (TypeConverter<Object, Object>) converter;
    }

    private TypeConverter<?, ?> createAvroLogicalTypeConverter(org.apache.avro.Schema schema,
                                                               LogicalType logicalType) {
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
            default -> null;
        };
    }

    private StructConverter createAvroRecordConverter(org.apache.avro.Schema recordSchema) {
        List<StructConverter.FieldInfo> fieldInfos = new ArrayList<>();

        for (org.apache.avro.Schema.Field field : recordSchema.getFields()) {
            org.apache.avro.Schema fieldSchema = field.schema();
            boolean nullable = isAvroNullable(fieldSchema);
            org.apache.avro.Schema effectiveSchema = unwrapAvroNullable(fieldSchema);

            TypeConverter<Object, Object> fieldConverter = createAvroConverter(effectiveSchema);
            fieldInfos.add(StructConverter.FieldInfo.of(
                    field.name(),
                    fieldConverter,
                    !nullable
            ));
        }

        return new StructConverter(config, fieldInfos);
    }

    private boolean isAvroNullable(org.apache.avro.Schema schema) {
        if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .anyMatch(s -> s.getType() == org.apache.avro.Schema.Type.NULL);
        }
        return false;
    }

    private org.apache.avro.Schema unwrapAvroNullable(org.apache.avro.Schema schema) {
        if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .filter(s -> s.getType() != org.apache.avro.Schema.Type.NULL)
                    .findFirst()
                    .orElse(schema);
        }
        return schema;
    }

    // ==================== Accessors ====================

    /**
     * Returns the schema type (AVRO or ICEBERG).
     */
    public SchemaType getSchemaType() {
        return schemaType;
    }

    /**
     * Returns the underlying schema.
     */
    public Object getSchema() {
        return schema;
    }

    /**
     * Returns the Iceberg schema if this converter was created for Iceberg.
     */
    public Optional<Schema> getIcebergSchema() {
        return schemaType == SchemaType.ICEBERG
                ? Optional.of((Schema) schema)
                : Optional.empty();
    }

    /**
     * Returns the Avro schema if this converter was created for Avro.
     */
    public Optional<org.apache.avro.Schema> getAvroSchema() {
        return schemaType == SchemaType.AVRO
                ? Optional.of((org.apache.avro.Schema) schema)
                : Optional.empty();
    }

    /**
     * Returns the configuration used by this converter.
     */
    public ConversionConfig getConfig() {
        return config;
    }

    /**
     * Returns whether case-insensitive matching is enabled.
     */
    public boolean isCaseInsensitive() {
        return caseInsensitive;
    }

    /**
     * Returns all schema field names.
     */
    public Set<String> getFieldNames() {
        return Collections.unmodifiableSet(fieldConverters.keySet());
    }

    /**
     * Returns whether a field is nullable.
     */
    public boolean isFieldNullable(String fieldName) {
        String schemaFieldName = findMatchingField(fieldName);
        return schemaFieldName != null && fieldNullability.getOrDefault(schemaFieldName, true);
    }

    /**
     * Checks if a field exists in the schema.
     */
    public boolean hasField(String fieldName) {
        return findMatchingField(fieldName) != null;
    }

    // ==================== Inner Converter Classes ====================

    /**
     * Converter for Avro enum types.
     */
    private static class AvroEnumConverter extends AbstractTypeConverter<String> {
        private final Set<String> validSymbols;

        public AvroEnumConverter(org.apache.avro.Schema enumSchema) {
            super(ConversionConfig.defaults(), "enum<" + enumSchema.getName() + ">");
            this.validSymbols = new HashSet<>(enumSchema.getEnumSymbols());
        }

        @Override
        protected String doConvert(Object value) {
            String symbol = charSequenceToString(value);
            if (symbol == null) {
                throw unsupportedType(value);
            }

            if (!validSymbols.contains(symbol)) {
                throw conversionError(value,
                        "Invalid enum symbol '" + symbol + "'. Valid: " + validSymbols);
            }

            return symbol;
        }
    }

    /**
     * Converter for Avro union types.
     */
    private class AvroUnionConverter extends AbstractTypeConverter<Object> {
        private final List<TypeConverter<Object, Object>> branchConverters;
        private final List<org.apache.avro.Schema> nonNullTypes;

        public AvroUnionConverter(org.apache.avro.Schema unionSchema,
                                  SchemaBasedMapConverter parent) {
            super(parent.config, "union");
            this.nonNullTypes = unionSchema.getTypes().stream()
                    .filter(s -> s.getType() != org.apache.avro.Schema.Type.NULL)
                    .toList();
            this.branchConverters = new ArrayList<>();

            for (org.apache.avro.Schema branchSchema : nonNullTypes) {
                branchConverters.add(parent.createAvroConverter(branchSchema));
            }
        }

        @Override
        protected Object doConvert(Object value) {
            for (TypeConverter<Object, Object> converter : branchConverters) {
                try {
                    return converter.convert(value);
                } catch (Exception ignored) {
                    // Try next branch
                }
            }
            throw conversionError(value, "Value does not match any union branch");
        }
    }

    /**
     * Converter for Avro time-millis.
     */
    private static class AvroTimeMillisConverter extends AbstractTypeConverter<Integer> {
        public AvroTimeMillisConverter(ConversionConfig config) {
            super(config, "time-millis");
        }

        @Override
        protected Integer doConvert(Object value) {
            TimeConverter timeConverter = new TimeConverter(config);
            Long micros = timeConverter.convert(value);
            return micros != null ? (int) (micros / 1000) : null;
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
            return micros != null ? micros / 1000 : null;
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

    /**
     * Converter for serialized arrays (arrays stored as delimited strings).
     * Returns Object to allow proper casting to TypeConverter<Object, Object>.
     */
    private static class SerializedArrayConverter extends AbstractTypeConverter<Object> {
        private final FlattenedDataType elementType;

        public SerializedArrayConverter(ConversionConfig config, FlattenedDataType elementType) {
            super(config, "array<" + elementType + ">");
            this.elementType = elementType;
        }

        @Override
        protected Object doConvert(Object value) {
            if (value == null) {
                return null;
            }

            // If already a string, return as-is (already serialized)
            if (value instanceof String) {
                return value;
            }

            // If it's a collection, serialize it
            if (value instanceof Collection<?> collection) {
                StringJoiner joiner = new StringJoiner(",");
                for (Object item : collection) {
                    joiner.add(convertElement(item));
                }
                return joiner.toString();
            }

            // If it's an array, serialize it
            if (value.getClass().isArray()) {
                StringJoiner joiner = new StringJoiner(",");
                int length = java.lang.reflect.Array.getLength(value);
                for (int i = 0; i < length; i++) {
                    joiner.add(convertElement(java.lang.reflect.Array.get(value, i)));
                }
                return joiner.toString();
            }

            return value.toString();
        }

        private String convertElement(Object element) {
            if (element == null) {
                return "";
            }
            return element.toString();
        }
    }

    // ==================== Schema Flattening Utilities ====================

    /**
     * Flattens an Avro schema to a map of flattened field names to type definitions.
     *
     * <p>This method traverses the Avro schema iteratively and produces a flat
     * representation suitable for use with pre-flattened data.</p>
     *
     * @param avroSchema the Avro schema to flatten
     * @param separator the separator to use between path components (typically "_")
     * @return map of flattened field names to their type definitions
     */
    public static Map<String, FlattenedFieldType> flattenAvroSchema(org.apache.avro.Schema avroSchema,
                                                                    String separator) {
        if (avroSchema == null) {
            return Collections.emptyMap();
        }

        Map<String, FlattenedFieldType> result = new LinkedHashMap<>();
        Deque<AvroSchemaNode> stack = new ArrayDeque<>();
        stack.push(new AvroSchemaNode(avroSchema, "", 0, false, false));

        while (!stack.isEmpty()) {
            AvroSchemaNode node = stack.pop();
            org.apache.avro.Schema currentSchema = node.schema;

            // Handle unions (nullable types)
            boolean nullable = node.nullable;
            if (currentSchema.getType() == org.apache.avro.Schema.Type.UNION) {
                for (org.apache.avro.Schema unionType : currentSchema.getTypes()) {
                    if (unionType.getType() == org.apache.avro.Schema.Type.NULL) {
                        nullable = true;
                    } else {
                        currentSchema = unionType;
                    }
                }
            }

            switch (currentSchema.getType()) {
                case RECORD -> {
                    List<org.apache.avro.Schema.Field> fields = currentSchema.getFields();
                    for (int i = fields.size() - 1; i >= 0; i--) {
                        org.apache.avro.Schema.Field field = fields.get(i);
                        String fieldPath = node.path.isEmpty()
                                ? field.name()
                                : node.path + separator + field.name();
                        stack.push(new AvroSchemaNode(field.schema(), fieldPath,
                                node.depth + 1, node.inArray, false));
                    }
                }
                case ARRAY -> {
                    org.apache.avro.Schema elementSchema = currentSchema.getElementType();
                    org.apache.avro.Schema actualElement = unwrapAvroNullableStatic(elementSchema);

                    if (actualElement.getType() == org.apache.avro.Schema.Type.RECORD) {
                        // Array of records - flatten each field
                        for (org.apache.avro.Schema.Field field : actualElement.getFields()) {
                            String fieldPath = node.path.isEmpty()
                                    ? field.name()
                                    : node.path + separator + field.name();
                            result.putAll(flattenAvroSchemaForArrayElement(
                                    field.schema(), fieldPath, separator, node.depth + 1));
                        }
                    } else {
                        // Array of primitives - use helper to capture decimal precision/scale
                        String fieldName = node.path.isEmpty() ? "value" : node.path;
                        result.put(fieldName, createFlattenedFieldTypeFromAvro(actualElement, fieldName, nullable, true));
                    }
                }
                case MAP -> {
                    // Maps serialize as strings
                    String fieldName = node.path.isEmpty() ? "value" : node.path;
                    result.put(fieldName, new FlattenedFieldType(fieldName,
                            FlattenedDataType.STRING, nullable));
                }
                default -> {
                    // Primitive types - use helper to capture decimal precision/scale
                    String fieldName = node.path.isEmpty() ? "value" : node.path;
                    result.put(fieldName, createFlattenedFieldTypeFromAvro(
                            currentSchema, fieldName, nullable, node.inArray));
                }
            }
        }

        return result;
    }

    private static Map<String, FlattenedFieldType> flattenAvroSchemaForArrayElement(
            org.apache.avro.Schema schema, String basePath, String separator, int depth) {
        Map<String, FlattenedFieldType> result = new LinkedHashMap<>();

        org.apache.avro.Schema actualSchema = unwrapAvroNullableStatic(schema);
        boolean nullable = schema.getType() == org.apache.avro.Schema.Type.UNION;

        switch (actualSchema.getType()) {
            case RECORD -> {
                for (org.apache.avro.Schema.Field field : actualSchema.getFields()) {
                    String fieldPath = basePath + separator + field.name();
                    result.putAll(flattenAvroSchemaForArrayElement(
                            field.schema(), fieldPath, separator, depth + 1));
                }
            }
            case ARRAY -> {
                org.apache.avro.Schema elementSchema = unwrapAvroNullableStatic(actualSchema.getElementType());
                // Use helper to capture decimal precision/scale
                result.put(basePath, createFlattenedFieldTypeFromAvro(elementSchema, basePath, nullable, true));
            }
            default -> {
                // Use helper to capture decimal precision/scale for array elements
                result.put(basePath, createFlattenedFieldTypeFromAvro(actualSchema, basePath, nullable, true));
            }
        }

        return result;
    }

    private static org.apache.avro.Schema unwrapAvroNullableStatic(org.apache.avro.Schema schema) {
        if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .filter(s -> s.getType() != org.apache.avro.Schema.Type.NULL)
                    .findFirst()
                    .orElse(schema);
        }
        return schema;
    }

    /**
     * Creates a FlattenedFieldType from an Avro schema, capturing precision/scale for decimals.
     */
    private static FlattenedFieldType createFlattenedFieldTypeFromAvro(
            org.apache.avro.Schema schema, String fieldName, boolean nullable, boolean inArray) {

        FlattenedDataType dataType = mapAvroToFlattenedType(schema);

        // For decimals, extract precision and scale from logical type
        if (dataType == FlattenedDataType.DECIMAL) {
            LogicalType logicalType = schema.getLogicalType();
            if (logicalType instanceof LogicalTypes.Decimal decimal) {
                int precision = decimal.getPrecision();
                int scale = decimal.getScale();
                if (inArray) {
                    // For arrays, we serialize to string but track element type
                    return new FlattenedFieldType(fieldName, FlattenedDataType.STRING,
                            true, FlattenedDataType.DECIMAL, nullable, precision, scale);
                } else {
                    return new FlattenedFieldType(fieldName, FlattenedDataType.DECIMAL,
                            false, null, nullable, precision, scale);
                }
            }
        }

        // Non-decimal types
        if (inArray) {
            return FlattenedFieldType.array(fieldName, dataType, nullable);
        } else {
            return new FlattenedFieldType(fieldName, dataType, nullable);
        }
    }

    private static FlattenedDataType mapAvroToFlattenedType(org.apache.avro.Schema schema) {
        // Check logical type first
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType != null) {
            return switch (logicalType.getName()) {
                case "decimal" -> FlattenedDataType.DECIMAL;
                case "date" -> FlattenedDataType.DATE;
                case "time-millis", "time-micros" -> FlattenedDataType.LONG;
                case "timestamp-millis", "timestamp-micros",
                     "local-timestamp-millis", "local-timestamp-micros" -> FlattenedDataType.TIMESTAMP;
                case "uuid" -> FlattenedDataType.UUID;
                default -> FlattenedDataType.STRING;
            };
        }

        return switch (schema.getType()) {
            case BOOLEAN -> FlattenedDataType.BOOLEAN;
            case INT -> FlattenedDataType.INT;
            case LONG -> FlattenedDataType.LONG;
            case FLOAT -> FlattenedDataType.FLOAT;
            case DOUBLE -> FlattenedDataType.DOUBLE;
            case STRING, ENUM -> FlattenedDataType.STRING;
            case BYTES, FIXED -> FlattenedDataType.BYTES;
            default -> FlattenedDataType.STRING;
        };
    }

    /**
     * Flattens an Iceberg schema to a map of flattened field names to type definitions.
     */
    public static Map<String, FlattenedFieldType> flattenIcebergSchema(Schema icebergSchema,
                                                                       String separator) {
        if (icebergSchema == null) {
            return Collections.emptyMap();
        }

        Map<String, FlattenedFieldType> result = new LinkedHashMap<>();

        for (Types.NestedField field : icebergSchema.columns()) {
            flattenIcebergType(field.name(), field.type(), field.isOptional(),
                    separator, false, result);
        }

        return result;
    }

    private static void flattenIcebergType(String path, Type type, boolean nullable,
                                           String separator, boolean inArray,
                                           Map<String, FlattenedFieldType> result) {
        switch (type.typeId()) {
            case STRUCT -> {
                Types.StructType structType = (Types.StructType) type;
                for (Types.NestedField field : structType.fields()) {
                    String fieldPath = path + separator + field.name();
                    flattenIcebergType(fieldPath, field.type(), field.isOptional(),
                            separator, inArray, result);
                }
            }
            case LIST -> {
                Types.ListType listType = (Types.ListType) type;
                Type elementType = listType.elementType();

                if (elementType.isStructType()) {
                    // Array of structs - flatten each field
                    Types.StructType structType = (Types.StructType) elementType;
                    for (Types.NestedField field : structType.fields()) {
                        String fieldPath = path + separator + field.name();
                        flattenIcebergType(fieldPath, field.type(), field.isOptional(),
                                separator, true, result);
                    }
                } else {
                    // Array of primitives
                    FlattenedDataType elemType = mapIcebergToFlattenedType(elementType);
                    result.put(path, FlattenedFieldType.array(path, elemType, nullable));
                }
            }
            case MAP -> {
                // Maps serialize as strings
                result.put(path, new FlattenedFieldType(path, FlattenedDataType.STRING, nullable));
            }
            default -> {
                FlattenedDataType dataType = mapIcebergToFlattenedType(type);
                if (inArray) {
                    result.put(path, FlattenedFieldType.array(path, dataType, nullable));
                } else {
                    result.put(path, new FlattenedFieldType(path, dataType, nullable));
                }
            }
        }
    }

    private static FlattenedDataType mapIcebergToFlattenedType(Type type) {
        return switch (type.typeId()) {
            case BOOLEAN -> FlattenedDataType.BOOLEAN;
            case INTEGER -> FlattenedDataType.INT;
            case LONG -> FlattenedDataType.LONG;
            case FLOAT -> FlattenedDataType.FLOAT;
            case DOUBLE -> FlattenedDataType.DOUBLE;
            case STRING -> FlattenedDataType.STRING;
            case DATE -> FlattenedDataType.DATE;
            case TIME -> FlattenedDataType.LONG;
            case TIMESTAMP, TIMESTAMP_NANO -> FlattenedDataType.TIMESTAMP;
            case UUID -> FlattenedDataType.UUID;
            case DECIMAL -> FlattenedDataType.DECIMAL;
            case BINARY, FIXED -> FlattenedDataType.BYTES;
            default -> FlattenedDataType.STRING;
        };
    }

    /**
     * Helper class for iterative Avro schema traversal.
     */
    private record AvroSchemaNode(
            org.apache.avro.Schema schema,
            String path,
            int depth,
            boolean inArray,
            boolean nullable
    ) {}

    // ==================== Additional Accessors ====================

    /**
     * Returns the flattened schema if this converter was created for a flattened schema.
     */
    public Optional<Map<String, FlattenedFieldType>> getFlattenedSchema() {
        return schemaType == SchemaType.FLATTENED
                ? Optional.of(Collections.unmodifiableMap(flattenedSchemaFields))
                : Optional.empty();
    }

    /**
     * Returns field type information for a flattened field.
     */
    public Optional<FlattenedFieldType> getFlattenedFieldType(String fieldName) {
        String schemaFieldName = findMatchingField(fieldName);
        return schemaFieldName != null
                ? Optional.ofNullable(flattenedSchemaFields.get(schemaFieldName))
                : Optional.empty();
    }
}