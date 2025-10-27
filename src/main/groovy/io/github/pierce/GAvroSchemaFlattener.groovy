package io.github.pierce;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer

/**
 * Flattens Avro schemas to match MapFlattener output and applies type casting
 * to flattened data based on the schema.
 * <p>
 * This class is designed for streaming environments where:
 * 1. Avro schemas are flattened once and cached
 * 2. Flattened data is type-cast based on the flattened schema
 * 3. Memory efficiency is critical (non-recursive traversal)
 * <p>
 * Thread-safe for concurrent use in streaming applications.
 *
 * <h3>Example Usage:</h3>
 * <pre>
 * // One-time schema flattening (cache this result)
 * Schema avroSchema = ...; // Your Avro schema
 * AvroSchemaFlattener flattener = new AvroSchemaFlattener(config);
 * Map&lt;String, FlattenedFieldType&gt; flattenedSchema = flattener.flattenSchema(avroSchema);
 * // Cache flattenedSchema...
 *
 * // For each incoming message (streaming)
 * Map&lt;String, Object&gt; jsonData = ...; // JSON as Map
 * MapFlattener dataFlattener = new MapFlattener();
 * Map&lt;String, Object&gt; flattenedData = dataFlattener.flatten(jsonData);
 *
 * // Apply correct types based on schema
 * Map&lt;String, Object&gt; typedData = flattener.applyTypes(flattenedData, flattenedSchema);
 * // Now typedData has correct types matching Glue table schema
 * </pre>
 */
public class GAvroSchemaFlattener implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(GAvroSchemaFlattener.class);

    private final AvroFlatteningConfig config;
    private final ObjectMapper objectMapper;

    // Cache for parsed array values (within a single flatten operation)
    private static final ThreadLocal<Map<String, List<?>>> ARRAY_PARSE_CACHE =
            ThreadLocal.withInitial(HashMap::new);

    /**
     * Configuration for Avro schema flattening
     */
    public static class AvroFlatteningConfig implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String separator;
        private final boolean useArrayBoundarySeparator;
        private final int maxDepth;
        private final boolean strictTypeEnforcement;
        private final boolean handleUnions;

        private AvroFlatteningConfig(Builder builder) {
            this.separator = builder.useArrayBoundarySeparator ? "__" : "_";
            this.useArrayBoundarySeparator = builder.useArrayBoundarySeparator;
            this.maxDepth = builder.maxDepth;
            this.strictTypeEnforcement = builder.strictTypeEnforcement;
            this.handleUnions = builder.handleUnions;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private boolean useArrayBoundarySeparator = false;
            private int maxDepth = 50;
            private boolean strictTypeEnforcement = true;
            private boolean handleUnions = true;

            public Builder useArrayBoundarySeparator(boolean use) {
                this.useArrayBoundarySeparator = use;
                return this;
            }

            public Builder maxDepth(int depth) {
                this.maxDepth = depth;
                return this;
            }

            public Builder strictTypeEnforcement(boolean strict) {
                this.strictTypeEnforcement = strict;
                return this;
            }

            public Builder handleUnions(boolean handle) {
                this.handleUnions = handle;
                return this;
            }

            public AvroFlatteningConfig build() {
                return new AvroFlatteningConfig(this);
            }
        }

        public String getSeparator() {
            return separator;
        }
    }

    public GAvroSchemaFlattener() {
        this(AvroFlatteningConfig.builder().build());
    }

    public GAvroSchemaFlattener(AvroFlatteningConfig config) {
        this.config = config;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Represents a flattened field with type information
     */
    public static class FlattenedFieldType implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String flattenedName;
        private final DataType dataType;
        private final boolean isArraySerialized;
        private final DataType arrayElementType;
        private final Schema.Type originalAvroType;
        private final boolean nullable;

        public FlattenedFieldType(String flattenedName, DataType dataType,
                                  boolean isArraySerialized, DataType arrayElementType,
                                  Schema.Type originalAvroType, boolean nullable) {
            this.flattenedName = flattenedName;
            this.dataType = dataType;
            this.isArraySerialized = isArraySerialized;
            this.arrayElementType = arrayElementType;
            this.originalAvroType = originalAvroType;
            this.nullable = nullable;
        }

        public String getFlattenedName() {
            return flattenedName;
        }

        public DataType getDataType() {
            return dataType;
        }

        public boolean isArraySerialized() {
            return isArraySerialized;
        }

        public DataType getArrayElementType() {
            return arrayElementType;
        }

        public Schema.Type getOriginalAvroType() {
            return originalAvroType;
        }

        public boolean isNullable() {
            return nullable;
        }

        @Override
        public String toString() {
            if (isArraySerialized) {
                return String.format("%s (array of %s, serialized as %s)%s",
                        flattenedName, arrayElementType, dataType, nullable ? " [nullable]" : "");
            }
            return String.format("%s (%s)%s", flattenedName, dataType, nullable ? " [nullable]" : "");
        }
    }

    /**
     * Data types after flattening
     */
    public enum DataType {
        STRING,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        BYTES,
        // Special types for Glue/Athena
        BIGINT,
        DECIMAL,
        TIMESTAMP,
        DATE
    }

    /**
     * Internal node for iterative traversal
     */
    private static class SchemaNode {
        final Schema schema;
        final String path;
        final int depth;
        final boolean inArray;

        SchemaNode(Schema schema, String path, int depth, boolean inArray) {
            this.schema = schema;
            this.path = path;
            this.depth = depth;
            this.inArray = inArray;
        }
    }

    /**
     * Flatten an Avro schema to match MapFlattener output structure.
     * This method uses iterative traversal to avoid stack overflow on deeply nested schemas.
     * <p>
     * The returned map should be CACHED for reuse across multiple data records.
     *
     * @param schema The Avro schema to flatten
     * @return Map of flattened field names to their type information
     */
    public Map<String, FlattenedFieldType> flattenSchema(Schema schema) {
        if (schema == null) {
            return Collections.emptyMap();
        }

        Map<String, FlattenedFieldType> result = new LinkedHashMap<>();

        // Use iterative approach with explicit stack to avoid recursion
        Deque<SchemaNode> stack = new ArrayDeque<>();
        stack.push(new SchemaNode(schema, "", 0, false));

        while (!stack.isEmpty()) {
            SchemaNode node = stack.pop();

            // Depth limit check
            if (node.depth >= config.maxDepth) {
                log.warn("Max depth {} reached at path: {}", config.maxDepth, node.path);
                // Treat as string at max depth
                result.put(node.path.isEmpty() ? "root" : node.path,
                        new FlattenedFieldType(node.path, DataType.STRING, false, null,
                                Schema.Type.STRING, false));
                continue;
            }

            Schema currentSchema = node.schema;

            // Handle unions (especially nullable types)
            if (currentSchema.getType() == Schema.Type.UNION) {
                UnionTypeInfo unionInfo = analyzeUnion(currentSchema);
                if (unionInfo.nonNullSchema != null) {
                    // Push the non-null type back onto stack
                    stack.push(new SchemaNode(unionInfo.nonNullSchema, node.path,
                            node.depth, node.inArray));
                    continue;
                }
            }

            switch (currentSchema.getType()) {
                case RECORD:
                    // Add fields to stack in reverse order (to maintain field order)
                    List<Field> fields = currentSchema.getFields();
                    for (int i = fields.size() - 1; i >= 0; i--) {
                        Field field = fields.get(i);
                        String fieldPath = buildPath(node.path, field.name());
                        stack.push(new SchemaNode(field.schema(), fieldPath,
                                node.depth + 1, node.inArray));
                    }
                    break;

                case ARRAY:
                    Schema elementSchema = currentSchema.getElementType();

                    if (elementSchema.getType() == Schema.Type.RECORD) {
                        // Array of records - extract fields, each becomes array serialized
                        String separator = config.useArrayBoundarySeparator ? "__" : "_";
                        List<Field> recordFields = elementSchema.getFields();

                        for (Field recordField : recordFields) {
                            String fieldPath = node.path.isEmpty()
                                    ? recordField.name()
                                    : node.path + separator + recordField.name();

                            // Recursively handle nested structures in array elements
                            Map<String, FlattenedFieldType> nestedFields =
                                    flattenSchemaForArrayElement(recordField.schema(), fieldPath, node.depth + 1);
                            result.putAll(nestedFields);
                        }
                    } else if (elementSchema.getType() == Schema.Type.ARRAY) {
                        // Nested array - serialize as string
                        DataType elementType = mapAvroTypeToDataType(elementSchema);
                        result.put(node.path.isEmpty() ? "value" : node.path,
                                new FlattenedFieldType(node.path, DataType.STRING, true,
                                        elementType, currentSchema.getType(), false));
                    } else {
                        // Array of primitives - becomes serialized string
                        DataType elementType = mapAvroTypeToDataType(elementSchema);
                        result.put(node.path.isEmpty() ? "value" : node.path,
                                new FlattenedFieldType(node.path, DataType.STRING, true,
                                        elementType, currentSchema.getType(), false));
                    }
                    break;

                case MAP:
                    // Avro maps are not directly supported by MapFlattener
                    // Treat as serialized string
                    log.warn("Avro MAP type at path {} will be serialized as STRING", node.path);
                    result.put(node.path.isEmpty() ? "value" : node.path,
                            new FlattenedFieldType(node.path, DataType.STRING, false, null,
                                    Schema.Type.MAP, false));
                    break;

                default:
                    // Primitive types
                    DataType dataType = mapAvroTypeToDataType(currentSchema);
                    boolean nullable = isNullable(currentSchema);
                    result.put(node.path.isEmpty() ? "value" : node.path,
                            new FlattenedFieldType(node.path, dataType, false, null,
                                    currentSchema.getType(), nullable));
                    break;
            }
        }

        return result;
    }

    /**
     * Helper method to flatten schema for elements within an array of records.
     * This handles nested structures within array elements.
     */
    private Map<String, FlattenedFieldType> flattenSchemaForArrayElement(Schema schema,
                                                                         String basePath,
                                                                         int depth) {
        Map<String, FlattenedFieldType> result = new LinkedHashMap<>();

        if (depth >= config.maxDepth) {
            result.put(basePath, new FlattenedFieldType(basePath, DataType.STRING,
                    true, DataType.STRING, Schema.Type.STRING, false));
            return result;
        }

        Schema actualSchema = schema;
        boolean nullable = false;

        // Handle unions
        if (schema.getType() == Schema.Type.UNION) {
            UnionTypeInfo unionInfo = analyzeUnion(schema);
            nullable = unionInfo.hasNull;
            if (unionInfo.nonNullSchema != null) {
                actualSchema = unionInfo.nonNullSchema;
            }
        }

        switch (actualSchema.getType()) {
            case RECORD:
                // Recursively flatten nested record
                String separator = config.useArrayBoundarySeparator ? "__" : "_";
                for (Field field : actualSchema.getFields()) {
                    String fieldPath = basePath + separator + field.name();
                    Map<String, FlattenedFieldType> nestedFields =
                            flattenSchemaForArrayElement(field.schema(), fieldPath, depth + 1);
                    result.putAll(nestedFields);
                }
                break;

            case ARRAY:
                // Nested array in array element - serialize
                DataType elementType = mapAvroTypeToDataType(actualSchema.getElementType());
                result.put(basePath, new FlattenedFieldType(basePath, DataType.STRING,
                        true, elementType, actualSchema.getType(), nullable));
                break;

            default:
                // Primitive in array - will be serialized
                DataType dataType = mapAvroTypeToDataType(actualSchema);
                result.put(basePath, new FlattenedFieldType(basePath, DataType.STRING,
                        true, dataType, actualSchema.getType(), nullable));
                break;
        }

        return result;
    }

    /**
     * Information about a union type
     */
    private static class UnionTypeInfo {
        final boolean hasNull;
        final Schema nonNullSchema;

        UnionTypeInfo(boolean hasNull, Schema nonNullSchema) {
            this.hasNull = hasNull;
            this.nonNullSchema = nonNullSchema;
        }
    }

    /**
     * Analyze a union type to extract nullable information and non-null schema
     */
    private UnionTypeInfo analyzeUnion(Schema unionSchema) {
        if (unionSchema.getType() != Schema.Type.UNION) {
            return new UnionTypeInfo(false, unionSchema);
        }

        List<Schema> types = unionSchema.getTypes();
        boolean hasNull = false;
        Schema nonNullSchema = null;

        for (Schema type : types) {
            if (type.getType() == Schema.Type.NULL) {
                hasNull = true;
            } else if (nonNullSchema == null) {
                nonNullSchema = type;
            }
        }

        return new UnionTypeInfo(hasNull, nonNullSchema);
    }

    /**
     * Check if a schema is nullable (union with null)
     */
    private boolean isNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return false;
        }

        for (Schema type : schema.getTypes()) {
            if (type.getType() == Schema.Type.NULL) {
                return true;
            }
        }
        return false;
    }

    /**
     * Map Avro type to DataType enum
     */
    private DataType mapAvroTypeToDataType(Schema schema) {
        Schema actualSchema = schema;

        // Handle unions - get non-null type
        if (schema.getType() == Schema.Type.UNION) {
            UnionTypeInfo unionInfo = analyzeUnion(schema);
            if (unionInfo.nonNullSchema != null) {
                actualSchema = unionInfo.nonNullSchema;
            }
        }

        switch (actualSchema.getType()) {
            case STRING:
            case ENUM:
                return DataType.STRING;
            case INT:
                return DataType.INT;
            case LONG:
                return DataType.LONG;
            case FLOAT:
                return DataType.FLOAT;
            case DOUBLE:
                return DataType.DOUBLE;
            case BOOLEAN:
                return DataType.BOOLEAN;
            case BYTES:
            case FIXED:
                return DataType.BYTES;
            case ARRAY:
            case MAP:
            case RECORD:
                // Complex types are serialized
                return DataType.STRING;
            default:
                log.warn("Unknown Avro type: {}, defaulting to STRING", actualSchema.getType());
                return DataType.STRING;
        }
    }

    /**
     * Build field path with appropriate separator
     */
    private String buildPath(String prefix, String fieldName) {
        if (prefix == null || prefix.isEmpty()) {
            return fieldName;
        }
        return prefix + config.getSeparator() + fieldName;
    }

    /**
     * Apply type casting to flattened data based on flattened schema.
     * This is the hot path method called for every record in streaming.
     * <p>
     * Performance optimizations:
     * - Reuses thread-local caches
     * - Minimal object allocation
     * - Early returns for null values
     * - Bulk operations where possible
     *
     * @param flattenedData The flattened data from MapFlattener
     * @param flattenedSchema The flattened schema (should be cached)
     * @return Map with correctly typed values
     */
    public Map<String, Object> applyTypes(Map<String, Object> flattenedData,
                                          Map<String, FlattenedFieldType> flattenedSchema) {
        if (flattenedData == null || flattenedData.isEmpty()) {
            return Collections.emptyMap();
        }

        if (flattenedSchema == null || flattenedSchema.isEmpty()) {
            log.warn("No schema provided, returning data as-is");
            return new LinkedHashMap<>(flattenedData);
        }

        Map<String, Object> result = new LinkedHashMap<>(flattenedData.size());
        Map<String, List<?>> parseCache = ARRAY_PARSE_CACHE.get();
        parseCache.clear();

        try {
            for (Map.Entry<String, FlattenedFieldType> schemaEntry : flattenedSchema.entrySet()) {
                String fieldName = schemaEntry.getKey();
                FlattenedFieldType fieldType = schemaEntry.getValue();

                // Get the value from flattened data
                Object value = flattenedData.get(fieldName);

                // Handle missing fields
                if (value == null) {
                    if (fieldType.isNullable() || !config.strictTypeEnforcement) {
                        result.put(fieldName, null);
                    } else if (config.strictTypeEnforcement) {
                        log.warn("Missing non-nullable field: {}", fieldName);
                        result.put(fieldName, getDefaultValue(fieldType.getDataType()));
                    }
                    continue;
                }

                // Apply type conversion
                try {
                    Object typedValue = convertValue(value, fieldType, parseCache);
                    result.put(fieldName, typedValue);
                } catch (Exception e) {
                    log.error("Error converting field {} to type {}: {}",
                            fieldName, fieldType.getDataType(), e.getMessage());
                    if (config.strictTypeEnforcement) {
                        throw new RuntimeException("Type conversion failed for field: " + fieldName, e);
                    } else {
                        // Keep original value on error
                        result.put(fieldName, value);
                    }
                }
            }

            // Include fields from data that aren't in schema (if not strict)
            if (!config.strictTypeEnforcement) {
                for (Map.Entry<String, Object> dataEntry : flattenedData.entrySet()) {
                    if (!result.containsKey(dataEntry.getKey())) {
                        result.put(dataEntry.getKey(), dataEntry.getValue());
                    }
                }
            }

        } finally {
            parseCache.clear();
        }

        return result;
    }

    /**
     * Convert a value to the specified type
     */
    private Object convertValue(Object value, FlattenedFieldType fieldType,
                                Map<String, List<?>> parseCache) {
        if (value == null) {
            return null;
        }

        // If it's an array serialized field
        if (fieldType.isArraySerialized()) {
            return convertSerializedArray(value, fieldType, parseCache);
        }

        // Direct type conversion for primitives
        return convertPrimitive(value, fieldType.getDataType());
    }

    /**
     * Convert a serialized array string to appropriate type
     */
    private Object convertSerializedArray(Object value, FlattenedFieldType fieldType,
                                          Map<String, List<?>> parseCache) {
        if (!(value instanceof String)) {
            log.warn("Expected string for serialized array, got: {}", value.getClass().getName());
            return value;
        }

        String serialized = (String) value;

        // Check cache first
        String cacheKey = fieldType.getFlattenedName();
        if (parseCache.containsKey(cacheKey)) {
            return formatArrayForOutput(parseCache.get(cacheKey), fieldType);
        }

        try {
            // Parse the JSON array
            List<?> parsedArray = objectMapper.readValue(serialized,
                    new TypeReference<List<Object>>() {});

            // Convert each element to the correct type
            List<Object> typedArray = new ArrayList<>(parsedArray.size());
            DataType elementType = fieldType.getArrayElementType();

            for (Object element : parsedArray) {
                if (element == null) {
                    typedArray.add(null);
                } else {
                    try {
                        Object typedElement = convertPrimitive(element, elementType);
                        typedArray.add(typedElement);
                    } catch (Exception e) {
                        log.warn("Failed to convert array element, keeping as-is: {}", e.getMessage());
                        typedArray.add(element);
                    }
                }
            }

            // Cache the parsed result
            parseCache.put(cacheKey, typedArray);

            return formatArrayForOutput(typedArray, fieldType);

        } catch (Exception e) {
            log.error("Failed to parse serialized array: {}", e.getMessage());
            // Return as-is if parsing fails
            return value;
        }
    }

    /**
     * Format array for output (keep as JSON string for Glue compatibility)
     */
    private Object formatArrayForOutput(List<?> array, FlattenedFieldType fieldType) {
        // For Glue tables, we typically keep arrays as JSON strings
        // This matches the MapFlattener output format
        try {
            return objectMapper.writeValueAsString(array);
        } catch (Exception e) {
            log.warn("Failed to serialize array back to JSON: {}", e.getMessage());
            return array.toString();
        }
    }

    /**
     * Convert a primitive value to the specified type
     */
    private Object convertPrimitive(Object value, DataType targetType) {
        if (value == null) {
            return null;
        }

        // If already correct type, return as-is
        if (isCorrectType(value, targetType)) {
            return value;
        }

        try {
            switch (targetType) {
                case STRING:
                    return value.toString();

                case INT:
                    if (value instanceof Number) {
                        return ((Number) value).intValue();
                    }
                    return Integer.parseInt(value.toString());

                case LONG:
                case BIGINT:
                    if (value instanceof Number) {
                        return ((Number) value).longValue();
                    }
                    return Long.parseLong(value.toString());

                case FLOAT:
                    if (value instanceof Number) {
                        return ((Number) value).floatValue();
                    }
                    return Float.parseFloat(value.toString());

                case DOUBLE:
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    }
                    return Double.parseDouble(value.toString());

                case BOOLEAN:
                    if (value instanceof Boolean) {
                        return value;
                    }
                    String strValue = value.toString().toLowerCase();
                    return "true".equals(strValue) || "1".equals(strValue);

                case DECIMAL:
                    if (value instanceof BigDecimal) {
                        return value;
                    }
                    if (value instanceof Number) {
                        return BigDecimal.valueOf(((Number) value).doubleValue());
                    }
                    return new BigDecimal(value.toString());

                case BYTES:
                    if (value instanceof byte[]) {
                        return value;
                    }
                    if (value instanceof ByteBuffer) {
                        return ((ByteBuffer) value).array();
                    }
                    if (value instanceof String) {
                        return Base64.getDecoder().decode((String) value);
                    }
                    return value.toString().getBytes();

                case TIMESTAMP:
                case DATE:
                    // Keep as string for Glue compatibility
                    return value.toString();

                default:
                    log.warn("Unknown target type: {}, returning as-is", targetType);
                    return value;
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Cannot convert value '%s' to type %s: %s",
                            value, targetType, e.getMessage()), e);
        }
    }

    /**
     * Check if value is already the correct type
     */
    private boolean isCorrectType(Object value, DataType targetType) {
        switch (targetType) {
            case STRING:
                return value instanceof String;
            case INT:
                return value instanceof Integer;
            case LONG:
            case BIGINT:
                return value instanceof Long;
            case FLOAT:
                return value instanceof Float;
            case DOUBLE:
                return value instanceof Double;
            case BOOLEAN:
                return value instanceof Boolean;
            case DECIMAL:
                return value instanceof BigDecimal;
            case BYTES:
                return value instanceof byte[] || value instanceof ByteBuffer;
            default:
                return false;
        }
    }

    /**
     * Get default value for a data type (used when strict enforcement is enabled)
     */
    private Object getDefaultValue(DataType dataType) {
        switch (dataType) {
            case STRING:
                return "";
            case INT:
                return 0;
            case LONG:
            case BIGINT:
                return 0L;
            case FLOAT:
                return 0.0f;
            case DOUBLE:
                return 0.0;
            case BOOLEAN:
                return false;
            case DECIMAL:
                return BigDecimal.ZERO;
            case BYTES:
                return new byte[0];
            default:
                return null;
        }
    }

    /**
     * Clear thread-local caches (call this periodically in long-running applications)
     */
    public static void clearCaches() {
        ARRAY_PARSE_CACHE.remove();
    }
}