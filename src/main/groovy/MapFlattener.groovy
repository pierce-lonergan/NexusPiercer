package io.github.pierce;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.github.pierce.MapFlattener.ArraySerializationFormat.*;
import static io.github.pierce.MapFlattener.FieldNamingStrategy.*;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Production-hardened Map flattener with comprehensive edge case handling
 * <p>
 * Thread-safe, handles circular references correctly, and provides extensive
 * configuration options for edge cases.
 *
 * <h3>Basic Flattening</h3>
 * <pre>
 * {user: {name: "John", age: 30}}
 * → {user_name: "John", user_age: 30}
 * </pre>
 *
 * <h3>Array Handling</h3>
 * Arrays are converted to strings based on ArraySerializationFormat:
 * <pre>
 * Input: {scores: [1, 2, 3]}
 *
 * JSON (default): {scores: "[1,2,3]"}
 * COMMA_SEPARATED: {scores: "1,2,3"}
 * PIPE_SEPARATED: {scores: "1|2|3"}
 * BRACKET_LIST: {scores: "[1, 2, 3]"}
 *
 * // Object arrays - fields are extracted:
 * Input: {users: [{name:"Alice", age:30}, {name:"Bob", age:25}]}
 *
 * JSON (default): {users_name: "[\"Alice\",\"Bob\"]", users_age: "[30,25]"}
 * COMMA_SEPARATED: {users_name: "Alice,Bob", users_age: "30,25"}
 * PIPE_SEPARATED: {users_name: "Alice|Bob", users_age: "30|25"}
 * BRACKET_LIST: {users_name: "[Alice, Bob]", users_age: "[30, 25]"}
 *
 * // Nested arrays preserve structure while extracting fields:
 * Input: {phones: [[{number:1}, {number:2}], [{number:3}]]}
 * Output: {phones_number: "[[1,2],[3]]"}
 *
 * // Mixed content uses _value sentinel:
 * Input: {data: [[{name:"A"}], "text"]}
 * Output: {data_name: "[["A"]]", data_value: "[[null],["text"]]"}
 * </pre>
 *
 * <h3>For AWS Athena</h3>
 * Recommended formats:
 * <pre>
 * // For CSV with simple values:
 * .arrayFormat(ArraySerializationFormat.COMMA_SEPARATED)
 *
 * // For custom delimiters in Athena:
 * .arrayFormat(ArraySerializationFormat.PIPE_SEPARATED)
 *
 * // For Athena array&lt;T&gt; types in Parquet/ORC:
 * .arrayFormat(ArraySerializationFormat.BRACKET_LIST)
 * </pre>
 *
 * <h3>Important: Delimiter Collisions</h3>
 * When using COMMA_SEPARATED or PIPE_SEPARATED formats, values containing
 * those delimiters will create ambiguous output:
 * <pre>
 * Input: {names: ["Smith, John", "Doe, Jane"]}
 * Output: {names: "Smith, John,Doe, Jane"}
 *
 * This cannot be distinguished from:
 * Input: {names: ["Smith", "John", "Doe", "Jane"]}
 * Output: {names: "Smith,John,Doe,Jane"}
 * </pre>
 *
 * <b>Recommendation:</b> Use JSON format if values may contain delimiters,
 * or sanitize/validate data before flattening.
 *
 * <h3>Null Handling in Delimited Formats</h3>
 * In COMMA_SEPARATED and PIPE_SEPARATED formats, null values become empty strings:
 * <pre>
 * [null, "", "value"] → ",,value"
 * </pre>
 * Empty string and null are indistinguishable in the output.
 *
 * <h3>Circular References</h3>
 * Detected and marked as [CIRCULAR_REFERENCE] when enabled.
 * Shared references (same object in multiple places) work correctly.
 *
 * <h3>Thread Safety</h3>
 * Fully thread-safe. Multiple threads can share a single MapFlattener instance.
 * Uses ThreadLocal for circular reference detection without contention.
 */
public class MapFlattener implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(MapFlattener.class);

    private final int maxDepth;
    private final int maxArraySize;
    private final int maxMapSize;
    private final int maxJsonStringLength;
    private final boolean useArrayBoundarySeparator;
    private final FieldNamingStrategy namingStrategy;
    private final Set<String> excludePaths;
    private final boolean detectCircularReferences;
    private final boolean strictKeyValidation;
    private final boolean parseNestedJsonStrings;
    private final boolean preserveBigDecimalPrecision;
    private final ArraySerializationFormat arrayFormat;

    // Thread-local context for tracking visited objects during recursion
    private static final ThreadLocal<FlattenContext> CONTEXT = ThreadLocal.withInitial(FlattenContext::new);

    // Thread-safe compiled pattern cache
    private final Map<String, Pattern> patternCache = new ConcurrentHashMap<>();

    // Jackson ObjectMapper for JSON operations
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Context for managing state during flattening operation
     */
    private static class FlattenContext {
        private final Set<Integer> visitedIds = new HashSet<>();
        private final Deque<Integer> visitStack = new ArrayDeque<>();

        void clear() {
            visitedIds.clear();
            visitStack.clear();
        }

        boolean enterObject(Object obj) {
            if (obj == null) return true;

            int id = System.identityHashCode(obj);
            if (visitedIds.contains(id)) {
                // Already visited - this is a circular reference
                return false;
            }

            visitedIds.add(id);
            visitStack.push(id);
            return true;
        }

        void exitObject(Object obj) {
            if (obj == null) return;

            int id = System.identityHashCode(obj);
            visitStack.remove(id);
            visitedIds.remove(id);
        }
    }

    /**
     * Legacy constructor for backward compatibility
     */
    public MapFlattener() {
        this(false, 50, 1000);
    }

    /**
     * Legacy constructor for backward compatibility
     */
    public MapFlattener(boolean useArrayBoundarySeparator, int maxDepth, int maxArraySize) {
        this(builder()
                .useArrayBoundarySeparator(useArrayBoundarySeparator)
                .maxDepth(maxDepth)
                .maxArraySize(maxArraySize));
    }

    private MapFlattener(Builder builder) {
        this.maxDepth = builder.maxDepth > 0 ? builder.maxDepth : 50;
        this.maxArraySize = builder.maxArraySize > 0 ? builder.maxArraySize : 1000;
        this.maxMapSize = builder.maxMapSize > 0 ? builder.maxMapSize : 10000;
        this.maxJsonStringLength = builder.maxJsonStringLength > 0 ? builder.maxJsonStringLength : 1000000;
        this.useArrayBoundarySeparator = builder.useArrayBoundarySeparator;
        this.namingStrategy = builder.namingStrategy;
        this.excludePaths = builder.excludePaths != null ? new HashSet<>(builder.excludePaths) : null;
        this.detectCircularReferences = builder.detectCircularReferences;
        this.strictKeyValidation = builder.strictKeyValidation;
        this.parseNestedJsonStrings = builder.parseNestedJsonStrings;
        this.preserveBigDecimalPrecision = builder.preserveBigDecimalPrecision;
        this.arrayFormat = builder.arrayFormat;
    }

    /**
     * Flatten a map into a flat key-value structure
     *
     * @param input The map to flatten
     * @return Flattened map
     */
    public Map<String, Object> flatten(Map<?, ?> input) {
        if (input == null || input.isEmpty()) {
            return new LinkedHashMap<>();
        }

        FlattenContext context = CONTEXT.get();
        boolean isTopLevel = context.visitStack.isEmpty();

        try {
            if (isTopLevel) {
                context.clear();
            }

            // Check map size limit
            if (input.size() > maxMapSize) {
                log.warn("Input map size ({}) exceeds maxMapSize ({}), truncating", input.size(), maxMapSize);
            }

            // Create defensive copy with sanitized keys
            Map<String, Object> safeCopy = new LinkedHashMap<>();
            Set<String> usedKeys = new HashSet<>();
            int count = 0;

            for (Map.Entry<?, ?> entry : input.entrySet()) {
                if (count >= maxMapSize) {
                    break;
                }
                String safeKey = sanitizeKey(entry.getKey(), usedKeys);
                usedKeys.add(safeKey);
                safeCopy.put(safeKey, entry.getValue());
                count++;
            }

            Map<String, Object> result = flattenObject(safeCopy, "", 0);

            if (namingStrategy != FieldNamingStrategy.AS_IS) {
                result = transformKeys(result);
            }

            return result;

        } catch (StackOverflowError e) {
            log.error("Stack overflow - circular reference or excessive depth detected", e);
            throw new IllegalStateException("Circular reference detected or maximum recursion depth exceeded", e);
        } catch (Exception e) {
            log.error("Error flattening map: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to flatten map", e);
        } finally {
            if (isTopLevel) {
                // Only remove ThreadLocal at the top level to prevent leaks
                CONTEXT.remove();
            }
        }
    }

    /**
     * Flatten a map and replace its contents in-place
     *
     * @param record The map to flatten and replace
     */
    public void flattenAndReplace(Map<String, Object> record) {
        if (record == null || record.isEmpty()) {
            return;
        }

        Map<String, Object> flattened = flatten(record);
        record.clear();
        record.putAll(flattened);
    }

    private Map<String, Object> flattenObject(Map<?, ?> obj, String prefix, int depth) {
        Map<String, Object> result = new LinkedHashMap<>();

        // Depth check
        if (depth >= maxDepth) {
            if (log.isDebugEnabled()) {
                log.debug("Max depth {} reached at prefix: {}", maxDepth, prefix);
            }
            result.put(prefix.isEmpty() ? "root" : prefix, stringifyObject(obj));
            return result;
        }

        // Circular reference check with proper backtracking
        FlattenContext context = CONTEXT.get();
        if (detectCircularReferences) {
            if (!context.enterObject(obj)) {
                if (log.isDebugEnabled()) {
                    log.debug("Circular reference detected at prefix: {}", prefix);
                }
                result.put(prefix.isEmpty() ? "root" : prefix, "[CIRCULAR_REFERENCE]");
                return result;
            }
        }

        try {
            String separator = getSeparator();
            Set<String> usedKeys = new HashSet<>();
            int entryCount = 0;

            for (Map.Entry<?, ?> entry : obj.entrySet()) {
                if (entryCount >= maxMapSize) {
                    log.warn("Map size limit reached at depth {}", depth);
                    break;
                }

                String safeKey = sanitizeKey(entry.getKey(), usedKeys);
                usedKeys.add(safeKey);

                String newKey = buildKey(prefix, separator, safeKey);

                if (shouldIncludePath(newKey)) {
                    result.putAll(flattenValue(newKey, entry.getValue(), depth));
                }

                entryCount++;
            }
        } finally {
            // Proper backtracking - remove from visited after processing children
            if (detectCircularReferences) {
                context.exitObject(obj);
            }
        }

        return result;
    }

    private String buildKey(String prefix, String separator, String key) {
        if (prefix == null || prefix.isEmpty()) {
            return key;
        }

        // Use StringBuilder for efficiency
        return prefix + separator + key;
    }

    private Map<String, Object> flattenValue(String key, Object value, int depth) {
        Map<String, Object> result = new LinkedHashMap<>();

        if (value == null) {
            result.put(key, null);

        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<?, ?> mapValue = (Map<?, ?>) value;
            if (mapValue.isEmpty()) {
                result.put(key, null);
            } else {
                result.putAll(flattenObject(mapValue, key, depth + 1));
            }

        } else if (value instanceof List) {
            result.putAll(flattenList(key, (List<?>) value, depth));

        } else if (value instanceof Set) {
            result.putAll(flattenList(key, new ArrayList<>((Set<?>) value), depth));

        } else if (value instanceof Collection) {
            result.putAll(flattenList(key, new ArrayList<>((Collection<?>) value), depth));

        } else if (value.getClass().isArray()) {
            result.putAll(flattenArray(key, value, depth));

        } else if (parseNestedJsonStrings && value instanceof String) {
            String strValue = (String) value;
            if (strValue.length() <= maxJsonStringLength) {
                Object parsed = tryParseJson(strValue);
                if (parsed != null && parsed != value) {
                    if (parsed instanceof Map) {
                        result.putAll(flattenObject((Map<?, ?>) parsed, key, depth + 1));
                    } else if (parsed instanceof List) {
                        result.putAll(flattenList(key, (List<?>) parsed, depth));
                    } else {
                        result.put(key, normalizePrimitive(parsed));
                    }
                } else {
                    result.put(key, normalizePrimitive(value));
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Skipping JSON parsing for string exceeding maxJsonStringLength: {}", key);
                }
                result.put(key, normalizePrimitive(value));
            }

        } else {
            result.put(key, normalizePrimitive(value));
        }

        return result;
    }

    private Map<String, Object> flattenArray(String key, Object array, int depth) {
        List<?> list;

        // Handle primitive arrays
        Class<?> componentType = array.getClass().getComponentType();
        if (componentType.isPrimitive()) {
            list = convertPrimitiveArray(array, componentType);
        } else {
            list = Arrays.asList((Object[]) array);
        }

        return flattenList(key, list, depth);
    }

    private List<?> convertPrimitiveArray(Object array, Class<?> componentType) {
        int length = Array.getLength(array);
        List<Object> list = new ArrayList<>(length);

        if (componentType == int.class) {
            for (int i = 0; i < length; i++) list.add(Array.getInt(array, i));
        } else if (componentType == long.class) {
            for (int i = 0; i < length; i++) list.add(Array.getLong(array, i));
        } else if (componentType == double.class) {
            for (int i = 0; i < length; i++) list.add(Array.getDouble(array, i));
        } else if (componentType == float.class) {
            for (int i = 0; i < length; i++) list.add(Array.getFloat(array, i));
        } else if (componentType == boolean.class) {
            for (int i = 0; i < length; i++) list.add(Array.getBoolean(array, i));
        } else if (componentType == byte.class) {
            for (int i = 0; i < length; i++) list.add(Array.getByte(array, i));
        } else if (componentType == short.class) {
            for (int i = 0; i < length; i++) list.add(Array.getShort(array, i));
        } else if (componentType == char.class) {
            for (int i = 0; i < length; i++) list.add(Array.getChar(array, i));
        }

        return list;
    }

    private Map<String, Object> flattenList(String key, List<?> list, int depth) {
        Map<String, Object> result = new LinkedHashMap<>();

        if (list.isEmpty()) {
            result.put(key, null);
            return result;
        }

        // Check for depth limit
        if (depth >= maxDepth) {
            if (log.isDebugEnabled()) {
                log.debug("Max depth {} reached at key: {}", maxDepth, key);
            }
            result.put(key, stringifyObject(list));
            return result;
        }

        int limit = Math.min(list.size(), maxArraySize);
        if (list.size() > maxArraySize && log.isDebugEnabled()) {
            log.debug("Array size ({}) exceeds maxArraySize ({}), truncating at key: {}",
                    list.size(), maxArraySize, key);
        }

        // Check what's in the array
        boolean allPrimitives = true;
        boolean hasNestedArrays = false;
        boolean hasMaps = false;

        for (int i = 0; i < limit; i++) {
            Object item = list.get(i);
            if (item instanceof List || (item != null && item.getClass().isArray())) {
                hasNestedArrays = true;
                allPrimitives = false;
            } else if (item instanceof Map) {
                hasMaps = true;
                allPrimitives = false;
            } else if (item != null && !isPrimitive(item)) {
                allPrimitives = false;
            }
        }

        // Case 1: All primitives - simple serialization
        if (allPrimitives) {
            List<Object> values = new ArrayList<>(limit);
            for (int i = 0; i < limit; i++) {
                Object item = list.get(i);
                values.add(item == null ? null : normalizePrimitive(item));
            }
            result.put(key, serializeArray(values));
            return result;
        }

        // Case 2: Nested arrays - recursively extract fields while preserving structure
        if (hasNestedArrays) {
            Map<String, List<Object>> fieldStructures = extractFieldsPreservingStructure(list, limit, depth);

            String separator = useArrayBoundarySeparator ? "__" : "_";
            for (Map.Entry<String, List<Object>> entry : fieldStructures.entrySet()) {
                String fieldName = entry.getKey();

                // Handle sentinel key for non-map items
                String fieldKey;
                if ("_value".equals(fieldName)) {
                    // No field extraction - just use the base key
                    fieldKey = key;
                } else {
                    fieldKey = buildKey(key, separator, fieldName);
                }

                result.put(fieldKey, serializeArray(entry.getValue()));
            }
            return result;
        }

        // Case 3: Array of maps at this level (FIXED VERSION with recursive flattening)
        if (hasMaps) {
            Map<String, List<Object>> fieldValues = new LinkedHashMap<>();
            String separator = useArrayBoundarySeparator ? "__" : "_";

            for (int i = 0; i < limit; i++) {
                Object item = list.get(i);

                if (item instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<?, ?> itemMap = (Map<?, ?>) item;

                    // FIXED: Recursively flatten each map in the array
                    Map<String, Object> flattenedItem = flattenObject(itemMap, "", depth + 1);

                    // Collect the flattened fields
                    for (Map.Entry<String, Object> entry : flattenedItem.entrySet()) {
                        String fieldKey = buildKey(key, separator, entry.getKey());

                        if (!fieldValues.containsKey(fieldKey)) {
                            fieldValues.put(fieldKey, new ArrayList<>());
                        }

                        fieldValues.get(fieldKey).add(entry.getValue());
                    }
                } else {
                    // Non-map item in the array
                    if (!fieldValues.containsKey(key)) {
                        fieldValues.put(key, new ArrayList<>());
                    }
                    Object normalizedValue = item == null ? null :
                            (isPrimitive(item) ? normalizePrimitive(item) : stringifyObject(item));
                    fieldValues.get(key).add(normalizedValue);
                }
            }

            // Ensure all arrays have consistent length (pad with nulls)
            int maxSize = fieldValues.values().stream()
                    .mapToInt(List::size)
                    .max()
                    .orElse(0);

            for (Map.Entry<String, List<Object>> entry : fieldValues.entrySet()) {
                List<Object> values = entry.getValue();
                while (values.size() < maxSize) {
                    values.add(null);
                }
                result.put(entry.getKey(), serializeArray(values));
            }

            return result;
        }

        return result;
    }

    /**
     * Extract fields from nested arrays while preserving the nesting structure
     * <p>
     * Example: [[{number:1}, {number:2}], [{number:3}]]
     * Returns: {number: [[1, 2], [3]]}
     */
    private Map<String, List<Object>> extractFieldsPreservingStructure(List<?> list, int limit, int depth) {
        Map<String, List<Object>> fieldStructures = new LinkedHashMap<>();

        int processedCount = 0;
        for (int i = 0; i < limit && processedCount < maxArraySize; i++) {
            Object item = list.get(i);

            if (item instanceof List) {
                List<?> nestedList = (List<?>) item;

                // Recursively extract from nested list
                Map<String, List<Object>> nestedFields = extractFieldsFromList(nestedList, depth + 1);

                // Merge into field structures, preserving nesting
                for (Map.Entry<String, List<Object>> entry : nestedFields.entrySet()) {
                    String fieldName = entry.getKey();
                    if (!fieldStructures.containsKey(fieldName)) {
                        fieldStructures.put(fieldName, new ArrayList<>());
                    }
                    // Add the nested array as a single element (preserves structure)
                    fieldStructures.get(fieldName).add(entry.getValue());
                }

            } else if (item != null && item.getClass().isArray()) {
                // Convert array to list and process
                Class<?> componentType = item.getClass().getComponentType();
                List<?> arrayAsList = componentType.isPrimitive()
                        ? convertPrimitiveArray(item, componentType)
                        : Arrays.asList((Object[]) item);

                Map<String, List<Object>> nestedFields = extractFieldsFromList(arrayAsList, depth + 1);

                for (Map.Entry<String, List<Object>> entry : nestedFields.entrySet()) {
                    String fieldName = entry.getKey();
                    if (!fieldStructures.containsKey(fieldName)) {
                        fieldStructures.put(fieldName, new ArrayList<>());
                    }
                    fieldStructures.get(fieldName).add(entry.getValue());
                }
            } else {
                // Non-array/non-map item at nested level - use sentinel key
                String sentinelKey = "_value";
                if (!fieldStructures.containsKey(sentinelKey)) {
                    fieldStructures.put(sentinelKey, new ArrayList<>());
                }
                Object normalizedValue = item == null ? null :
                        (isPrimitive(item) ? normalizePrimitive(item) : stringifyObject(item));
                fieldStructures.get(sentinelKey).add(normalizedValue);
            }

            processedCount++;
        }

        return fieldStructures;
    }

    /**
     * Extract field values from a list, handling both maps and nested arrays
     * Returns map of field names to their collected values
     * <p>
     * IMPORTANT: Uses "_value" as a sentinel key for non-map items to avoid
     * empty string key collisions
     */
    private Map<String, List<Object>> extractFieldsFromList(List<?> list, int depth) {
        Map<String, List<Object>> fields = new LinkedHashMap<>();

        if (list.isEmpty()) {
            return fields;
        }

        // Check for depth limit
        if (depth >= maxDepth) {
            if (log.isDebugEnabled()) {
                log.debug("Max depth {} reached during nested array extraction", maxDepth);
            }
            List<Object> stringified = new ArrayList<>();
            stringified.add(stringifyObject(list));
            fields.put("_value", stringified);
            return fields;
        }

        // Apply array size limit
        int limit = Math.min(list.size(), maxArraySize);
        if (list.size() > maxArraySize && log.isDebugEnabled()) {
            log.debug("Array size ({}) exceeds maxArraySize ({}) during extraction",
                    list.size(), maxArraySize);
        }

        // Determine the content type
        boolean hasNestedArrays = false;
        boolean hasMaps = false;

        for (int i = 0; i < limit; i++) {
            Object item = list.get(i);
            if (item instanceof List || (item != null && item.getClass().isArray())) {
                hasNestedArrays = true;
                break;
            } else if (item instanceof Map) {
                hasMaps = true;
                break;
            }
        }

        if (hasNestedArrays) {
            // Nested arrays - recurse deeper with structure preservation
            return extractFieldsPreservingStructure(list, limit, depth);
        }

        if (hasMaps) {
            // Array of maps - extract fields with proper key tracking (FIXED VERSION)
            Set<String> usedKeys = new HashSet<>();

            for (int i = 0; i < limit; i++) {
                Object item = list.get(i);

                if (item instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<?, ?> map = (Map<?, ?>) item;

                    // FIXED: Recursively flatten the map
                    Map<String, Object> flattenedMap = flattenObject(map, "", depth + 1);

                    for (Map.Entry<String, Object> entry : flattenedMap.entrySet()) {
                        String fieldName = entry.getKey();
                        if (!fields.containsKey(fieldName)) {
                            fields.put(fieldName, new ArrayList<>());
                        }
                        fields.get(fieldName).add(entry.getValue());
                    }
                } else {
                    // Mixed content - non-map item in array of maps
                    String sentinelKey = "_value";
                    if (!fields.containsKey(sentinelKey)) {
                        fields.put(sentinelKey, new ArrayList<>());
                    }
                    Object normalizedValue = item == null ? null :
                            (isPrimitive(item) ? normalizePrimitive(item) : stringifyObject(item));
                    fields.get(sentinelKey).add(normalizedValue);
                }
            }

            // Pad arrays for consistency
            int maxSize = fields.values().stream()
                    .mapToInt(List::size)
                    .max()
                    .orElse(0);

            for (Map.Entry<String, List<Object>> entry : fields.entrySet()) {
                List<Object> values = entry.getValue();
                while (values.size() < maxSize) {
                    values.add(null);
                }
            }

            return fields;
        }

        // Primitives - return as-is with sentinel key
        List<Object> values = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) {
            Object item = list.get(i);
            values.add(item == null ? null : normalizePrimitive(item));
        }
        fields.put("_value", values);
        return fields;
    }

    /**
     * Serialize an array/list according to the configured format
     */
    private String serializeArray(List<?> values) {
        switch (arrayFormat) {
            case JSON:
                try {
                    return objectMapper.writeValueAsString(values);
                } catch (JsonProcessingException e) {
                    log.warn("Failed to serialize array as JSON, falling back to toString", e);
                    return values.toString();
                }

            case COMMA_SEPARATED:
                // Simple comma-separated: 1,2,3 or Alice,Bob,Charlie
                return values.stream()
                        .map(v -> v == null ? "" : v.toString())
                        .collect(Collectors.joining(","));

            case PIPE_SEPARATED:
                // Pipe-separated: 1|2|3 (useful for Athena)
                return values.stream()
                        .map(v -> v == null ? "" : v.toString())
                        .collect(Collectors.joining("|"));

            case BRACKET_LIST:
                // Bracket notation without JSON escaping: [1, 2, 3]
                return values.toString();

            default:
                try {
                    return objectMapper.writeValueAsString(values);
                } catch (JsonProcessingException e) {
                    return values.toString();
                }
        }
    }

    private Object flattenSingleValue(Object value, int depth) {
        if (value == null) {
            return null;
        } else if (isPrimitive(value)) {
            return normalizePrimitive(value);
        } else {
            return stringifyObject(value);
        }
    }

    private boolean isPrimitive(Object value) {
        return !(value instanceof Map ||
                value instanceof List ||
                value instanceof Set ||
                value instanceof Collection ||
                (value != null && value.getClass().isArray()));
    }

    private Object normalizePrimitive(Object value) {
        if (value == null) {
            return null;
        }

        // Handle BigDecimal with optional precision preservation
        if (value instanceof BigDecimal) {
            if (preserveBigDecimalPrecision) {
                return value.toString();
            }
            double doubleValue = ((BigDecimal) value).doubleValue();
            if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                return value.toString();
            }
            return doubleValue;
        }

        if (value instanceof BigInteger) {
            return ((BigInteger) value).longValue();
        }

        // Handle Double/Float special values
        if (value instanceof Double) {
            double d = (Double) value;
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                return d.toString();
            }
        }
        if (value instanceof Float) {
            float f = (Float) value;
            if (Float.isNaN(f) || Float.isInfinite(f)) {
                return f.toString();
            }
        }

        // Handle Java 8+ date/time types
        if (value instanceof Temporal) {
            return value.toString();
        }

        // Handle enums
        if (value instanceof Enum) {
            return ((Enum<?>) value).name();
        }

        // Handle Optional
        if (value instanceof Optional) {
            Optional<?> opt = (Optional<?>) value;
            return opt.isPresent() ? normalizePrimitive(opt.get()) : null;
        }

        // Handle Date/Timestamp
        if (value instanceof Date) {
            return value.toString();
        }
        if (value instanceof Timestamp) {
            return value.toString();
        }

        // Handle primitives and wrappers
        if (value instanceof Number ||
                value instanceof Boolean ||
                value instanceof String ||
                value instanceof Character) {
            return value;
        }

        // Unknown type - stringify
        return value.toString();
    }

    private String stringifyObject(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to JSON-serialize object of type {}, falling back to toString",
                        obj.getClass().getName());
            }
            try {
                return obj.toString();
            } catch (Exception e2) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to call toString on object: {}", e2.getMessage());
                }
                return "[OBJECT:" + obj.getClass().getSimpleName() + "]";
            }
        }
    }

    /**
     * Try to parse a string as JSON
     * Returns the parsed object if successful, or the original string if not JSON
     */
    private Object tryParseJson(String str) {
        if (str == null || str.length() < 2) {
            return str;
        }

        String trimmed = str.trim();

        // Quick check: must start with { or [
        if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) {
            return str;
        }

        // Quick check: must end with } or ]
        if (!trimmed.endsWith("}") && !trimmed.endsWith("]")) {
            return str;
        }

        // Try parsing as-is first
        try {
            return objectMapper.readValue(trimmed, Object.class);
        } catch (Exception e) {
            // Failed - might be escaped JSON, try unescaping
            try {
                String unescaped = unescapeJson(trimmed);
                return objectMapper.readValue(unescaped, Object.class);
            } catch (Exception e2) {
                // Still failed - return original
                if (log.isTraceEnabled()) {
                    log.trace("Failed to parse potential JSON string");
                }
                return str;
            }
        }
    }

    /**
     * Unescape JSON strings that have been escaped
     * <p>
     * CRITICAL: Process \\\\ LAST to avoid double-unescaping
     * Example: "\\n" should become \n (literal backslash-n), not a newline
     * <p>
     * NOTE: This handles common double-escaped JSON patterns but may not
     * correctly handle exotic cases like triple-escaping ("\\\\\\n").
     * In practice, this method only runs when Jackson fails to parse,
     * which is rare. Standard JSON parsing handles escaping correctly.
     */
    private String unescapeJson(String str) {
        return str.replace("\\\"", "\"")
                .replace("\\/", "/")
                .replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("\\t", "\t")
                .replace("\\b", "\b")
                .replace("\\f", "\f")
                .replace("\\\\", "\\"); // MUST BE LAST!
    }

    private String sanitizeKey(Object key, Set<String> existingKeys) {
        if (key == null) {
            return generateUniqueKey("null_key", existingKeys);
        }

        String strKey;
        try {
            strKey = key.toString();
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Error converting key to string: {}", e.getMessage());
            }
            return generateUniqueKey("error_key", existingKeys);
        }

        // Empty key handling
        if (strKey.isEmpty()) {
            return generateUniqueKey("empty_key", existingKeys);
        }

        if (strictKeyValidation) {
            // Remove invalid characters for strict mode
            strKey = strKey.replaceAll("[^a-zA-Z0-9_]", "_");
        }

        // Prevent key conflicts with separator
        if (useArrayBoundarySeparator) {
            strKey = strKey.replace("__", "_");
        }

        // Ensure uniqueness
        return generateUniqueKey(strKey, existingKeys);
    }

    private String generateUniqueKey(String baseKey, Set<String> existingKeys) {
        if (!existingKeys.contains(baseKey)) {
            return baseKey;
        }

        // Key collision - find unique suffix
        // Safety: limit attempts to prevent infinite loops
        int counter = 2;
        int maxAttempts = 10000;
        String uniqueKey;

        do {
            uniqueKey = baseKey + "_" + counter;
            counter++;

            if (counter > maxAttempts) {
                log.error("Cannot generate unique key after {} attempts for base: {}", maxAttempts, baseKey);
                throw new IllegalStateException("Unable to generate unique key for: " + baseKey);
            }
        } while (existingKeys.contains(uniqueKey));

        return uniqueKey;
    }

    private String getSeparator() {
        return useArrayBoundarySeparator ? "__" : "_";
    }

    private boolean shouldIncludePath(String path) {
        if (excludePaths != null && !excludePaths.isEmpty()) {
            for (String pattern : excludePaths) {
                if (matchesPattern(path, pattern)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean matchesPattern(String path, String pattern) {
        if (pattern.contains("*")) {
            // Wildcard matching - use cached compiled pattern with proper escaping
            Pattern compiledPattern = patternCache.computeIfAbsent(pattern, p -> {
                // Properly escape the pattern
                String[] parts = p.split("\\*", -1);
                StringBuilder regex = new StringBuilder();

                for (int i = 0; i < parts.length; i++) {
                    regex.append(Pattern.quote(parts[i]));
                    if (i < parts.length - 1) {
                        regex.append(".*");
                    }
                }

                return Pattern.compile(regex.toString());
            });
            return compiledPattern.matcher(path).matches();
        } else {
            // Exact or prefix match
            String separator = getSeparator();
            return path.equals(pattern) || path.startsWith(pattern + separator);
        }
    }

    private Map<String, Object> transformKeys(Map<String, Object> input) {
        Map<String, Object> transformed = new LinkedHashMap<>();
        Set<String> usedKeys = new HashSet<>();

        for (Map.Entry<String, Object> entry : input.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String newKey = applyNamingStrategy(key);

            // Ensure uniqueness after transformation
            newKey = generateUniqueKey(newKey, usedKeys);
            usedKeys.add(newKey);

            transformed.put(newKey, value);
        }

        return transformed;
    }

    private String applyNamingStrategy(String key) {
        switch (namingStrategy) {
            case SNAKE_CASE:
                // CamelCase to snake_case
                return key.replaceAll("([A-Z])", "_\$1")
                        .toLowerCase()
                        .replaceAll("^_", "")
                        .replaceAll("_+", "_");
            case LOWER_CASE:
                return key.toLowerCase();
            case UPPER_CASE:
                return key.toUpperCase();
            default:
                return key;
        }
    }

    /**
     * Create a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for MapFlattener configuration
     */
    public static class Builder {
        private int maxDepth = 50;
        private int maxArraySize = 1000;
        private int maxMapSize = 10000;
        private int maxJsonStringLength = 1000000;
        private boolean useArrayBoundarySeparator = false;
        private FieldNamingStrategy namingStrategy = FieldNamingStrategy.AS_IS;
        private List<String> excludePaths = new ArrayList<>();
        private boolean detectCircularReferences = true;
        private boolean strictKeyValidation = false;
        private boolean parseNestedJsonStrings = false;
        private boolean preserveBigDecimalPrecision = false;
        private ArraySerializationFormat arrayFormat = ArraySerializationFormat.JSON;

        public Builder maxDepth(int depth) {
            if (depth < 1) {
                throw new IllegalArgumentException("maxDepth must be >= 1");
            }
            this.maxDepth = depth;
            return this;
        }

        public Builder maxArraySize(int size) {
            if (size < 1) {
                throw new IllegalArgumentException("maxArraySize must be >= 1");
            }
            this.maxArraySize = size;
            return this;
        }

        public Builder maxMapSize(int size) {
            if (size < 1) {
                throw new IllegalArgumentException("maxMapSize must be >= 1");
            }
            this.maxMapSize = size;
            return this;
        }

        public Builder maxJsonStringLength(int length) {
            if (length < 1) {
                throw new IllegalArgumentException("maxJsonStringLength must be >= 1");
            }
            this.maxJsonStringLength = length;
            return this;
        }

        public Builder useArrayBoundarySeparator(boolean use) {
            this.useArrayBoundarySeparator = use;
            return this;
        }

        public Builder namingStrategy(FieldNamingStrategy strategy) {
            this.namingStrategy = strategy != null ? strategy : FieldNamingStrategy.AS_IS;
            return this;
        }

        public Builder excludePaths(String... paths) {
            this.excludePaths = Arrays.asList(paths);
            return this;
        }

        public Builder detectCircularReferences(boolean detect) {
            this.detectCircularReferences = detect;
            return this;
        }

        public Builder strictKeyValidation(boolean strict) {
            this.strictKeyValidation = strict;
            return this;
        }

        public Builder parseNestedJsonStrings(boolean parse) {
            this.parseNestedJsonStrings = parse;
            return this;
        }

        public Builder preserveBigDecimalPrecision(boolean preserve) {
            this.preserveBigDecimalPrecision = preserve;
            return this;
        }

        public Builder arrayFormat(ArraySerializationFormat format) {
            this.arrayFormat = format != null ? format : ArraySerializationFormat.JSON;
            return this;
        }

        public MapFlattener build() {
            return new MapFlattener(this);
        }
    }

    /**
     * Field naming strategy for key transformation
     */
    public enum FieldNamingStrategy {
        AS_IS,
        SNAKE_CASE,
        LOWER_CASE,
        UPPER_CASE
    }

    /**
     * Array serialization format options
     */
    public enum ArraySerializationFormat {
        /**
         * JSON format with quotes and escaping: ["Alice","Bob"] or [1,2,3]
         * Best for: APIs, JSON storage
         */
        JSON,

        /**
         * Comma-separated without quotes: Alice,Bob or 1,2,3
         * Best for: CSV files, simple Athena queries
         * WARNING: Values containing commas will create ambiguous output
         */
        COMMA_SEPARATED,

        /**
         * Pipe-separated without quotes: Alice|Bob or 1|2|3
         * Best for: Athena with SERDEPROPERTIES ('field.delim'='|')
         * WARNING: Values containing pipes will create ambiguous output
         */
        PIPE_SEPARATED,

        /**
         * Bracket list without JSON escaping: [Alice, Bob] or [1, 2, 3]
         * Best for: Athena array&lt;string&gt; or array&lt;int&gt; types with primitive values
         */
        BRACKET_LIST
    }
}