package io.github.pierce;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.github.pierce.MapFlattener.ArraySerializationFormat.*;
import static io.github.pierce.MapFlattener.FieldNamingStrategy.*;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.temporal.Temporal;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import io.github.pierce.path.FlattenedPath;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Production-hardened Map flattener with comprehensive edge case handling
 * <p>
 * Thread-safe, handles circular references correctly, and provides extensive
 * configuration options for edge cases.
 *
 * <h2>Basic Flattening</h2>
 * <pre>
 * {user: {name: "John", age: 30}}
 * → {user_name: "John", user_age: 30}
 * </pre>
 *
 * <h2>Array Handling</h2>
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
 * // Mixed content at a NESTED array level - MEASURED:
 * Input:  {data: [[{name:"A"}], "text"]}
 * Output: {data_name: "[["A"],null]", data: "[[null],"text"]"}
 *
 * // Both columns are now padded to the OUTER element count (2.1.0, BL-018). data_name holds
 * // the inner list [["A"]] at outer position 0 and a bare null at position 1, because position
 * // 1 holds no nested list at all. data holds an inner list of one null at position 0 - the
 * // inner cardinality that position genuinely had - and the scalar at position 1.
 * //
 * // The sentinel still maps to the BASE key, not to a "_value" suffix. This javadoc once
 * // documented {data_name: "[["A"]]", data_value: "[[null],["text"]]"}, which the code has
 * // never produced and still does not; the fixture
 * // structural/mixed-nested-array-sentinel-collision exists because of that remaining half.
 * </pre>
 *
 * <h2>Positional contract for array-element columns</h2>
 * At ALL THREE array-element sites - {@code flattenList} Case 3, {@code extractFieldsFromList}
 * and {@code extractFieldsPreservingStructure} - every emitted column carries exactly one entry
 * per source element at that level (capped by {@code maxArraySize}), with element {@code i}'s
 * value at index {@code i} and an explicit hole where element {@code i} did not carry that
 * column. Values are written by INDEX and never appended.
 * <p>
 * THE SHAPE OF A HOLE DIFFERS BY SITE, because the element type of a column differs by site, and
 * that is the one genuine difference left:
 * <ul>
 *   <li>At the two ARRAY-OF-MAPS sites a column entry is a SCALAR, so a hole is a scalar
 *       {@code null}. This is what {@link #columnFor} writes.</li>
 *   <li>At the NESTED-ARRAY site a column entry is an INNER LIST, so a hole is an inner list of
 *       that outer position's inner cardinality - {@code []} exactly when the inner array was
 *       empty. A BARE {@code null} appears only where the outer position holds no nested list at
 *       all. Measured: {@code {"g":[[{"a":1},{"b":2}],[{"a":3}]]}} emits
 *       {@code g_a="[[1,null],[3]]"} beside {@code g_b="[[null,2],[null]]"} - two outer entries
 *       each, agreeing position by position on inner length as well.</li>
 * </ul>
 * Do not collapse the two fillers into one. Filling a nested hole with a bare {@code null}
 * changes the slot's TYPE in a column whose entries have always been lists; filling it with
 * {@code []} destroys the distinction the corpus pins in
 * {@code structural/array-of-arrays-with-empty-inner}, where "this position's inner array was
 * empty" and "this position's inner array had elements, none of which carried this column" are
 * different facts.
 *
 * <h2>For AWS Athena</h2>
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
 * <h2>Important: Delimiter Collisions</h2>
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
 * <h2>Null Handling in Delimited Formats</h2>
 * In COMMA_SEPARATED and PIPE_SEPARATED formats, null values become empty strings:
 * <pre>
 * [null, "", "value"] → ",,value"
 * </pre>
 * Empty string and null are indistinguishable in the output.
 *
 * <h2>Circular References</h2>
 * Detected and marked as [CIRCULAR_REFERENCE] when enabled.
 * Shared references (same object in multiple places) work correctly.
 *
 * <h2>Thread Safety</h2>
 * Fully thread-safe. Multiple threads can share a single MapFlattener instance.
 * Uses ThreadLocal for circular reference detection without contention.
 */
public class MapFlattener implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(MapFlattener.class);

    /**
     * Column name for an array element that is not a map.
     *
     * <p>It maps to the BASE key on the way out, not to a {@code _value}-suffixed column - see
     * {@code flattenList} Case 2. The class javadoc documented the suffixed shape for a long time
     * and the code has never produced it; the fixture
     * {@code structural/mixed-nested-array-sentinel-collision} exists because of that gap.</p>
     */
    private static final String VALUE_SENTINEL = "_value";

    /**
     * The floor every {@code Builder} bound shares.
     *
     * <p>Named rather than written as {@code 1} five times, which PMD counted five times under
     * {@code AvoidLiteralsInIfCondition} and which is genuinely worse to read: the five guards
     * are one rule, and a reader should not have to check that the fifth agrees with the first
     * four.</p>
     */
    private static final int MIN_BOUND = 1;

    /**
     * Default ceiling on array-element cells emitted by one {@link #flatten(Map)} call.
     *
     * <p>2^20, chosen against the published figure rather than picked. At the shipped defaults
     * the worst documented shape - 1000 sparse elements with 1000 distinct keys, the case
     * CHANGELOG item 13 and {@code SparseArrayOfMapsOutputSizeTest} both pin - is exactly
     * 1,000,000 cells. This sits just above it, so every document at or below the published
     * figure keeps working with 48,576 cells of headroom and no knife edge, while the 2e7 and
     * 1e8 shapes above it are refused.</p>
     *
     * <p>BE HONEST ABOUT WHAT THIS DOES NOT FIX. The default still permits roughly 391x
     * amplification: 12,787 input characters legitimately producing 5,005,780 output characters
     * is UNDER budget by design, because refusing it would invalidate a published release note.
     * This bound stops heap exhaustion. It does not make the library safe on untrusted input at
     * the default - lower {@code maxArrayCells} if you accept untrusted documents.</p>
     */
    public static final int DEFAULT_MAX_ARRAY_CELLS = 1_048_576;

    /**
     * One {@code flatten} call tried to emit more array-element cells than {@code maxArrayCells}.
     *
     * <p>Extends {@link IllegalStateException} so it stays inside the failure contract
     * {@link #flatten(Map)} already documents for excessive depth and circular references.</p>
     *
     * <h2>Why this REFUSES instead of truncating</h2>
     *
     * <p>Truncation is the obvious alternative and it is the dangerous one. Dropping columns past
     * the budget leaves a flat map whose surviving columns are all still exactly the right
     * length, so nothing downstream - not {@code AvroReconstructor}'s
     * {@code ArrayCardinalityException}, not {@code agreedElementCount}, not any length check
     * anywhere - can tell that whole fields have vanished. That is precisely the defect class of
     * the 2.1.0 array-element alignment repair: a length invariant satisfied while the data is
     * wrong. A refusal is the only outcome that cannot be silent. Do not soften this into a
     * truncation.</p>
     */
    public static class FlattenLimitExceededException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        public FlattenLimitExceededException(String message) {
            super(message);
        }
    }

    private final int maxDepth;
    private final int maxArraySize;
    private final int maxMapSize;
    private final int maxJsonStringLength;
    private final int maxArrayCells;
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

        /**
         * Array-element cells allocated so far by THIS invocation.
         *
         * <p>Per invocation, not per array. Measured: fifty sibling arrays of five hundred
         * sparse elements are 250,000 cells each - under any sane per-array budget - and
         * 62,778,390 output characters in total. A per-array budget lets all fifty through.</p>
         */
        private long arrayCells;

        void clear() {
            visitedIds.clear();
            visitStack.clear();
            arrayCells = 0;
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
        this.maxArrayCells = builder.maxArrayCells > 0 ? builder.maxArrayCells : DEFAULT_MAX_ARRAY_CELLS;
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

        } catch (FlattenLimitExceededException e) {
            // MUST BE FIRST, and the test that proves it is
            // MapFlattenerArrayCellBudgetTest#theTypedExceptionEscapesFlattenUnwrapped. The
            // catch(Exception) arm below would rewrap this into a bare RuntimeException, so a
            // caller writing `catch (FlattenLimitExceededException)` - exactly what this class's
            // javadoc tells them to write - would catch nothing. It is not silent in the log; it
            // is silent to the type system, which is where the guarantee was supposed to live.
            // It is also NOT logged again here: chargeCells already logged it at WARN with the
            // message and no stack, because a full stack trace per refusal is itself an
            // amplification vector on the exact input class an attacker controls.
            throw e;
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

    /**
     * Builds a flattened key by appending one escaped segment.
     *
     * <p>The segment is escaped so that a separator character occurring inside a field name
     * cannot be mistaken for a structural separator. Without this, {@code {"user_id": 1}} and
     * {@code {"user": {"id": 1}}} produce the identical key {@code "user_id"} and reconstruction
     * cannot tell them apart.</p>
     *
     * <p>That ambiguity was not only lossy, it was quadratic: the reconstructor groups by every
     * possible prefix, so each extra separator inside a field name multiplies the candidate
     * groupings and the number of paths falsely detected as arrays. Holding structure fixed at 40
     * flattened keys, one additional underscore per field name took reconstruction from ~200 ms
     * to heap exhaustion.</p>
     *
     * <p>Escaping incrementally here — rather than collecting segments and encoding at each leaf
     * — keeps this O(1) appends per level, and produces byte-identical output to
     * {@link FlattenedPath#encode}.</p>
     */
    private String buildKey(String prefix, String separator, String key) {
        return joinEncodedKey(prefix, separator, FlattenedPath.escapeSegment(key, separator));
    }

    /**
     * Appends an ALREADY-ENCODED path fragment, without escaping it again.
     *
     * <p>The array-extraction paths recursively call {@code flattenObject} on each element and
     * then prefix the resulting keys. Those keys are complete encoded paths — their internal
     * separators are structural and were escaped correctly on the way in — so passing them
     * through {@link #buildKey} would escape the structure itself, turning
     * {@code accounts_electronicDelivery_consentIndicator} into
     * {@code accounts_electronicDelivery\_consentIndicator} and collapsing two real levels into
     * one literal field name.</p>
     *
     * <p>Keeping the two operations separate is what guarantees every segment is escaped exactly
     * once: {@link #buildKey} at the raw-field-name boundary, this method everywhere a
     * pre-encoded path is being extended.</p>
     */
    private String joinEncodedKey(String prefix, String separator, String encodedPath) {
        if (prefix == null || prefix.isEmpty()) {
            return encodedPath;
        }
        return prefix + separator + encodedPath;
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
                if (VALUE_SENTINEL.equals(fieldName)) {
                    // No field extraction - just use the base key
                    fieldKey = key;
                } else {
                    fieldKey = joinEncodedKey(key, separator, fieldName);
                }

                result.put(fieldKey, serializeArray(entry.getValue()));
            }
            return result;
        }

        // Case 3: Array of maps at this level.
        //
        // POSITIONAL CONTRACT: every column is born `limit` long and is written by INDEX. Values
        // are never appended and the columns are never equalised afterwards.
        //
        // The previous shape appended each value as it was encountered and then tail-padded every
        // column up to the longest. That made the LENGTHS agree while leaving the VALUES under the
        // wrong elements - a column first seen at element k landed at index 0 - so no length check
        // anywhere downstream, including AvroReconstructor's ArrayCardinalityException, could see
        // it. The equaliser is deleted rather than kept beside the indexed writes precisely so a
        // future append path cannot silently re-shift and still pass every length assertion.
        if (hasMaps) {
            Map<String, List<Object>> fieldValues = new LinkedHashMap<>();
            String separator = useArrayBoundarySeparator ? "__" : "_";

            for (int i = 0; i < limit; i++) {
                Object item = list.get(i);

                if (item instanceof Map) {
                    Map<?, ?> itemMap = (Map<?, ?>) item;

                    // Recursively flatten each map in the array
                    Map<String, Object> flattenedItem = flattenObject(itemMap, "", depth + 1);

                    // Collect the flattened fields, each under its own element index
                    for (Map.Entry<String, Object> entry : flattenedItem.entrySet()) {
                        String fieldKey = joinEncodedKey(key, separator, entry.getKey());
                        columnFor(fieldValues, fieldKey, limit).set(i, entry.getValue());
                    }
                } else {
                    // Non-map item in the array - carried under the base key
                    Object normalizedValue = item == null ? null :
                            (isPrimitive(item) ? normalizePrimitive(item) : stringifyObject(item));
                    columnFor(fieldValues, key, limit).set(i, normalizedValue);
                }
            }

            for (Map.Entry<String, List<Object>> entry : fieldValues.entrySet()) {
                result.put(entry.getKey(), serializeArray(entry.getValue()));
            }

            return result;
        }

        return result;
    }

    /**
     * What one outer position of a nested array contributed.
     *
     * @param nested    the inner columns, or {@code null} when this position holds no nested list
     * @param innerSize the entry count those inner columns agree on; 0 for an empty inner list
     * @param scalar    the normalized value when this position holds a non-list; else {@code null}
     */
    private record NestedPosition(Map<String, List<Object>> nested, int innerSize, Object scalar) {

        boolean holdsNestedList() {
            return nested != null;
        }
    }

    /**
     * Extract fields from nested arrays while preserving the nesting structure.
     * <p>
     * Example: [[{number:1}, {number:2}], [{number:3}]] returns {number: [[1, 2], [3]]}.
     *
     * <h3>POSITIONAL CONTRACT - the third and last array-element site</h3>
     *
     * This used to append one entry per outer position with no padding, so a column first
     * produced at outer position k landed at index 0. {@code {"g":[[{"a":1}],[{"b":2}]]}} emitted
     * {@code g_a="[[1]]"} beside {@code g_b="[[2]]"}, and a consumer zipping the two columns by
     * outer index read {@code a=1} and {@code b=2} as one nested group. They came from different
     * groups. Same silent corruption the other two sites had, one level deeper.
     * <p>
     * It is now two passes: pass 1 records what each outer position produced, pass 2 materialises
     * every column {@code limit} slots long. THE FILLER IS SHAPE-AWARE and that is the whole
     * difficulty of this site. At the array-of-maps sites a column entry is a SCALAR, so
     * {@code columnFor} fills a hole with a scalar {@code null}. Here a column entry is an INNER
     * LIST, so a hole is an inner list of the right inner cardinality - the same rule one level
     * down. A bare {@code null} appears only where the outer position holds no nested list at all.
     *
     * @see #columnFor the sibling rule, and why the two differ
     */
    private Map<String, List<Object>> extractFieldsPreservingStructure(List<?> list, int limit, int depth) {
        // PASS 1. `limit` is already min(size, maxArraySize) at both call sites, so the old
        // processedCount guard could never fire and is gone.
        List<NestedPosition> positions = new ArrayList<>(limit);
        Set<String> names = new LinkedHashSet<>();

        for (int i = 0; i < limit; i++) {
            Object item = list.get(i);

            if (!isNestedList(item)) {
                Object normalizedValue = item == null ? null
                        : (isPrimitive(item) ? normalizePrimitive(item) : stringifyObject(item));
                positions.add(new NestedPosition(null, 0, normalizedValue));
                names.add(VALUE_SENTINEL);
                continue;
            }

            Map<String, List<Object>> nested = extractFieldsFromList(asNestedList(item), depth + 1);
            if (nested.isEmpty()) {
                // An empty inner list, or an empty Java array. The List arm always recorded the
                // position under the sentinel; the ARRAY arm recorded nothing at all, so the
                // outer position vanished from every column and everything after it shifted left.
                // Both are handled the same way now.
                positions.add(new NestedPosition(nested, 0, null));
                names.add(VALUE_SENTINEL);
            } else {
                positions.add(new NestedPosition(nested, innerEntryCount(nested), null));
                names.addAll(nested.keySet());
            }
        }

        return materialiseNestedColumns(limit, names, positions);
    }

    /** Whether an outer position holds a nested list or a Java array. */
    private static boolean isNestedList(Object item) {
        return item instanceof List || (item != null && item.getClass().isArray());
    }

    /**
     * The nested list at an outer position.
     *
     * <p>Split from {@link #isNestedList} rather than returning {@code null} for "not a list":
     * a method that returns a collection or null makes every caller a candidate for a
     * NullPointerException and PMD is right to say so. The predicate answers the question and
     * this answers the follow-up, and neither can be called in the wrong order by accident
     * because this one throws on input the predicate would have rejected.</p>
     */
    private List<?> asNestedList(Object item) {
        if (item instanceof List) {
            return (List<?>) item;
        }
        Class<?> componentType = item.getClass().getComponentType();
        return componentType.isPrimitive()
                ? convertPrimitiveArray(item, componentType)
                : Arrays.asList((Object[]) item);
    }

    /**
     * The entry count the inner columns agree on.
     *
     * <p>Every arm of {@link #extractFieldsFromList} produces columns of equal length - the two
     * array-of-maps arms through {@link #columnFor}, the primitive arm by construction, the
     * maxDepth arm by returning one single-entry column, and the nested arm by this method's own
     * caller. The MAXIMUM is taken rather than the first, so that if a future arm ever returns
     * ragged columns the holes are still padded to the widest, and the outer alignment survives
     * even though the inner would then be a separate defect.</p>
     */
    private static int innerEntryCount(Map<String, List<Object>> nested) {
        int max = 0;
        for (List<Object> column : nested.values()) {
            max = Math.max(max, column.size());
        }
        return max;
    }

    /**
     * PASS 2: one column per name, each exactly {@code limit} slots long, written by index.
     *
     * <p>Extracted from its caller so neither method carries the whole two-pass shape, which is
     * how a cyclomatic-complexity ratchet gets raised by accident. Ratchets only go down.</p>
     */
    private Map<String, List<Object>> materialiseNestedColumns(
            int limit, Set<String> names, List<NestedPosition> positions) {

        Map<String, List<Object>> columns = new LinkedHashMap<>();
        for (String name : names) {
            // Charged against the same per-invocation budget as the two array-of-maps sites.
            // Before the 2.1.0 positional repair this site APPENDED, so its cost was linear in
            // present values and the budget's original design excluded it on those grounds. It
            // now pre-sizes to the outer element count exactly like the others, so it is
            // quadratic exactly like them - measured, 1000 sparse nested positions emit
            // 6,999,890 characters against 4,999,890 for the flat equivalent. Leaving it
            // uncharged would leave the WIDEST of the three sites unbounded.
            chargeCells(name, columns.size() + 1, limit);
            List<Object> column = new ArrayList<>(limit);
            for (int i = 0; i < limit; i++) {
                NestedPosition p = positions.get(i);
                if (!p.holdsNestedList()) {
                    // No nested list here at all, so there is no inner cardinality to honour and
                    // a bare null is the honest slot. This is the ONLY shape in which a bare null
                    // appears in a column of inner lists.
                    column.add(VALUE_SENTINEL.equals(name) ? p.scalar() : null);
                    continue;
                }
                List<Object> present = p.nested().get(name);
                // A hole in a column of inner lists is an inner list, never a bare null and never
                // an empty list. `[]` would collide head-on with the genuinely-empty-inner-array
                // case that structural/array-of-arrays-with-empty-inner pins: "position i's inner
                // array was empty" and "position i's inner array had elements, none of which
                // carried this column" are different facts and must stay distinguishable.
                column.add(present != null ? present : nestedHole(p.innerSize()));
            }
            columns.put(name, column);
        }
        return columns;
    }

    /** An inner-list hole of {@code innerSize} nulls; {@code []} exactly when the inner list was empty. */
    private static List<Object> nestedHole(int innerSize) {
        return new ArrayList<>(Collections.nCopies(innerSize, null));
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
            fields.put(VALUE_SENTINEL, stringified);
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
            // Array of maps one level down. Same positional contract as flattenList Case 3:
            // every column is born `limit` long and written by index, never appended and
            // never equalised afterwards. No corpus fixture reaches this arm, which is exactly
            // why it is covered by MapFlattenerColumnAlignmentTest - an alignment invariant that
            // holds at depth one and fails at depth two is worse than one that fails everywhere,
            // because callers start trusting it.
            for (int i = 0; i < limit; i++) {
                Object item = list.get(i);

                if (item instanceof Map) {
                    Map<?, ?> map = (Map<?, ?>) item;

                    // Recursively flatten the map
                    Map<String, Object> flattenedMap = flattenObject(map, "", depth + 1);

                    for (Map.Entry<String, Object> entry : flattenedMap.entrySet()) {
                        columnFor(fields, entry.getKey(), limit).set(i, entry.getValue());
                    }
                } else {
                    // Mixed content - non-map item in array of maps
                    Object normalizedValue = item == null ? null :
                            (isPrimitive(item) ? normalizePrimitive(item) : stringifyObject(item));
                    columnFor(fields, VALUE_SENTINEL, limit).set(i, normalizedValue);
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
        fields.put(VALUE_SENTINEL, values);
        return fields;
    }

    /**
     * The SCALAR half of the positional-hole invariant - the two array-of-maps sites.
     *
     * <p>Returns the existing column for {@code key}, or creates one already {@code size} entries
     * long and filled with nulls. Callers then write with {@code set(i, value)}, so element
     * {@code i}'s value is at index {@code i} and an element that did not carry the column leaves
     * an explicit null exactly where it sits.</p>
     *
     * <p>THERE ARE TWO FILLERS, NOT ONE, and they sit at different levels. This one fills with a
     * scalar {@code null} because a column entry here IS a scalar. One level out, at
     * {@link #extractFieldsPreservingStructure}, a column entry is an INNER LIST and the hole is
     * an inner list of nulls - see {@code nestedHole}. The two are deliberately different and
     * must not be unified: a bare null among inner lists changes the slot's type, which is the
     * objection that deferred the nested-array repair in the first place.</p>
     *
     * <p>A column that is born the right length cannot be shifted by a later append, which is the
     * whole point: the defect this replaces was an append loop plus a tail pad, and the tail pad
     * made every column the correct LENGTH while the values sat under the wrong elements. Both
     * array-of-maps sites share this helper so they cannot drift apart.</p>
     */
    private List<Object> columnFor(Map<String, List<Object>> columns, String key, int size) {
        List<Object> column = columns.get(key);
        if (column == null) {
            chargeCells(key, columns.size() + 1, size);
            column = new ArrayList<>(Collections.nCopies(size, null));
            columns.put(key, column);
        }
        return column;
    }

    /**
     * Charge {@code size} cells to this invocation's budget, or refuse.
     *
     * <p>Called at the moment a column is CREATED, at all three array-element sites. That is the
     * exact frame a measured OOM named: a 4,057,897-character well-formed document (1000
     * elements, 300 distinct keys each) exhausted a 1 GB heap inside
     * {@code new ArrayList<>(Collections.nCopies(size, null))}.</p>
     *
     * @throws FlattenLimitExceededException before the allocation happens, never after
     */
    private void chargeCells(String key, int columnsSoFar, int size) {
        FlattenContext context = CONTEXT.get();
        long next = context.arrayCells + size;
        if (next > maxArrayCells) {
            String message = "array-element cell budget exceeded while creating column '" + key
                    + "': " + columnsSoFar + " columns so far, " + size + " slots each, "
                    + next + " cells against maxArrayCells=" + maxArrayCells
                    + ". A sparse array of maps emits (union of distinct keys) x (element count) "
                    + "cells, which no other bound covers - maxArraySize caps the SLOT axis and "
                    + "maxMapSize caps keys PER ELEMENT, not the union across them. Raise "
                    + "maxArrayCells if this document is legitimate; lower it if you accept "
                    + "untrusted input. This is refused rather than truncated because dropping "
                    + "columns leaves every surviving column the correct length, so no "
                    + "downstream check could see the loss.";
            // NOT LOGGED. Throwing IS the report: this message reaches the caller in full,
            // and logging it as well would be double reporting whose only distinctive effect is
            // one log line per refusal - on the exact input class an attacker controls. The
            // first draft of this method logged at WARN and find-sec-bugs counted it as
            // CRLF_INJECTION_LOGS, which was the right complaint about the wrong half: the fix
            // is not to sanitise the log line, it is not to have one.
            throw new FlattenLimitExceededException(message);
        }
        context.arrayCells = next;
    }

    /**
     * Serialize an array/list according to the configured format
     */
    private String serializeArray(List<?> values) {
        // First, serialize any ByteBuffers in the array to Base64 strings
        // This prevents data loss when ByteBuffers are converted to strings
        List<Object> serializedValues = new ArrayList<>(values.size());
        for (Object value : values) {
            serializedValues.add(serializeValue(value));
        }

        // Now serialize the array based on the configured format
        switch (arrayFormat) {
            case JSON:
                try {
                    return objectMapper.writeValueAsString(serializedValues);
                } catch (JsonProcessingException e) {
                    log.warn("Failed to serialize array as JSON, falling back to toString", e);
                    return serializedValues.toString();
                }

            case COMMA_SEPARATED:
                // Simple comma-separated: 1,2,3 or Alice,Bob,Charlie
                return serializedValues.stream()
                        .map(v -> v == null ? "" : v.toString())
                        .collect(Collectors.joining(","));

            case PIPE_SEPARATED:
                // Pipe-separated: 1|2|3 (useful for Athena)
                return serializedValues.stream()
                        .map(v -> v == null ? "" : v.toString())
                        .collect(Collectors.joining("|"));

            case BRACKET_LIST:
                // Build proper bracket notation that can be parsed back
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < serializedValues.size(); i++) {
                    if (i > 0) sb.append(", ");
                    Object v = serializedValues.get(i);
                    if (v == null) {
                        sb.append("null");
                    } else if (v instanceof String) {
                        // Quote strings so they can be parsed back
                        sb.append('"').append(escapeString((String) v)).append('"');
                    } else if (v instanceof List) {
                        // Recursively serialize nested lists
                        sb.append(serializeArray((List<?>) v));
                    } else {
                        sb.append(v.toString());
                    }
                }
                sb.append("]");
                return sb.toString();

            default:
                try {
                    return objectMapper.writeValueAsString(serializedValues);
                } catch (JsonProcessingException e) {
                    return serializedValues.toString();
                }
        }
    }

    private String escapeString(String s) {
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private boolean isPrimitive(Object value) {
        return !(value instanceof Map ||
                value instanceof List ||
                value instanceof Set ||
                value instanceof Collection ||
                (value != null && value.getClass().isArray()));
    }

    /**
     * Serialize a value for storage in the flattened map.
     * Handles ByteBuffers specially by converting them to Base64-encoded strings.
     * This prevents data loss that occurs when ByteBuffer.toString() is called.
     *
     * @param value The value to serialize
     * @return The serialized value - ByteBuffers become "B64:..." strings, others unchanged
     */
    private Object serializeValue(Object value) {
        if (value == null) {
            return null;
        }

        // Special handling for ByteBuffer - encode as Base64 to preserve data
        if (value instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer) value;

            // Create a copy of the remaining bytes
            byte[] bytes = new byte[buf.remaining()];
            int originalPosition = buf.position();
            buf.get(bytes);
            buf.position(originalPosition); // Reset position so original buffer is unchanged

            // Return Base64 encoded string with "B64:" marker prefix
            // This marker helps AvroReconstructor identify Base64-encoded ByteBuffers
            return "B64:" + Base64.getEncoder().encodeToString(bytes);
        }

        // For all other types, return as-is
        return value;
    }

    private Object normalizePrimitive(Object value) {
        if (value == null) {
            return null;
        }

        // IMPORTANT: Serialize ByteBuffers to Base64 to prevent data loss
        // This must be done BEFORE any other processing
        Object serialized = serializeValue(value);
        if (serialized != value) {
            // Value was transformed (e.g., ByteBuffer to Base64 string)
            return serialized;
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
            // longValue() TRUNCATES the low 64 bits on overflow rather than failing, so a
            // BigInteger outside long range silently became a different — often negative —
            // number. Measured: 123456789012345678901234567890 flattened to
            // -4362896299872285998. That is not a fidelity gap, it is a plausible-looking wrong
            // answer, and nothing downstream can detect it.
            //
            // longValueExact() throws instead; out-of-range values fall back to their exact
            // decimal text, which is lossless and round-trips. This matches the stricter
            // behaviour already implemented in converter/IntegerConverter, whose range checks
            // were the correct model all along.
            try {
                return ((BigInteger) value).longValueExact();
            } catch (ArithmeticException tooLargeForLong) {
                return value.toString();
            }
        }

        // Handle Double/Float special values
        if (value instanceof Double) {
            double d = (Double) value;
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                return Double.toString(d);
            }
        }
        if (value instanceof Float) {
            float f = (Float) value;
            if (Float.isNaN(f) || Float.isInfinite(f)) {
                return Float.toString(f);
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
                return key.replaceAll("([A-Z])", "_$1")
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
        private int maxArrayCells = DEFAULT_MAX_ARRAY_CELLS;
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
            if (depth < MIN_BOUND) {
                throw new IllegalArgumentException("maxDepth must be >= " + MIN_BOUND);
            }
            this.maxDepth = depth;
            return this;
        }

        public Builder maxArraySize(int size) {
            if (size < MIN_BOUND) {
                throw new IllegalArgumentException("maxArraySize must be >= " + MIN_BOUND);
            }
            this.maxArraySize = size;
            return this;
        }

        public Builder maxMapSize(int size) {
            if (size < MIN_BOUND) {
                throw new IllegalArgumentException("maxMapSize must be >= " + MIN_BOUND);
            }
            this.maxMapSize = size;
            return this;
        }

        /**
         * Ceiling on array-element cells emitted by one {@code flatten} call, across ALL arrays
         * in the document. Default {@value #DEFAULT_MAX_ARRAY_CELLS}.
         *
         * <p>A cell is one slot of one array-element column. A sparse array of maps emits
         * (union of distinct keys across elements) x (element count) cells, and no other bound
         * covers that product: {@code maxArraySize} caps the SLOT axis, {@code maxMapSize} caps
         * keys PER ELEMENT rather than their union, and {@code maxDepth} is all-or-nothing at
         * the array boundary with no breadth effect at all.</p>
         *
         * <p>Exceeding it throws {@link FlattenLimitExceededException} rather than truncating.
         * See that class for why truncation would be worse than the blow-up.</p>
         *
         * @param cells maximum cells per invocation, at least 1
         */
        public Builder maxArrayCells(int cells) {
            if (cells < MIN_BOUND) {
                throw new IllegalArgumentException("maxArrayCells must be >= " + MIN_BOUND);
            }
            this.maxArrayCells = cells;
            return this;
        }

        public Builder maxJsonStringLength(int length) {
            if (length < MIN_BOUND) {
                throw new IllegalArgumentException("maxJsonStringLength must be >= " + MIN_BOUND);
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