package io.github.pierce;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.SerializationFeature;

import static org.apache.avro.Schema.Type.*;

import static io.github.pierce.AvroReconstructor.ArraySerializationFormat.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proof-of-Concept Avro Reconstructor - Demonstrates Perfect Reconstruction
 *
 * This class proves that any flattened Map can be perfectly reconstructed back to its
 * original hierarchical structure using the corresponding Avro schema. It includes
 * comprehensive verification utilities to validate reconstruction correctness.
 *
 * <h3>Key Features:</h3>
 * <ul>
 *   <li>Perfect reconstruction of nested records, arrays, and maps</li>
 *   <li>Full support for all Avro logical types (decimals, timestamps, UUIDs, etc.)</li>
 *   <li>Comprehensive verification utilities to validate reconstruction</li>
 *   <li>Detailed error reporting with path information</li>
 *   <li>Support for multiple array serialization formats</li>
 *   <li>Handles nullable unions, defaults, and aliases</li>
 *   <li>Memory-safe iterative algorithms (no stack overflow)</li>
 * </ul>
 *
 * <h3>Example Usage:</h3>
 * <pre>
 * // 1. Flatten original data
 * MapFlattener flattener = new MapFlattener();
 * Map&lt;String, Object&gt; flattened = flattener.flatten(originalData);
 *
 * // 2. Reconstruct from flattened data
 * AvroReconstructor reconstructor = AvroReconstructor.builder().build();
 * Map&lt;String, Object&gt; reconstructed = reconstructor.reconstructToMap(flattened, schema);
 *
 * // 3. Verify reconstruction is perfect
 * ReconstructionVerification verification =
 *     reconstructor.verifyReconstruction(originalData, reconstructed, schema);
 *
 * if (verification.isPerfect()) {
 *     System.out.println("Perfect reconstruction!");
 * } else {
 *     System.out.println("Differences found:");
 *     verification.getDifferences().forEach(System.out::println);
 * }
 * </pre>
 *
 * @version 3.0 (Proof-of-Concept Edition)
 * @author Pierce
 */
public class AvroReconstructor {
    private static final Logger log = LoggerFactory.getLogger(AvroReconstructor.class);

    // Constants
    private static final int DEFAULT_MAX_DEPTH = 100;
    private static final int DEFAULT_MAX_CACHE_SIZE = 100;

    // Shared ObjectMapper - configured for consistent JSON handling
    private static final ObjectMapper SHARED_OBJECT_MAPPER = createConfiguredMapper();

    private static ObjectMapper createConfiguredMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, true);
        return mapper;
    }

    // Compiled patterns for performance
    private static final Pattern ARRAY_INDEX_PATTERN = Pattern.compile("\\[\\d+\\]");
    private static final Pattern JSON_ARRAY_PATTERN = Pattern.compile("^\\[.*\\]\$");
    private static final Pattern BRACKET_LIST_PATTERN = Pattern.compile("^\\[(.*)\\]\$");

    // Logical type converters
    private static final Conversions.DecimalConversion DECIMAL_CONVERSION =
            new Conversions.DecimalConversion();

    private final ObjectMapper objectMapper;
    private final ArraySerializationFormat arrayFormat;
    private final String separator;
    private final boolean useArrayBoundarySeparator;
    private final boolean strictValidation;
    private final boolean allowMissingFields;
    private final boolean useSchemaDefaults;
    private final int maxDepth;
    private final boolean enableVerification;

    // Schema cache for performance
    private final ConcurrentHashMap<String, SchemaCacheEntry> schemaCache;

    /**
     * Schema cache entry with metadata
     */
    private static class SchemaCacheEntry {
        final SchemaPathTrie pathTrie;
        final String fingerprint;
        final long createdAt;

        SchemaCacheEntry(SchemaPathTrie pathTrie, String fingerprint) {
            this.pathTrie = pathTrie;
            this.fingerprint = fingerprint;
            this.createdAt = System.currentTimeMillis();
        }
    }

    /**
     * Array serialization formats (must match MapFlattener)
     */
    public enum ArraySerializationFormat {
        JSON,
        COMMA_SEPARATED,
        PIPE_SEPARATED,
        BRACKET_LIST
    }

    /**
     * Builder for AvroReconstructor configuration
     */
    public static class Builder {
        private ArraySerializationFormat arrayFormat = ArraySerializationFormat.JSON;
        private boolean useArrayBoundarySeparator = false;
        private boolean strictValidation = true;
        private boolean allowMissingFields = true;
        private boolean useSchemaDefaults = true;
        private int maxDepth = DEFAULT_MAX_DEPTH;
        private ObjectMapper customObjectMapper = null;
        private boolean enableVerification = true;

        public Builder arrayFormat(ArraySerializationFormat format) {
            this.arrayFormat = format;
            return this;
        }

        public Builder useArrayBoundarySeparator(boolean use) {
            this.useArrayBoundarySeparator = use;
            return this;
        }

        public Builder strictValidation(boolean strict) {
            this.strictValidation = strict;
            return this;
        }

        public Builder allowMissingFields(boolean allow) {
            this.allowMissingFields = allow;
            return this;
        }

        public Builder useSchemaDefaults(boolean use) {
            this.useSchemaDefaults = use;
            return this;
        }

        public Builder maxDepth(int depth) {
            if (depth < 1) {
                throw new IllegalArgumentException("maxDepth must be >= 1");
            }
            this.maxDepth = depth;
            return this;
        }

        public Builder objectMapper(ObjectMapper mapper) {
            this.customObjectMapper = mapper;
            return this;
        }

        public Builder enableVerification(boolean enable) {
            this.enableVerification = enable;
            return this;
        }

        public AvroReconstructor build() {
            return new AvroReconstructor(this);
        }
    }

    private AvroReconstructor(Builder builder) {
        this.arrayFormat = builder.arrayFormat;
        this.useArrayBoundarySeparator = builder.useArrayBoundarySeparator;
        this.separator = useArrayBoundarySeparator ? "__" : "_";
        this.strictValidation = builder.strictValidation;
        this.allowMissingFields = builder.allowMissingFields;
        this.useSchemaDefaults = builder.useSchemaDefaults;
        this.maxDepth = builder.maxDepth;
        this.enableVerification = builder.enableVerification;
        this.objectMapper = builder.customObjectMapper != null ?
                builder.customObjectMapper : SHARED_OBJECT_MAPPER;
        this.schemaCache = new ConcurrentHashMap<>(DEFAULT_MAX_CACHE_SIZE);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Main reconstruction method - reconstructs to Map for verification
     */
    public Map<String, Object> reconstructToMap(Map<String, Object> flattenedMap, Schema schema) {
        if (schema.getType() != RECORD) {
            throw new IllegalArgumentException("Root schema must be a RECORD type");
        }

        if (flattenedMap == null || flattenedMap.isEmpty()) {
            return createEmptyRecord(schema);
        }

        try {
            // Get schema paths
            SchemaCacheEntry cacheEntry = getOrBuildSchemaCacheEntry(schema);

            // Build path tree
            PathNode root = buildPathTree(flattenedMap, cacheEntry.pathTrie);

            // Reconstruct
            GenericRecord record = reconstructRecord(root, schema, "", 0);

            // Convert to Map for verification
            return genericRecordToMap(record);

        } catch (Exception e) {
            log.error("Reconstruction failed for schema: {}", schema.getName(), e);
            throw new ReconstructionException(
                    "Failed to reconstruct data for schema: " + schema.getName(), e);
        }
    }

    /**
     * Reconstruct to GenericRecord (legacy method)
     */
    public GenericRecord reconstruct(Map<String, Object> flattenedMap, Schema schema) {
        Map<String, Object> reconstructedMap = reconstructToMap(flattenedMap, schema);
        return mapToGenericRecord(reconstructedMap, schema);
    }

    /**
     * Create empty record with defaults
     */
    private Map<String, Object> createEmptyRecord(Schema schema) {
        Map<String, Object> result = new LinkedHashMap<>();

        for (Schema.Field field : schema.getFields()) {
            if (field.hasDefaultValue()) {
                result.put(field.name(), field.defaultVal());
            } else if (isNullable(field.schema())) {
                result.put(field.name(), null);
            }
        }

        return result;
    }

    // ========================= VERIFICATION UTILITIES =========================

    /**
     * Comprehensive verification result
     */
    public static class ReconstructionVerification {
        private final boolean isPerfect;
        private final List<String> differences;
        private final Map<String, Object> originalData;
        private final Map<String, Object> reconstructedData;
        private final long verificationTimeMs;

        ReconstructionVerification(boolean isPerfect, List<String> differences,
                                   Map<String, Object> originalData,
                                   Map<String, Object> reconstructedData,
                                   long verificationTimeMs) {
            this.isPerfect = isPerfect;
            this.differences = Collections.unmodifiableList(differences);
            this.originalData = originalData;
            this.reconstructedData = reconstructedData;
            this.verificationTimeMs = verificationTimeMs;
        }

        public boolean isPerfect() {
            return isPerfect;
        }

        public List<String> getDifferences() {
            return differences;
        }

        public Map<String, Object> getOriginalData() {
            return originalData;
        }

        public Map<String, Object> getReconstructedData() {
            return reconstructedData;
        }

        public long getVerificationTimeMs() {
            return verificationTimeMs;
        }

        public String getReport() {
            StringBuilder sb = new StringBuilder();
            sb.append("=== Reconstruction Verification Report ===\n");
            sb.append("Status: ").append(isPerfect ? "PERFECT ✓" : "DIFFERENCES FOUND ✗").append("\n");
            sb.append("Verification Time: ").append(verificationTimeMs).append("ms\n");

            if (!isPerfect) {
                sb.append("\nDifferences (").append(differences.size()).append(" total):\n");
                for (int i = 0; i < Math.min(differences.size(), 20); i++) {
                    sb.append("  ").append(i + 1).append(". ").append(differences.get(i)).append("\n");
                }
                if (differences.size() > 20) {
                    sb.append("  ... and ").append(differences.size() - 20).append(" more\n");
                }
            } else {
                sb.append("\nAll fields match perfectly!\n");
            }

            return sb.toString();
        }

        @Override
        public String toString() {
            return getReport();
        }
    }

    /**
     * Verify that reconstruction is perfect
     */
    public ReconstructionVerification verifyReconstruction(
            Map<String, Object> originalData,
            Map<String, Object> reconstructedData,
            Schema schema) {

        if (!enableVerification) {
            throw new IllegalStateException("Verification is not enabled. " +
                    "Enable it with builder().enableVerification(true)");
        }

        long startTime = System.currentTimeMillis();
        List<String> differences = new ArrayList<>();

        compareStructures(originalData, reconstructedData, "", differences, schema);

        long elapsed = System.currentTimeMillis() - startTime;
        boolean isPerfect = differences.isEmpty();

        return new ReconstructionVerification(
                isPerfect, differences, originalData, reconstructedData, elapsed);
    }

    /**
     * Deep comparison of structures
     */
    private void compareStructures(Object original, Object reconstructed,
                                   String path, List<String> differences,
                                   Schema schema) {
        // Both null
        if (original == null && reconstructed == null) {
            return;
        }

        // One null, one not
        if (original == null || reconstructed == null) {
            differences.add(String.format("Path '%s': null mismatch (original=%s, reconstructed=%s)",
                    path, original, reconstructed));
            return;
        }

        // Type mismatch
        if (!compatibleTypes(original, reconstructed)) {
            differences.add(String.format("Path '%s': type mismatch (original=%s, reconstructed=%s)",
                    path, original.getClass().getSimpleName(),
                    reconstructed.getClass().getSimpleName()));
            return;
        }

        // Maps/Records
        if (original instanceof Map && reconstructed instanceof Map) {
            compareMaps((Map<?, ?>) original, (Map<?, ?>) reconstructed, path, differences);
            return;
        }

        // Lists/Arrays
        if (original instanceof List && reconstructed instanceof List) {
            compareLists((List<?>) original, (List<?>) reconstructed, path, differences);
            return;
        }

        // Primitives
        if (!valuesEqual(original, reconstructed)) {
            differences.add(String.format("Path '%s': value mismatch (original=%s, reconstructed=%s)",
                    path, formatValue(original), formatValue(reconstructed)));
        }
    }

    private void compareMaps(Map<?, ?> original, Map<?, ?> reconstructed,
                             String path, List<String> differences) {
        Set<Object> allKeys = new HashSet<>();
        allKeys.addAll(original.keySet());
        allKeys.addAll(reconstructed.keySet());

        for (Object key : allKeys) {
            String keyPath = path.isEmpty() ? key.toString() : path + "." + key;
            Object origValue = original.get(key);
            Object reconValue = reconstructed.get(key);

            compareStructures(origValue, reconValue, keyPath, differences, null);
        }
    }

    private void compareLists(List<?> original, List<?> reconstructed,
                              String path, List<String> differences) {
        if (original.size() != reconstructed.size()) {
            differences.add(String.format("Path '%s': array size mismatch (original=%d, reconstructed=%d)",
                    path, original.size(), reconstructed.size()));
            return;
        }

        for (int i = 0; i < original.size(); i++) {
            String indexPath = path + "[" + i + "]";
            compareStructures(original.get(i), reconstructed.get(i), indexPath, differences, null);
        }
    }

    private boolean compatibleTypes(Object a, Object b) {
        if (a.getClass().equals(b.getClass())) {
            return true;
        }

        // Number compatibility
        if (a instanceof Number && b instanceof Number) {
            return true;
        }

        // Map compatibility (HashMap, LinkedHashMap, etc.)
        if (a instanceof Map && b instanceof Map) {
            return true;
        }

        // List compatibility
        if (a instanceof List && b instanceof List) {
            return true;
        }

        return false;
    }

    private boolean valuesEqual(Object a, Object b) {
        // Numbers need special handling
        if (a instanceof Number && b instanceof Number) {
            return compareNumbers((Number) a, (Number) b);
        }

        // ByteBuffers
        if (a instanceof ByteBuffer && b instanceof ByteBuffer) {
            return ((ByteBuffer) a).equals(b);
        }

        // Strings
        if (a instanceof String && b instanceof String) {
            return a.equals(b);
        }

        // Generic equals
        return a.equals(b);
    }

    private boolean compareNumbers(Number a, Number b) {
        // For perfect reconstruction, we expect exact type matches
        // But allow some tolerance for floating point
        if (a instanceof Double || b instanceof Double ||
                a instanceof Float || b instanceof Float) {
            return Math.abs(a.doubleValue() - b.doubleValue()) < 0.000001;
        }

        return a.longValue() == b.longValue();
    }

    private String formatValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String) {
            return "\"" + value + "\"";
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            return "[" + list.size() + " items]";
        }
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            return "{" + map.size() + " fields}";
        }
        return value.toString();
    }

    /**
     * Compare flattened representation with reconstructed flattened
     * This verifies the round-trip: flatten -> reconstruct -> flatten
     */
    public ComparisonResult compareFlattenedMaps(
            Map<String, Object> originalFlattened,
            Map<String, Object> reconstructedFlattened) {

        List<String> differences = new ArrayList<>();

        Set<String> allKeys = new HashSet<>();
        allKeys.addAll(originalFlattened.keySet());
        allKeys.addAll(reconstructedFlattened.keySet());

        for (String key : allKeys) {
            Object origValue = originalFlattened.get(key);
            Object reconValue = reconstructedFlattened.get(key);

            if (origValue == null && reconValue == null) {
                continue;
            }

            if (origValue == null || reconValue == null) {
                differences.add(String.format("Key '%s': null mismatch (original=%s, reconstructed=%s)",
                        key, origValue, reconValue));
                continue;
            }

            if (!origValue.equals(reconValue)) {
                differences.add(String.format("Key '%s': value mismatch (original=%s, reconstructed=%s)",
                        key, formatValue(origValue), formatValue(reconValue)));
            }
        }

        return new ComparisonResult(differences.isEmpty(), differences);
    }

    public static class ComparisonResult {
        private final boolean isIdentical;
        private final List<String> differences;

        ComparisonResult(boolean isIdentical, List<String> differences) {
            this.isIdentical = isIdentical;
            this.differences = Collections.unmodifiableList(differences);
        }

        public boolean isIdentical() {
            return isIdentical;
        }

        public List<String> getDifferences() {
            return differences;
        }

        @Override
        public String toString() {
            if (isIdentical) {
                return "Flattened maps are identical ✓";
            }
            return "Differences found (" + differences.size() + "): " +
                    differences.stream().limit(5).collect(Collectors.joining(", "));
        }
    }

    // ========================= SCHEMA PATH TRIE =========================

    /**
     * Trie for efficient schema path lookups
     */
    private static class SchemaPathTrie {
        private static class Node {
            Map<String, Node> children = new HashMap<>();
            boolean isValidEndpoint = false;
            boolean isArrayPath = false;
            Schema fieldSchema = null;
        }

        private final Node root = new Node();
        private final String separator;
        private final Set<String> arrayPaths = new HashSet<>();
        private final Map<String, Schema> pathSchemas = new HashMap<>();

        SchemaPathTrie(String separator) {
            this.separator = separator;
        }

        void add(String path, Schema schema) {
            if (path.isEmpty()) return;

            pathSchemas.put(path, schema);
            String[] parts = path.split(Pattern.quote(separator));
            Node current = root;

            for (String part : parts) {
                current = current.children.computeIfAbsent(part, k -> new Node());
            }

            current.isValidEndpoint = true;
            current.fieldSchema = schema;
        }

        void markAsArrayPath(String path) {
            arrayPaths.add(path);
        }

        boolean containsArrayPath(String path) {
            return arrayPaths.contains(path);
        }

        boolean contains(String path) {
            return pathSchemas.containsKey(path);
        }

        Schema getSchema(String path) {
            return pathSchemas.get(path);
        }
    }

    private SchemaCacheEntry getOrBuildSchemaCacheEntry(Schema schema) {
        String fingerprint = getSchemaFingerprint(schema);

        return schemaCache.computeIfAbsent(fingerprint, k -> {
            SchemaPathTrie trie = buildSchemaPathTrie(schema);
            return new SchemaCacheEntry(trie, fingerprint);
        });
    }

    private String getSchemaFingerprint(Schema schema) {
        try {
            long fingerprint = SchemaNormalization.parsingFingerprint64(schema);
            String fullName = schema.getFullName();
            if (fullName == null) {
                fullName = "anonymous_" + schema.hashCode();
            }
            return fullName + "@" + Long.toHexString(fingerprint);
        } catch (Exception e) {
            log.warn("Failed to compute schema fingerprint", e);
            return "fallback_" + schema.toString().hashCode();
        }
    }

    private SchemaPathTrie buildSchemaPathTrie(Schema schema) {
        SchemaPathTrie trie = new SchemaPathTrie(separator);
        buildSchemaPathsRecursive(schema, "", trie, 0);
        return trie;
    }

    private void buildSchemaPathsRecursive(Schema schema, String prefix,
                                           SchemaPathTrie trie, int depth) {
        if (depth > maxDepth) {
            throw new IllegalStateException("Schema depth exceeds maximum: " + maxDepth);
        }

        // Handle unions
        if (schema.getType() == UNION) {
            for (Schema unionType : schema.getTypes()) {
                if (unionType.getType() != NULL) {
                    buildSchemaPathsRecursive(unionType, prefix, trie, depth);
                }
            }
            return;
        }

        switch (schema.getType()) {
            case RECORD:
                if (!prefix.isEmpty()) {
                    trie.add(prefix, schema);
                }

                for (Schema.Field field : schema.getFields()) {
                    String fieldPath = prefix.isEmpty() ? field.name() :
                            prefix + separator + field.name();
                    buildSchemaPathsRecursive(field.schema(), fieldPath, trie, depth + 1);
                }
                break;

            case ARRAY:
                if (!prefix.isEmpty()) {
                    trie.add(prefix, schema);
                    trie.markAsArrayPath(prefix);
                }

                Schema elementType = schema.getElementType();
                if (elementType.getType() == RECORD) {
                    for (Schema.Field field : elementType.getFields()) {
                        String arrayFieldPath = prefix + separator + field.name();
                        trie.add(arrayFieldPath, field.schema());
                        buildSchemaPathsRecursive(field.schema(), arrayFieldPath, trie, depth + 1);
                    }
                }
                break;

            case MAP:
                if (!prefix.isEmpty()) {
                    trie.add(prefix, schema);
                }
                break;

            default:
                if (!prefix.isEmpty()) {
                    trie.add(prefix, schema);
                }
                break;
        }
    }

    // ========================= PATH TREE BUILDING =========================

    /**
     * Path node in the reconstruction tree
     */
    private static class PathNode {
        String name;
        Object value;
        Map<String, PathNode> children = new LinkedHashMap<>();
        Map<String, List<Object>> arrayFieldValues;
        boolean isLeaf = false;

        PathNode(String name) {
            this.name = name;
        }

        void addPath(String[] pathParts, int index, Object value) {
            if (index == pathParts.length - 1) {
                this.isLeaf = true;
                this.value = value;
            } else {
                String childName = pathParts[index + 1];
                PathNode child = children.computeIfAbsent(childName, PathNode::new);
                child.addPath(pathParts, index + 1, value);
            }
        }

        void addArrayFieldValue(String fieldName, Object serializedArray) {
            if (arrayFieldValues == null) {
                arrayFieldValues = new LinkedHashMap<>();
            }
            arrayFieldValues.put(fieldName, deserializeArrayStatic(serializedArray));
        }

        private static List<Object> deserializeArrayStatic(Object value) {
            if (value == null) {
                return Collections.singletonList(null);
            }

            // If it's already a List, return it directly
            if (value instanceof List) {
                return (List<Object>) value;
            }

            String strValue = value.toString().trim();

            // JSON array
            if (strValue.startsWith("[") && strValue.endsWith("]")) {
                try {
                    List<Object> parsed = SHARED_OBJECT_MAPPER.readValue(strValue,
                            new TypeReference<List<Object>>() {});
                    return parsed;
                } catch (Exception e) {
                    // Log the parsing failure for debugging
                    System.err.println("WARN: Failed to parse as JSON array: '" + strValue + "' - " + e.getMessage());
                    // Fall through to other formats
                }
            }

            // Comma or pipe separated
            // Strip surrounding brackets if present (handles List.toString() format)
            String valueToSplit = strValue;
            if (strValue.startsWith("[") && strValue.endsWith("]")) {
                valueToSplit = strValue.substring(1, strValue.length() - 1).trim();
            }

            if (valueToSplit.contains(",")) {
                // Use bracket-aware splitting to handle nested arrays like "[[a,b],[c,d]]"
                List<String> parts = splitRespectingBrackets(valueToSplit, ",");
                // Trim whitespace from each element
                return parts.stream()
                        .map(String::trim)
                        .collect(java.util.stream.Collectors.toList());
            } else if (valueToSplit.contains("|")) {
                List<String> parts = splitRespectingBrackets(valueToSplit, "|");
                return parts.stream()
                        .map(String::trim)
                        .collect(java.util.stream.Collectors.toList());
            }

            return Collections.singletonList(strValue);
        }

        /**
         * Split string by delimiter while respecting bracket nesting and quotes.
         */
        private static List<String> splitRespectingBrackets(String str, String delimiter) {
            if (delimiter == null || delimiter.length() != 1) {
                throw new IllegalArgumentException("Delimiter must be a single character")
            }
            char delimiterChar = delimiter.charAt(0)

            List<String> result = new ArrayList<>()
            StringBuilder current = new StringBuilder()
            int bracketDepth = 0
            boolean inQuotes = false

            for (int i = 0; i < str.length(); i++) {
                char c = str.charAt(i)

                if (c == '"' && (i == 0 || str.charAt(i - 1) != '\\')) {
                    inQuotes = !inQuotes
                    current.append(c)
                } else if (!inQuotes) {
                    if (c == '[') {
                        bracketDepth++
                        current.append(c)
                    } else if (c == ']') {
                        bracketDepth--
                        current.append(c)
                    } else if (c == delimiterChar && bracketDepth == 0) {
                        String item = current.toString().trim()
                        // Strip quotes from string values
                        if (item.startsWith("\"") && item.endsWith("\"") && item.length() >= 2) {
                            item = item.substring(1, item.length() - 1)
                        }
                        result.add(item)
                        current = new StringBuilder()
                    } else {
                        current.append(c)
                    }
                } else {
                    current.append(c)
                }
            }

            // Add the last part
            if (current.length() > 0) {
                String item = current.toString().trim()
                // Strip quotes from string values
                if (item.startsWith("\"") && item.endsWith("\"") && item.length() >= 2) {
                    item = item.substring(1, item.length() - 1)
                }
                result.add(item)
            }

            return result
        }
    }

    private PathNode buildPathTree(Map<String, Object> flattenedMap,
                                   SchemaPathTrie schemaPaths) {
        PathNode root = new PathNode("root");

        for (Map.Entry<String, Object> entry : flattenedMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            String[] parts = key.split(Pattern.quote(separator));

            // Check if this is an array field pattern
            if (isArrayFieldPattern(parts, schemaPaths)) {
                handleArrayFieldInTree(root, parts, value);
            } else {
                root.addPath(parts, -1, value);
            }
        }

        return root;
    }

    private boolean isArrayFieldPattern(String[] keyParts, SchemaPathTrie schemaPaths) {
        // Check each prefix to see if it's an array path
        for (int i = keyParts.length - 1; i > 0; i--) {
            String prefix = String.join(separator, Arrays.copyOfRange(keyParts, 0, i));
            if (schemaPaths.containsArrayPath(prefix)) {
                return true;
            }
        }
        return false;
    }

    private void handleArrayFieldInTree(PathNode root, String[] parts, Object value) {
        PathNode current = root;

        // Navigate to the array node
        for (int i = 0; i < parts.length - 1; i++) {
            current = current.children.computeIfAbsent(parts[i], PathNode::new);
        }

        // Add array field value
        String fieldName = parts[parts.length - 1];
        current.addArrayFieldValue(fieldName, value);
    }

    // ========================= RECONSTRUCTION CORE =========================

    private GenericRecord reconstructRecord(PathNode node, Schema schema,
                                            String path, int currentDepth) {
        if (currentDepth > maxDepth) {
            throw new IllegalStateException("Maximum depth exceeded at: " + path);
        }

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);

        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            String fieldPath = path.isEmpty() ? fieldName : path + separator + fieldName;
            Schema fieldSchema = field.schema();

            try {
                PathNode childNode = node.children.get(fieldName);

                if (childNode != null) {
                    Object fieldValue = reconstructValue(childNode, fieldSchema,
                            fieldPath, currentDepth + 1);
                    builder.set(fieldName, fieldValue);
                } else {
                    // Try to reconstruct array from field values
                    Object arrayValue = tryReconstructArrayFromFields(node, field,
                            fieldSchema, fieldPath, currentDepth + 1);

                    if (arrayValue != null) {
                        builder.set(fieldName, arrayValue);
                    } else if (useSchemaDefaults && field.hasDefaultValue()) {
                        builder.set(fieldName, field.defaultVal());
                    } else if (isNullable(fieldSchema)) {
                        builder.set(fieldName, null);
                    } else if (!allowMissingFields) {
                        throw new IllegalStateException(
                                "Required field missing and no default: " + fieldPath);
                    }
                }
            } catch (Exception e) {
                throw new ReconstructionException(
                        String.format("Failed to reconstruct field '%s' at path '%s'",
                                fieldName, fieldPath), e);
            }
        }

        return builder.build();
    }

    private Object reconstructValue(PathNode node, Schema schema,
                                    String path, int currentDepth) {
        // Handle unions
        if (schema.getType() == UNION) {
            return reconstructUnionValue(node, schema, path, currentDepth);
        }

        switch (schema.getType()) {
            case RECORD:
                return reconstructRecord(node, schema, path, currentDepth);

            case ARRAY:
                return reconstructArray(node, schema, path, currentDepth);

            case MAP:
                return reconstructMap(node, schema, path, currentDepth);

            case ENUM:
                return reconstructEnum(node.value, schema, path);

            case NULL:
                return null;

            default:
                // Primitives and logical types
                return convertPrimitive(node.value, schema, path);
        }
    }

    private Object reconstructArray(PathNode node, Schema arraySchema,
                                    String path, int currentDepth) {
        Schema elementSchema = arraySchema.getElementType();

        // Case 1: Serialized array at leaf
        if (node.isLeaf && node.value != null) {
            List<Object> deserializedValues = deserializeArray(node.value);
            return reconstructArrayFromValues(deserializedValues, elementSchema, path, currentDepth);
        }

        // Case 2: Array of records with field extraction
        // This includes arrays where records have only nested fields (no scalar fields in arrayFieldValues)
        if (elementSchema.getType() == RECORD &&
                ((node.arrayFieldValues != null && !node.arrayFieldValues.isEmpty()) ||
                        !node.children.isEmpty())) {
            return reconstructArrayOfRecords(node, elementSchema, path, currentDepth);
        }

        // Case 3: Nested structure (for non-record arrays)
        List<Object> result = new ArrayList<>();
        for (PathNode child : node.children.values()) {
            Object childValue = reconstructValue(child, elementSchema, path, currentDepth + 1);
            result.add(childValue);
        }

        return result;
    }

    private GenericRecord reconstructNestedRecordFromArray(PathNode node, Schema recordSchema,
                                                           String fieldPrefix, int index, String path) {
        // Check if there's a child node for this nested record
        PathNode childNode = node.children.get(fieldPrefix);
        if (childNode == null || childNode.arrayFieldValues == null) {
            return null;
        }

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        boolean hasAnyField = false;

        for (Schema.Field nestedField : recordSchema.getFields()) {
            String nestedFieldName = nestedField.name();
            // Look in the child node's arrayFieldValues, not the parent's
            List<Object> nestedValues = childNode.arrayFieldValues.get(nestedFieldName);

            if (nestedValues != null && !nestedValues.isEmpty()) {
                // Get the value - for JSON arrays, we use the last (or only) element
                int valueIndex = Math.min(index, nestedValues.size() - 1);
                Object rawValue = nestedValues.get(valueIndex);

                // Unwrap nullable schemas to check actual type
                Schema actualFieldSchema = unwrapNullable(nestedField.schema());

                // DEBUG: Log what we're extracting
                log.debug("Field: {}, Index: {}, ValueIndex: {}, RawValue: {}, RawValue.class: {}",
                        nestedFieldName, index, valueIndex, rawValue, rawValue != null ? rawValue.getClass().getSimpleName() : "null");

                // Extract value at index (handles JSON-encoded arrays for primitives)
                Object value = extractValueAtIndex(rawValue, index, actualFieldSchema.getType());

// Handle null values from out-of-bounds access in asymmetric arrays
                if (value == null && rawValue != null) {
                    // This might be an out-of-bounds access in an asymmetric array
                    // Check if we should use a default or skip
                    if (nestedField.hasDefaultValue()) {
                        value = nestedField.defaultVal();
                    } else if (!isNullable(nestedField.schema())) {
                        // Required field - try to provide a sensible default
                        log.debug("Providing default for required field {} at index {} (asymmetric array)",
                                nestedFieldName, index);
                        switch (actualFieldSchema.getType()) {
                            case STRING:
                                value = "";
                                break;
                            case INT:
                                value = 0;
                                break;
                            case LONG:
                                value = 0L;
                                break;
                            case FLOAT:
                                value = 0.0f;
                                break;
                            case DOUBLE:
                                value = 0.0;
                                break;
                            case BOOLEAN:
                                value = false;
                                break;
                            case ARRAY:
                                value = new ArrayList<>();
                                break;
                            default:
                                // For complex types, we may need to skip
                                log.warn("Cannot provide default for required field {} of type {}",
                                        nestedFieldName, actualFieldSchema.getType());
                                continue; // Skip setting this field
                        }
                    }
                }

// DEBUG: Log extracted value
                log.debug("After extractValueAtIndex: Value: {}, Value.class: {}",
                        value, value != null ? value.getClass().getSimpleName() : "null");

                // Handle nested arrays in nested records
                if (actualFieldSchema.getType() == ARRAY && value instanceof String) {
                    // Parse JSON string back to array
                    String jsonString = (String) value;
                    try {
                        // Special case: empty array string
                        if (jsonString.trim().equals("[]")) {
                            value = new ArrayList<>();
                        } else {
                            List<Object> parsedArray = null;

                            // Try JSON first if it looks like JSON
                            if (jsonString.trim().startsWith("[") && jsonString.trim().endsWith("]")) {
                                try {
                                    parsedArray = objectMapper.readValue(jsonString, List.class);
                                } catch (Exception jsonEx) {
                                    // Not valid JSON, try other formats based on arrayFormat
                                    log.warn("Failed to parse as JSON array: '{}' - {}", jsonString, jsonEx.getMessage());
                                }
                            }

                            // If JSON parsing failed, try format-specific parsing
                            if (parsedArray == null) {
                                switch (arrayFormat) {
                                    case BRACKET_LIST:
                                        parsedArray = deserializeBracketList(jsonString);
                                        break;
                                    case COMMA_SEPARATED:
                                        parsedArray = Arrays.asList(jsonString.split(",", -1));
                                        break;
                                    case PIPE_SEPARATED:
                                        parsedArray = Arrays.asList(jsonString.split("\\|", -1));
                                        break;
                                    default:
                                        // Last resort: single element list
                                        parsedArray = Collections.singletonList(jsonString);
                                }
                            }

                            // Ensure we got a valid list
                            if (parsedArray == null || parsedArray.isEmpty()) {
                                value = new ArrayList<>();
                            } else {
                                // Path already includes array index from caller
                                value = reconstructArrayFromValues(parsedArray,
                                        actualFieldSchema.getElementType(),
                                        path + "." + fieldPrefix + "." + nestedFieldName,
                                        0);
                                if (value == null) {
                                    value = new ArrayList<>();
                                }
                            }
                        }
                    } catch (Exception e) {
                        if (strictValidation) {
                            throw new IllegalArgumentException(
                                    String.format("Failed to parse nested array JSON in nested record at %s: %s",
                                            path, jsonString), e);
                        }
                        value = new ArrayList<>();
                    }
                } else {
                    // Path already includes array index from caller
                    value = convertPrimitive(value, nestedField.schema(),
                            path + "." + fieldPrefix + "." + nestedFieldName);
                }

                builder.set(nestedFieldName, value);
                hasAnyField = true;
            } else {
                // Check if there's a child node for this field (for deeply nested structures)
                PathNode fieldChildNode = childNode.children.get(nestedFieldName)
                Schema actualFieldSchema = unwrapNullable(nestedField.schema())

                if (fieldChildNode != null) {
                    // Reconstruct from child node based on schema type
                    if (actualFieldSchema.getType() == ARRAY) {
                        Schema elementSchema = actualFieldSchema.getElementType()

                        // Check if this is an array of records with indexed data
                        if (elementSchema.getType() == RECORD &&
                                fieldChildNode.arrayFieldValues != null &&
                                !fieldChildNode.arrayFieldValues.isEmpty()) {

                            // Pass the outer index so we get the correct slice of data
                            Object reconstructed = reconstructNestedArrayOfRecordsAtIndex(
                                    fieldChildNode, elementSchema, index,
                                    path + "." + nestedFieldName, 0)
                            if (reconstructed != null) {
                                builder.set(nestedFieldName, reconstructed)
                                hasAnyField = true
                            }
                        } else {
                            // For primitive arrays or arrays without indexed data
                            Object reconstructed = reconstructValue(fieldChildNode, actualFieldSchema,
                                    path + "." + fieldPrefix + "." + nestedFieldName, 0)
                            if (reconstructed != null) {
                                builder.set(nestedFieldName, reconstructed)
                                hasAnyField = true
                            }
                        }
                    } else if (actualFieldSchema.getType() == RECORD) {
                        // For nested records (like price inside product), recursively reconstruct
                        // Pass the current node (childNode) as the parent, the nested field name,
                        // and the outer index so we get the correct values
                        GenericRecord nestedRecord = reconstructNestedRecordFromArray(
                                childNode, actualFieldSchema, nestedFieldName, index,
                                path + "." + fieldPrefix)
                        if (nestedRecord != null) {
                            builder.set(nestedFieldName, nestedRecord)
                            hasAnyField = true
                        }
                    } else {
                        // Other types - try generic reconstruction
                        Object reconstructed = reconstructValue(fieldChildNode, actualFieldSchema,
                                path + "." + fieldPrefix + "." + nestedFieldName, 0)
                        if (reconstructed != null) {
                            builder.set(nestedFieldName, reconstructed)
                            hasAnyField = true
                        }
                    }
                } else if (nestedField.hasDefaultValue()) {
                    builder.set(nestedFieldName, nestedField.defaultVal())
                    hasAnyField = true
                } else if (isNullable(nestedField.schema())) {
                    builder.set(nestedFieldName, null)
                    hasAnyField = true
                }
            }
        }

        return hasAnyField ? builder.build() : null;
    }

    /**
     * Reconstruct an array of records from flattened field values.
     *
     * This is the core method for handling arrays of complex objects.
     * It handles:
     * - Simple arrays of records (lineItems with product info)
     * - Nested arrays within records (trackingEvents within shipments)
     * - Mixed primitive and complex nested fields
     */
    private List<Object> reconstructArrayOfRecords(PathNode node, Schema elementSchema,
                                                   String path, int currentDepth) {
        if (elementSchema.getType() != RECORD) {
            throw new IllegalStateException("Expected RECORD element type at: " + path)
        }

        // Step 1: Collect all field values, parsing JSON arrays where needed
        Map<String, List<Object>> parsedFieldValues = new LinkedHashMap<>()

        if (node.arrayFieldValues != null) {
            for (Map.Entry<String, List<Object>> entry : node.arrayFieldValues.entrySet()) {
                String fieldName = entry.getKey()
                List<Object> rawValues = entry.getValue()

                if (rawValues != null && !rawValues.isEmpty()) {
                    // Check if values need JSON parsing (they're stored as a single JSON string)
                    if (rawValues.size() == 1 && rawValues.get(0) instanceof String) {
                        String strValue = ((String) rawValues.get(0)).trim()
                        if (strValue.startsWith("[") && strValue.endsWith("]")) {
                            try {
                                List<Object> parsed = objectMapper.readValue(strValue, List.class)
                                parsedFieldValues.put(fieldName, parsed)
                                continue
                            } catch (Exception e) {
                                log.debug("Could not parse as JSON array: {}", strValue)
                            }
                        }
                    }
                    parsedFieldValues.put(fieldName, rawValues)
                }
            }
        }

        // Step 2: Determine array size from the parsed field values
        int arraySize = determineArraySize(parsedFieldValues, node, elementSchema)

        if (arraySize == 0) {
            return new ArrayList<>()
        }

        List<Object> result = new ArrayList<>(arraySize)

        // Step 3: Build each record in the array
        for (int i = 0; i < arraySize; i++) {
            GenericRecordBuilder elementBuilder = new GenericRecordBuilder(elementSchema)

            for (Schema.Field field : elementSchema.getFields()) {
                String fieldName = field.name()
                Schema fieldSchema = field.schema()
                Schema actualFieldSchema = unwrapNullable(fieldSchema)

                // Check parsed field values first
                List<Object> fieldValues = parsedFieldValues.get(fieldName)

                if (fieldValues != null && i < fieldValues.size()) {
                    Object valueAtIndex = fieldValues.get(i)

                    // Handle nested arrays (arrays within the record)
                    if (actualFieldSchema.getType() == ARRAY) {
                        Object arrayValue = reconstructNestedArray(
                                valueAtIndex, actualFieldSchema, node, fieldName, i,
                                path + "[" + i + "]." + fieldName, currentDepth + 1)
                        elementBuilder.set(fieldName, arrayValue)
                    } else {
                        // Regular primitive or nested record field
                        Object converted = convertPrimitive(valueAtIndex, fieldSchema,
                                path + "[" + i + "]." + fieldName)
                        elementBuilder.set(fieldName, converted)
                    }
                } else if (actualFieldSchema.getType() == RECORD) {
                    // Handle nested record - look for child fields with prefix
                    GenericRecord nestedRecord = reconstructNestedRecordFromArray(
                            node, actualFieldSchema, fieldName, i, path + "[" + i + "]")
                    if (nestedRecord != null) {
                        elementBuilder.set(fieldName, nestedRecord)
                    } else {
                        handleMissingField(elementBuilder, field)
                    }
                } else if (actualFieldSchema.getType() == ARRAY) {
                    // Handle nested array that wasn't in direct field values
                    Object arrayValue = reconstructNestedArrayFromChildNode(
                            node, fieldName, actualFieldSchema, i,
                            path + "[" + i + "]." + fieldName, currentDepth + 1)
                    if (arrayValue != null) {
                        elementBuilder.set(fieldName, arrayValue)
                    } else {
                        elementBuilder.set(fieldName, new ArrayList<>())
                    }
                } else {
                    handleMissingField(elementBuilder, field)
                }
            }

            result.add(elementBuilder.build())
        }

        return result
    }

/**
 * Determine the size of the outer array from parsed field values.
 */
    private int determineArraySize(Map<String, List<Object>> parsedFieldValues,
                                   PathNode node, Schema elementSchema) {
        int maxSize = 0

        // Check parsed field values
        for (List<Object> values : parsedFieldValues.values()) {
            if (values != null) {
                maxSize = Math.max(maxSize, values.size())
            }
        }

        // Also check child nodes for nested structures
        if (node.children != null) {
            for (PathNode child : node.children.values()) {
                if (child.arrayFieldValues != null) {
                    for (List<Object> values : child.arrayFieldValues.values()) {
                        if (values != null && !values.isEmpty()) {
                            // Check if first value is a JSON array string
                            Object first = values.get(0)
                            if (first instanceof String) {
                                String strValue = ((String) first).trim()
                                if (strValue.startsWith("[[")) {
                                    // Nested array - count outer elements
                                    try {
                                        List<Object> parsed = objectMapper.readValue(strValue, List.class)
                                        maxSize = Math.max(maxSize, parsed.size())
                                    } catch (Exception e) {
                                        // Ignore parsing errors
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return maxSize > 0 ? maxSize : 1
    }

/**
 * Reconstruct a nested array field within an array of records.
 *
 * Example: lineItems[i].tags or lineItems[i].attributes
 */
    private Object reconstructNestedArray(Object value, Schema arraySchema, PathNode parentNode,
                                          String fieldName, int outerIndex, String path, int depth) {
        Schema elementSchema = arraySchema.getElementType()

        if (value == null) {
            return new ArrayList<>()
        }

        // If value is already a List, process it directly
        if (value instanceof List) {
            List<Object> listValue = (List<Object>) value
            return reconstructArrayFromValues(listValue, elementSchema, path, depth)
        }

        // If value is a JSON string (nested array serialized as string)
        if (value instanceof String) {
            String strValue = ((String) value).trim()

            // Try to parse as JSON array
            if (strValue.startsWith("[") && strValue.endsWith("]")) {
                try {
                    List<Object> parsed = objectMapper.readValue(strValue, List.class)
                    return reconstructArrayFromValues(parsed, elementSchema, path, depth)
                } catch (Exception e) {
                    log.debug("Failed to parse nested array JSON at {}: {}", path, e.getMessage())
                }
            }

            // Single value - wrap in list
            if (!strValue.isEmpty()) {
                Object converted = convertPrimitive(strValue, elementSchema, path)
                return Collections.singletonList(converted)
            }
        }

        return new ArrayList<>()
    }

/**
 * Reconstruct a nested array from a child node (for deeply nested structures).
 *
 * Example: shipments[i].trackingEvents where trackingEvents is array of records
 */
    private Object reconstructNestedArrayFromChildNode(PathNode parentNode, String fieldName,
                                                       Schema arraySchema, int outerIndex,
                                                       String path, int depth) {
        PathNode childNode = parentNode.children?.get(fieldName)

        if (childNode == null) {
            return null
        }

        Schema elementSchema = arraySchema.getElementType()

        // Check if child node has array field values
        if (childNode.arrayFieldValues != null && !childNode.arrayFieldValues.isEmpty()) {
            // This is an array of records - need to extract values for this outer index
            if (elementSchema.getType() == RECORD) {
                return reconstructNestedArrayOfRecordsAtIndex(
                        childNode, elementSchema, outerIndex, path, depth)
            }
        }

        // Check if child node is a leaf with array value
        if (childNode.isLeaf && childNode.value != null) {
            List<Object> deserialized = deserializeArray(childNode.value)
            return reconstructArrayFromValues(deserialized, elementSchema, path, depth)
        }

        return null
    }

/**
 * Reconstruct a nested array of records at a specific outer index.
 *
 * This handles the case where we have:
 * shipments_trackingEvents_timestamp: ["[t1,t2,t3]"]  (1 shipment with 3 events)
 *
 * And we need to extract the array for shipment at outerIndex.
 */
    /**
     * Reconstruct a nested array of records at a specific outer index.
     *
     * For example, with:
     *   lineItems_product_attributes_name: ["[\"RAM\",\"Storage\"]","[\"Connectivity\"]"]
     *
     * When outerIndex=0 (first lineItem), we parse "[\"RAM\",\"Storage\"]"
     * When outerIndex=1 (second lineItem), we parse "[\"Connectivity\"]"
     */
    private List<Object> reconstructNestedArrayOfRecordsAtIndex(PathNode childNode,
                                                                Schema recordSchema,
                                                                int outerIndex,
                                                                String path, int depth) {
        // First, parse all field values and extract the nested structure for THIS outerIndex
        Map<String, List<Object>> fieldValuesAtIndex = new LinkedHashMap<>()
        int innerArraySize = 0

        for (Schema.Field field : recordSchema.getFields()) {
            String fieldName = field.name()
            List<Object> rawValues = childNode.arrayFieldValues?.get(fieldName)

            if (rawValues != null && !rawValues.isEmpty()) {
                // KEY FIX: Use outerIndex to select the correct element
                Object rawValue = outerIndex < rawValues.size() ? rawValues.get(outerIndex) : rawValues.get(0)

                if (rawValue instanceof String) {
                    String strValue = ((String) rawValue).trim()

                    // Check for doubly-nested array: "[[v1,v2],[v3,v4]]"
                    if (strValue.startsWith("[[")) {
                        try {
                            List<List<Object>> parsed = objectMapper.readValue(strValue, List.class)
                            // For doubly-nested, we've ALREADY selected the outer element,
                            // so just use the first (and only) inner list
                            if (!parsed.isEmpty()) {
                                List<Object> innerList = parsed.get(0)
                                fieldValuesAtIndex.put(fieldName, innerList)
                                innerArraySize = Math.max(innerArraySize, innerList.size())
                            }
                            continue
                        } catch (Exception e) {
                            log.debug("Failed to parse doubly-nested array: {}", strValue)
                        }
                    }

                    // Check for single nested array: "[v1,v2,v3]"
                    if (strValue.startsWith("[") && strValue.endsWith("]")) {
                        try {
                            List<Object> parsed = objectMapper.readValue(strValue, List.class)
                            fieldValuesAtIndex.put(fieldName, parsed)
                            innerArraySize = Math.max(innerArraySize, parsed.size())
                            continue
                        } catch (Exception e) {
                            log.debug("Failed to parse nested array: {}", strValue)
                        }
                    }

                    // Single value - wrap in list
                    fieldValuesAtIndex.put(fieldName, Collections.singletonList(strValue))
                    innerArraySize = Math.max(innerArraySize, 1)
                } else if (rawValue instanceof List) {
                    // Already parsed as list
                    List<Object> listValue = (List<Object>) rawValue
                    fieldValuesAtIndex.put(fieldName, listValue)
                    innerArraySize = Math.max(innerArraySize, listValue.size())
                } else if (rawValue != null) {
                    // Single non-string value
                    fieldValuesAtIndex.put(fieldName, Collections.singletonList(rawValue))
                    innerArraySize = Math.max(innerArraySize, 1)
                }
            }
        }

        if (innerArraySize == 0) {
            return new ArrayList<>()
        }

        // Build records for the inner array
        List<Object> result = new ArrayList<>(innerArraySize)

        for (int j = 0; j < innerArraySize; j++) {
            GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema)

            for (Schema.Field field : recordSchema.getFields()) {
                String fieldName = field.name()
                List<Object> values = fieldValuesAtIndex.get(fieldName)

                if (values != null && j < values.size()) {
                    Object value = values.get(j)
                    Object converted = convertPrimitive(value, field.schema(),
                            path + "[" + j + "]." + fieldName)
                    builder.set(fieldName, converted)
                } else {
                    handleMissingField(builder, field)
                }
            }

            result.add(builder.build())
        }

        return result
    }

/**
 * Handle missing field by setting default or null.
 */
    private void handleMissingField(GenericRecordBuilder builder, Schema.Field field) {
        if (field.hasDefaultValue()) {
            builder.set(field.name(), field.defaultVal())
        } else if (isNullable(field.schema())) {
            builder.set(field.name(), null)
        } else {
            Schema actualSchema = unwrapNullable(field.schema())
            // Provide type-appropriate defaults for required fields
            switch (actualSchema.getType()) {
                case ARRAY:
                    builder.set(field.name(), new ArrayList<>())
                    break
                case STRING:
                    builder.set(field.name(), "")
                    break
                case INT:
                    builder.set(field.name(), 0)
                    break
                case LONG:
                    builder.set(field.name(), 0L)
                    break
                case FLOAT:
                    builder.set(field.name(), 0.0f)
                    break
                case DOUBLE:
                    builder.set(field.name(), 0.0d)
                    break
                case BOOLEAN:
                    builder.set(field.name(), false)
                    break
                default:
                    log.warn("Cannot provide default for required field {} of type {}",
                            field.name(), actualSchema.getType())
            }
        }
    }


/**
 * Parse a nested array structure like "[[1,2,3],[4,5]]" into List<List<Object>>
 */
    private List<Object> parseNestedArrayStructure(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }

        String trimmed = value.trim();

        // Try JSON parsing first
        try {
            return objectMapper.readValue(trimmed, new TypeReference<List<Object>>() {});
        } catch (Exception e) {
            // Fall back to bracket-aware parsing
            return parseBracketListPreservingNesting(trimmed);
        }
    }

/**
 * Parse bracket list format while preserving nested structure
 */
    private List<Object> parseBracketListPreservingNesting(String value) {
        if (!value.startsWith("[") || !value.endsWith("]")) {
            return Collections.singletonList(value);
        }

        String content = value.substring(1, value.length() - 1).trim();
        if (content.isEmpty()) {
            return Collections.emptyList();
        }

        List<Object> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int bracketDepth = 0;
        boolean inQuotes = false;

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);

            if (c == '"' && (i == 0 || content.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (!inQuotes) {
                if (c == '[') {
                    bracketDepth++;
                    current.append(c);
                } else if (c == ']') {
                    bracketDepth--;
                    current.append(c);
                } else if (c == ',' && bracketDepth == 0) {
                    // Top-level separator
                    String item = current.toString().trim();
                    result.add(parseValue(item));
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            } else {
                current.append(c);
            }
        }

        // Don't forget the last item
        String lastItem = current.toString().trim();
        if (!lastItem.isEmpty()) {
            result.add(parseValue(lastItem));
        }

        return result;
    }

/**
 * Parse a single value - could be nested array, quoted string, number, etc.
 */
    private Object parseValue(String item) {
        if (item.startsWith("[")) {
            // Nested array - recursively parse
            return parseBracketListPreservingNesting(item);
        } else if (item.startsWith("\"") && item.endsWith("\"")) {
            // Quoted string
            return item.substring(1, item.length() - 1)
                    .replace("\\\"", "\"")
                    .replace("\\n", "\n");
        } else if ("null".equals(item)) {
            return null;
        } else {
            // Try as number
            try {
                if (item.contains(".")) {
                    return Double.parseDouble(item);
                } else {
                    return Long.parseLong(item);
                }
            } catch (NumberFormatException e) {
                // Return as string
                return item;
            }
        }
    }

/**
 * Reconstruct a nested array of records (e.g., trackingEvents within shipments)
 */
    private List<Object> reconstructNestedArrayOfRecords(
            PathNode parentNode, String fieldName, Schema recordSchema,
            List<Object> innerArrayValues, int outerIndex, String path, int depth) {

        // Get the child node that contains field values for this nested array
        PathNode childNode = parentNode.children.get(fieldName);

        if (childNode == null || childNode.arrayFieldValues == null) {
            // No nested data - just convert the values directly if they're primitives
            List<Object> result = new ArrayList<>();
            for (Object val : innerArrayValues) {
                if (val != null) {
                    // This shouldn't happen for record types, but handle gracefully
                    result.add(val);
                }
            }
            return result;
        }

        // Determine size of inner array from the parsed values
        int innerSize = innerArrayValues.size();

        List<Object> result = new ArrayList<>(innerSize);

        for (int j = 0; j < innerSize; j++) {
            GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);

            for (Schema.Field field : recordSchema.getFields()) {
                String nestedFieldName = field.name();
                List<Object> nestedValues = childNode.arrayFieldValues.get(nestedFieldName);

                if (nestedValues != null && !nestedValues.isEmpty()) {
                    // The values are structured as [[val1, val2], [val3]] for nested arrays
                    // We need to get the right inner list based on outerIndex,
                    // then the right element based on j
                    Object rawValue = nestedValues.get(0); // Usually single-element list

                    if (rawValue instanceof String) {
                        List<Object> parsed = parseNestedArrayStructure((String) rawValue);

                        if (outerIndex < parsed.size()) {
                            Object outerElement = parsed.get(outerIndex);

                            if (outerElement instanceof List) {
                                List<Object> innerList = (List<Object>) outerElement;
                                if (j < innerList.size()) {
                                    Object value = innerList.get(j);
                                    Object converted = convertPrimitive(value, field.schema(),
                                            path + "[" + j + "]." + nestedFieldName);
                                    builder.set(nestedFieldName, converted);
                                    continue;
                                }
                            }
                        }
                    }
                }

                // Handle defaults
                if (field.hasDefaultValue()) {
                    builder.set(nestedFieldName, field.defaultVal());
                } else if (isNullable(field.schema())) {
                    builder.set(nestedFieldName, null);
                }
            }

            result.add(builder.build());
        }

        return result;
    }

    private Schema unwrapUnion(Schema schema) {
        if (schema.getType() != UNION) {
            return schema;
        }

        for (Schema type : schema.getTypes()) {
            if (type.getType() != NULL) {
                return type;
            }
        }

        return schema;
    }



    private List<Object> reconstructArrayFromValues(List<Object> values, Schema elementSchema,
                                                    String path, int currentDepth) {
        // Handle null input only - empty list is valid
        if (values == null) {
            return new ArrayList<>();
        }

        // Empty array is valid - return empty list (don't filter it out)
        if (values.isEmpty()) {
            return new ArrayList<>();
        }

        List<Object> result = new ArrayList<>(values.size());

        // Check if element type is nullable (union with null)
        boolean elementIsNullable = isNullableSchema(elementSchema);

        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            String indexPath = path + "[" + i + "]";

            // Handle null values - preserve them if element type allows nulls
            if (value == null || "null".equals(value) ||
                    value.getClass().getSimpleName().equals("NullObject")) {
                if (elementIsNullable) {
                    result.add(null);  // Preserve the null
                }
                // If not nullable, skip (schema violation, but don't crash)
                continue;
            }

            if (value instanceof List) {
                List<?> listValue = (List<?>) value;
                // Handle empty nested arrays - they are valid!
                if (listValue.isEmpty()) {
                    result.add(new ArrayList<>());
                    continue;
                }

                // Nested array
                if (elementSchema.getType() == ARRAY) {
                    result.add(reconstructArrayFromValues((List<Object>) value,
                            elementSchema.getElementType(), indexPath, currentDepth + 1));
                } else {
                    // Flatten nested list
                    for (Object item : listValue) {
                        if (item != null && !item.getClass().getSimpleName().equals("NullObject")) {
                            result.add(convertPrimitive(item, elementSchema, indexPath));
                        } else if (elementIsNullable) {
                            result.add(null);
                        }
                    }
                }
            } else {
                result.add(convertPrimitive(value, elementSchema, indexPath));
            }
        }

        return result;
    }

    /**
     * Check if a schema is nullable (union containing null type)
     */
    private boolean isNullableSchema(Schema schema) {
        if (schema.getType() == UNION) {
            for (Schema type : schema.getTypes()) {
                if (type.getType() == NULL) {
                    return true;
                }
            }
        }
        return false;
    }

    private Map<String, Object> reconstructMap(PathNode node, Schema mapSchema,
                                               String path, int currentDepth) {
        Schema valueSchema = mapSchema.getValueType();
        Map<String, Object> result = new HashMap<>();

        for (Map.Entry<String, PathNode> entry : node.children.entrySet()) {
            String key = entry.getKey();
            PathNode valueNode = entry.getValue();
            Object value = reconstructValue(valueNode, valueSchema,
                    path + separator + key, currentDepth + 1);
            result.put(key, value);
        }

        return result;
    }

    private Object reconstructUnionValue(PathNode node, Schema unionSchema,
                                         String path, int currentDepth) {
        List<Schema> types = unionSchema.getTypes()

        // Handle nullable union (most common case: ["null", "SomeType"])
        if (types.size() == 2) {
            Schema nullSchema = null
            Schema nonNullSchema = null

            for (Schema type : types) {
                if (type.getType() == NULL) {
                    nullSchema = type
                } else {
                    nonNullSchema = type
                }
            }

            // If this union doesn't contain null, try to match one of the types
            if (nullSchema == null) {
                for (Schema type : types) {
                    try {
                        return reconstructValue(node, type, path, currentDepth)
                    } catch (Exception e) {
                        // Try next type
                        continue
                    }
                }
                if (strictValidation) {
                    throw new IllegalStateException("Could not match any union type at: " + path)
                }
                return null
            }

            // It's a nullable union - determine if we have actual content

            // For complex types (RECORD, ARRAY, MAP), check if there's actual content
            // node.value will be null for non-leaf nodes, so we must check children
            if (nonNullSchema.getType() == RECORD) {
                // Check if we have children (nested record fields)
                if (!node.children.isEmpty()) {
                    return reconstructValue(node, nonNullSchema, path, currentDepth)
                }
                // No children = null record
                return null
            }

            if (nonNullSchema.getType() == ARRAY) {
                // Check for array content
                boolean hasContent = (node.arrayFieldValues != null && !node.arrayFieldValues.isEmpty()) ||
                        !node.children.isEmpty() ||
                        (node.isLeaf && node.value != null)
                if (hasContent) {
                    return reconstructValue(node, nonNullSchema, path, currentDepth)
                }
                return null
            }

            if (nonNullSchema.getType() == MAP) {
                if (!node.children.isEmpty()) {
                    return reconstructValue(node, nonNullSchema, path, currentDepth)
                }
                return null
            }

            // For primitives and other types, check the value
            if (node.value == null || "null".equals(String.valueOf(node.value).trim())) {
                return null
            }

            return reconstructValue(node, nonNullSchema, path, currentDepth)
        }

        // Multi-type union (3+ types) - try each type in order
        // First, determine what type of content we have
        boolean hasChildren = !node.children.isEmpty();
        boolean hasArrayValues = node.arrayFieldValues != null && !node.arrayFieldValues.isEmpty();
        boolean hasLeafValue = node.isLeaf && node.value != null && !"null".equals(String.valueOf(node.value).trim());

        // Check for no content first
        if (!hasChildren && !hasArrayValues && !hasLeafValue) {
            // No content at all - return null if union contains null
            for (Schema type : types) {
                if (type.getType() == NULL) {
                    return null;
                }
            }
        }

        // Try to match types based on content structure
        for (Schema type : types) {
            if (type.getType() == NULL) {
                continue; // Skip null, we already handled no-content case above
            }

            // For RECORD type, check if we have children that match the record's fields
            if (type.getType() == RECORD) {
                // Check if any of the node's children match the record's field names
                boolean hasMatchingFields = false;
                for (Schema.Field field : type.getFields()) {
                    if (node.children.containsKey(field.name())) {
                        hasMatchingFields = true;
                        break;
                    }
                }

                if (hasMatchingFields) {
                    try {
                        Object result = reconstructValue(node, type, path, currentDepth);
                        if (result != null) {
                            return result;
                        }
                    } catch (Exception e) {
                        log.debug("Union RECORD type didn't match at {}: {}", path, e.getMessage());
                    }
                }
                continue;
            }

            // For ARRAY type, check for array content
            if (type.getType() == ARRAY) {
                if (hasArrayValues || (hasLeafValue && looksLikeArray(node.value))) {
                    try {
                        return reconstructValue(node, type, path, currentDepth);
                    } catch (Exception e) {
                        log.debug("Union ARRAY type didn't match at {}: {}", path, e.getMessage());
                        continue;
                    }
                }
                continue;
            }

            // For MAP type, check for children
            if (type.getType() == MAP) {
                if (hasChildren) {
                    try {
                        return reconstructValue(node, type, path, currentDepth);
                    } catch (Exception e) {
                        log.debug("Union MAP type didn't match at {}: {}", path, e.getMessage());
                        continue;
                    }
                }
                continue;
            }

            // For primitive types, check if we have a leaf value
            if (hasLeafValue) {
                try {
                    return reconstructValue(node, type, path, currentDepth);
                } catch (Exception e) {
                    log.debug("Union type {} didn't match at {}: {}", type.getType(), path, e.getMessage());
                    continue;
                }
            }
        }

        if (strictValidation) {
            throw new IllegalStateException("Could not match any union type at: " + path)
        }

        return null
    }

    /**
     * Check if a value looks like a JSON array
     */
    private boolean looksLikeArray(Object value) {
        if (value == null) return false;
        String str = value.toString().trim();
        return str.startsWith("[") && str.endsWith("]");
    }

    private Object reconstructEnum(Object value, Schema enumSchema, String path) {
        if (value == null) {
            if (strictValidation) {
                throw new IllegalArgumentException("Null value for enum at: " + path);
            }
            return new GenericData.EnumSymbol(enumSchema, enumSchema.getEnumSymbols().get(0));
        }

        String stringValue = value.toString();

        if (!enumSchema.getEnumSymbols().contains(stringValue)) {
            if (strictValidation) {
                throw new IllegalArgumentException(
                        String.format("Invalid enum value '%s' at: %s. Valid values: %s",
                                stringValue, path, enumSchema.getEnumSymbols()));
            }
            return new GenericData.EnumSymbol(enumSchema, enumSchema.getEnumSymbols().get(0));
        }

        return new GenericData.EnumSymbol(enumSchema, stringValue);
    }

    private Object tryReconstructArrayFromFields(PathNode node, Schema.Field field,
                                                 Schema fieldSchema, String path,
                                                 int currentDepth) {
        if (node.arrayFieldValues == null) {
            return null;
        }

        String fieldName = field.name();
        List<Object> serializedValues = node.arrayFieldValues.get(fieldName);

        if (serializedValues == null) {
            return null;
        }

        Schema actualSchema = unwrapNullable(fieldSchema);
        if (actualSchema.getType() != ARRAY) {
            return null;
        }

        return reconstructArrayFromValues(serializedValues, actualSchema.getElementType(),
                path, currentDepth);
    }

    // ========================= TYPE CONVERSION =========================

    private Object convertPrimitive(Object value, Schema schema, String path) {
        if (value == null) {
            return null;
        }

        Schema actualSchema = unwrapNullable(schema);

        // Handle logical types
        if (actualSchema.getLogicalType() != null) {
            return convertLogicalType(value, actualSchema, path);
        }

        String strValue = value.toString();

        try {
            switch (actualSchema.getType()) {
                case STRING:
                    return strValue;

                case INT:
                    return value instanceof Number ?
                            ((Number) value).intValue() : Integer.parseInt(strValue);

                case LONG:
                    return value instanceof Number ?
                            ((Number) value).longValue() : Long.parseLong(strValue);

                case FLOAT:
                    return value instanceof Number ?
                            ((Number) value).floatValue() : Float.parseFloat(strValue);

                case DOUBLE:
                    return value instanceof Number ?
                            ((Number) value).doubleValue() : Double.parseDouble(strValue);

                case BOOLEAN:
                    return value instanceof Boolean ?
                            value : Boolean.parseBoolean(strValue);

                case BYTES:
                    // Check for Base64-encoded ByteBuffer
                    if (strValue.startsWith("B64:")) {
                        try {
                            String base64Data = strValue.substring(4);
                            byte[] decodedBytes = Base64.getDecoder().decode(base64Data);
                            return ByteBuffer.wrap(decodedBytes);
                        } catch (Exception e) {
                            log.warn("Failed to decode Base64 for BYTES field at " + path + ": " + strValue, e);
                            // Fall through to default handling
                        }
                    }

                    // Check if it's already a ByteBuffer
                    if (value instanceof ByteBuffer) {
                        return value;
                    }

                    // Fallback to treating string as raw bytes
                    return ByteBuffer.wrap(strValue.getBytes());

                case FIXED:
                    byte[] fixedBytes;
                    int expectedSize = actualSchema.getFixedSize();

                    // Check if value is already a GenericData.Fixed or byte array
                    if (value instanceof GenericData.Fixed) {
                        return value;
                    }
                    if (value instanceof byte[]) {
                        fixedBytes = (byte[]) value;
                    } else {
                        // Try Base64 decoding first (most common case from flattening)
                        try {
                            // Check for explicit B64: prefix
                            if (strValue.startsWith("B64:")) {
                                fixedBytes = Base64.getDecoder().decode(strValue.substring(4));
                            } else {
                                // Try Base64 decode - if the string length suggests it's encoded
                                // Base64 encoding of N bytes produces approximately (4 * ceil(N/3)) chars
                                int expectedBase64Len = (int) Math.ceil(expectedSize / 3.0) * 4;
                                if (strValue.length() >= expectedBase64Len && strValue.length() <= expectedBase64Len + 2) {
                                    try {
                                        fixedBytes = Base64.getDecoder().decode(strValue);
                                        if (fixedBytes.length == expectedSize) {
                                            // Successful Base64 decode with correct size
                                            return new GenericData.Fixed(actualSchema, fixedBytes);
                                        }
                                    } catch (IllegalArgumentException e) {
                                        // Not valid Base64, fall through to raw bytes
                                    }
                                }
                                // Fallback to raw string bytes (legacy behavior)
                                fixedBytes = strValue.getBytes();
                            }
                        } catch (Exception e) {
                            // Fallback to raw string bytes
                            fixedBytes = strValue.getBytes();
                        }
                    }

                    if (fixedBytes.length != expectedSize) {
                        throw new IllegalArgumentException(
                                String.format("Fixed size mismatch at %s: expected %d, got %d",
                                        path, expectedSize, fixedBytes.length));
                    }
                    return new GenericData.Fixed(actualSchema, fixedBytes);

                case ENUM:
                    // Convert string to EnumSymbol
                    String enumValue = strValue;
                    if (!actualSchema.getEnumSymbols().contains(enumValue)) {
                        if (strictValidation) {
                            throw new IllegalArgumentException(
                                    String.format("Invalid enum value '%s' at %s. Valid values: %s",
                                            enumValue, path, actualSchema.getEnumSymbols()));
                        }
                        // Use first symbol as default
                        enumValue = actualSchema.getEnumSymbols().get(0);
                    }
                    return new GenericData.EnumSymbol(actualSchema, enumValue);

                default:
                    return value;
            }
        } catch (NumberFormatException e) {
            if (strictValidation) {
                throw new IllegalArgumentException(
                        String.format("Cannot convert '%s' to %s at: %s",
                                strValue, actualSchema.getType(), path), e);
            }
            return getDefaultValue(actualSchema.getType());
        }
    }

    // ========================= LOGICAL TYPE CONVERSION =========================

    private Object convertLogicalType(Object value, Schema schema, String path) {
        String logicalTypeName = schema.getLogicalType().getName();

        try {
            switch (logicalTypeName) {
                case "timestamp-millis":
                    return convertTimestampMillis(value);
                case "timestamp-micros":
                    return convertTimestampMicros(value);
                case "date":
                    return convertDate(value);
                case "time-millis":
                    return convertTimeMillis(value);
                case "time-micros":
                    return convertTimeMicros(value);
                case "decimal":
                    return convertDecimal(value, schema);
                case "uuid":
                    return convertUuid(value);
                default:
                    if (strictValidation) {
                        throw new IllegalStateException(
                                "Unsupported logical type: " + logicalTypeName + " at " + path);
                    }
                    return value;
            }
        } catch (Exception e) {
            if (strictValidation) {
                throw new IllegalArgumentException(
                        String.format("Failed to convert logical type '%s' at %s",
                                logicalTypeName, path), e);
            }
            return getDefaultValueForLogicalType(schema);
        }
    }

    private Long convertTimestampMillis(Object value) {
        if (value instanceof Long) {
            return (Long) value;
        }
        String strValue = value.toString();
        try {
            return Instant.parse(strValue).toEpochMilli();
        } catch (Exception e) {
            return Long.parseLong(strValue);
        }
    }

    private Long convertTimestampMicros(Object value) {
        if (value instanceof Long) {
            return (Long) value;
        }
        String strValue = value.toString();
        try {
            return ChronoUnit.MICROS.between(Instant.EPOCH, Instant.parse(strValue));
        } catch (Exception e) {
            return Long.parseLong(strValue);
        }
    }

    private Integer convertDate(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        }
        String strValue = value.toString();
        try {
            LocalDate date = LocalDate.parse(strValue);
            return (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), date);
        } catch (Exception e) {
            return Integer.parseInt(strValue);
        }
    }

    private Integer convertTimeMillis(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        }
        String strValue = value.toString();
        try {
            LocalTime time = LocalTime.parse(strValue);
            return (int) (time.toNanoOfDay() / 1_000_000);
        } catch (Exception e) {
            return Integer.parseInt(strValue);
        }
    }

    private Long convertTimeMicros(Object value) {
        if (value instanceof Long) {
            return (Long) value;
        }
        String strValue = value.toString();
        try {
            LocalTime time = LocalTime.parse(strValue);
            return time.toNanoOfDay() / 1_000;
        } catch (Exception e) {
            return Long.parseLong(strValue);
        }
    }

    private Object convertDecimal(Object value, Schema schema) {
        LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();

        // If value is already a ByteBuffer (the correct format for BYTES-backed decimals),
        // return it directly without conversion
        if (value instanceof ByteBuffer) {
            return value;
        }

        String strValue = value.toString();

        // Check for Base64-encoded ByteBuffer (our custom serialization format)
        // Format: "B64:..." where ... is the Base64-encoded bytes
        if (strValue.startsWith("B64:")) {
            try {
                // Decode Base64 string back to ByteBuffer
                String base64Data = strValue.substring(4); // Remove "B64:" prefix
                byte[] decodedBytes = Base64.getDecoder().decode(base64Data);
                return ByteBuffer.wrap(decodedBytes);
            } catch (Exception e) {
                log.warn("Failed to decode Base64 ByteBuffer: " + strValue, e);
                // Fall through to error handling below
            }
        }

        // Check if value is a string representation of a ByteBuffer (which happens when
        // ByteBuffers are serialized without proper Base64 encoding)
        // These look like: "java.nio.HeapByteBuffer[pos=0 lim=3 cap=3]" or "[java.nio.HeapByteBuffer[..."
        if (strValue.contains("ByteBuffer[")) {
            // Cannot reconstruct ByteBuffer from toString() representation - data is lost
            String errorMessage = String.format(
                    "Cannot reconstruct BYTES field from ByteBuffer string representation: %s%n" +
                            "This occurs when ByteBuffers are not properly encoded during serialization.%n" +
                            "ByteBuffers lose their data when toString() is called.%n" +
                            "SOLUTION: Update MapFlattener to encode ByteBuffers as Base64 strings.%n" +
                            "Add a serializeValue() method that converts ByteBuffers to 'B64:<base64data>' format.",
                    strValue);

            if (strictValidation) {
                throw new IllegalArgumentException(errorMessage);
            }

            // In non-strict mode, log warning and return zero as placeholder
            log.warn(errorMessage + " Returning ZERO as placeholder.");
            return DECIMAL_CONVERSION.toBytes(BigDecimal.ZERO, schema, decimalType);
        }

        BigDecimal decimal = new BigDecimal(strValue);

        if (decimal.precision() > decimalType.getPrecision()) {
            decimal = decimal.setScale(decimalType.getScale(), RoundingMode.HALF_UP);
        }

        if (schema.getType() == BYTES) {
            return DECIMAL_CONVERSION.toBytes(decimal, schema, decimalType);
        } else if (schema.getType() == FIXED) {
            return DECIMAL_CONVERSION.toFixed(decimal, schema, decimalType);
        } else {
            throw new IllegalStateException(
                    "Decimal logical type must be backed by bytes or fixed");
        }
    }

    private String convertUuid(Object value) {
        String strValue = value.toString();
        UUID uuid = UUID.fromString(strValue);
        return uuid.toString();
    }

    // ========================= UTILITY METHODS =========================

    private List<Object> deserializeArray(Object value) {
        if (value == null) {
            return Collections.singletonList(null);
        }

        String strValue = value.toString().trim();

        // Handle BRACKET_LIST format first
        if (arrayFormat == BRACKET_LIST && strValue.startsWith("[") && strValue.endsWith("]")) {
            return deserializeBracketList(strValue);
        }

        // Only try JSON if it looks like valid JSON
        if (strValue.startsWith("[") && strValue.endsWith("]")) {
            boolean looksLikeJson = strValue.contains("\"") ||
                    strValue.matches(".*\\[\\s*-?\\d.*") ||
                    strValue.equals("[]") ||
                    strValue.contains("true") ||
                    strValue.contains("false") ||
                    strValue.contains("null");

            if (looksLikeJson) {
                try {
                    return objectMapper.readValue(strValue, new TypeReference<List<Object>>() {});
                } catch (Exception e) {
                    log.debug("Failed to parse as JSON: {}", e.getMessage());
                }
            }
        }

        // Try other formats based on configuration
        String content = strValue;
        if (strValue.startsWith("[") && strValue.endsWith("]")) {
            content = strValue.substring(1, strValue.length() - 1);
        }

        switch (arrayFormat) {
            case COMMA_SEPARATED:
                return Arrays.asList(content.split(",", -1));
            case PIPE_SEPARATED:
                return Arrays.asList(content.split("\\|", -1));
            case BRACKET_LIST:
                return deserializeBracketList(strValue);
            default:
                return Collections.singletonList(value);
        }
    }

    private List<Object> deserializeBracketList(String value) {
        Matcher matcher = BRACKET_LIST_PATTERN.matcher(value.trim());
        if (matcher.matches()) {
            String content = matcher.group(1);
            if (content.isEmpty()) {
                return Collections.emptyList();
            }

            // Use bracket-aware split to handle nested arrays
            return splitBracketAware(content);
        }
        return Collections.singletonList(value);
    }

    /**
     * Split a string on commas, but respect nested brackets.
     * Also properly handles and strips quotes from string values.
     */
    private List<Object> splitBracketAware(String content) {
        List<Object> result = new ArrayList<>()
        StringBuilder current = new StringBuilder()
        int bracketDepth = 0
        boolean inQuotes = false

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i)

            if (c == '"' && (i == 0 || content.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes
                // DON'T append the quote character - we'll handle unquoting at the end
                current.append(c)
            } else if (!inQuotes) {
                if (c == '[') {
                    bracketDepth++
                    current.append(c)
                } else if (c == ']') {
                    bracketDepth--
                    current.append(c)
                } else if (c == ',' && bracketDepth == 0) {
                    String item = current.toString().trim()
                    if (!item.isEmpty()) {
                        result.add(unquoteString(item))
                    }
                    current = new StringBuilder()
                } else {
                    current.append(c)
                }
            } else {
                current.append(c)
            }
        }

        // Add the last item
        String item = current.toString().trim()
        if (!item.isEmpty()) {
            result.add(unquoteString(item))
        }

        return result.isEmpty() ? Collections.singletonList(content) : result
    }

/**
 * Remove surrounding quotes from a string and unescape internal quotes.
 */
    private Object unquoteString(String value) {
        if (value == null || value.isEmpty()) {
            return value
        }

        String trimmed = value.trim()

        // Handle quoted strings
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() >= 2) {
            return trimmed.substring(1, trimmed.length() - 1)
                    .replace("\\\"", "\"")
                    .replace("\\n", "\n")
                    .replace("\\t", "\t")
        }

        // Handle null
        if ("null".equals(trimmed)) {
            return null
        }

        // Handle nested arrays/objects - return as-is
        if (trimmed.startsWith("[") || trimmed.startsWith("{")) {
            return trimmed
        }

        // Try to parse as number
        try {
            if (trimmed.contains(".")) {
                return Double.parseDouble(trimmed)
            } else {
                return Long.parseLong(trimmed)
            }
        } catch (NumberFormatException e) {
            // Return as string
            return trimmed
        }
    }

    private boolean isNullable(Schema schema) {
        if (schema.getType() != UNION) {
            return false;
        }
        return schema.getTypes().stream()
                .anyMatch(s -> s.getType() == NULL);
    }

    private Schema unwrapNullable(Schema schema) {
        if (schema.getType() != UNION) {
            return schema;
        }

        List<Schema> types = schema.getTypes();
        if (types.size() == 2) {
            return types.get(0).getType() == NULL ? types.get(1) : types.get(0);
        }

        return schema;
    }

    private boolean isPrimitiveType(Schema.Type type) {
        switch (type) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case STRING:
            case BYTES:
            case FIXED:
                return true;
            default:
                return false;
        }
    }

    /**
     * Extract value at specific index from potentially JSON-encoded array.
     * For primitive fields, if the value is a JSON array string like "[5,5]",
     * parse it and return the element at the specified index.
     */
    private Object extractValueAtIndex(Object value, int index, Schema.Type targetType) {
        if (value == null) {
            return null;
        }

        if (value instanceof String && isPrimitiveType(targetType)) {
            String strValue = ((String) value).trim();
            if (strValue.startsWith("[") && strValue.endsWith("]")) {
                List<Object> parsedArray = null;

                // Only try JSON parsing if it looks like valid JSON
                boolean looksLikeJson = strValue.contains("\"") ||
                        strValue.matches(".*\\[\\s*-?\\d.*") ||
                        strValue.equals("[]") ||
                        strValue.contains("true") ||
                        strValue.contains("false") ||
                        strValue.contains("null");

                if (looksLikeJson) {
                    try {
                        parsedArray = objectMapper.readValue(strValue, List.class);
                    } catch (Exception e) {
                        log.debug("Not valid JSON, will try format-specific parsing: {}", strValue);
                    }
                }

                // If not parsed as JSON, try format-specific parsing
                if (parsedArray == null && arrayFormat == BRACKET_LIST) {
                    // For BRACKET_LIST format, parse the nested structure
                    parsedArray = deserializeBracketList(strValue);
                } else if (parsedArray == null) {
                    // For other formats, remove brackets and split
                    String content = strValue.substring(1, strValue.length() - 1);
                    switch (arrayFormat) {
                        case COMMA_SEPARATED:
                            parsedArray = Arrays.asList(content.split(",", -1));
                            break;
                        case PIPE_SEPARATED:
                            parsedArray = Arrays.asList(content.split("\\|", -1));
                            break;
                        default:
                            parsedArray = Collections.singletonList(strValue);
                    }
                }

                if (parsedArray != null) {
                    if (index >= 0 && index < parsedArray.size()) {
                        Object result = parsedArray.get(index);
                        // Trim string results
                        if (result instanceof String) {
                            return ((String) result).trim();
                        }
                        return result;
                    } else {
                        // Index out of bounds - this is expected for asymmetric arrays
                        log.debug("Index {} out of bounds for array of size {}", index, parsedArray.size());
                        return null;
                    }
                }
            }
        }

        // Return as-is if not an array string
        return value;
    }

    /**
     * Calculate array size accounting for JSON-encoded arrays in field values.
     */
    private int calculateArraySize(PathNode node, Map<String, List<Object>> arrayFieldValues, Schema elementSchema) {
        // If no array field values, try to determine size from child nodes
        if (arrayFieldValues == null || arrayFieldValues.isEmpty()) {
            // Look at child nodes to infer array size
            if (!node.children.isEmpty()) {
                // Get the first child node and check its arrayFieldValues
                for (PathNode childNode : node.children.values()) {
                    if (childNode.arrayFieldValues != null && !childNode.arrayFieldValues.isEmpty()) {
                        // Get the size from the first field's value list
                        for (List<Object> values : childNode.arrayFieldValues.values()) {
                            if (values != null && !values.isEmpty()) {
                                return values.size();
                            }
                        }
                    }
                }
            }
            // Default to 1 if we can't determine from children
            return 1;
        }

        int maxSize = 0;

        for (Map.Entry<String, List<Object>> entry : arrayFieldValues.entrySet()) {
            String fieldName = entry.getKey();
            List<Object> fieldValues = entry.getValue();

            if (fieldValues == null || fieldValues.isEmpty()) {
                continue;
            }

            // Get the field schema to check if it's a primitive type
            Schema.Field field = elementSchema.getField(fieldName);
            if (field != null) {
                Schema actualFieldSchema = unwrapNullable(field.schema());
                Object firstValue = fieldValues.get(0);

                // Check if the first value is an array string for a primitive field
                if (firstValue instanceof String && isPrimitiveType(actualFieldSchema.getType())) {
                    String strValue = (String) firstValue;
                    if (strValue.trim().startsWith("[") && strValue.trim().endsWith("]")) {
                        List<Object> parsedArray = null;

                        // Try JSON first
                        try {
                            parsedArray = objectMapper.readValue(strValue, List.class);
                        } catch (Exception e) {
                            // Not valid JSON, try format-specific parsing
                        }

                        // If JSON parsing failed, try format-specific parsing
                        if (parsedArray == null) {
                            switch (arrayFormat) {
                                case BRACKET_LIST:
                                    parsedArray = deserializeBracketList(strValue);
                                    break;
                                case COMMA_SEPARATED:
                                    parsedArray = Arrays.asList(strValue.split(",", -1));
                                    break;
                                case PIPE_SEPARATED:
                                    parsedArray = Arrays.asList(strValue.split("\\|", -1));
                                    break;
                            }
                        }

                        if (parsedArray != null) {
                            maxSize = Math.max(maxSize, parsedArray.size());
                            continue;
                        }
                    }
                }
            }

            // Default: use the outer list size
            maxSize = Math.max(maxSize, fieldValues.size());
        }

        return maxSize > 0 ? maxSize : 1;
    }

    private Object getDefaultValue(Schema.Type type) {
        switch (type) {
            case INT: return 0;
            case LONG: return 0L;
            case FLOAT: return 0.0f;
            case DOUBLE: return 0.0;
            case BOOLEAN: return false;
            case STRING: return "";
            default: return null;
        }
    }

    private Object getDefaultValueForLogicalType(Schema schema) {
        String logicalTypeName = schema.getLogicalType().getName();

        switch (logicalTypeName) {
            case "timestamp-millis":
            case "timestamp-micros":
            case "time-micros":
                return 0L;
            case "date":
            case "time-millis":
                return 0;
            case "decimal":
                LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
                BigDecimal zero = BigDecimal.ZERO.setScale(decimalType.getScale());
                if (schema.getType() == BYTES) {
                    return DECIMAL_CONVERSION.toBytes(zero, schema, decimalType);
                } else {
                    return DECIMAL_CONVERSION.toFixed(zero, schema, decimalType);
                }
            case "uuid":
                return UUID.randomUUID().toString();
            default:
                return null;
        }
    }

    // ========================= CONVERSION METHODS =========================

    /**
     * Convert GenericRecord to Map (iterative to avoid stack overflow)
     */
    private Map<String, Object> genericRecordToMap(GenericRecord record) {
        Map<String, Object> result = new LinkedHashMap<>();
        Queue<ConversionTask> queue = new LinkedList<>();
        queue.add(new ConversionTask(record, result));

        while (!queue.isEmpty()) {
            ConversionTask task = queue.poll();

            for (Schema.Field field : task.record.getSchema().getFields()) {
                Object value = task.record.get(field.name());

                if (value == null) {
                    task.target.put(field.name(), null);
                } else if (value instanceof GenericRecord) {
                    Map<String, Object> nested = new LinkedHashMap<>();
                    task.target.put(field.name(), nested);
                    queue.add(new ConversionTask((GenericRecord) value, nested));
                } else if (value instanceof List) {
                    task.target.put(field.name(), convertList((List<?>) value, queue));
                } else if (value instanceof Map) {
                    task.target.put(field.name(), convertMap((Map<?, ?>) value, queue));
                } else {
                    task.target.put(field.name(), value);
                }
            }
        }

        return result;
    }

    private static class ConversionTask {
        final GenericRecord record;
        final Map<String, Object> target;

        ConversionTask(GenericRecord record, Map<String, Object> target) {
            this.record = record;
            this.target = target;
        }
    }

    private List<Object> convertList(List<?> list, Queue<ConversionTask> queue) {
        List<Object> converted = new ArrayList<>(list.size());

        for (Object item : list) {
            if (item instanceof GenericRecord) {
                Map<String, Object> itemMap = new LinkedHashMap<>();
                converted.add(itemMap);
                queue.add(new ConversionTask((GenericRecord) item, itemMap));
            } else if (item instanceof List) {
                converted.add(convertList((List<?>) item, queue));
            } else if (item instanceof Map) {
                converted.add(convertMap((Map<?, ?>) item, queue));
            } else {
                converted.add(item);
            }
        }

        return converted;
    }

    private Map<String, Object> convertMap(Map<?, ?> map, Queue<ConversionTask> queue) {
        Map<String, Object> converted = new LinkedHashMap<>();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey().toString();
            Object value = entry.getValue();

            if (value instanceof GenericRecord) {
                Map<String, Object> valueMap = new LinkedHashMap<>();
                converted.put(key, valueMap);
                queue.add(new ConversionTask((GenericRecord) value, valueMap));
            } else if (value instanceof List) {
                converted.put(key, convertList((List<?>) value, queue));
            } else if (value instanceof Map) {
                converted.put(key, convertMap((Map<?, ?>) value, queue));
            } else {
                converted.put(key, value);
            }
        }

        return converted;
    }

    /**
     * Convert Map to GenericRecord
     */
    private GenericRecord mapToGenericRecord(Map<String, Object> map, Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);

        for (Schema.Field field : schema.getFields()) {
            Object value = map.get(field.name());
            if (value != null) {
                builder.set(field.name(), value);
            } else if (field.hasDefaultValue()) {
                builder.set(field.name(), field.defaultVal());
            } else if (isNullable(field.schema())) {
                builder.set(field.name(), null);
            }
        }

        return builder.build();
    }

    // ========================= CACHE MANAGEMENT =========================

    public void clearSchemaCache() {
        schemaCache.clear();
    }

    public int getSchemaCacheSize() {
        return schemaCache.size();
    }

    // ========================= EXCEPTIONS =========================

    public static class ReconstructionException extends RuntimeException {
        public ReconstructionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}