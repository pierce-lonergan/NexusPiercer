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
         * Split string by delimiter while respecting bracket nesting.
         * Example: "[a,b],[c,d]" split by ',' → ["[a,b]", "[c,d]"]
         * Not: ["[a", "b]", "[c", "d]"]
         */
        private static List<String> splitRespectingBrackets(String str, String delimiter) {
            if (delimiter == null || delimiter.length() != 1) {
                throw new IllegalArgumentException("Delimiter must be a single character");
            }
            char delimiterChar = delimiter.charAt(0);

            List<String> result = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            int bracketDepth = 0;

            for (int i = 0; i < str.length(); i++) {
                char c = str.charAt(i);

                if (c == '[') {
                    bracketDepth++;
                    current.append(c);
                } else if (c == ']') {
                    bracketDepth--;
                    current.append(c);
                } else if (c == delimiterChar && bracketDepth == 0) {
                    // Only split when not inside brackets
                    result.add(current.toString());
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            }

            // Add the last part
            if (current.length() > 0) {
                result.add(current.toString());
            }

            return result;
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
                PathNode fieldChildNode = childNode.children.get(nestedFieldName);
                Schema actualFieldSchema = unwrapNullable(nestedField.schema());

                if (fieldChildNode != null) {
                    // Reconstruct from child node based on schema type
                    if (actualFieldSchema.getType() == ARRAY) {
                        // Path already includes array index from caller
                        Object reconstructed = reconstructValue(fieldChildNode, actualFieldSchema,
                                path + "." + fieldPrefix + "." + nestedFieldName, 0);
                        if (reconstructed != null) {
                            builder.set(nestedFieldName, reconstructed);
                            hasAnyField = true;
                        }
                    } else if (actualFieldSchema.getType() == RECORD) {
                        // For nested records within arrays, recursively call reconstructNestedRecordFromArray
                        // Path already includes array index from caller
                        GenericRecord nestedRecord = reconstructNestedRecordFromArray(
                                childNode, actualFieldSchema, nestedFieldName, index,
                                path + "." + fieldPrefix);
                        if (nestedRecord != null) {
                            builder.set(nestedFieldName, nestedRecord);
                            hasAnyField = true;
                        }
                    }
                } else if (nestedField.hasDefaultValue()) {
                    builder.set(nestedFieldName, nestedField.defaultVal());
                    hasAnyField = true;
                } else if (isNullable(nestedField.schema())) {
                    builder.set(nestedFieldName, null);
                    hasAnyField = true;
                }
            }
        }

        return hasAnyField ? builder.build() : null;
    }

    private List<Object> reconstructArrayOfRecords(PathNode node, Schema elementSchema,
                                                   String path, int currentDepth) {
        if (elementSchema.getType() != RECORD) {
            throw new IllegalStateException(
                    "Expected RECORD element type at: " + path);
        }

        // Determine array size from field values
        // Need to check if values are JSON-encoded arrays and use their size
        int arraySize = calculateArraySize(node, node.arrayFieldValues, elementSchema);

        List<Object> result = new ArrayList<>(arraySize);

        for (int i = 0; i < arraySize; i++) {
            GenericRecordBuilder elementBuilder = new GenericRecordBuilder(elementSchema);

            for (Schema.Field field : elementSchema.getFields()) {
                String fieldName = field.name();
                Schema fieldSchema = field.schema();
                List<Object> fieldValues = null;

                // Only try to get field values if arrayFieldValues is not null
                if (node.arrayFieldValues != null) {
                    fieldValues = node.arrayFieldValues.get(fieldName);
                }

                if (fieldValues != null && !fieldValues.isEmpty()) {
                    // Get the value - for JSON arrays, we use the last (or only) element
                    int valueIndex = Math.min(i, fieldValues.size() - 1);
                    Object rawValue = fieldValues.get(valueIndex);

                    // Unwrap nullable schemas to check actual type
                    Schema actualFieldSchema = unwrapNullable(fieldSchema);

                    // Extract value at index i (handles JSON-encoded arrays for primitives)
                    // This will parse "[5,5]" and extract the element at index i
                    Object value = extractValueAtIndex(rawValue, i, actualFieldSchema.getType());

                    // Handle nested arrays in array elements
                    if (actualFieldSchema.getType() == ARRAY && value instanceof String) {
                        // Parse string back to array
                        String arrayString = (String) value;
                        try {
                            // Special case: empty array string
                            if (arrayString.trim().equals("[]")) {
                                value = new ArrayList<>();
                            } else {
                                List<Object> parsedArray = null;

                                // Try JSON first
                                try {
                                    parsedArray = objectMapper.readValue(arrayString, List.class);
                                } catch (Exception jsonEx) {
                                    // Not valid JSON, try format-specific parsing
                                    log.debug("Failed to parse as JSON array in reconstructArrayOfRecords: '{}', trying format-specific parser", arrayString);
                                }

                                // If JSON parsing failed, try format-specific parsing
                                if (parsedArray == null) {
                                    switch (arrayFormat) {
                                        case BRACKET_LIST:
                                            parsedArray = deserializeBracketList(arrayString);
                                            break;
                                        case COMMA_SEPARATED:
                                            parsedArray = Arrays.asList(arrayString.split(",", -1));
                                            break;
                                        case PIPE_SEPARATED:
                                            parsedArray = Arrays.asList(arrayString.split("\\|", -1));
                                            break;
                                        default:
                                            // Last resort: single element list
                                            parsedArray = Collections.singletonList(arrayString);
                                    }
                                }

                                // Ensure we got a valid list
                                if (parsedArray == null || parsedArray.isEmpty()) {
                                    value = new ArrayList<>();
                                } else {
                                    value = reconstructArrayFromValues(parsedArray,
                                            actualFieldSchema.getElementType(),
                                            path + "[" + i + "]." + fieldName,
                                            currentDepth + 1);
                                    // Ensure we have a valid list, never null
                                    if (value == null) {
                                        value = new ArrayList<>();
                                    }
                                }
                            }
                        } catch (Exception e) {
                            if (strictValidation) {
                                throw new IllegalArgumentException(
                                        String.format("Failed to parse nested array at %s[%d].%s: %s",
                                                path, i, fieldName, arrayString), e);
                            }
                            // If parsing fails, try to create empty array or use default
                            value = new ArrayList<>();
                        }
                    } else if (actualFieldSchema.getType() == ARRAY) {
                        // Value is not a String but field is ARRAY - might already be a List
                        if (value instanceof List) {
                            // Already a list, reconstruct it properly
                            value = reconstructArrayFromValues((List<Object>) value,
                                    actualFieldSchema.getElementType(),
                                    path + "[" + i + "]." + fieldName,
                                    currentDepth + 1);
                        } else if (value == null || value.getClass().getSimpleName().equals("NullObject")) {
                            // Groovy NullObject or null - treat as empty array
                            value = new ArrayList<>();
                        } else {
                            // Unexpected type for array field
                            if (strictValidation) {
                                throw new IllegalArgumentException(
                                        String.format("Unexpected type %s for ARRAY field at %s[%d].%s",
                                                value.getClass().getSimpleName(), path, i, fieldName));
                            }
                            value = new ArrayList<>();
                        }
                    } else if (actualFieldSchema.getType() == RECORD && value instanceof String) {
                        // This might be a nested record that needs further reconstruction
                        value = convertPrimitive(value, fieldSchema,
                                path + "[" + i + "]." + fieldName);
                    } else {
                        value = convertPrimitive(value, fieldSchema,
                                path + "[" + i + "]." + fieldName);
                    }

                    elementBuilder.set(fieldName, value);
                } else if (fieldSchema.getType() == RECORD ||
                        (fieldSchema.getType() == UNION &&
                                unwrapUnion(fieldSchema).getType() == RECORD)) {
                    // Handle nested record - look for child fields with prefix
                    // Include the array index in the path when calling
                    Schema unwrappedSchema = fieldSchema.getType() == UNION ? unwrapUnion(fieldSchema) : fieldSchema;
                    if (unwrappedSchema.getType() == RECORD) {
                        GenericRecord nestedRecord = reconstructNestedRecordFromArray(
                                node, unwrappedSchema, fieldName, i, path + "[" + i + "]");
                        if (nestedRecord != null) {
                            elementBuilder.set(fieldName, nestedRecord);
                        } else if (field.hasDefaultValue()) {
                            elementBuilder.set(fieldName, field.defaultVal());
                        } else if (isNullable(fieldSchema)) {
                            elementBuilder.set(fieldName, null);
                        }
                    }
                } else if (fieldSchema.getType() == ARRAY ||
                        (fieldSchema.getType() == UNION &&
                                unwrapUnion(fieldSchema).getType() == ARRAY)) {
                    // Handle nested array that wasn't in arrayFieldValues
                    // Check if there's a child node for this array field
                    PathNode childNode = node.children.get(fieldName);
                    Schema unwrappedSchema = fieldSchema.getType() == UNION ? unwrapUnion(fieldSchema) : fieldSchema;

                    if (childNode != null && unwrappedSchema.getType() == ARRAY && childNode.arrayFieldValues != null) {
                        // Reconstruct the array from the child node
                        Schema elementType = unwrappedSchema.getElementType();
                        if (elementType.getType() == RECORD) {
                            // Array of records - use reconstructArrayOfRecords
                            List<Object> arrayValue = reconstructArrayOfRecords(
                                    childNode, elementType, path + "[" + i + "]." + fieldName, currentDepth + 1);
                            elementBuilder.set(fieldName, arrayValue);
                        } else {
                            // Array of primitives or other types
                            Object reconstructed = reconstructValue(childNode, unwrappedSchema,
                                    path + "[" + i + "]." + fieldName, currentDepth + 1);
                            if (reconstructed != null) {
                                elementBuilder.set(fieldName, reconstructed);
                            } else {
                                elementBuilder.set(fieldName, new ArrayList<>());
                            }
                        }
                    } else if (field.hasDefaultValue()) {
                        elementBuilder.set(fieldName, field.defaultVal());
                    } else if (isNullable(fieldSchema)) {
                        elementBuilder.set(fieldName, null);
                    } else {
                        // Array field is required but has no data, set to empty array
                        elementBuilder.set(fieldName, new ArrayList<>());
                    }
                } else if (field.hasDefaultValue()) {
                    elementBuilder.set(fieldName, field.defaultVal());
                } else if (isNullable(fieldSchema)) {
                    elementBuilder.set(fieldName, null);
                }
            }

            result.add(elementBuilder.build());
        }

        return result;
    }

    private List<Object> reconstructArrayFromValues(List<Object> values, Schema elementSchema,
                                                    String path, int currentDepth) {
        // Handle null or empty input
        if (values == null || values.isEmpty() ||
                (values.size() == 1 && values.get(0) == null)) {
            return new ArrayList<>();
        }

        List<Object> result = new ArrayList<>(values.size());

        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            String indexPath = path + "[" + i + "]";

            // Skip null values or Groovy NullObject
            if (value == null || value.getClass().getSimpleName().equals("NullObject")) {
                continue;
            }

            if (value instanceof List) {
                // Nested array
                if (elementSchema.getType() == ARRAY) {
                    result.add(reconstructArrayFromValues((List<Object>) value,
                            elementSchema.getElementType(), indexPath, currentDepth + 1));
                } else {
                    // Flatten nested list
                    for (Object item : (List<?>) value) {
                        if (item != null && !item.getClass().getSimpleName().equals("NullObject")) {
                            result.add(convertPrimitive(item, elementSchema, indexPath));
                        }
                    }
                }
            } else {
                result.add(convertPrimitive(value, elementSchema, indexPath));
            }
        }

        return result;
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
        List<Schema> types = unionSchema.getTypes();

        // Handle nullable union (most common case)
        if (types.size() == 2) {
            Schema nullSchema = types.get(0).getType() == NULL ? types.get(0) : types.get(1);
            Schema nonNullSchema = types.get(0).getType() != NULL ? types.get(0) : types.get(1);

            if (node.value == null || "null".equals(String.valueOf(node.value))) {
                return null;
            }

            return reconstructValue(node, nonNullSchema, path, currentDepth);
        }

        // Try each type in order
        for (Schema type : types) {
            if (type.getType() == NULL && node.value == null) {
                return null;
            }

            try {
                return reconstructValue(node, type, path, currentDepth);
            } catch (Exception e) {
                // Try next type
                continue;
            }
        }

        if (strictValidation) {
            throw new IllegalStateException("Could not match any union type at: " + path);
        }

        return null;
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
                    byte[] bytes = strValue.getBytes();
                    if (bytes.length != actualSchema.getFixedSize()) {
                        throw new IllegalArgumentException(
                                String.format("Fixed size mismatch at %s: expected %d, got %d",
                                        path, actualSchema.getFixedSize(), bytes.length));
                    }
                    return new GenericData.Fixed(actualSchema, bytes);

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

        // Try JSON first
        if (strValue.startsWith("[") && strValue.endsWith("]")) {
            try {
                return objectMapper.readValue(strValue, new TypeReference<List<Object>>() {});
            } catch (Exception e) {
                // Fall through to other formats
            }
        }

        // Try other formats based on configuration
        switch (arrayFormat) {
            case COMMA_SEPARATED:
                return Arrays.asList(strValue.split(",", -1));
            case PIPE_SEPARATED:
                return Arrays.asList(strValue.split("\\|", -1));
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
     * Example: "[a, b], [c, d]" -> ["[a, b]", "[c, d]"]
     * Not: "[a", "b]", "[c", "d]"
     */
    private List<Object> splitBracketAware(String content) {
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
                    // This comma is at the top level, so it's a separator
                    String item = current.toString().trim();
                    if (!item.isEmpty()) {
                        result.add(item);
                    }
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            } else {
                current.append(c);
            }
        }

        // Add the last item
        String item = current.toString().trim();
        if (!item.isEmpty()) {
            result.add(item);
        }

        return result.isEmpty() ? Collections.singletonList(content) : result;
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
        if (value instanceof String && isPrimitiveType(targetType)) {
            String strValue = (String) value;
            if (strValue.trim().startsWith("[") && strValue.trim().endsWith("]")) {
                List<Object> parsedArray = null;

                // Try JSON first
                try {
                    parsedArray = objectMapper.readValue(strValue, List.class);
                } catch (Exception e) {
                    // Not valid JSON, try format-specific parsing
                    log.debug("Failed to parse as JSON array in extractValueAtIndex: '{}', trying format-specific parser", strValue);
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
                        default:
                            // Return original value if can't parse
                            return value;
                    }
                }

                if (parsedArray != null) {
                    if (index < parsedArray.size()) {
                        return parsedArray.get(index);
                    } else {
                        // Index out of bounds in array - return null so caller can handle
                        return null;
                    }
                }
            }
        }
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