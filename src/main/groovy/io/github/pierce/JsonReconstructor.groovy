//package io.github.pierce;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.core.type.TypeReference;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.SerializationFeature;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.*;
//import java.nio.charset.Charset;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.function.Function;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//import java.util.stream.Collectors;
//
///**
// * Production-grade JSON Reconstructor - Converts flattened Maps back to hierarchical JSON.
// *
// * <p>This class reconstructs nested JSON structures from flattened key-value Maps produced by
// * {@link MapFlattener} or {@link JsonFlattener}. Unlike AvroReconstructor, this class does NOT
// * require a schema - it infers the structure from the flattened keys themselves.</p>
// *
// * <h3>Key Features:</h3>
// * <ul>
// *   <li>Schema-less reconstruction from flattened key patterns</li>
// *   <li>Automatic detection of arrays vs nested objects</li>
// *   <li>Support for all array serialization formats (JSON, comma, pipe, bracket)</li>
// *   <li>Deep reconstruction of nested arrays and objects</li>
// *   <li>Comprehensive verification utilities</li>
// *   <li>Thread-safe and production-ready</li>
// *   <li>Fluent API with builder pattern</li>
// *   <li>Detailed error reporting with path information</li>
// * </ul>
// *
// * <h3>Basic Usage:</h3>
// * <pre>
// * // 1. Flatten original data
// * MapFlattener flattener = MapFlattener.builder().build();
// * Map&lt;String, Object&gt; flattened = flattener.flatten(originalData);
// *
// * // 2. Reconstruct from flattened data
// * JsonReconstructor reconstructor = JsonReconstructor.builder().build();
// * Map&lt;String, Object&gt; reconstructed = reconstructor.reconstruct(flattened);
// *
// * // 3. Verify reconstruction
// * ReconstructionVerification verification =
// *     reconstructor.verify(originalData, reconstructed);
// *
// * if (verification.isPerfect()) {
// *     System.out.println("Perfect reconstruction!");
// * } else {
// *     verification.getDifferences().forEach(System.out::println);
// * }
// * </pre>
// *
// * <h3>With Custom Configuration:</h3>
// * <pre>
// * JsonReconstructor reconstructor = JsonReconstructor.builder()
// *     .separator("__")                                    // Match MapFlattener separator
// *     .arrayFormat(ArraySerializationFormat.JSON)         // Match MapFlattener array format
// *     .inferArraysFromValues(true)                        // Auto-detect arrays from serialized values
// *     .preserveNulls(true)                                // Keep null values in output
// *     .build();
// *
// * Map&lt;String, Object&gt; reconstructed = reconstructor.reconstruct(flattened);
// * </pre>
// *
// * <h3>Fluent API:</h3>
// * <pre>
// * // Simple one-liner
// * Map&lt;String, Object&gt; result = JsonReconstructor.create()
// *     .from(flattenedMap)
// *     .toMap();
// *
// * // With transformation
// * String json = JsonReconstructor.create()
// *     .from(flattenedMap)
// *     .transform(map -&gt; { map.put("reconstructed", true); return map; })
// *     .toPrettyJson();
// *
// * // From JSON string
// * Map&lt;String, Object&gt; result = JsonReconstructor.create()
// *     .fromJson(flattenedJsonString)
// *     .toMap();
// * </pre>
// *
// * <h3>Array Handling:</h3>
// * <p>The reconstructor detects arrays from flattened patterns:</p>
// * <ul>
// *   <li>Multiple keys with same prefix but different field suffixes: {@code users_name, users_age}
// *       → Reconstructed as array of objects if values are serialized arrays</li>
// *   <li>Serialized array values: {@code ["Alice","Bob"]} → Parsed and distributed to array elements</li>
// *   <li>Explicit array hints can be provided via {@code arrayPaths()} configuration</li>
// * </ul>
// *
// * @author Pierce
// * @version 1.0
// * @see MapFlattener
// * @see JsonFlattener
// */
//public class JsonReconstructor implements Serializable {
//    private static final long serialVersionUID = 1L;
//    private static final Logger log = LoggerFactory.getLogger(JsonReconstructor.class);
//
//    // ========================= CONSTANTS =========================
//
//    private static final int DEFAULT_MAX_DEPTH = 100;
//    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
//
//    // Shared ObjectMapper
//    private static final ObjectMapper SHARED_MAPPER = createConfiguredMapper();
//
//    private static ObjectMapper createConfiguredMapper() {
//        ObjectMapper mapper = new ObjectMapper();
//        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, false);
//        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, true);
//        return mapper;
//    }
//
//    // Patterns for detection
//    private static final Pattern JSON_ARRAY_PATTERN = Pattern.compile("^\\s*\\[.*\\]\\s*\$", Pattern.DOTALL);
//    private static final Pattern BRACKET_LIST_PATTERN = Pattern.compile("^\\s*\\[(.*)\\]\\s*\$", Pattern.DOTALL);
//
//    // Type references
//    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF =
//            new TypeReference<Map<String, Object>>() {};
//    private static final TypeReference<List<Object>> LIST_TYPE_REF =
//            new TypeReference<List<Object>>() {};
//
//    // ========================= CONFIGURATION =========================
//
//    private final String separator;
//    private final ArraySerializationFormat arrayFormat;
//    private final boolean inferArraysFromValues;
//    private final boolean preserveNulls;
//    private final int maxDepth;
//    private final Set<String> arrayPaths;
//    private final ObjectMapper objectMapper;
//
//    // ========================= ARRAY FORMAT ENUM =========================
//
//    /**
//     * Array serialization formats (must match MapFlattener)
//     */
//    public enum ArraySerializationFormat {
//        /** JSON format: ["a","b","c"] */
//        JSON,
//        /** Comma-separated: a,b,c */
//        COMMA_SEPARATED,
//        /** Pipe-separated: a|b|c */
//        PIPE_SEPARATED,
//        /** Bracket list: [a, b, c] */
//        BRACKET_LIST
//    }
//
//    // ========================= CONSTRUCTORS =========================
//
//    private JsonReconstructor(Builder builder) {
//        this.separator = builder.separator;
//        this.arrayFormat = builder.arrayFormat;
//        this.inferArraysFromValues = builder.inferArraysFromValues;
//        this.preserveNulls = builder.preserveNulls;
//        this.maxDepth = builder.maxDepth;
//        this.arrayPaths = builder.arrayPaths != null ?
//                new HashSet<>(builder.arrayPaths) : new HashSet<>();
//        this.objectMapper = builder.objectMapper != null ?
//                builder.objectMapper : SHARED_MAPPER;
//    }
//
//    // ========================= STATIC FACTORY METHODS =========================
//
//    /**
//     * Create a new builder for configuration.
//     */
//    public static Builder builder() {
//        return new Builder();
//    }
//
//    /**
//     * Create a reconstructor with default configuration.
//     */
//    public static FluentOperation create() {
//        return new FluentOperation(new JsonReconstructor(new Builder()));
//    }
//
//    /**
//     * Create with custom separator (convenience method).
//     */
//    public static FluentOperation withSeparator(String separator) {
//        return new FluentOperation(new JsonReconstructor(
//                new Builder().separator(separator)));
//    }
//
//    /**
//     * Create configured for useArrayBoundarySeparator mode.
//     */
//    public static FluentOperation withArrayBoundarySeparator() {
//        return new FluentOperation(new JsonReconstructor(
//                new Builder().separator("__")));
//    }
//
//    // ========================= BUILDER =========================
//
//    /**
//     * Builder for JsonReconstructor configuration.
//     */
//    public static class Builder {
//        private String separator = "_";
//        private ArraySerializationFormat arrayFormat = ArraySerializationFormat.JSON;
//        private boolean inferArraysFromValues = true;
//        private boolean preserveNulls = true;
//        private int maxDepth = DEFAULT_MAX_DEPTH;
//        private Set<String> arrayPaths = new HashSet<>();
//        private ObjectMapper objectMapper = null;
//
//        /**
//         * Set the separator used in flattened keys.
//         * Must match the separator used by MapFlattener.
//         *
//         * @param separator The separator string (default: "_")
//         */
//        public Builder separator(String separator) {
//            this.separator = separator != null ? separator : "_";
//            return this;
//        }
//
//        /**
//         * Enable array boundary separator mode (uses "__").
//         * Shortcut for separator("__").
//         */
//        public Builder useArrayBoundarySeparator(boolean use) {
//            this.separator = use ? "__" : "_";
//            return this;
//        }
//
//        /**
//         * Set the array serialization format.
//         * Must match the format used by MapFlattener.
//         *
//         * @param format The array format (default: JSON)
//         */
//        public Builder arrayFormat(ArraySerializationFormat format) {
//            this.arrayFormat = format != null ? format : ArraySerializationFormat.JSON;
//            return this;
//        }
//
//        /**
//         * Enable automatic array detection from serialized values.
//         * When true, values like "[1,2,3]" are automatically detected as arrays.
//         *
//         * @param infer Whether to infer arrays from values (default: true)
//         */
//        public Builder inferArraysFromValues(boolean infer) {
//            this.inferArraysFromValues = infer;
//            return this;
//        }
//
//        /**
//         * Preserve null values in reconstructed output.
//         *
//         * @param preserve Whether to preserve nulls (default: true)
//         */
//        public Builder preserveNulls(boolean preserve) {
//            this.preserveNulls = preserve;
//            return this;
//        }
//
//        /**
//         * Set maximum reconstruction depth.
//         *
//         * @param depth Maximum depth (default: 100)
//         */
//        public Builder maxDepth(int depth) {
//            if (depth < 1) {
//                throw new IllegalArgumentException("maxDepth must be >= 1");
//            }
//            this.maxDepth = depth;
//            return this;
//        }
//
//        /**
//         * Explicitly specify paths that should be treated as arrays.
//         * Useful when auto-detection cannot determine array vs object.
//         *
//         * @param paths Paths that are arrays (e.g., "users", "items_tags")
//         */
//        public Builder arrayPaths(String... paths) {
//            this.arrayPaths = new HashSet<>(Arrays.asList(paths));
//            return this;
//        }
//
//        /**
//         * Add an array path hint.
//         *
//         * @param path Path that should be treated as array
//         */
//        public Builder addArrayPath(String path) {
//            this.arrayPaths.add(path);
//            return this;
//        }
//
//        /**
//         * Use custom ObjectMapper.
//         *
//         * @param mapper Custom ObjectMapper
//         */
//        public Builder objectMapper(ObjectMapper mapper) {
//            this.objectMapper = mapper;
//            return this;
//        }
//
//        /**
//         * Build the reconstructor.
//         */
//        public JsonReconstructor build() {
//            return new JsonReconstructor(this);
//        }
//
//        /**
//         * Build and return fluent operation.
//         */
//        public FluentOperation buildFluent() {
//            return new FluentOperation(build());
//        }
//    }
//
//    // ========================= CORE RECONSTRUCTION =========================
//
//    /**
//     * Reconstruct a flattened Map back to hierarchical structure.
//     *
//     * @param flattenedMap The flattened key-value map
//     * @return Reconstructed hierarchical map
//     */
//    public Map<String, Object> reconstruct(Map<String, Object> flattenedMap) {
//        if (flattenedMap == null || flattenedMap.isEmpty()) {
//            return new LinkedHashMap<>();
//        }
//
//        try {
//            // Step 1: Analyze the flattened keys to detect structure
//            StructureAnalysis analysis = analyzeStructure(flattenedMap);
//
//            // Step 2: Build the hierarchical structure
//            Map<String, Object> result = buildHierarchy(flattenedMap, analysis);
//
//            // Step 3: Post-process arrays
//            result = processArrays(result, analysis, "");
//
//            return result;
//
//        } catch (Exception e) {
//            log.error("Reconstruction failed", e);
//            throw new ReconstructionException("Failed to reconstruct flattened map", e);
//        }
//    }
//
//    /**
//     * Reconstruct from JSON string.
//     */
//    public Map<String, Object> reconstructFromJson(String flattenedJson) {
//        try {
//            Map<String, Object> flattenedMap = objectMapper.readValue(flattenedJson, MAP_TYPE_REF);
//            return reconstruct(flattenedMap);
//        } catch (JsonProcessingException e) {
//            throw new ReconstructionException("Failed to parse flattened JSON", e);
//        }
//    }
//
//    /**
//     * Reconstruct and serialize to JSON.
//     */
//    public String reconstructToJson(Map<String, Object> flattenedMap) {
//        Map<String, Object> reconstructed = reconstruct(flattenedMap);
//        try {
//            return objectMapper.writeValueAsString(reconstructed);
//        } catch (JsonProcessingException e) {
//            throw new ReconstructionException("Failed to serialize reconstructed map", e);
//        }
//    }
//
//    /**
//     * Reconstruct and serialize to pretty JSON.
//     */
//    public String reconstructToPrettyJson(Map<String, Object> flattenedMap) {
//        Map<String, Object> reconstructed = reconstruct(flattenedMap);
//        try {
//            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(reconstructed);
//        } catch (JsonProcessingException e) {
//            throw new ReconstructionException("Failed to serialize reconstructed map", e);
//        }
//    }
//
//    // ========================= STRUCTURE ANALYSIS =========================
//
//    /**
//     * Analysis result containing detected structure information.
//     */
//    private static class StructureAnalysis {
//        /** Paths that are detected as arrays */
//        Set<String> arrayPaths = new HashSet<>();
//
//        /** Paths that are detected as objects */
//        Set<String> objectPaths = new HashSet<>();
//
//        /** Map of array path to field names within the array elements */
//        Map<String, Set<String>> arrayFields = new LinkedHashMap<>();
//
//        /** Map of path to detected array size (from serialized values) */
//        Map<String, Integer> arraySizes = new LinkedHashMap<>();
//
//        /** All intermediate paths */
//        Set<String> allPaths = new HashSet<>();
//    }
//
//    /**
//     * Analyze the flattened keys to detect arrays and nested structures.
//     */
//    private StructureAnalysis analyzeStructure(Map<String, Object> flattenedMap) {
//        StructureAnalysis analysis = new StructureAnalysis();
//
//        // Add explicitly configured array paths
//        analysis.arrayPaths.addAll(arrayPaths);
//
//        // Group keys by their prefixes to detect potential arrays
//        Map<String, Set<String>> prefixToSuffixes = new LinkedHashMap<>();
//        Map<String, List<Object>> prefixToValues = new LinkedHashMap<>();
//
//        for (Map.Entry<String, Object> entry : flattenedMap.entrySet()) {
//            String key = entry.getKey();
//            Object value = entry.getValue();
//
//            String[] parts = key.split(Pattern.quote(separator));
//
//            // Track all intermediate paths
//            StringBuilder pathBuilder = new StringBuilder();
//            for (int i = 0; i < parts.length - 1; i++) {
//                if (pathBuilder.length() > 0) {
//                    pathBuilder.append(separator);
//                }
//                pathBuilder.append(parts[i]);
//                analysis.allPaths.add(pathBuilder.toString());
//            }
//
//            // Group by all possible prefixes
//            for (int i = 1; i < parts.length; i++) {
//                String prefix = String.join(separator, Arrays.copyOfRange(parts, 0, i));
//                String suffix = String.join(separator, Arrays.copyOfRange(parts, i, parts.length));
//
//                prefixToSuffixes.computeIfAbsent(prefix, k -> new LinkedHashSet<>()).add(suffix);
//                prefixToValues.computeIfAbsent(prefix, k -> new ArrayList<>()).add(value);
//            }
//        }
//
//        // Detect arrays: paths where multiple fields exist AND values are serialized arrays
//        for (Map.Entry<String, Set<String>> entry : prefixToSuffixes.entrySet()) {
//            String prefix = entry.getKey();
//            Set<String> suffixes = entry.getValue();
//            List<Object> values = prefixToValues.get(prefix);
//
//            // Check if this looks like an array
//            boolean isArray = false;
//            int detectedSize = 0;
//
//            if (inferArraysFromValues) {
//                // Check if any values are serialized arrays
//                for (Object value : values) {
//                    if (value instanceof String) {
//                        String strValue = ((String) value).trim();
//                        if (looksLikeSerializedArray(strValue)) {
//                            List<Object> parsed = parseArrayValue(strValue);
//                            if (parsed != null && parsed.size() > 1) {
//                                isArray = true;
//                                detectedSize = Math.max(detectedSize, parsed.size());
//                            }
//                        }
//                    }
//                }
//            }
//
//            // Also check explicit array paths
//            if (arrayPaths.contains(prefix)) {
//                isArray = true;
//            }
//
//            if (isArray) {
//                analysis.arrayPaths.add(prefix);
//                analysis.arrayFields.put(prefix, suffixes);
//                if (detectedSize > 0) {
//                    analysis.arraySizes.put(prefix, detectedSize);
//                }
//            } else if (suffixes.size() > 0) {
//                analysis.objectPaths.add(prefix);
//            }
//        }
//
//        return analysis;
//    }
//
//    /**
//     * Check if a string looks like a serialized array.
//     */
//    private boolean looksLikeSerializedArray(String value) {
//        if (value == null || value.isEmpty()) {
//            return false;
//        }
//
//        String trimmed = value.trim();
//
//        switch (arrayFormat) {
//            case JSON:
//                return trimmed.startsWith("[") && trimmed.endsWith("]");
//            case BRACKET_LIST:
//                return trimmed.startsWith("[") && trimmed.endsWith("]");
//            case COMMA_SEPARATED:
//                return trimmed.contains(",");
//            case PIPE_SEPARATED:
//                return trimmed.contains("|");
//            default:
//                return trimmed.startsWith("[") && trimmed.endsWith("]");
//        }
//    }
//
//    /**
//     * Parse a serialized array value.
//     */
//    private List<Object> parseArrayValue(Object value) {
//        if (value == null) {
//            return null;
//        }
//
//        if (value instanceof List) {
//            return (List<Object>) value;
//        }
//
//        if (!(value instanceof String)) {
//            return Collections.singletonList(value);
//        }
//
//        String strValue = ((String) value).trim();
//
//        // Try JSON first
//        if (strValue.startsWith("[") && strValue.endsWith("]")) {
//            try {
//                return objectMapper.readValue(strValue, LIST_TYPE_REF);
//            } catch (Exception e) {
//                // Fall through to other formats
//            }
//        }
//
//        // Try format-specific parsing
//        switch (arrayFormat) {
//            case BRACKET_LIST:
//                return parseBracketList(strValue);
//
//            case COMMA_SEPARATED:
//                if (strValue.contains(",")) {
//                    return splitRespectingBrackets(strValue, ',');
//                }
//                break;
//
//            case PIPE_SEPARATED:
//                if (strValue.contains("|")) {
//                    return splitRespectingBrackets(strValue, '|');
//                }
//                break;
//
//            case JSON:
//            default:
//                // Already tried JSON above
//                break;
//        }
//
//        return Collections.singletonList(value);
//    }
//
//    /**
//     * Parse bracket list format: [a, b, c]
//     */
//    private List<Object> parseBracketList(String value) {
//        if (value == null) return null;
//
//        String trimmed = value.trim();
//        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
//            String inner = trimmed.substring(1, trimmed.length() - 1).trim();
//            if (inner.isEmpty()) {
//                return new ArrayList<>();
//            }
//            return splitRespectingBrackets(inner, ',');
//        }
//
//        return Collections.singletonList(value);
//    }
//
//    /**
//     * Split string by delimiter while respecting bracket nesting and quotes.
//     */
//    private List<Object> splitRespectingBrackets(String str, char delimiter) {
//        List<Object> result = new ArrayList<>();
//        StringBuilder current = new StringBuilder();
//        int bracketDepth = 0;
//        boolean inQuotes = false;
//
//        for (int i = 0; i < str.length(); i++) {
//            char c = str.charAt(i);
//
//            if (c == '"' && (i == 0 || str.charAt(i - 1) != '\\')) {
//                inQuotes = !inQuotes;
//                current.append(c);
//            } else if (!inQuotes) {
//                if (c == '[' || c == '{') {
//                    bracketDepth++;
//                    current.append(c);
//                } else if (c == ']' || c == '}') {
//                    bracketDepth--;
//                    current.append(c);
//                } else if (c == delimiter && bracketDepth == 0) {
//                    result.add(parseAtomicValue(current.toString().trim()));
//                    current = new StringBuilder();
//                } else {
//                    current.append(c);
//                }
//            } else {
//                current.append(c);
//            }
//        }
//
//        // Add the last part
//        if (current.length() > 0) {
//            result.add(parseAtomicValue(current.toString().trim()));
//        }
//
//        return result;
//    }
//
//    /**
//     * Parse an atomic value, attempting type inference.
//     */
//    private Object parseAtomicValue(String value) {
//        if (value == null || value.isEmpty()) {
//            return null;
//        }
//
//        // Remove surrounding quotes
//        if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
//            return value.substring(1, value.length() - 1);
//        }
//
//        // Check for null
//        if ("null".equalsIgnoreCase(value)) {
//            return null;
//        }
//
//        // Check for boolean
//        if ("true".equalsIgnoreCase(value)) {
//            return Boolean.TRUE;
//        }
//        if ("false".equalsIgnoreCase(value)) {
//            return Boolean.FALSE;
//        }
//
//        // Check for numbers
//        try {
//            if (value.contains(".") || value.contains("e") || value.contains("E")) {
//                return Double.parseDouble(value);
//            } else {
//                long longVal = Long.parseLong(value);
//                if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
//                    return (int) longVal;
//                }
//                return longVal;
//            }
//        } catch (NumberFormatException e) {
//            // Not a number, return as string
//        }
//
//        return value;
//    }
//
//    // ========================= HIERARCHY BUILDING =========================
//
//    /**
//     * Build the hierarchical structure from flattened map.
//     */
//    private Map<String, Object> buildHierarchy(Map<String, Object> flattenedMap,
//                                               StructureAnalysis analysis) {
//        Map<String, Object> root = new LinkedHashMap<>();
//
//        for (Map.Entry<String, Object> entry : flattenedMap.entrySet()) {
//            String key = entry.getKey();
//            Object value = entry.getValue();
//
//            String[] parts = key.split(Pattern.quote(separator));
//            setNestedValue(root, parts, value, analysis);
//        }
//
//        return root;
//    }
//
//    /**
//     * Set a value at a nested path, creating intermediate structures as needed.
//     */
//    @SuppressWarnings("unchecked")
//    private void setNestedValue(Map<String, Object> root, String[] parts, Object value,
//                                StructureAnalysis analysis) {
//        Map<String, Object> current = root;
//
//        for (int i = 0; i < parts.length - 1; i++) {
//            String part = parts[i];
//            String currentPath = String.join(separator, Arrays.copyOfRange(parts, 0, i + 1));
//
//            Object existing = current.get(part);
//
//            if (existing == null) {
//                // Check if this path is an array
//                if (analysis.arrayPaths.contains(currentPath)) {
//                    // Create array holder (will be processed later)
//                    Map<String, Object> arrayHolder = new LinkedHashMap<>();
//                    arrayHolder.put("__isArray__", true);
//                    arrayHolder.put("__arrayPath__", currentPath);
//                    current.put(part, arrayHolder);
//                    current = arrayHolder;
//                } else {
//                    // Create nested object
//                    Map<String, Object> nested = new LinkedHashMap<>();
//                    current.put(part, nested);
//                    current = nested;
//                }
//            } else if (existing instanceof Map) {
//                current = (Map<String, Object>) existing;
//            } else {
//                // Value exists but is not a map - convert to map with _value sentinel
//                Map<String, Object> wrapper = new LinkedHashMap<>();
//                wrapper.put("_value", existing);
//                current.put(part, wrapper);
//                current = wrapper;
//            }
//        }
//
//        // Set the leaf value
//        String leafKey = parts[parts.length - 1];
//        current.put(leafKey, value);
//    }
//
//    // ========================= ARRAY PROCESSING =========================
//
//    /**
//     * Post-process the hierarchy to expand arrays from serialized values.
//     */
//    @SuppressWarnings("unchecked")
//    private Map<String, Object> processArrays(Map<String, Object> map,
//                                              StructureAnalysis analysis,
//                                              String currentPath) {
//        Map<String, Object> result = new LinkedHashMap<>();
//
//        for (Map.Entry<String, Object> entry : map.entrySet()) {
//            String key = entry.getKey();
//            Object value = entry.getValue();
//            String path = currentPath.isEmpty() ? key : currentPath + separator + key;
//
//            // Skip internal markers
//            if (key.startsWith("__") && key.endsWith("__")) {
//                continue;
//            }
//
//            if (value instanceof Map) {
//                Map<String, Object> mapValue = (Map<String, Object>) value;
//
//                // Check if this is an array holder
//                if (Boolean.TRUE.equals(mapValue.get("__isArray__"))) {
//                    // Reconstruct as array
//                    List<Object> array = reconstructArray(mapValue, analysis, path);
//                    result.put(key, array);
//                } else {
//                    // Recursively process nested map
//                    result.put(key, processArrays(mapValue, analysis, path));
//                }
//            } else if (analysis.arrayPaths.contains(path) && value instanceof String) {
//                // This is a leaf that should be an array
//                List<Object> parsed = parseArrayValue(value);
//                result.put(key, parsed != null ? parsed : value);
//            } else {
//                // Regular value
//                if (value != null || preserveNulls) {
//                    result.put(key, value);
//                }
//            }
//        }
//
//        return result;
//    }
//
//    /**
//     * Reconstruct an array from its flattened representation.
//     */
//    @SuppressWarnings("unchecked")
//    private List<Object> reconstructArray(Map<String, Object> arrayHolder,
//                                          StructureAnalysis analysis,
//                                          String arrayPath) {
//        // Remove markers
//        Map<String, Object> fields = new LinkedHashMap<>(arrayHolder);
//        fields.remove("__isArray__");
//        fields.remove("__arrayPath__");
//
//        if (fields.isEmpty()) {
//            return new ArrayList<>();
//        }
//
//        // Parse all field values
//        Map<String, List<Object>> parsedFields = new LinkedHashMap<>();
//        int maxSize = 0;
//
//        for (Map.Entry<String, Object> entry : fields.entrySet()) {
//            String fieldName = entry.getKey();
//            Object value = entry.getValue();
//
//            List<Object> parsedValues;
//            if (value instanceof Map) {
//                // Nested object - keep as single element
//                parsedValues = Collections.singletonList(value);
//            } else {
//                parsedValues = parseArrayValue(value);
//                if (parsedValues == null) {
//                    parsedValues = Collections.singletonList(value);
//                }
//            }
//
//            parsedFields.put(fieldName, parsedValues);
//            maxSize = Math.max(maxSize, parsedValues.size());
//        }
//
//        // Use detected size if available
//        Integer detectedSize = analysis.arraySizes.get(arrayPath);
//        if (detectedSize != null && detectedSize > maxSize) {
//            maxSize = detectedSize;
//        }
//
//        // Build array elements
//        List<Object> result = new ArrayList<>(maxSize);
//
//        for (int i = 0; i < maxSize; i++) {
//            Map<String, Object> element = new LinkedHashMap<>();
//
//            for (Map.Entry<String, List<Object>> entry : parsedFields.entrySet()) {
//                String fieldName = entry.getKey();
//                List<Object> values = entry.getValue();
//
//                Object valueAtIndex;
//                if (i < values.size()) {
//                    valueAtIndex = values.get(i);
//                } else if (!values.isEmpty()) {
//                    // Use last value for shorter arrays (asymmetric arrays)
//                    valueAtIndex = values.get(values.size() - 1);
//                } else {
//                    valueAtIndex = null;
//                }
//
//                // Process nested structures
//                if (valueAtIndex instanceof Map) {
//                    valueAtIndex = processArrays((Map<String, Object>) valueAtIndex,
//                            analysis, arrayPath + separator + fieldName);
//                }
//
//                if (valueAtIndex != null || preserveNulls) {
//                    element.put(fieldName, valueAtIndex);
//                }
//            }
//
//            result.add(element);
//        }
//
//        return result;
//    }
//
//    // ========================= FLUENT API =========================
//
//    /**
//     * Fluent API for reconstruction operations.
//     */
//    public static class FluentOperation {
//        private final JsonReconstructor reconstructor;
//        private Map<String, Object> currentData;
//        private List<Function<Map<String, Object>, Map<String, Object>>> transformers = new ArrayList<>();
//
//        private FluentOperation(JsonReconstructor reconstructor) {
//            this.reconstructor = reconstructor;
//        }
//
//        /**
//         * Load from flattened Map.
//         */
//        public FluentOperation from(Map<String, Object> flattenedMap) {
//            this.currentData = reconstructor.reconstruct(flattenedMap);
//            return this;
//        }
//
//        /**
//         * Load from flattened JSON string.
//         */
//        public FluentOperation fromJson(String flattenedJson) {
//            try {
//                Map<String, Object> flattenedMap = SHARED_MAPPER.readValue(flattenedJson, MAP_TYPE_REF);
//                return from(flattenedMap);
//            } catch (JsonProcessingException e) {
//                throw new ReconstructionException("Failed to parse JSON", e);
//            }
//        }
//
//        /**
//         * Load from file containing flattened JSON.
//         */
//        public FluentOperation fromFile(Path path) {
//            try {
//                String content = Files.readString(path, DEFAULT_CHARSET);
//                return fromJson(content);
//            } catch (IOException e) {
//                throw new ReconstructionException("Failed to read file: " + path, e);
//            }
//        }
//
//        /**
//         * Load from file.
//         */
//        public FluentOperation fromFile(File file) {
//            return fromFile(file.toPath());
//        }
//
//        /**
//         * Apply a transformation.
//         */
//        public FluentOperation transform(Function<Map<String, Object>, Map<String, Object>> transformer) {
//            if (transformer != null) {
//                transformers.add(transformer);
//            }
//            return this;
//        }
//
//        /**
//         * Get result as Map.
//         */
//        public Map<String, Object> toMap() {
//            if (currentData == null) {
//                throw new IllegalStateException("No input data loaded. Call from() first.");
//            }
//
//            Map<String, Object> result = new LinkedHashMap<>(currentData);
//            for (Function<Map<String, Object>, Map<String, Object>> transformer : transformers) {
//                result = transformer.apply(result);
//            }
//            return result;
//        }
//
//        /**
//         * Get result as JSON string.
//         */
//        public String toJson() {
//            try {
//                return SHARED_MAPPER.writeValueAsString(toMap());
//            } catch (JsonProcessingException e) {
//                throw new ReconstructionException("Failed to serialize result", e);
//            }
//        }
//
//        /**
//         * Get result as pretty JSON string.
//         */
//        public String toPrettyJson() {
//            try {
//                return SHARED_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(toMap());
//            } catch (JsonProcessingException e) {
//                throw new ReconstructionException("Failed to serialize result", e);
//            }
//        }
//
//        /**
//         * Write result to file.
//         */
//        public void toFile(Path path) {
//            try {
//                Files.writeString(path, toPrettyJson(), DEFAULT_CHARSET);
//            } catch (IOException e) {
//                throw new ReconstructionException("Failed to write file: " + path, e);
//            }
//        }
//
//        /**
//         * Write result to file.
//         */
//        public void toFile(File file) {
//            toFile(file.toPath());
//        }
//    }
//
//    // ========================= VERIFICATION =========================
//
//    /**
//     * Verification result for reconstruction.
//     */
//    public static class ReconstructionVerification {
//        private final boolean isPerfect;
//        private final List<String> differences;
//        private final Map<String, Object> originalData;
//        private final Map<String, Object> reconstructedData;
//        private final long verificationTimeMs;
//
//        ReconstructionVerification(boolean isPerfect, List<String> differences,
//                                   Map<String, Object> originalData,
//                                   Map<String, Object> reconstructedData,
//                                   long verificationTimeMs) {
//            this.isPerfect = isPerfect;
//            this.differences = Collections.unmodifiableList(differences);
//            this.originalData = originalData;
//            this.reconstructedData = reconstructedData;
//            this.verificationTimeMs = verificationTimeMs;
//        }
//
//        public boolean isPerfect() { return isPerfect; }
//        public List<String> getDifferences() { return differences; }
//        public Map<String, Object> getOriginalData() { return originalData; }
//        public Map<String, Object> getReconstructedData() { return reconstructedData; }
//        public long getVerificationTimeMs() { return verificationTimeMs; }
//
//        public String getReport() {
//            StringBuilder sb = new StringBuilder();
//            sb.append("=== Reconstruction Verification Report ===\n");
//            sb.append("Status: ").append(isPerfect ? "PERFECT ✓" : "DIFFERENCES FOUND ✗").append("\n");
//            sb.append("Verification Time: ").append(verificationTimeMs).append("ms\n");
//
//            if (!isPerfect) {
//                sb.append("\nDifferences (").append(differences.size()).append(" total):\n");
//                for (int i = 0; i < Math.min(differences.size(), 20); i++) {
//                    sb.append("  ").append(i + 1).append(". ").append(differences.get(i)).append("\n");
//                }
//                if (differences.size() > 20) {
//                    sb.append("  ... and ").append(differences.size() - 20).append(" more\n");
//                }
//            } else {
//                sb.append("\nAll fields match perfectly!\n");
//            }
//
//            return sb.toString();
//        }
//
//        @Override
//        public String toString() {
//            return getReport();
//        }
//    }
//
//    /**
//     * Verify reconstruction against original data.
//     *
//     * @param originalData The original hierarchical data
//     * @param reconstructedData The reconstructed data
//     * @return Verification result
//     */
//    public ReconstructionVerification verify(Map<String, Object> originalData,
//                                             Map<String, Object> reconstructedData) {
//        long startTime = System.currentTimeMillis();
//        List<String> differences = new ArrayList<>();
//
//        compareStructures(originalData, reconstructedData, "", differences);
//
//        long elapsed = System.currentTimeMillis() - startTime;
//        boolean isPerfect = differences.isEmpty();
//
//        return new ReconstructionVerification(
//                isPerfect, differences, originalData, reconstructedData, elapsed);
//    }
//
//    /**
//     * Full round-trip verification: flatten -> reconstruct -> compare.
//     *
//     * @param originalData Original hierarchical data
//     * @param flattener The MapFlattener used for flattening
//     * @return Verification result
//     */
//    public ReconstructionVerification verifyRoundTrip(Map<String, Object> originalData,
//                                                      MapFlattener flattener) {
//        Map<String, Object> flattened = flattener.flatten(originalData);
//        Map<String, Object> reconstructed = reconstruct(flattened);
//        return verify(originalData, reconstructed);
//    }
//
//    /**
//     * Deep comparison of structures.
//     */
//    @SuppressWarnings("unchecked")
//    private void compareStructures(Object original, Object reconstructed,
//                                   String path, List<String> differences) {
//        // Both null
//        if (original == null && reconstructed == null) {
//            return;
//        }
//
//        // One null, one not
//        if (original == null || reconstructed == null) {
//            differences.add(String.format("Path '%s': null mismatch (original=%s, reconstructed=%s)",
//                    path, formatValue(original), formatValue(reconstructed)));
//            return;
//        }
//
//        // Maps
//        if (original instanceof Map && reconstructed instanceof Map) {
//            compareMaps((Map<String, Object>) original, (Map<String, Object>) reconstructed,
//                    path, differences);
//            return;
//        }
//
//        // Lists
//        if (original instanceof List && reconstructed instanceof List) {
//            compareLists((List<Object>) original, (List<Object>) reconstructed, path, differences);
//            return;
//        }
//
//        // Type mismatch
//        if (!compatibleTypes(original, reconstructed)) {
//            differences.add(String.format("Path '%s': type mismatch (original=%s [%s], reconstructed=%s [%s])",
//                    path, formatValue(original), original.getClass().getSimpleName(),
//                    formatValue(reconstructed), reconstructed.getClass().getSimpleName()));
//            return;
//        }
//
//        // Value comparison
//        if (!valuesEqual(original, reconstructed)) {
//            differences.add(String.format("Path '%s': value mismatch (original=%s, reconstructed=%s)",
//                    path, formatValue(original), formatValue(reconstructed)));
//        }
//    }
//
//    private void compareMaps(Map<String, Object> original, Map<String, Object> reconstructed,
//                             String path, List<String> differences) {
//        Set<String> allKeys = new LinkedHashSet<>();
//        allKeys.addAll(original.keySet());
//        allKeys.addAll(reconstructed.keySet());
//
//        for (String key : allKeys) {
//            String keyPath = path.isEmpty() ? key : path + "." + key;
//            Object origValue = original.get(key);
//            Object reconValue = reconstructed.get(key);
//
//            compareStructures(origValue, reconValue, keyPath, differences);
//        }
//    }
//
//    private void compareLists(List<Object> original, List<Object> reconstructed,
//                              String path, List<String> differences) {
//        if (original.size() != reconstructed.size()) {
//            differences.add(String.format("Path '%s': array size mismatch (original=%d, reconstructed=%d)",
//                    path, original.size(), reconstructed.size()));
//            // Continue comparing up to the smaller size
//        }
//
//        int minSize = Math.min(original.size(), reconstructed.size());
//        for (int i = 0; i < minSize; i++) {
//            String indexPath = path + "[" + i + "]";
//            compareStructures(original.get(i), reconstructed.get(i), indexPath, differences);
//        }
//    }
//
//    private boolean compatibleTypes(Object a, Object b) {
//        if (a.getClass().equals(b.getClass())) {
//            return true;
//        }
//
//        // Number compatibility
//        if (a instanceof Number && b instanceof Number) {
//            return true;
//        }
//
//        // Map compatibility
//        if (a instanceof Map && b instanceof Map) {
//            return true;
//        }
//
//        // List compatibility
//        if (a instanceof List && b instanceof List) {
//            return true;
//        }
//
//        // String to number comparison (common after parsing)
//        if ((a instanceof String && b instanceof Number) ||
//                (a instanceof Number && b instanceof String)) {
//            return true;
//        }
//
//        return false;
//    }
//
//    private boolean valuesEqual(Object a, Object b) {
//        // Numbers
//        if (a instanceof Number && b instanceof Number) {
//            return compareNumbers((Number) a, (Number) b);
//        }
//
//        // String to number
//        if (a instanceof String && b instanceof Number) {
//            try {
//                return compareNumbers(Double.parseDouble((String) a), (Number) b);
//            } catch (NumberFormatException e) {
//                return false;
//            }
//        }
//        if (a instanceof Number && b instanceof String) {
//            try {
//                return compareNumbers((Number) a, Double.parseDouble((String) b));
//            } catch (NumberFormatException e) {
//                return false;
//            }
//        }
//
//        return Objects.equals(a, b);
//    }
//
//    private boolean compareNumbers(Number a, Number b) {
//        if (a instanceof Double || b instanceof Double ||
//                a instanceof Float || b instanceof Float) {
//            return Math.abs(a.doubleValue() - b.doubleValue()) < 0.000001;
//        }
//        return a.longValue() == b.longValue();
//    }
//
//    private String formatValue(Object value) {
//        if (value == null) {
//            return "null";
//        }
//        if (value instanceof String) {
//            String str = (String) value;
//            if (str.length() > 50) {
//                return "\"" + str.substring(0, 47) + "...\"";
//            }
//            return "\"" + str + "\"";
//        }
//        if (value instanceof List) {
//            return "[" + ((List<?>) value).size() + " items]";
//        }
//        if (value instanceof Map) {
//            return "{" + ((Map<?, ?>) value).size() + " fields}";
//        }
//        return value.toString();
//    }
//
//    // ========================= STATIC CONVENIENCE METHODS =========================
//
//    /**
//     * Quick reconstruct (convenience method).
//     */
//    public static Map<String, Object> quickReconstruct(Map<String, Object> flattenedMap) {
//        return builder().build().reconstruct(flattenedMap);
//    }
//
//    /**
//     * Quick reconstruct with custom separator.
//     */
//    public static Map<String, Object> quickReconstruct(Map<String, Object> flattenedMap,
//                                                       String separator) {
//        return builder().separator(separator).build().reconstruct(flattenedMap);
//    }
//
//    /**
//     * Quick reconstruct to JSON.
//     */
//    public static String quickReconstructToJson(Map<String, Object> flattenedMap) {
//        return builder().build().reconstructToJson(flattenedMap);
//    }
//
//    /**
//     * Quick reconstruct to pretty JSON.
//     */
//    public static String quickReconstructToPrettyJson(Map<String, Object> flattenedMap) {
//        return builder().build().reconstructToPrettyJson(flattenedMap);
//    }
//
//    // ========================= EXCEPTION =========================
//
//    /**
//     * Exception for reconstruction failures.
//     */
//    public static class ReconstructionException extends RuntimeException {
//        public ReconstructionException(String message) {
//            super(message);
//        }
//
//        public ReconstructionException(String message, Throwable cause) {
//            super(message, cause);
//        }
//    }
//}