package io.github.pierce;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import io.github.pierce.path.FlattenedPath;

/**
 * Production-grade JSON Reconstructor - Converts flattened Maps back to hierarchical JSON.
 *
 * <p>This class reconstructs nested JSON structures from flattened key-value Maps produced by
 * {@link MapFlattener} or {@link JsonFlattener}. Unlike AvroReconstructor, this class does NOT
 * require a schema - it infers the structure from the flattened keys themselves.</p>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li>Schema-less reconstruction from flattened key patterns</li>
 *   <li>Automatic detection of arrays vs nested objects</li>
 *   <li>Support for all array serialization formats (JSON, comma, pipe, bracket)</li>
 *   <li>Deep reconstruction of nested arrays and objects</li>
 *   <li>Comprehensive verification utilities</li>
 *   <li>Thread-safe and production-ready</li>
 *   <li>Fluent API with builder pattern</li>
 *   <li>Detailed error reporting with path information</li>
 * </ul>
 *
 * <h2>Basic Usage:</h2>
 * <pre>
 * // 1. Flatten original data
 * MapFlattener flattener = MapFlattener.builder().build();
 * Map&lt;String, Object&gt; flattened = flattener.flatten(originalData);
 *
 * // 2. Reconstruct from flattened data
 * JsonReconstructor reconstructor = JsonReconstructor.builder().build();
 * Map&lt;String, Object&gt; reconstructed = reconstructor.reconstruct(flattened);
 *
 * // 3. Verify reconstruction
 * ReconstructionVerification verification =
 *     reconstructor.verify(originalData, reconstructed);
 *
 * if (verification.isPerfect()) {
 *     System.out.println("Perfect reconstruction!");
 * } else {
 *     verification.getDifferences().forEach(System.out::println);
 * }
 * </pre>
 *
 * <h2>With Custom Configuration:</h2>
 * <pre>
 * JsonReconstructor reconstructor = JsonReconstructor.builder()
 *     .separator("__")                                    // Match MapFlattener separator
 *     .arrayFormat(ArraySerializationFormat.JSON)         // Match MapFlattener array format
 *     .inferArraysFromValues(true)                        // Auto-detect arrays from serialized values
 *     .preserveNulls(true)                                // Keep null values in output
 *     .build();
 *
 * Map&lt;String, Object&gt; reconstructed = reconstructor.reconstruct(flattened);
 * </pre>
 *
 * <h2>Fluent API:</h2>
 * <pre>
 * // Simple one-liner
 * Map&lt;String, Object&gt; result = JsonReconstructor.create()
 *     .from(flattenedMap)
 *     .toMap();
 *
 * // With transformation
 * String json = JsonReconstructor.create()
 *     .from(flattenedMap)
 *     .transform(map -&gt; { map.put("reconstructed", true); return map; })
 *     .toPrettyJson();
 *
 * // From JSON string
 * Map&lt;String, Object&gt; result = JsonReconstructor.create()
 *     .fromJson(flattenedJsonString)
 *     .toMap();
 * </pre>
 *
 * <h2>Array Handling:</h2>
 * <p>The reconstructor detects arrays from flattened patterns:</p>
 * <ul>
 *   <li>Multiple keys with same prefix but different field suffixes: {@code users_name, users_age}
 *       → Reconstructed as array of objects if values are serialized arrays</li>
 *   <li>Serialized array values: {@code ["Alice","Bob"]} → Parsed and distributed to array elements</li>
 *   <li>Explicit array hints can be provided via {@code arrayPaths()} configuration</li>
 * </ul>
 *
 * @author Pierce
 * @version 1.0
 * @see MapFlattener
 * @see JsonFlattener
 */
public class JsonReconstructor implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(JsonReconstructor.class);

    // ========================= CONSTANTS =========================

    private static final int DEFAULT_MAX_DEPTH = 100;
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    // Shared ObjectMapper
    private static final ObjectMapper SHARED_MAPPER = createConfiguredMapper();

    private static ObjectMapper createConfiguredMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, true);
        return mapper;
    }

    // Patterns for detection
    // Note: the '$' anchors were written as '\$' in the Groovy source, where a bare '$' inside a
    // double-quoted string starts a GString interpolation and must be escaped. Java has no such
    // escape and rejects '\$' outright, so the backslashes are dropped here.
    private static final Pattern JSON_ARRAY_PATTERN = Pattern.compile("^\\s*\\[.*\\]\\s*$", Pattern.DOTALL);
    private static final Pattern BRACKET_LIST_PATTERN = Pattern.compile("^\\s*\\[(.*)\\]\\s*$", Pattern.DOTALL);

    // Type references
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF =
            new TypeReference<Map<String, Object>>() {};
    private static final TypeReference<List<Object>> LIST_TYPE_REF =
            new TypeReference<List<Object>>() {};

    // ========================= CONFIGURATION =========================

    private final String separator;
    private final ArraySerializationFormat arrayFormat;
    private final boolean inferArraysFromValues;
    private final boolean preserveNulls;
    private final int maxDepth;
    private final Set<String> arrayPaths;
    private final ObjectMapper objectMapper;
    private final CollisionPolicy collisionPolicy;

    // ========================= ARRAY FORMAT ENUM =========================

    /**
     * Array serialization formats (must match MapFlattener)
     */
    public enum ArraySerializationFormat {
        /** JSON format: ["a","b","c"] */
        JSON,
        /** Comma-separated: a,b,c */
        COMMA_SEPARATED,
        /** Pipe-separated: a|b|c */
        PIPE_SEPARATED,
        /** Bracket list: [a, b, c] */
        BRACKET_LIST
    }

    // ========================= COLLISION POLICY ENUM =========================

    /**
     * How {@link #reconstruct(Map)} resolves a leaf-versus-branch key collision.
     *
     * <p>A collision is a key that is ALSO an intermediate path of a longer key -
     * {@code a} beside {@code a_b}, {@code data} beside {@code data_name},
     * {@code orders_ship} beside {@code orders_ship_city}. The two decode to
     * {@code ["a"]} and {@code ["a","b"]}: distinct segment lists, so the FLATTENED form is
     * injective and loses nothing. It is the reconstructed tree that cannot hold both, because a
     * JSON node is either a scalar or an object and never both.</p>
     *
     * <p>This is NOT the same thing as a field name that literally contains the separator.
     * {@code FlattenedPath} escapes those, so {@code a\_b} is one segment and never collides;
     * detection compares encoded keys against encoded intermediate paths precisely so the two
     * stay apart.</p>
     *
     * <p>Whichever member is chosen, the answer does not depend on the iteration order of the
     * map handed to {@code reconstruct}. Before 2.1.0 it did: the colliding writes raced, and
     * the same two entries produced {@code {"a":"2"}} in one order and
     * {@code {"a":{"_value":"2","b":"1"}}} in the other.</p>
     *
     * @since 2.1.0
     */
    public enum CollisionPolicy {
        /**
         * Refuse: throw {@link KeyCollisionException} naming the key, everything it shadows, and
         * the escaped form that would have disambiguated. The default, and the only member that
         * cannot lose data.
         */
        FAIL,

        /**
         * Keep the SHORT key's scalar and discard every longer key that shares it as a prefix.
         * Reproduces the outcome the pre-2.1.0 code reached when the branch happened to be
         * written first - deterministically this time, and logged at WARN.
         */
        PREFER_LEAF,

        /**
         * Keep the SUBTREE and discard the colliding short key. Deterministic and logged at WARN.
         * Note this changes the outcome for the majority of colliding documents relative to
         * pre-2.1.0 behaviour, which most often landed on the leaf.
         */
        PREFER_BRANCH
    }

    // ========================= CONSTRUCTORS =========================

    private JsonReconstructor(Builder builder) {
        this.separator = builder.separator;
        this.arrayFormat = builder.arrayFormat;
        this.inferArraysFromValues = builder.inferArraysFromValues;
        this.preserveNulls = builder.preserveNulls;
        this.maxDepth = builder.maxDepth;
        this.arrayPaths = builder.arrayPaths != null ?
                new HashSet<>(builder.arrayPaths) : new HashSet<>();
        this.objectMapper = builder.objectMapper != null ?
                builder.objectMapper : SHARED_MAPPER;
        this.collisionPolicy = builder.collisionPolicy != null ?
                builder.collisionPolicy : CollisionPolicy.FAIL;
    }

    // ========================= STATIC FACTORY METHODS =========================

    /**
     * Create a new builder for configuration.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a reconstructor with default configuration.
     */
    public static FluentOperation create() {
        return new FluentOperation(new JsonReconstructor(new Builder()));
    }

    /**
     * Create with custom separator (convenience method).
     */
    public static FluentOperation withSeparator(String separator) {
        return new FluentOperation(new JsonReconstructor(
                new Builder().separator(separator)));
    }

    /**
     * Create configured for useArrayBoundarySeparator mode.
     */
    public static FluentOperation withArrayBoundarySeparator() {
        return new FluentOperation(new JsonReconstructor(
                new Builder().separator("__")));
    }

    // ========================= BUILDER =========================

    /**
     * Builder for JsonReconstructor configuration.
     */
    public static class Builder {
        private String separator = "_";
        private ArraySerializationFormat arrayFormat = ArraySerializationFormat.JSON;
        private boolean inferArraysFromValues = true;
        private boolean preserveNulls = true;
        private int maxDepth = DEFAULT_MAX_DEPTH;
        private Set<String> arrayPaths = new HashSet<>();
        private ObjectMapper objectMapper = null;
        private CollisionPolicy collisionPolicy = CollisionPolicy.FAIL;

        /**
         * Set the separator used in flattened keys.
         * Must match the separator used by MapFlattener.
         *
         * @param separator The separator string (default: "_")
         */
        public Builder separator(String separator) {
            this.separator = separator != null ? separator : "_";
            return this;
        }

        /**
         * Enable array boundary separator mode (uses "__").
         * Shortcut for separator("__").
         */
        public Builder useArrayBoundarySeparator(boolean use) {
            this.separator = use ? "__" : "_";
            return this;
        }

        /**
         * Set the array serialization format.
         * Must match the format used by MapFlattener.
         *
         * @param format The array format (default: JSON)
         */
        public Builder arrayFormat(ArraySerializationFormat format) {
            this.arrayFormat = format != null ? format : ArraySerializationFormat.JSON;
            return this;
        }

        /**
         * Enable automatic array detection from serialized values.
         * When true, values like "[1,2,3]" are automatically detected as arrays.
         *
         * @param infer Whether to infer arrays from values (default: true)
         */
        public Builder inferArraysFromValues(boolean infer) {
            this.inferArraysFromValues = infer;
            return this;
        }

        /**
         * Preserve null values in reconstructed output.
         *
         * @param preserve Whether to preserve nulls (default: true)
         */
        public Builder preserveNulls(boolean preserve) {
            this.preserveNulls = preserve;
            return this;
        }

        /**
         * How to resolve a leaf-versus-branch key collision.
         *
         * <p>A flattened map can hold a key {@code a} beside a key {@code a_b}, which makes
         * {@code a} both a leaf and an intermediate node of the same tree. JSON has no node that
         * is simultaneously a scalar and an object, so one of the two cannot be represented. The
         * default refuses; see {@link CollisionPolicy}.</p>
         *
         * @param policy What to do on a collision (default: {@link CollisionPolicy#FAIL})
         * @return this builder
         * @since 2.1.0
         */
        public Builder onKeyCollision(CollisionPolicy policy) {
            this.collisionPolicy = policy != null ? policy : CollisionPolicy.FAIL;
            return this;
        }

        /**
         * Set maximum reconstruction depth.
         *
         * @param depth Maximum depth (default: 100)
         */
        public Builder maxDepth(int depth) {
            if (depth < 1) {
                throw new IllegalArgumentException("maxDepth must be >= 1");
            }
            this.maxDepth = depth;
            return this;
        }

        /**
         * Explicitly specify paths that should be treated as arrays.
         * Useful when auto-detection cannot determine array vs object.
         *
         * @param paths Paths that are arrays (e.g., "users", "items_tags")
         */
        public Builder arrayPaths(String... paths) {
            this.arrayPaths = new HashSet<>(Arrays.asList(paths));
            return this;
        }

        /**
         * Add an array path hint.
         *
         * @param path Path that should be treated as array
         */
        public Builder addArrayPath(String path) {
            this.arrayPaths.add(path);
            return this;
        }

        /**
         * Use custom ObjectMapper.
         *
         * @param mapper Custom ObjectMapper
         */
        public Builder objectMapper(ObjectMapper mapper) {
            this.objectMapper = mapper;
            return this;
        }

        /**
         * Build the reconstructor.
         */
        public JsonReconstructor build() {
            return new JsonReconstructor(this);
        }

        /**
         * Build and return fluent operation.
         */
        public FluentOperation buildFluent() {
            return new FluentOperation(build());
        }
    }

    // ========================= CORE RECONSTRUCTION =========================

    /**
     * Reconstruct a flattened Map back to hierarchical structure.
     *
     * <h4>Leaf-versus-branch key collisions</h4>
     *
     * <p>A flattened map may hold a key that is also an intermediate path of a longer key -
     * {@code a} beside {@code a_b}. The flattened form is not ambiguous ({@code ["a"]} and
     * {@code ["a","b"]} are different segment lists) but the reconstructed tree cannot hold both,
     * because a JSON node is either a scalar or an object. Such keys are found BEFORE any write,
     * as a set intersection, so the outcome never depends on the iteration order of
     * {@code flattenedMap}; {@link CollisionPolicy} then decides. The default refuses with
     * {@link KeyCollisionException}.</p>
     *
     * @param flattenedMap The flattened key-value map
     * @return Reconstructed hierarchical map
     * @throws KeyCollisionException under the default {@link CollisionPolicy#FAIL}, when a key is
     *                               also an intermediate path of a longer key
     */
    public Map<String, Object> reconstruct(Map<String, Object> flattenedMap) {
        if (flattenedMap == null || flattenedMap.isEmpty()) {
            return new LinkedHashMap<>();
        }

        try {
            // Step 1: Analyze the flattened keys to detect structure
            StructureAnalysis analysis = analyzeStructure(flattenedMap);

            // Step 1b: Decide the leaf-versus-branch collisions BEFORE anything is written. This
            // is a set intersection against paths the analysis pass already collected, so the
            // decision cannot depend on which key the map happens to yield first - which is
            // exactly what the previous shape got wrong.
            Set<String> collisions = collidingLeafKeys(flattenedMap, analysis);

            // Step 2: Build the hierarchical structure
            Map<String, Object> result = buildHierarchy(flattenedMap, analysis, collisions);

            // Step 3: Post-process arrays
            result = processArrays(result, analysis, "");

            return result;

        } catch (ReconstructionException alreadyDiagnosed) {
            // A typed refusal survives the wrapper. ArrayParseException names the column, the
            // configured format and the raw value; re-wrapping it as "Failed to reconstruct
            // flattened map" would replace a diagnosis with a shrug, and a caller who wants to
            // catch that one specific contradiction could no longer do it by type.
            throw alreadyDiagnosed;
        } catch (Exception e) {
            log.error("Reconstruction failed", e);
            throw new ReconstructionException("Failed to reconstruct flattened map", e);
        }
    }

    /**
     * Reconstruct from JSON string.
     */
    public Map<String, Object> reconstructFromJson(String flattenedJson) {
        try {
            Map<String, Object> flattenedMap = objectMapper.readValue(flattenedJson, MAP_TYPE_REF);
            return reconstruct(flattenedMap);
        } catch (JsonProcessingException e) {
            throw new ReconstructionException("Failed to parse flattened JSON", e);
        }
    }

    /**
     * Reconstruct and serialize to JSON.
     */
    public String reconstructToJson(Map<String, Object> flattenedMap) {
        Map<String, Object> reconstructed = reconstruct(flattenedMap);
        try {
            return objectMapper.writeValueAsString(reconstructed);
        } catch (JsonProcessingException e) {
            throw new ReconstructionException("Failed to serialize reconstructed map", e);
        }
    }

    /**
     * Reconstruct and serialize to pretty JSON.
     */
    public String reconstructToPrettyJson(Map<String, Object> flattenedMap) {
        Map<String, Object> reconstructed = reconstruct(flattenedMap);
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(reconstructed);
        } catch (JsonProcessingException e) {
            throw new ReconstructionException("Failed to serialize reconstructed map", e);
        }
    }

    // ========================= STRUCTURE ANALYSIS =========================

    /**
     * Analysis result containing detected structure information.
     */
    private static class StructureAnalysis {
        /** Paths that are detected as arrays */
        Set<String> arrayPaths = new HashSet<>();

        /** Paths that are detected as objects */
        Set<String> objectPaths = new HashSet<>();

        /** Map of array path to field names within the array elements */
        Map<String, Set<String>> arrayFields = new LinkedHashMap<>();

        /** Map of path to detected array size (from serialized values) */
        Map<String, Integer> arraySizes = new LinkedHashMap<>();

        /** All intermediate paths */
        Set<String> allPaths = new HashSet<>();
    }

    /**
     * Analyze the flattened keys to detect arrays and nested structures.
     */
    private StructureAnalysis analyzeStructure(Map<String, Object> flattenedMap) {
        StructureAnalysis analysis = new StructureAnalysis();

        // Add explicitly configured array paths
        analysis.arrayPaths.addAll(arrayPaths);

        // Group keys by their prefixes to detect potential arrays
        Map<String, Set<String>> prefixToSuffixes = new LinkedHashMap<>();
        Map<String, List<Object>> prefixToValues = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : flattenedMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            List<String> segments = FlattenedPath.decodeSegments(key, separator);
            String[] parts = segments.toArray(new String[0]);

            // Track all intermediate paths. This is no longer a write-only field: the collision
            // detector reads it, so the escaping below is load-bearing for CORRECTNESS and not
            // merely for a set nobody consulted. It is computed by one shared method so the
            // detector can never disagree with the analysis about what an intermediate path is.
            collectIntermediatePaths(segments, analysis.allPaths);

            // Group by all possible prefixes
            for (int i = 1; i < parts.length; i++) {
                String prefix = FlattenedPath.encode(Arrays.asList(Arrays.copyOfRange(parts, 0, i)), separator);
                String suffix = FlattenedPath.encode(Arrays.asList(Arrays.copyOfRange(parts, i, parts.length)), separator);

                prefixToSuffixes.computeIfAbsent(prefix, k -> new LinkedHashSet<>()).add(suffix);
                prefixToValues.computeIfAbsent(prefix, k -> new ArrayList<>()).add(value);
            }
        }

        // Detect arrays: paths where multiple fields exist AND values are serialized arrays
        for (Map.Entry<String, Set<String>> entry : prefixToSuffixes.entrySet()) {
            String prefix = entry.getKey();
            Set<String> suffixes = entry.getValue();
            List<Object> values = prefixToValues.get(prefix);

            // Check if this looks like an array
            boolean isArray = false;
            int detectedSize = 0;

            if (inferArraysFromValues) {
                // Check if any values are serialized arrays
                for (Object value : values) {
                    if (value instanceof String) {
                        String strValue = ((String) value).trim();
                        if (looksLikeSerializedArray(strValue)) {
                            List<Object> parsed = tryParseArrayValue(strValue);
                            if (parsed != null && parsed.size() > 1) {
                                isArray = true;
                                detectedSize = Math.max(detectedSize, parsed.size());
                            }
                        }
                    }
                }
            }

            // Also check explicit array paths
            if (arrayPaths.contains(prefix)) {
                isArray = true;
            }

            if (isArray) {
                analysis.arrayPaths.add(prefix);
                analysis.arrayFields.put(prefix, suffixes);
                if (detectedSize > 0) {
                    analysis.arraySizes.put(prefix, detectedSize);
                }
            } else if (suffixes.size() > 0) {
                analysis.objectPaths.add(prefix);
            }
        }

        return analysis;
    }

    /**
     * Check if a string looks like a serialized array.
     */
    private boolean looksLikeSerializedArray(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        String trimmed = value.trim();

        switch (arrayFormat) {
            case JSON:
                return trimmed.startsWith("[") && trimmed.endsWith("]");
            case BRACKET_LIST:
                return trimmed.startsWith("[") && trimmed.endsWith("]");
            case COMMA_SEPARATED:
                return trimmed.contains(",");
            case PIPE_SEPARATED:
                return trimmed.contains("|");
            default:
                return trimmed.startsWith("[") && trimmed.endsWith("]");
        }
    }

    /**
     * SPECULATIVE PROBE: is this value a serialized array?
     *
     * <p>Used only by structure inference, which asks this of every value in every prefix group.
     * "No" is the ordinary answer there, not a failure, so this never throws and never logs above
     * DEBUG. A document carrying a marker like {@code [CIRCULAR_REFERENCE]} must reconstruct
     * exactly as before.</p>
     *
     * <p>Implemented as a thin wrapper that CALLS the committed converter and catches its
     * refusal, rather than as a second copy of the cascade. If the two were separate
     * implementations they could drift - inference deciding a prefix is an array that the
     * converter then refuses - which would turn a silently-wrong output into a hard failure on
     * data that works today. Sharing the body makes that impossible by construction.</p>
     */
    private List<Object> tryParseArrayValue(Object value) {
        try {
            List<Object> parsed = parseArrayValue(value, null);
            return parsed == null ? Collections.emptyList() : parsed;
        } catch (ArrayParseException notAnArray) {
            if (log.isDebugEnabled()) {
                log.debug("Structure inference: value is bracketed but not parseable as an array");
            }
            // EMPTY, not null. The caller's test is `size() > 1`, so empty and null are already
            // indistinguishable to it, and this class maintains a gate - see
            // ReconstructorNeverReturnsNullListContractTest - that a list-returning path does not
            // hand back null.
            return Collections.emptyList();
        }
    }

    /**
     * COMMITTED CONVERTER: turn a serialized array value into its elements.
     *
     * <p>Callers of this method have already decided the path is an array - either the caller
     * named it in {@code arrayPaths()} or inference ruled the prefix an array from its sibling
     * columns. At that point "I could not parse it" is not an answer, it is a silent
     * substitution: the value used to come back as a ONE-ELEMENT list holding the raw unparsed
     * text, indistinguishable from a legitimate one-element array. In {@code reconstructArray} it
     * was worse than that - the single unparsed element was then REPLICATED into every element of
     * an N-element array by the last-value clamp, so one piece of garbage was presented as N
     * successfully parsed values.</p>
     *
     * @param columnPath the column being converted, for the message; null when called from the
     *                   probe, which discards the exception anyway
     * @throws ArrayParseException under {@code arrayFormat=JSON} when the value is bracketed but
     *                             is not parseable JSON
     */
    private List<Object> parseArrayValue(Object value, String columnPath) {
        if (value == null) {
            return null;
        }

        if (value instanceof List) {
            return (List<Object>) value;
        }

        if (!(value instanceof String)) {
            return Collections.singletonList(value);
        }

        String strValue = ((String) value).trim();

        // Try JSON first
        if (strValue.startsWith("[") && strValue.endsWith("]")) {
            try {
                return objectMapper.readValue(strValue, LIST_TYPE_REF);
            } catch (JsonProcessingException notJson) {
                // Under BRACKET_LIST, COMMA_SEPARATED and PIPE_SEPARATED this is a legitimate
                // try-this-then-that cascade and the switch below recovers, so a decline there is
                // the cascade working and deserves no complaint.
                //
                // Under the DEFAULT arrayFormat JSON it is not a cascade. The switch falls
                // straight through and the value used to be returned as a one-element list
                // holding the raw unparsed text.
                //
                // WHAT IS AND IS NOT TRUE ABOUT THE WRITER. MapFlattener's ARRAY writer,
                // serializeArray, emits parseable JSON - including for a single-element column -
                // so an array column it wrote will parse. MapFlattener as a whole does NOT only
                // emit parseable JSON: it writes the literal marker [CIRCULAR_REFERENCE] for a
                // cycle, and stringifyObject falls back to obj.toString() and then to
                // [OBJECT:SimpleName], any of which can be bracketed and unparseable. An earlier
                // draft of this message asserted the stronger claim and would have told a caller
                // holding MapFlattener's own circular marker that the value "did not come from
                // it" - so the message below names those three first and does not deny the
                // value's provenance.
                //
                // A WARN WAS THE OLD ANSWER AND IT WAS NOT ENOUGH. AvroReconstructor already
                // settled the same question in the mirror-image direction and wrote down why:
                // "a warning here would be read and ignored by exactly the caller it is aimed at
                // - a Spark job whose logs nobody tails - and the data would still be wrong."
                // One library must not give two different answers to the same misconfiguration.
                //
                // The speculative probe, tryParseArrayValue, catches this and returns an EMPTY
                // list - not null; see ReconstructorNeverReturnsNullListContractTest - so
                // structure inference is UNCHANGED and a document carrying a marker such as
                // [CIRCULAR_REFERENCE] still reconstructs exactly as before unless the caller has
                // named that column in arrayPaths().
                if (arrayFormat == ArraySerializationFormat.JSON) {
                    throw new ArrayParseException(
                            "Column " + (columnPath == null ? "(inferred)" : "'" + columnPath + "'")
                                    + " is bracketed but is not parseable JSON, and arrayFormat is "
                                    + arrayFormat + ". Check first whether this is one of the "
                                    + "bracketed non-JSON values MapFlattener itself writes: the "
                                    + "[CIRCULAR_REFERENCE] cycle marker, an object's toString(), "
                                    + "or [OBJECT:SimpleName] - none of those is an array, and a "
                                    + "column holding one should not be named in arrayPaths(). "
                                    + "Otherwise the column is not an array, or the configured "
                                    + "arrayFormat does not match the writer - MapFlattener's "
                                    + "array writer always emits parseable JSON. Raw value: "
                                    + strValue,
                            notJson);
                }
            }
        }

        // Try format-specific parsing
        switch (arrayFormat) {
            case BRACKET_LIST:
                return parseBracketList(strValue);

            case COMMA_SEPARATED:
                if (strValue.contains(",")) {
                    return splitRespectingBrackets(strValue, (char) ',');
                }
                break;

            case PIPE_SEPARATED:
                if (strValue.contains("|")) {
                    return splitRespectingBrackets(strValue, (char) '|');
                }
                break;

            case JSON:
            default:
                // Already tried JSON above
                break;
        }

        return Collections.singletonList(value);
    }

    /**
     * Parse bracket list format: [a, b, c]
     */
    private List<Object> parseBracketList(String value) {
        if (value == null) return null;

        String trimmed = value.trim();
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
            String inner = trimmed.substring(1, trimmed.length() - 1).trim();
            if (inner.isEmpty()) {
                return new ArrayList<>();
            }
            return splitRespectingBrackets(inner, (char) ',');
        }

        return Collections.singletonList(value);
    }

    /**
     * Split string by delimiter while respecting bracket nesting and quotes.
     */
    private List<Object> splitRespectingBrackets(String str, char delimiter) {
        List<Object> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int bracketDepth = 0;
        boolean inQuotes = false;

        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);

            if (c == '"' && (i == 0 || str.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (!inQuotes) {
                if (c == '[' || c == '{') {
                    bracketDepth++;
                    current.append(c);
                } else if (c == ']' || c == '}') {
                    bracketDepth--;
                    current.append(c);
                } else if (c == delimiter && bracketDepth == 0) {
                    result.add(parseAtomicValue(current.toString().trim()));
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            } else {
                current.append(c);
            }
        }

        // Add the last part
        if (current.length() > 0) {
            result.add(parseAtomicValue(current.toString().trim()));
        }

        return result;
    }

    /**
     * Parse an atomic value, attempting type inference.
     */
    private Object parseAtomicValue(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }

        // Remove surrounding quotes
        if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
            return value.substring(1, value.length() - 1);
        }

        // Check for null
        if ("null".equalsIgnoreCase(value)) {
            return null;
        }

        // Check for boolean
        if ("true".equalsIgnoreCase(value)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(value)) {
            return Boolean.FALSE;
        }

        // Check for numbers
        try {
            if (value.contains(".") || value.contains("e") || value.contains("E")) {
                return Double.parseDouble(value);
            } else {
                long longVal = Long.parseLong(value);
                if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
                    return (int) longVal;
                }
                return longVal;
            }
        } catch (NumberFormatException expected) {
            // THE ONE SITE OF THE 25 WHERE NOTHING SHOULD HAPPEN AT RUNTIME, and the only one
            // where the naming escape hatch is used.
            //
            // This is a type-inference PROBE over flattened text: the question is "does this text
            // parse as a number?", and NumberFormatException is the POSITIVE SIGNAL that the
            // answer is no. The next line acts on that answer. There is no failure here to
            // report, no cause to carry and no caller to warn - reporting it would produce a WARN
            // for every ordinary string in the document.
            //
            // The variable is named `expected` because PMD's EmptyCatchBlock exempts
            // ^(ignored|expected)$ via allowExceptionNameRegex. That is NOT a claim that the
            // exception is uninteresting - it is the result - and it is stated here so the next
            // reader does not copy the pattern to a site where something IS being swallowed.
            // Note also that a commented catch does not satisfy the rule as configured; the
            // comment you are reading resolves nothing on its own.
        }

        return value;
    }

    // ========================= LEAF/BRANCH COLLISION =========================

    /**
     * Every intermediate path of a decoded key, in the same escaped encoding as the keys.
     *
     * <p>Shared by {@link #analyzeStructure} and {@link #collidingLeafKeys} so a detector and the
     * analysis can never disagree about what an intermediate path is. The comparison that matters
     * is ENCODED key against ENCODED path: {@code a\_b} is a field literally named {@code a_b},
     * one segment, and must never be read as the intermediate {@code a}.</p>
     */
    private void collectIntermediatePaths(List<String> segments, Collection<String> sink) {
        StringBuilder pathBuilder = new StringBuilder();
        for (int i = 0; i < segments.size() - 1; i++) {
            if (pathBuilder.length() > 0) {
                pathBuilder.append(separator);
            }
            pathBuilder.append(FlattenedPath.escapeSegment(segments.get(i), separator));
            sink.add(pathBuilder.toString());
        }
    }

    /**
     * The list form, for the two collision paths that need to iterate them.
     *
     * <p>{@link #analyzeStructure} uses the sink form instead and adds straight into
     * {@code allPaths}: it runs once per key on every reconstruct, and a per-key list that is
     * immediately drained would be a new allocation on the hottest path in the class for no
     * benefit. The two callers here run only after a collision has already been found.</p>
     */
    private List<String> intermediatePaths(List<String> segments) {
        List<String> paths = new ArrayList<>(Math.max(1, segments.size() - 1));
        collectIntermediatePaths(segments, paths);
        return paths;
    }

    /**
     * The keys that are ALSO an intermediate path of some longer key, decided before any write.
     *
     * <p>A set intersection has no iteration order, which is the entire point: the previous shape
     * let whichever colliding key the map happened to yield LAST decide the outcome, so the same
     * two entries reconstructed differently depending on how the caller built the map.</p>
     *
     * <p>Returned sorted, so the exception message and the WARN line are reproducible too.</p>
     */
    private Set<String> collidingLeafKeys(Map<String, Object> flattenedMap,
                                          StructureAnalysis analysis) {
        Set<String> colliding = new TreeSet<>();
        for (String key : flattenedMap.keySet()) {
            if (key != null && analysis.allPaths.contains(key)) {
                colliding.add(key);
            }
        }
        if (colliding.isEmpty()) {
            return Collections.emptySet();
        }

        String first = colliding.iterator().next();
        List<String> shadowed = shadowedKeys(flattenedMap, first);

        if (collisionPolicy == CollisionPolicy.FAIL) {
            throw new KeyCollisionException(first, shadowed, separator);
        }
        if (log.isWarnEnabled()) {
            log.warn("Leaf/branch key collision resolved by policy {}: {} colliding key(s), "
                            + "discarding the {} side. This is lossy but deterministic; "
                            + "CollisionPolicy.FAIL refuses instead.",
                    collisionPolicy, colliding.size(),
                    collisionPolicy == CollisionPolicy.PREFER_LEAF ? "branch" : "leaf");
        }
        return colliding;
    }

    /** The longer keys that a colliding key shadows, sorted, for the diagnosis. */
    private List<String> shadowedKeys(Map<String, Object> flattenedMap, String collidingKey) {
        List<String> shadowed = new ArrayList<>();
        for (String key : flattenedMap.keySet()) {
            if (key == null || key.equals(collidingKey)) {
                continue;
            }
            if (intermediatePaths(FlattenedPath.decodeSegments(key, separator))
                    .contains(collidingKey)) {
                shadowed.add(key);
            }
        }
        Collections.sort(shadowed);
        return shadowed;
    }

    /** Whether any strict prefix of {@code key} is one of the colliding keys. */
    private boolean hasCollidingPrefix(List<String> segments, Set<String> collisions) {
        for (String path : intermediatePaths(segments)) {
            if (collisions.contains(path)) {
                return true;
            }
        }
        return false;
    }

    // ========================= HIERARCHY BUILDING =========================

    /**
     * Build the hierarchical structure from flattened map.
     *
     * @param collisions keys that are also intermediate paths, decided by
     *                   {@link #collidingLeafKeys} before this method runs. Empty for the
     *                   overwhelming majority of documents, in which case nothing here changes.
     */
    private Map<String, Object> buildHierarchy(Map<String, Object> flattenedMap,
                                               StructureAnalysis analysis,
                                               Set<String> collisions) {
        Map<String, Object> root = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : flattenedMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            List<String> segments = FlattenedPath.decodeSegments(key, separator);

            if (!collisions.isEmpty() && isDiscardedByPolicy(key, segments, collisions)) {
                continue;
            }

            setNestedValue(root, segments.toArray(new String[0]), value, analysis);
        }

        return root;
    }

    /**
     * Whether this key is the side the configured policy drops.
     *
     * <p>Only reachable when {@link #collidingLeafKeys} already returned a non-empty set, which
     * under {@link CollisionPolicy#FAIL} it never does - that member throws instead of
     * returning.</p>
     */
    private boolean isDiscardedByPolicy(String key, List<String> segments, Set<String> collisions) {
        if (collisionPolicy == CollisionPolicy.PREFER_LEAF) {
            return hasCollidingPrefix(segments, collisions);
        }
        return collisionPolicy == CollisionPolicy.PREFER_BRANCH && collisions.contains(key);
    }

    /**
     * Set a value at a nested path, creating intermediate structures as needed.
     *
     * <p>Two guards here are BACKSTOPS, not the primary detection. A non-Map found at an
     * intermediate segment, or a Map found where a leaf is about to be written, both mean the
     * same thing: a shorter key and a longer key are asking for the same node. For a CANONICALLY
     * encoded key set that is already decided by {@link #collidingLeafKeys} before this method
     * runs, so neither guard can fire.</p>
     *
     * <p>They exist for the case the set intersection cannot see. {@code reconstruct(Map)} is
     * public and takes any map, and a caller can hand it a NON-CANONICAL key - one whose own
     * encoding is not what {@code FlattenedPath} would have produced for its segments. The key
     * {@code a\b} decodes to the single segment {@code a\b} (a backslash escaping nothing is a
     * literal backslash) and re-encodes to {@code a\\b}, so it is not equal to the intermediate
     * path that {@code a\b_c} contributes and the intersection misses it. Both orders are
     * covered: the branch-then-leaf order trips the leaf guard, the leaf-then-branch order trips
     * the intermediate guard, and a caller gets the same exception type either way instead of
     * one silent overwrite and one fabricated key.</p>
     */
    @SuppressWarnings("unchecked")
    private void setNestedValue(Map<String, Object> root, String[] parts, Object value,
                                StructureAnalysis analysis) {
        Map<String, Object> current = root;

        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i];
            String currentPath = FlattenedPath.encode(Arrays.asList(Arrays.copyOfRange(parts, 0, i + 1)), separator);

            Object existing = current.get(part);

            if (existing == null) {
                // Check if this path is an array
                if (analysis.arrayPaths.contains(currentPath)) {
                    // Create array holder (will be processed later)
                    Map<String, Object> arrayHolder = new LinkedHashMap<>();
                    arrayHolder.put("__isArray__", true);
                    arrayHolder.put("__arrayPath__", currentPath);
                    current.put(part, arrayHolder);
                    current = arrayHolder;
                } else {
                    // Create nested object
                    Map<String, Object> nested = new LinkedHashMap<>();
                    current.put(part, nested);
                    current = nested;
                }
            } else if (existing instanceof Map) {
                current = (Map<String, Object>) existing;
            } else {
                // A scalar is already parked at an intermediate segment. Until 2.1.0 this branch
                // wrapped it as {"_value": existing, ...}: a key the source never carried, which
                // does not survive a re-flatten (the node re-encodes to a_\_value, not to a) and
                // which silently duelled with a genuine field named _value. It was reachable only
                // from a collision, and collisions are now decided in front of this walk, so the
                // wrapper is deleted rather than kept as a fallback - keeping it would have
                // preserved the exact nondeterminism the detection exists to remove.
                throw new KeyCollisionException(currentPath,
                        Collections.singletonList(FlattenedPath.encode(Arrays.asList(parts), separator)),
                        separator);
            }
        }

        // Set the leaf value. The guard is the mirror of the one in the loop above: a Map
        // already sitting where a leaf is about to be written was put there by a LONGER key, so
        // overwriting it is the collision seen from the other end. Without this, the two orders
        // are not symmetric - the loop refuses one and this line silently destroyed the other.
        String leafKey = parts[parts.length - 1];
        Object occupant = current.get(leafKey);
        if (occupant instanceof Map && !(value instanceof Map)) {
            String here = FlattenedPath.encode(Arrays.asList(parts), separator);
            List<String> shadowed = new ArrayList<>();
            for (Object child : ((Map<?, ?>) occupant).keySet()) {
                shadowed.add(here + separator
                        + FlattenedPath.escapeSegment(String.valueOf(child), separator));
            }
            throw new KeyCollisionException(here, shadowed, separator);
        }
        current.put(leafKey, value);
    }

    // ========================= ARRAY PROCESSING =========================

    /**
     * Post-process the hierarchy to expand arrays from serialized values.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> processArrays(Map<String, Object> map,
                                              StructureAnalysis analysis,
                                              String currentPath) {
        Map<String, Object> result = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String path = currentPath.isEmpty() ? key : currentPath + separator + key;

            // Skip internal markers
            if (key.startsWith("__") && key.endsWith("__")) {
                continue;
            }

            if (value instanceof Map) {
                Map<String, Object> mapValue = (Map<String, Object>) value;

                // Check if this is an array holder
                if (Boolean.TRUE.equals(mapValue.get("__isArray__"))) {
                    // Reconstruct as array
                    List<Object> array = reconstructArray(mapValue, analysis, path);
                    result.put(key, array);
                } else {
                    // Recursively process nested map
                    result.put(key, processArrays(mapValue, analysis, path));
                }
            } else if (analysis.arrayPaths.contains(path) && value instanceof String) {
                // This is a leaf that should be an array. The caller named this path in
                // arrayPaths(), so a refusal is an answer rather than a reason to substitute.
                List<Object> parsed = parseArrayValue(value, path);
                result.put(key, parsed);
            } else {
                // Regular value
                if (value != null || preserveNulls) {
                    result.put(key, value);
                }
            }
        }

        return result;
    }

    /**
     * Reconstruct an array from its flattened representation.
     */
    @SuppressWarnings("unchecked")
    private List<Object> reconstructArray(Map<String, Object> arrayHolder,
                                          StructureAnalysis analysis,
                                          String arrayPath) {
        // Remove markers
        Map<String, Object> fields = new LinkedHashMap<>(arrayHolder);
        fields.remove("__isArray__");
        fields.remove("__arrayPath__");

        if (fields.isEmpty()) {
            return new ArrayList<>();
        }

        // Parse all field values
        Map<String, List<Object>> parsedFields = new LinkedHashMap<>();
        int maxSize = 0;

        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();

            List<Object> parsedValues;
            if (value instanceof Map) {
                // Nested object - keep as single element
                parsedValues = Collections.singletonList(value);
            } else {
                // This fallback used to reconstruct EXACTLY what parseArrayValue already
                // returned - a one-element list holding the raw text - which the last-value
                // clamp below then replicated into every element of the array.
                // Named with the SEPARATOR, so the message quotes the key the caller actually put
                // in the flattened map rather than a structural path they never wrote.
                parsedValues = parseArrayValue(value, arrayPath + separator + fieldName);
                if (parsedValues == null) {
                    parsedValues = Collections.singletonList(value);
                }
            }

            parsedFields.put(fieldName, parsedValues);
            maxSize = Math.max(maxSize, parsedValues.size());
        }

        // Use detected size if available
        Integer detectedSize = analysis.arraySizes.get(arrayPath);
        if (detectedSize != null && detectedSize > maxSize) {
            maxSize = detectedSize;
        }

        // Build array elements
        List<Object> result = new ArrayList<>(maxSize);

        for (int i = 0; i < maxSize; i++) {
            Map<String, Object> element = new LinkedHashMap<>();

            for (Map.Entry<String, List<Object>> entry : parsedFields.entrySet()) {
                String fieldName = entry.getKey();
                List<Object> values = entry.getValue();

                Object valueAtIndex;
                if (i < values.size()) {
                    valueAtIndex = values.get(i);
                } else if (!values.isEmpty()) {
                    // Use last value for shorter arrays (asymmetric arrays)
                    valueAtIndex = values.get(values.size() - 1);
                } else {
                    valueAtIndex = null;
                }

                // Process nested structures
                if (valueAtIndex instanceof Map) {
                    valueAtIndex = processArrays((Map<String, Object>) valueAtIndex,
                            analysis,
                            arrayPath + separator + FlattenedPath.escapeSegment(fieldName, separator));
                }

                if (valueAtIndex != null || preserveNulls) {
                    element.put(fieldName, valueAtIndex);
                }
            }

            result.add(element);
        }

        return result;
    }

    // ========================= FLUENT API =========================

    /**
     * Fluent API for reconstruction operations.
     */
    public static class FluentOperation {
        private final JsonReconstructor reconstructor;
        private Map<String, Object> currentData;
        private List<Function<Map<String, Object>, Map<String, Object>>> transformers = new ArrayList<>();

        private FluentOperation(JsonReconstructor reconstructor) {
            this.reconstructor = reconstructor;
        }

        /**
         * Load from flattened Map.
         */
        public FluentOperation from(Map<String, Object> flattenedMap) {
            this.currentData = reconstructor.reconstruct(flattenedMap);
            return this;
        }

        /**
         * Load from flattened JSON string.
         */
        public FluentOperation fromJson(String flattenedJson) {
            try {
                Map<String, Object> flattenedMap = SHARED_MAPPER.readValue(flattenedJson, MAP_TYPE_REF);
                return from(flattenedMap);
            } catch (JsonProcessingException e) {
                throw new ReconstructionException("Failed to parse JSON", e);
            }
        }

        /**
         * Load from file containing flattened JSON.
         */
        public FluentOperation fromFile(Path path) {
            try {
                String content = Files.readString(path, DEFAULT_CHARSET);
                return fromJson(content);
            } catch (IOException e) {
                throw new ReconstructionException("Failed to read file: " + path, e);
            }
        }

        /**
         * Load from file.
         */
        public FluentOperation fromFile(File file) {
            return fromFile(file.toPath());
        }

        /**
         * Apply a transformation.
         */
        public FluentOperation transform(Function<Map<String, Object>, Map<String, Object>> transformer) {
            if (transformer != null) {
                transformers.add(transformer);
            }
            return this;
        }

        /**
         * Get result as Map.
         */
        public Map<String, Object> toMap() {
            if (currentData == null) {
                throw new IllegalStateException("No input data loaded. Call from() first.");
            }

            Map<String, Object> result = new LinkedHashMap<>(currentData);
            for (Function<Map<String, Object>, Map<String, Object>> transformer : transformers) {
                result = transformer.apply(result);
            }
            return result;
        }

        /**
         * Get result as JSON string.
         */
        public String toJson() {
            try {
                return SHARED_MAPPER.writeValueAsString(toMap());
            } catch (JsonProcessingException e) {
                throw new ReconstructionException("Failed to serialize result", e);
            }
        }

        /**
         * Get result as pretty JSON string.
         */
        public String toPrettyJson() {
            try {
                return SHARED_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(toMap());
            } catch (JsonProcessingException e) {
                throw new ReconstructionException("Failed to serialize result", e);
            }
        }

        /**
         * Write result to file.
         */
        public void toFile(Path path) {
            try {
                Files.writeString(path, toPrettyJson(), DEFAULT_CHARSET);
            } catch (IOException e) {
                throw new ReconstructionException("Failed to write file: " + path, e);
            }
        }

        /**
         * Write result to file.
         */
        public void toFile(File file) {
            toFile(file.toPath());
        }
    }

    // ========================= VERIFICATION =========================

    /**
     * Verification result for reconstruction.
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

        public boolean isPerfect() { return isPerfect; }
        public List<String> getDifferences() { return differences; }
        public Map<String, Object> getOriginalData() { return originalData; }
        public Map<String, Object> getReconstructedData() { return reconstructedData; }
        public long getVerificationTimeMs() { return verificationTimeMs; }

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
     * Verify reconstruction against original data.
     *
     * @param originalData The original hierarchical data
     * @param reconstructedData The reconstructed data
     * @return Verification result
     */
    public ReconstructionVerification verify(Map<String, Object> originalData,
                                             Map<String, Object> reconstructedData) {
        long startTime = System.currentTimeMillis();
        List<String> differences = new ArrayList<>();

        compareStructures(originalData, reconstructedData, "", differences);

        long elapsed = System.currentTimeMillis() - startTime;
        boolean isPerfect = differences.isEmpty();

        return new ReconstructionVerification(
                isPerfect, differences, originalData, reconstructedData, elapsed);
    }

    /**
     * Full round-trip verification: flatten -> reconstruct -> compare.
     *
     * <p>This is the other public entry point that {@link #reconstruct(Map)}'s failure contract
     * reaches. A document whose flattened form holds a leaf/branch key collision no longer comes
     * back with differences listed - it throws, because the reconstruction step refuses before
     * there is anything to compare. That is the intended shape: a verification that reported
     * "one difference" for a document with an entire subtree deleted was understating it.</p>
     *
     * @param originalData Original hierarchical data
     * @param flattener The MapFlattener used for flattening
     * @return Verification result
     * @throws KeyCollisionException under the default {@link CollisionPolicy#FAIL}, when the
     *                               flattened form holds a key that is also an intermediate path
     *                               of a longer key
     */
    public ReconstructionVerification verifyRoundTrip(Map<String, Object> originalData,
                                                      MapFlattener flattener) {
        Map<String, Object> flattened = flattener.flatten(originalData);
        Map<String, Object> reconstructed = reconstruct(flattened);
        return verify(originalData, reconstructed);
    }

    /**
     * Deep comparison of structures.
     */
    @SuppressWarnings("unchecked")
    private void compareStructures(Object original, Object reconstructed,
                                   String path, List<String> differences) {
        // Both null
        if (original == null && reconstructed == null) {
            return;
        }

        // One null, one not
        if (original == null || reconstructed == null) {
            differences.add(String.format("Path '%s': null mismatch (original=%s, reconstructed=%s)",
                    path, formatValue(original), formatValue(reconstructed)));
            return;
        }

        // Maps
        if (original instanceof Map && reconstructed instanceof Map) {
            compareMaps((Map<String, Object>) original, (Map<String, Object>) reconstructed,
                    path, differences);
            return;
        }

        // Lists
        if (original instanceof List && reconstructed instanceof List) {
            compareLists((List<Object>) original, (List<Object>) reconstructed, path, differences);
            return;
        }

        // Type mismatch
        if (!compatibleTypes(original, reconstructed)) {
            differences.add(String.format("Path '%s': type mismatch (original=%s [%s], reconstructed=%s [%s])",
                    path, formatValue(original), original.getClass().getSimpleName(),
                    formatValue(reconstructed), reconstructed.getClass().getSimpleName()));
            return;
        }

        // Value comparison
        if (!valuesEqual(original, reconstructed)) {
            differences.add(String.format("Path '%s': value mismatch (original=%s, reconstructed=%s)",
                    path, formatValue(original), formatValue(reconstructed)));
        }
    }

    private void compareMaps(Map<String, Object> original, Map<String, Object> reconstructed,
                             String path, List<String> differences) {
        Set<String> allKeys = new LinkedHashSet<>();
        allKeys.addAll(original.keySet());
        allKeys.addAll(reconstructed.keySet());

        for (String key : allKeys) {
            String keyPath = path.isEmpty() ? key : path + "." + key;
            Object origValue = original.get(key);
            Object reconValue = reconstructed.get(key);

            compareStructures(origValue, reconValue, keyPath, differences);
        }
    }

    private void compareLists(List<Object> original, List<Object> reconstructed,
                              String path, List<String> differences) {
        if (original.size() != reconstructed.size()) {
            differences.add(String.format("Path '%s': array size mismatch (original=%d, reconstructed=%d)",
                    path, original.size(), reconstructed.size()));
            // Continue comparing up to the smaller size
        }

        int minSize = Math.min(original.size(), reconstructed.size());
        for (int i = 0; i < minSize; i++) {
            String indexPath = path + "[" + i + "]";
            compareStructures(original.get(i), reconstructed.get(i), indexPath, differences);
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

        // Map compatibility
        if (a instanceof Map && b instanceof Map) {
            return true;
        }

        // List compatibility
        if (a instanceof List && b instanceof List) {
            return true;
        }

        // String to number comparison (common after parsing)
        if ((a instanceof String && b instanceof Number) ||
                (a instanceof Number && b instanceof String)) {
            return true;
        }

        return false;
    }

    private boolean valuesEqual(Object a, Object b) {
        // Numbers
        if (a instanceof Number && b instanceof Number) {
            return compareNumbers((Number) a, (Number) b);
        }

        // String to number
        if (a instanceof String && b instanceof Number) {
            try {
                return compareNumbers(Double.parseDouble((String) a), (Number) b);
            } catch (NumberFormatException e) {
                return false;
            }
        }
        if (a instanceof Number && b instanceof String) {
            try {
                return compareNumbers((Number) a, Double.parseDouble((String) b));
            } catch (NumberFormatException e) {
                return false;
            }
        }

        return Objects.equals(a, b);
    }

    private boolean compareNumbers(Number a, Number b) {
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
            String str = (String) value;
            if (str.length() > 50) {
                return "\"" + str.substring(0, 47) + "...\"";
            }
            return "\"" + str + "\"";
        }
        if (value instanceof List) {
            return "[" + ((List<?>) value).size() + " items]";
        }
        if (value instanceof Map) {
            return "{" + ((Map<?, ?>) value).size() + " fields}";
        }
        return value.toString();
    }

    // ========================= STATIC CONVENIENCE METHODS =========================

    /**
     * Quick reconstruct (convenience method).
     */
    public static Map<String, Object> quickReconstruct(Map<String, Object> flattenedMap) {
        return builder().build().reconstruct(flattenedMap);
    }

    /**
     * Quick reconstruct with custom separator.
     */
    public static Map<String, Object> quickReconstruct(Map<String, Object> flattenedMap,
                                                       String separator) {
        return builder().separator(separator).build().reconstruct(flattenedMap);
    }

    /**
     * Quick reconstruct to JSON.
     */
    public static String quickReconstructToJson(Map<String, Object> flattenedMap) {
        return builder().build().reconstructToJson(flattenedMap);
    }

    /**
     * Quick reconstruct to pretty JSON.
     */
    public static String quickReconstructToPrettyJson(Map<String, Object> flattenedMap) {
        return builder().build().reconstructToPrettyJson(flattenedMap);
    }

    // ========================= EXCEPTION =========================

    /**
     * A column that a caller or inference has already committed to being an array is bracketed
     * but is not parseable JSON, under {@code arrayFormat=JSON}.
     *
     * <p>Sibling of {@code AvroReconstructor.ArrayFormatMismatchException}, and thrown for the
     * same reason: a warning on this path would be read and ignored by exactly the caller it is
     * aimed at, and the data would still be wrong. Structure INFERENCE never raises this - there
     * a bracketed non-array is the ordinary answer to a speculative question.</p>
     *
     * @since 2.1.0
     */
    public static class ArrayParseException extends ReconstructionException {
        private static final long serialVersionUID = 1L;

        public ArrayParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * A key in the flattened map is ALSO an intermediate path of a longer key, so the two cannot
     * both be represented in the reconstructed tree.
     *
     * <p>{@code {"a":"2","a_b":"1"}} asks for a node at {@code a} that is simultaneously the
     * string {@code "2"} and the object {@code {"b":"1"}}. JSON has no such node. The flattened
     * form itself is fine - {@code ["a"]} and {@code ["a","b"]} are distinct segment lists and
     * {@code FlattenedPath} encodes them injectively - so nothing has been lost YET at the moment
     * this is thrown.</p>
     *
     * <p>Sibling of {@link ArrayParseException} and of
     * {@code MapFlattener.FlattenLimitExceededException}, and refused for their reason: the
     * alternatives all discard one side of the collision, and a caller who wanted that can ask
     * for it by name with {@link Builder#onKeyCollision(CollisionPolicy)}. Until 2.1.0 the choice
     * was made by map iteration order, with no log and no exception, and the two outcomes were
     * structurally different documents.</p>
     *
     * <p>Three producers of a colliding key set are known, and only one is a flattener sentinel:
     * {@code MapFlattener}'s base-key mapping for non-map array elements; an ordinary nullable
     * nested record inside an array of records ({@code orders_ship} beside
     * {@code orders_ship_city}); and the {@code LOWER_CASE} naming strategy's unescaped {@code _2}
     * dedup suffix. A caller-built flat map is a fourth.</p>
     *
     * @since 2.1.0
     */
    public static class KeyCollisionException extends ReconstructionException {
        private static final long serialVersionUID = 1L;

        private final String collidingKey;
        private final List<String> shadowedKeys;

        public KeyCollisionException(String collidingKey, List<String> shadowedKeys, String separator) {
            super(buildMessage(collidingKey, shadowedKeys, separator));
            this.collidingKey = collidingKey;
            this.shadowedKeys = Collections.unmodifiableList(new ArrayList<>(shadowedKeys));
        }

        private static String buildMessage(String collidingKey, List<String> shadowedKeys,
                                           String separator) {
            // The escaped form is shown for the LONGER keys, because that is where the caller's
            // fix lives: if a_b was meant as one field literally named a_b rather than as a
            // nesting of a under b, the encoder writes it a\_b and no collision exists.
            List<String> escaped = new ArrayList<>(shadowedKeys.size());
            for (String shadowed : shadowedKeys) {
                escaped.add(FlattenedPath.escapeSegment(shadowed, separator));
            }
            return "leaf/branch key collision: the key '" + collidingKey + "' is also an "
                    + "intermediate path of " + shadowedKeys + ", so the reconstructed node at '"
                    + collidingKey + "' would have to be a scalar and an object at once. "
                    + "If any of " + shadowedKeys + " was meant as a literal field name containing "
                    + "the separator '" + separator + "' rather than as a nesting level, it must "
                    + "be escaped - " + escaped + ". Otherwise choose which side to keep with "
                    + "JsonReconstructor.builder().onKeyCollision(PREFER_LEAF | PREFER_BRANCH).";
        }

        /** The key that is also an intermediate path. */
        public String getCollidingKey() {
            return collidingKey;
        }

        /** The longer keys it shadows, sorted. */
        public List<String> getShadowedKeys() {
            return shadowedKeys;
        }
    }

    /**
     * Exception for reconstruction failures.
     */
    public static class ReconstructionException extends RuntimeException {
        public ReconstructionException(String message) {
            super(message);
        }

        public ReconstructionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}