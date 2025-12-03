package io.github.pierce;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Production-grade JSON flattening utility with comprehensive input/output support.
 * <p>
 * JsonFlattener provides a fluent API for transforming hierarchical JSON structures
 * into flat key-value representations, with full support for streaming, batch processing,
 * validation, and multiple serialization formats.
 *
 * <h3>Key Features:</h3>
 * <ul>
 *   <li>Fluent API for intuitive chaining</li>
 *   <li>Multiple input sources: String, File, InputStream, Reader, byte[], Path, URL</li>
 *   <li>Multiple output targets: String, File, OutputStream, Writer, byte[], Path</li>
 *   <li>Streaming support for large JSON files (NDJSON/JSON Lines)</li>
 *   <li>Batch processing with parallel execution</li>
 *   <li>Schema validation and custom transformations</li>
 *   <li>Compression support (GZIP)</li>
 *   <li>Comprehensive error handling and reporting</li>
 *   <li>Thread-safe for concurrent use</li>
 * </ul>
 *
 * <h3>Basic Usage:</h3>
 * <pre>
 * // Simple JSON string flattening
 * String result = JsonFlattener.create()
 *     .from("{\"user\": {\"name\": \"John\", \"age\": 30}}")
 *     .toJson();
 * // Output: {"user_name":"John","user_age":30}
 *
 * // With custom MapFlattener configuration
 * MapFlattener flattener = MapFlattener.builder()
 *     .arrayFormat(ArraySerializationFormat.JSON)
 *     .maxDepth(100)
 *     .build();
 *
 * String result = JsonFlattener.with(flattener)
 *     .from(jsonString)
 *     .toJson(OutputOptions.pretty());
 * </pre>
 *
 * <h3>File Processing:</h3>
 * <pre>
 * // File to file
 * JsonFlattener.create()
 *     .from(new File("input.json"))
 *     .toFile(new File("output.json"));
 *
 * // With GZIP compression
 * JsonFlattener.create()
 *     .from(Path.of("input.json.gz"), InputOptions.gzipped())
 *     .toFile(Path.of("output.json.gz"), OutputOptions.gzipped());
 * </pre>
 *
 * <h3>Batch Processing:</h3>
 * <pre>
 * // Process multiple JSON objects
 * List&lt;String&gt; inputs = Arrays.asList(json1, json2, json3);
 * List&lt;Map&lt;String, Object&gt;&gt; results = JsonFlattener.create()
 *     .batch()
 *     .fromStrings(inputs)
 *     .toMaps();
 *
 * // Parallel processing for large batches
 * List&lt;String&gt; results = JsonFlattener.create()
 *     .batch()
 *     .parallel(8)  // 8 threads
 *     .fromStrings(largeList)
 *     .toJsonStrings();
 * </pre>
 *
 * <h3>Streaming (NDJSON/JSON Lines):</h3>
 * <pre>
 * // Process large NDJSON file line by line
 * JsonFlattener.create()
 *     .stream()
 *     .fromNdjsonFile(Path.of("large-file.ndjson"))
 *     .forEach(flattened -&gt; process(flattened));
 *
 * // Stream to output file
 * JsonFlattener.create()
 *     .stream()
 *     .fromNdjsonFile(inputPath)
 *     .toNdjsonFile(outputPath);
 * </pre>
 *
 * <h3>Validation and Transformation:</h3>
 * <pre>
 * // With validation
 * JsonFlattener.create()
 *     .from(jsonString)
 *     .validate(ValidationRules.builder()
 *         .requireFields("id", "timestamp")
 *         .maxKeyLength(100)
 *         .build())
 *     .toJson();
 *
 * // With post-processing transformation
 * JsonFlattener.create()
 *     .from(jsonString)
 *     .transform(map -&gt; {
 *         map.put("processed_at", System.currentTimeMillis());
 *         return map;
 *     })
 *     .toJson();
 * </pre>
 *
 * @author Pierce
 * @version 1.0
 * @see MapFlattener
 */
public class JsonFlattener implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(JsonFlattener.class);

    // ========================= CONSTANTS =========================

    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    // ========================= SHARED RESOURCES =========================

    /** Thread-safe ObjectMapper with standard configuration */
    private static final ObjectMapper STANDARD_MAPPER = createStandardMapper();

    /** Thread-safe ObjectMapper with pretty printing */
    private static final ObjectMapper PRETTY_MAPPER = createPrettyMapper();

    /** Type reference for Map deserialization */
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF =
            new TypeReference<Map<String, Object>>() {};

    /** Type reference for List of Maps deserialization */
    private static final TypeReference<List<Map<String, Object>>> LIST_MAP_TYPE_REF =
            new TypeReference<List<Map<String, Object>>>() {};

    // ========================= INSTANCE FIELDS =========================

    private final MapFlattener mapFlattener;
    private final ObjectMapper objectMapper;
    private final JsonFlattenerConfig config;

    // ========================= STATIC FACTORY METHODS =========================

    /**
     * Create a JsonFlattener with default configuration.
     *
     * @return New fluent operation builder
     */
    public static FluentOperation create() {
        return new FluentOperation(new JsonFlattener(
                MapFlattener.builder().build(),
                JsonFlattenerConfig.defaults()
        ));
    }

    /**
     * Create a JsonFlattener with a custom MapFlattener.
     *
     * @param mapFlattener The MapFlattener to use for flattening operations
     * @return New fluent operation builder
     */
    public static FluentOperation with(MapFlattener mapFlattener) {
        return new FluentOperation(new JsonFlattener(
                mapFlattener,
                JsonFlattenerConfig.defaults()
        ));
    }

    /**
     * Create a JsonFlattener with full configuration.
     *
     * @param mapFlattener The MapFlattener to use
     * @param config Configuration options
     * @return New fluent operation builder
     */
    public static FluentOperation with(MapFlattener mapFlattener, JsonFlattenerConfig config) {
        return new FluentOperation(new JsonFlattener(mapFlattener, config));
    }

    /**
     * Create a JsonFlattener using a builder pattern.
     *
     * @return New builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    // ========================= CONSTRUCTORS =========================

    private JsonFlattener(MapFlattener mapFlattener, JsonFlattenerConfig config) {
        this.mapFlattener = mapFlattener != null ? mapFlattener : MapFlattener.builder().build();
        this.config = config != null ? config : JsonFlattenerConfig.defaults();
        this.objectMapper = config.isUsePrettyPrint() ? PRETTY_MAPPER : STANDARD_MAPPER;
    }

    // ========================= STATIC HELPERS =========================

    private static ObjectMapper createStandardMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, false);
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        return mapper;
    }

    private static ObjectMapper createPrettyMapper() {
        ObjectMapper mapper = createStandardMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        return mapper;
    }

    // ========================= CORE OPERATIONS =========================

    /**
     * Flatten a JSON string to a Map.
     *
     * @param json JSON string input
     * @return Flattened Map
     * @throws JsonFlattenException if parsing or flattening fails
     */
    public Map<String, Object> flattenToMap(String json) {
        if (json == null || json.isBlank()) {
            return new LinkedHashMap<>();
        }

        try {
            Map<String, Object> parsed = objectMapper.readValue(json, MAP_TYPE_REF);
            return mapFlattener.flatten(parsed);
        } catch (JsonProcessingException e) {
            throw new JsonFlattenException("Failed to parse JSON input", e);
        }
    }

    /**
     * Flatten a JSON string to a JSON string.
     *
     * @param json JSON string input
     * @param pretty Whether to pretty-print output
     * @return Flattened JSON string
     * @throws JsonFlattenException if processing fails
     */
    public String flattenToJson(String json, boolean pretty) {
        Map<String, Object> flattened = flattenToMap(json);
        return serializeToJson(flattened, pretty);
    }

    /**
     * Flatten a Map and serialize to JSON string.
     *
     * @param input Map to flatten
     * @param pretty Whether to pretty-print output
     * @return Flattened JSON string
     * @throws JsonFlattenException if serialization fails
     */
    public String flattenMapToJson(Map<?, ?> input, boolean pretty) {
        Map<String, Object> flattened = mapFlattener.flatten(input);
        return serializeToJson(flattened, pretty);
    }

    /**
     * Serialize a Map to JSON string.
     */
    private String serializeToJson(Map<String, Object> map, boolean pretty) {
        try {
            ObjectMapper mapper = pretty ? PRETTY_MAPPER : STANDARD_MAPPER;
            return mapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new JsonFlattenException("Failed to serialize flattened result", e);
        }
    }

    // ========================= BUILDER =========================

    /**
     * Builder for JsonFlattener configuration.
     */
    public static class Builder {
        private MapFlattener mapFlattener;
        private MapFlattener.Builder mapFlattenerBuilder;
        private JsonFlattenerConfig.ConfigBuilder configBuilder = JsonFlattenerConfig.builder();

        public Builder mapFlattener(MapFlattener flattener) {
            this.mapFlattener = flattener;
            return this;
        }

        public Builder maxDepth(int depth) {
            ensureMapFlattenerBuilder().maxDepth(depth);
            return this;
        }

        public Builder maxArraySize(int size) {
            ensureMapFlattenerBuilder().maxArraySize(size);
            return this;
        }

        public Builder arrayFormat(MapFlattener.ArraySerializationFormat format) {
            ensureMapFlattenerBuilder().arrayFormat(format);
            return this;
        }

        public Builder useArrayBoundarySeparator(boolean use) {
            ensureMapFlattenerBuilder().useArrayBoundarySeparator(use);
            return this;
        }

        public Builder detectCircularReferences(boolean detect) {
            ensureMapFlattenerBuilder().detectCircularReferences(detect);
            return this;
        }

        public Builder namingStrategy(MapFlattener.FieldNamingStrategy strategy) {
            ensureMapFlattenerBuilder().namingStrategy(strategy);
            return this;
        }

        public Builder prettyPrint(boolean pretty) {
            configBuilder.usePrettyPrint(pretty);
            return this;
        }

        public Builder charset(Charset charset) {
            configBuilder.charset(charset);
            return this;
        }

        public Builder bufferSize(int size) {
            configBuilder.bufferSize(size);
            return this;
        }

        public Builder failOnError(boolean fail) {
            configBuilder.failOnError(fail);
            return this;
        }

        private MapFlattener.Builder ensureMapFlattenerBuilder() {
            if (mapFlattenerBuilder == null) {
                mapFlattenerBuilder = MapFlattener.builder();
            }
            return mapFlattenerBuilder;
        }

        public FluentOperation build() {
            MapFlattener flattener = mapFlattener != null
                    ? mapFlattener
                    : (mapFlattenerBuilder != null ? mapFlattenerBuilder.build() : MapFlattener.builder().build());

            return new FluentOperation(new JsonFlattener(flattener, configBuilder.build()));
        }
    }

    // ========================= CONFIGURATION =========================

    /**
     * Configuration options for JsonFlattener operations.
     */
    public static class JsonFlattenerConfig implements Serializable {
        private static final long serialVersionUID = 1L;

        private final boolean usePrettyPrint;
        private final Charset charset;
        private final int bufferSize;
        private final boolean failOnError;
        private final boolean preserveNulls;
        private final boolean sortKeys;

        private JsonFlattenerConfig(ConfigBuilder builder) {
            this.usePrettyPrint = builder.usePrettyPrint;
            this.charset = builder.charset;
            this.bufferSize = builder.bufferSize;
            this.failOnError = builder.failOnError;
            this.preserveNulls = builder.preserveNulls;
            this.sortKeys = builder.sortKeys;
        }

        public static JsonFlattenerConfig defaults() {
            return builder().build();
        }

        public static ConfigBuilder builder() {
            return new ConfigBuilder();
        }

        public boolean isUsePrettyPrint() { return usePrettyPrint; }
        public Charset getCharset() { return charset; }
        public int getBufferSize() { return bufferSize; }
        public boolean isFailOnError() { return failOnError; }
        public boolean isPreserveNulls() { return preserveNulls; }
        public boolean isSortKeys() { return sortKeys; }

        public static class ConfigBuilder {
            private boolean usePrettyPrint = false;
            private Charset charset = DEFAULT_CHARSET;
            private int bufferSize = DEFAULT_BUFFER_SIZE;
            private boolean failOnError = true;
            private boolean preserveNulls = true;
            private boolean sortKeys = false;

            public ConfigBuilder usePrettyPrint(boolean pretty) {
                this.usePrettyPrint = pretty;
                return this;
            }

            public ConfigBuilder charset(Charset charset) {
                this.charset = charset != null ? charset : DEFAULT_CHARSET;
                return this;
            }

            public ConfigBuilder bufferSize(int size) {
                this.bufferSize = size > 0 ? size : DEFAULT_BUFFER_SIZE;
                return this;
            }

            public ConfigBuilder failOnError(boolean fail) {
                this.failOnError = fail;
                return this;
            }

            public ConfigBuilder preserveNulls(boolean preserve) {
                this.preserveNulls = preserve;
                return this;
            }

            public ConfigBuilder sortKeys(boolean sort) {
                this.sortKeys = sort;
                return this;
            }

            public JsonFlattenerConfig build() {
                return new JsonFlattenerConfig(this);
            }
        }
    }

    // ========================= INPUT OPTIONS =========================

    /**
     * Options for input processing.
     */
    public static class InputOptions {
        private final boolean gzipped;
        private final Charset charset;
        private final boolean lenient;
        private final boolean skipInvalid;

        private InputOptions(InputOptionsBuilder builder) {
            this.gzipped = builder.gzipped;
            this.charset = builder.charset;
            this.lenient = builder.lenient;
            this.skipInvalid = builder.skipInvalid;
        }

        public static InputOptions defaults() {
            return builder().build();
        }

        public static InputOptions gzipped() {
            return builder().gzipped(true).build();
        }

        public static InputOptionsBuilder builder() {
            return new InputOptionsBuilder();
        }

        public boolean isGzipped() { return gzipped; }
        public Charset getCharset() { return charset; }
        public boolean isLenient() { return lenient; }
        public boolean isSkipInvalid() { return skipInvalid; }

        public static class InputOptionsBuilder {
            private boolean gzipped = false;
            private Charset charset = DEFAULT_CHARSET;
            private boolean lenient = false;
            private boolean skipInvalid = false;

            public InputOptionsBuilder gzipped(boolean gzip) {
                this.gzipped = gzip;
                return this;
            }

            public InputOptionsBuilder charset(Charset charset) {
                this.charset = charset != null ? charset : DEFAULT_CHARSET;
                return this;
            }

            public InputOptionsBuilder lenient(boolean lenient) {
                this.lenient = lenient;
                return this;
            }

            public InputOptionsBuilder skipInvalid(boolean skip) {
                this.skipInvalid = skip;
                return this;
            }

            public InputOptions build() {
                return new InputOptions(this);
            }
        }
    }

    // ========================= OUTPUT OPTIONS =========================

    /**
     * Options for output formatting.
     */
    public static class OutputOptions {
        private final boolean pretty;
        private final boolean gzipped;
        private final Charset charset;
        private final boolean sortKeys;
        private final boolean includeNulls;
        private final String lineSeparator;

        private OutputOptions(OutputOptionsBuilder builder) {
            this.pretty = builder.pretty;
            this.gzipped = builder.gzipped;
            this.charset = builder.charset;
            this.sortKeys = builder.sortKeys;
            this.includeNulls = builder.includeNulls;
            this.lineSeparator = builder.lineSeparator;
        }

        public static OutputOptions defaults() {
            return builder().build();
        }

        public static OutputOptions pretty() {
            return builder().pretty(true).build();
        }

        public static OutputOptions gzipped() {
            return builder().gzipped(true).build();
        }

        public static OutputOptions compact() {
            return builder().pretty(false).includeNulls(false).build();
        }

        public static OutputOptionsBuilder builder() {
            return new OutputOptionsBuilder();
        }

        public boolean isPretty() { return pretty; }
        public boolean isGzipped() { return gzipped; }
        public Charset getCharset() { return charset; }
        public boolean isSortKeys() { return sortKeys; }
        public boolean isIncludeNulls() { return includeNulls; }
        public String getLineSeparator() { return lineSeparator; }

        public static class OutputOptionsBuilder {
            private boolean pretty = false;
            private boolean gzipped = false;
            private Charset charset = DEFAULT_CHARSET;
            private boolean sortKeys = false;
            private boolean includeNulls = true;
            private String lineSeparator = System.lineSeparator();

            public OutputOptionsBuilder pretty(boolean pretty) {
                this.pretty = pretty;
                return this;
            }

            public OutputOptionsBuilder gzipped(boolean gzip) {
                this.gzipped = gzip;
                return this;
            }

            public OutputOptionsBuilder charset(Charset charset) {
                this.charset = charset != null ? charset : DEFAULT_CHARSET;
                return this;
            }

            public OutputOptionsBuilder sortKeys(boolean sort) {
                this.sortKeys = sort;
                return this;
            }

            public OutputOptionsBuilder includeNulls(boolean include) {
                this.includeNulls = include;
                return this;
            }

            public OutputOptionsBuilder lineSeparator(String separator) {
                this.lineSeparator = separator != null ? separator : System.lineSeparator();
                return this;
            }

            public OutputOptions build() {
                return new OutputOptions(this);
            }
        }
    }

    // ========================= FLUENT OPERATION =========================

    /**
     * Fluent API for single-document operations.
     * <p>
     * Provides a chainable interface for loading, transforming, validating,
     * and outputting flattened JSON.
     */
    public static class FluentOperation {
        private final JsonFlattener flattener;
        private Map<String, Object> currentData;
        private List<Function<Map<String, Object>, Map<String, Object>>> transformers = new ArrayList<>();
        private ValidationRules validationRules;
        private Predicate<Map<String, Object>> filter;

        private FluentOperation(JsonFlattener flattener) {
            this.flattener = flattener;
        }

        // ==================== INPUT METHODS ====================

        /**
         * Load from JSON string.
         */
        public FluentOperation from(String json) {
            if (json == null || json.isBlank()) {
                this.currentData = new LinkedHashMap<>();
            } else {
                this.currentData = flattener.flattenToMap(json);
            }
            return this;
        }

        /**
         * Load from Map.
         */
        public FluentOperation from(Map<?, ?> map) {
            this.currentData = flattener.mapFlattener.flatten(map);
            return this;
        }

        /**
         * Load from JsonNode.
         */
        public FluentOperation from(JsonNode node) {
            if (node == null || node.isNull()) {
                this.currentData = new LinkedHashMap<>();
            } else {
                try {
                    Map<String, Object> map = STANDARD_MAPPER.convertValue(node, MAP_TYPE_REF);
                    this.currentData = flattener.mapFlattener.flatten(map);
                } catch (Exception e) {
                    throw new JsonFlattenException("Failed to convert JsonNode", e);
                }
            }
            return this;
        }

        /**
         * Load from File.
         */
        public FluentOperation from(File file) {
            return from(file, InputOptions.defaults());
        }

        /**
         * Load from File with options.
         */
        public FluentOperation from(File file, InputOptions options) {
            try {
                InputStream is = new FileInputStream(file);
                if (options.isGzipped()) {
                    is = new GZIPInputStream(is);
                }
                return from(is, options.getCharset());
            } catch (IOException e) {
                throw new JsonFlattenException("Failed to read file: " + file.getPath(), e);
            }
        }

        /**
         * Load from Path.
         */
        public FluentOperation from(Path path) {
            return from(path, InputOptions.defaults());
        }

        /**
         * Load from Path with options.
         */
        public FluentOperation from(Path path, InputOptions options) {
            return from(path.toFile(), options);
        }

        /**
         * Load from InputStream.
         */
        public FluentOperation from(InputStream inputStream) {
            return from(inputStream, DEFAULT_CHARSET);
        }

        /**
         * Load from InputStream with charset.
         */
        public FluentOperation from(InputStream inputStream, Charset charset) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(inputStream, charset), DEFAULT_BUFFER_SIZE)) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                return from(sb.toString());
            } catch (IOException e) {
                throw new JsonFlattenException("Failed to read input stream", e);
            }
        }

        /**
         * Load from Reader.
         */
        public FluentOperation from(Reader reader) {
            try (BufferedReader buffered = reader instanceof BufferedReader
                    ? (BufferedReader) reader
                    : new BufferedReader(reader)) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = buffered.readLine()) != null) {
                    sb.append(line);
                }
                return from(sb.toString());
            } catch (IOException e) {
                throw new JsonFlattenException("Failed to read from reader", e);
            }
        }

        /**
         * Load from byte array.
         */
        public FluentOperation from(byte[] bytes) {
            return from(bytes, DEFAULT_CHARSET);
        }

        /**
         * Load from byte array with charset.
         */
        public FluentOperation from(byte[] bytes, Charset charset) {
            if (bytes == null || bytes.length == 0) {
                this.currentData = new LinkedHashMap<>();
                return this;
            }
            return from(new String(bytes, charset));
        }

        // ==================== TRANSFORMATION METHODS ====================

        /**
         * Apply a transformation to the flattened data.
         */
        public FluentOperation transform(Function<Map<String, Object>, Map<String, Object>> transformer) {
            if (transformer != null) {
                transformers.add(transformer);
            }
            return this;
        }

        /**
         * Add a field to the result.
         */
        public FluentOperation addField(String key, Object value) {
            return transform(map -> {
                map.put(key, value);
                return map;
            });
        }

        /**
         * Remove a field from the result.
         */
        public FluentOperation removeField(String key) {
            return transform(map -> {
                map.remove(key);
                return map;
            });
        }

        /**
         * Remove fields matching a predicate.
         */
        public FluentOperation removeFieldsWhere(Predicate<String> keyPredicate) {
            return transform(map -> {
                map.keySet().removeIf(keyPredicate);
                return map;
            });
        }

        /**
         * Rename a field.
         */
        public FluentOperation renameField(String oldKey, String newKey) {
            return transform(map -> {
                if (map.containsKey(oldKey)) {
                    map.put(newKey, map.remove(oldKey));
                }
                return map;
            });
        }

        /**
         * Filter to include only specified fields.
         */
        public FluentOperation includeOnly(String... fields) {
            Set<String> fieldSet = new HashSet<>(Arrays.asList(fields));
            return transform(map -> {
                map.keySet().retainAll(fieldSet);
                return map;
            });
        }

        /**
         * Exclude specified fields.
         */
        public FluentOperation exclude(String... fields) {
            Set<String> fieldSet = new HashSet<>(Arrays.asList(fields));
            return transform(map -> {
                fieldSet.forEach(map::remove);
                return map;
            });
        }

        /**
         * Apply prefix to all keys.
         */
        public FluentOperation prefixKeys(String prefix) {
            return transform(map -> {
                Map<String, Object> prefixed = new LinkedHashMap<>();
                map.forEach((k, v) -> prefixed.put(prefix + k, v));
                return prefixed;
            });
        }

        /**
         * Apply suffix to all keys.
         */
        public FluentOperation suffixKeys(String suffix) {
            return transform(map -> {
                Map<String, Object> suffixed = new LinkedHashMap<>();
                map.forEach((k, v) -> suffixed.put(k + suffix, v));
                return suffixed;
            });
        }

        // ==================== VALIDATION METHODS ====================

        /**
         * Apply validation rules.
         */
        public FluentOperation validate(ValidationRules rules) {
            this.validationRules = rules;
            return this;
        }

        /**
         * Set a filter predicate (result will be empty Map if filter fails).
         */
        public FluentOperation filter(Predicate<Map<String, Object>> predicate) {
            this.filter = predicate;
            return this;
        }

        // ==================== OUTPUT METHODS ====================

        /**
         * Get the flattened result as a Map.
         */
        public Map<String, Object> toMap() {
            return processAndGet();
        }

        /**
         * Get the flattened result as a JSON string.
         * Uses the prettyPrint setting from builder configuration if set.
         */
        public String toJson() {
            return flattener.config.isUsePrettyPrint()
                    ? toJson(OutputOptions.pretty())
                    : toJson(OutputOptions.defaults());
        }

        /**
         * Get the flattened result as a pretty JSON string.
         */
        public String toPrettyJson() {
            return toJson(OutputOptions.pretty());
        }

        /**
         * Get the flattened result as a JSON string with options.
         */
        public String toJson(OutputOptions options) {
            Map<String, Object> result = processAndGet();

            if (options.isSortKeys()) {
                result = new TreeMap<>(result);
            }

            if (!options.isIncludeNulls()) {
                result.entrySet().removeIf(e -> e.getValue() == null);
            }

            try {
                ObjectMapper mapper = options.isPretty() ? PRETTY_MAPPER : STANDARD_MAPPER;
                return mapper.writeValueAsString(result);
            } catch (JsonProcessingException e) {
                throw new JsonFlattenException("Failed to serialize result", e);
            }
        }

        /**
         * Get the flattened result as a JsonNode.
         */
        public JsonNode toJsonNode() {
            Map<String, Object> result = processAndGet();
            return STANDARD_MAPPER.valueToTree(result);
        }

        /**
         * Get the flattened result as bytes.
         */
        public byte[] toBytes() {
            return toBytes(OutputOptions.defaults());
        }

        /**
         * Get the flattened result as bytes with options.
         */
        public byte[] toBytes(OutputOptions options) {
            String json = toJson(options);
            return json.getBytes(options.getCharset());
        }

        /**
         * Write to File.
         */
        public void toFile(File file) {
            toFile(file, OutputOptions.defaults());
        }

        /**
         * Write to File with options.
         */
        public void toFile(File file, OutputOptions options) {
            try {
                OutputStream os = new FileOutputStream(file);
                if (options.isGzipped()) {
                    os = new GZIPOutputStream(os);
                }
                os = new BufferedOutputStream(os, DEFAULT_BUFFER_SIZE);

                try (Writer writer = new OutputStreamWriter(os, options.getCharset())) {
                    writer.write(toJson(options));
                }
            } catch (IOException e) {
                throw new JsonFlattenException("Failed to write to file: " + file.getPath(), e);
            }
        }

        /**
         * Write to Path.
         */
        public void toFile(Path path) {
            toFile(path.toFile(), OutputOptions.defaults());
        }

        /**
         * Write to Path with options.
         */
        public void toFile(Path path, OutputOptions options) {
            toFile(path.toFile(), options);
        }

        /**
         * Write to OutputStream.
         */
        public void toStream(OutputStream outputStream) {
            toStream(outputStream, OutputOptions.defaults());
        }

        /**
         * Write to OutputStream with options.
         */
        public void toStream(OutputStream outputStream, OutputOptions options) {
            try {
                OutputStream os = options.isGzipped()
                        ? new GZIPOutputStream(outputStream)
                        : outputStream;

                try (Writer writer = new OutputStreamWriter(os, options.getCharset())) {
                    writer.write(toJson(options));
                    writer.flush();
                }
            } catch (IOException e) {
                throw new JsonFlattenException("Failed to write to output stream", e);
            }
        }

        /**
         * Write to Writer.
         */
        public void toWriter(Writer writer) {
            toWriter(writer, OutputOptions.defaults());
        }

        /**
         * Write to Writer with options.
         */
        public void toWriter(Writer writer, OutputOptions options) {
            try {
                writer.write(toJson(options));
                writer.flush();
            } catch (IOException e) {
                throw new JsonFlattenException("Failed to write to writer", e);
            }
        }

        /**
         * Process the data and get the final result.
         */
        private Map<String, Object> processAndGet() {
            if (currentData == null) {
                throw new IllegalStateException("No input data loaded. Call from() first.");
            }

            Map<String, Object> result = new LinkedHashMap<>(currentData);

            // Apply transformations
            for (Function<Map<String, Object>, Map<String, Object>> transformer : transformers) {
                result = transformer.apply(result);
            }

            // Apply filter
            if (filter != null && !filter.test(result)) {
                return new LinkedHashMap<>();
            }

            // Apply validation
            if (validationRules != null) {
                ValidationResult validation = validationRules.validate(result);
                if (!validation.isValid()) {
                    throw new JsonValidationException(validation);
                }
            }

            return result;
        }

        // ==================== BATCH/STREAM ACCESS ====================

        /**
         * Switch to batch processing mode.
         */
        public BatchOperation batch() {
            return new BatchOperation(flattener);
        }

        /**
         * Switch to streaming mode.
         */
        public StreamOperation stream() {
            return new StreamOperation(flattener);
        }
    }

    // ========================= BATCH OPERATION =========================

    /**
     * Batch processing for multiple JSON documents.
     * <p>
     * Supports parallel processing for high-throughput scenarios.
     */
    public static class BatchOperation {
        private final JsonFlattener flattener;
        private int parallelism = 1;
        private boolean failFast = true;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private List<Function<Map<String, Object>, Map<String, Object>>> transformers = new ArrayList<>();

        private BatchOperation(JsonFlattener flattener) {
            this.flattener = flattener;
        }

        /**
         * Enable parallel processing with specified thread count.
         */
        public BatchOperation parallel(int threads) {
            this.parallelism = Math.max(1, threads);
            return this;
        }

        /**
         * Use all available processors for parallel processing.
         */
        public BatchOperation parallelAuto() {
            this.parallelism = Runtime.getRuntime().availableProcessors();
            return this;
        }

        /**
         * Set whether to fail fast on first error or continue processing.
         */
        public BatchOperation failFast(boolean fail) {
            this.failFast = fail;
            return this;
        }

        /**
         * Set batch size for chunked processing.
         */
        public BatchOperation batchSize(int size) {
            this.batchSize = Math.max(1, size);
            return this;
        }

        /**
         * Apply transformation to each result.
         */
        public BatchOperation transform(Function<Map<String, Object>, Map<String, Object>> transformer) {
            if (transformer != null) {
                transformers.add(transformer);
            }
            return this;
        }

        /**
         * Process list of JSON strings.
         */
        public BatchResult fromStrings(List<String> jsonStrings) {
            return processBatch(jsonStrings, flattener::flattenToMap);
        }

        /**
         * Process list of Maps.
         */
        public BatchResult fromMaps(List<Map<?, ?>> maps) {
            return processBatch(maps, map -> flattener.mapFlattener.flatten(map));
        }

        /**
         * Process JSON array string.
         */
        public BatchResult fromJsonArray(String jsonArray) {
            try {
                List<Map<String, Object>> items = STANDARD_MAPPER.readValue(jsonArray, LIST_MAP_TYPE_REF);
                return fromMaps(new ArrayList<>(items));
            } catch (JsonProcessingException e) {
                throw new JsonFlattenException("Failed to parse JSON array", e);
            }
        }

        /**
         * Process file containing JSON array.
         */
        public BatchResult fromJsonArrayFile(Path path) {
            try {
                String content = Files.readString(path, DEFAULT_CHARSET);
                return fromJsonArray(content);
            } catch (IOException e) {
                throw new JsonFlattenException("Failed to read file: " + path, e);
            }
        }

        /**
         * Core batch processing method.
         */
        private <T> BatchResult processBatch(List<T> items, Function<T, Map<String, Object>> processor) {
            List<Map<String, Object>> results = new ArrayList<>(items.size());
            List<BatchError> errors = new ArrayList<>();
            AtomicLong processedCount = new AtomicLong(0);

            if (parallelism > 1 && items.size() > parallelism) {
                // Parallel processing
                ExecutorService executor = Executors.newFixedThreadPool(parallelism);
                try {
                    List<Future<ProcessResult>> futures = new ArrayList<>();

                    for (int i = 0; i < items.size(); i++) {
                        final int index = i;
                        final T item = items.get(i);

                        Callable<ProcessResult> task = new Callable<ProcessResult>() {
                            @Override
                            public ProcessResult call() {
                                try {
                                    Map<String, Object> result = processor.apply(item);
                                    result = applyTransformers(result);
                                    return new ProcessResult(index, result, null);
                                } catch (Exception e) {
                                    return new ProcessResult(index, null, e);
                                }
                            }
                        };
                        futures.add(executor.submit(task));
                    }

                    // Collect results in order
                    Map<Integer, Map<String, Object>> orderedResults = new ConcurrentHashMap<>();

                    for (int fi = 0; fi < futures.size(); fi++) {
                        Future<ProcessResult> future = futures.get(fi);
                        try {
                            ProcessResult pr = future.get();
                            if (pr != null) {
                                processedCount.incrementAndGet();

                                if (pr.error != null) {
                                    if (failFast) {
                                        throw new JsonFlattenException("Batch processing failed at index " + pr.index, pr.error);
                                    }
                                    errors.add(new BatchError(pr.index, pr.error));
                                } else if (pr.result != null) {
                                    orderedResults.put(pr.index, pr.result);
                                }
                            }
                        } catch (InterruptedException | ExecutionException e) {
                            if (failFast) {
                                throw new JsonFlattenException("Batch processing interrupted", e);
                            }
                            errors.add(new BatchError(fi, e instanceof ExecutionException ? (Exception) e.getCause() : e));
                        }
                    }

                    // Reconstruct ordered list
                    for (int i = 0; i < items.size(); i++) {
                        if (orderedResults.containsKey(i)) {
                            results.add(orderedResults.get(i));
                        }
                    }

                } finally {
                    executor.shutdown();
                    try {
                        // Wait for all tasks to complete
                        if (!executor.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
                            executor.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        executor.shutdownNow();
                        Thread.currentThread().interrupt();
                    }
                }
            } else {
                // Sequential processing
                for (int i = 0; i < items.size(); i++) {
                    try {
                        Map<String, Object> result = processor.apply(items.get(i));
                        result = applyTransformers(result);
                        results.add(result);
                        processedCount.incrementAndGet();
                    } catch (Exception e) {
                        if (failFast) {
                            throw new JsonFlattenException("Batch processing failed at index " + i, e);
                        }
                        errors.add(new BatchError(i, e));
                    }
                }
            }

            return new BatchResult(results, errors, processedCount.get());
        }

        private Map<String, Object> applyTransformers(Map<String, Object> map) {
            Map<String, Object> result = map;
            for (Function<Map<String, Object>, Map<String, Object>> transformer : transformers) {
                result = transformer.apply(result);
            }
            return result;
        }

        private static class ProcessResult {
            final int index;
            final Map<String, Object> result;
            final Exception error;

            ProcessResult(int index, Map<String, Object> result, Exception error) {
                this.index = index;
                this.result = result;
                this.error = error;
            }
        }
    }

    // ========================= BATCH RESULT =========================

    /**
     * Result of batch processing operation.
     */
    public static class BatchResult {
        private final List<Map<String, Object>> results;
        private final List<BatchError> errors;
        private final long processedCount;

        private BatchResult(List<Map<String, Object>> results, List<BatchError> errors, long processedCount) {
            this.results = Collections.unmodifiableList(results);
            this.errors = Collections.unmodifiableList(errors);
            this.processedCount = processedCount;
        }

        /**
         * Get results as list of Maps.
         */
        public List<Map<String, Object>> toMaps() {
            return results;
        }

        /**
         * Get results as list of JSON strings.
         */
        public List<String> toJsonStrings() {
            return toJsonStrings(OutputOptions.defaults());
        }

        /**
         * Get results as list of JSON strings with options.
         */
        public List<String> toJsonStrings(OutputOptions options) {
            ObjectMapper mapper = options.isPretty() ? PRETTY_MAPPER : STANDARD_MAPPER;
            return results.stream()
                    .map(m -> {
                        try {
                            return mapper.writeValueAsString(m);
                        } catch (JsonProcessingException e) {
                            throw new JsonFlattenException("Failed to serialize result", e);
                        }
                    })
                    .collect(Collectors.toList());
        }

        /**
         * Get results as a single JSON array string.
         */
        public String toJsonArray() {
            return toJsonArray(OutputOptions.defaults());
        }

        /**
         * Get results as a single JSON array string with options.
         */
        public String toJsonArray(OutputOptions options) {
            try {
                ObjectMapper mapper = options.isPretty() ? PRETTY_MAPPER : STANDARD_MAPPER;
                return mapper.writeValueAsString(results);
            } catch (JsonProcessingException e) {
                throw new JsonFlattenException("Failed to serialize results array", e);
            }
        }

        /**
         * Write results to file as JSON array.
         */
        public void toFile(Path path) {
            toFile(path, OutputOptions.defaults());
        }

        /**
         * Write results to file as JSON array with options.
         */
        public void toFile(Path path, OutputOptions options) {
            try {
                String json = toJsonArray(options);
                if (options.isGzipped()) {
                    try (OutputStream os = new GZIPOutputStream(
                            new BufferedOutputStream(Files.newOutputStream(path)))) {
                        os.write(json.getBytes(options.getCharset()));
                    }
                } else {
                    Files.writeString(path, json, options.getCharset());
                }
            } catch (IOException e) {
                throw new JsonFlattenException("Failed to write to file: " + path, e);
            }
        }

        /**
         * Write results to NDJSON file (one JSON per line).
         */
        public void toNdjsonFile(Path path) {
            toNdjsonFile(path, OutputOptions.defaults());
        }

        /**
         * Write results to NDJSON file with options.
         */
        public void toNdjsonFile(Path path, OutputOptions options) {
            try (BufferedWriter writer = Files.newBufferedWriter(path, options.getCharset())) {
                for (Map<String, Object> result : results) {
                    writer.write(STANDARD_MAPPER.writeValueAsString(result));
                    writer.write(options.getLineSeparator());
                }
            } catch (IOException e) {
                throw new JsonFlattenException("Failed to write NDJSON file: " + path, e);
            }
        }

        /**
         * Get the list of errors (empty if all succeeded).
         */
        public List<BatchError> getErrors() {
            return errors;
        }

        /**
         * Check if all items processed successfully.
         */
        public boolean isSuccess() {
            return errors.isEmpty();
        }

        /**
         * Get count of successfully processed items.
         */
        public int getSuccessCount() {
            return results.size();
        }

        /**
         * Get count of failed items.
         */
        public int getErrorCount() {
            return errors.size();
        }

        /**
         * Get total items attempted.
         */
        public long getProcessedCount() {
            return processedCount;
        }

        /**
         * Apply post-processing to results.
         */
        public BatchResult transform(Function<Map<String, Object>, Map<String, Object>> transformer) {
            List<Map<String, Object>> transformed = results.stream()
                    .map(transformer)
                    .collect(Collectors.toList());
            return new BatchResult(transformed, errors, processedCount);
        }

        /**
         * Filter results.
         */
        public BatchResult filter(Predicate<Map<String, Object>> predicate) {
            List<Map<String, Object>> filtered = results.stream()
                    .filter(predicate)
                    .collect(Collectors.toList());
            return new BatchResult(filtered, errors, processedCount);
        }
    }

    /**
     * Represents an error during batch processing.
     */
    public static class BatchError {
        private final int index;
        private final Exception exception;

        private BatchError(int index, Exception exception) {
            this.index = index;
            this.exception = exception;
        }

        public int getIndex() { return index; }
        public Exception getException() { return exception; }
        public String getMessage() { return exception.getMessage(); }

        @Override
        public String toString() {
            return String.format("BatchError[index=%d, message=%s]", index, exception.getMessage());
        }
    }

    // ========================= STREAM OPERATION =========================

    /**
     * Streaming operations for large NDJSON/JSON Lines files.
     * <p>
     * Processes records one at a time to minimize memory usage.
     */
    public static class StreamOperation {
        private final JsonFlattener flattener;
        private List<Function<Map<String, Object>, Map<String, Object>>> transformers = new ArrayList<>();
        private Predicate<Map<String, Object>> filter;
        private Consumer<StreamError> errorHandler;
        private boolean skipErrors = false;

        private StreamOperation(JsonFlattener flattener) {
            this.flattener = flattener;
        }

        /**
         * Apply transformation to each record.
         */
        public StreamOperation transform(Function<Map<String, Object>, Map<String, Object>> transformer) {
            if (transformer != null) {
                transformers.add(transformer);
            }
            return this;
        }

        /**
         * Filter records.
         */
        public StreamOperation filter(Predicate<Map<String, Object>> predicate) {
            this.filter = predicate;
            return this;
        }

        /**
         * Set error handler (called for each error when skipErrors is true).
         */
        public StreamOperation onError(Consumer<StreamError> handler) {
            this.errorHandler = handler;
            return this;
        }

        /**
         * Skip errors instead of failing.
         */
        public StreamOperation skipErrors(boolean skip) {
            this.skipErrors = skip;
            return this;
        }

        /**
         * Stream from NDJSON file.
         */
        public StreamResult fromNdjsonFile(Path path) {
            return fromNdjsonFile(path, InputOptions.defaults());
        }

        /**
         * Stream from NDJSON file with options.
         */
        public StreamResult fromNdjsonFile(Path path, InputOptions options) {
            try {
                InputStream is = Files.newInputStream(path);
                if (options.isGzipped()) {
                    is = new GZIPInputStream(is);
                }
                return fromNdjsonStream(is, options.getCharset());
            } catch (IOException e) {
                throw new JsonFlattenException("Failed to open file: " + path, e);
            }
        }

        /**
         * Stream from NDJSON input stream.
         */
        public StreamResult fromNdjsonStream(InputStream inputStream, Charset charset) {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(inputStream, charset), DEFAULT_BUFFER_SIZE);

            return new StreamResult(createLineStream(reader), this);
        }

        /**
         * Stream from iterable of JSON strings.
         */
        public StreamResult fromStrings(Iterable<String> jsonStrings) {
            Iterator<String> iterator = jsonStrings.iterator();

            Stream<String> stream = Stream.generate(() -> {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                return null;
            }).takeWhile(Objects::nonNull);

            return new StreamResult(stream, this);
        }

        private Stream<String> createLineStream(BufferedReader reader) {
            return Stream.generate(() -> {
                try {
                    return reader.readLine();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).takeWhile(Objects::nonNull);
        }

        private Map<String, Object> processLine(String line, long lineNumber) {
            if (line == null || line.isBlank()) {
                return null;
            }

            try {
                Map<String, Object> result = flattener.flattenToMap(line);

                for (Function<Map<String, Object>, Map<String, Object>> transformer : transformers) {
                    result = transformer.apply(result);
                }

                if (filter != null && !filter.test(result)) {
                    return null;
                }

                return result;
            } catch (Exception e) {
                if (skipErrors) {
                    if (errorHandler != null) {
                        errorHandler.accept(new StreamError(lineNumber, line, e));
                    }
                    return null;
                }
                throw new JsonFlattenException("Failed to process line " + lineNumber, e);
            }
        }
    }

    /**
     * Result of streaming operation.
     */
    public static class StreamResult {
        private final Stream<String> source;
        private final StreamOperation operation;
        private final AtomicLong lineCounter = new AtomicLong(0);

        private StreamResult(Stream<String> source, StreamOperation operation) {
            this.source = source;
            this.operation = operation;
        }

        /**
         * Get stream of flattened Maps.
         */
        public Stream<Map<String, Object>> toMapStream() {
            return source
                    .map(line -> operation.processLine(line, lineCounter.incrementAndGet()))
                    .filter(Objects::nonNull);
        }

        /**
         * Get stream of flattened JSON strings.
         */
        public Stream<String> toJsonStream() {
            return toMapStream().map(m -> {
                try {
                    return STANDARD_MAPPER.writeValueAsString(m);
                } catch (JsonProcessingException e) {
                    throw new JsonFlattenException("Failed to serialize", e);
                }
            });
        }

        /**
         * Process each record with a consumer.
         */
        public void forEach(Consumer<Map<String, Object>> consumer) {
            toMapStream().forEach(consumer);
        }

        /**
         * Collect all results to a list.
         */
        public List<Map<String, Object>> toList() {
            return toMapStream().collect(Collectors.toList());
        }

        /**
         * Write to NDJSON file.
         */
        public long toNdjsonFile(Path path) {
            return toNdjsonFile(path, OutputOptions.defaults());
        }

        /**
         * Write to NDJSON file with options.
         */
        public long toNdjsonFile(Path path, OutputOptions options) {
            AtomicLong count = new AtomicLong(0);

            try {
                OutputStream os = Files.newOutputStream(path,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

                if (options.isGzipped()) {
                    os = new GZIPOutputStream(os);
                }

                try (BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(os, options.getCharset()), DEFAULT_BUFFER_SIZE)) {

                    toMapStream().forEach(map -> {
                        try {
                            writer.write(STANDARD_MAPPER.writeValueAsString(map));
                            writer.write(options.getLineSeparator());
                            count.incrementAndGet();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
                }

                return count.get();
            } catch (IOException e) {
                throw new JsonFlattenException("Failed to write to file: " + path, e);
            }
        }

        /**
         * Count total records processed.
         */
        public long count() {
            return toMapStream().count();
        }

        /**
         * Get the number of lines read so far.
         */
        public long getLinesRead() {
            return lineCounter.get();
        }
    }

    /**
     * Represents an error during stream processing.
     */
    public static class StreamError {
        private final long lineNumber;
        private final String line;
        private final Exception exception;

        private StreamError(long lineNumber, String line, Exception exception) {
            this.lineNumber = lineNumber;
            this.line = line;
            this.exception = exception;
        }

        public long getLineNumber() { return lineNumber; }
        public String getLine() { return line; }
        public Exception getException() { return exception; }

        @Override
        public String toString() {
            return String.format("StreamError[line=%d, message=%s]", lineNumber, exception.getMessage());
        }
    }

    // ========================= VALIDATION =========================

    /**
     * Validation rules for flattened output.
     */
    public static class ValidationRules {
        private final Set<String> requiredFields;
        private final Set<String> forbiddenFields;
        private final int maxKeyLength;
        private final int maxValueLength;
        private final int maxFields;
        private final Map<String, Predicate<Object>> fieldValidators;
        private final Predicate<Map<String, Object>> customValidator;

        private ValidationRules(ValidationRulesBuilder builder) {
            this.requiredFields = builder.requiredFields;
            this.forbiddenFields = builder.forbiddenFields;
            this.maxKeyLength = builder.maxKeyLength;
            this.maxValueLength = builder.maxValueLength;
            this.maxFields = builder.maxFields;
            this.fieldValidators = builder.fieldValidators;
            this.customValidator = builder.customValidator;
        }

        public static ValidationRulesBuilder builder() {
            return new ValidationRulesBuilder();
        }

        /**
         * Validate a flattened map.
         */
        public ValidationResult validate(Map<String, Object> data) {
            List<String> violations = new ArrayList<>();

            // Check required fields
            for (String required : requiredFields) {
                if (!data.containsKey(required)) {
                    violations.add("Missing required field: " + required);
                }
            }

            // Check forbidden fields
            for (String forbidden : forbiddenFields) {
                if (data.containsKey(forbidden)) {
                    violations.add("Forbidden field present: " + forbidden);
                }
            }

            // Check max fields
            if (maxFields > 0 && data.size() > maxFields) {
                violations.add("Too many fields: " + data.size() + " (max: " + maxFields + ")");
            }

            // Check key/value constraints
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (maxKeyLength > 0 && key.length() > maxKeyLength) {
                    violations.add("Key too long: " + key + " (length: " + key.length() + ", max: " + maxKeyLength + ")");
                }

                if (maxValueLength > 0 && value != null) {
                    String strValue = value.toString();
                    if (strValue.length() > maxValueLength) {
                        violations.add("Value too long for key " + key + " (length: " + strValue.length() + ", max: " + maxValueLength + ")");
                    }
                }

                // Check field-specific validators
                if (fieldValidators.containsKey(key)) {
                    Predicate<Object> validator = fieldValidators.get(key);
                    if (!validator.test(value)) {
                        violations.add("Field validation failed for: " + key);
                    }
                }
            }

            // Custom validation
            if (customValidator != null && !customValidator.test(data)) {
                violations.add("Custom validation failed");
            }

            return new ValidationResult(violations.isEmpty(), violations);
        }

        public static class ValidationRulesBuilder {
            private Set<String> requiredFields = new HashSet<>();
            private Set<String> forbiddenFields = new HashSet<>();
            private int maxKeyLength = 0;
            private int maxValueLength = 0;
            private int maxFields = 0;
            private Map<String, Predicate<Object>> fieldValidators = new HashMap<>();
            private Predicate<Map<String, Object>> customValidator;

            public ValidationRulesBuilder requireFields(String... fields) {
                requiredFields.addAll(Arrays.asList(fields));
                return this;
            }

            public ValidationRulesBuilder forbidFields(String... fields) {
                forbiddenFields.addAll(Arrays.asList(fields));
                return this;
            }

            public ValidationRulesBuilder maxKeyLength(int length) {
                this.maxKeyLength = length;
                return this;
            }

            public ValidationRulesBuilder maxValueLength(int length) {
                this.maxValueLength = length;
                return this;
            }

            public ValidationRulesBuilder maxFields(int count) {
                this.maxFields = count;
                return this;
            }

            public ValidationRulesBuilder validateField(String field, Predicate<Object> validator) {
                fieldValidators.put(field, validator);
                return this;
            }

            public ValidationRulesBuilder customValidator(Predicate<Map<String, Object>> validator) {
                this.customValidator = validator;
                return this;
            }

            public ValidationRules build() {
                return new ValidationRules(this);
            }
        }
    }

    /**
     * Result of validation.
     */
    public static class ValidationResult {
        private final boolean valid;
        private final List<String> violations;

        private ValidationResult(boolean valid, List<String> violations) {
            this.valid = valid;
            this.violations = Collections.unmodifiableList(violations);
        }

        public boolean isValid() { return valid; }
        public List<String> getViolations() { return violations; }

        @Override
        public String toString() {
            if (valid) {
                return "ValidationResult[valid=true]";
            }
            return "ValidationResult[valid=false, violations=" + violations + "]";
        }
    }

    // ========================= EXCEPTIONS =========================

    /**
     * Base exception for JSON flattening operations.
     */
    public static class JsonFlattenException extends RuntimeException {
        public JsonFlattenException(String message) {
            super(message);
        }

        public JsonFlattenException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Exception thrown when validation fails.
     */
    public static class JsonValidationException extends JsonFlattenException {
        private final ValidationResult validationResult;

        public JsonValidationException(ValidationResult result) {
            super("Validation failed: " + result.getViolations());
            this.validationResult = result;
        }

        public ValidationResult getValidationResult() {
            return validationResult;
        }

        public List<String> getViolations() {
            return validationResult.getViolations();
        }
    }

    // ========================= UTILITY METHODS =========================

    /**
     * Quick flatten of JSON string to Map (convenience method).
     */
    public static Map<String, Object> quickFlatten(String json) {
        return create().from(json).toMap();
    }

    /**
     * Quick flatten of JSON string to JSON string (convenience method).
     */
    public static String quickFlattenToJson(String json) {
        return create().from(json).toJson();
    }

    /**
     * Quick flatten of JSON string to pretty JSON string (convenience method).
     */
    public static String quickFlattenToPrettyJson(String json) {
        return create().from(json).toPrettyJson();
    }

    /**
     * Quick flatten of Map to JSON string (convenience method).
     */
    public static String quickFlattenMapToJson(Map<?, ?> map) {
        return create().from(map).toJson();
    }
}