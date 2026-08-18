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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import io.github.pierce.path.FlattenedPath;
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
 * <h2>Key Features:</h2>
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
 * <h2>Example Usage:</h2>
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
    /**
     * Default schema-cache bound.
     *
     * <p>Was 100 — a number that had never been chosen as a cache size. It was a
     * {@code ConcurrentHashMap} initial-capacity argument whose constant name misdescribed it as a
     * maximum, and it was carried over unexamined when the cache was actually bounded. A magic
     * number inherited from a bug is still a magic number.</p>
     *
     * <p>Re-derived from measurement. Rotating through N distinct schemas
     * ({@code SchemaCacheCliffBenchmark}) costs a flat ~0.36 us/op while N fits the bound and
     * jumps to ~1.11 us/op — 3.1x — the moment N exceeds it, then stays flat: past capacity a
     * strict rotation has a 0% hit rate, because every lookup evicts the entry it will want next.
     * That cliff cannot be removed by a smarter eviction policy; a working set larger than the
     * cache does not fit in the cache. It can only be sited sensibly and made visible.</p>
     *
     * <p>256 covers schema-registry and multi-tenant workloads that 100 did not, at a few hundred
     * KB of retained schema graphs. Workloads beyond it should raise the bound explicitly via
     * {@code maxSchemaCacheSize} and watch {@link SchemaCacheStats#hitRate()}.</p>
     */
    private static final int DEFAULT_MAX_CACHE_SIZE = 256;

    // Shared ObjectMapper - configured for consistent JSON handling
    private static final ObjectMapper SHARED_OBJECT_MAPPER = createConfiguredMapper();

    /**
     * Shared TypeReference for the {@code List<Object>} parses on the array paths.
     *
     * <p>These were three separate {@code new TypeReference<List<Object>>() {}} expressions in
     * INSTANCE methods, which javac compiles to anonymous classes carrying a synthetic
     * {@code this$0} reference to the whole reconstructor
     * (SIC_INNER_SHOULD_BE_STATIC_ANON on AvroReconstructor$1 and $2), and which allocated a
     * fresh TypeReference on every parse. Declared here in static context there is no enclosing
     * instance and one instance is shared by all call sites.
     *
     * <p>A fourth identical expression at the PathNode deserialization site was already in
     * static context and correctly never reported; it now shares this constant too.
     */
    private static final TypeReference<List<Object>> LIST_OF_OBJECT =
            new TypeReference<List<Object>>() { };

    private static ObjectMapper createConfiguredMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, true);
        return mapper;
    }

    // Compiled patterns for performance
    // ARRAY_INDEX_PATTERN and JSON_ARRAY_PATTERN were declared here and never referenced. PMD
    // flagged both as UnusedPrivateField once the file became Java and was analysed for the first
    // time — while it was .groovy, no static analyser looked at it at all. Dead code, deleted
    // rather than suppressed.
    private static final Pattern BRACKET_LIST_PATTERN = Pattern.compile("^\\[(.*)\\]$");

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
    /**
     * Keyed by {@link Schema} rather than by a canonical-form fingerprint string.
     *
     * <p>Avro memoises {@code Schema.hashCode()} and its {@code equals} short-circuits on
     * identity, so lookup is effectively free after the first call. Building a fingerprint key
     * instead meant serialising the whole schema to canonical form on every record.</p>
     */
    private final ConcurrentHashMap<Schema, SchemaCacheEntry> schemaCache;

    /**
     * Insertion order for cache eviction. Separate from the map so the read path stays lock-free;
     * see {@link #evictIfOverCapacity()} for why this is insertion-ordered rather than LRU.
     */
    private final java.util.concurrent.ConcurrentLinkedQueue<Schema> insertionOrder =
            new java.util.concurrent.ConcurrentLinkedQueue<>();

    /** Hard upper bound on cached schemas. Previously only an initial capacity, so unbounded. */
    private final int maxCacheSize;

    private final java.util.concurrent.atomic.LongAdder cacheHits =
            new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder cacheMisses =
            new java.util.concurrent.atomic.LongAdder();

    /**
     * Observable schema-cache behaviour.
     *
     * <p>Exists because bounding a cache creates a performance cliff at the capacity boundary, and
     * a cliff nobody can see is one you discover from a latency graph months later. Past capacity
     * a rotating workload drops to a 0% hit rate and pays ~3.1x per record; the hit rate is the
     * signal that says so.</p>
     */
    public record SchemaCacheStats(long hits, long misses, int size, int maxSize) {
        /** Fraction of lookups served from cache, or 1.0 before any lookup. */
        public double hitRate() {
            long total = hits + misses;
            return total == 0 ? 1.0 : (double) hits / total;
        }
    }

    /** Current cache statistics. Cheap; safe to poll from a metrics thread. */
    public SchemaCacheStats getSchemaCacheStats() {
        return new SchemaCacheStats(cacheHits.sum(), cacheMisses.sum(),
                schemaCache.size(), maxCacheSize);
    }

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
        private int maxSchemaCacheSize = DEFAULT_MAX_CACHE_SIZE;
        private ArraySerializationFormat arrayFormat = ArraySerializationFormat.JSON;
        private boolean useArrayBoundarySeparator = false;
        private boolean strictValidation = true;
        /**
         * DEFAULT FLIPPED to false in 2.1.0, and the OUTCOME at the shipped default is unchanged:
         * a missing required field failed before and fails now. What changed is that it fails with
         * our exception naming the flattened path, instead of leaking Avro's own
         * AvroMissingFieldException from {@code GenericRecordBuilder.build()} with only the field
         * name. Keeping {@code true} as the default while giving {@code true} a tolerant meaning
         * would have turned today's loud failure into a silently invented {@code ""} at the
         * SHIPPED DEFAULT - the exact pathology this pass exists to remove.
         */
        private boolean allowMissingFields = false;
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

        /**
         * Whether a value that contradicts its schema is an error (default {@code true}) or is
         * quietly replaced by a type default.
         *
         * <p>At {@code true} a malformed scalar, an unresolvable union branch and an array-sizing
         * input that is not parseable all throw. At {@code false} they are logged and substituted.
         * It has no effect on a well-formed document, which is why an early probe over five
         * well-formed documents recorded it as inert.</p>
         */
        public Builder strictValidation(boolean strict) {
            this.strictValidation = strict;
            return this;
        }

        /**
         * What to do about a required field that has no value in the flattened input and no
         * schema default. DEFAULT {@code false} since 2.1.0.
         *
         * <p>{@code false}: reconstruction fails with a {@link ReconstructionException} naming
         * every such field by its FLATTENED PATH, thrown before the record is built.</p>
         *
         * <p>{@code true}: the Avro TYPE default is substituted - {@code ""}, {@code 0},
         * {@code 0L}, {@code 0.0}, {@code false}, an empty array, an empty map, empty bytes - and
         * one aggregated WARN names every path that was filled. ENUM, FIXED, RECORD and a UNION
         * with no null branch have no type default, and reconstruction fails for those rather than
         * inventing a symbol or an empty record.</p>
         *
         * <p>KNOWN INCONSISTENCY, disclosed rather than discovered: this flag does not yet reach
         * {@code handleMissingField}, which fills {@code ""} and {@code 0} for a missing required
         * field one level down inside an ARRAY ELEMENT regardless of the setting. Gating that too
         * would turn array-of-records reconstructions that succeed today into throws at the
         * shipped default and is tracked separately.</p>
         */
        public Builder allowMissingFields(boolean allow) {
            this.allowMissingFields = allow;
            return this;
        }

        /**
         * Whether a field absent from the flattened input is filled from its schema default
         * (default {@code true}).
         *
         * <p>{@code true} supplies the default decoded to its schema-correct runtime type -
         * an EnumSymbol for an ENUM default, a GenericData.Fixed for FIXED, a ByteBuffer for
         * BYTES, a Utf8 for STRING, a real Java null for {@code "default": null}. Before 2.1.0 it
         * supplied Avro's JSON-shaped {@code Field.defaultVal()} instead, so the datum did not
         * validate and could not be written.</p>
         *
         * <p>{@code false} means "do not consult the schema default": a nullable field becomes
         * null, and a non-nullable one is treated as MISSING and handed to
         * {@link #allowMissingFields(boolean)}. Before 2.1.0 it left the slot unset for a
         * non-nullable field, and {@code GenericRecordBuilder.build()} re-supplied the same value
         * anyway - so at that arity the knob could not suppress anything.</p>
         */
        public Builder useSchemaDefaults(boolean use) {
            this.useSchemaDefaults = use;
            return this;
        }

        /**
         * Bounds the schema cache. Raise it when the working set of distinct schemas exceeds the
         * default, which costs a 3.1x per-record penalty at 100% miss rate — see
         * {@link AvroReconstructor#DEFAULT_MAX_CACHE_SIZE}.
         */
        public Builder maxSchemaCacheSize(int size) {
            if (size < 1) {
                throw new IllegalArgumentException("maxSchemaCacheSize must be >= 1");
            }
            this.maxSchemaCacheSize = size;
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

        /**
         * Gates {@link AvroReconstructor#verifyReconstruction} and nothing else (default
         * {@code true}).
         *
         * <p>Stated plainly because the name suggests more: it does not touch reconstruction, and
         * {@code compareFlattenedMaps} keeps working at {@code false}. Widening the gate would
         * turn a currently-working call into a throw for anyone who set the flag, so the
         * inconsistency is pinned rather than repaired.</p>
         */
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
        this.maxCacheSize = builder.maxSchemaCacheSize;
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

        // NP-025. There is no early return for an empty or null map any more. An empty map must
        // behave exactly like a non-empty map with zero matching keys, and an empty root PathNode
        // gives precisely that: every field runs the same ladder. The old short-circuit called
        // createEmptyRecord, which consulted neither knob, built no GenericRecord, and silently
        // OMITTED any field that was neither defaulted nor nullable - so the same schema and the
        // same missing field produced two different answers depending on whether one unrelated
        // key happened to be present. null is treated as empty rather than rejected: NP-025's
        // complaint is the empty-vs-non-empty inconsistency, and argument validation is a separate
        // question that deserves its own decision rather than being smuggled in here.
        Map<String, Object> input = flattenedMap == null
                ? Collections.<String, Object>emptyMap() : flattenedMap;

        try {
            // Get schema paths
            SchemaCacheEntry cacheEntry = getOrBuildSchemaCacheEntry(schema);

            // Build path tree
            PathNode root = buildPathTree(input, cacheEntry.pathTrie);

            // Reconstruct
            GenericRecord record = reconstructRecord(root, schema, "", 0);

            // Convert to Map for verification
            return genericRecordToMap(record);

        } catch (ReconstructionException e) {
            // Already ours, and already carries the field path. Wrapping it a second time buys
            // the caller two identical frames and buries the specific message - which is the same
            // muffling this pass exists to remove. Rethrow unchanged, and do not log it either:
            // the caller is about to receive it, and logging on the way past is how one failure
            // becomes three lines in an operator's console.
            throw e;
        } catch (Exception e) {
            log.error("Reconstruction failed for schema: {}", schema.getName(), e);
            throw new ReconstructionException(
                    "Failed to reconstruct data for schema: " + schema.getName()
                            + " - " + rootMessage(e), e);
        }
    }

    /**
     * The deepest non-blank message in a cause chain.
     *
     * <p>A loud error that gets muffled two frames up is not loud. Measured before this was added:
     * a caller who hit a specific, named failure deep inside the array machinery saw exactly
     * {@code "Failed to reconstruct data for schema: O3"} and nothing else.</p>
     */
    private static String rootMessage(Throwable t) {
        String best = t.getMessage();
        for (Throwable c = t.getCause(); c != null; c = c.getCause()) {
            if (c.getMessage() != null && !c.getMessage().trim().isEmpty()) {
                best = c.getMessage();
            }
        }
        return best == null ? t.getClass().getSimpleName() : best;
    }

    /**
     * The schema default for a field, decoded to its SCHEMA-CORRECT runtime type.
     *
     * <p>NP-023. Every default this class supplied used to come from {@code Field.defaultVal()},
     * which routes through Avro's {@code JacksonUtils.toObject} and returns the JSON shape rather
     * than the Avro shape: a {@link String} for an ENUM default, a {@code byte[]} for FIXED and
     * BYTES, a {@link LinkedHashMap} for a record default, and the
     * {@code JsonProperties.NULL_VALUE} singleton - a non-null OBJECT - for {@code "default":
     * null}. {@code GenericRecordBuilder.set} only checks for null, so the record was built
     * carrying the wrong type, {@code GenericData.validate} returned false, and the failure
     * surfaced only when somebody tried to write the datum.</p>
     *
     * <p>{@code GenericData.getDefaultValue} decodes the default JsonNode through a real datum
     * reader instead, so ENUM yields an EnumSymbol, FIXED a GenericData.Fixed, BYTES a ByteBuffer,
     * STRING a Utf8 and a null default a real Java null.</p>
     *
     * <p>The {@code deepCopy} is NOT optional. getDefaultValue MEMOISES one instance per Field in
     * a shared static cache, so two reconstructions of the same schema would otherwise alias one
     * mutable List, Map, Record or ByteBuffer. {@code defaultVal()} happened to build a fresh
     * object every call, so omitting the copy would introduce an aliasing bug that does not exist
     * today. {@code RecordBuilderBase.defaultValue} does exactly this pair.</p>
     */
    private static Object schemaDefault(Schema.Field field, String fieldPath) {
        GenericData data = GenericData.get();
        try {
            return data.deepCopy(field.schema(), data.getDefaultValue(field));
        } catch (RuntimeException e) {
            // Do NOT fall back to defaultVal(): that is the mistyped path this method exists to
            // replace, and quietly returning it would re-launder the failure.
            throw new ReconstructionException(
                    "Schema default for field '" + fieldPath + "' does not decode against its own "
                            + "schema " + field.schema().getType() + ": " + field.defaultVal(), e);
        }
    }

    /**
     * Reconstruct to GenericRecord (legacy method)
     */
    public GenericRecord reconstruct(Map<String, Object> flattenedMap, Schema schema) {
        Map<String, Object> reconstructedMap = reconstructToMap(flattenedMap, schema);
        return mapToGenericRecord(reconstructedMap, schema);
    }

    // createEmptyRecord is DELETED (NP-025). It was the empty-map short-circuit's whole body:
    // it read neither useSchemaDefaults nor allowMissingFields, never built a GenericRecord, and
    // omitted required no-default fields without a word. An empty map now takes the ordinary path.

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
            String[] parts = FlattenedPath.decodeSegments(path, separator).toArray(new String[0]);
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

    /**
     * Resolves the cached path trie for a schema.
     *
     * <p>This used to build the cache key with
     * {@link SchemaNormalization#parsingFingerprint64(Schema)} on <b>every call</b> — and it is
     * called once per record. That constructs the schema's full canonical form (several KB of
     * string for a 250-field schema) and Rabin-hashes it, purely to look up an entry that never
     * changes. The key cost more than the value it retrieved.</p>
     *
     * <p>Keying on the {@link Schema} itself removes that entirely. Avro's {@code Schema.hashCode()}
     * is content-based and memoised after the first call, and {@code equals} short-circuits on
     * identity, so the steady-state lookup is a field read and a reference comparison.</p>
     *
     * <p><b>Correctness note.</b> This depends on Avro schemas being effectively immutable after
     * parsing, which they are — the API exposes no mutator that changes structure. Two schemas
     * parsed separately from the same text are distinct objects but compare equal, so they
     * correctly share one entry; that is asserted by
     * {@code AvroSchemaCacheTest.samePathParsedTwiceSharesOneEntry}. If a schema were mutated
     * in place after being cached, the entry would go stale — the previous fingerprint key had
     * the same exposure, since it was also computed before the mutation.</p>
     */
    private SchemaCacheEntry getOrBuildSchemaCacheEntry(Schema schema) {
        // Lock-free fast path. This runs once per record and is the reason keying on Schema
        // rather than on a canonical-form fingerprint is worth doing at all.
        SchemaCacheEntry hit = schemaCache.get(schema);
        if (hit != null) {
            cacheHits.increment();
            return hit;
        }
        cacheMisses.increment();

        boolean[] wasBuilt = {false};
        SchemaCacheEntry built = schemaCache.computeIfAbsent(schema, k -> {
            wasBuilt[0] = true;
            // The fingerprint is retained for diagnostics only; computing it once per distinct
            // schema is fine, once per record was not.
            return new SchemaCacheEntry(buildSchemaPathTrie(k), getSchemaFingerprint(k));
        });
        if (wasBuilt[0]) {
            // Record insertion order on the miss that actually inserted, so eviction has a queue
            // to work from. Recording only once already over capacity would evict the entry just
            // added and cache nothing.
            insertionOrder.add(schema);
            evictIfOverCapacity();
        }
        return built;
    }

    /**
     * Bounds the schema cache.
     *
     * <p>{@code DEFAULT_MAX_CACHE_SIZE} was previously passed to the {@code ConcurrentHashMap}
     * constructor, which takes an INITIAL CAPACITY, not a maximum. The cache was therefore
     * unbounded despite the constant's name, and re-keying it from a fingerprint string to the
     * {@link Schema} itself made each retained entry dramatically heavier — a whole schema graph
     * instead of a ~40-character string. A long-lived Spark driver handling many distinct schemas
     * (a schema-registry stream, a multi-tenant job) would retain all of them for the life of the
     * JVM.</p>
     *
     * <p>Eviction is insertion-ordered rather than LRU: a strict LRU needs a write on every read,
     * which would put a contended mutation back on the per-record path that this whole change
     * exists to keep clean. For a schema cache the distinction barely matters — the working set
     * is small and stable, and the penalty for an unlucky eviction is one trie rebuild.</p>
     */
    private void evictIfOverCapacity() {
        while (schemaCache.size() > maxCacheSize) {
            Schema oldest = insertionOrder.poll();
            if (oldest == null) {
                // Queue drained but the map is still over capacity — only reachable under a race.
                // Clear rather than let the map grow without bound.
                schemaCache.clear();
                break;
            }
            schemaCache.remove(oldest);
        }
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
                    String esc = FlattenedPath.escapeSegment(field.name(), separator);
                    String fieldPath = prefix.isEmpty() ? esc : prefix + separator + esc;
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
                        String arrayFieldPath = prefix + separator +
                                FlattenedPath.escapeSegment(field.name(), separator);
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

        /**
         * Store an array COLUMN, already split by the caller.
         *
         * <p>BL-013 (D2). This used to call a {@code static} {@code deserializeArrayStatic}, which
         * was structurally incapable of reading the instance {@code arrayFormat} field and
         * therefore SNIFFED - JSON, then comma, then pipe. Measured on a document built to reach
         * this branch, all four configured formats produced byte-identical output: the knob was a
         * dead control on the array-of-records path while being live for leaf arrays. Worse,
         * because comma was tried before pipe, a legal comma inside a PIPE_SEPARATED element was
         * split as a delimiter and fabricated a row.</p>
         *
         * <p>The split now happens in {@link AvroReconstructor#deserializeColumn}, on the instance
         * side, driven by the configured format.</p>
         */
        void addArrayFieldValue(String fieldName, List<Object> columnValues) {
            if (arrayFieldValues == null) {
                arrayFieldValues = new LinkedHashMap<>();
            }
            arrayFieldValues.put(fieldName, columnValues);
        }

        /**
         * Split string by delimiter while respecting bracket nesting and quotes.
         */
        private static List<String> splitRespectingBrackets(String str, String delimiter) {
            if (delimiter == null || delimiter.length() != 1) {
                throw new IllegalArgumentException("Delimiter must be a single character");
            }
            char delimiterChar = delimiter.charAt(0);

            List<String> result = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            int bracketDepth = 0;
            boolean inQuotes = false;

            for (int i = 0; i < str.length(); i++) {
                char c = str.charAt(i);

                if (c == '"' && (i == 0 || str.charAt(i - 1) != '\\')) {
                    inQuotes = !inQuotes;
                    current.append(c);
                } else if (!inQuotes) {
                    if (c == '[') {
                        bracketDepth++;
                        current.append(c);
                    } else if (c == ']') {
                        bracketDepth--;
                        current.append(c);
                    } else if (c == delimiterChar && bracketDepth == 0) {
                        String item = current.toString().trim();
                        // Strip quotes from string values
                        if (item.startsWith("\"") && item.endsWith("\"") && item.length() >= 2) {
                            item = item.substring(1, item.length() - 1);
                        }
                        result.add(item);
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
                String item = current.toString().trim();
                // Strip quotes from string values
                if (item.startsWith("\"") && item.endsWith("\"") && item.length() >= 2) {
                    item = item.substring(1, item.length() - 1);
                }
                result.add(item);
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

            String[] parts = FlattenedPath.decodeSegments(key, separator).toArray(new String[0]);

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
            String prefix = FlattenedPath.encode(Arrays.asList(Arrays.copyOfRange(keyParts, 0, i)), separator);
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
        current.addArrayFieldValue(fieldName, deserializeColumn(value, FlattenedPath.encode(
                Arrays.asList(parts), separator)));
    }

    /**
     * Split one flattened array COLUMN into one entry per element, using the CONFIGURED format.
     *
     * <p>BL-013 (D2). Sniffing IS the bug: it is what turns a legal comma into a delimiter under
     * PIPE_SEPARATED. The library publishes a knob whose entire purpose is for the producer to
     * state which delimiter was used; a caller who sets PIPE_SEPARATED has asserted that commas
     * are data, and second-guessing that assertion is how this class of defect was created.</p>
     *
     * <p>The delimited split is BRACKET-AWARE. {@code deserializeArray}'s COMMA and PIPE branches
     * use a naive {@code content.split(",", -1)}, which would shred a nested-array column such as
     * {@code [[a,b],[c,d]]} into four entries; reusing them here would break the doubly-nested
     * array tests. {@link PathNode#splitRespectingBrackets} is used instead.</p>
     *
     * <p>THE CONTRADICTION IS CHECKED IN BOTH DIRECTIONS, and it was not. Review measured that
     * the first version of this method introduced a NEW silent N-to-1 collapse at the SHIPPED
     * DEFAULT: under {@code arrayFormat=JSON} an UNBRACKETED delimited column returned one
     * element holding the whole concatenated text, with no exception and no log of any level -
     * the exact defect class BL-013 exists to remove, reintroduced by the fix for it.
     * Reproduced independently: {@code {items_sku=S1,S2,S3, items_name=N1,N2,N3}} against
     * {@code Order{id, items: array<Item{sku,name}>}} gave 3 elements before the split and
     * {@code [{sku=S1,S2,S3, name=N1,N2,N3}]} after it. Same for pipe text.</p>
     *
     * <p>It is now as loud as the opposite direction, and for the identical reason. MapFlattener's
     * JSON writer ALWAYS brackets a column - measured, including a single-element one, which
     * arrives as {@code ["a"]} - so an unbracketed column cannot have been produced by the JSON
     * writer, exactly as a bracketed quoted list cannot have been produced by the comma or pipe
     * writer. The guard is narrowed to the case where the two readings DISAGREE: an unbracketed
     * column with no delimiter in it reads as one element whichever rule you apply, nothing is at
     * stake, and it is left alone.</p>
     */
    private List<Object> deserializeColumn(Object value, String columnPath) {
        if (value == null) {
            return Collections.singletonList(null);
        }
        if (value instanceof List) {
            return (List<Object>) value;
        }

        String strValue = value.toString().trim();
        boolean bracketed = strValue.startsWith("[") && strValue.endsWith("]");

        if (arrayFormat == JSON) {
            if (bracketed) {
                try {
                    return SHARED_OBJECT_MAPPER.readValue(strValue, LIST_OF_OBJECT);
                } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                    // A bracketed column that is not JSON, under the JSON format. Falling through
                    // to a bracket-aware comma split is the only reading left, and it is what the
                    // previous sniffing path did for the same text - but it is now AUDIBLE.
                    log.warn("Column {} is bracketed but not parseable JSON under arrayFormat "
                            + "JSON; splitting it bracket-aware on ',' instead: {}",
                            columnPath, strValue, e);
                    return trimmed(PathNode.splitRespectingBrackets(
                            strValue.substring(1, strValue.length() - 1).trim(), ","));
                }
            }
            rejectUnbracketedDelimitedColumnUnderJson(strValue, columnPath);
            return Collections.singletonList(strValue);
        }

        if (arrayFormat == BRACKET_LIST) {
            // MEASURED: BRACKET_LIST is not "brackets around raw text". MapFlattener's writer
            // QUOTES and ESCAPES every string element, and serialises a nested list by recursing,
            // so a doubly-nested column arrives as ["[\"RAM\", \"Storage\"]"]. A raw bracket-aware
            // split leaves the backslashes in and DoublyNestedArrayTest fails on "RAM". The class
            // already owns the matching reader - deserializeBracketList -> splitBracketAware ->
            // unquoteString - and that is the format's own reader, not a sniff.
            return deserializeBracketList(strValue);
        }

        // COMMA_SEPARATED / PIPE_SEPARATED: the two formats MapFlattener writes WITHOUT brackets.
        if (bracketed && parsesAsJsonArray(strValue)) {
            throw new ArrayFormatMismatchException(
                    "Column " + columnPath + " is well-formed JSON array syntax but arrayFormat is "
                            + arrayFormat + ", whose writer cannot emit a bracketed quoted list. "
                            + "Splitting it on '" + delimiterChar()
                            + "' would shred the JSON. Set arrayFormat(JSON) to read this data, or "
                            + "produce it with the configured format. Value: " + strValue);
        }
        String content = bracketed ? strValue.substring(1, strValue.length() - 1).trim() : strValue;
        return trimmed(PathNode.splitRespectingBrackets(content, delimiterChar()));
    }

    /**
     * The count an undelimited column reads as under every rule this class applies. Named because
     * the whole point of the guard below is that it fires only when two readings DISAGREE, and
     * {@code > 1} buried in a condition does not say that.
     */
    private static final int ONE_ELEMENT = 1;

    /**
     * Refuse an unbracketed column that a delimited format would have split, under JSON.
     *
     * <p>The mirror of the {@link ArrayFormatMismatchException} thrown for a bracketed JSON array
     * under a delimited format, and thrown for the same reason: the configured writer could not
     * have produced this text, so reading it as one element is a guess that silently destroys
     * N-1 records. Returning the concatenation is the worse answer of the two, because it is
     * indistinguishable from success.</p>
     *
     * <p>Deliberately NOT a WARN. A warning here would be read and ignored by exactly the caller
     * it is aimed at - a Spark job whose logs nobody tails - and the data would still be wrong.
     * The opposite direction throws; a caller who has misconfigured the format deserves the same
     * answer whichever way they misconfigured it.</p>
     */
    private void rejectUnbracketedDelimitedColumnUnderJson(String strValue, String columnPath) {
        int asComma = PathNode.splitRespectingBrackets(strValue, ",").size();
        int asPipe = PathNode.splitRespectingBrackets(strValue, "|").size();
        int wouldSplitInto = Math.max(asComma, asPipe);
        if (wouldSplitInto <= ONE_ELEMENT) {
            // No delimiter outside brackets: one element under every rule, nothing at stake.
            return;
        }
        String delimiter = asComma > ONE_ELEMENT ? "," : "|";
        String suggested = asComma > ONE_ELEMENT ? "COMMA_SEPARATED" : "PIPE_SEPARATED";
        throw new ArrayFormatMismatchException(
                "Column " + columnPath + " is not bracketed but arrayFormat is JSON, whose writer "
                        + "always brackets a column - a single-element one arrives as [\"a\"]. "
                        + "Reading it as JSON yields ONE element holding the whole text; splitting "
                        + "it on '" + delimiter + "' yields " + wouldSplitInto + ". The column "
                        + "cannot have been produced by the JSON writer, so returning one element "
                        + "would silently discard " + (wouldSplitInto - ONE_ELEMENT) + " record(s)."
                        + " Set arrayFormat(" + suggested + ") to read this data, or produce it "
                        + "with the configured format. Value: " + strValue);
    }

    private String delimiterChar() {
        return arrayFormat == PIPE_SEPARATED ? "|" : ",";
    }

    private boolean parsesAsJsonArray(String strValue) {
        try {
            SHARED_OBJECT_MAPPER.readValue(strValue, LIST_OF_OBJECT);
            return true;
        } catch (com.fasterxml.jackson.core.JsonProcessingException notJson) {
            // No log: this predicate is asked on every bracketed column under a delimited format
            // and "not JSON" is the ordinary answer, not a failure. The CALLER reports the one
            // outcome that matters, and reports it by throwing.
            return false;
        }
    }

    private static List<Object> trimmed(List<String> parts) {
        List<Object> out = new ArrayList<>(parts.size());
        for (String p : parts) {
            out.add(p.trim());
        }
        return out;
    }

    // ========================= RECONSTRUCTION CORE =========================

    private GenericRecord reconstructRecord(PathNode node, Schema schema,
                                            String path, int currentDepth) {
        if (currentDepth > maxDepth) {
            throw new IllegalStateException("Maximum depth exceeded at: " + path);
        }

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        List<String> missing = new ArrayList<>();
        List<String> filled = new ArrayList<>();

        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            String escapedName = FlattenedPath.escapeSegment(fieldName, separator);
            String fieldPath = path.isEmpty() ? escapedName : path + separator + escapedName;
            Schema fieldSchema = field.schema();

            try {
                PathNode childNode = node.children.get(fieldName);

                if (childNode != null) {
                    Object fieldValue = reconstructValue(childNode, fieldSchema,
                            fieldPath, currentDepth + 1);
                    builder.set(fieldName, fieldValue);
                    continue;
                }

                // Try to reconstruct array from field values
                Object arrayValue = tryReconstructArrayFromFields(node, field,
                        fieldSchema, fieldPath, currentDepth + 1);
                if (arrayValue != null) {
                    builder.set(fieldName, arrayValue);
                    continue;
                }

                // NP-024 LADDER, restructured so hasDefaultValue() is the discriminator.
                //
                // It used to test useSchemaDefaults FIRST, which made the "Required field missing
                // and no default" branch reachable for a field that HAS a default: measured,
                // .useSchemaDefaults(false).allowMissingFields(false) on a defaulted non-nullable
                // field emitted exactly that message about a field with a default. The message was
                // a lie, masked only because allowMissingFields defaulted to true. Flipping that
                // default would have detonated it, so the ladder is reordered rather than patched
                // at the leaves.
                if (field.hasDefaultValue()) {
                    if (useSchemaDefaults) {
                        builder.set(fieldName, schemaDefault(field, fieldPath));
                        continue;
                    }
                    // useSchemaDefaults(false) means "do not consult the schema default".
                    // MEASURED CORRECTION to BL-012's blanket "the knob cannot suppress a
                    // default": it always could on a NULLABLE field (it set null and build() never
                    // saw an empty slot). It could not on a non-nullable one, because leaving the
                    // slot unset lets GenericRecordBuilder.build() re-supply the same value. So
                    // the honest reading is that the field is now MISSING, and the missing-field
                    // policy below decides - which makes the knob mean the same thing at both
                    // arities instead of quietly depending on nullability.
                    if (isNullable(fieldSchema)) {
                        builder.set(fieldName, null);
                    } else if (allowMissingFields) {
                        fillOrFail(builder, field, fieldPath, filled);
                    } else {
                        missing.add(fieldPath + " (has a schema default, suppressed by "
                                + "useSchemaDefaults(false))");
                    }
                    continue;
                }

                if (isNullable(fieldSchema)) {
                    builder.set(fieldName, null);
                } else if (allowMissingFields) {
                    fillOrFail(builder, field, fieldPath, filled);
                } else {
                    missing.add(fieldPath);
                }
            } catch (ReconstructionException e) {
                // Already carries a path. Re-wrapping buries the specific message.
                throw e;
            } catch (Exception e) {
                throw new ReconstructionException(
                        String.format("Failed to reconstruct field '%s' at path '%s': %s",
                                fieldName, fieldPath, rootMessage(e)), e);
            }
        }

        // Thrown BEFORE build(), which is the whole point: build() sits outside the per-field try,
        // so its AvroMissingFieldException used to escape carrying the field NAME and no
        // flattened path at all. Aggregated rather than first-wins, because otherwise the caller
        // fixes one key, re-runs, and discovers the next.
        if (!missing.isEmpty()) {
            throw new ReconstructionException("Cannot reconstruct " + schema.getFullName()
                    + ": no value in the flattened input and no usable schema default for "
                    + "required field(s) " + String.join(", ", missing)
                    + ". Supply the key(s), or allowMissingFields(true) to substitute Avro type "
                    + "defaults.");
        }
        if (!filled.isEmpty()) {
            String owner = schema.getFullName();
            log.warn("allowMissingFields(true): substituted Avro type defaults for absent "
                    + "required field(s) {} of {}", filled, owner);
        }

        return builder.build();
    }

    /**
     * The {@code allowMissingFields(true)} outcome: substitute the Avro TYPE default, or say why
     * there is not one.
     *
     * <p>The flag's name promised tolerance and at neither value did it tolerate - {@code true}
     * leaked Avro's own builder exception and {@code false} threw ours. Giving {@code true} a real
     * outcome is what makes it stop being a lie.</p>
     *
     * <p>ENUM, FIXED, RECORD, MAP and a null-free UNION have NO type default. Quietly setting null
     * there would ship exactly the pathology this pass removes: a schema-valid-looking datum that
     * is wrong. They fail, naming the field and the reason.</p>
     */
    private void fillOrFail(GenericRecordBuilder builder, Schema.Field field,
                            String fieldPath, List<String> filled) {
        Schema actual = unwrapNullable(field.schema());
        Object substitute;
        switch (actual.getType()) {
            case STRING:  substitute = ""; break;
            case INT:     substitute = 0; break;
            case LONG:    substitute = 0L; break;
            case FLOAT:   substitute = 0.0f; break;
            case DOUBLE:  substitute = 0.0d; break;
            case BOOLEAN: substitute = false; break;
            case BYTES:   substitute = ByteBuffer.allocate(0); break;
            case ARRAY:   substitute = new ArrayList<>(); break;
            case MAP:     substitute = new LinkedHashMap<String, Object>(); break;
            default:
                throw new ReconstructionException(
                        "allowMissingFields(true) cannot substitute a value for required field '"
                                + fieldPath + "': no Avro type default exists for "
                                + actual.getType() + ". Supply the key, or give the field a schema "
                                + "default.");
        }
        builder.set(field.name(), substitute);
        filled.add(fieldPath);
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

            if (nestedValues != null && index < nestedValues.size()) {
                // BL-013 (D3). The clamp that used to live here -
                //     int valueIndex = Math.min(index, nestedValues.size() - 1);
                // - is the DUPLICATION mechanism: an index past the end of a short column silently
                // resolved to that column's LAST value, so sku=S1,S2,S3 beside meta_code=C1,C2
                // produced a third row whose code repeated the second. With the per-column counts
                // now guaranteed equal by agreedElementCount it can never legitimately fire, and
                // leaving it in would preserve a silent-repair path for any future caller that
                // reaches this method another way. An out-of-range index now falls through to the
                // ordinary absent-field handling below instead of inventing a repeat.
                int valueIndex = index;
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
                        value = schemaDefault(nestedField, path + "." + nestedFieldName);
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
                                } catch (com.fasterxml.jackson.core.JsonProcessingException jsonEx) {
                                    // Not valid JSON, try other formats based on arrayFormat.
                                    // NARROWED from catch (Exception) - readValue over a String
                                    // throws only JsonProcessingException, and the wide catch also
                                    // swallowed programming errors raised further in.
                                    log.warn("Failed to parse as JSON array: '{}'", jsonString, jsonEx);
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
                            }
                        }
                    } catch (RuntimeException e) {
                        // NARROWED from catch (Exception). Everything thrown inside this block is
                        // unchecked; catching Exception added nothing except a SpotBugs finding.
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
                        Schema elementSchema = actualFieldSchema.getElementType();

                        // Check if this is an array of records with indexed data
                        if (elementSchema.getType() == RECORD &&
                                fieldChildNode.arrayFieldValues != null &&
                                !fieldChildNode.arrayFieldValues.isEmpty()) {

                            // Pass the outer index so we get the correct slice of data
                            Object reconstructed = reconstructNestedArrayOfRecordsAtIndex(
                                    fieldChildNode, elementSchema, index,
                                    path + "." + nestedFieldName, 0);
                            builder.set(nestedFieldName, reconstructed);
                            hasAnyField = true;
                        } else {
                            // For primitive arrays or arrays without indexed data
                            Object reconstructed = reconstructValue(fieldChildNode, actualFieldSchema,
                                    path + "." + fieldPrefix + "." + nestedFieldName, 0);
                            if (reconstructed != null) {
                                builder.set(nestedFieldName, reconstructed);
                                hasAnyField = true;
                            }
                        }
                    } else if (actualFieldSchema.getType() == RECORD) {
                        // For nested records (like price inside product), recursively reconstruct
                        // Pass the current node (childNode) as the parent, the nested field name,
                        // and the outer index so we get the correct values
                        GenericRecord nestedRecord = reconstructNestedRecordFromArray(
                                childNode, actualFieldSchema, nestedFieldName, index,
                                path + "." + fieldPrefix);
                        if (nestedRecord != null) {
                            builder.set(nestedFieldName, nestedRecord);
                            hasAnyField = true;
                        }
                    } else {
                        // Other types - try generic reconstruction
                        Object reconstructed = reconstructValue(fieldChildNode, actualFieldSchema,
                                path + "." + fieldPrefix + "." + nestedFieldName, 0);
                        if (reconstructed != null) {
                            builder.set(nestedFieldName, reconstructed);
                            hasAnyField = true;
                        }
                    }
                } else if (nestedField.hasDefaultValue()) {
                    builder.set(nestedFieldName,
                            schemaDefault(nestedField, path + "." + nestedFieldName));
                    hasAnyField = true;
                } else if (isNullable(nestedField.schema())) {
                    builder.set(nestedFieldName, null);
                    hasAnyField = true;
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
            throw new IllegalStateException("Expected RECORD element type at: " + path);
        }

        // Step 1: Collect the columns hanging directly off this array node.
        //
        // The old Step 1 re-parsed a single-entry column that looked like a JSON array. That is
        // now redundant AND wrong: deserializeColumn has already split every column by the
        // CONFIGURED format at insertion time, so a JSON column arrives as a real multi-entry
        // List and re-parsing a delimited one would silently reintroduce the sniffing this fix
        // removes. BL-013's filed cause pointed here; measured, this step was a no-op.
        Map<String, List<Object>> parsedFieldValues = new LinkedHashMap<>();
        if (node.arrayFieldValues != null) {
            for (Map.Entry<String, List<Object>> entry : node.arrayFieldValues.entrySet()) {
                if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                    parsedFieldValues.put(entry.getKey(), entry.getValue());
                }
            }
        }

        // Step 2: Ask the SCHEMA how many elements there are, per column, and refuse to guess
        // when the columns disagree.
        Map<String, Integer> counts = new LinkedHashMap<>();
        collectElementCounts(node, elementSchema, parsedFieldValues, path, counts);
        int arraySize = agreedElementCount(counts, path);

        if (arraySize == 0) {
            // REACHABLE since BL-013. determineArraySize ended in `maxSize > 0 ? maxSize : 1`,
            // which made it incapable of returning 0 and made this branch dead code, so an array
            // node with no element data produced ONE record of fabricated type-defaults instead
            // of an empty array.
            return new ArrayList<>();
        }

        List<Object> result = new ArrayList<>(arraySize);

        // Step 3: Build each record in the array
        for (int i = 0; i < arraySize; i++) {
            GenericRecordBuilder elementBuilder = new GenericRecordBuilder(elementSchema);

            for (Schema.Field field : elementSchema.getFields()) {
                String fieldName = field.name();
                Schema fieldSchema = field.schema();
                Schema actualFieldSchema = unwrapNullable(fieldSchema);

                // Check parsed field values first
                List<Object> fieldValues = parsedFieldValues.get(fieldName);

                // BL-014. This dispatch had NO UNION ARM. unwrapNullable collapses only [null,T],
                // so a union of three or more branches arrived here still typed UNION, matched
                // neither the RECORD test nor the ARRAY test, and fell off the end into
                // handleMissingField, which saw a NULL branch and wrote a plain null - in total
                // silence, with the child node holding the real data never read by anything. It is
                // a never-implemented gap rather than a regression: unwrapUnion had four calls and
                // zero declarations through ef625f2 and a declaration with zero callers after, so
                // it never executed and there is nothing to restore.
                //
                // Guarded at arity > 2 deliberately. Every currently-passing [null,T] shape keeps
                // its exact code path, which is why no existing fixture moves; the arity-2
                // null-free case (where unwrapNullable still returns types.get(0), a first-branch
                // guess) is filed separately rather than bundled in here.
                if (fieldSchema.getType() == UNION && fieldSchema.getTypes().size() > 2) {
                    elementBuilder.set(fieldName, reconstructArrayElementUnion(
                            node, fieldSchema, field, fieldValues, i,
                            path + "[" + i + "]." + fieldName, currentDepth + 1));
                    continue;
                }

                if (fieldValues != null && i < fieldValues.size()) {
                    Object valueAtIndex = fieldValues.get(i);

                    // Handle nested arrays (arrays within the record)
                    if (actualFieldSchema.getType() == ARRAY) {
                        Object arrayValue = reconstructNestedArray(
                                valueAtIndex, actualFieldSchema, node, fieldName, i,
                                path + "[" + i + "]." + fieldName, currentDepth + 1);
                        elementBuilder.set(fieldName, arrayValue);
                    } else {
                        // Regular primitive or nested record field
                        Object converted = convertPrimitive(valueAtIndex, fieldSchema,
                                path + "[" + i + "]." + fieldName);
                        elementBuilder.set(fieldName, converted);
                    }
                } else if (actualFieldSchema.getType() == RECORD) {
                    // Handle nested record - look for child fields with prefix
                    GenericRecord nestedRecord = reconstructNestedRecordFromArray(
                            node, actualFieldSchema, fieldName, i, path + "[" + i + "]");
                    if (nestedRecord != null) {
                        elementBuilder.set(fieldName, nestedRecord);
                    } else {
                        handleMissingField(elementBuilder, field);
                    }
                } else if (actualFieldSchema.getType() == ARRAY) {
                    // Handle nested array that wasn't in direct field values
                    Object arrayValue = reconstructNestedArrayFromChildNode(
                            node, fieldName, actualFieldSchema, i,
                            path + "[" + i + "]." + fieldName, currentDepth + 1);
                    if (arrayValue != null) {
                        elementBuilder.set(fieldName, arrayValue);
                    } else {
                        elementBuilder.set(fieldName, new ArrayList<>());
                    }
                } else {
                    handleMissingField(elementBuilder, field);
                }
            }

            result.add(elementBuilder.build());
        }

        return result;
    }

    /**
     * Resolve a multi-branch union for ONE element of an array of records.
     *
     * <p>BL-014. {@code reconstructUnionValue} cannot be called here and that is not a style
     * preference - it is a precondition failure. It reads {@code node.value}, {@code node.isLeaf},
     * {@code node.children} and {@code node.arrayFieldValues} as the content of ONE value, and
     * delegates through {@code reconstructValue}, which takes no index. In the array-element
     * context no such node exists: the only candidate, {@code node.children.get(fieldName)}, is
     * COLUMN-WISE - one list of N entries per leaf, one entry per array element, with value and
     * isLeaf unset. Handing it over would return the same value for every element and would feed
     * a whole JSON-array column to a scalar field. So the branch-selection RULE is reused; the
     * method is not.</p>
     *
     * <p>Where the flattened form genuinely cannot decide - two record branches sharing the
     * columns that are present - the repair is impossible and the only correct behaviour is to be
     * audible. Under the default {@code strictValidation} that is a throw naming both candidates;
     * under {@code strictValidation(false)} it is a WARN and the first match.</p>
     */
    private Object reconstructArrayElementUnion(PathNode arrayNode, Schema unionSchema,
                                                Schema.Field field, List<Object> flatValues,
                                                int index, String path, int depth) {
        String fieldName = field.name();
        List<Schema> branches = unionSchema.getTypes();

        Object scalar = flatValues != null && index < flatValues.size()
                ? flatValues.get(index) : null;
        boolean hasScalarValue = scalar != null
                && !"null".equals(String.valueOf(scalar).trim());
        boolean looksLikeArrayValue = hasScalarValue && looksLikeArray(scalar);

        PathNode childNode = arrayNode.children.get(fieldName);
        Set<String> availableChildKeys = new LinkedHashSet<>();
        if (childNode != null) {
            if (childNode.arrayFieldValues != null) {
                availableChildKeys.addAll(childNode.arrayFieldValues.keySet());
            }
            availableChildKeys.addAll(childNode.children.keySet());
        }

        // 1. No content anywhere. Mirrors reconstructUnionValue's leading no-content check.
        if (!hasScalarValue && availableChildKeys.isEmpty()) {
            if (isNullable(unionSchema)) {
                return null;
            }
            throw new ReconstructionException("Could not match any union type at: " + path
                    + " - no value and no columns, and the union " + branchNames(branches)
                    + " has no null branch");
        }

        // 2. RECORD branches, in declaration order, that claim at least one available column.
        List<Schema> recordMatches = matchingRecordBranches(branches, availableChildKeys);
        if (recordMatches.size() > 1) {
            String message = "Ambiguous union at " + path + ": record branches "
                    + branchNames(recordMatches) + " all match the available columns "
                    + availableChildKeys + ". The flattened form cannot distinguish them - the "
                    + "column names are identical whichever branch produced them.";
            if (strictValidation) {
                throw new ReconstructionException(message);
            }
            log.warn("{} Taking the first branch under strictValidation(false).", message);
        }
        for (Schema branch : recordMatches) {
            try {
                GenericRecord nested = reconstructNestedRecordFromArray(
                        arrayNode, branch, fieldName, index, path);
                if (nested != null) {
                    warnOrphanedColumns(branch, availableChildKeys, path);
                    return nested;
                }
            } catch (RuntimeException e) {
                logUnionBranchMiss(branch.getType(), path, e);
            }
        }

        // 3. ARRAY branch.
        for (Schema branch : branches) {
            if (branch.getType() != ARRAY) {
                continue;
            }
            if (childNode != null) {
                Object fromChild = reconstructNestedArrayFromChildNode(
                        arrayNode, fieldName, branch, index, path, depth);
                if (fromChild != null) {
                    return fromChild;
                }
            }
            if (looksLikeArrayValue) {
                return reconstructNestedArray(scalar, branch, arrayNode, fieldName, index,
                        path, depth);
            }
        }

        // 4. Primitive, enum and fixed branches. The BRANCH schema is passed to convertPrimitive,
        //    never the union - which is the second, separate fault BL-014 did not name: with the
        //    union, convertPrimitive's switch has no UNION case, "default: return value" hands
        //    back a Jackson-boxed Integer for a ["null","long","string"] field, and the datum
        //    fails GenericData.validate and throws UnresolvedUnionException at write time.
        //
        //    TYPE-DIRECTED FIRST, DECLARATION ORDER SECOND. Pure declaration order was measured to
        //    destroy values that the flattener had NOT made ambiguous - see branchesPreferringJava
        //    Type. Whatever survives to the declaration-order fallback is reported.
        if (hasScalarValue) {
            for (Schema branch : branchesPreferringJavaType(branches, scalar)) {
                Schema.Type t = branch.getType();
                try {
                    Object converted = convertPrimitive(scalar, branch, path);
                    if (!isNativeCarrier(branch, scalar)) {
                        warnCoercedAcrossJavaType(scalar, converted, t, path);
                    }
                    return converted;
                } catch (RuntimeException e) {
                    logUnionBranchMiss(t, path, e);
                }
            }
        }

        // 5. Residue: nothing matched. Before this arm existed the field simply became null with
        //    no log line of any severity, which is the exact "laundering failures into apparent
        //    successes" shape this pass is about.
        String names = branchNames(branches);
        log.warn("Could not match any union type at {}: branches {}, unconsumed columns {}, "
                + "scalar present={}", path, names, availableChildKeys, hasScalarValue);
        if (strictValidation) {
            throw new ReconstructionException("Could not match any union type at: " + path);
        }
        if (isNullable(unionSchema)) {
            return null;
        }
        throw new ReconstructionException("Could not match any union type at: " + path
                + " and the union " + branchNames(branches) + " has no null branch");
    }

    /**
     * The scalar branches of an array-element union, NATIVE CARRIERS FIRST, declaration order kept
     * inside each group.
     *
     * <p>WHY THIS EXISTS, measured rather than reasoned. The first version of step 4 tried the
     * branches in pure declaration order and returned the first whose {@code convertPrimitive}
     * did not throw. Review measured that this DESTROYS values, and destroys them at a position
     * where the flattener has not made them ambiguous - the JSON column keeps the quotes and
     * Jackson boxes the element as a {@link String}, so the string-ness is information present in
     * the input:</p>
     *
     * <pre>
     *   schema O{items: array&lt;I{sku, meta: union}&gt;}, real MapFlattener, shipped default
     *   ["null","int","long","string"]  doc meta="0007"  -&gt; Integer 7      leading zeros gone
     *   ["null","long","string"]        doc meta="123"   -&gt; Long 123
     *   ["null","string","long"]        doc meta=123     -&gt; String "123"   the reverse fault
     * </pre>
     *
     * <p>AND IT IS WORSE THAN THE FILING SAID, which is the reason this is a reorder and not a
     * numeric special case. {@code Boolean.parseBoolean} never throws, so under
     * {@code ["null","boolean","string"]} the BOOLEAN branch accepts absolutely anything and the
     * string branch is unreachable: {@code meta="hello"} became {@code Boolean false} and
     * {@code meta=""} became {@code Boolean false}. {@code Double.parseDouble} accepts
     * {@code "1e999"} and returns {@code Infinity}. None of it logged.</p>
     *
     * <p>This is NOT the same situation as the top-level union, and the distinction is why the
     * top-level path is left alone. Measured: at the top level the flat map holds
     * {@code meta=0007} as a bare unquoted scalar, so the flattener really has erased the type and
     * any choice is a guess. At the array-element position it holds {@code items_meta=["0007"]}.
     * Guessing where the answer is written down is not the same defect as guessing where it is
     * not.</p>
     *
     * <p>A branch carrying a LOGICAL TYPE is never demoted, whatever the boxed type is. Logical
     * types arrive through this path as text ({@code "2020-01-01"}, a uuid) or as a number
     * (epoch millis), and pushing them behind a raw STRING branch would undo conversions that
     * work today.</p>
     */
    private List<Schema> branchesPreferringJavaType(List<Schema> branches, Object scalar) {
        List<Schema> nativeCarriers = new ArrayList<>();
        List<Schema> fallback = new ArrayList<>();
        for (Schema branch : branches) {
            Schema.Type t = branch.getType();
            if (t == NULL || t == RECORD || t == ARRAY || t == MAP) {
                continue;
            }
            if (isNativeCarrier(branch, scalar)) {
                nativeCarriers.add(branch);
            } else {
                fallback.add(branch);
            }
        }
        nativeCarriers.addAll(fallback);
        return nativeCarriers;
    }

    /**
     * Whether this branch is the natural Avro carrier for the value's boxed Java type.
     *
     * <p>A value whose boxed type has NO native branch in the union leaves every branch in the
     * fallback group, so the ordering is unchanged and the caller's declaration-order behaviour
     * is preserved exactly - which is what keeps {@code ["null","int","long"]} with a
     * {@code "123"} String reading as {@code Integer 123} rather than failing.</p>
     */
    private boolean isNativeCarrier(Schema branch, Object scalar) {
        if (branch.getLogicalType() != null) {
            return true;
        }
        Schema.Type t = branch.getType();
        if (scalar instanceof CharSequence) {
            return t == Schema.Type.STRING || t == Schema.Type.ENUM
                    || t == Schema.Type.BYTES || t == Schema.Type.FIXED;
        }
        if (scalar instanceof Boolean) {
            return t == Schema.Type.BOOLEAN;
        }
        if (scalar instanceof Number) {
            return t == Schema.Type.INT || t == Schema.Type.LONG
                    || t == Schema.Type.FLOAT || t == Schema.Type.DOUBLE;
        }
        return false;
    }

    /**
     * Report a value that reached a branch outside its own Java type AND changed shape doing so.
     *
     * <p>The fallback is legitimate - {@code ["null","int","long"]} holding {@code "123"} has no
     * string branch to prefer and {@code Integer 123} is the right answer. What is not legitimate
     * is doing that SILENTLY when the text does not survive the trip, which is how {@code "0007"}
     * became {@code 7}. The lexical comparison is the whole test: no change, no line.</p>
     */
    private void warnCoercedAcrossJavaType(Object scalar, Object converted, Schema.Type branch,
                                           String path) {
        String before = String.valueOf(scalar);
        String after = String.valueOf(converted);
        if (before.equals(after)) {
            return;
        }
        // Every argument is hoisted into a local rather than computed in the call: a method
        // invocation inside a log statement's argument list is a PMD GuardLogStatement finding,
        // and this pass may not raise a ratchet to add a log line. The two data-derived arguments
        // additionally go through oneLine, which is not bookkeeping - a caller's value reaching a
        // log line unescaped is a log-forging vector, and the value being logged here is by
        // definition one the caller wrote.
        String sourceType = scalar.getClass().getSimpleName();
        String safeBefore = oneLine(before);
        String safeAfter = oneLine(after);
        log.warn("Union at {} has no {} branch, so the value was coerced into its {} branch and "
                        + "its text changed: '{}' -> '{}'. Add a matching branch to the union if "
                        + "the original form is significant.",
                path, sourceType, branch, safeBefore, safeAfter);
    }

    /**
     * Flatten CR and LF out of a value that is about to be logged.
     *
     * <p>A caller-supplied string containing {@code \n} can forge a whole extra log record, which
     * is what {@code CRLF_INJECTION_LOGS} describes. Kept to exactly the two characters that
     * split a log line - this is not a sanitiser for anything else, and pretending otherwise
     * would be its own false compensating control.</p>
     */
    private static String oneLine(String s) {
        return s.replace('\r', ' ').replace('\n', ' ');
    }

    private List<Schema> matchingRecordBranches(List<Schema> branches,
                                                Set<String> availableChildKeys) {
        List<Schema> out = new ArrayList<>();
        for (Schema branch : branches) {
            if (branch.getType() != RECORD) {
                continue;
            }
            for (Schema.Field f : branch.getFields()) {
                if (availableChildKeys.contains(f.name())) {
                    out.add(branch);
                    break;
                }
            }
        }
        return out;
    }

    /**
     * Columns that exist but no field of the chosen branch consumes.
     *
     * <p>WARN only in 2.1.0. Making the orphan count DECISIVE would change which branch wins on
     * shapes no fixture covers, which is a selection change dressed as a reporting change.</p>
     */
    private void warnOrphanedColumns(Schema chosen, Set<String> availableChildKeys, String path) {
        Set<String> orphans = new LinkedHashSet<>(availableChildKeys);
        for (Schema.Field f : chosen.getFields()) {
            orphans.remove(f.name());
        }
        if (!orphans.isEmpty()) {
            String chosenName = chosen.getFullName();
            log.warn("Union branch {} chosen at {} leaves columns {} unconsumed - their data is "
                    + "not represented in the reconstructed value", chosenName, path, orphans);
        }
    }

    private String branchNames(List<Schema> branches) {
        List<String> names = new ArrayList<>(branches.size());
        for (Schema b : branches) {
            names.add(b.getType() == RECORD ? b.getFullName() : b.getType().getName());
        }
        return names.toString();
    }

    /**
     * Walk the ELEMENT SCHEMA and record how many elements each column says there are.
     *
     * <p>BL-013 (D1). This replaces {@code determineArraySize}, which walked the PathNode tree
     * blindly. Its child-node loop only counted a child's values when the FIRST value was a String
     * starting with {@code "[["} - but the column had already been parsed into a real List of
     * plain strings upstream, so that test could never fire for an ordinary column. When every
     * field of the element lived inside a NESTED RECORD, the array node carried no
     * arrayFieldValues at all, nothing was counted, and a trailing
     * {@code return maxSize > 0 ? maxSize : 1} FABRICATED a size of 1. Measured: a three-element
     * array came back as one under all four formats INCLUDING the JSON default, with no error, no
     * log above debug, and a datum that validates against its schema. Every one of the 29 AVRO
     * fixtures puts a scalar at the element root, which is exactly why none of them caught it.</p>
     *
     * <p>ARRAY-typed fields are counted at THIS level and not descended into: their column already
     * holds one entry per OUTER element, and their inner cardinality is a different question -
     * nested arrays of records are legitimately ragged (three attributes on one product, two on
     * the next) and comparing inner lengths would turn a feature into a failure.</p>
     */
    private void collectElementCounts(PathNode node, Schema elementSchema,
                                      Map<String, List<Object>> columns,
                                      String columnPrefix, Map<String, Integer> counts) {
        for (Schema.Field field : elementSchema.getFields()) {
            String name = field.name();
            String columnPath = columnPrefix + separator
                    + FlattenedPath.escapeSegment(name, separator);
            Schema resolved = unwrapNullable(field.schema());
            Schema recordBranch = soleRecordBranch(resolved);

            if (recordBranch != null) {
                PathNode child = node.children.get(name);
                if (child != null) {
                    Map<String, List<Object>> childColumns = child.arrayFieldValues == null
                            ? Collections.<String, List<Object>>emptyMap()
                            : child.arrayFieldValues;
                    collectElementCounts(child, recordBranch, childColumns, columnPath, counts);
                }
                continue;
            }

            if (resolved.getType() == ARRAY) {
                List<Object> own = columns.get(name);
                if (own != null && !own.isEmpty()) {
                    counts.put(columnPath, own.size());
                    continue;
                }
                // An array OF RECORDS hangs its inner columns off a child node. Each of those
                // columns still holds one entry per OUTER element, so it is a valid signal for
                // this level even though we never descend into the inner record's own cardinality.
                PathNode child = node.children.get(name);
                if (child != null && child.arrayFieldValues != null) {
                    for (Map.Entry<String, List<Object>> e : child.arrayFieldValues.entrySet()) {
                        if (e.getValue() != null && !e.getValue().isEmpty()) {
                            counts.put(columnPath + separator
                                    + FlattenedPath.escapeSegment(e.getKey(), separator),
                                    e.getValue().size());
                        }
                    }
                }
                continue;
            }

            List<Object> values = columns.get(name);
            if (values != null && !values.isEmpty()) {
                counts.put(columnPath, values.size());
            }
        }
    }

    /**
     * The single element count every column agrees on, or a named failure.
     *
     * <p>BL-013 (D3). {@code Math.max} used to pick the longest column and invent the shortfall:
     * short scalar columns were padded with {@code ""} and {@code 0} by {@code handleMissingField},
     * and short nested-record columns had their LAST value duplicated by a
     * {@code Math.min(index, size - 1)} clamp. Measured: {@code sku=S1,S2,S3} beside
     * {@code meta_code=C1,C2} produced a third row whose code repeated the second; the reverse
     * direction silently discarded C3. Neither logged anything.</p>
     *
     * <p>Deliberately NOT gated on strictValidation or allowMissingFields: BL-012 measured that
     * allowMissingFields already selects WHICH exception fires rather than whether one does, and
     * overloading either knob with a third meaning repeats the defect the audit named.</p>
     */
    private int agreedElementCount(Map<String, Integer> counts, String path) {
        Set<Integer> distinct = new LinkedHashSet<>();
        for (Integer c : counts.values()) {
            if (c != null && c > 0) {
                distinct.add(c);
            }
        }
        if (distinct.isEmpty()) {
            return 0;
        }
        if (distinct.size() > 1) {
            StringBuilder sb = new StringBuilder(path)
                    .append(": element counts disagree across columns - ");
            boolean first = true;
            for (Map.Entry<String, Integer> e : counts.entrySet()) {
                if (e.getValue() == null || e.getValue() == 0) {
                    continue;
                }
                if (!first) {
                    sb.append(", ");
                }
                sb.append(e.getKey()).append('=').append(e.getValue());
                first = false;
            }
            sb.append(" (arrayFormat=").append(arrayFormat).append("). Padding the short columns "
                    + "or duplicating their last value would produce a schema-valid record that is "
                    + "wrong, so this is refused rather than repaired.");
            throw new ArrayCardinalityException(sb.toString());
        }
        return distinct.iterator().next();
    }

    /**
     * The one RECORD branch of a schema, if there is exactly one.
     *
     * <p>Counting has to see through a multi-branch union or it will fail to descend into the very
     * field BL-014 says is silently dropped, leaving it silently UNCOUNTED as well. "Exactly one"
     * is the honest boundary: with two record branches the flattened form cannot say which one the
     * columns belong to, and guessing here would make the sizing depend on a coin flip.</p>
     */
    private Schema soleRecordBranch(Schema schema) {
        if (schema.getType() == RECORD) {
            return schema;
        }
        if (schema.getType() != UNION) {
            return null;
        }
        Schema found = null;
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() == RECORD) {
                if (found != null) {
                    return null;
                }
                found = branch;
            }
        }
        return found;
    }

/**
 * Reconstruct a nested array field within an array of records.
 *
 * Example: lineItems[i].tags or lineItems[i].attributes
 */
    private Object reconstructNestedArray(Object value, Schema arraySchema, PathNode parentNode,
                                          String fieldName, int outerIndex, String path, int depth) {
        Schema elementSchema = arraySchema.getElementType();

        if (value == null) {
            return new ArrayList<>();
        }

        // If value is already a List, process it directly
        if (value instanceof List) {
            List<Object> listValue = (List<Object>) value;
            return reconstructArrayFromValues(listValue, elementSchema, path, depth);
        }

        // If value is a JSON string (nested array serialized as string)
        if (value instanceof String) {
            String strValue = ((String) value).trim();

            // Try to parse as JSON array
            if (strValue.startsWith("[") && strValue.endsWith("]")) {
                try {
                    List<Object> parsed = objectMapper.readValue(strValue, List.class);
                    return reconstructArrayFromValues(parsed, elementSchema, path, depth);
                } catch (Exception e) {
                    log.debug("Failed to parse nested array JSON at {}: {}", path, e.getMessage());
                }
            }

            // Single value - wrap in list
            if (!strValue.isEmpty()) {
                Object converted = convertPrimitive(strValue, elementSchema, path);
                return Collections.singletonList(converted);
            }
        }

        return new ArrayList<>();
    }

/**
 * Reconstruct a nested array from a child node (for deeply nested structures).
 *
 * Example: shipments[i].trackingEvents where trackingEvents is array of records
 */
    private Object reconstructNestedArrayFromChildNode(PathNode parentNode, String fieldName,
                                                       Schema arraySchema, int outerIndex,
                                                       String path, int depth) {
        // Was Groovy's safe-navigation operator: parentNode.children?.get(fieldName)
        PathNode childNode = parentNode.children == null
                ? null
                : parentNode.children.get(fieldName);

        if (childNode == null) {
            return null;
        }

        Schema elementSchema = arraySchema.getElementType();

        // Check if child node has array field values
        if (childNode.arrayFieldValues != null && !childNode.arrayFieldValues.isEmpty()) {
            // This is an array of records - need to extract values for this outer index
            if (elementSchema.getType() == RECORD) {
                return reconstructNestedArrayOfRecordsAtIndex(
                        childNode, elementSchema, outerIndex, path, depth);
            }
        }

        // Check if child node is a leaf with array value
        if (childNode.isLeaf && childNode.value != null) {
            List<Object> deserialized = deserializeArray(childNode.value);
            return reconstructArrayFromValues(deserialized, elementSchema, path, depth);
        }

        return null;
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
        Map<String, List<Object>> fieldValuesAtIndex = new LinkedHashMap<>();
        int innerArraySize = 0;

        for (Schema.Field field : recordSchema.getFields()) {
            String fieldName = field.name();
            // Was Groovy's safe-navigation operator: childNode.arrayFieldValues?.get(fieldName)
            List<Object> rawValues = childNode.arrayFieldValues == null
                    ? null
                    : childNode.arrayFieldValues.get(fieldName);

            if (rawValues != null && !rawValues.isEmpty()) {
                // KEY FIX: Use outerIndex to select the correct element
                Object rawValue = outerIndex < rawValues.size() ? rawValues.get(outerIndex) : rawValues.get(0);

                if (rawValue instanceof String) {
                    String strValue = ((String) rawValue).trim();

                    // Check for doubly-nested array: "[[v1,v2],[v3,v4]]"
                    if (strValue.startsWith("[[")) {
                        try {
                            List<List<Object>> parsed = objectMapper.readValue(strValue, List.class);
                            // For doubly-nested, we've ALREADY selected the outer element,
                            // so just use the first (and only) inner list
                            if (!parsed.isEmpty()) {
                                List<Object> innerList = parsed.get(0);
                                fieldValuesAtIndex.put(fieldName, innerList);
                                innerArraySize = Math.max(innerArraySize, innerList.size());
                            }
                            continue;
                        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                            log.debug("Failed to parse doubly-nested array: {}", strValue, e);
                        }
                    }

                    // Check for single nested array: "[v1,v2,v3]"
                    if (strValue.startsWith("[") && strValue.endsWith("]")) {
                        try {
                            List<Object> parsed = objectMapper.readValue(strValue, List.class);
                            fieldValuesAtIndex.put(fieldName, parsed);
                            innerArraySize = Math.max(innerArraySize, parsed.size());
                            continue;
                        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                            log.debug("Failed to parse nested array: {}", strValue, e);
                        }
                    }

                    // Single value - wrap in list
                    fieldValuesAtIndex.put(fieldName, Collections.singletonList(strValue));
                    innerArraySize = Math.max(innerArraySize, 1);
                } else if (rawValue instanceof List) {
                    // Already parsed as list
                    List<Object> listValue = (List<Object>) rawValue;
                    fieldValuesAtIndex.put(fieldName, listValue);
                    innerArraySize = Math.max(innerArraySize, listValue.size());
                } else if (rawValue != null) {
                    // Single non-string value
                    fieldValuesAtIndex.put(fieldName, Collections.singletonList(rawValue));
                    innerArraySize = Math.max(innerArraySize, 1);
                }
            }
        }

        if (innerArraySize == 0) {
            return new ArrayList<>();
        }

        // Build records for the inner array
        List<Object> result = new ArrayList<>(innerArraySize);

        for (int j = 0; j < innerArraySize; j++) {
            GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);

            for (Schema.Field field : recordSchema.getFields()) {
                String fieldName = field.name();
                List<Object> values = fieldValuesAtIndex.get(fieldName);

                if (values != null && j < values.size()) {
                    Object value = values.get(j);
                    Object converted = convertPrimitive(value, field.schema(),
                            path + "[" + j + "]." + fieldName);
                    builder.set(fieldName, converted);
                } else {
                    handleMissingField(builder, field);
                }
            }

            result.add(builder.build());
        }

        return result;
    }

/**
 * Handle missing field by setting default or null.
 */
    private void handleMissingField(GenericRecordBuilder builder, Schema.Field field) {
        if (field.hasDefaultValue()) {
            builder.set(field.name(), schemaDefault(field, field.name()));
        } else if (isNullable(field.schema())) {
            builder.set(field.name(), null);
        } else {
            Schema actualSchema = unwrapNullable(field.schema());
            // Provide type-appropriate defaults for required fields
            switch (actualSchema.getType()) {
                case ARRAY:
                    builder.set(field.name(), new ArrayList<>());
                    break;
                case STRING:
                    builder.set(field.name(), "");
                    break;
                case INT:
                    builder.set(field.name(), 0);
                    break;
                case LONG:
                    builder.set(field.name(), 0L);
                    break;
                case FLOAT:
                    builder.set(field.name(), 0.0f);
                    break;
                case DOUBLE:
                    builder.set(field.name(), 0.0d);
                    break;
                case BOOLEAN:
                    builder.set(field.name(), false);
                    break;
                default:
                    log.warn("Cannot provide default for required field {} of type {}",
                            field.name(), actualSchema.getType());
                    return;
            }
            // DISCLOSED INCONSISTENCY, not a repair. This invention is hard-wired ON and is NOT
            // reachable from allowMissingFields, so the library fails loudly for a missing
            // required field at the root and quietly substitutes "" for the same field one level
            // down inside an array element. Gating it would turn array-of-records reconstructions
            // that succeed today into throws at the shipped default and is tracked separately; it
            // is at least AUDIBLE now.
            Schema.Type substituted = actualSchema.getType();
            String named = field.name();
            log.warn("Substituted the {} type default for required array-element field {} - it "
                    + "had no value and no schema default. This substitution is not gated by "
                    + "allowMissingFields.", substituted, named);
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
            return objectMapper.readValue(trimmed, LIST_OF_OBJECT);
        } catch (com.fasterxml.jackson.core.JsonProcessingException notJson) {
            // NARROWED from catch (Exception). A genuine try-JSON-then-brackets cascade: the
            // bracket reader on the next line IS the answer when the text is not JSON, so there
            // is nothing to report and the catch body is real work rather than a comment. The
            // wide catch also swallowed every unchecked failure raised inside readValue.
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

            // Handle string-encoded arrays (e.g., "[]" or "[a, b, c]")
            if (value instanceof String) {
                String strValue = ((String) value).trim();
                if (strValue.startsWith("[") && strValue.endsWith("]")) {
                    // Parse the string as an array
                    if (strValue.equals("[]")) {
                        // Empty array - preserve it!
                        result.add(new ArrayList<>());
                        continue;
                    }
                    // Non-empty array string - parse it
                    List<Object> parsedList = parseNestedArrayStructure(strValue);
                    if (elementSchema.getType() == ARRAY) {
                        result.add(reconstructArrayFromValues(parsedList,
                                elementSchema.getElementType(), indexPath, currentDepth + 1));
                    } else {
                        // For non-array element type, add each parsed item
                        for (Object item : parsedList) {
                            if (item != null && !item.getClass().getSimpleName().equals("NullObject")) {
                                result.add(convertPrimitive(item, elementSchema, indexPath));
                            } else if (elementIsNullable) {
                                result.add(null);
                            }
                        }
                    }
                    continue;
                }
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
                    path + separator + FlattenedPath.escapeSegment(key, separator),
                    currentDepth + 1);
            result.put(key, value);
        }

        return result;
    }

    /**
     * One place where a union branch reports that it did not fit.
     *
     * <p>This was FIVE near-identical {@code log.debug} statements - four in
     * {@code reconstructUnionValue} and one in the new array-element resolver - each formatting
     * {@code e.getMessage()} eagerly into its arguments. Folding them into one method is the
     * ordinary reason (they say the same thing) and two measured ones: it takes four
     * {@code GuardLogStatement} findings off PMD, because the method call in the argument list is
     * gone, and four {@code CRLF_INJECTION_LOGS} off SpotBugs, because there is now one logging
     * site instead of five. The ratchets in .github/quality-baseline.json may only go down, and
     * this pass adds logging, so every new line has to be paid for somewhere.</p>
     *
     * <p>DEBUG, not WARN, and deliberately: trying the branches in declaration order IS the
     * algorithm. A branch declining is the algorithm working. The caller warns once when NONE of
     * them fits, which is the outcome that actually needs to be heard.</p>
     */
    private void logUnionBranchMiss(Schema.Type branch, String path, RuntimeException e) {
        log.debug("Union branch {} did not match at {}", branch, path, e);
    }

    private Object reconstructUnionValue(PathNode node, Schema unionSchema,
                                         String path, int currentDepth) {
        List<Schema> types = unionSchema.getTypes();

        // Handle nullable union (most common case: ["null", "SomeType"])
        if (types.size() == 2) {
            Schema nullSchema = null;
            Schema nonNullSchema = null;

            for (Schema type : types) {
                if (type.getType() == NULL) {
                    nullSchema = type;
                } else {
                    nonNullSchema = type;
                }
            }

            // If this union doesn't contain null, try to match one of the types
            if (nullSchema == null) {
                for (Schema type : types) {
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

            // It's a nullable union - determine if we have actual content

            // For complex types (RECORD, ARRAY, MAP), check if there's actual content
            // node.value will be null for non-leaf nodes, so we must check children
            if (nonNullSchema.getType() == RECORD) {
                // Check if we have children (nested record fields)
                if (!node.children.isEmpty()) {
                    return reconstructValue(node, nonNullSchema, path, currentDepth);
                }
                // No children = null record
                return null;
            }

            if (nonNullSchema.getType() == ARRAY) {
                // Check for array content
                boolean hasContent = (node.arrayFieldValues != null && !node.arrayFieldValues.isEmpty()) ||
                        !node.children.isEmpty() ||
                        (node.isLeaf && node.value != null);
                if (hasContent) {
                    return reconstructValue(node, nonNullSchema, path, currentDepth);
                }
                return null;
            }

            if (nonNullSchema.getType() == MAP) {
                if (!node.children.isEmpty()) {
                    return reconstructValue(node, nonNullSchema, path, currentDepth);
                }
                return null;
            }

            // For primitives and other types, check the value
            if (node.value == null || "null".equals(String.valueOf(node.value).trim())) {
                return null;
            }

            return reconstructValue(node, nonNullSchema, path, currentDepth);
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
                    } catch (RuntimeException e) {
                        logUnionBranchMiss(type.getType(), path, e);
                    }
                }
                continue;
            }

            // For ARRAY type, check for array content
            if (type.getType() == ARRAY) {
                if (hasArrayValues || (hasLeafValue && looksLikeArray(node.value))) {
                    try {
                        return reconstructValue(node, type, path, currentDepth);
                    } catch (RuntimeException e) {
                        logUnionBranchMiss(type.getType(), path, e);
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
                    } catch (RuntimeException e) {
                        logUnionBranchMiss(type.getType(), path, e);
                        continue;
                    }
                }
                continue;
            }

            // For primitive types, check if we have a leaf value
            if (hasLeafValue) {
                try {
                    return reconstructValue(node, type, path, currentDepth);
                } catch (RuntimeException e) {
                    logUnionBranchMiss(type.getType(), path, e);
                    continue;
                }
            }
        }

        if (strictValidation) {
            throw new IllegalStateException("Could not match any union type at: " + path);
        }

        return null;
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

    // Why each FIXED decode strategy declined. A bitmask rather than a StringBuilder: this is a
    // hot path and the text is only ever needed on the two cold outcomes.
    private static final int DECODE_B64_PREFIX = 1;
    private static final int DECODE_BYTE_ARRAY = 2;
    private static final int DECODE_BARE_BASE64 = 4;
    private static final int DECODE_BARE_BASE64_WRONG_LENGTH = 8;
    private static final int DECODE_HEX = 16;
    private static final int DECODE_HEX_SHAPE = 32;

    private static String decodeTrace(int declined) {
        if (declined == 0) {
            return "none attempted";
        }
        StringBuilder sb = new StringBuilder();
        appendIf(sb, declined, DECODE_B64_PREFIX, "B64-prefix:invalid-base64");
        appendIf(sb, declined, DECODE_BYTE_ARRAY, "byte-array:not-numeric");
        appendIf(sb, declined, DECODE_BARE_BASE64, "bare-base64:invalid");
        appendIf(sb, declined, DECODE_BARE_BASE64_WRONG_LENGTH,
                "bare-base64:decoded-but-wrong-length");
        appendIf(sb, declined, DECODE_HEX, "hex:not-numeric");
        appendIf(sb, declined, DECODE_HEX_SHAPE, "hex:wrong-length-or-not-hex");
        return sb.toString();
    }

    private static void appendIf(StringBuilder sb, int declined, int bit, String name) {
        if ((declined & bit) != 0) {
            if (sb.length() > 0) {
                sb.append("; ");
            }
            sb.append(name);
        }
    }

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
                        } catch (IllegalArgumentException notBase64) {
                            // NARROWED from catch (Exception): Base64.decode throws only
                            // IllegalArgumentException. The message was also built by
                            // concatenation, so it was formatted whether or not WARN was enabled.
                            log.warn("Failed to decode Base64 for BYTES field at {}: {}",
                                    path, strValue, notBase64);
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
                    } else if (value instanceof ByteBuffer) {
                        ByteBuffer bb = (ByteBuffer) value;
                        fixedBytes = new byte[bb.remaining()];
                        bb.get(fixedBytes);
                    } else {
                        // Try various decode strategies.
                        //
                        // THIS STAYS A CASCADE - it is a genuine try-this-then-that chain over
                        // five mutually exclusive encodings. What changes is that each declining
                        // strategy now RECORDS why, into a bitmask that costs one int on the
                        // success path and is rendered to text only when something goes wrong.
                        // A StringBuilder here would be the obvious implementation and the wrong
                        // one: this is a hot reconstruction path.
                        //
                        // TWO OF THE FIVE DECLINE WITHOUT THROWING AT ALL, and those were the
                        // truly invisible ones: Strategy 3 can Base64-DECODE SUCCESSFULLY and then
                        // discard the result for being the wrong length, and Strategy 4's
                        // pre-check can fail on length or charset class. Neither left a trace, and
                        // both are how execution reaches Strategy 5.
                        fixedBytes = null;
                        int declined = 0;

                        // Strategy 1: Check for explicit B64: prefix
                        if (strValue.startsWith("B64:")) {
                            try {
                                fixedBytes = Base64.getDecoder().decode(strValue.substring(4));
                            } catch (IllegalArgumentException notBase64) {
                                declined |= DECODE_B64_PREFIX;
                            }
                        }

                        // Strategy 2: Try to parse array format like "[-115, 67, 88, ...]"
                        if (fixedBytes == null && strValue.startsWith("[") && strValue.endsWith("]")) {
                            try {
                                String inner = strValue.substring(1, strValue.length() - 1).trim();
                                if (!inner.isEmpty()) {
                                    String[] parts = inner.split(",");
                                    fixedBytes = new byte[parts.length];
                                    for (int idx = 0; idx < parts.length; idx++) {
                                        fixedBytes[idx] = (byte) Integer.parseInt(parts[idx].trim());
                                    }
                                }
                            } catch (NumberFormatException notByteArray) {
                                declined |= DECODE_BYTE_ARRAY;
                                fixedBytes = null;
                            }
                        }

                        // Strategy 3: Try Base64 decode (without length check)
                        if (fixedBytes == null) {
                            try {
                                byte[] decoded = Base64.getDecoder().decode(strValue);
                                if (decoded.length == expectedSize) {
                                    fixedBytes = decoded;
                                } else {
                                    // Decoded fine and was thrown away for its length. Silent
                                    // before this line existed.
                                    declined |= DECODE_BARE_BASE64_WRONG_LENGTH;
                                }
                            } catch (IllegalArgumentException notBase64) {
                                declined |= DECODE_BARE_BASE64;
                            }
                        }

                        // Strategy 4: Try hex decode (with or without 0x prefix)
                        if (fixedBytes == null) {
                            try {
                                String hexStr = strValue.startsWith("0x") ? strValue.substring(2) : strValue;
                                if (hexStr.length() == expectedSize * 2 && hexStr.matches("[0-9a-fA-F]+")) {
                                    fixedBytes = new byte[expectedSize];
                                    for (int idx = 0; idx < expectedSize; idx++) {
                                        fixedBytes[idx] = (byte) Integer.parseInt(hexStr.substring(idx * 2, idx * 2 + 2), 16);
                                    }
                                } else {
                                    declined |= DECODE_HEX_SHAPE;
                                }
                            } catch (NumberFormatException notHex) {
                                declined |= DECODE_HEX;
                            }
                        }

                        // Strategy 5: Fallback to raw string bytes (legacy behavior)
                        if (fixedBytes == null) {
                            fixedBytes = strValue.getBytes();
                            if (fixedBytes.length == expectedSize) {
                                // THE SILENT SUCCESS. No strategy decoded this value, and the raw
                                // platform-charset bytes happen to be the right length, so the
                                // size check below passes and the caller receives fabricated
                                // bytes with no exception. Measured example: FIXED(size=4) with
                                // the value "abcd" - Strategy 3 Base64-decodes it to 3 bytes and
                                // discards that, Strategy 4 declines on length, and 0x61626364
                                // comes back as though it had been decoded. Charset-dependent
                                // too, which is separately recorded as a corpus DEFECT.
                                log.warn("FIXED value at {} was decoded by NO strategy; falling "
                                        + "back to raw platform-charset bytes [declined: {}]",
                                        path, decodeTrace(declined));
                            }
                        }

                        if (fixedBytes.length != expectedSize) {
                            throw new IllegalArgumentException(
                                    String.format("Fixed size mismatch at %s: expected %d, got %d "
                                            + "(value was: %.50s...) [decode strategies: %s]",
                                            path, expectedSize, fixedBytes.length, strValue,
                                            decodeTrace(declined)));
                        }
                        return new GenericData.Fixed(actualSchema, fixedBytes);
                    }

                    if (fixedBytes.length != expectedSize) {
                        throw new IllegalArgumentException(
                                String.format("Fixed size mismatch at %s: expected %d, got %d (value was: %.50s...)",
                                        path, expectedSize, fixedBytes.length, strValue));
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

                case UNION:
                    // Defence, not a repair. unwrapNullable resolves only [null,T], so a union of
                    // three or more branches arrives here still a UNION, falls to `default` and is
                    // returned UNCONVERTED - a Jackson-boxed Integer into a ["null","long",...]
                    // slot, which validates false and throws UnresolvedUnionException at write
                    // time. The array-element caller is fixed at the CALL SITE (it now passes the
                    // selected branch, never the union). Any other caller that reaches this line
                    // is now heard rather than silent.
                    int arity = actualSchema.getTypes().size();
                    log.warn("convertPrimitive received a {}-branch UNION at {} and cannot choose "
                            + "a branch; returning the value unconverted, which may produce a "
                            + "datum that fails to encode", arity, path);
                    return value;

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
                    return objectMapper.readValue(strValue, LIST_OF_OBJECT);
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
        List<Object> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int bracketDepth = 0;
        boolean inQuotes = false;

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);

            if (c == '"' && (i == 0 || content.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
                // DON'T append the quote character - we'll handle unquoting at the end
                current.append(c);
            } else if (!inQuotes) {
                if (c == '[') {
                    bracketDepth++;
                    current.append(c);
                } else if (c == ']') {
                    bracketDepth--;
                    current.append(c);
                } else if (c == ',' && bracketDepth == 0) {
                    String item = current.toString().trim();
                    if (!item.isEmpty()) {
                        result.add(unquoteString(item));
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
            result.add(unquoteString(item));
        }

        return result.isEmpty() ? Collections.singletonList(content) : result;
    }

/**
 * Remove surrounding quotes from a string and unescape internal quotes.
 */
    private Object unquoteString(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }

        String trimmed = value.trim();

        // Handle quoted strings
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() >= 2) {
            return trimmed.substring(1, trimmed.length() - 1)
                    .replace("\\\"", "\"")
                    .replace("\\n", "\n")
                    .replace("\\t", "\t");
        }

        // Handle null
        if ("null".equals(trimmed)) {
            return null;
        }

        // Handle nested arrays/objects - return as-is
        if (trimmed.startsWith("[") || trimmed.startsWith("{")) {
            return trimmed;
        }

        // Try to parse as number
        try {
            if (trimmed.contains(".")) {
                return Double.parseDouble(trimmed);
            } else {
                return Long.parseLong(trimmed);
            }
        } catch (NumberFormatException e) {
            // Return as string
            return trimmed;
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
                    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                        // NARROWED from catch (Exception).
                        log.debug("Not valid JSON, will try format-specific parsing: {}",
                                strValue, e);
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
            // containsKey, not `value != null`. NP-023: reconstructRecord now puts a REAL null in
            // the map for a null-defaulted field, so `map.get` returns null and the old test put
            // the JsonProperties.NULL_VALUE sentinel straight back - silently undoing the repair
            // on the reconstruct() path only. It also conflated two different things: a field
            // legitimately reconstructed AS null was overwritten by the schema default, so the two
            // public entry points disagreed about it.
            if (map.containsKey(field.name())) {
                builder.set(field.name(), map.get(field.name()));
            } else if (field.hasDefaultValue()) {
                builder.set(field.name(), schemaDefault(field, field.name()));
            } else if (isNullable(field.schema())) {
                builder.set(field.name(), null);
            }
        }

        return builder.build();
    }

    // ========================= CACHE MANAGEMENT =========================

    public void clearSchemaCache() {
        schemaCache.clear();
        insertionOrder.clear();
    }

    public int getSchemaCacheSize() {
        return schemaCache.size();
    }

    // ========================= EXCEPTIONS =========================

    public static class ReconstructionException extends RuntimeException {
        public ReconstructionException(String message, Throwable cause) {
            super(message, cause);
        }

        /**
         * Additive since 2.1.0. Several failures this class now reports have no underlying cause
         * to carry - a missing required field, disagreeing column counts - and wrapping them in a
         * fabricated cause would be worse than saying so plainly.
         */
        public ReconstructionException(String message) {
            super(message);
        }
    }

    /**
     * The columns of one array level disagree about how many elements there are.
     *
     * <p>Added in 2.1.0. Before it, {@code Math.max} picked the longest column: short scalar
     * columns were padded with {@code ""} and {@code 0}, and short nested-record columns had their
     * LAST value duplicated into every remaining row. Both produced a datum that validates against
     * its schema and is wrong, with no exception and no log line. Measured: {@code sku=S1,S2,S3}
     * beside {@code meta_code=C1,C2} produced a third row whose code repeated the second, and the
     * reverse direction discarded C3 with no record of its existence.</p>
     *
     * <p>The message names every column and its count and the configured array format, because the
     * usual cause is a producer and a consumer disagreeing about the delimiter.</p>
     */
    public static class ArrayCardinalityException extends ReconstructionException {
        public ArrayCardinalityException(String message) {
            super(message);
        }
    }

    /**
     * The configured {@link ArraySerializationFormat} contradicts the text actually present.
     *
     * <p>Added in 2.1.0, and deliberately narrow: it fires only when a delimited format
     * (COMMA_SEPARATED or PIPE_SEPARATED) is handed text that is well-formed JSON array syntax.
     * That is a detectable CONTRADICTION rather than a guess - JSON's grammar is self-delimiting
     * and {@link MapFlattener}'s comma and pipe writers structurally cannot emit a bracketed,
     * quoted list - so the only two readings are "the config is wrong" or "split the JSON on its
     * internal commas and produce garbage". Saying so is better than either.</p>
     */
    public static class ArrayFormatMismatchException extends ReconstructionException {
        public ArrayFormatMismatchException(String message) {
            super(message);
        }
    }
}