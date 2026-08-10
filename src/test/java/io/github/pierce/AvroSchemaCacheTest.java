package io.github.pierce;

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Behaviour of {@code AvroReconstructor}'s schema cache after it was re-keyed from a
 * canonical-form fingerprint string to the {@link Schema} object itself.
 *
 * <p>The old key called {@code SchemaNormalization.parsingFingerprint64} on every call, which
 * serialises the entire schema to canonical form and Rabin-hashes it — once per record, purely to
 * look up an entry that never changes. Measured cost of the change on a 43-field schema:
 * 11.52 µs → 7.32 µs per reconstruction with a shared schema, and 38,400 → 28,944 bytes.</p>
 *
 * <p>Re-keying only holds if {@code Schema} equality behaves the way the optimisation assumes, so
 * that assumption is asserted here rather than trusted.</p>
 */
@DisplayName("Avro schema cache")
class AvroSchemaCacheTest {

    private static final String SCHEMA_JSON = """
            {
              "type": "record",
              "name": "Order",
              "namespace": "test",
              "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "created_at", "type": "string"}
              ]
            }
            """;

    private AvroReconstructor reconstructor;

    @BeforeEach
    void setUp() {
        reconstructor = AvroReconstructor.builder().build();
        reconstructor.clearSchemaCache();
    }

    private static Map<String, Object> flatSource() {
        Map<String, Object> src = new LinkedHashMap<>();
        src.put("order_id", "ORD-1");
        src.put("created_at", "2026-08-09");
        return new MapFlattener(false, 50, 1000).flatten(src);
    }

    /**
     * The load-bearing assumption. Two schemas parsed separately from identical text are distinct
     * objects, but Avro defines equality structurally — so they must share one cache entry rather
     * than growing the cache per parse.
     *
     * <p>Were this false, a streaming job parsing its schema per micro-batch would leak an entry
     * every batch, which is strictly worse than the fingerprint key it replaced.</p>
     */
    @Test
    @DisplayName("the same schema parsed twice shares one cache entry")
    void samePathParsedTwiceSharesOneEntry() {
        Schema first = new Schema.Parser().parse(SCHEMA_JSON);
        Schema second = new Schema.Parser().parse(SCHEMA_JSON);

        assertThat(first).as("distinct objects").isNotSameAs(second);
        assertThat(first).as("but structurally equal").isEqualTo(second);

        Map<String, Object> flat = flatSource();
        reconstructor.reconstructToMap(flat, first);
        reconstructor.reconstructToMap(flat, second);

        assertThat(reconstructor.getSchemaCacheSize())
                .as("equal schemas must not each occupy their own entry")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("structurally different schemas get separate entries")
    void differentSchemasGetSeparateEntries() {
        Schema a = new Schema.Parser().parse(SCHEMA_JSON);
        Schema b = new Schema.Parser().parse(SCHEMA_JSON.replace("created_at", "updated_at"));

        Map<String, Object> flatA = flatSource();

        // Must go through MapFlattener, not be hand-built: field names containing the separator
        // are escaped on the way in, so a literal "order_id" key does not match the schema path
        // that the reconstructor derives (which is "order\_id"). Hand-writing flattened keys
        // bypasses the encoding and silently fails to match.
        Map<String, Object> rawB = new LinkedHashMap<>();
        rawB.put("order_id", "ORD-2");
        rawB.put("updated_at", "2026-08-10");
        Map<String, Object> flatB = new MapFlattener(false, 50, 1000).flatten(rawB);

        reconstructor.reconstructToMap(flatA, a);
        reconstructor.reconstructToMap(flatB, b);

        assertThat(reconstructor.getSchemaCacheSize())
                .as("a different field name is a different schema and needs its own path trie")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("repeated reconstruction with one schema does not grow the cache")
    void repeatedUseDoesNotGrowTheCache() {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        Map<String, Object> flat = flatSource();

        for (int i = 0; i < 50; i++) {
            reconstructor.reconstructToMap(flat, schema);
        }

        assertThat(reconstructor.getSchemaCacheSize()).isEqualTo(1);
    }

    /**
     * The retention test. This is the case that exposed the bug the {@code Schema}-keyed cache
     * introduced.
     *
     * <p>{@code DEFAULT_MAX_CACHE_SIZE = 100} was passed to the {@code ConcurrentHashMap}
     * constructor, which takes an INITIAL CAPACITY, not a maximum — so the cache was unbounded
     * despite the constant's name. Re-keying it from a short fingerprint string to the
     * {@link Schema} object made every retained entry a whole schema graph rather than ~40
     * characters, so the optimisation quietly turned a mild leak into a serious one for any
     * long-lived driver handling many distinct schemas.</p>
     *
     * <p>250 distinct schemas against a bound of 100.</p>
     */
    @Test
    @DisplayName("many distinct schemas do not grow the cache without bound")
    void manyDistinctSchemasStayBounded() {
        for (int i = 0; i < 250; i++) {
            Schema s = new Schema.Parser().parse(
                    SCHEMA_JSON.replace("\"Order\"", "\"Order" + i + "\""));
            Map<String, Object> src = new LinkedHashMap<>();
            src.put("order_id", "ORD-" + i);
            src.put("created_at", "2026-08-09");
            reconstructor.reconstructToMap(new MapFlattener(false, 50, 1000).flatten(src), s);
        }

        assertThat(reconstructor.getSchemaCacheSize())
                .as("cache must be bounded; an unbounded cache retains every Schema graph forever")
                .isLessThanOrEqualTo(100);
    }

    @Test
    @DisplayName("eviction does not break correctness — an evicted schema still reconstructs")
    void evictedSchemaStillWorks() {
        Schema first = new Schema.Parser().parse(SCHEMA_JSON);
        Map<String, Object> flat = flatSource();
        Map<String, Object> before = reconstructor.reconstructToMap(flat, first);

        // Push it out of the cache.
        for (int i = 0; i < 150; i++) {
            Schema s = new Schema.Parser().parse(
                    SCHEMA_JSON.replace("\"Order\"", "\"Filler" + i + "\""));
            Map<String, Object> src = new LinkedHashMap<>();
            src.put("order_id", "x");
            src.put("created_at", "y");
            reconstructor.reconstructToMap(new MapFlattener(false, 50, 1000).flatten(src), s);
        }

        assertThat(reconstructor.reconstructToMap(flat, first))
                .as("a cache miss must rebuild the trie, not change the answer")
                .isEqualTo(before);
    }
}
