package io.github.pierce.bench;

import io.github.pierce.AvroReconstructor;
import io.github.pierce.MapFlattener;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Schema-driven Avro reconstruction.
 *
 * <p><b>Why this exists.</b> {@link ReconstructBenchmark} covers {@code JsonReconstructor}, the
 * schema-less path. {@code AvroReconstructor} — 2,983 lines, the largest class in the library and
 * the only path with a working round-trip before 2.0 — had no benchmark whatsoever. That gap was
 * discovered the hard way: an optimisation to its schema-cache keying showed no movement in any
 * existing benchmark, because no existing benchmark executed the code.</p>
 *
 * <h2>Schema reuse is the variable that matters</h2>
 *
 * <p>{@code AvroReconstructor} caches a path trie per schema. How that cache is keyed only shows
 * up under the right usage pattern, so both are measured:</p>
 *
 * <ul>
 *   <li>{@link #reconstruct_sharedSchema} — one parsed {@link Schema} reused for every record.
 *       This is the Spark batch pattern, and the common case.</li>
 *   <li>{@link #reconstruct_reparsedSchema} — the schema re-parsed per operation, as a streaming
 *       job handling a schema-registry payload per micro-batch would. Distinct-but-equal Schema
 *       objects, which is exactly where identity-keyed caching degrades to a 0% hit rate.</li>
 * </ul>
 *
 * <p>A benchmark that only ever reused one Schema instance would report a 100% cache hit rate
 * that production never sees, and would make any cache-keying change look free.</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 3, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
public class AvroReconstructBenchmark {

    /** Wide-ish record with nesting, deliberately using snake_case names. */
    private static final String SCHEMA_JSON = buildSchemaJson();

    /**
     * A 203-field record whose every data field is a three-branch union with the string branch
     * LAST, paired with {@link Corpus#unionNullable()}.
     *
     * <p>Added 2026-09-20. Until then the union path had no benchmark on EITHER side: the only
     * Avro-schema benchmark in the suite was {@link #SCHEMA_JSON}, which is 43 required fields
     * with zero unions, zero nullables and zero logical types, while
     * {@code Corpus.unionNullable()} was generated and consumed by nothing. That is the gap this
     * closes, and it matters because the published fidelity corpus pins four DEFECT rows to
     * top-level union branch selection and 2.1.0 added a type-directed array-element branch
     * selector on the same reconstruct path.</p>
     */
    private static final String UNION_SCHEMA_JSON = buildUnionSchemaJson();

    private Schema sharedSchema;
    private Map<String, Object> flattened;
    private AvroReconstructor reconstructor;

    private Schema sharedUnionSchema;
    private Map<String, Object> flattenedUnion;

    @Setup(Level.Trial)
    public void setUp() {
        sharedSchema = new Schema.Parser().parse(SCHEMA_JSON);
        reconstructor = AvroReconstructor.builder().build();

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("order_id", "ORD-1");
        source.put("created_at", "2026-08-09");
        Map<String, Object> customer = new LinkedHashMap<>();
        customer.put("full_name", "Ada Lovelace");
        customer.put("email_address", "ada@example.com");
        Map<String, Object> address = new LinkedHashMap<>();
        address.put("street_line_1", "1 Analytical Way");
        address.put("postal_code", "00001");
        customer.put("billing_address", address);
        source.put("customer", customer);
        for (int i = 0; i < 40; i++) {
            source.put("attribute_" + i, "value_" + i);
        }

        flattened = new MapFlattener(false, 50, 1000).flatten(source);

        sharedUnionSchema = new Schema.Parser().parse(UNION_SCHEMA_JSON);
        flattenedUnion = new MapFlattener(false, 50, 1000).flatten(Corpus.toMap(Corpus.unionNullable()));
    }

    /**
     * The common case: one schema instance, many records. The schema-cache lookup happens once
     * per record regardless, so this is where a costly cache KEY shows up as pure overhead.
     */
    @Benchmark
    public void reconstruct_sharedSchema(Blackhole bh) {
        bh.consume(reconstructor.reconstructToMap(flattened, sharedSchema));
    }

    /**
     * The streaming case: a freshly parsed schema each time. Distinct objects that compare equal,
     * which is what content-based cache keys must handle correctly.
     *
     * <p>Includes the parse cost, so this is not directly comparable to
     * {@link #reconstruct_sharedSchema} — it is a control for cache-keying changes, tracked
     * against its own history rather than against the shared-schema number.</p>
     */
    @Benchmark
    public void reconstruct_reparsedSchema(Blackhole bh) {
        Schema fresh = new Schema.Parser().parse(SCHEMA_JSON);
        bh.consume(reconstructor.reconstructToMap(flattened, fresh));
    }

    /**
     * Union branch selection, shared schema. The corpus is skewed 15% null / 15% long / 70%
     * non-numeric string against a schema that declares string LAST, so roughly seven values in
     * ten reach the final branch only after the long branch has been tried and rejected.
     *
     * <p>This is deliberately the WORST realistic case, as {@code Corpus.unionNullable()}'s own
     * javadoc says. A first-branch-dominant workload would show near-zero cost on identical
     * code, so this number must never be quoted as "the cost of unions" on its own.</p>
     */
    @Benchmark
    public void reconstruct_unionNullable_sharedSchema(Blackhole bh) {
        bh.consume(reconstructor.reconstructToMap(flattenedUnion, sharedUnionSchema));
    }

    /**
     * The same, with the union schema re-parsed per operation. Paired with the shared-schema
     * form for the same reason {@link #reconstruct_reparsedSchema} is paired with
     * {@link #reconstruct_sharedSchema}: it holds the cache-keying variable controlled, so a
     * change to how schemas are keyed cannot be mistaken for a change in branch selection.
     */
    @Benchmark
    public void reconstruct_unionNullable_reparsedSchema(Blackhole bh) {
        Schema fresh = new Schema.Parser().parse(UNION_SCHEMA_JSON);
        bh.consume(reconstructor.reconstructToMap(flattenedUnion, fresh));
    }

    /**
     * 200 three-branch unions plus the three extras {@code Corpus.unionNullable()} emits, with
     * the branch ORDER chosen to match what the corpus is built to punish: null, then long,
     * then string.
     */
    private static String buildUnionSchemaJson() {
        StringBuilder sb = new StringBuilder(8192);
        sb.append("{\"type\":\"record\",\"name\":\"UnionRecord\",\"namespace\":\"bench\",\"fields\":[");
        for (int i = 0; i < 200; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"name\":\"union_field_").append(i)
              .append("\",\"type\":[\"null\",\"long\",\"string\"],\"default\":null}");
        }
        sb.append(",{\"name\":\"created_at\",\"type\":[\"null\",\"string\"],\"default\":null}");
        sb.append(",{\"name\":\"updated_at\",\"type\":[\"null\",\"string\"],\"default\":null}");
        sb.append(",{\"name\":\"mixed_union_value\",\"type\":[\"null\",\"long\",\"string\"],\"default\":null}");
        sb.append("]}");
        return sb.toString();
    }

    private static String buildSchemaJson() {
        SchemaBuilder.FieldAssembler<Schema> f = SchemaBuilder
                .record("Order").namespace("bench").fields()
                .requiredString("order_id")
                .requiredString("created_at")
                .name("customer").type(
                        SchemaBuilder.record("Customer").namespace("bench").fields()
                                .requiredString("full_name")
                                .requiredString("email_address")
                                .name("billing_address").type(
                                        SchemaBuilder.record("Address").namespace("bench").fields()
                                                .requiredString("street_line_1")
                                                .requiredString("postal_code")
                                                .endRecord())
                                .noDefault()
                                .endRecord())
                .noDefault();
        for (int i = 0; i < 40; i++) {
            f = f.requiredString("attribute_" + i);
        }
        return f.endRecord().toString();
    }
}
