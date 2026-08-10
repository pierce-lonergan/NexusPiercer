package io.github.pierce.bench;

import io.github.pierce.AvroReconstructor;
import io.github.pierce.MapFlattener;
import org.apache.avro.Schema;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Measures the schema cache across its capacity boundary.
 *
 * <h2>Why</h2>
 *
 * <p>The reconstructor's schema cache is bounded at 100 with FIFO eviction. Two things about that
 * deserve measurement rather than assumption:</p>
 *
 * <ol>
 *   <li><b>Bounding introduces a cliff.</b> A workload cycling through N distinct schemas in a
 *       fixed rotation gets a 100% hit rate while N ≤ capacity and, under strict FIFO, a
 *       <em>0%</em> hit rate the moment N exceeds it — every lookup evicts the entry it will need
 *       next time round. That is strictly worse than the unbounded cache it replaced, which at
 *       least kept hitting. "Unbounded → bounded" is not automatically an improvement.</li>
 *   <li><b>The number 100 was inherited from the bug.</b> It was never chosen as a cache size; it
 *       was a {@code ConcurrentHashMap} initial-capacity argument that the constant's name
 *       misdescribed as a maximum. A value carried over from a defect is still a magic number.</li>
 * </ol>
 *
 * <p>The rotation here is deliberately the worst case for FIFO. Real workloads cluster, so this
 * is a lower bound on hit rate, not a typical one — but the cliff, if it exists, is real for any
 * workload whose working set exceeds the bound.</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
public class SchemaCacheCliffBenchmark {

    /** Straddles the bound of 100 deliberately: just under, just over, and well beyond. */
    @Param({"2", "50", "99", "101", "250", "1000"})
    public int distinctSchemas;

    private List<Schema> schemas;
    private Map<String, Object> flattened;
    private AvroReconstructor reconstructor;
    private int cursor;

    @Setup(Level.Trial)
    public void setUp() {
        reconstructor = AvroReconstructor.builder().build();

        schemas = new ArrayList<>(distinctSchemas);
        for (int i = 0; i < distinctSchemas; i++) {
            schemas.add(new Schema.Parser().parse(schemaJson(i)));
        }

        Map<String, Object> src = new LinkedHashMap<>();
        src.put("order_id", "ORD-1");
        src.put("created_at", "2026-08-10");
        flattened = new MapFlattener(false, 50, 1000).flatten(src);
    }

    /**
     * One reconstruction per invocation, rotating through the schema set. With N ≤ capacity every
     * call hits; with N > capacity under FIFO every call misses and rebuilds the path trie.
     */
    @Benchmark
    public void rotateThroughSchemas(Blackhole bh) {
        Schema s = schemas.get(cursor);
        cursor = (cursor + 1) % schemas.size();
        bh.consume(reconstructor.reconstructToMap(flattened, s));
    }

    private static String schemaJson(int i) {
        return "{\"type\":\"record\",\"name\":\"Order" + i + "\",\"namespace\":\"bench\","
                + "\"fields\":["
                + "{\"name\":\"order_id\",\"type\":\"string\"},"
                + "{\"name\":\"created_at\",\"type\":\"string\"}]}";
    }
}
