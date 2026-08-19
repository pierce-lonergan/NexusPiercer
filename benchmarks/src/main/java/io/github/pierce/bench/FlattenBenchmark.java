package io.github.pierce.bench;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.pierce.JsonFlattenerConsolidator;
import io.github.pierce.MapFlattener;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Flatten-path benchmarks.
 *
 * <p>Every method consumes its result through a {@link Blackhole}. Returning the value would also
 * work, but an explicit Blackhole makes it obvious at review time that dead-code elimination has
 * been considered — a flatten whose result is discarded is exactly the shape the JIT is best at
 * deleting entirely, which produces a benchmark that measures nothing and looks fast.</p>
 *
 * <p>Corpora are built once in {@link Level#Trial} setup. Building them per-invocation would
 * measure the generator rather than the library.</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 3, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
public class FlattenBenchmark {

    private String wideFlatJson;
    private String deepNarrowJson;
    private String arrayHeavyJson;
    private String mixedJson;

    private Map<String, Object> wideFlatMap;
    private Map<String, Object> deepNarrowMap;
    private Map<String, Object> deepNarrow64Map;
    private Map<String, Object> arrayHeavyMap;
    private Map<String, Object> mixedMap;

    private JsonFlattenerConsolidator consolidator;
    private MapFlattener mapFlattener;

    @Setup(Level.Trial)
    public void setUp() {
        ObjectNode wide = Corpus.wideFlat();
        ObjectNode deep = Corpus.deepNarrow(24);
        ObjectNode deep64 = Corpus.deepNarrow(64);
        ObjectNode arrays = Corpus.arrayHeavy();
        ObjectNode mixed = Corpus.mixedProduction();

        wideFlatJson = Corpus.toJson(wide);
        deepNarrowJson = Corpus.toJson(deep);
        arrayHeavyJson = Corpus.toJson(arrays);
        mixedJson = Corpus.toJson(mixed);

        wideFlatMap = Corpus.toMap(wide);
        deepNarrowMap = Corpus.toMap(deep);
        deepNarrow64Map = Corpus.toMap(deep64);
        arrayHeavyMap = Corpus.toMap(arrays);
        mixedMap = Corpus.toMap(mixed);

        // maxNesting 100 so the deep-narrow corpora are not truncated by a depth limit; a
        // benchmark that silently stops early is worse than no benchmark.
        consolidator = new JsonFlattenerConsolidator(",", null, 100, 10_000, false);
        mapFlattener = new MapFlattener(false, 100, 10_000);
    }

    // ---------------------------------------------------------- JsonFlattenerConsolidator

    @Benchmark
    public void consolidate_wideFlat(Blackhole bh) {
        bh.consume(consolidator.flattenAndConsolidateJson(wideFlatJson));
    }

    @Benchmark
    public void consolidate_deepNarrow(Blackhole bh) {
        bh.consume(consolidator.flattenAndConsolidateJson(deepNarrowJson));
    }

    /**
     * The largest single-document allocator in the suite, and the pass-after-pass target.
     *
     * <p>This javadoc used to say the corpus "is where exception-driven type detection
     * dominates: numeric-looking strings drive one NumberFormatException construction per
     * element". Both halves were wrong by 2026-08-19. The exception path was removed in 843a461
     * on 2026-08-09, and the strings were never numeric-looking in the first place - the
     * generator emits randomToken over [a-z0-9], so roughly 98% contain a letter outside the
     * double grammar and are rejected by a character scan on their first or second character.
     * The corpus property that actually matters is "many array elements", not "expensive to
     * type-check".</p>
     */
    @Benchmark
    public void consolidate_arrayHeavy(Blackhole bh) {
        bh.consume(consolidator.flattenAndConsolidateJson(arrayHeavyJson));
    }

    @Benchmark
    public void consolidate_mixedProduction(Blackhole bh) {
        bh.consume(consolidator.flattenAndConsolidateJson(mixedJson));
    }

    // ---------------------------------------------------------- MapFlattener

    @Benchmark
    public void mapFlatten_wideFlat(Blackhole bh) {
        bh.consume(mapFlattener.flatten(wideFlatMap));
    }

    /**
     * The discriminating case for per-node map allocation: cost here is depth-driven, and the
     * corpus is deliberately tiny so size cannot confound the reading.
     */
    @Benchmark
    public void mapFlatten_deepNarrow(Blackhole bh) {
        bh.consume(mapFlattener.flatten(deepNarrowMap));
    }

    @Benchmark
    public void mapFlatten_deepNarrow64(Blackhole bh) {
        bh.consume(mapFlattener.flatten(deepNarrow64Map));
    }

    @Benchmark
    public void mapFlatten_arrayHeavy(Blackhole bh) {
        bh.consume(mapFlattener.flatten(arrayHeavyMap));
    }

    @Benchmark
    public void mapFlatten_mixedProduction(Blackhole bh) {
        bh.consume(mapFlattener.flatten(mixedMap));
    }

    /**
     * Contention probe.
     *
     * <p>A single-threaded run cannot surface a lock, and this library runs inside Spark
     * executors where the same flattener is hit by every task thread. Unbuffered
     * {@code System.err} writes on a hot path serialize on the {@code PrintStream} monitor;
     * shared caches and metaclass lookups have their own contention profile. Near-linear scaling
     * against {@link #mapFlatten_mixedProduction} is the expectation — a large gap is the
     * finding.</p>
     */
    @Benchmark
    @Threads(8)
    public void mapFlatten_mixedProduction_8threads(Blackhole bh) {
        bh.consume(mapFlattener.flatten(mixedMap));
    }

    /**
     * Batch throughput, reported per 1,000 records. This is the externally-quoted number, and it
     * is the one that behaves most like production: distinct records, so per-record caches see a
     * realistic hit rate rather than a synthetic 100%.
     */
    @State(Scope.Benchmark)
    public static class BatchState {
        List<String> records;
        JsonFlattenerConsolidator flattener;

        @Setup(Level.Trial)
        public void setUp() {
            records = Corpus.batch(1_000);
            flattener = new JsonFlattenerConsolidator(",", null, 100, 10_000, false);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void consolidate_batch1000(BatchState state, Blackhole bh) {
        for (String record : state.records) {
            bh.consume(state.flattener.flattenAndConsolidateJson(record));
        }
    }
}
