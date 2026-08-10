package io.github.pierce.bench;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.pierce.JsonReconstructor;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Reconstruction-path benchmarks.
 *
 * <p>Reconstruction is the more expensive direction: it must group flat keys by structural prefix
 * and rebuild nested containers. It is also the path that was entirely untested until the
 * {@code JsonReconstructor} class was restored to the build, so these numbers are the first ever
 * taken for it.</p>
 *
 * <p>Inputs are produced by actually flattening a corpus rather than by hand-writing flat maps.
 * That keeps the benchmark honest: it measures reconstruction of input the library itself
 * produces, which is the only input shape that matters for the round-trip guarantee.</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 3, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
public class ReconstructBenchmark {

    private Map<String, Object> flatWide;
    private Map<String, Object> flatDeep;
    private Map<String, Object> flatArrays;
    private Map<String, Object> flatMixed;

    @Setup(Level.Trial)
    public void setUp() {
        MapFlattener flattener = new MapFlattener(false, 100, 10_000);

        ObjectNode wide = Corpus.wideFlat();
        ObjectNode deep = Corpus.deepNarrow(24);
        ObjectNode arrays = Corpus.arrayHeavy();
        ObjectNode mixed = Corpus.mixedProduction();

        flatWide = flattener.flatten(Corpus.toMap(wide));
        flatDeep = flattener.flatten(Corpus.toMap(deep));
        flatArrays = flattener.flatten(Corpus.toMap(arrays));
        flatMixed = flattener.flatten(Corpus.toMap(mixed));
    }

    @Benchmark
    public void reconstruct_wideFlat(Blackhole bh) {
        bh.consume(JsonReconstructor.quickReconstruct(flatWide));
    }

    /**
     * Depth-driven. Path parsing and prefix grouping cost scale with nesting, so this corpus
     * isolates the per-key split and prefix-walk work from raw key count.
     */
    @Benchmark
    public void reconstruct_deepNarrow(Blackhole bh) {
        bh.consume(JsonReconstructor.quickReconstruct(flatDeep));
    }

    @Benchmark
    public void reconstruct_arrayHeavy(Blackhole bh) {
        bh.consume(JsonReconstructor.quickReconstruct(flatArrays));
    }

    @Benchmark
    public void reconstruct_mixedProduction(Blackhole bh) {
        bh.consume(JsonReconstructor.quickReconstruct(flatMixed));
    }

    @Benchmark
    public void reconstructToJson_mixedProduction(Blackhole bh) {
        bh.consume(JsonReconstructor.quickReconstructToJson(flatMixed));
    }

    /**
     * Full round trip: flatten then reconstruct.
     *
     * <p>Exists because the two directions can trade against each other — an encoding change that
     * makes flattening cheaper can make grouping more expensive, and measuring only one side
     * would hide that. This is also the benchmark whose correctness matters most, since it is the
     * library's headline promise.</p>
     */
    @State(Scope.Benchmark)
    public static class RoundTripState {
        Map<String, Object> source;
        MapFlattener flattener;

        @Setup(Level.Trial)
        public void setUp() {
            source = Corpus.toMap(Corpus.mixedProduction());
            flattener = new MapFlattener(false, 100, 10_000);
        }
    }

    @Benchmark
    public void roundTrip_mixedProduction(RoundTripState state, Blackhole bh) {
        Map<String, Object> flat = state.flattener.flatten(state.source);
        bh.consume(JsonReconstructor.quickReconstruct(flat));
    }
}
