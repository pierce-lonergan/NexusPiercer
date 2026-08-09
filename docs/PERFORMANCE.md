# Performance baseline

Recorded **2026-08-09** after the Groovy→Java port. JDK 21 (Temurin), Windows 11, 2 GB heap,
JMH 1.37, `-f 1 -wi 3 -i 5 -w 1s -r 1s -prof gc`.

> **Provisional for throughput, solid for allocation.** One fork is not enough to gate wall-clock
> time on; the gating baseline must be recorded on the CI runner class at `-f 3 -wi 5 -i 10`.
> Allocation per operation is a near-deterministic counter (errors below are ±0.1 B/op in most
> rows) and is trustworthy as recorded. See [ANTI_REGRESSION.md](ANTI_REGRESSION.md).

## Current results

| Benchmark | µs/op | ± | MB alloc/op |
|---|---:|---:|---:|
| `consolidate_arrayHeavy` | 10,791.5 | 174.5 | 23.124 |
| `mapFlatten_arrayHeavy` | 1,166.3 | 8.5 | 2.509 |
| `reconstruct_arrayHeavy` | 220.1 | 4.5 | 0.886 |
| `consolidate_mixedProduction` | 374.4 | 4.4 | 0.860 |
| `roundTrip_mixedProduction` | 147.5 | 2.2 | 0.645 |
| `consolidate_wideFlat` | 471.9 | 3.9 | 0.623 |
| `reconstruct_wideFlat` | 123.7 | 1.8 | 0.578 |
| `mapFlatten_wideFlat` | 143.8 | 3.2 | 0.539 |
| `reconstructToJson_mixedProduction` | 87.4 | 1.3 | 0.355 |
| `reconstruct_mixedProduction` | 73.2 | 1.0 | 0.331 |
| `mapFlatten_mixedProduction_8threads` | 84.4 | 1.0 | 0.316 |
| `mapFlatten_mixedProduction` | 76.8 | 1.0 | 0.315 |
| `reconstruct_deepNarrow` | 22.9 | 0.3 | 0.077 |
| `mapFlatten_deepNarrow64` | 7.6 | 0.0 | 0.069 |
| `mapFlatten_deepNarrow` | 2.6 | 0.0 | 0.021 |
| `consolidate_deepNarrow` | 71.3 | 0.8 | 0.014 |

## The Groovy→Java port, with its own control group

The port produced improvements far larger than predicted. The adversarial verification pass had
put the realistic gain from removing Groovy dynamic dispatch at **1.5–3x**, on the reasoning that
this build already enables `invokeDynamic`, so monomorphic call sites stabilise into MethodHandle
chains the JIT inlines tolerably well.

**That prediction was wrong by more than an order of magnitude.** Rather than assert the large
numbers, here is the evidence that makes them credible: this dataset contains a natural control.

`JsonFlattenerConsolidator` — the `consolidate_*` benchmarks — was always Java and was never
touched by the port. It ran on the same machine, same harness, same corpora, before and after:

### Control (always Java, unmodified)

| Benchmark | before MB/op | after MB/op |
|---|---:|---:|
| `consolidate_arrayHeavy` | 23.124 | **23.124** |
| `consolidate_wideFlat` | 0.623 | **0.623** |
| `consolidate_mixedProduction` | 0.860 | **0.860** |

Identical to three decimal places.

### Treatment (ported from Groovy)

| Benchmark | before | after | factor |
|---|---:|---:|---:|
| `mapFlatten_wideFlat` | 9,555 µs / 25.54 MB | 143.8 µs / 0.539 MB | 66x / **47x** |
| `mapFlatten_mixedProduction` | 3,437 µs / 9.09 MB | 76.8 µs / 0.315 MB | 45x / **29x** |
| `mapFlatten_arrayHeavy` | 25,926 µs / 20.76 MB | 1,166 µs / 2.509 MB | 22x / **8.3x** |
| `reconstruct_wideFlat` | 5,208 µs / 14.32 MB | 123.7 µs / 0.578 MB | 42x / **25x** |
| `reconstruct_mixedProduction` | 1,859 µs / 3.85 MB | 73.2 µs / 0.331 MB | 25x / **12x** |
| `roundTrip_mixedProduction` | 6,108 µs / 13.09 MB | 147.5 µs / 0.645 MB | 41x / **20x** |

Because the control is unchanged to three decimals while the treatment moves 8–47x, the result
cannot be explained by measurement noise, JVM version, machine state, or a harness change. The
only variable that differs between the two groups is the language the code was compiled from.

### Why it is so much larger than predicted

The 1.5–3x estimate assumed the cost was JIT-level dispatch overhead, which `invokeDynamic`
largely mitigates. The measurements say the dominant cost was **allocation**, not dispatch:
every Groovy call site boxes its arguments and materialises invocation state, so a recursive
flattener performing millions of small operations allocates on every one of them. Dispatch
overhead is a constant factor the JIT can attack; allocation is garbage the JIT cannot remove
once it escapes.

The earlier figure of 25.54 MB allocated to flatten a **45 KB** document — 560x amplification —
was flagged as implausible when it was recorded. It was real. At 0.539 MB the amplification is
~12x, which is merely unremarkable for a flattener that materialises 1,000 map entries.

**The honest summary:** this was predicted at 1.5–3x and measured at 8–47x on allocation. The
prediction was not conservative, it was wrong, and it would have stayed wrong without the harness.

## Remaining headroom

`consolidate_arrayHeavy` is now by far the largest allocator at **23.1 MB/op** — 9x the next
entry, and untouched by everything so far. Its cause is already identified: exception-driven
numeric type detection constructs one `NumberFormatException` per array element with no early
exit, and the array-heavy corpus contains 10,000 elements in string-typed arrays. Isolated
measurement put this at 418.6 ns/element versus 1.3 ns for an equivalent character scan.

That is optimization iteration 1 in [the roadmap](audit/ROADMAP.md), and it is now the single
highest-value remaining item: one file, one language, no API change.

## Static metrics

| Metric | Before | Now | Target |
|---|---:|---:|---:|
| `invokedynamic` in `io.github.pierce.**` | 7,168 | **378** | 378 |

The remaining 378 are ordinary Java lambda, method-reference and string-concat sites, not dynamic
dispatch. Ratcheted by
[`benchmarks/results/indy-ceiling.txt`](../benchmarks/results/indy-ceiling.txt); the count may
only decrease.

## Reproducing

```bash
./mvnw install -DskipTests
```
```bash
mvn -f benchmarks/pom.xml clean package
```
```bash
java -jar benchmarks/target/benchmarks.jar -f 1 -wi 3 -i 5 -prof gc -rf json -rff results.json
```
```bash
python3 benchmarks/compare.py --baseline benchmarks/results/baseline.json --current results.json
```
