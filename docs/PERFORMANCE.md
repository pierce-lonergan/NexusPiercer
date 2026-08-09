# Performance baseline

Recorded **2026-08-09**. JDK 21 (Temurin), Windows 11, 2 GB heap, JMH 1.37.

Configuration: `-f 1 -wi 3 -i 5 -w 1s -r 1s -prof gc`.

> **This is a provisional baseline, not the gating one.** One fork and five iterations produce
> wide confidence intervals — several benchmarks show ±25% or worse. It is enough to establish
> allocation figures (which are near-deterministic counters) and to rank hot paths. It is **not**
> enough to gate throughput on. The gating baseline must be recorded on the CI runner class at
> `-f 3 -wi 5 -i 10`, and the runner must demonstrate <8% throughput variance across three
> consecutive runs before Tier 2 is armed. See [ANTI_REGRESSION.md](ANTI_REGRESSION.md).

## Results

Sorted by allocation per operation, which is the primary gate metric.

| Benchmark | µs/op | ± | **MB allocated/op** |
|---|---:|---:|---:|
| `mapFlatten_wideFlat` | 9,432 | 1,209 | **25.39** |
| `consolidate_arrayHeavy` | 11,354 | 1,516 | 23.12 |
| `mapFlatten_arrayHeavy` | 22,228 | 7,305 | 21.38 |
| `roundTrip_mixedProduction` | 9,586 | 4,807 | 19.27 |
| `reconstruct_wideFlat` | 7,455 | 2,192 | 17.84 |
| `reconstructToJson_mixedProduction` | 5,297 | 1,879 | 10.61 |
| `reconstruct_mixedProduction` | 4,904 | 1,531 | 9.96 |
| `mapFlatten_mixedProduction` | 3,377 | 902 | 9.26 |
| `mapFlatten_mixedProduction_8threads` | 5,533 | 3,114 | 8.76 |
| `consolidate_mixedProduction` | 377 | 34 | 0.86 |
| `consolidate_wideFlat` | 484 | 51 | 0.62 |
| `mapFlatten_deepNarrow64` | 33.6 | 8.1 | 0.24 |
| `reconstruct_deepNarrow` | 120.3 | 264.8 | 0.22 |
| `mapFlatten_deepNarrow` | 14.5 | 1.9 | 0.06 |
| `consolidate_deepNarrow` | 73.7 | 6.0 | 0.01 |

`reconstruct_arrayHeavy` is **absent because it does not complete** — it exhausts a 2 GB heap.
See [SECURITY.md](../SECURITY.md#perfnp-021--separator-driven-reconstruction-blow-up).

## What the numbers say

### 1. Allocation amplification is the headline

Flattening the wide-flat corpus — a **45 KB** document — allocates **25.39 MB**. That is roughly
**560x the size of the input**, to produce an output of comparable size to the input.

This is the per-node throwaway-map finding made visible. `MapFlattener.flattenValue` allocates a
`LinkedHashMap` per node and `putAll`s it into the parent's map, which does the same one level up,
so a leaf at depth *d* is re-inserted about *2d* times and every interior node allocates two maps
that are immediately discarded.

The depth corpora corroborate the mechanism cleanly: `deepNarrow` (depth 24) allocates 0.06 MB and
`deepNarrow64` (depth 64) allocates 0.24 MB — **4x the allocation for 2.7x the depth**, on a
document with exactly one leaf value. Allocation is growing faster than depth, which is what a
copy-per-level recursion produces and what a single-accumulator recursion would not.

### 2. The Groovy and Java implementations differ by more than an order of magnitude

| | µs/op | MB/op |
|---|---:|---:|
| `consolidate_wideFlat` (Java `JsonFlattenerConsolidator`) | 484 | 0.62 |
| `mapFlatten_wideFlat` (Groovy `MapFlattener`) | 9,432 | 25.39 |

**19.5x slower, 41x more allocation** — and the Java path is doing *more* work, not less: it parses
a JSON string, flattens, and serialises back to a string, while the Groovy path walks an
already-parsed `Map` and returns a `Map`.

Stated carefully: **the inputs are not identical, so this is not a controlled comparison** and the
ratio should not be quoted as "Groovy is 19x slower than Java". Some of the gap is the algorithm
(the accumulator problem above), some is dynamic dispatch, and the two are not separated here.
Separating them is exactly what optimization iterations 4 and 5 are designed to do.

What the comparison *does* establish is that the two parallel flatten implementations in this
library have wildly different performance characteristics for overlapping purposes — which is a
concrete cost of the architectural duplication, measured rather than asserted.

### 3. Concurrency is not a bottleneck on this path

`mapFlatten_mixedProduction` is 3,377 µs/op single-threaded and 5,533 µs/op at `@Threads(8)` — a
1.64x per-operation degradation for 8x the concurrency. That is reasonable scaling, and it
**rules out** lock contention as a significant factor on the flatten path.

Worth stating explicitly because a plausible-sounding hypothesis (shared caches, metaclass
lookups, and `System.err` writes serialising on the `PrintStream` monitor) predicted otherwise.
The error bar here is wide (±3,114) so this should be re-measured with more forks, but there is no
evidence of a contention cliff.

### 4. Where the remaining headroom is

`consolidate_arrayHeavy` at 23.12 MB/op is the second-largest allocator and the one with the
clearest identified cause: exception-driven numeric type detection constructs one
`NumberFormatException` per array element with no early exit. The array-heavy corpus contains
10,000 elements in string-typed arrays. Isolated measurement put the per-element cost at 418.6 ns
versus 1.3 ns for an equivalent character scan.

That is optimization iteration 1, and it is the highest-confidence, lowest-risk item in the
campaign: single language, single file, no API change.

## Reproducing

```bash
./mvnw install -DskipTests
```
```bash
mvn -f benchmarks/pom.xml clean package
```
```bash
java -jar benchmarks/target/benchmarks.jar -f 1 -wi 3 -i 5 -prof gc -e '.*reconstruct_arrayHeavy.*' -rf json -rff results.json
```

Drop the `-e` exclusion once NP-021 is fixed.

## Static metrics

| Metric | Value | Target |
|---|---:|---:|
| `invokedynamic` call sites in `io.github.pierce.**` | **7,168** | 0 |

Top contributors: `AvroReconstructor` 1,773 · `MapFlattener` 677 · `JsonReconstructor` 537 ·
`GAvroSchemaFlattener` 501 · `JsonFlattener$FluentOperation` 265.

Every one of these is a dynamic call site in code that uses **no Groovy language feature at all** —
no `def`, no closures, no GStrings, no `@CompileStatic`. Ratcheted by
[`benchmarks/results/indy-ceiling.txt`](../benchmarks/results/indy-ceiling.txt); the count may only
decrease.
