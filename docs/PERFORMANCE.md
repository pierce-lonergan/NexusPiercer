# Performance baseline

Recorded **2026-08-19** at commit `bb8e988`, after the array-index scan pass. JDK 21.0.7
(Temurin), Windows 11, 2 GB heap, JMH 1.37, `-f 1 -wi 3 -i 5 -w 1s -r 1s -prof gc`, all 20
benchmarks, 43 rows.

> **Provisional for throughput, solid for allocation.** One fork is not enough to gate wall-clock
> time on; the gating baseline must be recorded on the CI runner class at `-f 3 -wi 5 -i 10`.
> Allocation per operation is a near-deterministic counter and is trustworthy as recorded — over
> three repeat runs of the same bytecode during this pass, `consolidate_arrayHeavy` varied by 480
> bytes in 8.17 million (0.006%) while its wall-clock varied by 23%. See
> [ANTI_REGRESSION.md](ANTI_REGRESSION.md).

> **The counter is machine-independent, not JVM-independent.** This is recorded on JDK 21 and
> `.github/workflows/benchmark.yml` installs JDK 17. `gc.alloc.rate.norm` comes from
> thread-allocation accounting, so it does not move with runner load — but string-concat indy
> shapes, HashMap growth and record internals do differ across a JVM major version. Scope the
> hard 2% allocation gate to a baseline recorded on the same JDK major version, or record one
> per version.

> **The previous baseline was 26 `src/main` commits stale and banked 32% of unearned slack.** It
> was recorded at `790d216` on 2026-08-09 and never re-recorded, including across `843a461`
> — a performance fix that landed **23 minutes later** and cut `consolidate_arrayHeavy` by a
> third. Every figure this document published for that benchmark between those dates described
> bytecode that no longer shipped. A change making the real code 45% worse would still have
> passed Tier 1.

## Current results

| Benchmark | µs/op | ± | MB alloc/op |
|---|---:|---:|---:|
| `consolidate_arrayHeavy` | 2,906.0 | 62.2 | 7.664 |
| `mapFlatten_arrayHeavy` | 1,119.8 | 9.6 | 2.486 |
| `reconstruct_arrayHeavy` | 220.1 | 6.6 | 0.888 |
| `roundTrip_mixedProduction` | 147.7 | 4.2 | 0.652 |
| `reconstruct_wideFlat` | 127.4 | 0.5 | 0.578 |
| `mapFlatten_wideFlat` | 140.6 | 3.5 | 0.539 |
| `consolidate_wideFlat` | 142.1 | 11.2 | 0.398 |
| `reconstructToJson_mixedProduction` | 89.2 | 2.8 | 0.356 |
| `reconstruct_mixedProduction` | 76.5 | 1.8 | 0.339 |
| `mapFlatten_mixedProduction` | 72.4 | 1.3 | 0.314 |
| `mapFlatten_mixedProduction_8threads` | 108.2 | 4.6 | 0.314 |
| `consolidate_mixedProduction` | 103.5 | 46.9 | 0.300 |
| `reconstruct_reparsedSchema` | 21.0 | 0.4 | 0.094 |
| `reconstruct_deepNarrow` | 24.6 | 7.6 | 0.078 |
| `mapFlatten_deepNarrow64` | 7.6 | 0.1 | 0.069 |
| `reconstruct_sharedSchema` | 7.4 | 0.1 | 0.029 |
| `mapFlatten_deepNarrow` | 2.6 | 0.2 | 0.021 |
| `consolidate_deepNarrow` | 1.8 | 0.1 | 0.014 |
| `rotateThroughSchemas [distinctSchemas=1000]` | 1.1 | 0.4 | 0.004 |
| `rotateThroughSchemas [distinctSchemas=50]` | 0.3 | 0.0 | 0.002 |
| `rotateThroughSchemas [distinctSchemas=101]` | 0.3 | 0.0 | 0.002 |
| `rotateThroughSchemas [distinctSchemas=250]` | 0.3 | 0.0 | 0.001 |
| `rotateThroughSchemas [distinctSchemas=99]` | 0.3 | 0.0 | 0.001 |
| `rotateThroughSchemas [distinctSchemas=2]` | 0.3 | 0.0 | 0.001 |

Plus `consolidate_batch1000`, which is throughput-only and so has no µs/op row: **10.12 ops/s,
299,448,647 B/op**. It is 999.99x `consolidate_mixedProduction` and always has been, which means
batching adds no measurable per-batch overhead and the two are **not independent data points**.
It is the externally-quoted number; quote it with that caveat attached.

The last four rows of the table are the first measurements this repository has ever gated for
`AvroReconstructBenchmark` and `SchemaCacheCliffBenchmark`. Eight entries — including every
`@Param` row of the schema-cache cliff — were absent from the previous baseline, so `compare.py`
recorded them as `NEW` and skipped every check.

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

> **These are 2026-08-09 figures and none of them is current.** They are a before/after pair from
> one dated experiment and are correct as such — do not "update" them, the pairing is the whole
> argument. But read them as history: `consolidate_arrayHeavy` is **7.664** MB/op today and
> `consolidate_wideFlat` is **0.398**. This table is how the brief for the 2026-08-19 pass came
> to believe arrayHeavy was still 23.1 MB/op, which sent it after a cost that had been paid ten
> days earlier. The **Current results** table at the top of this document is the live one.

| Benchmark | 2026-08-09 before MB/op | 2026-08-09 after MB/op |
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

## What the 2026-08-19 pass changed, predicted against measured

Four optimizations attempted, three kept, one reverted. Allocation leads because it is the
deterministic counter; wall-clock is corroboration only. Every run reported its natural control
alongside its target, and the controls held: `mapFlatten_arrayHeavy` and `mapFlatten_wideFlat`
were flat **to the byte** (+0.00%) across all nine measurement runs of the pass.

Starting point measured on this machine at HEAD *before* any change — **not** the figure the
previous baseline published, which was 26 commits stale:

| Benchmark | published baseline | actual HEAD | after this pass | total |
|---|---:|---:|---:|---:|
| `consolidate_arrayHeavy` | 23,124,060 | 15,637,015 | **7,663,876** | −51.0% vs actual |
| `consolidate_mixedProduction` | 859,875 | 804,954 | **300,297** | −62.7% |
| `consolidate_wideFlat` | 622,540 | 622,540 | **398,449** | −36.0% |
| `consolidate_deepNarrow` | 13,912 | 13,888 | **13,504** | −2.8% |
| `consolidate_batch1000` | 859,866,333 | — | **299,448,647** | −65.2% |

### Iteration 1 — character scans replace the two per-key regexes · KEPT

Predicted −25% to −36% on `wideFlat` (point −33%), −4.5 to −7.0 MB on `arrayHeavy` (point
−5.5 MB), −20% to −30% on `mixedProduction`, −1% to −2% on `deepNarrow`.

Measured: `wideFlat` **−35.98%**, `arrayHeavy` **−40.0%** (−6.26 MB), `mixedProduction`
**−18.2%**, `deepNarrow` **−1.44%**.

Two of the four landed inside the predicted band; `arrayHeavy` came in *above* it and
`mixedProduction` *below* it. The `deepNarrow` prediction was the interesting one: one analysis
predicted −35% there on the belief that the corpus has 24 keys. `deepNarrow(24)` is a single
nested chain and produces exactly **one** flattened key, so one Matcher out of 13,888 bytes was
all there was to win. That analysis was refuted before measuring, and the measurement agreed.

The wall-clock is the outlier of the whole pass, and it is not the Matcher. `deepNarrow`
allocates 1.4% less and runs **39x faster** (70.4 to 1.8 µs). The pattern `(.+?)\[(\d+)\](.*)`
against a ~200-character key containing no bracket forces the reluctant quantifier to try every
start position against every expansion — quadratic backtracking, paid once per key. `deepNarrow`
has one very long key; `wideFlat` pays it a thousand times and dropped 71%.

### Iteration 2 — hoist the `Pattern.compile` out of the per-group loop · KEPT

Predicted −0.7 to −1.6 MB on `arrayHeavy` (point −1.1 MB), and −3% to −12% on `mixedProduction`.

Measured: `arrayHeavy` **−12.9%** (−1.21 MB), `mixedProduction` **−53.0%**.

The `mixedProduction` prediction was badly wrong and the record should say so plainly. One
analysis had predicted −10% to −25% there; that was dismissed on the grounds that 432 compiles at
~800 B each would be 52% of the benchmark's entire allocation and therefore implausible. It is
53%. Two benchmarks independently price a `Pattern.compile` of this regex identically:
1,211,663 / 1,500 = 808 B and 348,936 / 432 = 808 B.

`consolidate_wideFlat` and `consolidate_deepNarrow` are the in-class controls for this one —
neither corpus has an array index, so the hoisted block is skipped outright — and both held.

This is also gate drill #1 from `benchmarks/README.md` ("reintroduce a `Pattern.compile` inside a
loop") found sitting in shipped code, executing 432 times per `mixedProduction` record, the whole
time the README described it as a drill to perform artificially.

### Iteration 3 — build the value array at its known size · KEPT

Predicted −0.15 to −0.25 MB on `arrayHeavy`, point −0.18 MB. Measured **−0.19 MB (−2.33%)**.

Corrected on the way: the proposal claimed `new HashSet<>(Arrays.asList(values))` "rehashes as it
grows". It does not — `HashSet(Collection)` already sizes its map to
`max((int)(size/.75f)+1, 16)`. There was never a growth chain to recover there.

### Iteration 4 — a conservative ACCEPT filter for `determineArrayType` · KEPT

Predicted −0.25 to −0.35 MB on `arrayHeavy`, point −0.30 MB. Measured **−0.316 MB (−3.96%)**.

`wideFlat` and `deepNarrow` moved **exactly zero**, which is the attribution: neither corpus has
an array, so `determineArrayType` is never reached. That is the precise inverse of iteration 1's
signature, where `wideFlat` moved most.

### Iteration 5 — pre-size `consolidatedOutput` from `flattened.size()` · REVERTED

An analysis proposed this as "the safe half" of a pre-sizing change. Predicted, before measuring,
that it would make `arrayHeavy` **worse**: 14,000 flattened keys collapse to roughly 420 output
entries, so seeding from the input size allocates a 16,384-slot table for 420 entries.

Measured: `arrayHeavy` **+1.62%** (+124 KB) — worse, as predicted. `wideFlat` −2.07%,
`mixedProduction` −1.52%, `deepNarrow` −0.94% — better, because for those corpora the flattened
size really is close to the output size.

It is a workload-dependent trade, not an optimization, and it regresses the largest allocator in
the suite by more than it gains everywhere else combined (−111 KB net across the four). Reverted.
Recorded here so nobody spends the afternoon rediscovering it.

## Remaining headroom

`consolidate_arrayHeavy` is still the largest single-document allocator at **7.66 MB/op**, 3.1x
the next entry (`mapFlatten_arrayHeavy`). It is no longer 9x, and the exception-driven type
detection that used to be blamed for it is long gone — that shipped in `843a461` on 2026-08-09,
and after iteration 4 the residual `parseDouble` cost is gone too.

What is left there has **not** been attributed by measurement, and this document should not
pretend otherwise. At 14,000 leaves and 7.66 MB the figure is ~547 B/leaf, spread across
Jackson's `readTree`, 14,000 `FlattenTask` objects and prefix strings, 14,000 `LinkedHashMap`
entries, 14,000 `KeyedValue` objects and the stripped base keys. **Before optimizing further, run
an allocation class histogram.** Every estimate in this section is arithmetic rather than
observation, and this pass has already shown such estimates to be wrong by a factor of four in
both directions.

Two structural items are known, and both are out of scope for a performance pass because both
change output:

* `processGroupedValues` compares `consolidatedKey.replace("_", ".")` against an array-field
  prefix that still contains underscores, so for any snake_case document the comparison can never
  match and `wasTrackedAsArray` is permanently false. Iteration 2 made that dead comparison cheap;
  it did not make it correct.
* `cannotBeNumeric` admits only `Character.isWhitespace`, while `Double.parseDouble` begins with
  `String.trim`, which strips everything at or below `U+0020`. The 23 characters in the gap —
  `U+0000`–`U+0008` and `U+000E`–`U+001B` — cause a numeric column to be published as a string
  column. Found during this pass, pinned by `ArrayTypeClassificationDifferentialTest`, not fixed.

## Static metrics

| Metric | Before | Now | Target |
|---|---:|---:|---:|
| `invokedynamic` in `io.github.pierce.**` | 7,168 | **413** (2026-08-11) | not gated |

All 413 are ordinary Java lambda, method-reference and string-concat sites, not dynamic dispatch.
Recorded in [`benchmarks/results/indy-ceiling.txt`](../benchmarks/results/indy-ceiling.txt),
which is **updated by hand and enforces nothing** — `benchmark.yml` reports the count into the
job summary and fails on no value. It is an observation, not a ratchet; the ratchet was retired
on 2026-08-11 because it counted lambdas and record accessors and failed a build for adding a
`record`.

Two corrections to what this section used to say. It published **378** immediately below a table
reading 413, and 378 corresponds to nothing in the repository — `indy-ceiling.txt` says 413 and so
does `docs/ANTI_REGRESSION.md`. And it called the file a ratchet whose "count may only decrease",
which the same table's own Target column already contradicted with "not gated". The 413 itself was
measured on 2026-08-11 against a `src/main` of ~19,860 lines; the tree is larger now and the count
has not been re-run.

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
