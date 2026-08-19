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
>
> **How near is "near-deterministic": measured.** Four further runs of one unchanged bytecode on
> 2026-08-19 reproduced `consolidate_wideFlat` to 0.07 B, `consolidate_deepNarrow` to 0.001 B and
> `consolidate_arrayHeavy` to 1,442 B in 7.66 M (0.019%). The largest fork-to-fork difference
> anywhere in the suite was `consolidate_mixedProduction` `avgt`: 840 B in 300 K, **0.28%**, and
> it reproduced in both modes on the second run, so the baseline's `avgt` fork is the outlier
> rather than today's. Read 0.3% as the observed floor of this counter and the 2% Tier-1 band as
> having real room in it — but not as having 2% of room.

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

Every figure in that table and in the sentence above it is checked against
`benchmarks/results/baseline.json` by `PublishedBenchmarkNumbersMatchTheBaselineTest`, because a
number in this document that disagrees with the file the gate actually reads is how a REVERTED
iteration's measurement came to be published as a shipped result.

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

The earlier figure of 25.54 MB allocated to flatten `wideFlat` was flagged as implausible when it
was recorded, and was published here as **560x** amplification against a document this repository
described as "~45 KB". It was real, and it was worse than published: `wideFlat` was measured on
2026-08-19 at **24,665 bytes**, so the amplification was **1,035x**. At 0.539 MB it is **~22x**,
not the ~12x this paragraph used to claim. Both old figures were arithmetic on a document size
nobody had ever measured — every corpus size in `benchmarks/README.md` was 1.8x to 6.5x too large,
and anything derived per-byte inherited the error.

**The honest summary:** this was predicted at 1.5–3x and measured at 8–47x on allocation. The
prediction was not conservative, it was wrong, and it would have stayed wrong without the harness.

## What the 2026-08-19 pass changed, predicted against measured

Four optimizations attempted, three kept, one reverted. Allocation leads because it is the
deterministic counter; wall-clock is corroboration only.

**The before/after below is one same-session A/B, re-measured end to end by the adversarial-review
follow-up rather than assembled from four separate runs' notes.** The library was rebuilt at
`7446651` — the commit before the first optimization — in a scratch worktree, installed, and the
harness rebuilt against it; the six benchmarks were run; the tree was then restored to HEAD, the
harness rebuilt, its *contents* checked class-by-class against `target/classes`, and the identical
filter re-run. The two runs demonstrably measured different bytecode:
`JsonFlattenerConsolidator.class` is md5 `09782540…` at `7446651` and `048b4873…` at HEAD.
Protocol both sides: `-f 1 -wi 3 -i 5 -w 1s -r 1s -prof gc`, JDK 21.0.7 Temurin, 2 GB heap, same
machine, 100 minutes apart, `avgt` mode except `batch1000`.

| Benchmark | before B/op | after B/op | Δ alloc | before µs/op | after µs/op | Δ time |
|---|---:|---:|---:|---:|---:|---:|
| `consolidate_arrayHeavy` | 15,637,016 | 7,662,434 | **−51.0%** | 4,520.9 | 2,860.1 | −36.7% |
| `consolidate_mixedProduction` | 804,954 | 299,457 | **−62.8%** | 326.0 | 98.7 | −69.7% |
| `consolidate_wideFlat` | 622,540 | 398,449 | **−36.0%** | 469.3 | 147.0 | −68.7% |
| `consolidate_deepNarrow` | 13,912 | 13,632 | **−2.0%** | 71.68 | 1.83 | **39.2x** |
| `consolidate_batch1000` † | 804,946,331 | 299,448,660 | **−62.8%** | 2.527 | 10.120 | 4.01x |
| **control** `mapFlatten_arrayHeavy` | 2,486,303.73 | 2,486,303.93 | +0.000008% | 1,114.3 | 1,119.2 | **+0.44%** |
| **control** `mapFlatten_wideFlat` | 538,576.997 | 538,576.974 | −0.000004% | 143.7 | 140.5 | **−2.22%** |

† throughput-only, so its last three columns are ops/s, not µs/op.

**One published number in the previous version of this table was wrong: `consolidate_deepNarrow`
read 13,504, which is the measurement from iteration 5 — the iteration that was REVERTED.** The
shipped value is 13,632, recorded in `benchmarks/results/baseline.json` in the same commit that
published the 13,504, and reproduced in three independent JMH runs on this tree today, in both
modes, spanning 13,632.012 to 13,632.014 B/op. The table therefore credited the pass with 128
bytes that no code in the tree saves. Nothing bound the document to the baseline file;
`PublishedBenchmarkNumbersMatchTheBaselineTest` now does.

### The controls, and the correction to how they were reported

`MapFlattener` was not touched by any iteration in this pass, so `mapFlatten_arrayHeavy` and
`mapFlatten_wideFlat` are the natural control group — same machine, same harness, same corpora,
same JVM invocations as the treatments.

The previous version of this section reported those controls **as allocation only**: "flat to the
byte (+0.00%) across all nine measurement runs". The adversarial review was right that this is not
enough. `gc.alloc.rate.norm` is machine-independent by construction — this document says so at the
top, and RULE 4 of the pass brief is premised on it — so a control measured only in bytes *cannot*
move when the machine moves. It establishes attribution (the code path was untouched) and says
nothing whatever about environmental stability, which is the other half of what a control is for.
Two of this pass's headline claims are wall-clock, and neither had a wall-clock control beside it.

Both metrics are reported above now. Across the 100 minutes separating the two runs the controls
moved **+0.44%** and **−2.22%** on wall-clock while the treatments moved −36.7% to −97.4%; on
allocation the same controls moved +0.000008% and −0.000004%. Measured twice at HEAD nine minutes
apart, those two controls differed by 0.4% and 5.0% on wall-clock while their allocation figures
agreed to 0.06 B. A workstation timing figure carries
several percent of noise in either direction; the allocation counter carries about a millionth of
one. That contrast is the reason this document leads with bytes — and the reason a timing headline
needs a timing control, not a byte control.

### Against what the previous baseline published

The baseline this pass replaced was 26 `src/main` commits stale, so three of the five "before"
figures above are **not** the ones previously published, and that gap is not the pass's doing:

| Benchmark | stale published baseline | measured at `7446651` | overstatement |
|---|---:|---:|---:|
| `consolidate_arrayHeavy` | 23,124,060 | 15,637,016 | 1.48x |
| `consolidate_mixedProduction` | 859,875 | 804,954 | 1.07x |
| `consolidate_batch1000` | 859,866,333 | 804,946,331 | 1.07x |
| `consolidate_wideFlat` | 622,540 | 622,540 | none |
| `consolidate_deepNarrow` | 13,912 | 13,912 | none |

Quoting the stale column would credit this pass with −66.9% on `arrayHeavy` and −65.2% on
`batch1000`. The honest figures are −51.0% and −62.8%. The earlier version of this table published
the −65.2% precisely because it had no measurement of `batch1000` at `7446651` to divide by and
left that cell empty; the cell is filled now.

**The baseline file was NOT re-recorded for this follow-up, deliberately.** `src/main` is
byte-identical from `bb8e988` through this commit — `git diff bb8e988 HEAD -- src/main` is empty —
so `benchmarks/results/baseline.json` still describes exactly the bytecode that ships, and
re-recording it would only bank one more run's noise into the gate's reference. Future comparisons
therefore keep the same reference: a PR is measured against numbers recorded at `740d532` on
JDK 21, and the 2% Tier-1 band is measured from there. The one figure in it worth knowing about is
`consolidate_mixedProduction` `avgt`, recorded at 300,297 B/op and reproduced today at 299,457 in
both modes — a 0.28% fork-to-fork difference, the largest seen in this suite, and in the safe
direction (a future run reproducing 299,457 reads as an improvement, not a regression).

The four iteration sections below keep the pass's own per-iteration measurements. Those were made
one change at a time and are **not** re-measured here — re-running them would mean rebuilding four
intermediate trees, and the end-to-end A/B above already confirms where they landed in aggregate.
Read the per-iteration deltas as that pass's record and the table above as this one's.

### Iteration 1 — character scans replace the two per-key regexes · KEPT

Predicted −25% to −36% on `wideFlat` (point −33%), −4.5 to −7.0 MB on `arrayHeavy` (point
−5.5 MB), −20% to −30% on `mixedProduction`, −1% to −2% on `deepNarrow`.

Measured: `wideFlat` **−35.98%**, `arrayHeavy` **−40.0%** (−6.26 MB), `mixedProduction`
**−18.2%**, `deepNarrow` **−1.44%**.

Two of the four landed inside the predicted band; `arrayHeavy` came in *above* it and
`mixedProduction` *below* it. The `deepNarrow` prediction was the interesting one: one analysis
predicted −35% there on the belief that the corpus has 24 keys. `deepNarrow(24)` is a single
nested chain and produces exactly **one** flattened key, so one Matcher out of 13,912 bytes was
all there was to win. That analysis was refuted before measuring, and the measurement agreed.

The wall-clock is the outlier of the whole pass, and it is not the Matcher. `deepNarrow` allocates
1.4% less at this iteration (2.0% across the pass) and runs **39.2x faster** — 71.68 → 1.83 µs in
the end-to-end pair above, and 40.2x against a second HEAD run nine minutes later. The pattern
`(.+?)\[(\d+)\](.*)` against a ~200-character key containing no bracket forces the reluctant
quantifier to try every start position against every expansion — quadratic backtracking, paid once
per key. `deepNarrow` has one very long key; `wideFlat` pays it a thousand times and dropped
**68.7%** (469.3 → 147.0 µs). The wall-clock controls moved +0.44% and −2.22% across that same
pair, which is what licenses reading 39.2x and −68.7% as the code rather than as the machine.

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

Ranked by measured allocation at HEAD, against measured corpus sizes. Every document size below
was measured on 2026-08-19; the sizes this section used to divide by were estimates that ran 1.8x
to 6.5x high, so every per-byte figure it previously published was wrong by that factor.

| # | Benchmark | B/op | input | amplification |
|---:|---|---:|---:|---:|
| 1 | `consolidate_arrayHeavy` | 7,663,876 | 189,951 B | 40.3x |
| 2 | `mapFlatten_arrayHeavy` | 2,486,304 | 189,951 B | 13.1x |
| 3 | `reconstruct_arrayHeavy` | 887,914 | † | † |
| 4 | `roundTrip_mixedProduction` | 651,705 | 11,356 B | 57.4x |
| 5 | `reconstruct_wideFlat` | 578,105 | † | † |
| 6 | `mapFlatten_wideFlat` | 538,577 | 24,665 B | 21.8x |
| 7 | `consolidate_wideFlat` | 398,449 | 24,665 B | 16.2x |
| 8 | `reconstructToJson_mixedProduction` | 356,282 | † | † |
| 9 | `reconstruct_mixedProduction` | 338,859 | † | † |
| 10 | `mapFlatten_mixedProduction` | 314,457 | 11,356 B | 27.7x |
| 11 | `consolidate_mixedProduction` | 300,297 | 11,356 B | 26.4x |
| — | `mapFlatten_deepNarrow64` | 69,000 | 834 B | 82.7x |

† the reconstruct path is fed a flattened map, not the JSON document, so a ratio against the
document size would not mean anything. Left blank rather than filled with a plausible number.

**1. `consolidate_arrayHeavy`, 7.66 MB/op.** Still the largest single-document allocator, 3.08x
the next entry. It is no longer 9x, and the exception-driven type detection once blamed for it is
long gone — that shipped in `843a461` on 2026-08-09, and iteration 4 removed the residual
`parseDouble` cost. What remains has **not** been attributed by measurement and this document
should not pretend otherwise. The arithmetic: 14,000 input leaves → **547 B/leaf**; 420 output keys
→ **18.2 KB per output key**; 40.3x the document itself. Candidates are Jackson's `readTree`,
14,000 `FlattenTask` objects and prefix strings, 14,000 `LinkedHashMap` entries, 14,000
`KeyedValue` objects and the stripped base keys — all of that is arithmetic, not observation.
**Before optimizing it further, run an allocation class histogram**; this pass has already shown
such estimates wrong by a factor of four in both directions.

**2. `MapFlattener`'s per-node map, now measured rather than asserted.** `benchmarks/README.md`
has listed "a per-node map allocation — `MapFlattener.flattenValue` allocates a throwaway
`LinkedHashMap` per value and is still there" as an unmeasured drill. Measured now, with
`ThreadMXBean.getThreadAllocatedBytes` over 20,000 warmed iterations per point at depths 8 to 96:

| depth | 8 | 16 | 24 | 32 | 48 | 64 | 96 |
|---|---:|---:|---:|---:|---:|---:|---:|
| B/op | 7,204 | 13,696 | 21,648 | 30,096 | 48,680 | 70,040 | 119,848 |

A least-squares fit of `a·d + b·d²` gives **786 B per level plus 4.81 B per level²**, and
reproduces six of the seven points to within 0.8% (depth 8 is 8% off, which is the constant term
the model omits). Two things follow, and they qualify the roadmap's framing of JFLAT-04 rather
than confirming it:

* The quadratic term is **real** and its coefficient is close to the predicted cost of rebuilding
  the prefix at every level: a level-`i` prefix of `level_NN.` keys is roughly `8i` bytes, and
  Σ 8i ≈ 4d², against a fitted 4.81.
* It is **not** the dominant cost at realistic depths. The quadratic share is 12.8% at depth 24,
  28% at 64 and 37% at 96. The linear 786 B/level term — one throwaway `LinkedHashMap`, its table,
  its entry and the level's key string, per level — is the larger prize until depth ~90.

For scale against the rest of the suite: at depth 24 that is 21,248 B to flatten a **314-byte**
document with exactly one leaf, i.e. 67.7x amplification, the worst ratio in the corpus.

**3. `consolidate_batch1000` is not an independent data point.** It measures 999.97x
`consolidate_mixedProduction` and always has. Optimizing "the batch number" means optimizing the
single-record number; there is no per-batch overhead to find.

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
