# NexusPiercer Benchmarks

JMH harness for the flatten, reconstruct, and round-trip paths.

## Why this is a separate project

It is not a module of the library reactor, and that is deliberate.

It depends on the library **artifact**, not its sources, so JMH measures exactly the bytecode
that ships to Maven Central rather than a recompilation under different flags. That is the whole
of the current justification.

It used to be half. The original rationale led with a second reason: JMH generates its harness
through an annotation processor, and the library module interleaved `maven-compiler-plugin` with
a five-execution gmavenplus lifecycle, so a processor added there would have run against a source
tree containing generated Java stubs for the Groovy classes — duplicate-class errors and ordering
that depended on which compile execution won the race.

**That reason expired on 2026-08-11**, when the Groovy toolchain was removed. There is no
gmavenplus lifecycle and no stub generation; the library module is a plain `maven-compiler-plugin`
build. Whether the separation is still worth its cost — a second reactor, a manual `install` step,
a `dependency-reduced-pom.xml` checked into the tree — now rests entirely on the artifact-fidelity
argument above. Recorded rather than silently deleted, because the answer may be that these should
be merged back, and that decision should be taken knowingly rather than inherited.

## Running

```bash
./mvnw install -DskipTests
```
```bash
mvn -f benchmarks/pom.xml clean package
```
```bash
java -jar benchmarks/target/benchmarks.jar -f 3 -wi 5 -i 10 -prof gc -rf json -rff results.json
```

Filter to one class or method:

```bash
java -jar benchmarks/target/benchmarks.jar FlattenBenchmark.mapFlatten_deepNarrow -prof gc
```

Compare against the committed baseline:

```bash
python3 benchmarks/compare.py --baseline benchmarks/results/baseline.json --current results.json
```

## The corpora

Five shapes, each isolating one complexity dimension. The point of separate corpora rather than
one "realistic" blob is attribution — when a number moves, you need to know which property of the
input moved it.

| Corpus | Shape | Isolates |
|---|---|---|
| `wideFlat` | 1,000 scalar fields, depth 1, ~45 KB | Cost linear in key count, no structural confound |
| `deepNarrow` | depth 24 (and 64), one field per level, ~2 KB | Depth-driven cost; quadratic-in-depth effects |
| `arrayHeavy` | 20 arrays x 500 + 5 record-arrays x 100 x 8, ~600 KB | Per-element cost and allocation rate |
| `unionNullable` | 200 three-branch unions, skewed to the last branch, ~12 KB | **Generated but never measured** — no `@Benchmark` consumes it |
| `mixedProduction` | 250 fields, depth 4, 12 arrays (p50 8 / p99 400), ~35 KB | Headline number; realistic shape |

Two properties are non-negotiable in the generator:

- **Deterministic.** Every corpus is seeded from a fixed constant, so baseline and PR measure
  byte-identical input. A corpus that varies between runs turns the gate into a coin flip.
- **Field names contain the separator.** `user_id`, `created_at`, `order_total` — snake_case is
  what real schemas look like in this domain, which makes `mixedProduction` a live regression
  test for key injectivity rather than a shape that quietly avoids the broken case.

## Metrics

`gc.alloc.rate.norm` (bytes per operation) is the **primary** gate metric, not throughput. Most
of the findings this harness exists to track are allocation-driven — throwaway maps, exception
objects, Pattern/Matcher pairs, boxed integers, intermediate strings — and allocation per
operation is a near-deterministic counter rather than a timing measurement. It does not move with
runner load, which is why it can carry a 2% gate where throughput needs 10%.

## The gate

| Tier | Metric | Threshold | Runs on |
|---|---|---|---|
| 1 | `gc.alloc.rate.norm` | +2%, absolute, no retry | Every PR |
| 1 | `invokedynamic` count | Any increase | Every PR |
| 2 | Throughput | −10% **and** disjoint 99.9% CIs | Merge queue, nightly |
| 2 | Suite geometric mean | −5% with disjoint CIs | Merge queue, nightly |

Tier 2 never fails on overlapping confidence intervals, regardless of the point estimate. That
rule is what prevents the false failures that get performance gates disabled.

Intentional trades go in [`waivers.yml`](waivers.yml) with an expiry date. That mechanism was
documented from the start and **implemented on 2026-08-19** — before then `compare.py` never
opened the file, so a waiver would have been silently ignored and its author blocked by the very
regression they had just declared acceptable. Expired and malformed waivers now fail the build,
as the file always claimed.

## A gate is not live until it has been seen to fail

The drills are now executable. Run them:

```bash
python benchmarks/test_compare.py
```

23 drills, added 2026-08-19, covering both directions. Until then **nothing exercised
`compare.py` at all** — `docs/ANTI_REGRESSION.md` records exactly two manual drills, both of
which injected a regression and watched it block, and one of which was itself a false pass (it
exited 1 from a `FileNotFoundError` rather than from a gate decision). Every drill therefore
asserts the reported REASON as well as the exit code.

The drills that mattered were the ones nobody had run — the states where the gate reports success
because it measured nothing. All four were real, and all four are fixed:

1. **Empty results file** → used to exit 0 printing "No blocking regressions detected".
2. **A baseline benchmark did not run** → used to exit 0. The loop iterated the CURRENT run only,
   so a renamed benchmark, a broken `@Setup` or a mistyped filter silently removed that
   benchmark's Tier-1 check.
3. **`-prof gc` dropped** → used to exit 0. Tier 1 lived inside `if "alloc" in b and "alloc" in
   c`, so with no allocation data the only blocking tier evaluated on zero benchmarks. CI passes
   `--throughput advisory`, so nothing else could block either: the whole gate went decorative
   and said so in green.
4. **`@Param` rows collapsing** → `load()` keyed on benchmark+mode with no params, so all six
   rows of `SchemaCacheCliffBenchmark.rotateThroughSchemas` mapped to one key and five were
   discarded on load. The cliff row this benchmark exists to expose was among them.

The two original drills below are still worth running, and both were found **in shipped code**
rather than needing to be reintroduced:

1. `Pattern.compile` inside a loop — was live at `processGroupedValues`, 432 compiles per
   `mixedProduction` record, removed 2026-08-19.
2. A per-node map allocation — `MapFlattener.flattenValue` allocates a throwaway `LinkedHashMap`
   per value and is still there. Unmeasured by this pass.

A stale harness is a fifth vacuous pass and has its own check, because `benchmarks/` depends on
the library ARTIFACT rather than its sources — skip `./mvnw install -DskipTests` and you measure
the previous build while reporting a clean 0.00%:

```bash
python benchmarks/check_harness_fresh.py
```

That one is not hypothetical either: it was hit during the 2026-08-19 pass, by the person writing
up the hole, and caught only because the wrong number happened to be recognisable.

An untested gate is indistinguishable from a disabled one. This repository already shipped a 20%
coverage floor against 60% actual coverage and three static-analysis plugins that had never been
downloaded; all of them reported success continuously.
