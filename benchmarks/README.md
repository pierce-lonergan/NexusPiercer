# NexusPiercer Benchmarks

JMH harness for the flatten, reconstruct, and round-trip paths.

## Why this is a separate project

It is not a module of the library reactor, and that is deliberate.

JMH generates its harness through an annotation processor. The library module interleaves
`maven-compiler-plugin` with a five-execution gmavenplus lifecycle, so an annotation processor
added there runs against a source tree that includes generated Java stubs for the Groovy classes
— producing duplicate-class errors and ordering that depends on which compile execution wins.

Keeping the benchmarks in a pure-Java project sidesteps that, and buys something more valuable:
this project depends on the library **artifact**, so JMH measures exactly the bytecode that ships
to Maven Central rather than a recompilation under different flags.

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
| `unionNullable` | 200 three-branch unions, skewed to the last branch, ~12 KB | Exception-driven control flow |
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

Intentional trades go in [`waivers.yml`](waivers.yml) with an expiry date.

## A gate is not live until it has been seen to fail

Before trusting any of this, run both drills:

1. Reintroduce a `Pattern.compile` inside a loop → Tier 2 must block.
2. Reintroduce a per-node map allocation → Tier 1 must block.

An untested gate is indistinguishable from a disabled one. This repository already shipped a 20%
coverage floor against 60% actual coverage and three static-analysis plugins that had never been
downloaded; all of them reported success continuously.
