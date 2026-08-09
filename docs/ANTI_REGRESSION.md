# The anti-regression system

This document describes how NexusPiercer prevents backsliding, and — more importantly — why each
control is shaped the way it is. Most of the design decisions here are reactions to a specific
failure this repository already had.

## The governing principle

> **A gate that has never been observed to fail is indistinguishable from a disabled gate.**

That is not an abstraction. Before this system existed, the project had:

- A **20% JaCoCo coverage floor** against 60.3% actual coverage. It reported
  `All coverage checks have been met` on every build and would have continued to do so through a
  two-thirds regression.
- **Checkstyle, PMD, and SpotBugs** configured in the POM, with a checked-in Checkstyle ruleset and
  PMD ruleset — none of which had ever executed. The audit proved it by observing that all three
  plugins were absent from the local `~/.m2` repository entirely: never downloaded, let alone run.
  They sat inside a `quality` profile that an `activeByDefault` `development` profile cancelled,
  and pointed at SpotBugs filter files that did not exist on disk.
- A **CI workflow** at the repository root, where GitHub Actions never looks. Zero runs, ever.
- A **release pipeline** that would have published unsigned artifacts to a decommissioned endpoint.

Every one of those looked like a control and functioned as decoration. So each gate below has an
explicit answer to "how do we know this one actually fails?"

## Three axes, three gates

Regression is possible along three independent axes. A system that gates only one of them will
lose the other two.

| Axis | Question | Gate | Blocks on |
|---|---|---|---|
| **Correctness** | Does it still do the right thing? | [`ci.yml`](../.github/workflows/ci.yml) | Any test failure on JDK 17/21, Linux/Windows; coverage below the ratchet; cold-clone build failure; Groovy compiling twice |
| **Quality** | Is the code still maintainable and safe? | [`quality.yml`](../.github/workflows/quality.yml) | High-severity advisories in new dependencies; CodeQL findings; Checkstyle/PMD/SpotBugs violations *(reporting until Phase 2)* |
| **Performance** | Is it still fast? | [`benchmark.yml`](../.github/workflows/benchmark.yml) | Allocation +2%; `invokedynamic` count increase; throughput −10% with disjoint CIs |

## Correctness

### The coverage ratchet

`jacoco.minimum.coverage` may only ever increase. If a PR raises coverage, it raises the floor in
the same PR. Lowering it to make a build pass is prohibited by [CONTRIBUTING.md](../CONTRIBUTING.md).

The floor is set just under measured coverage rather than at it, so ordinary variation does not
produce false failures — but "just under" means a point or two, not forty.

### Structural assertions

Some regressions are invisible to tests. `ci.yml` asserts them directly:

- **Groovy compiles exactly once.** The build previously compiled all 76 Groovy sources twice,
  because `gmavenplus-plugin` was declared in both `pluginManagement` and `build/plugins` with
  different execution ids, so Maven merged the two sets instead of overriding. Nothing failed; the
  build was just silently doing double work. A grep on the reactor log now catches it.
- **`--add-opens` is still configured.** Spark and Hadoop reflect into JDK internals. If the
  property is dropped, the failure surfaces as an `InaccessibleObjectException` deep in a Spark
  stack trace. The check fails early with a readable message instead.
- **A cold clone builds.** A build that only works against a warm `~/.m2` is not reproducible.

## Performance

### Why allocation, not throughput, is the primary metric

Most of the audit's performance findings are allocation-driven: throwaway `LinkedHashMap`s per
tree node, exception objects used for control flow, `Pattern`/`Matcher` pairs, boxed integers,
intermediate strings.

`gc.alloc.rate.norm` — bytes allocated per operation — is derived from thread-allocation
accounting, not from a clock. It does not move with runner load. That is what lets it carry a 2%
band where throughput needs 10%, and it is why a shared-vCPU GitHub runner can gate on it honestly.

### Why throughput uses confidence intervals, not point estimates

Tier 2 blocks only when **both** the 99.9% confidence intervals are disjoint **and** the point
estimate is more than 10% worse. Overlapping intervals never fail, regardless of the point estimate.

This is the rule that keeps the gate alive. A perf gate that fires on noise gets muted within a
month, and a muted gate is the 20%-coverage-floor problem again in a new costume.

Suite-wide drift is caught separately: a 5% regression in the geometric mean across all benchmarks
blocks even when no individual benchmark trips. That is the death-by-a-thousand-cuts case where
everything degrades 4% and nothing individually looks wrong.

### Deterministic static metrics

Some things have zero variance and therefore deserve an absolute threshold rather than a noise band:

- **`invokedynamic` count.** Every Groovy dynamic call site compiles to one. The count is a direct
  proxy for how much of the hot path is still dynamically dispatched, and it is exact. It may only
  decrease. Ceiling lives in [`benchmarks/results/indy-ceiling.txt`](../benchmarks/results/indy-ceiling.txt).

  Measured on 2026-08-09: **7,168** across `io.github.pierce.**`.

  | Class | Sites |
  |---|---:|
  | `AvroReconstructor` | 1,773 |
  | `MapFlattener` | 677 |
  | `JsonReconstructor` | 537 |
  | `GAvroSchemaFlattener` | 501 |
  | `JsonFlattener$FluentOperation` | 265 |

  Worth stating plainly: none of the Groovy sources use a single Groovy language feature — no
  `def`, no closures, no GStrings, no `@CompileStatic`. They are Java source with a `.groovy`
  extension, paying dynamic-dispatch cost on the per-record path for no language benefit. The
  target for this counter is 0.
- **Hot-method bytecode size.** Methods over ~325 bytes are never inlined by HotSpot, which blocks
  scalar replacement of the temporaries they return. Gating on the size directly is far stronger
  than hoping the effect shows through benchmark noise.

### The corpora

Five deterministic shapes, each isolating one complexity dimension —
see [benchmarks/README.md](../benchmarks/README.md). Separate corpora rather than one realistic
blob, because attribution matters: when a number moves you need to know *which property of the
input* moved it.

Determinism is enforced by seeding every generator from a fixed constant. A corpus that varies
between runs turns the regression gate into a random number generator.

### Waivers

An intentional performance trade goes in [`benchmarks/waivers.yml`](../benchmarks/waivers.yml)
with a named benchmark, an accepted percentage, an expiry date, and a justification. Waivers need
CODEOWNERS approval and appear in the CI summary.

Expired waivers fail the build. That is deliberate — it forces a re-decision instead of letting a
temporary allowance become permanent through inattention.

## Validating the gates

**Do not trust any of this until you have watched it fail.** Four drills:

| Drill | Expected |
|---|---|
| Introduce a syntax error | `ci.yml` blocks the merge |
| Delete a test to drop coverage 2pp | Coverage gate blocks |
| Reintroduce a `Pattern.compile` inside a loop | Tier 2 blocks |
| Reintroduce a per-node map allocation | Tier 1 blocks |

Until all four have been run and observed, the correct description of this system is "probably
works", not "works".

## Current status

| Control | State |
|---|---|
| CI on every push and PR | Live |
| JDK 17 + 21, Linux + Windows matrix | Live |
| Coverage ratchet at 58% | Live |
| Cold-clone reproducibility check | Live |
| Single-Groovy-compile assertion | Live |
| Dependency review (new deps) | Live, blocking |
| CodeQL | Live |
| SBOM generation | Live |
| Checkstyle / PMD / SpotBugs | **Reporting only** — flips to blocking in Phase 2 |
| OWASP CVE scan | **Reporting only** — two known CVEs to clear first |
| JMH Tier 1 / Tier 2 | Harness live; **baseline not yet recorded** |
| Gate-failure drills | **Not yet run** |

The reporting-only entries are a deliberate, time-boxed state, not the end state. Turning
Checkstyle, PMD, and SpotBugs on for the first time against ~19,860 never-linted lines produces a
large violation count at once; without a trustworthy green baseline underneath, that flood is
unattributable noise and the predictable response is to switch the gates back off — which is
exactly the history recorded in this POM. The sequencing is in
[docs/audit/ROADMAP.md](audit/ROADMAP.md).
