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
| **Correctness** | Does it still do the right thing? | [`ci.yml`](../.github/workflows/ci.yml) | Any test failure on JDK 17/21, Linux/Windows; coverage below the ratchet; cold-clone build failure; any `.groovy` file or Groovy/Spock coordinate returning |
| **Quality** | Is the code still maintainable and safe? | [`quality.yml`](../.github/workflows/quality.yml) | High-severity advisories in new dependencies; CodeQL findings; Checkstyle above 0 or PMD/SpotBugs above the recorded ceilings |
| **Performance** | Is it still fast? | [`benchmark.yml`](../.github/workflows/benchmark.yml) | Allocation +2%; throughput −10% with disjoint CIs |

## Correctness

### The coverage ratchet

`jacoco.minimum.coverage` may only ever increase. If a PR raises coverage, it raises the floor in
the same PR. Lowering it to make a build pass is prohibited by [CONTRIBUTING.md](../CONTRIBUTING.md).

The floor is set just under measured coverage rather than at it, so ordinary variation does not
produce false failures — but "just under" means a point or two, not forty.

### Structural assertions

Some regressions are invisible to tests. `ci.yml` asserts them directly:

- **No Groovy anywhere.** Job `no Groovy anywhere` in `ci.yml`, running
  [`scripts/assert-groovy-free.sh`](../scripts/assert-groovy-free.sh). It fails if any
  `.groovy` file is tracked in the repository, if any POM — root *or* `benchmarks/` — declares
  `gmavenplus`, a Groovy runtime or Spock, or if a Groovy or Spock artifact reaches the resolved
  test classpath. The third check is the one that catches a transitive return: Spock is
  Groovy-native and brings the runtime back with it.

  This **replaced** "Groovy compiles exactly once" on 2026-08-11. That assertion guarded a real
  defect — `gmavenplus-plugin` declared in both `pluginManagement` and `build/plugins` with
  different execution ids, so Maven merged rather than overrode and compiled all 76 Groovy sources
  twice per build — but the plugin, both declarations and the last `.groovy` source are now gone,
  so its `grep -c 'Compiled .* files'` could only return 0 and its `-gt 1` check could only pass.

  It was first placed in `benchmark.yml` and that was wrong: that workflow's `pull_request`
  trigger is path-filtered to `src/main`, `benchmarks`, `pom.xml` and itself, so a PR adding
  `src/test/groovy/FooTest.groovy` started no workflow at all and the gate never ran. `ci.yml`
  has no path filter. Recorded because the failure mode — a correct check that cannot be reached —
  is indistinguishable from no check, and a local drill cannot detect it.
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

- **`invokedynamic` count — NO LONGER A GATE.** Reported into the job summary; nothing fails on it.

  It was a ratchet, and a good one while it measured what it claimed to. Every Groovy dynamic call
  site compiled to one `invokedynamic`, so the count was an exact, zero-variance proxy for how much
  of the hot path was still dynamically dispatched. It went **7,168 → ~378** as `src/main` was
  ported to Java.

  Then it started firing on correct work: adding a `record` for cache statistics took it 378 → 381
  and failed the build for writing idiomatic Java. With Groovy gone, everything it counts is
  ordinary Java — lambdas, method references, string concatenation, record accessors — none of
  which carries dynamic-dispatch cost, and all of which grows legitimately. A gate that blocks
  correct work gets switched off, so it was downgraded rather than left to be disabled later.

  The invariant it was a proxy for is now asserted directly: see **No Groovy anywhere** above.

  Recorded value in [`benchmarks/results/indy-ceiling.txt`](../benchmarks/results/indy-ceiling.txt)
  was **378** and had drifted — measured **413** on 2026-08-11 across `target/classes`, and the
  file is updated to match. Under the old ratchet that 35-site rise would have been a build
  failure; it is 35 more lambdas and record accessors. The filename still says "ceiling"; it is a
  last-recorded observation.

  The 7,168 baseline, for the record — all five of these classes are now Java and contribute a
  handful of sites between them:

  | Class | Sites (2026-08-09, as Groovy) |
  |---|---:|
  | `AvroReconstructor` | 1,773 |
  | `MapFlattener` | 677 |
  | `JsonReconstructor` | 537 |
  | `GAvroSchemaFlattener` | 501 |
  | `JsonFlattener$FluentOperation` | 265 |

  **Test-count conservation across the port, measured both sides.** The Groovy toolchain is gone
  from `main`, but it is still resolvable from `~/.m2`, so the pre-port tree was built in a
  detached worktree at `fc1139e` and the two runs compared on Maven's own summary line:

  | | pre-port (Groovy) | post-port (Java) |
  |---|---:|---:|
  | whole suite | **2325** | **2333** |
  | failures / errors | 0 / 0 | 0 / 0 |
  | skipped | 15 | 15 |
  | the 14 case-bearing ported classes | **232** | **232** |
  | new tests for the 3 zero-case example classes | 0 | 8 |

  All fourteen classes match one-for-one — 1, 46, 9, 3, 1, 4, 2, 7, 62, 45, 6, 4, 33, 9 — and the
  whole-suite delta is exactly +8, the two new example tests. Nothing was lost.

  **Correction worth stating loudly:** the figure carried into this work was that the 17 `.groovy`
  files contributed **249** executed cases. Measured, it is **232**. The 249 appears to have come
  from summing per-class log lines, which double-count `@Nested` classes (Surefire prints each one
  twice) — the same undercount-by-XML / overcount-by-log trap this document warns about in the
  other direction. Chasing 249 would have meant manufacturing 17 tests that never existed.

  Worth stating plainly, because it is why the port was cheap: none of the Groovy sources used a
  single Groovy language feature — no `def`, no closures, no GStrings, no `@CompileStatic`. They
  were Java source with a `.groovy` extension, paying dynamic-dispatch cost on the per-record path
  for no language benefit. That is also why they failed at *runtime* rather than compile time when
  edited as Java, and why the port was a transcription rather than a rewrite.
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
| Coverage ratchet at 64% | Live — raised from 58% on 2026-08-11 against a measured 65.46% |
| Cold-clone reproducibility check | Live |
| No Groovy anywhere / no Groovy or Spock coordinate | Live — `ci.yml` job `no Groovy anywhere`, on every unfiltered push and PR |
| ~~Single-Groovy-compile assertion~~ | **Removed 2026-08-11** — superseded by the row above; the plugin it guarded no longer exists, so it could only ever pass |
| Dependency review (new deps) | Live, blocking |
| CodeQL | Live |
| SBOM generation | Live |
| Checkstyle / PMD / SpotBugs | Live, **blocking** against the ceilings in `.github/quality-baseline.json` (0 / 323 / 231) |
| OWASP CVE scan | **Reporting only** — two known CVEs to clear first |
| JMH harness + recorded baseline | Live — see [PERFORMANCE.md](PERFORMANCE.md) |
| ~~`invokedynamic` ratchet at 7,168~~ | **Downgraded to an observation 2026-08-11** — see below. Currently 413, reported not gated |
| Tier 1 / Tier 2 comparison gate | Live, and **drilled in both directions** |
| Gate-failure drills (CI-level) | **Not yet run** — the four in the table above |

### Drills completed

`compare.py` has been exercised against both outcomes on the real baseline:

| Input | Result |
|---|---|
| Baseline vs. itself | `No regressions detected`, geometric-mean drift +0.00%, exit 0 |
| Baseline vs. synthesized +25% allocation / +40% time | Tier 1 failure reported, `BLOCKED`, exit 1 |

The first attempt at that second drill produced a **false pass**: it exited 1, but from a
`FileNotFoundError` rather than a gate decision. Worth recording, because an exit code alone does
not tell you the gate fired — the drill has to assert on the reported reason.

The four CI-level drills (syntax error, coverage drop, hoisted `Pattern.compile`, per-node
allocation) still need to be run against a live pull request before the pipeline as a whole can
be described as verified.

The reporting-only entries are a deliberate, time-boxed state, not the end state. Turning
Checkstyle, PMD, and SpotBugs on for the first time against ~19,860 never-linted lines produces a
large violation count at once; without a trustworthy green baseline underneath, that flood is
unattributable noise and the predictable response is to switch the gates back off — which is
exactly the history recorded in this POM. The sequencing is in
[docs/audit/ROADMAP.md](audit/ROADMAP.md).
