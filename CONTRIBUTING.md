# Contributing to NexusPiercer

Thanks for your interest. This document is short and specific — it covers what the build expects
and what will block your PR.

## Prerequisites

- **JDK 17** (the build targets 17; CI also runs 21)
- No Maven install needed — use the wrapper: `./mvnw`

Verify your toolchain matches the build's:

```bash
./mvnw -v
```

If your `JAVA_HOME` points at a JDK newer than 21, set it to 17 for local work. Maven and `java`
resolving to *different* JDKs is a known source of confusing failures here.

## Build and test

```bash
./mvnw verify
```

That runs the full suite (2,689 test invocations, roughly 4 minutes). Faster loops:

**Read the count from Maven's `Tests run:` summary line, not from the surefire XML.** Summing
`target/surefire-reports/*.xml` UNDERCOUNTS here by **exactly 532** — measured 2,157 against
Maven's 2,689 test invocations on 2026-08-19 — because `@Nested` classes emit a separate report
per nested class
while the outer class's own report records `tests="0"`. The gap has been 532 at every measurement
since 2026-08-17, when the suite was 1,840 against 2,372. The Maven summary line is authoritative.

Both figures are gated. `PublishedProjectFactsMatchTheSourceTest` reads EVERY occurrence of the
phrase `N test invocations` in this file, `docs/INSTALL.md` and `docs/ANTI_REGRESSION.md` and
requires all of them to equal the count recorded in `.github/quality-baseline.json`, and it checks
the surefire-XML figure and the stated gap against the same file. A HISTORICAL suite size must not
be written in that phrase — date it and say "the suite was N invocations", which the gate
deliberately does not match.

```bash
./mvnw -Pfast package
```
```bash
./mvnw test -Dtest=JsonFlattenerConsolidatorTest
```

Static analysis and supply-chain scans run under their own profiles:

```bash
./mvnw -Pquality verify
```
```bash
./mvnw -Psecurity verify
```

## What CI checks

Every pull request runs three independent gates. All three must be green to merge.

| Axis | Workflow | What blocks you |
|---|---|---|
| **Correctness** | `ci.yml` | Any test failure on JDK 17 or 21, Linux or Windows. Coverage below the `jacoco.minimum.coverage` floor. A cold-clone build failure. Any `.groovy` file, or any Groovy/Spock coordinate, reappearing in the repository. |
| **Quality** | `quality.yml` | New dependencies with high-severity advisories. CodeQL security findings. Checkstyle above 0, or PMD/SpotBugs above the ceilings in `.github/quality-baseline.json` — **blocking**, not reporting. |
| **Performance** | `benchmark.yml` | Allocation per operation up more than 2% against baseline. Throughput down more than 10% with disjoint confidence intervals. |

The performance gate is two-tier because allocation counters and wall-clock timings have very
different noise characteristics — details in
[docs/audit/ROADMAP.md](docs/audit/ROADMAP.md#regression-gate).

## Coverage is a ratchet

`jacoco.minimum.coverage` in `pom.xml` may only ever go **up**. If your PR raises coverage, raise
the floor to match in the same PR. It is not permitted to lower it to make a build pass.

This rule exists because the floor previously sat at `0.20` against 60.3% actual coverage, which
meant the gate could report success through a two-thirds regression.

**The same rule governs the static-analysis ceilings**, and they are the gate you are far more
likely to trip. `.github/quality-baseline.json` records a ceiling for PMD and SpotBugs, and
Checkstyle at a hard zero. Ceilings may only ever go **down**. Fix the finding; never raise the
number, and never add a suppression to `src/main/spotbugs/spotbugs-exclude.xml` in place of a fix.

If your change lowers a count, lower the ceiling in the same commit to lock it in — the workflow
prints `::notice::<tool> fell from X to Y` to tell you to. To re-measure:

```bash
./mvnw -Pquality verify -DskipTests -Djacoco.skip=true \
    -Dspotbugs.fail=false -Dcheckstyle.fail=false -Dpmd.violation.buildFailOnViolation=false
```

A suppression must name a specific class **and** method with written reasoning. A `<Match>` that
enumerates several classes with no `<Method>` is a blanket exemption and is rejected by
`SpotBugsExcludeHasNoBlanketClassBlockTest` — one such block hid ten real findings for months.

## Benchmarks

If you change anything on a flatten, reconstruct, or conversion path, include a JMH delta:

```bash
./mvnw -Pbenchmarks package
java -jar benchmarks/target/benchmarks.jar -f 3 -wi 5 -i 10 -prof gc -rf json -rff after.json
```

Paste the before/after into the PR. If a change intentionally trades throughput for correctness,
add an entry to `benchmarks/waivers.yml` with a justification and an expiry date — accepted
regressions should be visible and time-boxed, not silently absorbed.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/). The release notes are generated
from them.

```
feat(flatten): thread a single accumulator through the recursion
fix(avro): guard against self-referential schemas
perf(reconstruct): hoist schema fingerprint out of the per-record path
docs(readme): correct the NexusPiercerPatterns method list
```

Use `!` or a `BREAKING CHANGE:` footer for anything that changes public API.

The history before this convention was adopted contains sequences like `enhanced the project`
repeated three times and `added type conversion logic` repeated seven; please don't extend it.

## Pull requests

1. Branch from `main`.
2. Keep one logical change per PR. Refactors, behaviour changes, and performance work must not be
   combined — a mixed PR cannot be bisected or reverted cleanly.
3. Add tests. New behaviour without a test will be asked for one.
4. Do not commit binaries. `lib/` is gitignored for this reason.
5. Fill in the PR template, including the benchmark section when it applies.

## Adding tests

- Java tests: `src/test/java`, named `*Test.java`. **Java only — this repository is Groovy-free.**
  A `.groovy` file anywhere in the tree fails CI (`no Groovy anywhere` in `ci.yml`), and since the
  compiler plugin was removed it would not be compiled or executed either: it would sit there
  looking like a test and running nothing.
- Property tests use [jqwik](https://jqwik.net/) and must be named `*Test.java` to be picked up
  by Surefire. A file named `*Properties.java` **will not run** — that mistake previously left 26
  property tests silently unexecuted.
- Integration tests are `*IntegrationTest.java` and run under Failsafe.

Avoid wall-clock assertions. Tests that assert on absolute milliseconds or on `System.gc()`
memory deltas are flaky by construction; assert on complexity or allocation behaviour instead.

## Reporting bugs

Open an issue with a minimal reproducing input — for this library that usually means the smallest
JSON document or `.avsc` that shows the problem, plus the expected and actual output.

Security issues go to [SECURITY.md](SECURITY.md), not the public tracker.
