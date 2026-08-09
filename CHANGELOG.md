# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- **Flattened key encoding is now injective.** `FlattenedPath` escapes separator characters
  inside field names, so a field literally named `user_id` encodes to `user\_id` and no longer
  collides with the nested path `user` → `id`. Field names containing no separator encode
  byte-identically to before, so the common case is unaffected; this is asserted by a property
  test. `FlattenedPath.encodeLegacy` is retained, deprecated, for reading older data.

  This fixes two defects at once:
  - `arch/NP-002` — reconstruction was lossy for any schema whose field names contain the
    separator, i.e. most snake_case schemas.
  - `perf/NP-021` — reconstruction cost was superlinear in the number of separator characters per
    field name. Holding structure fixed at 40 flattened keys, `nested_field_{n}` went 1,198 ms →
    3,435 ms → OutOfMemoryError as record count rose. It is now 3–7 ms and, more importantly,
    **independent of underscore count**: a three-underscore field name costs the same as a
    one-underscore name. The `reconstruct_arrayHeavy` benchmark went from exhausting a 2 GB heap
    to allocating ~1.0 MB/op.

### Added
- Continuous integration (`ci.yml`): full test suite on JDK 17 and 21 across Linux and Windows,
  a coverage gate, and a cold-clone reproducibility check.
- Quality workflow (`quality.yml`): Checkstyle, PMD, and SpotBugs execution, GitHub dependency
  review, OWASP CVE scanning, CycloneDX SBOM generation, and CodeQL.
- Release workflow (`release.yml`) that produces signed, tagged, SBOM-accompanied artifacts.
- Maven wrapper (`mvnw`), so the build no longer depends on a locally-installed Maven.
- `security` Maven profile for CVE scanning and SBOM generation.
- SpotBugs include/exclude filters at `src/main/spotbugs/` — the paths the POM had referenced
  for some time without the files existing.
- Governance: `README.md`, `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, `CODEOWNERS`,
  issue and pull-request templates, and Dependabot configuration.
- Full engineering audit at `docs/audit/FINDINGS.md` (200 verified findings) with a phased
  remediation plan at `docs/audit/ROADMAP.md`.

- JMH benchmark harness at `benchmarks/`, with five deterministic corpora (wide-flat,
  deep-narrow, array-heavy, union/nullable-heavy, mixed-production), a two-tier regression gate,
  and a recorded baseline.
- `SparkAvailability` probe, so Spark tests self-skip on environments that cannot open an NIO
  selector instead of erroring — and are *required* to run on Linux CI.
- `docs/ANTI_REGRESSION.md` describing the three gate axes and why each is shaped as it is.

### Fixed
- **`JsonReconstructor` was 1,294 lines of entirely commented-out source** sitting in the main
  compile root — the class was documented and referenced but absent from the shipped jar.
  Restored, with 11 Groovy semantic fixes (8 bare enum labels in `switch` cases, which Groovy
  resolves as property reads and which threw `MissingPropertyException` at runtime, plus 3
  single-quoted char literals). Its 45 tests were also fully commented out; all 45 now pass,
  putting the JSON flatten→reconstruct round-trip invariant under test for the first time.
- **26 jqwik property tests had never executed.** Only `jqwik-api` was declared — no engine — so
  the JUnit Platform had nothing to run `@Property` methods with, and the file name
  (`TypeConverterProperties.java`) matched no Surefire include pattern. Switched to the `jqwik`
  aggregate, moved it to `test` scope (it was leaking into consumers' compile classpath), and
  renamed the class. All 26 now run and pass.
- **All four `io.github.pierce.spark` test classes were 100% commented out**, leaving 2,995
  instructions of shipped public API at 0% coverage. Restored; they now execute on CI.
- Groovy sources were compiled **twice** on every build. `gmavenplus-plugin` was declared in both
  `pluginManagement` and `build/plugins` with different execution ids, so Maven merged the two
  sets rather than overriding.
- The `maven-enforcer-plugin` `requireOS` rule listed two `<family>` elements. The parameter takes
  a single value, so it silently evaluated to `family=windows` and would have hard-failed every
  Linux CI run at the `validate` phase.
- Removed the `development` profile, which was `activeByDefault` and set
  `checkstyle.skip`/`pmd.skip`/`spotbugs.skip` to `true` — the reason the checked-in rulesets had
  never run.
- The release workflow no longer publishes unsigned artifacts: it activates `-Prelease` (without
  which `gpg.skip` stayed `true`), uses a `server-id` matching the publishing plugin, derives
  `gpg.keyname` from the imported key, and asserts every artifact has a `.asc` signature before
  attaching it to a release.

### Changed
- `lib/` is now gitignored. It held 16MB of Groovy 5.0.0 jars, untracked but not ignored, while
  the build compiles against Groovy 4.0.21 — one `git add -A` away from entering permanent history.
- `README.md` now documents the API that exists. The previous documentation described four
  `NexusPiercerPatterns` methods that were never implemented; the class has two.

### Removed
- `maven-publish.yml` from the repository root, where GitHub Actions never executed it.
  Superseded by `.github/workflows/release.yml`.

## [1.0.8] - 2025-12-09

Published to Maven Central. No corresponding git tag exists for this release; it is not traceable
to a specific commit. Earlier history is recorded only in commit messages.

[Unreleased]: https://github.com/pierce-lonergan/NexusPiercer/compare/v1.0.8...HEAD
[1.0.8]: https://central.sonatype.com/artifact/io.github.pierce-lonergan/nexus-piercer/1.0.8
