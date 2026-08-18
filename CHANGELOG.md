# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Version on `main` is `2.1.0-SNAPSHOT`. Everything below is additive; `2.0.0` is released and
staged on Maven Central and `main` must not sit on a released coordinate.

### Added

- **`JsonFlattener.Builder.buildFlattener()` and `JsonFlattener.newOperation()`** — the reusable
  engine. Before this, no consumer could obtain a `JsonFlattener` at all: the constructor is
  private and `create()`, both `with(...)` overloads and `Builder.build()` all return
  `FluentOperation`. A caller who configured `builder().maxDepth(64)` therefore had to either
  re-run the whole builder chain per document or reuse one `FluentOperation` across documents —
  and `FluentOperation` holds unsynchronised mutable state, so two threads calling `from(...)`
  on a shared one race on the loaded document. `buildFlattener()` yields the immutable,
  shareable engine; `newOperation()` yields a fresh per-document pipeline.

  ```java
  JsonFlattener engine = JsonFlattener.builder().maxDepth(64).buildFlattener(); // share freely
  Map<String, Object> a = engine.newOperation().from(docA).toMap();             // per document
  ```

  `build()` is unchanged and still returns `FluentOperation`; existing chains recompile and
  re-link untouched. Closes [BL-010].

### Fixed

- **`JsonFlattener.with(mapFlattener, null)` no longer throws `NullPointerException`.** The
  constructor guarded the parameter into `this.config` and then dereferenced the raw parameter
  on the next line, so the null-defence was present and inert. Observed verbatim before the fix:
  `NullPointerException: Cannot invoke "…JsonFlattenerConfig.isUsePrettyPrint()" because
  "config" is null` at `JsonFlattener.<init>(JsonFlattener.java:224)`. This is a behaviour change
  on a released method — it starts working instead of throwing — and no reasonable caller depends
  on a constructor NPE, but it is listed rather than slipped in silently.

- **Ten SpotBugs findings that a blanket exclude was hiding.** `src/main/spotbugs/spotbugs-exclude.xml`
  carried a five-class × three-pattern `<Match>` with no method narrowing over `AvroReconstructor`,
  `GAvroSchemaFlattener`, `JsonFlattener`, `JsonReconstructor` and `MapFlattener`. It is deleted,
  not narrowed, and all ten are fixed: five dead private methods removed, three anonymous
  `TypeReference` instances hoisted into shared `private static final` constants, and two
  unreachable null checks removed. No behaviour change — the 156-fixture fidelity corpus was
  re-run and no row moved. SpotBugs 241 → 238 from these ten, and **237 once the constructor
  repair above is counted**; PMD 361 → 351. The end state is 237, which is the ceiling recorded
  in `.github/quality-baseline.json`. (The walk: 241 with the block, 251 without it, 237 after
  the fixes. An earlier version of this bullet and of the baseline note both stated the final
  figure as 238 and never printed 237, so the public record disagreed with the repository.)

### Changed

- `JsonFlattener`'s class javadoc no longer claims "thread-safe for concurrent use" without
  qualification. That was true of `JsonFlattener` and false of `FluentOperation`, which was the
  only object the API could hand out. The two halves are now stated separately.

### Documentation

- `docs/BACKLOG.md` [BL-007] is closed as **refuted**: the claim that "the entire JsonReconstructor
  class (~1294 lines) is commented out" was false. The class is 1295 lines of live Java with 72
  comment lines and **zero** commented-out code lines, covered by 45 tests. The same false sentence
  was carried in `docs/CONCERNS.md` C-001, `docs/CLASS_REGISTRY.md`, `docs/PROJECT_OVERVIEW.md`
  and `docs/ARCHITECTURE_GRAPH.md`, and is corrected in all of them.
- `docs/BACKLOG.md` [BL-010]'s claim that JsonFlattener "has **zero** tests anywhere in the
  repository" was also false when written: `src/test/groovy/JsonFlattenerTest.groovy` existed at
  the commit where the entry was filed and is now
  `src/test/java/io/github/pierce/JsonFlattenerTest.java`, 63 tests in 12 nested classes.
- `docs/INSTALL.md` still told consumers "the 2.0 line is not published there yet" and pinned a
  `2.0.0-SNAPSHOT` coordinate that has never existed on any repository, while `SECURITY.md` and
  `README.md` in the same tree correctly published 2.0.0 from Central. Route 1 now publishes the
  released coordinate, locally built artifact filenames use a `${nexus.version}` placeholder so a
  version bump cannot invalidate them again, and the test count is corrected from "~1,400".
- The `unwrapUnion` dead-code verdict is corrected wherever it was recorded — see **Known issues**
  below for why the correction matters rather than being cosmetic.

### Known issues

Confirmed present in the released **2.0.0** and still open on `main`. Recorded here so that a
consumer reading release notes on upgrade learns of them without opening the backlog. Full detail
in [docs/BACKLOG.md](docs/BACKLOG.md); consumer-facing rows in [SECURITY.md](SECURITY.md).

- **`AvroReconstructor.reconstruct()` produces an unwritable datum at the shipped default
  configuration** (`recon/NP-023`, BL-012). A schema with a defaulted **enum** field absent from
  the input reconstructs with a `java.lang.String` in that position, and
  `GenericData.get().validate(schema, record)` returns `false`, so the record cannot be
  binary-encoded. No unusual configuration is required — the default is the broken setting.
  **Workaround: `useSchemaDefaults(false)`.** The same shape affects FIXED/BYTES (`byte[]`) and
  record-typed (`LinkedHashMap`) defaults.
- **`allowMissingFields` does not allow missing fields at either value** (`recon/NP-024`,
  BL-012). It selects which exception you get, not whether reconstruction succeeds.
- **An empty flattened map silently returns `{}`** (`recon/NP-025`, BL-012) even against a schema
  with a required no-default field, because the empty-map short-circuit consults neither knob.
- **Five `JsonFlattenerConfig` knobs are inert** (BL-015): `charset`, `bufferSize`, `failOnError`,
  `preserveNulls` and `sortKeys` are read nowhere in `src/main`. `failOnError(false)` in
  particular does **not** make parsing lenient. They are documented rather than repaired because
  making them live would silently change behaviour for released callers; they are now pinned as
  inert by a test so that wiring one up cannot happen unnoticed.
- **3+ branch unions inside Avro array elements are silently dropped** (BL-014). Corrected framing:
  this is a gap that has **always** been present, not a regression — the `unwrapUnion` method it
  was originally attributed to never had a declaration in any revision where it was called, so it
  never executed and no behaviour was lost when it was deleted.

---

## [2.0.0] - 2026-08-11

First tagged, reproducible release from this repository (`v2.0.0`, commit `fc1139e`).

### Added

- **`io.github.pierce.schema` — the enriched flattening API.** Ten new public types:
  `EnrichedSchemaFlattener`, `FlattenedField`, `FlattenOptions`, `TypeMapper`, `LeafInterceptor`,
  `NameCollisionPolicy`, `PathSegment`, and the typed exceptions `SchemaFlattenException`,
  `RecursiveSchemaException` and `SchemaLimitExceededException`. Flattening an Avro schema now
  yields structured `FlattenedField` records carrying the path segments, the Avro schema, the
  mapped type and nullability, rather than a bare name→type map, and `LeafInterceptor` lets a
  caller annotate each leaf as it is emitted.

- **The round-trip fidelity corpus, published as an enforced guarantee.** 156 fixtures under
  `src/test/resources/fidelity/`, each classified `LOSSLESS` (51), `ACCEPTED_LOSS` (23) or
  `DEFECT` (82) with a recorded round-trip and a written rationale. `manifest.json` is the
  contract; `docs/ROUND_TRIP_FIDELITY.md` is generated from it and drift fails the build. A
  `DEFECT` fixture asserts the defect is still present, so repairing one turns the build red by
  design.

- **`FlattenedPath`** — the single shared key-encoding used by both the flatten and reconstruct
  sides.

### Fixed

- **`quality/NP-001` — `FileFinder`'s safety options are now enforced.** `validatePaths`,
  `allowedExtensions` and `maxFileSize` were declared on the config and read by nothing. All
  three are now consulted, and `maxFileSize` throws on breach.

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
  the build still compiled against Groovy 4.0.21 — one `git add -A` away from entering permanent
  history. The Groovy toolchain has since been removed entirely (see **Removed** below); the
  directory is still ignored because nothing in the build has ever read it.
- Coverage floor `jacoco.minimum.coverage` raised `0.58` → `0.64` against a measured 65.46%
  instruction coverage. Ratchets only tighten.
- `README.md` now documents the API that exists. The previous documentation described four
  `NexusPiercerPatterns` methods that were never implemented; the class has two.

### Removed
- **The Groovy toolchain, entirely.** No build-visible change for consumers — `src/main` had been
  pure Java for some time and the published artifact never contained Groovy — but the build no
  longer has a second language in it.
  - `gmavenplus-plugin` deleted from `pom.xml`, along with both of its declarations (it had been
    declared in *both* `pluginManagement` and `build/plugins` with different execution ids, so
    Maven merged rather than overrode them and compiled every Groovy source twice per build).
  - The Groovy runtime (`org.apache.groovy:groovy`, `:groovy-json`) and **Spock**
    (`org.spockframework:spock-core`) dropped as dependencies. Spock is Groovy-native and pulls
    the Groovy runtime back onto the test classpath transitively, so leaving it declared would
    have kept the toolchain alive after the compiler plugin was gone. The repository contained no
    Spock specification.
  - The last 17 `.groovy` test sources (7,778 lines) ported to Java under `src/test/java`, with
    **no loss of test coverage, verified by building both sides**: the pre-port tree was rebuilt
    in a worktree at `fc1139e` and compared on Maven's summary line. Whole suite 2325 -> 2333,
    0 failures both sides, 15 skipped both sides; the 14 case-bearing ported classes contribute
    232 before and 232 after, matching one-for-one per class. The +8 is two new test classes for
    example classes that previously had none. Packages preserved — including the two legitimate same-simple-name collisions between
    `io.github.pierce.avro.AvroSchemaFlattenerTest` and
    `io.github.pierce.avroTesting.AvroSchemaFlattenerTest`. `AvroReconstructorTest` was in the
    default package and now lives in `io.github.pierce`.
  - Replaced the now-vacuous "Groovy compiles exactly once" CI assertion with a
    `no Groovy anywhere` gate (`scripts/assert-groovy-free.sh`, run from `ci.yml`): it fails on any
    tracked `.groovy` file, on any POM declaring `gmavenplus`/Groovy/Spock, and on any Groovy or
    Spock artifact reaching the resolved test classpath.
- `maven-publish.yml` from the repository root, where GitHub Actions never executed it.
  Superseded by `.github/workflows/release.yml`.

## [1.0.8] - 2025-12-09

Published to Maven Central. No corresponding git tag exists for this release; it is not traceable
to a specific commit. Earlier history is recorded only in commit messages.

[Unreleased]: https://github.com/pierce-lonergan/NexusPiercer/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/pierce-lonergan/NexusPiercer/compare/v1.0.8...v2.0.0
[1.0.8]: https://central.sonatype.com/artifact/io.github.pierce-lonergan/nexus-piercer/1.0.8
