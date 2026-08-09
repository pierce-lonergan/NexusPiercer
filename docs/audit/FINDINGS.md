# NexusPiercer Audit — Findings Register

Generated 2026-08-09 by a 13-agent audit (8 parallel specialists, adversarial verification of every
performance claim, then synthesis). **7 performance claims were refuted under verification and removed**;
several survivors had their magnitudes corrected downward. What follows is what survived.

## Measured baseline at audit time

| Metric | Value |
|---|---|
| `mvn compile` | 14s, green |
| `mvn test` | 1224 invocations, 0 failures, 0 errors, 3 skipped |
| Test wall time | 4:18 (`reuseForks=false` forks a JVM per class) |
| Instruction coverage | 60.26% (JaCoCo gate set at 20%) |
| Active `@Test` methods | 636 across 46 files |
| Commented-out tests | 115 across 7 files |
| `io.github.pierce.spark` coverage | 0.0% (2,995 instructions) |

## Summary

200 findings survived verification: **18 critical, 57 high, 98 medium, 27 low**.

| Cluster | Findings |
|---|---|
| `quality` | 42 |
| `infra` | 33 |
| `perf-flatten` | 24 |
| `hygiene` | 23 |
| `arch` | 22 |
| `perf-avro` | 21 |
| `perf-schema` | 20 |
| `tests` | 15 |

---


## CRITICAL

### `NP-001` — Two complete, disconnected flatten/reconstruct stacks ship in one package with zero cross-references

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:77`
- **Cluster:** arch | **Category:** duplication | **Effort:** large
- **Complexity:** 2 parallel implementations, ~5,000 LOC of overlapping traversal logic
  - → 1 core engine + 2 strategy interfaces

**Evidence.** `grep -rn "MapFlattener|JsonFlattener|AvroReconstructor|GAvroSchemaFlattener|JsonReconstructor" src/main/java/` returns ZERO hits. The reverse grep (`AvroSchemaFlattener|JsonFlattenerConsolidator|FileFinder|AvroSchemaLoader|CreateSparkStruct` over src/main/groovy/) returns exactly one hit, and it is a Javadoc comment: GAvroSchemaFlattener.groovy:27 `* AvroSchemaFlattener flattener = new AvroSchemaFlattener(config);` — a stale doc referencing the class it was copied from. Stack A = JsonFlattenerConsolidator (820) + AvroSchemaFlattener (1121) + spark/* (1454), the only stack the public Spark API touches, with NO data-reconstruction path. Stack B = MapFlattener (1299) + JsonFlattener (2004) + GAvroSchemaFlattener (867) + AvroReconstructor (2979), which owns the only working reconstruction and has NO Spark integration. Both live in package io.github.pierce.

**Impact.** Neither stack is canonical: A owns the shipped Spark surface, B owns round-trip fidelity, and they cannot interoperate. A consumer who flattens with NexusPiercerSparkPipeline has no supported way to reconstruct; a consumer who reconstructs with AvroReconstructor has no supported way to run on Spark. Every bug fix to flattening semantics must be applied twice or the two stacks silently diverge further. 8,442 lines of Groovy are dead weight in the artifact for any user of the Spark API.

**Fix.** Pick Stack B's MapFlattener/AvroReconstructor as the semantic core (it is the only pair with a verified inverse and the largest test mass: 15 files reference MapFlattener, 10 reference AvroReconstructor). Extract a single `FlattenEngine` with `flatten(Map)->Map` and `reconstruct(Map, Schema)->Record`. Reimplement JsonFlattenerConsolidator's two genuinely distinct behaviours — delimiter-joined array consolidation and array statistics — as pluggable `ArrayCollapsePolicy` and `StatisticsEmitter` strategies on that engine, then delete JsonFlattenerConsolidator and rewire the spark/ package to the engine. Target: one flatten path, one reconstruct path, ~4,000 LOC removed.

### `NP-002` — Flattened key encoding is not injective — no escaping of the separator makes reconstruction provably lossy

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:873`
- **Cluster:** arch | **Category:** missing-abstraction | **Effort:** medium
- **Complexity:** O(1) split, non-injective encoding
  - → O(depth) trie-guided decode, injective encoding

**Evidence.** AvroReconstructor.groovy:873 `String[] parts = key.split(Pattern.quote(separator));` where separator is `_` (line 205: `this.separator = useArrayBoundarySeparator ? "__" : "_";`). MapFlattener.groovy:333 builds keys with `return prefix + separator + key;` and its sanitizeKey (MapFlattener.groovy:1025-1057) performs NO escaping of `_` in field names — worse, under boundary-separator mode line 1052 does `strKey = strKey.replace("__", "_")`, actively destroying information in any field literally named `a__b`. Under strictKeyValidation (line 1047) `strKey.replaceAll("[^a-zA-Z0-9_]", "_")` maps `a.b` and `a-b` onto the same key as nested `{a:{b:...}}`.

**Impact.** Any Avro/JSON field name containing an underscore — `user_id`, `created_at`, `order_total`, i.e. the overwhelming majority of real snake_case schemas — is split into fragments during reconstruction and rebuilt into the wrong nesting, or collides outright with a genuinely nested path. `{user_id: 1}` and `{user: {id: 1}}` produce the identical flat key `user_id`. This is not an edge case; it silently corrupts data on the most common schema style in the domain the library targets. Every 'perfect reconstruction' claim in the AvroReconstructor Javadoc (lines 34-49) is false for such schemas.

**Fix.** Introduce a `FlattenedPath` value type that owns encoding and decoding, and never pass raw Strings between flatten and reconstruct. Encode segment boundaries unambiguously — either percent-escape the separator inside segments (`user_id` -> `user%5Fid`) or, better, carry the segment list structurally and only render a String at the Spark column-name boundary. Reconstruction must consult the schema trie to disambiguate rather than blind-splitting: the SchemaPathTrie at AvroReconstructor.groovy:587-634 already holds every legal path and could do longest-prefix matching instead of `String.split`. Add a property test that round-trips schemas whose field names contain the separator.

### `NP-003` — AvroSchemaFlattener's static cache silently returns empty analysis metadata on every cache hit

- **Location:** `src/main/java/io/github/pierce/AvroSchemaFlattener.java:111`
- **Cluster:** arch | **Category:** correctness | **Effort:** medium
- **Complexity:** static cache + 8 mutable instance accumulators
  - → pure function + cached immutable result

**Evidence.** Line 38: `private static final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();` — STATIC. Lines 111-114: `getFlattenedSchema(Schema)` returns `schemaCache.computeIfAbsent(cacheKey, k -> flattenSchema(schema))`. All the analysis metadata (`arrayFieldNames`, `terminalArrayFieldNames`, `nonTerminalArrayFieldNames`, `mapFieldPaths`, `fieldMetadataList`, `recordDefinitions`) is INSTANCE state populated only inside `flattenSchema` (lines 116-156). On a cache hit the mapping function never runs, so a freshly constructed instance is left with all those collections empty. NexusPiercerSparkPipeline.loadSchema() then reads exactly those getters (`schemaFlattener.getArrayFieldNames()` etc.) to build CachedSchema. Compounding it, the cache key is `schema.getFullName() + ":" + includeArrayStatistics + ":" + includeNonTerminalArrays` — no content hash, so schema v1 and v2 of `com.acme.Event` collide.

**Impact.** Reproducible: build one pipeline over a schema, then build a second pipeline over the same schema in the same JVM (normal in a long-lived driver, notebook, or test suite). The second pipeline gets a CachedSchema whose arrayFields/terminalArrayFields/mapFieldPaths sets are all empty, so all downstream terminal-vs-non-terminal array branching silently degrades — with no error, no warning, and wrong output. Separately, schema evolution produces the previous version's flattened schema. `NexusPiercerSparkPipeline.clearSchemaCache()` (line 947) clears only SCHEMA_CACHE, not this static, so the documented escape hatch does not fix it.

**Fix.** Stop mixing a shared cache with per-instance accumulator state. Make `flattenSchema` return an immutable `FlattenedSchemaResult { Schema schema; Set<String> arrayFields; Set<String> terminalArrays; ... }` and cache THAT value object, not the bare Schema. Key the cache on `SchemaNormalization.parsingFingerprint64(schema)` (AvroReconstructor.groovy:646 already does this correctly) rather than getFullName(). Make AvroSchemaFlattener stateless, or drop the static cache entirely and let callers cache the result value.

### `OSS-01` — Spark README and API_SURFACE document four NexusPiercerPatterns methods that do not exist

- **Location:** `src/main/java/io/github/pierce/spark/NexusPiercerPatterns.java:25`
- **Cluster:** hygiene | **Category:** documentation-accuracy | **Effort:** medium
- **Complexity:** N/A
  - → N/A

**Evidence.** NexusPiercerPatterns.java is 160 lines and declares exactly two public methods: `generateDataQualityReport` (line 46) and `profileJsonStructure` (line 93). Its own class Javadoc at line 25 shows `NexusPiercerPatterns.jsonToDelta(spark,`. `jsonToParquet`, `jsonToDelta`, `jsonToNormalizedTables`, and `processIncremental` appear nowhere in src/ — only in 'NexusPiercer Spark Pipeline README.md' (lines 110, 222, 249, 267, 641, 670), docs/API_SURFACE.md (lines 103, 113, 135), docs/CLASS_REGISTRY.md:73, and docs/PROJECT_OVERVIEW.md:103.

**Impact.** Two thirds of the documented 'Common Patterns' surface is fiction. Every user who copies the README's ETL Pipeline, Normalize-to-Multiple-Tables, or Incremental Processing example gets a compile error against the published 1.0.8 artifact. This is the single most damaging accuracy defect because the README presents these as the library's headline convenience API.

**Fix.** Either implement the four methods or delete them from the Spark README, docs/API_SURFACE.md, docs/CLASS_REGISTRY.md, docs/PROJECT_OVERVIEW.md, and the NexusPiercerPatterns class Javadoc. Add a docs test or a compiled examples module so README snippets cannot silently rot again.

### `OSS-02` — No canonical README.md — both READMEs have spaces in their filenames, so GitHub renders no landing page

- **Location:** `JsonFlattenerConsolidator README.md:1`
- **Cluster:** hygiene | **Category:** repo-hygiene | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** `git ls-files` shows only 'JsonFlattenerConsolidator README.md' and 'NexusPiercer Spark Pipeline README.md' at the root; no README.md exists. Neither file cross-links to the other, neither carries build/install instructions or Maven coordinates, and only the Spark one has a License section. docs/BACKLOG.md lists 'README.md' as an Affected File three times (lines 53, 63, 77) for a file that has never existed.

**Impact.** The GitHub repo page for a library published to Maven Central shows a bare file listing. Filenames with spaces also break raw-URL links, shell one-liners, and REUSE/tooling paths. New users have no entry point, no install snippet, and no way to discover which of the two documents applies to them.

**Fix.** Create a root README.md that states what the library is, the Maven coordinates (io.github.pierce-lonergan:nexus-piercer:1.0.8), a minimal working example, build instructions, and links to the two deep-dive docs. Rename the existing files to docs/json-flattener-consolidator.md and docs/spark-pipeline.md (git mv, preserving history) and update the docs/BACKLOG.md references.

### `CI-001` — No CI runs at all — maven-publish.yml is stranded in the repo root with no .github/ directory

- **Location:** `maven-publish.yml:1`
- **Cluster:** infra | **Category:** ci-cd | **Effort:** small

**Evidence.** `ls -d .github` → `No such file or directory`. The workflow file sits at repo root as `./maven-publish.yml`. GitHub Actions only reads `.github/workflows/*.yml`, so this file is inert. The POM nonetheless advertises `<ciManagement><system>GitHub Actions</system><url>https://github.com/pierce-lonergan/NexusPiercer/actions</url></ciManagement>` (79-82). There is also no `.github/dependabot.yml` and no `renovate.json` anywhere (`find . -name dependabot.yml -o -name renovate.json` → empty).

**Impact.** Zero automated verification on any commit or PR: no compile check, no test run, no coverage, no publish. Every finding in this report is invisible to the project because nothing ever executes outside the maintainer's Windows workstation. Compounds with Q-001 — the gates are off AND nothing would catch it.

**Fix.** `mkdir -p .github/workflows && git mv maven-publish.yml .github/workflows/`. Add a separate `.github/workflows/ci.yml` running `mvn -B verify` on push/PR across a JDK 17 + 21 matrix. Add `.github/dependabot.yml` with the `maven` and `github-actions` ecosystems on a weekly schedule.

### `ENF-001` — requireOS collapses two <family> elements to family=windows — enforcer will fail the build on Linux CI

- **Location:** `pom.xml:860`
- **Cluster:** infra | **Category:** enforcer | **Effort:** trivial

**Evidence.** The rule block is:
```xml
<requireOS>
    <family>unix</family>
    <family>windows</family>
</requireOS>
```
`RequireOS.family` is a single `String` field, not a collection, so Plexus last-wins. Confirmed empirically with `mvn -o -B enforcer:enforce@enforce-maven -X`:
`[DEBUG] Executing Rule 2: EnforcerRuleDesc[name=requireOS, rule=RequireOS[message=null, arch=null, family=windows, name=null, version=null, display=false], level=ERROR]`
followed by `[INFO] Rule 2: org.apache.maven.enforcer.rules.RequireOS passed` on this Windows host. The execution also sets `<fail>true</fail>` (872).

**Impact.** The rule reads as 'unix OR windows' but is actually 'windows only'. The moment maven-publish.yml is moved into `.github/workflows/` (CI-001), the very first `mvn` invocation on `runs-on: ubuntu-latest` dies at the `validate` phase with 'OS Arch: ... Family: unix ... is not allowed by Family=windows'. This is a latent landmine that will make the CI fix appear to be the cause of the breakage.

**Fix.** Delete the `<requireOS>` rule — it adds no value for a pure-Java library. If OS gating is genuinely wanted, use a single `<family>` value or the `requireEnvironmentVariable`/profile-activation mechanism instead.

### `Q-001` — Checkstyle/PMD/SpotBugs are unreachable in every build path — proven never downloaded

- **Location:** `pom.xml:1306`
- **Cluster:** infra | **Category:** quality-gates | **Effort:** medium
- **Complexity:** 0 gates executed
  - → 3 gates executed at verify

**Evidence.** The only declarations of maven-checkstyle-plugin, maven-pmd-plugin and spotbugs-maven-plugin live inside `<profile><id>quality</id>` (lines 1336-1425). The default build has none. Meanwhile `<profile><id>development</id><activation><activeByDefault>true</activeByDefault></activation><properties><gpg.skip>true</gpg.skip><checkstyle.skip>true</checkstyle.skip><pmd.skip>true</pmd.skip><spotbugs.skip>true</spotbugs.skip></properties>` (1306-1317). The `release` profile (1447-1479) flips those skips back to `false` but adds only maven-gpg-plugin — it never adds the analysis plugins, so `mvn -Prelease deploy` runs zero static analysis. Empirical proof they have NEVER run: `ls ~/.m2/repository/org/apache/maven/plugins/maven-checkstyle-plugin` and `.../maven-pmd-plugin` and `~/.m2/repository/com/github/spotbugs/spotbugs-maven-plugin` all return nothing, and `mvn -o checkstyle:check -Pquality` fails with `No plugin found for prefix 'checkstyle'`.

**Impact.** There is no lint, no bug-pattern analysis and no security analysis (findsecbugs is configured but never loads) on any build, any developer machine, or any release. A declared-but-toothless gate is worse than none: the POM advertises five quality plugins and the README/reporting section implies enforcement, so reviewers assume coverage that does not exist.

**Fix.** Move checkstyle/pmd/spotbugs out of the `quality` profile into `<build><plugins>` with explicit `<phase>verify</phase>` bindings. Delete the `development` activeByDefault profile entirely (an activeByDefault profile that disables gates is an anti-pattern) and keep a single opt-out `-Dcheckstyle.skip=true` for local loops. Verify with `mvn -o verify` that all three plugins appear in the reactor log.

### `Q-002` — `quality` profile references three config files that do not exist — profile cannot execute even when activated

- **Location:** `pom.xml:1349`
- **Cluster:** infra | **Category:** quality-gates | **Effort:** trivial

**Evidence.** SpotBugs config: `<includeFilterFile>${project.basedir}/src/main/spotbugs/spotbugs-include.xml</includeFilterFile>` and `<excludeFilterFile>${project.basedir}/src/main/spotbugs/spotbugs-exclude.xml</excludeFilterFile>` (1349-1350). Checkstyle config: `<suppressionsLocation>${project.basedir}/src/main/checkstyle/checkstyle-suppressions.xml</suppressionsLocation>` (1408). On disk: `src/main/spotbugs/` does not exist at all (`ls: cannot access 'src/main/spotbugs/': No such file or directory`), and `src/main/checkstyle/` contains only `checkstyle.xml` (471 bytes). A repo-wide `find . -name 'spotbugs-*.xml' -not -path './target/*'` and `find . -name 'checkstyle-suppressions.xml'` both return zero results.

**Impact.** Anyone who discovers the gates are off and runs `mvn -Pquality verify` to fix it gets an immediate MojoExecutionException from SpotBugs ('Unable to find filter file') and Checkstyle ('Cannot find suppressions file'), not a violation report. The natural remediation path is blocked, which is why the gates have stayed off.

**Fix.** Either create the three files (a minimal `<FindBugsFilter/>` for each SpotBugs filter and `<suppressions/>` for Checkstyle) or delete the `includeFilterFile`/`excludeFilterFile`/`suppressionsLocation` elements. Prefer creating them so real suppressions have a home.

### `REL-001` — Release workflow produces unsigned artifacts — `mvn deploy` without -Prelease leaves gpg.skip=true

- **Location:** `maven-publish.yml:36`
- **Cluster:** infra | **Category:** release | **Effort:** small

**Evidence.** The workflow's publish step is `run: mvn -B deploy --file pom.xml -Dgpg.passphrase=${{ secrets.MAVEN_GPG_PASSPHRASE }}` — note the absence of `-P release`. maven-gpg-plugin is declared ONLY inside `<profile><id>release</id>` (1459-1476). Without `-Prelease`, the `development` profile stays activeByDefault and pins `<gpg.skip>true</gpg.skip>` (1312). Setting `-Dgpg.passphrase=...` defines a property but does not activate a profile. Additionally the sign execution configures `<keyname>${gpg.keyname}</keyname><passphraseServerId>${gpg.keyname}</passphraseServerId>` (1471-1472) and `gpg.keyname` is never defined anywhere in the POM — a grep for `<gpg.keyname>` returns nothing, so it would resolve to the literal string `${gpg.keyname}` and be passed to gpg as `--local-user ${gpg.keyname}`.

**Impact.** Maven Central rejects bundles without `.asc` signatures for every artifact. The workflow imports a GPG key via `crazy-max/ghaction-import-gpg@v6` and then never uses it — the release silently produces an unsignable bundle and fails at the Portal, or worse, deploys unsigned to a repo that accepts it. Even if `-Prelease` were added, the undefined `gpg.keyname` would break signing.

**Fix.** Change the run line to `mvn -B -P release deploy`. Define `<gpg.keyname>` as a property (or drop `<keyname>`/`<passphraseServerId>` and rely on the imported default key). With maven-gpg-plugin 3.1.0, `-Dgpg.passphrase` is deprecated — add `<gpgArguments><arg>--pinentry-mode</arg><arg>loopback</arg></gpgArguments>` or set `GPG_TTY`/use gpg-agent as the import action configures.

### `RECON-02` — SchemaNormalization.parsingFingerprint64() recomputed on every single record — the cache key costs more than the cached value

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:647`
- **Cluster:** perf-avro | **Category:** redundant-schema-work | **Effort:** trivial
- **Complexity:** O(S) canonical-form string construction + Rabin hash per record, i.e. O(N·S) for N records
  - → O(1) identity/hashCode map lookup per record, O(S) once per schema

**Evidence.** reconstructToMap line 234 calls `getOrBuildSchemaCacheEntry(schema)` per record, which calls getSchemaFingerprint (line 645):
```java
long fingerprint = SchemaNormalization.parsingFingerprint64(schema);
String fullName = schema.getFullName();
...
return fullName + "@" + Long.toHexString(fingerprint);
```
parsingFingerprint64 internally does `fingerprint64(toParsingForm(s).getBytes(UTF8))` — it builds the ENTIRE canonical-form JSON string of the schema (StringBuilder over every field, plus a name-resolution Map) on every invocation. Then two more String allocations for the key, then a ConcurrentHashMap lookup.

**Impact.** For a 100-field schema the canonical form is several KB. At 10,000 records/partition that is 10,000 full schema serializations plus ~30,000 String allocations, purely to look up a cache entry that never changes. This is the classic 'schema lookup per record' miss and it is pure waste — the schema object is identical across the whole batch.

**Fix.** Key the cache on the Schema instance, not on a derived string: replace `ConcurrentHashMap<String, SchemaCacheEntry>` (line 114) with `Map<Schema, SchemaCacheEntry>` — org.apache.avro.Schema caches its own hashCode, so lookup is O(1) with no allocation. Better still, hoist the whole thing: expose a `prepare(Schema)` that returns a reusable per-schema plan and have the Spark caller build it once per partition rather than per row.

### `RECON-03` — key.split(Pattern.quote(separator)) forces a fresh Pattern.compile for every flat key of every record

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:872`
- **Cluster:** perf-avro | **Category:** regex-recompilation | **Effort:** trivial
- **Complexity:** O(n) Pattern compilations per record is right. The fix should be a hand-rolled indexOf-based splitter (correct for both '_' and '__'), not `split(separator)`; O(n*|key|) char scan, zero regex.
  - → O(n·|key|) char scan per record with zero regex; O(n) total if split results are memoized per schema

**Evidence.** buildPathTree, inside the per-entry loop:
```java
String[] parts = key.split(Pattern.quote(separator));
```
`Pattern.quote("_")` allocates the String `"\\Q_\\E"` (5 chars) on every call. String.split's fastpath only applies when the regex is 1 char (non-metachar) or 2 chars starting with a backslash — a 5-char regex fails both tests, so String.split falls through to `Pattern.compile(regex).split(this, limit)`. Bytecode confirms both calls are also dynamic: `107: invokedynamic #76:invoke:(Class;String)Object` (Pattern.quote) then `112: invokedynamic #77:invoke:(String;Object)Object` (split). The same idiom appears at line 608 in SchemaPathTrie.add.

**Impact.** One full java.util.regex.Pattern compilation (node tree construction) plus a quote-String allocation per flat key per record. At 100 fields x 10,000 records that is 1,000,000 Pattern compiles — likely dominating buildPathTree entirely. Ironically `key.split("_")` with the raw separator WOULD hit the fastpath; Pattern.quote is actively defeating it.

**Fix.** Hoist a `private static final Pattern SEP_PATTERN` compiled once in the constructor from Pattern.quote(separator), and call `SEP_PATTERN.split(key)`. Even better: since the separator is a literal 1-2 char string, write a manual indexOf-based splitter that returns a reusable String[] and skips regex entirely. Combine with RECON-04 by memoizing the split result per distinct flat key in the per-schema cache entry — key sets are identical across records in a batch, so the split is computed once per schema instead of once per record.

### `JFLAT-04` — MapFlattener allocates a LinkedHashMap per tree node and putAll()s it up the recursion — a leaf at depth d is copied d times (measured 4.0x)

- **Location:** `src/main/groovy/io/github/pierce/MapFlattener.groovy:312`
- **Cluster:** perf-flatten | **Category:** algorithmic-complexity | **Effort:** medium
- **Complexity:** O(n·d) hash insertions is correct and, if anything, understates by ~2x. There are TWO throwaway maps per recursion level, not one: `flattenValue` line 337 allocates a LinkedHashMap and line 348 `putAll`s `flattenObject`'s result into it, then `flattenObject` line 312 `putAll`s that into ITS own map from line 272. So a leaf at depth d is re-inserted ~2d times and ~2 maps are allocated per interior node. And in real Groovy every one of those `new LinkedHashMap`/`put`/`putAll` operations is an `invokedynamic` (verified in the flattenValue disassembly: offset 2 is `invokedynamic #5:init` for the LinkedHashMap, offset 29 is `invokedynamic #30:invoke` for the put), which also kills any hope of escape analysis eliminating them.
  - → O(n) hash insertions and 1 map allocation; measured 31.0 us for the same tree (4.0x)

**Evidence.** Three lines define the pattern. `flattenObject` line 272: `Map<String, Object> result = new LinkedHashMap<>();`. `flattenValue` line 337: `Map<String, Object> result = new LinkedHashMap<>();` — allocated even to hold a SINGLE leaf value (line 340 `result.put(key, null);`, line 386 `result.put(key, normalizePrimitive(value));`). Then `flattenObject` line 312: `result.putAll(flattenValue(newKey, entry.getValue(), depth));` copies the child map into the parent, and `flattenValue` line 348: `result.putAll(flattenObject(mapValue, key, depth + 1));` does it again one level up. So every leaf entry is re-hashed and re-inserted once per ancestor level. I modelled exactly this cascade in plain Java against a single-accumulator + threaded-StringBuilder version, depth 6, 972 leaves: cascade 124.3 us, accumulator 31.0 us — 4.0x, in Java, before any Groovy dispatch cost.

**Impact.** For a depth-6 record with 972 leaves this is ~5,800 hash insertions instead of 972, plus ~1,400 throwaway LinkedHashMap objects (each ~48 bytes header + an 80-byte 16-slot table = ~128 bytes) = ~180 KB of garbage per record. At 1,000 records that is ~180 MB of short-lived allocation and ~5M redundant hash insertions, all of it in dynamic-Groovy callsites (see JFLAT-03). This is the single largest cost in MapFlattener and it scales with depth, so it hits precisely the deeply-nested workload the library targets.

**Fix.** Thread one accumulator through the recursion: change `flattenObject`/`flattenValue`/`flattenList`/`flattenArray` to `void flattenObject(Map<?,?> obj, StringBuilder prefix, int depth, Map<String,Object> out)` and have leaves `out.put(...)` directly. Allocate the single output LinkedHashMap once in `flatten()` with an estimated initial capacity. This subsumes JFLAT-10 (the same refactor removes the per-level key concatenation).

### `NP-001` — Path traversal: FileFinder's declared security controls are never enforced

- **Location:** `src/main/java/io/github/pierce/files/FileFinder.java:527`
- **Cluster:** quality | **Category:** security | **Effort:** medium
- **Complexity:** N/A
  - → N/A

**Evidence.** Config declares `private Set<String> allowedExtensions`, `private long maxFileSize = 100MB`, `private boolean validatePaths = true` (lines 145-158). `grep -rn 'validatePaths|allowedExtensions|maxFileSize' src/main` returns ONLY those three declaration lines — they are read nowhere. findFileHandle Strategy 1 is: `if (fileName.contains("/") || fileName.contains("\\")) { Path directPath = Paths.get(fileName); if (Files.exists(directPath)) return createLocalFileHandle(directPath); }` with no normalization or root check. The configured search paths additionally include "..", "../..", "../../.." (lines 139-141).

**Impact.** Any caller-influenced file name yields arbitrary local file read. NexusPiercerSparkPipeline.loadSchema() passes `config.schemaPath` straight into `FileFinder.Util.readAsString`, and AvroSchemaLoader.loadAvroSchema passes the schema name; if either comes from job config, a request parameter, or a manifest, `../../../../etc/shadow` or `C:/Users/x/.aws/credentials` is read and its contents are surfaced (or embedded in a Schema parse error message). maxFileSize being unenforced also means `getFileContent` will `readAllBytes()` a 100 GB file into a Guava cache and OOM the driver.

**Fix.** Resolve and normalize every candidate against an allow-listed set of base directories: `Path base = Paths.get(root).toRealPath(); Path target = base.resolve(fileName).normalize(); if (!target.startsWith(base)) throw new SecurityException(...)`. Reject names containing `..` or absolute prefixes up front. Enforce `allowedExtensions` in findFileHandle and `maxFileSize` in createLocalFileHandle/openInputStream before any read. Remove ".."/"../.."/"../../.." from the default search paths.

### `NP-002` — Unbounded recursion on recursive Avro schemas → StackOverflowError

- **Location:** `src/main/java/io/github/pierce/AvroSchemaFlattener.java:190`
- **Cluster:** quality | **Category:** correctness | **Effort:** medium
- **Complexity:** unbounded / O(∞) on cyclic input
  - → O(fields × maxDepth)

**Evidence.** `processFieldRecursively(..., int depth, ...)` takes a depth parameter but the only use is `schemaStats.maxNestingDepth = Math.max(..., depth)` — there is no `if (depth > MAX)` anywhere in the file (grep for `depth >`/`MAX_DEPTH`/`visited` in AvroSchemaFlattener.java returns nothing). `collectRecordDefinitions(Schema, String)` (line 159) likewise recurses into RECORD and ARRAY-of-RECORD element types with no visited set.

**Impact.** Avro explicitly supports self-referential named types. Feeding `{"type":"record","name":"Node","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","Node"]}]}` to getFlattenedSchema() recurses forever: collectRecordDefinitions blows the stack first, and if it survived, processFieldRecursively would generate infinitely long field names (`next_next_next_...`). In a Spark driver this is an unrecoverable StackOverflowError from a single malformed/hostile schema file — a trivially reachable DoS for a library whose stated job is loading schemas from disk/HDFS/S3.

**Fix.** Thread a `maxDepth` (default ~64) through both methods and throw a typed SchemaTooDeepException when exceeded, and carry an `IdentityHashMap<Schema,Boolean>`/`Set<String> visitedFullNames` down the recursion so a named type already on the current path is emitted as a terminal STRING field instead of being re-expanded.

### `NP-003` — flattenAvroSchema loops forever and exhausts heap on recursive schemas (depth tracked, never checked)

- **Location:** `src/main/java/io/github/pierce/converter/SchemaBasedMapConverter.java:1100`
- **Cluster:** quality | **Category:** security | **Effort:** small
- **Complexity:** unbounded / O(∞) on cyclic input
  - → O(fields × maxDepth)

**Evidence.** The iterative walk pushes `new AvroSchemaNode(field.schema(), fieldPath, node.depth + 1, ...)` (line 1124) but the `while (!stack.isEmpty())` loop never inspects `node.depth` — grep for `depth >` in this file returns no matches. The array-of-record path calls the genuinely recursive `flattenAvroSchemaForArrayElement` (line 1165), which recurses into RECORD fields with no depth or cycle guard either.

**Impact.** `SchemaBasedMapConverter.forFlattenedAvro(recursiveSchema)` on `record Node { array<Node> children }` recurses until StackOverflowError; on `record Node { Node next }` the iterative loop pushes a new node every iteration with an ever-growing `fieldPath` String, so the process hangs while allocating quadratically-growing key strings until OutOfMemoryError — worse than a crash because it is a silent, unkillable heap death rather than a fast failure.

**Fix.** Check `if (node.depth > maxDepth) { emit terminal STRING field; continue; }` at the top of the while loop, pass the same limit into flattenAvroSchemaForArrayElement, and track visited `Schema.getFullName()` values along the current path to break cycles.

### `TA-001` — JsonReconstructor main class is 100% commented out — 45 tests dead, JSON flatten→reconstruct round-trip invariant untested (PROVEN 11-line fix restores 45/45 green)

- **Location:** `src/main/groovy/io/github/pierce/JsonReconstructor.groovy:1`
- **Cluster:** tests | **Category:** dead-code-and-missing-invariant | **Effort:** small
- **Complexity:** 0 of 45 JSON-reconstruction tests executing; 0% coverage of 1294 lines
  - → 45/45 passing (verified); JSON round-trip identity property under test

**Evidence.** All 1294 non-blank lines of JsonReconstructor.groovy begin with `//` (100.0% commented; measured). File head: `//package io.github.pierce;`. JaCoCo reports `default.JsonReconstructor` at 0.0% (15 instr — the empty Groovy Script shell). src/test/groovy/JsonReconstructorTest.groovy is likewise 100% commented (806 lines, 45 @Test). Both were ADDED already commented in commit ad422d3 (2025-12-02 'added type conversion logic') — they were never live. docs/CONCERNS.md C-001 and docs/BACKLOG.md BL-007 flag this as an unresolved 'Medium' chore. I EMPIRICALLY VERIFIED revivability: uncommented both files into a scratch dir, compiled with groovyc 4.0.21 (clean, 29 classes), ran under JUnit Platform 1.13.1 → 45 found / 22 passed / 23 failed, ALL 23 failures sharing one root cause: `groovy.lang.MissingPropertyException: No such property: JSON for class: io.github.pierce.JsonReconstructor` (Java-style bare enum labels `case JSON:` at lines 517,519,521,523,559,562,568,574 — illegal idiom in a Groovy switch). After qualifying those 8 labels → 43/45 pass; remaining 2 failed with `MissingMethodException: splitRespectingBrackets() ... argument types: (String, String)` (Groovy passes `','` as String, not char, at lines 564, 570, 595). After `',' as char` → **45/45 PASS, 0 failures**.

**Impact.** The library ships a documented public reconstruction API (referenced in docs/API_SURFACE.md, ARCHITECTURE_GRAPH.md, CLASS_REGISTRY.md, MODULE_INDEX.md, PROJECT_OVERVIEW.md) that does not exist at runtime. Consequently THE critical invariant for a flattener library — flatten(x) then reconstruct = x for schema-less JSON — has zero test coverage. JsonReconstructorTest contains a dedicated `@Nested class RoundTripTests` (`shouldRoundTripSimpleNestedObject`, `shouldRoundTripDeeplyNestedObject`) using MapFlattener→JsonReconstructor→verify().isPerfect() that is currently inert. Avro round-trip IS covered (46 tests); JSON round-trip is entirely uncovered. JsonFlattenerConsolidator (2256 instr, 90.1% line coverage, the flagship Java class) has no inverse operation tested at all.

**Fix.** Uncomment src/main/groovy/io/github/pierce/JsonReconstructor.groovy and src/test/groovy/JsonReconstructorTest.groovy, then apply exactly 11 line edits: (1) lines 517/519/521/523/559/562/568/574 — `case JSON:` → `case ArraySerializationFormat.JSON:` (and BRACKET_LIST, COMMA_SEPARATED, PIPE_SEPARATED likewise); (2) lines 564/570/595 — `splitRespectingBrackets(strValue, ',')` → `splitRespectingBrackets(strValue, ',' as char)` (and `'|'`). Move the test file to src/test/groovy/io/github/pierce/ to match its `package io.github.pierce` declaration. Close docs/BACKLOG.md BL-007 and docs/CONCERNS.md C-001 as resolved-by-completion, not by deletion.

### `TA-002` — Entire io.github.pierce.spark package has 0.0% coverage — 2995 instructions of shipped public API, all 3 test classes fully commented out and 2 of them are pure comment-outs of previously-passing code

- **Location:** `src/test/java/io/github/pierce/spark/NexusPiercerSparkPipelineTest.java:1`
- **Cluster:** tests | **Category:** coverage-gap | **Effort:** medium
- **Complexity:** 0/2995 spark instructions covered; 33 tests inert
  - → 25-33 tests executing over the Spark surface

**Evidence.** JaCoCo (target/site/jacoco/jacoco.csv, generated 2026-08-09): NexusPiercerSparkPipeline 0/1986 instr, NexusPiercerFunctions 0/702, NexusPiercerPatterns 0/307 — 2995 instructions, 0.0% covered. All three test classes are 100% comment-prefixed: NexusPiercerSparkPipelineTest.java (514 lines, 11 @Test), NexusPiercerFunctionsTest.java (237 lines, 12 @Test), NexusPiercerPatternsTest.java (187 lines, 2 @Test) — 25 tests total, plus NexusPiercerExamplesTest.java (1833 lines, 8 @Test) exercising the same package. `git show c7b3dde -- .../NexusPiercerSparkPipelineTest.java` shows the diff is a PURE comment-out: every line went from `x` to `//x`, zero content change. The only main-source change in that same commit was `private final Set<String> arrayFields` → `ThreadLocal<Set<String>>` in JsonFlattenerConsolidator — API-compatible. Same for NexusPiercerPatternsTest. I compiled all four uncommented files with JDK 17 javac against the full Maven test classpath: **all four compile with zero errors**. Every API they reference still exists (verified: flattenJson/flattenJsonWithStatistics/extractArray/arrayCount/arrayDistinctCount/explodeJsonArray/isValid/jsonError/extractField in NexusPiercerFunctions.java lines 240-264; forBatch/withSchema/enableArrayStatistics/processDataset/explodeArrays/includeMetadata/includeRawJson/validateConfiguration/getSchemaCacheSize/clearSchemaCache in NexusPiercerSparkPipeline.java lines 234-956; generateDataQualityReport line 46 and profileJsonStructure line 93 in NexusPiercerPatterns.java). The Delta Lake dependency that NexusPiercerPatternsTest's header comment asks for ('ensure you have the io.delta:delta-spark dependency in your pom.xml with a test scope') IS now present at pom.xml:683-688.

**Impact.** The Spark integration layer — the reason the artifact exists ('ready for Apache Spark') — is published to Maven Central with literally zero executed tests. A regression in schema caching, error-handling/quarantine, array explosion, metrics, or any of the 10 registered UDFs would ship undetected. This is also the single largest block of the 36 zero-coverage classes that are eating the JaCoCo CLASS MISSEDCOUNT budget (see TA-011).

**Fix.** Classify: NexusPiercerSparkPipelineTest.java = revivable-as-is (compiles clean, pure comment-out, no API drift). NexusPiercerPatternsTest.java = revivable-as-is (compiles clean, Delta dep now present). NexusPiercerFunctionsTest.java = revivable-with-fixes (compiles clean, but replace the `@BeforeEach getOrCreate()` / `@AfterEach spark.stop()` lifecycle at lines 44-72 with a shared `@BeforeAll`/`@AfterAll` or SparkTestBase — stopping the JVM-global SparkContext between tests is the classic cause of 'SparkContext has been shutdown'; and the truncated `SqlRegistrationTest` nested class at lines 220-237 ends in `//}}` with its test bodies deleted — either restore or remove it). Uncomment all three, run under `mvn test`. Note: I could not execute them here — this sandbox blocks loopback sockets, so every Spark local[*] session dies at `java.net.SocketException: Invalid argument: connect` → `Unable to establish loopback connection` → netty 'failed to create a child event loop' (0 of 11 tests even started). That is an environment artifact, and plausibly the same class of Windows/JDK-17 failure that motivated the original comment-out — so the revival must be validated on the target CI JDK before being declared done.


## HIGH

### `NP-004` — The Java/Groovy split is entirely accidental — zero Groovy language features are used anywhere

- **Location:** `src/main/groovy/io/github/pierce/MapFlattener.groovy:102`
- **Cluster:** arch | **Category:** language-split | **Effort:** medium
- **Complexity:** 2 compilers, stub gen/remove, dynamic dispatch on hot path
  - → 1 compiler, static dispatch

**Evidence.** Measured across all four live .groovy files (7,149 lines): 0 `def` declarations, 0 GString interpolations, 0 `.each{}/.collect{}/.find{}` closures, 0 `@CompileStatic`/`@TypeChecked`, and 1,945 lines terminating in a Java semicolon. Every class declares Java-style `public class X implements Serializable` with explicit types and Java generics. The ONLY non-Java syntax in the entire tree is 47 qualified enum case labels in GAvroSchemaFlattener.groovy (`case Schema.Type.RECORD:`) plus reliance on Groovy's implicit `java.math.BigDecimal` import in GAvroSchemaFlattener and MapFlattener. GAvroSchemaFlattener.groovy:27 even documents itself with the Java class's name, revealing it as a copy that was moved to .groovy rather than fixed.

**Impact.** Without @CompileStatic, groovyc compiles every call site to dynamic dispatch through the Groovy runtime — on MapFlattener.flattenObject and AvroReconstructor.reconstructRecord, which execute per record inside Spark executors, this is a 2-10x throughput penalty for zero language benefit. It also costs: a second compiler in the build, gmavenplus stub generate/remove cycles (pom.xml:963-1040) for a Java->Groovy dependency that does not exist, no javac static type checking on 7,149 lines, no IDE refactoring parity, groovy + groovy-json as runtime dependencies of a published library, and a separate groovydoc execution (pom.xml:735).

**Fix.** Rename the four live files to .java. The complete mechanical work is: unqualify 47 enum case labels in GAvroSchemaFlattener (`case Schema.Type.RECORD:` -> `case RECORD:`), add `import java.math.BigDecimal;` to GAvroSchemaFlattener and MapFlattener, and add the handful of other java.util imports Groovy was supplying implicitly. Then delete the gmavenplus plugin block, the groovydoc execution, the build-helper add-groovy-source executions, and the groovy/groovy-json compile dependencies from pom.xml. Since no Java file references any Groovy class, the migration cannot break a cross-language boundary — there isn't one.

### `NP-005` — io.github.pierce.converter is an orphan island: 31 classes, 6,130 LOC, unreachable from all main code

- **Location:** `src/main/java/io/github/pierce/converter/SchemaBasedMapConverter.java:1`
- **Cluster:** arch | **Category:** dead-code | **Effort:** small

**Evidence.** `grep -rn "io.github.pierce.converter" src/main --include=*.java --include=*.groovy` excluding the package itself returns ZERO hits. Grepping for the concrete types (SchemaBasedMapConverter, TypeConverterRegistry, AvroSchemaConverter, IcebergSchemaConverter, ConversionConfig) outside the package returns zero. Only its own 9 test files reference it. It totals 6,130 of the 19,860 LOC in src/main (30.9%). It also introduces `io.github.pierce.converter.GenericRecord` (129 lines, Iceberg-backed), which name-collides with `org.apache.avro.generic.GenericRecord` used throughout AvroReconstructor.

**Impact.** 31% of the source tree is maintained, compiled, javadoc'd, coverage-measured and shipped for nothing. It drags iceberg-core, iceberg-api and iceberg-common into compile scope (pom.xml:568-585) for every consumer of the library, including Spark users who will never touch Iceberg. The GenericRecord name collision guarantees import confusion the moment anyone does wire the two together.

**Fix.** Decide its fate explicitly. If Iceberg output is a real roadmap item, promote it: give it a facade, make AvroReconstructor delegate its logical-type conversion to TypeConverterRegistry (see NP-006), and rename converter.GenericRecord to IcebergRecord. If not, delete the package and the three iceberg dependencies — that is a 6,130-line, 3-dependency reduction with zero behavioural risk since nothing calls it.

### `NP-006` — AvroReconstructor is a 2,979-line god class spanning seven distinct responsibilities

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:77`
- **Cluster:** arch | **Category:** god-class | **Effort:** large
- **Complexity:** 1 class, 2,979 LOC, 7 responsibilities, max method 219 LOC
  - → 7 classes, ~400 LOC each, max method ~60 LOC

**Evidence.** Seven separable concerns in one file, each with its own cluster of private methods: (1) verification/diffing — ReconstructionVerification, compareStructures, compareMaps, compareLists, compatibleTypes, valuesEqual, compareNumbers, ComparisonResult (lines 282-586); (2) schema path analysis and caching — SchemaPathTrie, SchemaCacheEntry, getSchemaFingerprint, buildSchemaPathTrie (587-728); (3) flat-key parsing into a node tree — PathNode, buildPathTree, isArrayFieldPattern, splitRespectingBrackets (729-910); (4) structural reconstruction — reconstructRecord through reconstructUnionValue (911-2132); (5) primitive and logical-type conversion — convertPrimitive, convertLogicalType, convertDecimal, convertUuid, convertTimestampMillis/Micros, convertDate, convertTimeMillis/Micros (2133-2475); (6) array string deserialization — deserializeArray, deserializeBracketList, splitBracketAware, unquoteString, parseNestedArrayStructure (2476-2625); (7) GenericRecord<->Map marshalling — genericRecordToMap, convertList, convertMap, mapToGenericRecord (2860-2964). Single methods reach 219 lines (reconstructNestedRecordFromArray, 1014-1232), 173 lines (convertPrimitive, 2133-2305) and 165 lines (reconstructUnionValue, 1915-2079). Responsibility (5) reimplements from scratch what the orphaned converter package already provides as DecimalConverter, UUIDConverter, TimestampConverter, DateConverter and TimeConverter.

**Impact.** No part of this can be tested, reused or replaced independently. The verification code (responsibility 1) is a test/QA concern compiled into every production deployment. The logical-type conversion is a third parallel implementation of type coercion in the codebase (alongside GAvroSchemaFlattener.convertPrimitive and the converter package). A 219-line method with nested array-of-record index arithmetic is where reconstruction bugs live and where they cannot be isolated.

**Fix.** Decompose along the seven seams, which are already clean: `ReconstructionVerifier` (move to test scope or a separate optional artifact), `SchemaPathIndex` (the trie + fingerprint cache), `FlatKeyParser` (produces the PathNode tree), `RecordReconstructor` (the structural core), `ValueCoercer` (delegate to converter.TypeConverterRegistry rather than reimplementing), `ArrayCodec` (paired with MapFlattener's serializer — see NP-011), and `RecordMarshaller`. Target ~400 lines each. The trie already carries the information needed to make the structural core schema-driven rather than heuristic.

### `NP-007` — FileFinder leaks a non-daemon scheduled executor for the JVM lifetime and deep-searches parent directories of the CWD

- **Location:** `src/main/java/io/github/pierce/files/FileFinder.java:332`
- **Cluster:** arch | **Category:** resource-leak | **Effort:** medium

**Evidence.** Line 332: `this.scheduledExecutor = Executors.newScheduledThreadPool(2);` — default thread factory, so NON-daemon threads (contrast line 326-330 where executorService explicitly sets daemon). Line 366: `scheduledExecutor.scheduleAtFixedRate(this::performMaintenance, 5, 5, TimeUnit.MINUTES);`. `grep -n "shutdown" FileFinder.java` returns nothing — neither executor is ever shut down, and there is no close()/AutoCloseable. The singleton (line 68 `private static volatile FileFinder INSTANCE`) is created lazily on the first static findFile() call and never released. Separately, Config.searchPaths (lines 95-142) includes `".."`, `"../.."` and `"../../.."`, and performDeepSearch (line 662-676) walks from `Paths.get(".").toAbsolutePath()` to depth 5. Line 831 `Files.walk(path, 2)` is used without try-with-resources, leaking a file-handle-backed Stream on every discovery pass.

**Impact.** The non-daemon scheduled pool prevents JVM exit: any CLI, test harness or spark-submit driver that touches a schema file will hang on shutdown until killed. In a Spark executor, the CWD is the YARN/k8s container work dir, so `../../..` walks into sibling containers' scratch space and the NodeManager tree — a slow, noisy, and privacy-relevant filesystem scan performed as a fallback for a simple 'file not found'. The maintenance task logs at INFO every 5 minutes forever. Leaked Files.walk streams exhaust file descriptors under repeated discovery.

**Fix.** Make FileFinder an instantiable, AutoCloseable component rather than a static singleton with hidden global lifecycle; let the caller own it. If the static convenience API must stay, mark both executors daemon and register a shutdown hook. Replace the ad-hoc scheduled maintenance with Guava's built-in cache eviction (already configured at line 337) — the maintenance task does nothing that CacheBuilder isn't already doing. Delete `".."`, `"../.."`, `"../../.."` from the default search paths and make deep search opt-in with an explicit root. Wrap line 831 in try-with-resources.

### `NP-008` — Spark UDF closures capture `this`, shipping the SparkSession and all schema analysis metadata to every task

- **Location:** `src/main/java/io/github/pierce/spark/NexusPiercerSparkPipeline.java:483`
- **Cluster:** arch | **Category:** spark-serialization | **Effort:** small
- **Complexity:** closure carries pipeline + SparkSession + schema metadata
  - → closure carries one JsonFlattenerConsolidator

**Evidence.** processFlattenedMode (line 477-489) builds a UDF whose lambda body is `return flattener.flattenAndConsolidateJson(json);` where `flattener` is the INSTANCE field declared at line 47. Java captures `this` for an instance-field reference, so the closure drags the whole NexusPiercerSparkPipeline: `SparkSession spark` (line 42), `PipelineConfig config` (line 44), and `AvroSchemaFlattener schemaFlattener` (line 48) with its fieldMetadataList, recordDefinitions (each holding a full Avro schema STRING per record type) and fieldHierarchyMap. initializeProcessors (line 1050+) has the same defect. The author clearly knows the correct idiom — processJsonColumn line 573 does exactly the right thing: `final JsonFlattenerConsolidator flattener = this.flattener;` — but applies it in only one of three places.

**Impact.** Every task closure serializes hundreds of KB of schema analysis metadata that the UDF never reads, on a 500-field schema. It also makes the pipeline's serializability contingent on SparkSession's readResolve trick rather than on the closure being genuinely self-contained, so any future non-serializable field added to the pipeline turns into a runtime `Task not serializable` in production rather than a compile error. The pipeline field `flattenUdf` (line 49) is additionally shadowed by the local at line 477 and is dead.

**Fix.** Never reference instance fields from a UDF lambda. Hoist the minimal state to `final` locals immediately before every udf() call, as processJsonColumn already does, or better: make the UDF factory a static method taking only the values it needs (`static UserDefinedFunction flattenUdf(JsonFlattenerConsolidator f)`). Delete the unused `flattenUdf` field. Add a unit test that serializes each produced closure with JavaSerializer and asserts the byte size stays under a threshold.

### `NP-009` — JsonReconstructor.groovy is 1,293 lines of 100% commented-out code still in the compile source root

- **Location:** `src/main/groovy/io/github/pierce/JsonReconstructor.groovy:1`
- **Cluster:** arch | **Category:** dead-code | **Effort:** trivial

**Evidence.** `grep -v "^\s*//" JsonReconstructor.groovy | grep -v "^\s*$"` returns zero lines. The file opens `//package io.github.pierce;` and closes `//}` — every one of its 1,293 lines is commented out, including the class declaration, an entire Builder, and a ReconstructionException type. A live test file JsonReconstructorTest.groovy still exists in src/test/groovy. docs/CONCERNS.md already registers this as C-001 but rates it Medium and it has not been actioned.

**Impact.** This is the JSON-side counterpart to AvroReconstructor and its absence is why Stack B has no JSON round-trip. Leaving it commented rather than deleted means the gap is invisible: the package looks like it has a JsonReconstructor, the docs reference one, and consumers will grep the jar and find nothing. It is also a live merge-conflict and confusion surface in every future refactor.

**Fix.** Delete the file and its test, and record the missing capability as a backlog item instead of as commented source. If the intent is to revive it, do so on a branch — git history is the place for code that does not compile, not the main source root. Note that reviving it as written would create a fourth key-encoding convention; it should instead be rebuilt as a thin adapter over the unified engine from NP-001.

### `NP-010` — No coherent public API: 109 public types and 699 public methods, no facade, no package-info, no module-info

- **Location:** `src/main/java/io/github/pierce/AvroSchemaFlattener.java:33`
- **Cluster:** arch | **Category:** api-surface | **Effort:** large
- **Complexity:** 109 public types, 699 public methods, 1 flat package
  - → ~25 public types behind a facade, 5 packages, module-info

**Evidence.** 39 top-level public types plus 70 public nested types = 109 exported types; 699 lines matching `^\s*public .*(`. `find src -name module-info.java -o -name package-info.java` returns nothing. There is no @Internal/@Experimental annotation anywhere. JsonFlattener.groovy alone exports 19 public nested classes (Builder, JsonFlattenerConfig, ConfigBuilder, InputOptions, InputOptionsBuilder, OutputOptions, OutputOptionsBuilder, FluentOperation, BatchOperation, BatchResult, BatchError, StreamOperation, StreamResult, StreamError, ValidationRules, ValidationRulesBuilder, ValidationResult, and two exception types). Implementation detail types are public: AvroSchemaFlattener.FieldHierarchy, .RecordDefinition, .FieldReference, .ArrayDefinition, .TypeTransformation. Root package io.github.pierce mixes ten classes from two mutually-incompatible stacks with no naming or packaging signal of which belongs to which.

**Impact.** There is no answer to 'what do I import?'. A consumer facing io.github.pierce cannot tell that JsonFlattener and JsonFlattenerConsolidator belong to different, non-interoperating systems (docs/CONCERNS.md C-002 notices the naming confusion but not the cause). Every one of those 109 types is a semver commitment — the project cannot refactor internals without a breaking release. Nested-class explosion in JsonFlattener means the fluent API's intermediate states are all public and independently constructible.

**Fix.** Introduce packaging that encodes the architecture: `io.github.pierce.api` (facade + value types only), `io.github.pierce.flatten`, `io.github.pierce.reconstruct`, `io.github.pierce.spark`, `io.github.pierce.internal.*`. Add a single `NexusPiercer` facade class as the documented entry point. Add package-info.java to every package with a one-paragraph contract statement, and add module-info.java exporting only api/ and spark/. Demote FieldHierarchy, RecordDefinition, FieldReference, ArrayDefinition and the JsonFlattener intermediate builders to package-private or internal. Target a public surface under 25 types.

### `NP-011` — The flattened-name contract is duplicated as string literals in four places instead of being one abstraction

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:731`
- **Cluster:** arch | **Category:** missing-abstraction | **Effort:** small

**Evidence.** The `_count/_distinct_count/_min_length/_max_length/_avg_length/_type` suffix contract exists four times: (1) as typed constants in AvroSchemaFlattener.java:59-64; (2) as raw string literals in JsonFlattenerConsolidator.java:663-664, 684-690 and 731-736; (3) as concatenated literals in NexusPiercerFunctions.java:109 and :129; (4) as a regex in NexusPiercerPatterns.java:126 `".*_count$|.*_type$|.*_distinct_count$|.*_min_length$|.*_max_length$|.*_avg_length$"` and again as a replaceAll at line 133. Separately, ArraySerializationFormat is declared twice as identical 4-constant enums — MapFlattener.groovy:1273 and AvroReconstructor.groovy:132, the latter carrying the comment `// Array serialization formats (must match MapFlattener)`, i.e. an invariant maintained by a code comment.

**Impact.** AvroSchemaFlattener declares these fields in the flattened Avro schema; JsonFlattenerConsolidator emits them into the data; NexusPiercerFunctions reads them back; NexusPiercerPatterns classifies them. Four independent copies of one contract, with the producer and consumer sides not sharing a single symbol. Adding a statistic, renaming a suffix, or changing a delimiter requires four coordinated edits, and drift produces silent nulls (the Spark UDF at NexusPiercerFunctions.java:112 returns null when the key is missing, so a suffix mismatch is invisible). The duplicated ArraySerializationFormat enum means a new format added to MapFlattener produces output AvroReconstructor cannot parse, with no compile error.

**Fix.** Create one `FlattenedNaming` type owning the separator, the array-index encoding, and the statistic suffixes, with `statName(base, Stat)` and `parseStat(key) -> Optional<Stat>`. Every producer and consumer goes through it. Hoist ArraySerializationFormat to a single top-level enum shared by the flattener and the reconstructor, and pair each constant with its serializer/deserializer implementation so a new format cannot be added on only one side.

### `NP-012` — System.err.println in a per-record hot path plus printStackTrace in library code

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:475`
- **Cluster:** arch | **Category:** error-handling | **Effort:** small

**Evidence.** JsonFlattenerConsolidator.java has 14 System.err.println calls and 2 printStackTrace calls and imports no logger at all. The worst is shouldKeepAsArrayElements (lines 475-504), invoked once per array node per record, which unconditionally prints `"shouldKeepAsArrayElements: checking '" + currentPath + "'"` and then either `"  -> YES (direct match...)"` or `"  -> NO"`. performExplosionOnFlattened (lines 218-225) prints a `=== performExplosionOnFlattened ===` banner plus the first 10 key/value pairs of every record. Lines 127-129 and 179-181 catch Exception, print the message, call `e.printStackTrace()`, and return `"{}"`. AvroReconstructor.groovy and JsonReconstructor.groovy add 3 more System.out/err calls.

**Impact.** On a Spark job the per-record stderr writes are a synchronized, unbuffered, unfilterable write behind a global lock — this alone can dominate runtime and will fill executor logs with gigabytes of debug output that no log level can suppress. The exception handling is worse than the printing: returning `"{}"` on any failure converts a parse error into a silently empty record, so a malformed-input bug shows up as quietly missing data rather than as a job failure or a quarantined row — directly undermining the pipeline's own ErrorHandling.QUARANTINE contract.

**Fix.** Add an SLF4J logger to JsonFlattenerConsolidator and convert the hot-path prints to `log.trace` guarded by isTraceEnabled, or delete them outright (they are leftover debugging, as the `=== ... ===` banner shows). Replace `catch (Exception e) { print; return "{}"; }` with a typed `FlattenException` propagated to the caller, and let NexusPiercerSparkPipeline's existing ErrorHandling strategy decide whether to fail, skip or quarantine — that decision belongs to the pipeline, not to the flattener. Add a checkstyle/PMD rule banning System.out, System.err and printStackTrace in src/main (the pom already wires PMD at line 1371 and checkstyle at 1397).

### `NP-013` — Serializable is declared where it cannot hold and omitted where Spark needs it

- **Location:** `src/main/java/io/github/pierce/spark/NexusPiercerSparkPipeline.java:95`
- **Cluster:** arch | **Category:** spark-serialization | **Effort:** small

**Evidence.** NexusPiercerSparkPipeline.CachedSchema (line 95) declares `implements Serializable` with `serialVersionUID` but holds two non-transient `org.apache.avro.Schema` fields (lines 97-98) — org.apache.avro.Schema does not implement Serializable, so any attempt to actually serialize a CachedSchema throws NotSerializableException. Conversely AvroReconstructor.groovy:77 is `public class AvroReconstructor {` with no Serializable at all, despite being the reconstruction entry point of a library whose stated purpose is Spark. JsonFlattenerConsolidator's private FlattenTask (line 796) declares Serializable while its `Object value` field holds Jackson JsonNode instances, also non-serializable.

**Impact.** CachedSchema's Serializable declaration is a false promise that will pass code review and fail at runtime the first time someone broadcasts or captures one — the code currently avoids it only by accident (processJsonColumn line 566 broadcasts just `cachedSchema.sparkSchema`). AvroReconstructor's omission is the harder blocker: it simply cannot be used inside a Spark closure, which means Stack B's reconstruction is unreachable from Spark by construction, reinforcing NP-001.

**Fix.** Remove `implements Serializable` from CachedSchema and FlattenTask — they are driver-local and the declaration is misleading. Where Avro Schema genuinely must cross the wire, hold it as a `transient Schema` plus a `String schemaJson` and reparse in a readObject/lazy getter (Avro's canonical pattern). Make AvroReconstructor Serializable with the same treatment for its ConcurrentHashMap schema cache (mark it transient and rebuild lazily). Add a `SerializableAssertions` test that round-trips every type intended to enter a closure.

### `NP-014` — Seven Spark UDFs allocate a new JsonFlattenerConsolidator per row and swallow every exception into null

- **Location:** `src/main/java/io/github/pierce/spark/NexusPiercerFunctions.java:30`
- **Cluster:** arch | **Category:** efficiency | **Effort:** small
- **Complexity:** 1 object allocation per row x 7 UDFs
  - → 1 shared instance per configuration

**Evidence.** flattenJson (line 30), flattenJsonWithDelimiter (45), flattenJsonWithStats (61), extractJsonArray (78), jsonArrayCount (99), jsonArrayDistinctCount (119), explodeJsonArray (141) and extractNestedField (218) each construct `new JsonFlattenerConsolidator(...)` inside the lambda body — i.e. once per input row. Every one of them wraps the work in `catch (Exception e) { return null; }` (explodeJsonArray returns `new String[]{null}`). Additionally the UDFs are declared as `public static UserDefinedFunction flattenJson = udf(...)` — public, static, and NOT final, at lines 30, 45, 61, 78, 99, 119, 141, 162, 185 and 218.

**Impact.** Per-row allocation of a configuration object across billions of rows is pure GC pressure with no upside — the object is immutable and stateless per call, so one instance per UDF would do. The blanket `return null` means a genuine flattening bug is indistinguishable from a legitimately null input: the job succeeds, the column is null, and nobody knows. The non-final public static fields are globally mutable shared state — any code on the classpath can reassign NexusPiercerFunctions.flattenJson, and registerAll() at line 270 will then register the replacement.

**Fix.** Hoist one `private static final JsonFlattenerConsolidator DEFAULT_FLATTENER` (and one per distinct config) outside the lambdas — the class is already thread-safe by design. Mark every UDF field `final`. Replace the blanket null-swallow with a policy consistent with the pipeline's ErrorHandling enum: at minimum, log at warn with a sampled rate, and expose a companion `*_error` UDF so callers can distinguish null-input from failed-parse. Consider collapsing the ten near-identical UDFs into a parameterized factory — they differ only in the constructor flags and the key suffix they read.

### `OSS-03` — GitHub Actions workflow sits at repo root instead of .github/workflows/ — no CI runs at all

- **Location:** `maven-publish.yml:1`
- **Cluster:** hygiene | **Category:** ci-cd | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** `ls -a` at the repo root shows no .github directory. maven-publish.yml is a valid workflow (`name: Publish package to Maven Central`, `on: release: types: [published]`, `workflow_dispatch`) but GitHub only executes workflows under .github/workflows/. There is no build/test workflow of any kind.

**Impact.** Zero automated verification: no PR builds, no test runs, no dependency or license scanning, and the release-publish path is dead so every Maven Central release must be a manual local `mvn deploy`. Combined with OSS-05 (quality gates skipped by default) nothing mechanically prevents a broken or unformatted commit from landing on main.

**Fix.** git mv maven-publish.yml .github/workflows/maven-publish.yml and add a .github/workflows/ci.yml that runs `mvn -P quality verify` on push and pull_request against JDK 17. Verify the OSSRH server-id and secrets referenced in the workflow still match the pom's distributionManagement.

### `OSS-04` — No CONTRIBUTING, SECURITY, CODE_OF_CONDUCT, CHANGELOG, CODEOWNERS, or issue/PR templates

- **Location:** `pom.xml:181`
- **Cluster:** hygiene | **Category:** governance | **Effort:** medium
- **Complexity:** N/A
  - → N/A

**Evidence.** `git ls-files` returns no CONTRIBUTING.md, SECURITY.md, CODE_OF_CONDUCT.md, CHANGELOG.md, .github/CODEOWNERS, or .github/ISSUE_TEMPLATE. The pom advertises `<issueManagement><system>GitHub Issues</system>` (line ~181) and a single developer with a personal email (lonerganpierce@gmail.com), so vulnerability reports have no private channel and would land in public issues.

**Impact.** A published dependency with no SECURITY.md has no coordinated-disclosure path — a reporter's only option is a public issue that discloses the vulnerability before a fix exists. No CONTRIBUTING means external contributors cannot learn the build, test, or profile conventions (which are non-obvious here: `mvn verify` silently skips all static analysis). No CHANGELOG means downstream consumers cannot assess upgrade risk between 1.0.x releases.

**Fix.** Add SECURITY.md with a private reporting channel (GitHub private vulnerability reporting or a dedicated alias) and a supported-versions table; CONTRIBUTING.md documenting `mvn -P quality verify`, the Groovy/Java source-root split, and commit conventions; CODE_OF_CONDUCT.md (Contributor Covenant); CHANGELOG.md in Keep a Changelog format backfilled from 1.0.5 onward; and .github/CODEOWNERS plus bug/feature issue templates and a PR template.

### `OSS-05` — Default-active Maven profile disables Checkstyle, PMD, and SpotBugs, so the checked-in rulesets never run

- **Location:** `pom.xml:1306`
- **Cluster:** hygiene | **Category:** quality-gates | **Effort:** medium
- **Complexity:** N/A
  - → N/A

**Evidence.** The `development` profile is `<activeByDefault>true</activeByDefault>` and sets `<checkstyle.skip>true</checkstyle.skip>`, `<pmd.skip>true</pmd.skip>`, `<spotbugs.skip>true</spotbugs.skip>` (pom.xml lines ~1306-1318). The three plugins are only bound inside the non-default `quality` profile (lines ~1342, 1371, 1397) and in `<reporting>` (lines ~1661-1682). src/main/checkstyle/checkstyle.xml and src/main/pmd/pmd-ruleset.xml are tracked but unreachable in a normal build. `jacoco.minimum.coverage` is set to 0.20.

**Impact.** The repository looks governed — rulesets committed, `pmd.violation.buildFailOnViolation=true`, `spotbugs.threshold=Low`, `checkstyle.violationSeverity=warning` — but a plain `mvn verify` enforces none of it. With no CI (OSS-03), no static analysis has ever gated a commit. A 20% coverage floor is also below any meaningful bar for a data-transformation library.

**Fix.** Bind checkstyle/pmd/spotbugs in the main <build><plugins> section with `check` goals on the verify phase, and invert the profiles so opting *out* (`-P fast`) is explicit rather than the default. Have CI run `mvn -P quality verify`. Raise jacoco.minimum.coverage in steps toward a realistic target once a baseline is measured.

### `OSS-06` — 16MB of Groovy 5.0.0 jars in lib/ are untracked and not gitignored, and contradict the pom's Groovy 4.0.21

- **Location:** `.gitignore:38`
- **Cluster:** hygiene | **Category:** repo-hygiene | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** `du -sh lib` reports 16M across 58 files (groovy-5.0.0.jar, groovy-ant-5.0.0.jar, plus -sources jars). .gitignore is 38 lines and contains no `lib/` entry, so `git status` reports the directory as untracked. pom.xml line 150 sets `<groovy.version>4.0.21</groovy.version>` and spock.version is `2.3-groovy-4.0`. docs/PROJECT_OVERVIEW.md:118 documents lib/ as 'Groovy 5.0.0 runtime JARs'.

**Impact.** A single `git add .` commits 16MB of binaries to permanent history — irreversible without a history rewrite, and it would bloat every future clone. Separately, a Groovy 5.0.0 classpath sitting next to a Maven build pinned to 4.0.21 is a latent compile/runtime divergence: whatever the IDE resolves from lib/ is not what Maven builds against.

**Fix.** Add `lib/` to .gitignore immediately. Then decide the jars' purpose: if they are an IDE-only convenience, delete them and let Maven resolve Groovy; if a Groovy 5 upgrade is intended, do it in the pom (groovy.version plus a Groovy-5-compatible Spock) rather than via a side-loaded directory. Remove the lib/ row from docs/PROJECT_OVERVIEW.md once resolved.

### `OSS-07` — docs/BACKLOG.md marks BL-004 'COMPLETED' but none of the claimed pom.xml changes were ever made

- **Location:** `docs/BACKLOG.md:102`
- **Cluster:** hygiene | **Category:** documentation-accuracy | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** BL-004 'Dependency Hygiene & Modernization — COMPLETED ✅' (lines 102-119) claims: ByteBuddy extracted to a property at 1.15.10, Objenesis extracted at 3.4, Delta-Spark extracted to `${delta-spark.version}`, Mockito 5.7.0→5.14.2, AssertJ 3.24.2→3.26.3, Testcontainers 1.19.3→1.20.4. Actual pom.xml: `<mockito.version>5.7.0</mockito.version>` (line 144), `<assertj.version>3.24.2</assertj.version>` (145), `<testcontainers.version>1.19.3</testcontainers.version>` (147), byte-buddy hardcoded `<version>1.14.4</version>` (line 644), objenesis hardcoded `<version>3.3</version>` (651), delta-spark hardcoded `<version>3.1.0</version>` (684). Every value is the documented 'before' state.

**Impact.** The project's own backlog is not a reliable record of what has been done. Anyone triaging dependency hygiene will skip this work believing it is finished, leaving outdated test dependencies and hardcoded versions in place. It also casts doubt on the other 'COMPLETED' entry (BL-003, org.json removal) and on the docs corpus generally.

**Fix.** Move BL-004 back to an open priority tier with a note that the changes were reverted or never landed, then actually apply them and verify with `mvn dependency:tree`. Going forward, require a commit SHA on every backlog item marked completed so the claim is checkable.

### `OSS-08` — Published version 1.0.8 has zero git tags, no CHANGELOG, and a stale version in the API docs

- **Location:** `docs/API_SURFACE.md:296`
- **Cluster:** hygiene | **Category:** release-management | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** pom.xml line 13 is `<version>1.0.8</version>`; `git tag` returns nothing across 49 commits; docs/API_SURFACE.md line 296 still advertises `<version>1.0.5</version>` in its Maven coordinates block. maven-publish.yml triggers on `release: published`, an event that requires a tag.

**Impact.** No commit in history is identifiable as any released version, so a consumer on 1.0.5 cannot diff against 1.0.8 or bisect a regression. The publish workflow's own trigger can never fire. Users copying the documented Maven coordinates pull a three-versions-old artifact whose API differs from what the same document describes.

**Fix.** Tag the commits corresponding to released versions retroactively where identifiable, tag every future release (v1.0.9 etc.), adopt maven-release-plugin or a tag-driven CI release, add CHANGELOG.md, and replace the hardcoded version in docs/API_SURFACE.md with a reference to the badge/latest-release link so it cannot drift.

### `BLD-001` — Groovy main sources compile twice — gmavenplus declared in both pluginManagement and build/plugins

- **Location:** `pom.xml:720`
- **Cluster:** infra | **Category:** build-correctness | **Effort:** trivial
- **Complexity:** 2x Groovy compilation of 76 units
  - → 1x

**Evidence.** `<pluginManagement>` declares gmavenplus with its own executions (720-760):
```xml
<execution><id>compile</id><goals><goal>addSources</goal><goal>addTestSources</goal><goal>compile</goal><goal>compileTests</goal></goals></execution>
```
and `<build><plugins>` declares it again with six differently-named executions (961-1038: `add-sources`, `generate-stubs`, `compile-groovy`, `add-test-sources`, `generate-test-stubs`, `compile-groovy-tests`). Maven MERGES pluginManagement executions into the build plugin rather than replacing them. Confirmed by `mvn -o -B compile`:
```
[INFO] --- gplus:3.0.2:addSources (compile) @ nexus-piercer ---
[INFO] --- gplus:3.0.2:addTestSources (compile) @ nexus-piercer ---
[INFO] --- gplus:3.0.2:addSources (add-sources) @ nexus-piercer ---
[INFO] --- gplus:3.0.2:addTestSources (add-test-sources) @ nexus-piercer ---
...
[INFO] --- gplus:3.0.2:compile (compile) @ nexus-piercer ---
[INFO] Compiled 76 files.
[INFO] --- gplus:3.0.2:compile (compile-groovy) @ nexus-piercer ---
[INFO] Compiled 76 files.
```
The `mixed-compilation` profile (1568-1593) declares gmavenplus a third time with a default-id execution binding all eight goals, which would add yet another set.

**Impact.** Every build compiles 76 Groovy source units twice and adds the source roots twice, roughly doubling the slowest phase of the build. More dangerously, the pluginManagement `compile` execution runs at the default phase with no `removeStubs` pairing, so generated Java stubs and real Groovy output race — a class-ordering bug waiting to surface. Activating `-Pmixed-compilation` triples it.

**Fix.** Delete the gmavenplus `<plugin>` block from `<pluginManagement>` entirely (or strip it down to just `<version>`), keeping only the explicit executions in `<build><plugins>`. Delete the `mixed-compilation` profile — it duplicates what the main build already does.

### `COV-001` — JaCoCo threshold is 20% against 60.3% actual coverage — the gate permits a 40-point regression

- **Location:** `pom.xml:187`
- **Cluster:** infra | **Category:** coverage | **Effort:** small
- **Complexity:** 20% floor vs 60.3% actual
  - → 58% floor vs 60.3% actual

**Evidence.** `<jacoco.minimum.coverage>0.20</jacoco.minimum.coverage>` (187), consumed by the single check rule:
```xml
<rule><element>BUNDLE</element><limits>
  <limit><counter>INSTRUCTION</counter><value>COVEREDRATIO</value><minimum>${jacoco.minimum.coverage}</minimum></limit>
  <limit><counter>CLASS</counter><value>MISSEDCOUNT</value><maximum>50</maximum></limit>
</limits></rule>
```
(1163-1182). Measured from the existing `target/site/jacoco/jacoco.csv`: instructions missed=17612, covered=26711 → ratio 0.6026. Fully-uncovered classes = 36 out of 167 total. Running `mvn -o -B jacoco:check@check` prints `[INFO] Analyzed bundle 'nexus-piercer' with 167 classes` / `[INFO] All coverage checks have been met.` The rule is BUNDLE-scoped only — there are no PACKAGE or CLASS element rules, and no BRANCH, LINE, METHOD or COMPLEXITY counters.

**Impact.** Coverage can fall from 60% to 20% and 36 uncovered classes can grow to 50 without the build noticing. A ratchet set 40 points below the current value provides no regression protection whatsoever — it exists to make the reactor log say 'coverage checks have been met'.

**Fix.** Raise `jacoco.minimum.coverage` to just under current actual (e.g. 0.58) and treat it as a ratchet. Add a `<counter>BRANCH</counter>` limit and a second `<rule><element>CLASS</element>` rule with a modest minimum plus an `<excludes>` list for genuinely untestable classes, so new zero-coverage classes fail instead of consuming the MISSEDCOUNT budget. Lower `CLASS/MISSEDCOUNT` maximum from 50 to 36.

### `DOC-001` — Published javadoc jar omits all five Groovy public API classes

- **Location:** `pom.xml:1241`
- **Cluster:** infra | **Category:** release | **Effort:** medium

**Evidence.** maven-javadoc-plugin runs `jar` (1257-1264) but only javadoc's Java sources; groovydoc is bound to the `site` phase in the pluginManagement gmavenplus block (`<execution><id>groovydoc</id><phase>site</phase>`, 734-740), which `deploy` never reaches. Built it to check: `mvn -o -B javadoc:jar` → BUILD SUCCESS, producing a 527KB jar with 177 `.html` entries. Grepping the jar for the five Groovy classes in `src/main/groovy/io/github/pierce/` (AvroReconstructor, GAvroSchemaFlattener, JsonFlattener, JsonReconstructor, MapFlattener) returns zero matches, while the Java class `io/github/pierce/JsonFlattenerConsolidator.html` is present. All five compile into `target/classes` and ship in the main jar (e.g. `target/classes/io/github/pierce/AvroReconstructor.class` plus 16 inner classes).

**Impact.** Roughly half the advertised public API — `JsonFlattener`, `MapFlattener`, `AvroReconstructor` are the headline classes in the project description — is undocumented on javadoc.io and in IDE tooltips for every consumer. Maven Central accepts the bundle because a javadoc jar exists; the gap is invisible until a user complains.

**Fix.** Add a gmavenplus `groovydoc`/`groovydocJar` execution bound to `package`, and attach the output with build-helper `attach-artifact`, or switch to the `gmavenplus:groovydoc` + `maven-javadoc-plugin` combined output directory so one javadoc jar covers both languages.

### `JDK-001` — The java.version property shadows the JVM's own java.version, so Build-Jdk always records "17" regardless of the real JDK

- **Location:** `pom.xml:105`
- **Cluster:** infra | **Category:** toolchain | **Effort:** small

**Evidence.** `<java.version>17</java.version>` (105) overrides the JVM system property of the same name for the whole POM. It is then used for four distinct purposes: `maven.compiler.release` (106-108), gmavenplus `<targetBytecode>${java.version}</targetBytecode>` (1018), the enforcer's `<requireJavaVersion><version>[${java.version},)</version>` (857-859), and the jar manifest's `<Build-Jdk>${java.version}</Build-Jdk>` (809). Actual environment: `mvn -v` → `Java version: 17.0.15, vendor: Eclipse Adoptium`, while `java -version` on PATH → `openjdk version "21.0.7" ... Temurin-21.0.7+6`. Surefire forks confirm the tests ran on 17 (`name="java.version" value="17.0.15"` in the surefire XML).

**Impact.** Four unrelated concerns are coupled to one symbol, and the manifest tells a lie: build on JDK 21 and the jar still claims `Build-Jdk: 17`, which is exactly the provenance field consumers check when debugging a class-version mismatch. The overload also means the `java8` profile's `<java.version>8</java.version>` silently rewrites the enforcer floor to `[8,)` and the Groovy target bytecode to 8. Separately, `requireJavaVersion [17,)` is an open upper bound, so a JDK 25 build passes enforcement while `release 17` quietly changes behaviour.

**Fix.** Rename the project property to `maven.compiler.release` (or `project.jdk.version`) and stop reusing `java.version`. Drop `Build-Jdk` entirely per REPRO-001. Bound the enforcer rule: `<version>[17,22)</version>`. Add a `<toolchain>` or document the required JDK so the 17-vs-21 split is deliberate rather than accidental.

### `PUB-001` — distributionManagement points at the decommissioned s01.oss.sonatype.org while central-publishing-maven-plugin is also active

- **Location:** `pom.xml:85`
- **Cluster:** infra | **Category:** release | **Effort:** small

**Evidence.** ```xml
<distributionManagement>
  <snapshotRepository><id>ossrh</id><url>https://s01.oss.sonatype.org/content/repositories/snapshots</url></snapshotRepository>
  <repository><id>ossrh</id><url>https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/</url></repository>
```
(85-93). Simultaneously `org.sonatype.central:central-publishing-maven-plugin:0.8.0` is declared with `<extensions>true</extensions>` and `<publishingServerId>central</publishingServerId>` (1286-1297) — confirmed active, every offline `mvn` invocation logs `[INFO] Installing Central Publishing features`. Legacy OSSRH (both oss.sonatype.org and s01.oss.sonatype.org) was permanently sunset on 2025-06-30; today is 2026-08-09. Meanwhile maven-publish.yml configures `server-id: ossrh` with `server-username: OSSRH_USERNAME`, so settings.xml gets a `<server><id>ossrh></id>` entry while the plugin looks up `central`. The property `<nexus-staging-maven-plugin.version>1.6.13</nexus-staging-maven-plugin.version>` (172) is defined but the plugin is never declared — a fossil of the pre-Portal setup.

**Impact.** Three independent publish failures stacked: dead endpoint URLs, credential id mismatch (`ossrh` vs `central`) producing a 401 from the Portal, and `<autoPublish>false</autoPublish>` meaning even a successful upload sits in the Portal as an un-promoted draft requiring a manual click — so the 'publish on release' automation never actually publishes.

**Fix.** Delete the entire `<distributionManagement>` block (the Central Publishing plugin explicitly requires its absence). Change maven-publish.yml `server-id: ossrh` → `server-id: central` with `CENTRAL_USERNAME`/`CENTRAL_TOKEN`. Set `<autoPublish>true</autoPublish>` once the first manual release has succeeded. Delete the unused `nexus-staging-maven-plugin.version` property.

### `Q-003` — checkstyle.xml enforces three import rules at severity=warning — a nominal gate

- **Location:** `src/main/checkstyle/checkstyle.xml:5`
- **Cluster:** infra | **Category:** quality-gates | **Effort:** medium

**Evidence.** The entire 471-byte config is:
```xml
<module name="Checker">
    <property name="charset" value="UTF-8"/>
    <property name="severity" value="warning"/>
    <module name="TreeWalker">
        <module name="UnusedImports"/>
        <module name="RedundantImport"/>
        <module name="IllegalImport"/>
    </module>
</module>
```
The POM pairs this with `<checkstyle.violationSeverity>warning</checkstyle.violationSeverity>` (191) and `<includeTestSourceDirectory>false</includeTestSourceDirectory>` (1413).

**Impact.** Even if the quality profile were fixed and wired into the default build, Checkstyle would only reject unused/redundant/illegal imports across 39 main Java files, and would never look at the 29 test Java files. No naming, whitespace, javadoc, complexity, magic-number, or equals/hashCode rules. The plugin version pinning (`checkstyle.version` 10.12.5, Nov 2023) suggests real intent that was never followed through.

**Fix.** Base the config on `google_checks.xml` or `sun_checks.xml` and subtract, rather than starting from three rules. Set `severity=error` for the subset the team will actually honour. Flip `includeTestSourceDirectory` to true.

### `REPRO-001` — Reproducible builds are defeated by jar manifest entries despite project.build.outputTimestamp being set

- **Location:** `pom.xml:806`
- **Cluster:** infra | **Category:** reproducible-builds | **Effort:** trivial

**Evidence.** `<project.build.outputTimestamp>2024-01-01T00:00:00Z</project.build.outputTimestamp>` is set (114) and does work for entry timestamps — `unzip -l target/nexus-piercer-1.0.8-javadoc.jar` shows every entry dated `2024-01-01 00:00`. But maven-jar-plugin injects four non-deterministic manifest entries (806-812):
```xml
<manifestEntries>
    <Built-By>${user.name}</Built-By>
    <Build-Timestamp>${maven.build.timestamp}</Build-Timestamp>
    <Build-Jdk>${java.version}</Build-Jdk>
    <Build-OS>${os.name} ${os.version} ${os.arch}</Build-OS>
    <Implementation-Build>${buildNumber}</Implementation-Build>
</manifestEntries>
```
`${maven.build.timestamp}` changes every invocation; `${user.name}` and `${os.*}` vary per machine. `${buildNumber}` is never produced by anything — buildnumber-maven-plugin is not declared — so it emits the literal string `${buildNumber}`.

**Impact.** The jar's SHA is different on every build and on every machine, so the `project.build.outputTimestamp` declaration is cosmetic. Reproducible-build verifiers (and Maven Central's reproducibility badge) will fail. `Built-By: <windows username>` also leaks the maintainer's local account name into every published artifact.

**Fix.** Delete `Built-By`, `Build-Timestamp`, `Build-OS` and `Implementation-Build` from `<manifestEntries>`. If build provenance is wanted, the git-commit-id plugin already writes `target/classes/git.properties` (verified present, containing `git.commit.id.full=e698ae45...` and a deterministic `git.build.time=2023-12-31T19:00:00-0500` derived from outputTimestamp). Optionally set `project.build.outputTimestamp` to `${git.commit.time}` so it tracks the commit.

### `SUP-001` — No CVE scanning, no SBOM, no dependency update automation

- **Location:** `pom.xml:1298`
- **Cluster:** infra | **Category:** supply-chain | **Effort:** medium

**Evidence.** The `<build><plugins>` list ends at line 1298 with no `org.owasp:dependency-check-maven`, no `org.cyclonedx:cyclonedx-maven-plugin`, and no `spdx-maven-plugin`. `find . -name dependabot.yml -o -name renovate.json` → empty, and there is no `.github/` directory to hold one. `versions-maven-plugin` IS declared (1188-1195) but with `<configuration><generateBackupPoms>false</generateBackupPoms></configuration>` and no `<executions>` — it never runs during a build; it only surfaces as three reports in `<reporting>` (1683-1696), which requires `mvn site`, which nothing runs. findsecbugs 1.12.0 is configured (1352-1356) but only inside the broken `quality` profile.

**Impact.** A published library aggregating Spark 3.5.0, Hadoop 3.3.6/hadoop-aws, Iceberg 1.7.1, POI 5.2.5, Jackson, Guava and Avro has a very large transitive attack surface and zero visibility into it. No SBOM means downstream consumers cannot do their own analysis either. Dependency versions are frozen at 2023-2024 vintage with no mechanism to notice.

**Fix.** Add `org.owasp:dependency-check-maven` bound to `verify` with `<failBuildOnCVSS>7</failBuildOnCVSS>` (use an NVD API key to avoid rate limits) and `org.cyclonedx:cyclonedx-maven-plugin` bound to `package` producing `application` SBOMs. Add `.github/dependabot.yml` covering the `maven` and `github-actions` ecosystems.

### `SUP-002` — poi-ooxml 5.2.5 (CVE-2025-31672) at compile scope and logback 1.4.14 (CVE-2024-12798/12801) at test scope

- **Location:** `pom.xml:122`
- **Cluster:** infra | **Category:** supply-chain | **Effort:** trivial

**Evidence.** `<poi.version>5.2.5</poi.version>` (122), used by both `poi-ooxml` (466-488) and `poi` (677-681), both at compile scope. CVE-2025-31672 (Apache POI OOXML, denial of service via crafted OOXML parsing) affects poi-ooxml before 5.4.0. `<logback.version>1.4.14</logback.version>` (146) at test scope — CVE-2024-12798 (JaninoEventEvaluator arbitrary code execution) and CVE-2024-12801 (SSRF in SaxEventRecorder) affect logback through 1.5.12, fixed in 1.5.13. Note the version comment history in the POM shows security awareness elsewhere (`<!-- org.json dependency removed - using Jackson instead (Apache 2.0 License) -->`, 465), so this is drift rather than indifference.

**Impact.** poi-ooxml is compile scope, so the CVE is inherited by every consumer of nexus-piercer. logback is test-only so the blast radius is CI, but `src/test/resources/logback-test.xml` is present so it does load. With no dependency-check plugin (SUP-001) there is nothing to surface either.

**Fix.** Bump `poi.version` to 5.4.0+ and `logback.version` to 1.5.13+. Also consider whether POI belongs at compile scope at all — only one file under `src/` references `org.apache.poi`, so it may be a candidate for `optional` or removal.

### `SUP-003` — 16MB of untracked Groovy 5.0.0 jars in lib/ while the build compiles against Groovy 4.0.21, and .gitignore does not cover them

- **Location:** `.gitignore:1`
- **Cluster:** infra | **Category:** supply-chain | **Effort:** trivial

**Evidence.** `git status` shows `?? lib/`. `ls -la lib/` lists 60 files totalling ~15.5MB, all `groovy-*-5.0.0.jar` and `-5.0.0-sources.jar` (groovy-5.0.0.jar alone is 8.05MB). The POM pins `<groovy.version>4.0.21</groovy.version>` (150), imports `org.apache.groovy:groovy-bom:4.0.21`, and declares `spock-core:2.3-groovy-4.0` (398) which is compiled against the Groovy 4.0 ABI. `.gitignore` contains `target/`, IDE dirs and `build/` but no `lib/`, no `*.jar`, and no `!lib/**` exception — nothing prevents `git add .` from committing all 16MB.

**Impact.** One careless `git add -A` permanently adds 16MB of binaries to git history — unremovable without a history rewrite, and a red flag for any Maven Central / OSS reviewer. The Groovy 5 vs 4.0.21 mismatch also means IDE classpaths sourced from `lib/` disagree with the Maven build: code that compiles in IntelliJ (Groovy 5 APIs) will fail `mvn compile`, and vice versa. Vendored jars are also invisible to any future CVE scanner.

**Fix.** Delete `lib/` — every jar in it is available from Maven Central and the build does not reference it (no `system` scope dependencies exist in the POM). Add `lib/` and `*.jar` to `.gitignore` as a backstop. If an IDE needs Groovy 5 for tooling, configure it as an IDE-level SDK, not a repo directory. Decide deliberately whether to move the project to Groovy 5 (which also requires a `spock-core:...-groovy-5.0` bump).

### `TST-001` — 26 jqwik @Property tests never execute — no engine on the classpath and the class name misses surefire's include patterns

- **Location:** `pom.xml:562`
- **Cluster:** infra | **Category:** test-execution | **Effort:** trivial

**Evidence.** `src/test/java/io/github/pierce/converter/TypeConverterProperties.java` imports `net.jqwik.api.*` and `net.jqwik.api.constraints.*` and contains 26 occurrences of `@Property` (and zero `@Test`). The POM declares only the API, at COMPILE scope:
```xml
<dependency><groupId>net.jqwik</groupId><artifactId>jqwik-api</artifactId><version>${jqwik.version}</version></dependency>
```
(562-566) — no `<scope>test</scope>`, and `grep -c 'jqwik-engine' pom.xml` → 0. Separately, surefire's includes are `**/*Test.java`, `**/*Tests.java`, `**/*TestCase.java` (+ dead .groovy variants) — `TypeConverterProperties` matches none. Confirmed: `target/test-classes/io/github/pierce/converter/TypeConverterProperties.class` exists but there is no corresponding file in `target/surefire-reports/` (`ls target/surefire-reports | grep -i properties` → empty), against 126 report files for the tests that did run.

**Impact.** An entire property-based test suite for the type-converter layer — arguably the highest-value tests in the project — is dead twice over. Even fixing the naming pattern would not run them: without `jqwik-engine` on the test classpath the JUnit Platform has no engine for `@Property`. Meanwhile `jqwik-api` at compile scope is published in the POM as a mandatory runtime dependency for every downstream consumer of the library.

**Fix.** Add `<scope>test</scope>` to `jqwik-api`, add `net.jqwik:jqwik-engine:${jqwik.version}` at test scope, and either rename the class to `TypeConverterPropertyTest` or add `<include>**/*Properties.java</include>` to surefire. Verify a surefire report appears for it.

### `RECON-01` — Entire reconstruction path is dynamically dispatched Groovy — no @CompileStatic anywhere in the repo

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:77`
- **Cluster:** perf-avro | **Category:** groovy-dynamic-dispatch | **Effort:** medium
- **Complexity:** Same asymptotics; realistic measured @CompileStatic speedup on dispatch-heavy Groovy of this shape is ~2-6x, not 3-15x. The floor is set by the 257 ScriptBytecodeAdapter calls and pervasive Integer boxing, not by the indy sites themselves (many of which JIT well when monomorphic).
  - → Direct getfield / iadd / if_icmplt / invokevirtual; JIT can devirtualize and inline

**Evidence.** `grep -rn "CompileStatic|TypeChecked" src/main/groovy/` returns ZERO matches. pom.xml compiles src/main/groovy with gmavenplus `<invokeDynamic>true</invokeDynamic>`. javap of target/classes/io/github/pierce/AvroReconstructor.class: 1773 invokedynamic, 257 ScriptBytecodeAdapter calls (90 compareEqual, 70 isCase, 57 compareNotEqual, 13 compareLessThan), 430 indy `cast` sites, 187 indy `getProperty` sites. Proof from reconstructRecord (line 921 `path + separator + fieldName`) bytecode: `148: invokedynamic #24:invoke:(String;String)Object` then `155: invokedynamic #24:invoke:(Object;String)Object` then `160: invokedynamic cast:(Object)String` — two dynamic dispatches and a dynamic cast for one string concat. And `currentDepth + 1` at line 929 compiles to `229: invokedynamic #24:invoke:(II)Ljava/lang/Object;` — integer addition dispatched through the metaclass and boxed to Object.

**Impact.** Multiplies the cost of EVERY other finding in this cluster by roughly 3-15x. Each indy site is a guarded MethodHandle chain with a per-call class check; arguments are boxed to Object; results need a dynamic cast. On a 100-field record this is thousands of guarded dispatches per record before any real work happens. This single change is worth more than all the algorithmic fixes combined.

**Fix.** Add `@groovy.transform.CompileStatic` to AvroReconstructor (and GAvroSchemaFlattener). The source is already 95% Java syntax so most of it will compile statically unchanged. Expect to fix: the `?.` on typed refs (lines 1426, 1479), the `List<List<Object>> parsed = objectMapper.readValue(strValue, List.class)` at line 1491 (needs an explicit cast), and Groovy `switch` arms with fully-qualified enum labels. Verify afterward with `javap -c | grep -c invokedynamic` — it should drop to near zero. Alternative if static compilation proves too invasive: port the class back to src/main/java (it is already written in Java syntax).

### `RECON-07` — Every PathNode field read goes through the metaclass, and PathNode::new allocates a fresh Closure per path segment

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:901`
- **Cluster:** perf-avro | **Category:** groovy-dynamic-dispatch | **Effort:** small
- **Complexity:** O(d) Closure allocations + O(d) metaclass property lookups + O(d) boxed comparisons per key
  - → O(d) getfield + O(d) direct map ops, zero closure allocation

**Evidence.** PathNode declares fields with no modifier (lines 730-734), so Groovy turns them into properties. `javap -p AvroReconstructor$PathNode.class` shows `private java.util.Map children;` plus generated `getChildren()`, `getValue()`, `isIsLeaf()`, a `metaClass` field, and methodMissing/propertyMissing. There are 58 `invokedynamic getProperty:(...AvroReconstructor$PathNode;)` sites in AvroReconstructor.class — every `node.children`, `node.value`, `node.isLeaf`, `node.arrayFieldValues` is a dynamic property lookup. Worse, handleArrayFieldInTree line 901 `current.children.computeIfAbsent(parts[i], PathNode::new)` compiles inside the loop to:
```
50: ldc_w class AvroReconstructor$PathNode
53: ldc_w String new
56: invokestatic ScriptBytecodeAdapter.getMethodPointer:(Object;String)Lgroovy/lang/Closure;
```
— a NEW groovy.lang.Closure allocated on every loop iteration. PathNode.class contains 3 more getMethodPointer sites (addPath line 746, and the two `String::trim` refs at lines 795/800). The loop counter is also boxed: `14: Integer.valueOf(i)` and `29: ScriptBytecodeAdapter.compareLessThan(Object,Object)`.

**Impact.** Per flat key of depth d: d Closure allocations, d boxed loop comparisons, d metaclass property lookups, plus an eagerly-allocated LinkedHashMap in every PathNode constructor (line 732) even for leaves that never get children. At 100 keys x depth 5 x 10,000 records that is 5M Closure allocations that a Java compiler would emit as a single static lambda.

**Fix.** Fixed largely by RECON-01 (@CompileStatic makes PathNode::new a real constant lambda and field reads real getfields). Additionally: declare PathNode's fields `public final` where possible so Groovy emits fields not properties, and make `children` lazily allocated (null until the first child) so leaf nodes cost one object instead of two.

### `JFLAT-01` — Backtracking regex .find() used as a "does this key contain [n]?" test on every flattened key — measured 8.3 us/key

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:616`
- **Cluster:** perf-flatten | **Category:** algorithmic-complexity | **Effort:** trivial
- **Complexity:** O(K·L²) regex steps per record (K keys of length L); measured 8,261 ns per 68-char key
  - → O(K·L) character comparisons per record; measured 7.7 ns per key, or O(K) with zero scanning if the flag is propagated from flattenJson

**Evidence.** Line 616: `boolean hasArrayIndex = ARRAY_INDEX_PATTERN.matcher(key).find();` where line 42 declares `Pattern.compile("(.+?)\\[(\\d+)\\](.*)")`. The reluctant `.+?` prefix expands one character at a time from every start offset that `find()` tries, so a NON-matching key costs O(L^2) regex steps. I benchmarked this exact pattern against realistic 68-char dotted keys (`payload.customer.account.preferences.notification.channelN.enabled`): 8,261.5 ns/key, versus 7.7 ns/key for an equivalent hand-rolled `indexOf('[')` + digit scan. That is a 1,070x gap. The two capture groups are never read — only the boolean result of find() is used.

**Impact.** This sits in `consolidateFlattened`, which runs once per key per record on the PRIMARY `flattenAndConsolidateJson` path (not an optional branch). At 200 flattened keys/record x 1,000 records = 200,000 evaluations x 8.26 us = ~1.65 seconds of pure regex backtracking per 1,000 records, dwarfing Jackson parse time. Cost grows quadratically with key length, so deeply nested schemas (longer dotted paths) degrade superlinearly. Worse, the information is already known: `flattenJson` itself appended the `[i]` suffixes at line 571 and tracked array prefixes in `arrayFieldsThreadLocal` at line 574.

**Fix.** Replace with a scan: `int b = key.indexOf('['); boolean hasArrayIndex = b >= 0 && key.indexOf(']', b) > b;` (or the full digit-validating scan if strictness matters). Better still, have `flattenJson` return the array-index flag alongside each key — it already knows, since it constructs `prefix + "[" + i + "]"` at line 571 — eliminating the test entirely.

### `JFLAT-03` — No @CompileStatic on either Groovy class — 677 invokedynamic Groovy callsites in MapFlattener.class alone

- **Location:** `src/main/groovy/io/github/pierce/MapFlattener.groovy:102`
- **Cluster:** perf-flatten | **Category:** groovy-dynamic-dispatch | **Effort:** small
- **Complexity:** The mechanism is confirmed exactly; the multiplier is the overstated part. `javap -c -p target/classes/io/github/pierce/MapFlattener.class` returns exactly 677 `invokedynamic`, with exactly the claimed distribution (flattenList=93, serializeArray=50, flattenValue=47, flattenObject=42, normalizePrimitive=28, sanitizeKey=20; also extractFieldsFromList=75, extractFieldsPreservingStructure=68). Even `new LinkedHashMap<>()` at line 337 compiles to `invokedynamic #5:init` and every `result.put` to `invokedynamic #30:invoke`. But 3-10x is the pre-indy (callsite-caching) Groovy figure. With `<invokeDynamic>true</invokeDynamic>` (pom.xml:1020) monomorphic sites stabilize into a GuardWithTest MethodHandle chain that JIT inlines reasonably well; the realistic @CompileStatic win on Java-syntax Groovy like this is ~1.5-3x, not 3-10x. The 'megamorphic' argument is also overstated: most of the 28 callsites in `normalizePrimitive` sit immediately after an explicit cast (`((BigDecimal) value).doubleValue()`, `((BigInteger) value).longValue()`), so each individual callsite sees one receiver type and is effectively monomorphic. The genuinely polymorphic sites are the bare `value.toString()` and the `flattenValue` dispatch.
  - → Direct invokevirtual/invokestatic bytecode; JIT-inlinable; same asymptotics, 3-10x constant-factor reduction

**Evidence.** `grep -rn 'CompileStatic|TypeChecked|groovy.transform'` over the whole repository returns NO MATCHES — no class, method, or pom configScript enables static compilation. Both MapFlattener.groovy (line 102 `public class MapFlattener`) and JsonFlattener.groovy (line 142 `public class JsonFlattener`) are Java-syntax sources under src/main/groovy compiled by gmavenplus (pom.xml:1021-1028) with `<invokeDynamic>true</invokeDynamic>` (pom.xml:1020). Proof they are genuinely Groovy-compiled: MapFlattener.groovy:880 and :891 use `BigDecimal` and `BigInteger` with no `java.math` import — legal only under Groovy's default imports. javap on the compiled classes confirms the cost: MapFlattener.class contains 677 `invokedynamic` instructions, distributed as flattenList=93, serializeArray=50, flattenValue=47, flattenObject=42, sanitizeKey=20 — i.e. the entire recursive core. Even `buildKey`, a three-line String method, disassembles to `ScriptBytecodeAdapter.compareEqual` for the null test, an indy `invoke` + `DefaultTypeTransformation.booleanUnbox` for `prefix.isEmpty()`, and two separate indy `invoke` callsites for the concatenation.

**Impact.** Every `result.put`, `list.get(i)`, `entry.getKey()`, `map.containsKey` and enum `switch` on the recursive flattening path dispatches through Groovy's IndyInterface with a guard chain instead of a direct invokevirtual. Megamorphic sites — `normalizePrimitive` (28 indy callsites, dispatching over ~12 runtime types) and `flattenValue` (Map/List/Set/Collection/array/String/primitive) — cannot be reduced to inline caches and fall to the slow selection path. This is the standard 3-10x penalty applied to the hottest recursive path in the library, and it multiplies every other finding in MapFlattener. It also explains why the Java-syntax code reads as if it should be fast but is not.

**Fix.** Add `@groovy.transform.CompileStatic` at the class level on both MapFlattener and JsonFlattener. Both files are already written in pure Java syntax with fully declared types, so this should compile clean or expose only a handful of implicit-coercion sites. Verify afterward that the indy count drops to ~0 with `javap -c -p target/classes/io/github/pierce/MapFlattener.class | grep -c invokedynamic`.

### `JFLAT-05` — determineArrayType detects numeric arrays by throwing NumberFormatException per element, with no early exit — measured 322x slower than a char scan

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:766`
- **Cluster:** perf-flatten | **Category:** allocation-pressure | **Effort:** trivial
- **Complexity:** O(m) NumberFormatException constructions per array field, each O(stack depth) for fillInStackTrace, no early exit; measured 418.6 ns/element
  - → O(m·L) character comparisons with early break and zero allocation; measured 1.3 ns/element

**Evidence.** Lines 760-774: `for (String val : values) { try { ... Double.parseDouble(val); } catch (NumberFormatException e) { allNumbers = false; } if (!val.equalsIgnoreCase("true") && ...) allBooleans = false; }`. For a string-valued array — the common case — EVERY element throws. `NumberFormatException.forInputString` constructs a fresh exception and `fillInStackTrace` walks the entire call stack (deep under Spark). There is also no short-circuit: once `allNumbers` and `allBooleans` are both false the loop keeps parsing and throwing to the end. Measured against 100 realistic string values: 418.6 ns/element via the exception path versus 1.3 ns/element via a character scan with early break — 322x.

**Impact.** `determineArrayType` is called from `processArrayValues` line 736, which runs for every consolidated array group whenever `gatherStatistics` is true — the default, set by the 5-arg constructor at lines 62-63. A record with 20 string array fields of 100 elements each = 2,000 exceptions/record = ~0.84 ms/record, i.e. ~0.84 s per 1,000 records, and 2 million exception objects per 1,000 records handed to the GC. Under Spark's deep executor stacks fillInStackTrace is materially more expensive than in this isolated benchmark, so this is a conservative floor.

**Fix.** Guard with a cheap scan before parsing: `if (allNumbers && !looksNumeric(val)) allNumbers = false;` where looksNumeric rejects on the first character outside `[0-9.+-eE]`; only call Double.parseDouble on survivors. Add `if (!allNumbers && !allBooleans) break;` at the end of the loop body.

### `JFLAT-06` — System.err.println on the per-array-node and per-record path — unbuffered, synchronized, auto-flushing

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:475`
- **Cluster:** perf-flatten | **Category:** io-on-hot-path | **Effort:** trivial
- **Complexity:** Volume and per-call cost are right; the scoping needs one qualifier the title omits. `shouldKeepAsArrayElements`, `flattenJsonForExplosion` and `performExplosionOnFlattened` are reachable ONLY through `flattenAndExplodeJson`, which short-circuits at line 138-140 to `flattenAndConsolidateJson` when `explosionPaths` is empty. So this is per-array-node/per-record on the explosion path, not on the default consolidate path. Measured cost: 2,012 ns per `System.err.println` with autoflush to a redirected buffered file stream on JDK 21 — squarely inside the claimed 1-10 us band. At 20 array nodes x 2 writes + 13 per record that is ~0.09 s per 1,000 records single-threaded, and the PrintStream-monitor serialization argument holds under Spark.
  - → Zero writes and zero concatenation at default log levels

**Evidence.** `shouldKeepAsArrayElements` opens with line 475: `System.err.println("shouldKeepAsArrayElements: checking '" + currentPath + "'");` and exits through line 481 `System.err.println("  -> YES (direct match with " + explosionPath + ")");`, line 497, or line 503 `System.err.println("  -> NO");` — so 2 writes minimum on every call. It is called from `flattenJsonForExplosion` line 430 for every non-empty array node. Separately, `performExplosionOnFlattened` lines 218-225 print a header plus the first 10 flattened entries for EVERY record: `System.err.println("  " + entry.getKey() + " = " + entry.getValue());`. System.err is a PrintStream with autoFlush=true: each call takes the stream monitor and issues a write syscall.

**Impact.** Two effects compound. Per-call cost is ~1-10 us (syscall-bound), so 20 array nodes/record x 2 calls x 1,000 records = 40,000 writes ≈ 0.04-0.4 s per 1,000 records, plus 13 more writes per record from lines 218-225. Far worse on Spark: every executor core contends on the single PrintStream monitor, serialising all flattening threads through one lock and destroying parallel scaling. The string arguments are also concatenated eagerly before the call, allocating even if stderr were redirected to /dev/null.

**Fix.** Delete these, or route through the SLF4J logger behind `if (log.isTraceEnabled())` (this class has no logger at all — JsonFlattener.groovy:144 shows the pattern). Guarding matters as much as the sink: the `+` concatenations must not execute when logging is off.

### `SCHEMA-03` — Every NexusPiercerFunctions UDF allocates a new flattener per row and flattens+serialises+re-parses the entire document to read one key

- **Location:** `src/main/java/io/github/pierce/spark/NexusPiercerFunctions.java:82`
- **Cluster:** perf-schema | **Category:** per-row-allocation | **Effort:** medium
- **Complexity:** per row: O(K) flatten + O(K) serialise (OBJECT_MAPPER.writeValueAsString) + O(K) re-parse (readTree) + 1 lookup → per row: O(K) flatten + 1 lookup; note 'zero extra allocations' is unreachable since the flatten itself allocates the ObjectNode and K keys
  - → per row: O(K) flatten + 1 map lookup; zero extra allocations

**Evidence.** ```java
public static UserDefinedFunction extractJsonArray = udf(
    (String json, String arrayPath) -> {
        ...
        JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                DEFAULT_DELIMITER, null, DEFAULT_MAX_NESTING, DEFAULT_MAX_ARRAY_SIZE, false);
        String flattened = flattener.flattenAndConsolidateJson(json);
        JsonNode obj = STRICT_JSON_MAPPER.readTree(flattened);
        String key = arrayPath.replace(".", "_");
        JsonNode valueNode = obj.get(key);
```
The same `new JsonFlattenerConsolidator(...)` per-row construction appears at lines 34, 49, 65, 82, 103, 123, 145 and 222; the flatten→readTree→single-key pattern repeats at lines 88, 108, 128 and 227.

**Impact.** Per row the extraction UDFs do: one flattener allocation (HashSet + Arrays.asList, cheap but needless), a full recursive flatten producing a Map of K entries, a Jackson serialisation of all K entries into a JSON String, a full Jackson deserialisation of that String back into a tree of K nodes — and then read exactly one key. The serialise/deserialise round-trip is pure waste and is O(K) in both time and garbage. `arrayPath` arrives as a constant literal column (functions pass `lit(arrayPath)` at lines 250/253/256/265) yet `arrayPath.replace(".", "_")` is recomputed and re-allocated for every row.

**Fix.** Hoist the flattener to a `private static final` field (it is Serializable and stateless apart from a ThreadLocal). Add an overload on JsonFlattenerConsolidator that returns the flattened `Map<String,Object>` so the extraction UDFs read the key straight off the map with no serialise/re-parse. Precompute the underscored key once — either close over it by making the UDF a factory `udf(json -> ...)` bound to the already-normalised path, or normalise lazily into a one-entry memo.

### `SCHEMA-07` — DateTimeFormatter.ofPattern is built per value and format detection is driven by thrown exceptions

- **Location:** `src/main/java/io/github/pierce/converter/DateConverter.java:128`
- **Cluster:** perf-schema | **Category:** per-value-allocation | **Effort:** small
- **Complexity:** For a US M/d/yyyy date column: exactly 3 DateTimeParseException constructions (each capturing a full stack) + 1 DateTimeFormatter.ofPattern build per value; 4 exceptions plus a NumberFormatException per unparseable value. For ISO-8601 input — the default from JSON/Avro — 0 exceptions and 0 formatter allocations, since LocalDate.parse succeeds on the first try
  - → per value: 1 successful parse, 0 exceptions, 0 formatter allocations

**Evidence.** DateConverter.parseDateString tries four formats in sequence, each in a try/catch, and builds a formatter inside the fourth:
```java
// Try common US format (MM/dd/yyyy)
try {
    LocalDate ld = LocalDate.parse(str, DateTimeFormatter.ofPattern("M/d/yyyy"));
```
Identical pattern in TimeConverter.java:108 (`DateTimeFormatter.ofPattern("H:mm")`) and the five-deep cascade in TimestampConverter.parseTimestampString (lines 136-178), which also allocates `str.replace(" ", "T")` at line 155.

**Impact.** `DateTimeFormatter.ofPattern` runs a full DateTimeFormatterBuilder parse of the pattern string and allocates the composite printer/parser graph — hundreds of nanoseconds and several objects, per value. Worse is the cascade: for a column in US M/d/yyyy format every single value throws three DateTimeParseExceptions before reaching the format that works, and DateTimeParseException fills in a stack trace. Inside a Spark task the stack is typically 60-100 frames deep, so each value pays ~3 stack captures. For a 10M-row date column this is tens of millions of exception constructions — easily an order of magnitude more expensive than the parse itself. There is no SimpleDateFormat anywhere in the codebase (good), but the effect here is worse.

**Fix.** Hoist every ofPattern call to a `private static final DateTimeFormatter`. Replace the exception cascade with cheap shape discrimination before parsing (length, presence of '/', '-', 'T', ':', 'Z') and dispatch to a single formatter. Best: since the converter is per-column, memoise the format that succeeded for the first non-null value in that column and try it first on subsequent values, falling back only on mismatch.

### `SCHEMA-09` — Union branch selection is exception-driven: every non-first branch costs a thrown exception per value

- **Location:** `src/main/java/io/github/pierce/converter/SchemaBasedMapConverter.java:966`
- **Cluster:** perf-schema | **Category:** exception-control-flow | **Effort:** medium
- **Complexity:** Understated, not overstated: for ['null','long','string'] with a non-numeric string the long branch alone constructs 3 exceptions (NumberFormatException from Long.parseLong, a second from new BigInteger at LongConverter:57, then the TypeConversionException at :60) before the string branch is reached. So it is O(B) *failed branches* × up to 3 stack-filling constructions per value, plus 1 dead ArrayList per value in AvroSchemaConverter
  - → O(1) type-tag dispatch, 0 exceptions on the success path

**Evidence.** ```java
protected Object doConvert(Object value) {
    for (TypeConverter<Object, Object> converter : branchConverters) {
        try {
            return converter.convert(value);
        } catch (Exception ignored) {
            // Try next branch
        }
    }
    throw conversionError(value, "Value does not match any union branch");
}
```
AvroSchemaConverter.java:552-563 is the same loop but additionally allocates a list that is then never read:
```java
List<Exception> errors = new ArrayList<>();
for (int i = 0; i < branchConverters.size(); i++) {
    try { return branchConverters.get(i).convert(value); }
    catch (Exception e) { errors.add(e); }
}
throw conversionError(value, "Value does not match any union branch. Tried: " + nonNullTypes);
```

**Impact.** For a union like ["null","long","string"] where values are strings, every single value throws and catches a TypeConversionException from the long branch first. Each such exception runs AbstractTypeConverter's catch/rethrow (line 33-37), TypeConversionException.formatMessage — which calls String.format and value.toString() (TypeConversionException.java:47-52) — and fillInStackTrace. That is roughly 1-3 microseconds per value versus ~20ns for the correct branch. In AvroSchemaConverter the caught exceptions are also accumulated into an ArrayList that the throw at line 562 does not reference, so the list and its contents are pure garbage.

**Fix.** Dispatch on the runtime type of the value against the branch schemas (a small switch on Number/CharSequence/ByteBuffer/Map/Collection/Boolean) and only call the matching branch converter. Fall back to the try-cascade only when the type is genuinely ambiguous. Delete the dead `errors` list in AvroSchemaConverter, or use it in the final message.

### `NP-004` — Files.walk stream never closed — file-descriptor leak on every discovery call

- **Location:** `src/main/java/io/github/pierce/files/FileFinder.java:831`
- **Cluster:** quality | **Category:** resource-leak | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
Files.walk(path, 2)
        .filter(p -> { ... })
        .forEach(p -> { ... });
```
The returned Stream holds an open directory handle and is documented as requiring try-with-resources; it is neither assigned nor closed. This runs once per configured search path (≈40 paths) per `performDiscovery` call, in parallel on the executor.

**Impact.** `discoverAvroSchemas()` / `discoverFiles(ext)` leaks ~40 directory handles per invocation, and `createNotFoundException` calls `performDiscovery` on every cache miss for a missing file. A long-running Spark driver that repeatedly probes for optional schemas exhausts the process fd limit and then fails with `java.io.IOException: Too many open files` in completely unrelated code (socket accepts, shuffle files).

**Fix.** Wrap in try-with-resources: `try (Stream<Path> s = Files.walk(path, 2)) { s.filter(...).forEach(...); }`.

### `NP-005` — AvroSchemaLoader leaks an InputStream on every successful schema load

- **Location:** `src/main/java/io/github/pierce/AvroSchemaLoader.java:283`
- **Cluster:** quality | **Category:** resource-leak | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
InputStream is = FileFinder.findFile(schemaName);
if (is != null) {
    LOG.info("Schema {} found via FileFinder", schemaName);
    return new Schema.Parser().parse(is);
}
```
No close, no try-with-resources. Same pattern at line 368 in `loadFromClasspath` (leaked on success, on the `continue` in the catch, and when a later location is tried) and at line 428 in `discoverSchemasInClasspath`, where the stream is opened, logged about, and dropped without ever being read.

**Impact.** FileFinder is Strategy 1, so this leaks on essentially every schema load. Each leak is a BufferedInputStream over a file channel (or a JarFile entry stream). `loadFlattenedSchemas(list)` over a few hundred schemas, or a streaming job that reloads schemas per micro-batch, exhausts file descriptors; on Windows the underlying .avsc files also stay locked and cannot be replaced during a hot schema update.

**Fix.** `try (InputStream is = FileFinder.findFile(schemaName)) { return new Schema.Parser().parse(is); }` in all three places, and delete the no-op `discoverSchemasInClasspath` loop entirely since it discards the stream and adds nothing to `schemas`.

### `NP-006` — NDJSON stream reader is never closed — leaks a file handle per file processed

- **Location:** `src/main/groovy/io/github/pierce/JsonFlattener.groovy:1575`
- **Cluster:** quality | **Category:** resource-leak | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
public StreamResult fromNdjsonFile(Path path, InputOptions options) {
    InputStream is = Files.newInputStream(path);
    if (options.isGzipped()) is = new GZIPInputStream(is);
    return fromNdjsonStream(is, options.getCharset());
}
```
and `fromNdjsonStream` builds a `BufferedReader` handed to `Stream.generate(reader::readLine).takeWhile(nonNull)` with **no `onClose` handler registered**. Nothing in StreamResult (`toList`, `forEach`, `count`, `toNdjsonFile`) closes the reader or the stream. If the GZIPInputStream constructor throws on a corrupt header, the raw FileInputStream leaks too.

**Impact.** Batch-processing 10,000 NDJSON files with `JsonFlattener.create().stream().fromNdjsonFile(p).toList()` leaks 10,000 file descriptors and 10,000 8 KB buffers; the job dies with `Too many open files` well before finishing. Because `StreamResult` is not AutoCloseable there is no way for a correct caller to fix this from outside.

**Fix.** Register cleanup on the stream — `return new StreamResult(createLineStream(reader).onClose(() -> { try { reader.close(); } catch (IOException ignored) {} }), this)` — make StreamResult implement AutoCloseable delegating to `source.close()`, and close the source in the terminal operations (`toList`, `count`, `forEach`, `toNdjsonFile`). Guard the GZIP wrap so the raw stream is closed if the constructor throws.

### `NP-007` — searchClasspath opens and abandons an InputStream on every probe

- **Location:** `src/main/java/io/github/pierce/files/FileFinder.java:583`
- **Cluster:** quality | **Category:** resource-leak | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
URL resource = getClass().getResourceAsStream(location) != null
        ? getClass().getResource(location) : null;
```
The stream returned by `getResourceAsStream` is used only for a null test and is never assigned or closed. The loop runs over 6 candidate classpath locations for every cache miss.

**Impact.** When resources live inside a JAR, each abandoned stream holds a `java.util.zip.Inflater` plus a reference into the shared JarFile; these are only reclaimed by GC finalization, which is unreliable under load. Repeated lookups for missing files (the common case, since findFileHandle probes 6 locations before falling through) accumulate native inflater memory outside the heap and can pin the JAR against replacement.

**Fix.** Use `getClass().getResource(location)` alone for existence checking — it does not open a stream. If a stream test is genuinely needed, close it: `try (InputStream probe = getClass().getResourceAsStream(location)) { ... }`.

### `NP-008` — FileFinder starts non-daemon threads with no shutdown — JVM never exits

- **Location:** `src/main/java/io/github/pierce/files/FileFinder.java:332`
- **Cluster:** quality | **Category:** resource-leak | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
this.scheduledExecutor = Executors.newScheduledThreadPool(2);
...
scheduledExecutor.scheduleAtFixedRate(this::performMaintenance, 5, 5, TimeUnit.MINUTES);
```
`Executors.newScheduledThreadPool(int)` uses `defaultThreadFactory()`, which creates **non-daemon** threads (contrast the fixed pool above at line 326, which explicitly calls `t.setDaemon(true)`). Grep for `shutdown` in the file returns nothing — the singleton has no close/shutdown method and no JVM shutdown hook.

**Impact.** Any process that calls `FileFinder.findFile(...)` even once instantiates the singleton and permanently pins two non-daemon threads running a periodic task. A CLI tool, a `spark-submit` driver, or a JUnit fork will complete its work and then hang forever at exit instead of terminating; CI runs time out. It also prevents webapp classloader unloading on redeploy.

**Fix.** Pass a daemon thread factory to `newScheduledThreadPool` (matching the fixed pool), and add a public `static void shutdown()` that shuts both executors down and nulls INSTANCE, plus a `Runtime.getRuntime().addShutdownHook` as a backstop.

### `NP-009` — Cached flattened schema returns stale analytics metadata and is keyed by name only

- **Location:** `src/main/java/io/github/pierce/AvroSchemaFlattener.java:111`
- **Cluster:** quality | **Category:** correctness | **Effort:** medium
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
public Schema getFlattenedSchema(Schema schema) {
    String cacheKey = schema.getFullName() + ":" + includeArrayStatistics + ":" + includeNonTerminalArrays;
    return schemaCache.computeIfAbsent(cacheKey, k -> flattenSchema(schema));
}
```
On a cache hit the mapping function never runs, so `flattenSchema`'s side effects — populating `fieldMetadataList`, `recordDefinitions`, `arrayFieldNames`, `terminalArrayFieldNames`, `mapFieldPaths`, `schemaStats` — are skipped. Every accessor (`getTerminalArrayFieldNames()`, `getMapFieldPaths()`, `reconstructOriginalSchema()`, `exportToExcel()`) reads those instance fields.

**Impact.** Two failure modes. (a) On a fresh instance, a cache hit leaves `recordDefinitions` empty, so `reconstructOriginalSchema()` throws `IllegalStateException("No record definitions available")` even though flattening "succeeded". (b) `NexusPiercerSparkPipeline.loadSchema()` calls `getFlattenedSchema(originalSchema)` then immediately reads `schemaFlattener.getNonTerminalArrayFieldNames()` — on a cache hit those sets describe whatever schema this instance flattened *last*, so processJsonColumn drops the wrong columns from the output DataFrame. Separately, keying on `getFullName()` means two different revisions of `com.acme.User` (v1 and v2) collide and the second caller silently receives v1's flattened schema.

**Fix.** Key on `SchemaNormalization.parsingFingerprint64(schema)` (as AvroReconstructor already does) rather than the full name, and cache the derived metadata alongside the Schema in a single immutable result object returned by the method, instead of relying on mutated instance state.

### `NP-010` — AvroSchemaFlattener is Serializable for Spark but flattenSchema mutates unsynchronized instance collections

- **Location:** `src/main/java/io/github/pierce/AvroSchemaFlattener.java:116`
- **Cluster:** quality | **Category:** concurrency | **Effort:** large
- **Complexity:** N/A
  - → N/A

**Evidence.** The class `implements Serializable` and holds `HashSet`/`ArrayList`/`HashMap` fields (`arrayFieldNames`, `fieldMetadataList`, `typeTransformations`, `fieldHierarchyMap`, `recordDefinitions`, ...). `flattenSchema` begins by calling `.clear()` on all of them and then repopulates them during recursion. NexusPiercerSparkPipeline holds one `schemaFlattener` instance as a mutable field (line 48) and lazily initializes it in `initializeProcessors()` with a non-atomic `if (schemaFlattener == null)`.

**Impact.** Two threads calling `loadSchema()`/`getFlattenedSchemaNoCache()` on the same pipeline (e.g. two concurrent streaming queries, or a thread pool of Spark jobs sharing a pipeline) interleave clear+populate: one thread's `fieldMetadataList` entries are wiped mid-flatten, producing a flattened schema missing arbitrary fields, or `ConcurrentModificationException` from the ArrayList. Because the corruption is data-dependent it appears as intermittently missing output columns rather than a clean crash. The non-atomic lazy init can also construct two flatteners and publish one unsafely.

**Fix.** Make flattening produce an immutable result object (schema + metadata) with all state local to the call, so the class becomes stateless and genuinely thread-safe; or, minimally, synchronize flattenSchema and document the class as single-threaded and mark the collection fields `transient`.

### `NP-011` — collectMetrics references a `_error` column that FAIL_FAST/PERMISSIVE never create → AnalysisException

- **Location:** `src/main/java/io/github/pierce/spark/NexusPiercerSparkPipeline.java:921`
- **Cluster:** quality | **Category:** correctness | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** `processFlattenedMode` adds `_error` only inside `if (config.errorHandling == QUARANTINE || config.errorHandling == SKIP_MALFORMED)` (line 510); `processExplosionMode` has the same guard (line 769). But `processDataset` then runs `if (config.enableMetrics && mode == BATCH) collectMetrics(allProcessedRecords, metrics);` (line 463) and collectMetrics unconditionally does `.withColumn("_error_type", when(col("_error").isNull(), ...))`. `enableMetrics` defaults to `true` (line 141).

**Impact.** `NexusPiercerSparkPipeline.forBatch(spark).withSchema(s).withErrorHandling(ErrorHandling.FAIL_FAST).process(path)` throws `org.apache.spark.sql.AnalysisException: cannot resolve '_error' given input columns [...]` — the two documented error strategies FAIL_FAST and PERMISSIVE are completely unusable with default settings. Same for `ErrorHandling.PERMISSIVE`.

**Fix.** Guard collectMetrics: `if (!Arrays.asList(df.columns()).contains("_error")) { metrics.totalRecords = df.count(); metrics.successfulRecords = metrics.totalRecords; return; }` — or always add the `_error` column and let the filtering strategy decide what to do with it.

### `NP-012` — Spark UDF closure captures `this`, dragging a non-transient SparkSession into task serialization

- **Location:** `src/main/java/io/github/pierce/spark/NexusPiercerSparkPipeline.java:477`
- **Cluster:** quality | **Category:** correctness | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** `processFlattenedMode`'s UDF body calls `flattener.flattenAndConsolidateJson(json)` — an *instance field* read, so the lambda captures `this`. The enclosing class is `Serializable` with `private final SparkSession spark;` (line 42, not transient) and a `PipelineConfig` field. `processExplosionMode`'s UDF likewise reads `config.errorHandling` (line 744). Note that `processJsonColumn` deliberately does the opposite at line 573: `final JsonFlattenerConsolidator flattener = this.flattener;` — the author knew the pattern but did not apply it in the other two methods.

**Impact.** Spark's ClosureCleaner must serialize the whole pipeline object to every executor. Depending on Spark version and session state this surfaces as `org.apache.spark.SparkException: Task not serializable` at job submission, or — worse — succeeds and ships a SparkSession that resolves to a broken/other session on the executor, producing NPEs deep inside the UDF that are attributed to malformed input rather than to closure capture. `initializeProcessors()`'s non-atomic `if (flattener == null)` also races if two jobs are launched from the same pipeline.

**Fix.** Hoist every captured value into a local final before building the UDF (as processJsonColumn already does) — `final JsonFlattenerConsolidator f = this.flattener; final ErrorHandling eh = config.errorHandling;` — and mark `spark` transient. Better still, stop implementing Serializable on the pipeline class.

### `NP-013` — Explosion produces an unbounded cross-product of records → OOM on adversarial input

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:229`
- **Cluster:** quality | **Category:** security | **Effort:** small
- **Complexity:** O(∏ arraySize_i) records materialized
  - → O(min(∏ arraySize_i, cap))

**Evidence.** ```java
for (String explosionPath : explosionPaths) {
    List<Map<String, Object>> nextRecords = new ArrayList<>();
    for (Map<String, Object> record : currentRecords) {
        nextRecords.addAll(explodeFlattened(record, explosionPath));
    }
    currentRecords = nextRecords;
}
```
Each pass multiplies the record count by the array cardinality at that path. `maxArraySize` (default 1000) bounds each *individual* array but nothing bounds the product, and nothing bounds `results.size()`.

**Impact.** A single input document with three exploded paths of 1000 elements each yields 1000³ = 10⁹ LinkedHashMaps held in the driver/executor heap simultaneously — OutOfMemoryError from one record. Even two paths at 1000 (10⁶ maps, each holding every non-array field) will typically blow an executor. Because this happens inside a UDF, the failure kills the whole Spark task and retries replay the same poison record until the stage fails.

**Fix.** Add a `maxExplodedRecords` cap (checked after each path expansion) that either truncates with a warning or throws a typed exception, and short-circuit as soon as `currentRecords.size()` exceeds it rather than after materializing the next level.

### `NP-014` — Flattened-key collisions silently lose data — and the schema and the data disagree about which field wins

- **Location:** `src/main/groovy/io/github/pierce/MapFlattener.groovy:312`
- **Cluster:** quality | **Category:** correctness | **Effort:** medium
- **Complexity:** N/A
  - → N/A

**Evidence.** MapFlattener: `result.putAll(flattenValue(newKey, entry.getValue(), depth));` — `usedKeys`/`sanitizeKey` only dedupe *sibling* keys at one level, never keys produced by nesting, so a later nested expansion silently overwrites an earlier literal. AvroSchemaFlattener.addField does the reverse (line 694): `for (Field existing : fields) if (existing.name().equals(name)) { LOG.trace("Field {} already exists, skipping"); return; }` — first writer wins, and the drop is only visible at TRACE.

**Impact.** For `{"user_name":"literal","user":{"name":"nested"}}` MapFlattener emits `{user_name="nested"}` — "literal" is gone with no log line at any level. The matching Avro schema `{user_name: string, user: {name: string}}` flattens with addField keeping the *top-level* `user_name` and dropping the nested one. So the Spark StructType says column `user_name` is the top-level field while the row data actually carries the nested value: a silent, undetectable column/value mismatch in the warehouse. Neither side reports the collision above TRACE.

**Fix.** Detect collisions explicitly in both flatteners and apply one deterministic policy — either disambiguate (append `_2`, as sanitizeKey already does for siblings) or fail with a `DuplicateFlattenedFieldException` naming both source paths. At minimum raise the log level to WARN on both sides so the loss is observable.

### `NP-015` — BigInteger.longValue() silently wraps large integers in both flatteners

- **Location:** `src/main/groovy/io/github/pierce/MapFlattener.groovy:891`
- **Cluster:** quality | **Category:** correctness | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** MapFlattener.normalizePrimitive: `if (value instanceof BigInteger) { return ((BigInteger) value).longValue(); }` — `BigInteger.longValue()` is documented to return only the low-order 64 bits with no overflow signal. Identical bug in JsonFlattenerConsolidator.getNodeValue line 601: `return node.bigIntegerValue().longValue();`, and precision loss on the adjacent line 599: `node.decimalValue().doubleValue()`.

**Impact.** JSON `{"id": 18446744073709551615}` (a common unsigned-64 ID, e.g. a Snowflake/Twitter ID or a uint64 from a protobuf gateway) becomes `-1`. `{"id": 9223372036854775808}` becomes `-9223372036854775808`. The value is written to the warehouse as a plausible-looking number, so it corrupts joins and dedup keys downstream with no error, no warning, and no way to detect it after the fact. The BigDecimal→double path loses cents on high-precision monetary values by default (`preserveBigDecimalPrecision` defaults to false).

**Fix.** Use `bigInteger.longValueExact()` and catch ArithmeticException to fall back to the string form (preserving the exact value), mirroring what LongConverter already does correctly via `convertBigInteger`'s `bitLength() > 63` check. Default `preserveBigDecimalPrecision` to true, or at least emit a WARN when a conversion is lossy.

### `NP-016` — IntegerConverter silently truncates BigInteger/BigDecimal, bypassing its own overflow guards

- **Location:** `src/main/java/io/github/pierce/converter/IntegerConverter.java:61`
- **Cluster:** quality | **Category:** correctness | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
private Integer convertNumber(Number n) {
    if (n instanceof Long l)   return convertLong(l, n);
    if (n instanceof Double d) return convertDouble(d, n);
    if (n instanceof Float f)  return convertDouble(f.doubleValue(), n);
    // Short, Byte, etc. - safe to convert
    return n.intValue();
}
```
BigInteger and BigDecimal are Numbers and fall into the "safe to convert" branch. LongConverter.convertNumber handles both explicitly (`if (n instanceof BigDecimal bd) ... if (n instanceof BigInteger bi) ...`) — the two converters are inconsistent.

**Impact.** With `ConversionConfig.strict()` (which sets `allowNumericOverflow(false)` and `allowPrecisionLoss(false)` precisely to prevent this), converting `new BigInteger("4294967296")` to an int field yields `0` and converting `new BigDecimal("3.99")` yields `3` — both silently, with no exception. Jackson produces BigInteger for any integer literal exceeding long range and BigDecimal when USE_BIG_DECIMAL_FOR_FLOATS is on, so this is reachable from ordinary JSON input, and it defeats the entire purpose of strict mode.

**Fix.** Add `if (n instanceof BigInteger bi) { if (bi.bitLength() > 31 && !config.isAllowNumericOverflow()) throw conversionError(...); return bi.intValue(); }` and route BigDecimal through `convertDouble`/`intValueExact` so allowPrecisionLoss is honored — mirroring LongConverter.

### `NP-017` — java.sql.Timestamp branch is unreachable (Date matches first) — sub-millisecond precision silently lost

- **Location:** `src/main/java/io/github/pierce/converter/TimestampConverter.java:60`
- **Cluster:** quality | **Category:** correctness | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
// Handle java.util.Date
if (value instanceof Date date) {
    return date.getTime() * MICROS_PER_MILLI;
}

// Handle java.sql.Timestamp
if (value instanceof java.sql.Timestamp ts) { ... nanos ... }
```
`java.sql.Timestamp extends java.util.Date`, so the first branch always matches and the Timestamp branch is dead code. (The dead branch is itself wrong: `ts.getTime()/1000` truncates toward zero, so a pre-1970 Timestamp of -1500 ms yields -0.5 s instead of -1.5 s.)

**Impact.** Iceberg timestamps are microsecond-precision. A JDBC row with `Timestamp` nanos = 123_456_789 converts to `...123000` micros — the 456 microseconds are dropped on every row read from a database source. This is exactly the precision the dedicated branch was written to preserve, and the loss is silent. Additionally `date.getTime() * 1000L` overflows long for Dates beyond ~year 294247.

**Fix.** Reorder so `java.sql.Timestamp` is tested before `java.util.Date`, and fix the arithmetic to `Math.floorDiv(ts.getTime(), 1000L) * MICROS_PER_SECOND + ts.getNanos() / 1000` so pre-epoch values are correct. Use Math.multiplyExact for the Date path.

### `NP-019` — Converter caches keyed by System.identityHashCode return the wrong converter and never evict

- **Location:** `src/main/java/io/github/pierce/converter/SchemaBasedMapConverter.java:334`
- **Cluster:** quality | **Category:** correctness | **Effort:** medium
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
String key = "iceberg:" + System.identityHashCode(icebergSchema);
return CONVERTER_CACHE.computeIfAbsent(key, k -> forIceberg(icebergSchema));
```
Same pattern at lines 343 and 352 here, at AvroSchemaConverter.java:101 (`int key = System.identityHashCode(schema)`), and IcebergSchemaConverter.java:86. identityHashCode is neither unique among live objects nor stable across object lifetimes — the JVM reuses values freely once an object is collected. `CONVERTER_CACHE` is a `static ConcurrentHashMap` with no eviction and no size bound.

**Impact.** Parse a schema per request/per file (the normal pattern with `new Schema.Parser().parse(...)`); schemaA is cached and then garbage-collected; a later, structurally different schemaB is allocated and happens to receive the same identity hash → `cached(schemaB)` returns schemaA's converter. Every field is then converted against the wrong types and any field absent from schemaA is silently dropped by `findMatchingField`'s `continue`. Separately, the cache strongly references every converter (and through it every schema) forever, so a driver that sees many distinct schemas leaks unboundedly. `AvroSchemaConverter.cached(schema, config)` additionally ignores `config` on a hit, so a later `strict()` call silently reuses a `lenient()` converter.

**Fix.** Key on a content fingerprint — `SchemaNormalization.parsingFingerprint64(avroSchema)` for Avro, `schema.schemaId()`/`toString()` for Iceberg — combined with a config identity, and back the cache with a bounded, weak-keyed cache (Guava `CacheBuilder.weakKeys().maximumSize(n)`) instead of an unbounded static ConcurrentHashMap.

### `NP-020` — Avro union resolution is "first branch that doesn't throw", and exceptions are swallowed

- **Location:** `src/main/java/io/github/pierce/converter/SchemaBasedMapConverter.java:966`
- **Cluster:** quality | **Category:** correctness | **Effort:** medium
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
protected Object doConvert(Object value) {
    for (TypeConverter<Object, Object> converter : branchConverters) {
        try { return converter.convert(value); }
        catch (Exception ignored) { /* Try next branch */ }
    }
    throw conversionError(value, "Value does not match any union branch");
}
```
StringConverter accepts essentially every input, so if `string` appears anywhere before the intended branch, it always wins.

**Impact.** For the union `["string","long"]`, the value `12345L` is converted to the String "12345" instead of a long — the union branch is chosen by declaration order rather than by the runtime type of the value. Writing that to Avro/Iceberg with a long-typed branch fails downstream, or (with a string branch present) silently changes the column type of the record. The blanket `catch (Exception ignored)` also swallows genuine bugs — an NPE or OOM-adjacent failure inside a branch converter is indistinguishable from "branch didn't match".

**Fix.** Resolve the branch by inspecting the runtime type of the value first (`GenericData.get().resolveUnion(unionSchema, value)` semantics: match Integer→int, Long→long, CharSequence→string, ByteBuffer→bytes, Map→record/map, ...) and only fall back to trial conversion for genuinely ambiguous cases. Catch only TypeConversionException, never Exception, and collect the per-branch failures into the final error message.

### `TA-003` — All 26 jqwik property-based tests never execute — jqwik-engine is absent from the POM and the filename does not match surefire includes

- **Location:** `src/test/java/io/github/pierce/converter/TypeConverterProperties.java:27`
- **Cluster:** tests | **Category:** dead-test-infrastructure | **Effort:** trivial
- **Complexity:** 26 @Property methods, 0 executed
  - → 26 properties × default 1000 tries each

**Evidence.** TypeConverterProperties.java declares 26 `@Property` methods (grep -c '@Property' = 26) with `import net.jqwik.api.*`. TWO independent blockers, both confirmed: (1) pom.xml declares ONLY `net.jqwik:jqwik-api` (lines 562-566) and in **compile scope, not test scope** — the `jqwik-engine` artifact that registers the `net.jqwik.engine.JqwikTestEngine` TestEngine on the JUnit Platform is not a dependency at all, so the platform has no engine that can discover `@Property`. (2) The surefire includes at pom.xml:1092-1100 are `**/*Test.java`, `**/*Tests.java`, `**/*TestCase.java` (+3 inert .groovy patterns) — `TypeConverterProperties` matches none of them. Empirical confirmation: target/surefire-reports contains 126 report XMLs from a run at 2026-08-09 15:11-15:14, and NO report exists for TypeConverterProperties (verified programmatically). 737 tests ran; none were properties.

**Impact.** 392 lines of the only property-based testing in the repo are dead. Round-trip properties that would catch converter edge cases across the input space (e.g. `integerRoundTrip`, `integerStringParsingPreservesValue`, `longRoundTrip`, `longFromIntegerPreservesValue`) never run, while the classes they target sit at 43.9% (LongConverter) and 66.5% (IntegerConverter) instruction coverage. jqwik-api at compile scope also leaks a test-only dependency into the published POM's transitive compile classpath for every downstream consumer.

**Fix.** Replace the jqwik-api dependency with the `net.jqwik:jqwik` aggregate at `<scope>test</scope>` (it pulls jqwik-api + jqwik-engine + jqwik-time), and rename the file to `TypeConverterPropertyTest.java` (or add `**/*Properties.java` to the surefire `<includes>`). Then verify a surefire report appears for it and the properties actually shrink-and-report. This is the natural home for the flatten→reconstruct round-trip property once TA-001 lands.

### `TA-004` — SchemaCompatibilityIntegrationTest (35 tests, 2488 lines) was commented out by a botched sed-style org.json→Jackson migration and does not compile

- **Location:** `src/test/java/io/github/pierce/integrationTests/SchemaCompatibilityIntegrationTest.java:460`
- **Cluster:** tests | **Category:** broken-migration | **Effort:** small
- **Complexity:** 35 tests, 0 executed, does not compile
  - → 35 tests compiling and executing under failsafe

**Evidence.** File is 100% comment-prefixed, 35 commented @Test. Git history pinpoints the cause: it was live through f4802d8 and became `//package io.github.pierce.integrationTe...` at commit b1023d0 ('updated to not use org.json dependency', 2025-12-07), whose diff touched 4970 lines of this file. The migration was applied to already-commented text and was mechanical and wrong. I uncommented it and compiled with JDK 17 javac against the full test classpath → **12 compile errors**, all the same two substitution bugs: `new JSONObject()` was replaced by `MAPPER.readTree()` with no arguments (lines 489, 773, 989, 1723 — `error: method readTree in class ObjectMapper cannot be applied to given types; required: JsonParser, found: no arguments`), and `JSONObject.put(k,v)` was left as `.put()` on a `JsonNode` (lines 460, 490, 774, 775, 776, 991, 1531 — `error: cannot find symbol: method put(String,List<String>) location: variable json of type JsonNode`), plus line 648 `flatObj.keySet()` on a `JsonNode`. Separately: this is the ONLY file matching the failsafe includes (`**/*IntegrationTest.java`, pom.xml:1124), and surefire explicitly excludes it (pom.xml:1104) — so it runs under `mvn verify`, never under `mvn test`.

**Impact.** The single largest test class in the repo (2488 lines, 35 tests) is the only end-to-end contract test asserting that JsonFlattenerConsolidator output is loadable by AvroSchemaFlattener-derived Spark schemas — the core compatibility promise of the library. Its method list covers null handling, unicode, numeric edge cases, arrays at/beyond maxArraySize, nesting-depth limits, matrix denotors, map handling, and 12 statistics-on/off comparisons. All of it is dark, and because it is failsafe-bound, even reviving it will not surface in a plain `mvn test`.

**Fix.** Revivable-with-fixes, ~12 mechanical sites: `MAPPER.readTree()` → `MAPPER.createObjectNode()` with the variable typed `ObjectNode`; build arrays with `MAPPER.createArrayNode()` + `json.set(name, arrayNode)` instead of `.put(name, List)`; `flatObj.keySet()` → `flatObj.fieldNames()` (or `properties()` on Jackson 2.18). Verify with `javac` before uncommenting. Then decide deliberately whether it should be `*IntegrationTest` (failsafe/`mvn verify`) or renamed to run in `mvn test` — today the CI signal depends on which lifecycle phase is invoked.

### `TA-005` — Surefire's three `**/*.groovy` include patterns and two `.groovy` exclude patterns are silently inert — Groovy tests run only by accident, and any Spock `*Spec.groovy` would never run

- **Location:** `pom.xml:1097`
- **Cluster:** tests | **Category:** build-config | **Effort:** trivial
- **Complexity:** 5 of 11 surefire patterns are no-ops
  - → patterns match what surefire actually resolves

**Evidence.** pom.xml:1092-1106 declares includes `**/*Test.java`, `**/*Tests.java`, `**/*TestCase.java`, `**/*Test.groovy`, `**/*Tests.groovy`, `**/*Spec.groovy` and excludes `**/*IT.java`, `**/*IntegrationTest.java`, `**/*IT.groovy`, `**/*IntegrationTest.groovy`. Surefire only understands two extensions: I extracted `org/apache/maven/surefire/api/testset/ResolvedTest.class` from surefire-api-3.2.2.jar and `javap -c` shows its only extension constants are `.java` (converted to) `.class` — there is no `.groovy` handling anywhere in maven-surefire-common-3.2.2 or surefire-api-3.2.2. A pattern ending in `.groovy` is matched literally against `.class` file paths and can never hit. ANSWER TO THE DIRECT QUESTION: AsymmetricDoublyNestedArrayTest.groovy IS being picked up — target/surefire-reports/TEST-io.github.pierce.AsymmetricDoublyNestedArrayTest.xml exists with 1 test, 0 failures — but only because it declares `package io.github.pierce` (line 1), compiles to target/test-classes/io/github/pierce/AsymmetricDoublyNestedArrayTest.class, and is caught by the `**/*Test.java`→`**/*Test.class` conversion. All 13 Groovy test classes run for that incidental reason, not because of the `.groovy` patterns.

**Impact.** Latent, not yet biting: the project declares `org.spockframework:spock-core:2.3-groovy-4.0` (pom.xml:395-400) plus byte-buddy and objenesis purely for Spock mocking, yet has ZERO `*Spec.groovy` files. The moment anyone writes one — the obvious thing to do given the declared dependency and the `**/*Spec.groovy` include — it will compile, produce no report, and silently never execute, with a green build. Symmetrically, a `FooIntegrationTest.groovy` would be excluded correctly only via the `.java`-derived pattern, so the intent is not what the config says.

**Fix.** Delete the three `.groovy` includes and two `.groovy` excludes (they are noise implying capability that does not exist), or replace them with the `.class` forms surefire actually honours: `**/*Spec.class`. Add a smoke Spock spec and confirm a surefire report is produced before trusting the Spock dependency. Also drop spock-core/byte-buddy/objenesis if Spock is not going to be used — three unused test dependencies.

### `TA-006` — No mutation testing, no fuzzing, and the one round-trip identity invariant that IS tested is example-based only with 3 permanently-disabled known-broken cases

- **Location:** `src/test/groovy/AvroFlattenReconstructStressTest.groovy:1099`
- **Cluster:** tests | **Category:** test-strategy | **Effort:** large
- **Complexity:** 46 example-based round-trip tests, 3 skipped, 0 properties, 0 mutants
  - → generative round-trip coverage + mutation score as the quality gate

**Evidence.** Zero occurrences of pitest/org.pitest/jazzer/jqf/fuzz in pom.xml or docs/*.md. Property-based testing exists but never runs (TA-003). The Avro round-trip IS genuinely covered: AvroFlattenReconstructStressTest.groovy has 46 @Test methods that all delegate to `private void verifyRoundTrip(Map original, Schema schema, String testName)` (lines 56-83) which does MapFlattener.flatten → AvroReconstructor.reconstructToMap → `verifyReconstruction(...).isPerfect()` — 52 call sites of verifyRoundTrip/verifyReconstruction in that file, 7 in AvroReconstructorTest, 7 in EdgeCaseAvroReconstructionTest, 4 in DeepNestingDiagnosticTest, 3 in NestedArrayDiagnosticTest, 1 in ComplexAvroReconstructionTest. But exactly 3 of these are @Disabled, all with the SAME product-defect message: line 1099 `@Disabled("KNOWN LIMITATION: Map keys containing underscore separator collide with flattened path structure")` (testMapSpecialKeys), line 1260 `@Disabled("KNOWN LIMITATION: Underscore separator cannot distinguish 'field_name' field from nested 'field.name' path")` (testUnderscoreFieldNames), line 1280 `@Disabled("KNOWN LIMITATION: 'user_name' field collides with 'user.name' nested path when using underscore separator")` (testFieldNameCollision). Confirmed in the live run: surefire reports show exactly 3 skipped, all in AvroFlattenReconstructStressTest$EdgeCaseTests and $MapTests.

**Impact.** Three @Disabled tests encode a real, unfixed data-corruption hazard in the library's core separator design: any input record with an underscore in a field name or map key round-trips incorrectly. This is the highest-consequence bug class for a flattener, and it has been converted from a failing test into a silent skip. Meanwhile the invariant is only probed by 46 hand-written examples — with jqwik dead and no PIT, there is no evidence the 636 active tests actually kill mutants; a 60.26% instruction coverage number with 1375 assertions says nothing about assertion strength (see TA-008 for a concrete tautological test that would survive every mutant).

**Fix.** (a) Treat the 3 @Disabled cases as open product bugs, not test debt — either escape the separator on flatten (e.g. double the separator inside literal field names) or reject/validate colliding names, then re-enable. (b) Add org.pitest:pitest-maven under the existing `quality` profile scoped to io.github.pierce.* with a mutation threshold, to measure assertion strength rather than line coverage. (c) Once TA-003 and TA-001 land, express round-trip as a jqwik property over generated Avro schemas + records rather than 46 handwritten examples.


## MEDIUM

### `NP-015` — Four unbounded caches with no eviction, one of which mistakes initial capacity for a size bound

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:209`
- **Cluster:** arch | **Category:** resource-leak | **Effort:** small

**Evidence.** AvroReconstructor.groovy:209 `this.schemaCache = new ConcurrentHashMap<>(DEFAULT_MAX_CACHE_SIZE);` where DEFAULT_MAX_CACHE_SIZE = 100 (line 82) — this is ConcurrentHashMap's INITIAL CAPACITY argument, not a maximum; the name and the constant make the intent unmistakable and the effect is an unbounded cache. getOrBuildSchemaCacheEntry (line 636) computeIfAbsent's into it with no eviction path, and SchemaCacheEntry even records `createdAt` (line 125) that nothing ever reads. The other three: AvroSchemaFlattener.java:38 static schemaCache, CreateSparkStructFromAvroSchema.java:24 static structTypeCache, AvroSchemaLoader.java:41-42 two static caches, and NexusPiercerSparkPipeline.java:40 static SCHEMA_CACHE — all plain ConcurrentHashMaps with only manual clearCache() escape hatches. FileFinder is the sole component that uses Guava CacheBuilder with real bounds (FileFinder.java:335-344).

**Impact.** In a long-lived driver or a multi-tenant Spark application processing many distinct schemas — exactly the streaming/Glue use case the Javadoc targets — these grow without limit. Each entry holds a full parsed Avro Schema graph plus, for AvroReconstructor, an entire SchemaPathTrie. The `createdAt` field is evidence that TTL eviction was intended and never implemented.

**Fix.** Standardise on Guava CacheBuilder (already a dependency) or Caffeine with `maximumSize` and `expireAfterAccess` for all five caches, matching what FileFinder already does correctly. Delete the unread createdAt field once real eviction exists. Better still, per NP-003, make the cached values immutable result objects so caching is a pure memoization concern that can be factored into one shared `SchemaCache` component.

### `NP-016` — Three ThreadLocals used as hidden parameter channels, none of them removed

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:36`
- **Cluster:** arch | **Category:** state-management | **Effort:** small

**Evidence.** JsonFlattenerConsolidator.java:36 `private static final ThreadLocal<Set<String>> arrayFieldsThreadLocal` — written during flattenJson (lines 428, 454, 462, 566, 574) and read during processGroupedValues (line 641) to decide whether a key was 'tracked as an array'. It is `.clear()`ed at line 110 but never `.remove()`d. MapFlattener.groovy:120 `private static final ThreadLocal<FlattenContext> CONTEXT` (this one does remove() correctly at line 251). GAvroSchemaFlattener.groovy:49 `ARRAY_PARSE_CACHE`, whose cache key is only the field name (line 667), so within a single applyTypes call each field is visited exactly once and the cache can never hit — it is dead code that still costs a ThreadLocal lookup per field.

**Impact.** The consolidation result depends on invisible thread state rather than on arguments, so consolidateFlattened is not a pure function of its input and cannot be unit-tested or reused in isolation — this is the root reason flattening and consolidation cannot be separated into composable stages. In Spark, executor task threads are pooled and long-lived, so the never-removed ThreadLocal in JsonFlattenerConsolidator pins a HashSet per thread for the JVM lifetime and, more importantly, retains a reference to the library's classloader in shared-JVM deployments.

**Fix.** Replace all three with an explicit context object threaded through the call chain — `FlattenContext { Set<String> arrayFields; Deque<Integer> visited; }` passed as a parameter. This is the same change that makes flatten/consolidate separable stages and unblocks the strategy decomposition in NP-001. Delete GAvroSchemaFlattener's ARRAY_PARSE_CACHE entirely, since it provably never hits.

### `NP-017` — poi-ooxml, iceberg and a property-testing framework are compile-scope dependencies of a published Spark library

- **Location:** `pom.xml:467`
- **Cluster:** arch | **Category:** dependencies | **Effort:** medium

**Evidence.** pom.xml:467 declares org.apache.poi:poi-ooxml at compile scope, used solely by AvroSchemaFlattener.exportToExcel (line 417) and its ~15 sheet/style helpers. pom.xml:562 declares net.jqwik:jqwik-api — a property-based TESTING framework — at compile scope with no <scope>test</scope>. pom.xml:570-585 declares iceberg-core, iceberg-api and iceberg-common at compile scope for the orphaned converter package (NP-005). pom.xml:551 declares Guava at compile scope, used only by FileFinder's LoadingCache. Seven of the ten Excel sheet builders that exportToExcel invokes are empty stubs: createFieldLineageSheet (920), createComplexityAnalysisSheet (924), createReconstructionMetadataSheet (928), createDataFlowAnalysisSheet (932), createFieldCatalogSheet (936), createTypeTransformationsSheet (940), createNestingAnalysisSheet (944) — all with a comment body and no code.

**Impact.** Every consumer inherits POI (~12MB with transitives), three Iceberg artifacts, and a test framework on their runtime classpath. Guava is the classic Spark classpath-conflict source since Spark ships its own shaded and unshaded Guava at different versions across distributions. And the feature justifying POI produces a workbook whose ten sheets are seven blanks — exportToExcel silently writes an almost-empty file while logging `"PhD-level schema analysis exported to: {}"` at INFO (line 441).

**Fix.** Move jqwik-api to test scope (a one-word fix). Extract the Excel reporting into a separate optional module (nexus-piercer-report) or mark poi-ooxml <optional>true</optional> and guard exportToExcel behind a ClassNotFoundException check — either way it must not be a transitive runtime dependency of the Spark path. Delete or implement the seven stub sheet methods; a method that is called and does nothing while the caller logs success is worse than a missing feature. Drop the Iceberg trio with the converter package (NP-005). Replace Guava's LoadingCache with Caffeine, or shade Guava in the release profile (maven-shade-plugin is already configured at pom.xml:1488 but relocates nothing).

### `NP-018` — AvroSchemaFlattener conflates schema flattening, reconstruction metadata, statistics and Excel reporting in one class

- **Location:** `src/main/java/io/github/pierce/AvroSchemaFlattener.java:33`
- **Cluster:** arch | **Category:** god-class | **Effort:** medium
- **Complexity:** findFieldMetadata O(n) per lookup, O(arrays x fields) in report generation
  - → O(1) map lookup

**Evidence.** 1,121 lines carrying four unrelated jobs: schema traversal (flattenSchema, processFieldRecursively, isTerminalArrayType, addField — lines 116-360); reconstruction metadata (collectRecordDefinitions, reconstructOriginalSchema, reconstructRecord, parseSchemaFromString — 159-414); analytics scoring (calculateComplexityScore, getComplexityLevel, getComplexityRecommendation, getProcessingEfficiencyAssessment, getArrayComplexityImpact — 791-838); and Excel/POI presentation (exportToExcel plus ~20 sheet and CellStyle builders — 417-958). It carries eight mutable accumulator collections (lines 44-56) that processFieldRecursively writes to as a side effect, plus five public static value classes. findFieldMetadata (840) and findArrayDefinition (847) do O(n) stream scans over the accumulator lists, and are called once per array field inside createArraySection's loop (line 650), making that O(arrays x fields).

**Impact.** Presentation code (POI CellStyle construction) sits in the same class as the core schema algorithm, forcing POI onto every consumer (NP-017) and making the flattening logic untestable without dragging in a workbook. The accumulator-as-side-effect design is precisely what creates the stale-metadata bug in NP-003. The 'complexity score' heuristics are opinion encoded as library behaviour with hardcoded thresholds (score < 10 = Low, < 25 = Medium) and no way to configure them.

**Fix.** Split into `AvroSchemaFlattener` (pure: Schema -> FlattenedSchemaResult, no mutable fields), `SchemaAnalytics` (consumes the result, produces metrics), and `SchemaReportWriter` in an optional module (consumes analytics, writes Excel/JSON/Markdown). Replace findFieldMetadata/findArrayDefinition linear scans with a Map<String,FieldMetadata> built once during traversal. This split is a prerequisite for NP-003's fix and removes the POI dependency from the core path in NP-017.

### `NP-019` — GAvroSchemaFlattener documents itself as non-recursive and stack-overflow-safe while recursing on the array path

- **Location:** `src/main/groovy/io/github/pierce/GAvroSchemaFlattener.groovy:369`
- **Cluster:** arch | **Category:** correctness | **Effort:** small

**Evidence.** The class Javadoc (line 18) states 'Memory efficiency is critical (non-recursive traversal)' and flattenSchema's Javadoc (line 232) states 'This method uses iterative traversal to avoid stack overflow on deeply nested schemas'. flattenSchema itself (line 229) is indeed iterative with an explicit ArrayDeque. But the moment it encounters an array of records it delegates to flattenSchemaForArrayElement (line 369), which recurses into itself at lines 399, 422 and is called again from lines 302, 323 — an unbounded mutual recursion over nested records inside arrays, bounded only by config.maxDepth (default 50). Line 491 `private boolean isNullable(Schema)` is dead: the only reference is the comment at line 354 explaining it was replaced by node.nullable.

**Impact.** The one traversal path most likely to be deep (arrays of records containing arrays of records) is exactly the recursive one, so the safety property the class advertises does not hold where it matters. A consumer trusting the Javadoc and raising maxDepth to handle a deep schema will get a StackOverflowError instead of the promised graceful behaviour. The dead isNullable is a leftover from an incomplete fix and a trap for the next reader.

**Fix.** Either finish the iterative conversion by pushing array-element frames onto the same ArrayDeque used by flattenSchema, or correct the Javadoc to state the actual guarantee (iterative for records, recursive with a depth cap for array elements). Delete the dead isNullable(Schema) method. Add a test with a 60-deep array-of-record chain asserting a clean exception rather than StackOverflowError.

### `NP-020` — Four unrelated exception hierarchies and no library base type; declared checked exceptions that are never thrown

- **Location:** `src/main/java/io/github/pierce/AvroSchemaFlattener.java:94`
- **Cluster:** arch | **Category:** error-handling | **Effort:** medium

**Evidence.** Four unrelated exception types with no common ancestor: AvroReconstructor.ReconstructionException extends RuntimeException (line 2975), JsonFlattener.JsonFlattenException extends RuntimeException (line 1946), converter.SchemaConversionException extends RuntimeException, and FileFinder.FileFinderException extends FileNotFoundException (line 270). Alongside them, 26 bare `throw new RuntimeException` and 44 bare IllegalState/IllegalArgumentException across src/main, while JsonFlattenerConsolidator throws nothing at all and returns `"{}"` instead. AvroSchemaFlattener.getFlattenedSchema(String) at line 94 declares `throws IOException` but the IOException is caught at line 101 and rewrapped as RuntimeException inside the computeIfAbsent lambda, so the declared checked exception can never be thrown — and the InputStream from FileFinder.findFile at line 98 is never closed.

**Impact.** A consumer cannot write `catch (NexusPiercerException e)`; they must enumerate four unrelated types plus RuntimeException and hope. The pipeline's ErrorHandling enum (FAIL_FAST / SKIP_MALFORMED / QUARANTINE / PERMISSIVE) is meaningless for failures that never surface as exceptions in the first place. The phantom `throws IOException` forces every caller into a try/catch that can never fire while the real failure arrives as an unchecked RuntimeException they did not anticipate. The unclosed InputStream leaks a file handle per cache miss.

**Fix.** Define `NexusPiercerException extends RuntimeException` as the single root, with FlattenException, ReconstructionException, SchemaException and FileResolutionException beneath it, each carrying the field path or file name as structured data rather than only in the message. Every library throw goes through one of them. Remove the phantom `throws IOException` and wrap the FileFinder stream in try-with-resources. Document the exception contract in the api/ package-info from NP-010.

### `OSS-09` — docs/API_SURFACE.md misstates processStream's return type and the array-statistics default

- **Location:** `docs/API_SURFACE.md:43`
- **Cluster:** hygiene | **Category:** documentation-accuracy | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** API_SURFACE.md line 43 declares `DataStreamWriter processStream(String source, Map<String, String> options)`; NexusPiercerSparkPipeline.java line 382 is `public ProcessingResult processStream(String source, Map<String, String> options)`. API_SURFACE.md line 27 gives `enableArrayStatistics()` a default of 'disabled'; PipelineConfig line 130 is `private boolean includeArrayStatistics = true;` — and the Spark README's own config table (line 180) correctly says the default is `true`.

**Impact.** The return-type error breaks any code written against the API reference. The default-value error is worse in practice because it is silent: a user who believes statistics are off by default will get unexpected _count/_distinct_count/_min_length/_max_length/_avg_length/_type columns in every output schema, changing downstream Parquet/Iceberg layouts. The two documents also disagree with each other, so there is no authoritative answer.

**Fix.** Correct both entries in docs/API_SURFACE.md against NexusPiercerSparkPipeline.java, and designate one document as the API source of truth (ideally generate it from Javadoc) so the README and API_SURFACE cannot diverge.

### `OSS-10` — JsonFlattenerConsolidator README advertises an `_avg` array statistic that the code never emits

- **Location:** `JsonFlattenerConsolidator README.md:57`
- **Cluster:** hygiene | **Category:** documentation-accuracy | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** The README's Consolidation example (line 57) shows `"orders_amount_avg": 150.0` and the Understanding-the-Output section (line 215) shows `"user_orders_amount_avg": 150.0`. JsonFlattenerConsolidator.java emits only six suffixes — `_count`, `_distinct_count`, `_min_length`, `_max_length`, `_avg_length`, `_type` (lines 663-664, 684-690, 731-736). No numeric mean is computed anywhere. The README's own 'Custom Statistics' list (lines 349-353) correctly omits `_avg`, contradicting its two examples.

**Impact.** The README's two most prominent output examples show a column that will never appear. Anyone writing a Spark schema, a dbt model, or a downstream SELECT against `*_avg` gets a missing-column failure at runtime. `_avg_length` is a string-length mean, not a value mean, so the confusion is semantic as well as syntactic.

**Fix.** Replace `orders_amount_avg` / `user_orders_amount_avg` in the two examples with the statistics actually produced, and add a note that `_avg_length` measures string length rather than numeric value. If a true numeric mean is wanted, file it as a feature rather than documenting it as existing.

### `OSS-11` — PROJECT_OVERVIEW documents a NexusPiercerSparkPipeline.create(...).build() builder that does not exist

- **Location:** `docs/PROJECT_OVERVIEW.md:169`
- **Cluster:** hygiene | **Category:** documentation-accuracy | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** docs/PROJECT_OVERVIEW.md line 169 lists `NexusPiercerSparkPipeline.create(spark).withSchema("path").build()` under 'Discovered Patterns / Builder Pattern'. NexusPiercerSparkPipeline.java has no `create` factory and no `build()` method — the only entry points are `forBatch(SparkSession)` (line 234) and `forStreaming(SparkSession)` (line 241), and the fluent chain terminates in `process(...)` / `processStream(...)` / `processDataset(...)`, not `build()`. (By contrast MapFlattener.builder() and AvroReconstructor.builder() in the same list are real.)

**Impact.** The one Spark example in the architecture overview is uncompilable, and it mischaracterises the pipeline's lifecycle: there is no separate build step, so a reader may assume configuration is frozen at build() when in fact the mutable config object is read at process() time.

**Fix.** Replace the line with `NexusPiercerSparkPipeline.forBatch(spark).withSchema("path").process("input/*.json")` and note that the pipeline is a mutable fluent configurator rather than a classic immutable builder.

### `OSS-12` — Spark README Performance Tuning snippet passes a String to withRepartition(int)

- **Location:** `NexusPiercer Spark Pipeline README.md:323`
- **Cluster:** hygiene | **Category:** documentation-accuracy | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** README line 323: `.withRepartition(spark.conf.get("spark.sql.shuffle.partitions"))`. NexusPiercerSparkPipeline.java line 319 is `public NexusPiercerSparkPipeline withRepartition(int partitions)`, and `RuntimeConfig.get(String)` returns `String`. The snippet cannot compile.

**Impact.** The Partitioning subsection of Performance Tuning — the part users are most likely to copy verbatim when tuning a slow job — is broken. It also appears immediately after a correct `clearSchemaCache()` example, so nothing signals which snippets were ever compiled.

**Fix.** Change to `.withRepartition(Integer.parseInt(spark.conf().get("spark.sql.shuffle.partitions")))`, or add a `withRepartition(String)` overload if the string form is intended. More durably, extract README snippets into a compiled examples source set that CI builds.

### `OSS-13` — docs/MODULE_INDEX.md line counts are wrong and contradict docs/PROJECT_OVERVIEW.md by up to 7x

- **Location:** `docs/MODULE_INDEX.md:21`
- **Cluster:** hygiene | **Category:** documentation-consistency | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** MODULE_INDEX.md claims JsonFlattenerConsolidator 923 lines (actual 820), AvroSchemaFlattener 1122 (actual 1121), NexusPiercerSparkPipeline 948 (actual 975), IcebergSchemaConverter 409 (actual 408), AvroReconstructor '~400' (actual 2,979) and JsonFlattener '~500' (actual 2,004). PROJECT_OVERVIEW.md lines 151 and 154 independently state JsonFlattener is 2,005 lines and AvroReconstructor is 2,980 — directly contradicting MODULE_INDEX for the same two files. MODULE_INDEX also omits SchemaBasedMapConverter.java (1,377 lines, added in commit 81305b2).

**Impact.** The module index is the map a new contributor reads first, and it understates the two largest and most complex files in the codebase by roughly 7x and 4x. Anyone sizing a refactor of AvroReconstructor from these numbers will plan for the wrong order of magnitude. The internal contradiction between two docs written in the same session shows the corpus is hand-maintained and unverified.

**Fix.** Regenerate the table mechanically (a `wc -l` script or `cloc` run committed alongside) rather than by hand, add the missing converter classes, reconcile against PROJECT_OVERVIEW, and drop the 'Explored?' column — it tracks an authoring session, not a property of the code.

### `OSS-14` — Four test packages violate Java lowercase package naming convention

- **Location:** `src/test/java/io/github/pierce/FlattenConsolidatorTests/JsonFlattenerConsolidatorTest.java:1`
- **Cluster:** hygiene | **Category:** package-naming | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** `git ls-files` shows test sources under `io/github/pierce/FlattenConsolidatorTests/` (5 files), `io/github/pierce/avroTesting/` (6 files), `io/github/pierce/fileFinderTests/` (1 file), and `io/github/pierce/integrationTests/` (1 file). JLS 6.1 and Oracle convention require all-lowercase package components; capitalised and camelCase segments are reserved-by-convention for types. Only `io/github/pierce/converter/` and `io/github/pierce/spark/` follow the convention.

**Impact.** Capitalised package segments are ambiguous with class names to both readers and tooling, and they are case-sensitivity hazards: on Windows and macOS (case-insensitive filesystems) `avroTesting` and `avrotesting` collide, while on Linux CI they do not — a class of failure that only surfaces after CI is introduced. It also signals to contributors that convention is not enforced, which is consistent with Checkstyle being skipped by default (OSS-05).

**Fix.** Rename to `io.github.pierce.flattenconsolidator`, `io.github.pierce.avro`, `io.github.pierce.filefinder`, and `io.github.pierce.integration` (single git mv per package plus package-declaration updates; no production code imports these). Enable the Checkstyle PackageName rule so regressions fail the build.

### `OSS-15` — Groovy test sources sit flat in src/test/groovy while declaring three different packages; two are in the default package

- **Location:** `src/test/groovy/AvroReconstructorTest.groovy:1`
- **Cluster:** hygiene | **Category:** source-layout | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** All 17 files are directly in src/test/groovy/ with no directory nesting, yet they declare `package io.github.pierce` (10 files), `package io.github.pierce.avro` (AvroSchemaFlattenerAlignmentTest, AvroSchemaFlattenerTest), and `package io.github.pierce.examples` (MapFlattenerExamples, StreamingProcessorExample). AvroReconstructorTest.groovy and JsonReconstructorTest.groovy declare no package at all and therefore compile into the default package. Declarations are also inconsistently terminated — some with a semicolon, some without.

**Impact.** The directory layout does not mirror the package structure, so IDE navigation, package-scoped test helpers, and any tooling that walks source roots by path all misbehave. Default-package classes cannot be imported by any packaged class, cannot be referenced from Java tests, and are a known hazard under Surefire/Spock discovery. The mismatch also means the Java and Groovy test trees follow entirely different conventions within one repo.

**Fix.** Move each file into a directory matching its declared package (src/test/groovy/io/github/pierce/..., .../avro/, .../examples/), assign the two default-package files to io.github.pierce, and normalise semicolon usage. Note AvroReconstructorTest and JsonReconstructorTest may be non-functional today — verify they actually execute under Surefire before or after the move.

### `OSS-16` — JsonReconstructor.groovy is 1,293 lines of entirely commented-out code shipped in main sources

- **Location:** `src/main/groovy/io/github/pierce/JsonReconstructor.groovy:1`
- **Cluster:** hygiene | **Category:** dead-code | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** The file is 1,293 lines and every line matches `^//`, beginning with `//package io.github.pierce;` — so it has no package declaration and contributes no class. It is tracked under src/main/groovy and covered by the .reuse Apache-2.0 stanza. A companion src/test/groovy/JsonReconstructorTest.groovy exists in the default package. docs/CONCERNS.md C-001 and docs/BACKLOG.md BL-007 both record this at Medium priority, discovered 'Session 2', with no movement since.

**Impact.** A 1,293-line commented-out file in the main source tree is indistinguishable from work-in-progress to a new contributor and is dead weight in every clone and source jar. Its test file is also dead but still counted in the test inventory, inflating apparent coverage of reconstruction logic. The class is referenced in docs/PROJECT_OVERVIEW.md and MODULE_INDEX.md as though it were a real module.

**Fix.** Resolve BL-007 rather than re-logging it: delete both JsonReconstructor.groovy and JsonReconstructorTest.groovy (git history preserves them and can be recovered by SHA), and remove the class from MODULE_INDEX.md and PROJECT_OVERVIEW.md. If the schema-less reconstruction feature is still wanted, open a tracked issue describing the requirement rather than keeping the corpse in main.

### `OSS-17` — .reuse/dep5 has a stanza for a nonexistent README.md, a wrong upstream URL, and no coverage for several tracked trees

- **Location:** `.reuse/dep5:30`
- **Cluster:** hygiene | **Category:** licensing | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** Line 30 declares `Files: README.md` — no such file exists (OSS-02). Line 4 gives `Source: https://github.com/piercelonergan/nexuspiercer`, but pom.xml line 24 and the SCM block give `https://github.com/pierce-lonergan/NexusPiercer` (different org slug and casing). Stanzas exist for src/main/java, src/test/java, src/main/groovy, docs, pom.xml, and .gitignore, but not for src/test/groovy, src/test/avro, src/main/resources, src/test/resources, LICENSES/, NOTICE, maven-publish.yml, .gitattributes, or the two space-named READMEs — those fall through to the `Files: *` catch-all rather than being declared.

**Impact.** The repository presents itself as REUSE-compliant (LICENSES/Apache-2.0.txt, LICENSES/CC0-1.0.txt, SPDX header in LICENSE, a NOTICE file) but the metadata is unmaintained. The wrong Source URL is the one machine-readable pointer a downstream license scanner or SBOM generator would follow, and it resolves to nothing. The dead README.md stanza is evidence the file was intended to exist and its absence was never noticed.

**Fix.** Fix the Source URL to match the pom, delete or repoint the README.md stanza once a real README exists, and add explicit stanzas for the remaining tracked trees. Then run `reuse lint` in CI so the manifest is verified rather than asserted.

### `OSS-18` — Tracked .idea/misc.xml pins a machine-local JDK name, and .idea/ is only partially ignored

- **Location:** `.idea/misc.xml:9`
- **Cluster:** hygiene | **Category:** repo-hygiene | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** Four .idea files are tracked (.gitignore, encodings.xml, misc.xml, vcs.xml). misc.xml line 9 reads `<component name="ProjectRootManager" version="2" languageLevel="JDK_17" project-jdk-name="ms-17" project-jdk-type="JavaSDK" />`. The root .gitignore ignores only .idea/modules.xml, .idea/jarRepositories.xml, .idea/compiler.xml, and .idea/libraries/ (lines 7-10), so any other .idea file IntelliJ generates is untracked-but-not-ignored and will be swept up by `git add .`.

**Impact.** `ms-17` is the author's local Microsoft OpenJDK SDK name; any contributor opening the project gets an unresolved-SDK error until they reconfigure, and their fix shows as a spurious diff. The partial ignore list is also an ongoing source of accidental commits of IDE state — the same failure mode as lib/ (OSS-06). vcs.xml and encodings.xml are harmless but establish the precedent that IDE config belongs in git.

**Fix.** Either ignore .idea/ wholesale (`.idea/` in .gitignore plus `git rm -r --cached .idea`) and let contributors configure their own IDE, or keep only genuinely portable files and replace `project-jdk-name="ms-17"` with a neutral value such as `temurin-17`. Encode the JDK requirement in the pom's maven-enforcer rules, which apply to every contributor regardless of IDE.

### `OSS-19` — Commit history has no tags, no conventional-commit structure, and identical messages spanning unrelated changes

- **Location:** `pom.xml:13`
- **Cluster:** hygiene | **Category:** git-history | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** 49 commits, zero tags, one branch. Messages repeat verbatim: 'enhanced the project' x3, 'added type conversion logic' x7, 'implemented full reconstruction logic' x6, 'creating groovy implementation' x8, 'Added detailed visualization...' x2. The three 'enhanced the project' commits cover entirely unrelated work: e879628 adds the whole legal layer (.reuse/dep5, LICENSES/, NOTICE, LICENSE rewrite), 81305b2 adds 3,352 lines of new feature code (SchemaBasedMapConverter + test), and c7b3dde adds the entire docs/ corpus. No commit references an issue.

**Impact.** A licensing change is indistinguishable from a feature drop in `git log`, which is precisely the kind of change an auditor or downstream consumer needs to find. Bisecting is unreliable when seven consecutive commits share a message, and no changelog can be generated automatically. Combined with the absence of tags (OSS-08), there is no path from a released artifact back to the code that produced it.

**Fix.** Adopt Conventional Commits (feat/fix/docs/chore/build with a scope) and enforce it with a commitlint hook or a CI check on PR titles; require an issue reference for non-trivial changes. Document the convention in CONTRIBUTING.md. History cannot be rewritten safely on a published repo, so apply from the next commit forward and tag releases going forward.

### `OSS-20` — converter/README.md documents a different standalone project and omits half the package's real files

- **Location:** `src/main/java/io/github/pierce/converter/README.md:1`
- **Cluster:** hygiene | **Category:** documentation-accuracy | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** The file is titled 'Schema Forge Converter' and its Project Structure block shows a standalone repo rooted at `schema-forge-converter/` with `├── pom.xml` — a project that does not exist here. Its file inventory omits SchemaBasedMapConverter.java (1,377 lines), TimestampNanoConverter.java, and ConversionResult/Notification helpers, and lists the test directory as containing only TypeConverterProperties.java when `git ls-files` shows 9 test files under src/test/java/io/github/pierce/converter/. Alongside it sits RESEARCH_README.md, 488 lines of third-person research prose ('Building a production-grade Java Map-to-Schema converter...') under a Java source root.

**Impact.** The converter package — the largest subsystem at 27 files — is documented as if it were a separate library with its own build, misleading anyone trying to locate or extend it. Both files sit under src/main/java, an unusual location for prose, and are blanket-claimed as Apache-2.0 by the .reuse `src/main/java/*` stanza despite RESEARCH_README reading as imported third-party or generated material with no stated provenance.

**Fix.** Rewrite converter/README.md as a package-level document that reflects the actual file inventory and drops the standalone-project framing, or convert it to package-info.java. Move RESEARCH_README.md to docs/ (or delete it) and record its origin and licence; if its provenance cannot be established, do not ship it under the project's Apache-2.0 claim.

### `OSS-21` — docs/CONCERNS.md reports zero critical and zero high concerns and records the missing READMEs as a strength

- **Location:** `docs/CONCERNS.md:18`
- **Cluster:** hygiene | **Category:** governance | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** The Critical table (line 18) reads 'No critical concerns identified' and the High table (line 26) 'No high concerns identified'. The only registered items are C-001 (commented-out JsonReconstructor, Medium) and C-002 (naming confusion, Low). Observation I-002 (line 51) records 'Comprehensive READMEs with examples provided | Root' as a positive. The registry contains no entry for the absent README.md, the misplaced CI workflow, the default-skipped quality gates, untracked lib/, or the nonexistent Patterns API — all findings in this audit.

**Impact.** The one artefact in the repo whose stated purpose is tracking risk asserts there is none, which is worse than having no registry: it gives a reader false assurance and makes the genuine gaps look like they were considered and dismissed. I-002 specifically praises the documentation layer that OSS-01, OSS-10, OSS-11, and OSS-12 show to be substantially inaccurate.

**Fix.** Repopulate CONCERNS.md from this audit with honest severities, correct I-002, and stop maintaining it by hand — either drive it from GitHub Issues with labels, or add a dated 'last verified against commit <sha>' line so a stale registry is visibly stale rather than silently wrong.

### `BLD-002` — No Maven wrapper — build reproducibility depends on whatever mvn/JDK happens to be on PATH

- **Location:** `.gitignore:2`
- **Cluster:** infra | **Category:** build-correctness | **Effort:** small

**Evidence.** `ls -d .mvn mvnw mvnw.cmd` → all `No such file or directory`. Yet `.gitignore` line 2 is `!.mvn/wrapper/maven-wrapper.jar` — a negation with no preceding ignore pattern to negate, i.e. a fossil from a template where the wrapper once existed. The POM enforces `<requireMavenVersion><version>[${maven.minimum.version},)</version>` with `maven.minimum.version=3.8.1` (154, 854-856), but nothing pins the actual version. Local reality: Maven 3.9.10 running on JDK 17.0.15 (Adoptium) while PATH `java` is Temurin 21.0.7 — two different JDKs in one environment with no declaration of which is intended.

**Impact.** Every contributor and every future CI runner builds with a different Maven and a different JDK. The enforcer's open-ended `[3.8.1,)` and `[17,)` ranges accept all of them. Combined with JDK-001 (the manifest always claims 17) there is no way to tell after the fact what actually built a given artifact.

**Fix.** Run `mvn wrapper:wrapper -Dmaven=3.9.10` to generate `mvnw`, `mvnw.cmd` and `.mvn/wrapper/`. Commit them, and change CI + docs to invoke `./mvnw`. Remove the orphan `!.mvn/wrapper/maven-wrapper.jar` line or pair it with a `.mvn/` ignore. Add a `maven-toolchains-plugin` entry or document the required JDK explicitly.

### `CI-002` — Workflow grants a GPG signing key and OSSRH token to a third-party action pinned to a mutable tag, with no permissions block

- **Location:** `maven-publish.yml:26`
- **Cluster:** infra | **Category:** supply-chain | **Effort:** small

**Evidence.** ```yaml
- name: Import GPG key
  uses: crazy-max/ghaction-import-gpg@v6
  with:
    gpg_private_key: ${{ secrets.MAVEN_GPG_PRIVATE_KEY }}
    passphrase: ${{ secrets.MAVEN_GPG_PASSPHRASE }}
```
`@v6` is a mutable tag, not a commit SHA. The job has no top-level or job-level `permissions:` block, so it inherits the repository default for `GITHUB_TOKEN`. `actions/checkout@v4` and `actions/setup-java@v4` are likewise tag-pinned. There is also no dependency caching (`cache: maven` on setup-java is absent).

**Impact.** A compromised or retagged `v6` release of the third-party action executes with the project's Maven Central publishing token and the private GPG signing key in scope — the exact credentials needed to publish a malicious signed artifact under this groupId. On repos with the legacy default, the inherited GITHUB_TOKEN is read/write across the whole repo.

**Fix.** Pin all four actions to full commit SHAs with a version comment. Add `permissions: contents: read` at the job level. Add `cache: maven` to setup-java. Consider moving signing into the `central-publishing` flow with a key stored as a Portal-side credential to reduce the number of places the private key lands.

### `COV-002` — @{argLine} breaks the build if jacoco is skipped independently of tests

- **Location:** `pom.xml:1088`
- **Cluster:** infra | **Category:** build-correctness | **Effort:** trivial

**Evidence.** Both surefire (1088-1091) and failsafe (1118-1121) use `<argLine>@{argLine} ${spark.test.jvm.args}</argLine>`. The `argLine` property is produced only by the jacoco `prepare-agent` execution (1145-1150), which respects `${jacoco.skip}` (201). The `fast` profile sets `<jacoco.skip>true</jacoco.skip>` AND `<skipTests>true</skipTests>` together (1323-1330), which masks the problem — but `mvn verify -Djacoco.skip=true` alone (a very common CI speed-up) leaves `@{argLine}` unresolved and passed literally to the JVM.

**Impact.** `mvn test -Djacoco.skip=true` fails with `Error: Could not find or load main class @{argLine}` — a confusing failure whose cause is nowhere near the flag that triggered it.

**Fix.** Add `<argLine></argLine>` (empty) to the POM `<properties>` so late replacement always has a value, or use `<jacoco.agent.argLine>` with a defaulted property. Also add a `prepare-agent-integration` execution so failsafe gets its own agent rather than reusing the unit-test `@{argLine}`.

### `COV-003` — JaCoCo report runs at `test`, before integration tests, and there is no prepare-agent-integration

- **Location:** `pom.xml:1151`
- **Cluster:** infra | **Category:** coverage | **Effort:** small

**Evidence.** ```xml
<execution><id>report</id><phase>test</phase><goals><goal>report</goal></goals></execution>
<execution><id>check</id><goals><goal>check</goal></goals></execution>
```
(1151-1162). `report` is explicitly pinned to `test`; `check` has no phase so it uses jacoco's default of `verify`. maven-failsafe-plugin runs `integration-test` and `verify` (1129-1136) and reuses the unit-test `@{argLine}`. There is no `prepare-agent-integration` and no `report-integration` execution.

**Impact.** The HTML/XML/CSV coverage report is generated before any integration test has executed, so IT coverage never appears in the report a human reads. `check` at verify does see the appended IT data (jacoco append defaults to true), so the report and the gate disagree about the numbers — confusing when tuning the threshold in COV-001.

**Fix.** Move `report` to `<phase>verify</phase>`, or add the standard pair: `prepare-agent-integration` + `report-integration` writing to `jacoco-it.exec`, and have failsafe use `@{failsafeArgLine}`.

### `DEP-001` — dependency:analyze-only runs with failOnWarning=false, so declared-unused and used-undeclared are never enforced

- **Location:** `pom.xml:1209`
- **Cluster:** infra | **Category:** dependency-hygiene | **Effort:** small

**Evidence.** ```xml
<execution><id>analyze</id><goals><goal>analyze-only</goal></goals>
  <configuration>
    <failOnWarning>false</failOnWarning>
    <ignoreNonCompile>true</ignoreNonCompile>
    <ignoredUnusedDeclaredDependencies>
      <ignoredUnusedDeclaredDependency>org.projectlombok:lombok</ignoredUnusedDeclaredDependency>
```
(1203-1215). Zero-reference greps across `src/`: `lombok` → 0, `objenesis` → 0, `net.bytebuddy` → 0, `spock` → 0, `jackson.dataformat.yaml` → 0, `datatype.jsr310` → 0. The lombok whitelist entry is maintaining an exception for a library with no usages anywhere, which also makes the `<annotationProcessorPaths>` lombok entry (776-782) and the `provided` lombok dependency (602-607) pure overhead.

**Impact.** A permanently-warning-only analyzer accumulates cruft indefinitely. Two of the zero-usage dependencies — `jackson-dataformat-yaml` and `jackson-datatype-jsr310` — are at COMPILE scope, so they and their transitive closure (dataformat-yaml drags snakeyaml) are imposed on every downstream consumer of the published POM for no reason.

**Fix.** Remove the six zero-usage dependencies and the lombok annotationProcessorPath, then flip `<failOnWarning>true</failOnWarning>` and add narrowly-scoped ignores only for genuine convergence pins (commons-codec, commons-compress are declared to fix convergence rather than for direct use — mark those `<ignoredUnusedDeclaredDependency>` explicitly so the intent is documented).

### `ENF-002` — Enforcer has no bannedDependencies, requirePluginVersions, or bytecode-level rule

- **Location:** `pom.xml:853`
- **Cluster:** infra | **Category:** enforcer | **Effort:** small

**Evidence.** The rule set is exactly: `requireMavenVersion [3.8.1,)`, `requireJavaVersion [17,)`, `requireOS`, `banDuplicatePomDependencyVersions`, `dependencyConvergence`, `requireReleaseDeps` (with `onlyWhenRelease=true`), `requireUpperBoundDeps` (853-871), with `<fail>true</fail>` (872). Verified working: `mvn -o -B enforcer:enforce@enforce-maven` passes cleanly and `mvn -o -B compile` shows `[INFO] --- enforcer:3.6.0:enforce (enforce-maven) @ nexus-piercer ---` at validate, so `dependencyConvergence` and `requireUpperBoundDeps` genuinely do hold across the Spark/Hadoop/Iceberg/POI/Delta graph — that is real and worth preserving. Missing: `bannedDependencies` (nothing stops log4j-core 1.x/2.x-vulnerable, commons-collections 3.x, or an accidental `org.json` re-introduction — note line 465's comment shows org.json was deliberately removed for licensing), `requirePluginVersions`, `enforceBytecodeVersion` (would catch a dependency compiled for a newer class-file version than release 17), and `banDistributionManagement`.

**Impact.** The convergence rules are the strongest gate in the build, but the rule set has no allow/deny list. Nothing prevents re-adding the org.json dependency that was removed for license reasons, and nothing detects a transitive jar compiled for Java 21 sneaking into a release-17 artifact.

**Fix.** Add a `<bannedDependencies>` rule with `<exclude>org.json:json</exclude>`, `<exclude>log4j:log4j</exclude>`, `<exclude>commons-collections:commons-collections:[,3.2.2)</exclude>`. Add `<requirePluginVersions><banSnapshots>true</banSnapshots></requirePluginVersions>` and `extra-enforcer-rules`' `enforceBytecodeVersion` with `maxJdkVersion=17`.

### `LIC-001` — NOTICE and LICENSE are not packaged into the jar, and NOTICE omits most bundled dependencies

- **Location:** `NOTICE:1`
- **Cluster:** infra | **Category:** licensing | **Effort:** small

**Evidence.** `<resources>` (696-715) covers only `src/main/resources`; there is no maven-jar-plugin resource entry, no `maven-remote-resources-plugin`, and no `<addMavenDescriptor>`-adjacent config placing `LICENSE`/`NOTICE` under `META-INF/` — so the published jar carries neither. NOTICE itself lists eight projects (Avro, Iceberg, Spark, Jackson, SLF4J, JUnit 5, AssertJ) and omits every other compile-scope dependency actually shipped or required: Apache Groovy, Guava, POI/poi-ooxml, commons-io, commons-compress, commons-codec, commons-lang3, SnakeYAML, jqwik (EPL-2.0, currently compile scope per TST-001), and Lombok. It also omits Logback, whose EPL-1.0 OR LGPL-2.1 dual licence is the one entry in the tree that actually warrants attention.

**Impact.** Consumers unpacking the artifact find no licence text at all, which is the standard expectation for an Apache-2.0 jar and is what compliance scanners look for. The NOTICE that does exist is misleading by omission — it reads as a complete third-party inventory but covers roughly a third of the tree.

**Fix.** Add to `<build><resources>`: a resource with `<directory>${project.basedir}</directory>`, `<targetPath>META-INF</targetPath>`, `<includes>LICENSE, NOTICE</includes>`. Regenerate the dependency list mechanically with `license-maven-plugin:add-third-party` or `mvn project-info-reports:licenses` rather than maintaining it by hand.

### `POM-001` — 1727-line POM with duplicated properties, hardcoded versions bypassing their own properties, and dead declarations

- **Location:** `pom.xml:151`
- **Cluster:** infra | **Category:** pom-bloat | **Effort:** medium
- **Complexity:** 1727 lines, single module
  - → ~1100 lines

**Evidence.** Duplicate property: `<gmavenplus.version>3.0.2</gmavenplus.version>` appears at line 151 AND line 206. Properties bypassed by hardcoded literals in dependencyManagement: `<commons-codec><version>1.17.1</version>` (324) duplicates `${commons-codec.version}` (127), and `<commons-lang3><version>3.16.0</version>` (329) duplicates `${commons-lang3.version}` (128) — the same value written twice, so a bump to one silently diverges. Versions hardcoded while every peer uses a property: `byte-buddy 1.14.4` (644), `objenesis 3.3` (651), `jsr305 3.0.2` (319), `delta-spark 3.1.0` (686, with the telling comment `<!-- Or a version compatible with your Spark version -->`), `findsecbugs 1.12.0` (1355), `central-publishing-maven-plugin 0.8.0` (1289). Unused property: `nexus-staging-maven-plugin.version` (172) — plugin never declared. Referenced-but-undefined: `${gpg.keyname}` (1471-1472). Redundant depMgmt after importing `groovy-bom` (262-269): explicit `groovy` (271-275), `groovy-json` (277-281), and `groovy-templates` with `<optional>true</optional>` (283-288) — the latter is never declared as an actual dependency, and `<optional>` in dependencyManagement is meaningless. Redundant deps: `junit-jupiter` aggregate (610-614) plus `junit-jupiter-api` (665-669) and `junit-jupiter-params` (670-674) which it already contains; `poi` (677-681) which `poi-ooxml` already brings. Redundant properties: `maven.compiler.source`/`maven.compiler.target` (107-108) are ignored because the compiler plugin uses `<release>${java.version}</release>` (772). Dead config: `<site>` distributionManagement (94-97) with no maven-scm-publish-plugin; `<contributors>` empty (60-62); `<repositories>`/`<pluginRepositories>` re-declaring `central` (1703-1727) which is inherited from the super-POM and is discouraged in a POM published to Central.

**Impact.** A single-module library POM at 1727 lines is unmaintainable by inspection — which is precisely why Q-001 through Q-003 went unnoticed. The duplicated version literals are a live drift hazard: bumping `commons-lang3.version` to 3.17.0 leaves dependencyManagement pinning 3.16.0, and dependencyManagement wins.

**Fix.** Replace the hardcoded depMgmt literals with their `${...}` properties; delete the duplicate `gmavenplus.version`, the unused `nexus-staging-maven-plugin.version`, the redundant groovy/junit/poi declarations, the empty `<contributors>`, the `<site>` block, and the `<repositories>`/`<pluginRepositories>` sections. Define `gpg.keyname` or remove its references. This alone should remove several hundred lines.

### `PROF-001` — java8 profile is unusable and would silently weaken the enforcer if activated

- **Location:** `pom.xml:1546`
- **Cluster:** infra | **Category:** pom-bloat | **Effort:** trivial

**Evidence.** ```xml
<profile><id>java8</id><properties>
    <java.version>8</java.version>
    <maven.compiler.source>1.8</maven.compiler.source>
    <maven.compiler.target>1.8</maven.compiler.target>
    <maven.compiler.release>8</maven.compiler.release>
```
(1546-1553). Because `java.version` is overloaded (JDK-001), this simultaneously rewrites the enforcer floor to `requireJavaVersion [8,)`, sets gmavenplus `targetBytecode` to 8, and sets `Build-Jdk: 8` in the manifest. It also cannot work: `spark.test.jvm.args` (209-234) passes 18 `--add-opens` flags with no `-XX:+IgnoreUnrecognizedVMOptions`, which a JDK 8 JVM rejects outright; and the project depends on Spark 3.5.0 + Iceberg 1.7.1 + Delta 3.1.0, none of which target Java 8 across the board.

**Impact.** Dead configuration that looks like a supported build mode. If anyone tries `-Pjava8`, they get an obscure JVM startup failure at test time, and in the meantime the enforcer's Java floor has been silently dropped from 17 to 8.

**Fix.** Delete the `java8` profile. Delete `mixed-compilation` (1568-1593) too — it re-declares gmavenplus a third time (BLD-001) and duplicates what the default build already does.

### `REL-002` — maven-release-plugin never pushes the tag, so the release-triggered workflow can never fire

- **Location:** `pom.xml:1272`
- **Cluster:** infra | **Category:** release | **Effort:** small

**Evidence.** ```xml
<pushChanges>false</pushChanges>
<localCheckout>true</localCheckout>
<remoteTagging>false</remoteTagging>
<preparationGoals>clean verify</preparationGoals>
<releaseProfiles>release</releaseProfiles>
```
(1272-1283). Meanwhile maven-publish.yml triggers `on: release: types: [published]`. `preparationGoals` is `clean verify` executed WITHOUT the release profile, so the pre-release validation runs with `development` activeByDefault — i.e. with checkstyle/pmd/spotbugs skipped (Q-001).

**Impact.** `release:prepare` creates version-bump commits and a `v1.0.8` tag purely locally; nothing reaches GitHub, so no GitHub Release is ever published and the workflow's only automatic trigger can never fire — leaving `workflow_dispatch` as the sole path. The release verification step is also the weakest build configuration in the project rather than the strongest.

**Fix.** Set `pushChanges` and `remoteTagging` to `true` (or drop maven-release-plugin entirely and drive releases from a tag-triggered workflow, which is simpler for a single-module project). Change `preparationGoals` to `clean verify -P release,quality` so the release candidate is validated with gates on.

### `SHADE-001` — Shade profile sets a Main-Class that does not exist and relocates to a package prefix that does not match the project

- **Location:** `pom.xml:1503`
- **Cluster:** infra | **Category:** release | **Effort:** small

**Evidence.** ```xml
<transformer implementation="...ManifestResourceTransformer">
    <mainClass>io.github.pierce.lonergan.nexuspiercer.Main</mainClass>
</transformer>
```
(1502-1504). `find src -name 'Main.java' -o -name 'Main.groovy'` → no results; the real base package is `io.github.pierce` (39 Java files under `src/main/java/io/github/pierce/`), not `io.github.pierce.lonergan.nexuspiercer`. The relocations (1527-1536) use the same non-existent prefix: `com.google.common` → `io.github.pierce.lonergan.nexuspiercer.shaded.guava`. The filters also strip `META-INF/LICENSE.txt`, `META-INF/NOTICE.txt`, `META-INF/DEPENDENCIES`, `LICENSE` and `NOTICE` (1517-1521) while simultaneously configuring `ApacheLicenseResourceTransformer` and `ApacheNoticeResourceTransformer` (1505-1508) — mutually contradictory.

**Impact.** `java -jar nexus-piercer-1.0.8-uber.jar` fails with ClassNotFoundException. The uber jar also ships with all upstream LICENSE/NOTICE files deliberately deleted, which is an Apache-2.0 §4(d) redistribution violation for the bundled Guava/Commons/Jackson code.

**Fix.** Either remove `<mainClass>` (this is a library, not an application) or point it at a real class. Fix the relocation prefix to `io.github.pierce.shaded.*`. Remove `META-INF/LICENSE.txt`, `META-INF/NOTICE.txt`, `LICENSE` and `NOTICE` from the exclude filter and let the Apache transformers merge them.

### `SPARK-001` — surefire --add-opens list omits jdk.internal.ref and sun.security.krb5 needed by Spark/Netty off-heap and Hadoop UGI

- **Location:** `pom.xml:209`
- **Cluster:** infra | **Category:** test-execution | **Effort:** trivial

**Evidence.** `spark.test.jvm.args` (209-234) supplies 18 `--add-opens` plus `-Xmx4g -XX:+UseG1GC -Duser.timezone=UTC ...`, and it IS reaching the forked JVM — verified from `target/surefire-reports/TEST-io.github.pierce.MapFlattenerTest.xml`, which records `user.timezone=UTC`, `file.encoding=UTF-8`, `java.awt.headless=true`, `java.net.preferIPv4Stack=true` and `io.netty.tryReflectionSetAccessible=true`, all sourced from that property. The multi-line `<argLine>@{argLine} ${spark.test.jvm.args}</argLine>` (1088-1091) works because Commandline tokenises on whitespace including newlines, and `@{argLine}` correctly picks up the JaCoCo agent (jacoco.exec is 7.2MB). However, comparing against Spark's own `JavaModuleOptions` for JDK 17+, four entries are missing: `--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED`, `--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED`, `-Djdk.reflect.useDirectMethodHandle=false`, and `-XX:+IgnoreUnrecognizedVMOptions`.

**Impact.** `jdk.internal.ref` is required by Spark's `StorageUtils.bufferCleaner`/`Platform.cleanDirectBuffer` and by Netty's direct-buffer cleaner — any test that touches off-heap memory (Arrow via iceberg-arrow, Tungsten spill, shuffle) throws `InaccessibleObjectException`. `sun.security.krb5` is needed by Hadoop's UserGroupInformation, so any hadoop-aws/Kerberos path fails. These have not bitten yet because the current tests are schema-level, but they will as soon as a real Spark job test is added. The absence of `-XX:+IgnoreUnrecognizedVMOptions` also means this argLine hard-fails under the `java8` profile.

**Fix.** Add the four missing entries to `spark.test.jvm.args`. Put `-XX:+IgnoreUnrecognizedVMOptions` first so the block degrades gracefully across JDKs.

### `TST-002` — Surefire's .groovy include patterns are inert — Groovy tests run only by accident via the .java patterns

- **Location:** `pom.xml:1092`
- **Cluster:** infra | **Category:** test-execution | **Effort:** trivial

**Evidence.** ```xml
<includes>
    <include>**/*Test.java</include>
    <include>**/*Tests.java</include>
    <include>**/*TestCase.java</include>
    <include>**/*Test.groovy</include>
    <include>**/*Tests.groovy</include>
    <include>**/*Spec.groovy</include>
</includes>
```
(1092-1100). Surefire's `DirectoryScanner.processIncludesExcludes` rewrites a trailing `.java` to `.class` and leaves every other suffix alone, then matches against `.class` files in `target/test-classes`. So `**/*Test.groovy` can never match anything, and `**/*Spec.groovy` provides no Spock support at all — the correct spelling is `**/*Spec.java` or bare `**/*Spec`. Groovy tests do run today, but only because `**/*Test.java` → `**/*Test.class` happens to match Groovy-compiled classes: `target/surefire-reports/` contains `AvroReconstructorTest.txt`, `TEST-io.github.pierce.JsonFlattenerTest*.xml`, `TEST-io.github.pierce.MapFlattenerTest.xml` etc. The same trap affects the excludes: `<exclude>**/*IT.groovy</exclude>` and `<exclude>**/*IntegrationTest.groovy</exclude>` (1104-1105) are also no-ops. Failsafe (1122-1125) has no `.groovy` patterns at all.

**Impact.** The config expresses an intent (`*Spec.groovy` for Spock) that is silently unimplemented, so adding a single Spock specification would produce a green build with zero tests run — the worst possible failure mode. The dead exclude patterns mean a future `FooIT.groovy` would be picked up by surefire as a unit test instead of by failsafe.

**Fix.** Replace all six includes with extension-less patterns: `**/*Test`, `**/*Tests`, `**/*TestCase`, `**/*Spec`. Same for the excludes (`**/*IT`, `**/*IntegrationTest`) and for failsafe's includes. This is the documented Spock-on-Maven idiom.

### `TST-003` — Spock, byte-buddy and objenesis are declared as test dependencies with zero usages

- **Location:** `pom.xml:395`
- **Cluster:** infra | **Category:** dependency-hygiene | **Effort:** trivial

**Evidence.** `spock-core:${spock.version}` at test scope (395-400) plus, explicitly commented `<!-- Required for Spock mocking -->`, `net.bytebuddy:byte-buddy:1.14.4` (641-646) and `org.objenesis:objenesis:3.3` (648-653). Repo-wide greps over `src/`: `spock` → 0 files, `net.bytebuddy` → 0 files, `objenesis` → 0 files. No file under `src/test/groovy/` extends `Specification`. Note byte-buddy 1.14.4 predates JDK 21 class-file support (added around 1.14.9), so it would break the moment tests are run on the PATH JDK 21.

**Impact.** Three test dependencies, one hardcoded version each, plus the `spock.version` property (205) and the dead `**/*Spec.groovy` surefire include (TST-002) — an entire phantom testing stack that a maintainer must reason about. The byte-buddy pin is also a JDK-21 landmine if the project ever switches JDKs.

**Fix.** Remove spock-core, byte-buddy and objenesis and the `spock.version` property, or actually adopt Spock (in which case fix TST-002's include patterns and bump byte-buddy to a JDK-21-capable version). Enabling failOnWarning on dependency:analyze (DEP-001) would have caught this.

### `RECON-04` — isArrayFieldPattern rebuilds every prefix string of every key — O(depth^2) chars per key, and the trie built to avoid this is dead code

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:888`
- **Cluster:** perf-avro | **Category:** quadratic-path-building | **Effort:** small
- **Complexity:** O(d^2) chars and O(d) allocations per key is correct, but d is bounded by schema nesting depth (typically 2-6), so this is an allocation-rate problem, not an asymptotic one. The dominant real cost is ~2(d-1) allocations per key (String[] copy + joined String), roughly 400/record at the claim's dimensions — meaningfully smaller per key than the Pattern.compile in RECON-03.
  - → O(d) pointer-chasing walk, zero allocation → O(n·d) per record, or O(1) amortized with memoization

**Evidence.** ```java
private boolean isArrayFieldPattern(String[] keyParts, SchemaPathTrie schemaPaths) {
    for (int i = keyParts.length - 1; i > 0; i--) {
        String prefix = String.join(separator, Arrays.copyOfRange(keyParts, 0, i));
        if (schemaPaths.containsArrayPath(prefix)) {
```
Called once per flat key from buildPathTree line 875. For a key of depth d it allocates up to d-1 String[] copies and d-1 Strings; the joins together copy sum(i) = O(d^2) characters. Non-array keys (the majority) always run the full d-1 iterations. Meanwhile SchemaPathTrie DOES build a node tree (lines 587-617) with a per-node `boolean isArrayPath` field at line 591 that is NEVER SET — markAsArrayPath (line 619) only adds to a flat HashSet, and `contains()`/`getSchema()`/`root` are never called from anywhere (verified by grep: line 889 is the only trie call site). Bytecode also shows `keyParts.length` itself read via `invokedynamic getProperty:([String)Object` plus intUnbox, and `i--` via `invokedynamic invoke:(I)Object`.

**Impact.** With 100 keys at depth 5 that is ~500 array copies + 500 String allocations + ~1,200 char copies per record, i.e. 5M allocations over 10k records — all to answer a boolean that a single trie walk answers with zero allocation.

**Fix.** Make the trie actually work: set `current.isArrayPath = true` in markAsArrayPath by walking to the node, then replace isArrayFieldPattern with a single forward walk over keyParts checking `node = node.children.get(part); if (node != null && node.isArrayPath) return true;`. Zero string allocation. Alternatively, since the flat-key SET is identical for every record in a batch, memoize `Map<String,Boolean> isArrayKeyCache` inside SchemaCacheEntry keyed on the raw flat key — then the work is done once per schema, not once per record.

### `RECON-05` — Public reconstruct() materializes each record three times: GenericRecord → Map → GenericRecord

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:256`
- **Cluster:** perf-avro | **Category:** redundant-materialization | **Effort:** trivial
- **Complexity:** 2 full tree traversals + 2 complete materializations + 1 shallow root-level pass per record (not 3 full traversals). Extra allocations are ~1 container per container node of the duplicate graph, not 2 per node.
  - → 1 traversal + 1 materialization for the GenericRecord path

**Evidence.** ```java
public GenericRecord reconstruct(Map<String, Object> flattenedMap, Schema schema) {
    Map<String, Object> reconstructedMap = reconstructToMap(flattenedMap, schema);
    return mapToGenericRecord(reconstructedMap, schema);
}
```
reconstructToMap (line 240) already produces a fully built `GenericRecord record = reconstructRecord(...)`, then throws it away by calling `genericRecordToMap(record)` (line 243) which walks the whole tree building a parallel LinkedHashMap graph. mapToGenericRecord (line 2946) then walks the top level a third time. Worse, mapToGenericRecord only converts the ROOT level — nested values stay as Maps (line 2952 `builder.set(fieldName, value)` with value being a Map), so the third pass is both wasteful and lossy.

**Impact.** 3x tree traversal and 2x peak live object graph per record on the GenericRecord API path. For a record with 500 nodes that is 1,000 extra LinkedHashMap/ArrayList allocations per record, plus a full second copy alive simultaneously — direct GC pressure on Spark executors.

**Fix.** Split the pipeline: extract a private `GenericRecord reconstructInternal(Map,Schema)` containing lines 232-241, have `reconstruct()` return it directly, and have `reconstructToMap()` call it then convert once. Delete mapToGenericRecord.

### `RECON-08` — All five hottest reconstruction methods are 3-6x over the JIT FreqInlineSize threshold and can never be inlined

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:1014`
- **Cluster:** perf-avro | **Category:** jit-inlining | **Effort:** medium
- **Complexity:** 0 of 5 un-inlinable is correct. The blocked optimization is narrower than claimed: intra-method escape analysis still runs; what is lost is scalar replacement of returned temporaries and cross-call check elision. Note reconstructRecord (481 bytes) is also above FreqInlineSize.
  - → Hot inner bodies under 325 bytes become inline candidates, enabling scalar replacement of per-element temporaries

**Evidence.** Measured with `javap -c` on target/classes/io/github/pierce/AvroReconstructor.class (max bytecode offset per method):
- reconstructNestedRecordFromArray (line 1014): 1932 bytes
- convertPrimitive (line 2133): 1628 bytes
- reconstructUnionValue (line 1915): 1479 bytes
- reconstructArrayOfRecords (line 1233): 1106 bytes
- reconstructNestedArrayOfRecordsAtIndex (line 1469): 1010 bytes
(next tier: reconstructArrayFromValues 857, calculateArraySize 837, reconstructRecord 481)
HotSpot's -XX:FreqInlineSize default is 325 bytes and -XX:MaxInlineSize is 35. Every one of these is far above 325, so none will ever be inlined into its caller regardless of how hot it gets. They are all below -XX:HugeMethodLimit (8000) so they still JIT-compile, but with no inlining the JIT cannot see across the call boundary to do escape analysis, scalar replacement of the intermediate Lists, or null-check elision.

**Impact.** Blocks the optimizations that matter most on this workload: the temporary ArrayLists and boxed values created inside these methods can never be scalar-replaced because the JIT cannot inline them into the calling loop. reconstructArrayOfRecords calling a 1932-byte reconstructNestedRecordFromArray inside a per-element loop is the worst case.

**Fix.** Split by responsibility, targeting <325 bytes for the per-element inner bodies. convertPrimitive is the easiest win: extract the FIXED decode strategies (lines 2193-2275, ~700 bytes of the 1628) into a cold `decodeFixed()` method and the BYTES branch into `decodeBytes()`, leaving a small hot switch over INT/LONG/STRING/DOUBLE. Do the same for reconstructNestedRecordFromArray: hoist the asymmetric-array default-value block (lines 1045-1084) and the nested-array-JSON block (lines 1091-1150) into separate methods.

### `RECON-09` — GAvroSchemaFlattener parses every serialized array then immediately re-serializes it back to the same JSON string

- **Location:** `src/main/groovy/io/github/pierce/GAvroSchemaFlattener.groovy:674`
- **Cluster:** perf-avro | **Category:** redundant-round-trip | **Effort:** small
- **Complexity:** O(|json|) parse + O(n) convert + O(|json|) serialize per array field per record is correct. A behavior-preserving fix removes the serialize half and the typedArray allocation (~50% saving) by returning the original string when every convertPrimitive was identity; a full O(1) pass-through requires trusting the declared element type and changes JSON whitespace/number formatting. Separately, the parseCache is dead — it is cleared per record and keyed by a name that is unique within a record, so it can never hit.
  - → O(1) pass-through for identity-mapped element types; parse only when a real conversion is required

**Evidence.** convertSerializedArray, on the documented hot path (`applyTypes` line 559 comment: "This is the hot path method called for every record in streaming"):
```groovy
List<?> parsedArray = objectMapper.readValue(serialized, new TypeReference<List<Object>>() {});
List<Object> typedArray = new ArrayList<>(parsedArray.size());
for (Object element : parsedArray) { ... convertPrimitive(element, elementType) ... }
parseCache.put(cacheKey, typedArray);
return formatArrayForOutput(typedArray, fieldType);
```
and formatArrayForOutput (line 710) is:
```groovy
return objectMapper.writeValueAsString(array);
```
So the method's net contract is String → String. For the overwhelmingly common case (a JSON array of ints/strings that are already the right type), the output is byte-identical to the input.

**Impact.** Per array-valued field per record: one full Jackson parse, one anonymous TypeReference allocation, two ArrayLists, n boxed elements, and one full Jackson serialization — to reproduce the input. On a record with 10 array fields x 10,000 records that is 100,000 needless parse+serialize round trips.

**Fix.** Detect the no-op case and short-circuit: if every element already satisfies isCorrectType() for the declared element type, return the original string unchanged without serializing. Better, decide this once per field at schema-flattening time — if the Avro element type maps to a DataType whose JSON rendering is identity (INT/LONG/DOUBLE/BOOLEAN/STRING), mark the FlattenedFieldType as pass-through and skip the whole method. Also hoist `new TypeReference<List<Object>>() {}` (line 675) to a static final constant.

### `RECON-10` — isNullable() allocates a Groovy Closure and a Stream pipeline per call, and resolves the NULL enum constant via metaclass getProperty

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:2630`
- **Cluster:** perf-avro | **Category:** allocation-pressure | **Effort:** trivial
- **Complexity:** O(|union|) with ~6 object allocations + 1 metaclass lookup per call
  - → O(|union|) with zero allocation

**Evidence.** ```java
private boolean isNullable(Schema schema) {
    if (schema.getType() != UNION) return false;
    return schema.getTypes().stream().anyMatch(s -> s.getType() == NULL);
}
```
javap of this exact method:
```
 8: invokedynamic #66:getProperty:(Ljava/lang/Class;)Ljava/lang/Object;   // resolving NULL
13: invokestatic  ScriptBytecodeAdapter.compareNotEqual:(Object;Object)Z
32: new           class io/github/pierce/AvroReconstructor$_isNullable_closure2
35: dup
38: invokespecial AvroReconstructor$_isNullable_closure2."<init>"
41: invokedynamic #202:invoke:(Object;Lgroovy/lang/Closure;)Object
46: invokestatic  DefaultTypeTransformation.booleanUnbox
```
The lambda is compiled as a Closure class (AvroReconstructor$_isNullable_closure2.class exists on disk) and a NEW instance is allocated on every call. The statically-imported `NULL` (line 14 wildcard import) is a dynamic getProperty on the Schema$Type Class object, not a getstatic — there are 103 such Class-getProperty sites in this file. isNullable is called per field per record from lines 269, 940, 1051, 1214, 1569, 1955, 2955. Note line 1888 isNullableSchema already does the same job with a plain loop.

**Impact.** Per field per record: 1 Closure allocation + ~5 Stream pipeline objects + a metaclass property lookup + boxed comparison + booleanUnbox — versus a 2-iteration for loop. At 100 fields x 10,000 records that is ~6M allocations.

**Fix.** Replace the stream with the loop form already present at line 1888, and delete the duplicate: `for (Schema t : schema.getTypes()) if (t.getType() == Schema.Type.NULL) return true; return false;`. Replace the wildcard static import at line 14 with explicit `Schema.Type.X` references (under @CompileStatic these become getstatic).

### `RECON-11` — Full dotted paths are built eagerly for every field of every array element but are only read by error messages

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:1294`
- **Cluster:** perf-avro | **Category:** string-building | **Effort:** medium
- **Complexity:** O(m·f·|path|) chars copied per array, x4 intermediates from dynamic concat
  - → O(1) per element on the happy path; O(depth) only when an error is actually reported

**Evidence.** reconstructArrayOfRecords, inside `for (int i = 0; i < arraySize; i++)` over every field:
```groovy
path + "[" + i + "]." + fieldName
```
at lines 1294, 1299, 1305 and 1315 — four separate concatenation expressions per element iteration. Also line 921 (`path + separator + fieldName`, per field per record), lines 1136/1154 (`path + "." + fieldPrefix + "." + nestedFieldName`), line 1550, line 1814 (`path + "[" + i + "]"` per array element). The resulting `path` argument is consumed only by exception messages and log.debug calls. Under dynamic Groovy each `+` is its own indy call site producing an intermediate String (see RECON-01 bytecode), so a 4-part concat is 4 dispatches and 4 Strings, not one StringBuilder.

**Impact.** For an array of m elements with f fields: m·f string constructions each copying the whole accumulated prefix, i.e. O(m·f·|path|) characters, plus 4x that in intermediate Strings under dynamic dispatch. Deep structures make |path| grow, so the cost is superlinear in nesting depth. All of it is discarded on the happy path.

**Fix.** Stop threading a materialized String. Pass an immutable PathRef {parent, segment, index} and only render it (recursively) when an exception or a debug log actually fires. If that is too invasive, at minimum hoist `String elemPath = path + "[" + i + "]"` to the top of the `for i` body so the four sites share one prefix instead of rebuilding it four times.

### `RECON-12` — Groovy switch statements compile to linear ScriptBytecodeAdapter.isCase chains, not tableswitch — 70 sites in AvroReconstructor, 47 in GAvroSchemaFlattener

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:2148`
- **Cluster:** perf-avro | **Category:** groovy-dynamic-dispatch | **Effort:** small
- **Complexity:** O(arms) dynamic isCase + O(arms) metaclass getProperty per switch evaluation
  - → O(1) tableswitch on enum ordinal

**Evidence.** Unlike Java, Groovy's `switch` is sugar for a chain of `isCase()` calls; it never emits tableswitch/lookupswitch. javap counts: 70 `ScriptBytecodeAdapter.isCase` in AvroReconstructor.class, 47 in GAvroSchemaFlattener.class. The hottest instances: convertPrimitive line 2148 `switch (actualSchema.getType())` with 10 arms (STRING/INT/LONG/FLOAT/DOUBLE/BOOLEAN/BYTES/FIXED/ENUM/default), convertLogicalType line 2310 `switch (logicalTypeName)` with 7 String arms, handleMissingField line 1574, getDefaultValue line 2818. In GAvroSchemaFlattener, convertPrimitive line 735 / isCorrectType line 812 / getDefaultValue line 839 all switch on DataType with `case DataType.STRING:` labels — and each label is itself an `invokedynamic getProperty:(Class)Object` (84 such sites in that class), so reaching the BYTES arm costs ~9 metaclass property lookups plus ~9 dynamic isCase calls.

**Impact.** Turns what should be one O(1) tableswitch jump into an O(arms) chain of dynamic dispatches, executed once per field per record on the single hottest conversion path in both classes. For BYTES/FIXED/ENUM (late arms) that is ~18 dynamic operations before the real work.

**Fix.** @CompileStatic (RECON-01) lets Groovy emit a real switch for enum and String subjects. Independently, reorder arms so the statistically common types (STRING, LONG, INT, DOUBLE) come first and the rare ones (FIXED, ENUM, BYTES) last, and replace `case DataType.STRING:` with a plain `case STRING:` under an explicit import so the constant is not re-resolved dynamically.

### `RECON-13` — Class.getSimpleName().equals("NullObject") executed for every element of every reconstructed array

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:1818`
- **Cluster:** perf-avro | **Category:** type-check | **Effort:** trivial
- **Complexity:** O(|classname|) string derivation + String.equals per array element
  - → O(1) reference/instanceof check per element

**Evidence.** reconstructArrayFromValues, three separate sites in the per-element loop:
```java
if (value == null || "null".equals(value) ||
        value.getClass().getSimpleName().equals("NullObject")) {
```
(line 1818), and again at line 1844 `if (item != null && !item.getClass().getSimpleName().equals("NullObject"))` and line 1870 (identical). Class.getSimpleName() does substring surgery on the binary name; the result is compared as a String against a class identity that could be tested with instanceof.

**Impact.** Per array element: a getClass, a getSimpleName (which under the Groovy dynamic path is itself two indy dispatches), and a String.equals — where a single `instanceof` reference check would do. On arrays of thousands of primitives this is a measurable fraction of reconstructArrayFromValues.

**Fix.** Replace with `value instanceof org.codehaus.groovy.runtime.NullObject` (the class is already on the classpath since this is a Groovy artifact), or call `org.codehaus.groovy.runtime.NullObject.getNullObject() == value` for an identity compare — NullObject is a singleton.

### `RECON-14` — String.matches() recompiles a regex on the array hot path while three pre-compiled Patterns sit unused

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:2491`
- **Cluster:** perf-avro | **Category:** regex-recompilation | **Effort:** trivial
- **Complexity:** 1 Pattern.compile + O(6·|s|) scanning per array value
  - → O(1) first-char dispatch, zero regex

**Evidence.** The class declares, under the comment "Compiled patterns for performance":
```java
private static final Pattern ARRAY_INDEX_PATTERN = Pattern.compile("\\[\\d+\\]");
private static final Pattern JSON_ARRAY_PATTERN = Pattern.compile("^\\[.*\\]$");
```
(lines 95-96). Grep confirms ARRAY_INDEX_PATTERN and JSON_ARRAY_PATTERN have exactly one occurrence each — their declaration. Only BRACKET_LIST_PATTERN is ever used (line 2525). Meanwhile the hot path uses inline String.matches, which is `Pattern.matches(regex, this)` = a fresh Pattern.compile every call:
- deserializeArray line 2491: `strValue.matches(".*\\[\\s*-?\\d.*")`
- extractValueAtIndex line 2680: the identical expression, duplicated
- convertPrimitive line 2253: `hexStr.matches("[0-9a-fA-F]+")` in the FIXED path
The `.*...*` shape also forces backtracking over the whole array string.

**Impact.** One Pattern compilation plus a backtracking scan per array-valued field per record, on the two methods that handle every serialized array. Exactly the cost the unused static Patterns were declared to avoid.

**Fix.** Compile the looksLikeJson probe once as a static Pattern and call `PAT.matcher(s).find()`. Better: the whole `looksLikeJson` heuristic (lines 2490-2495 / 2679-2684, which is copy-pasted verbatim) is five String.contains scans plus a regex over the same string — replace it with a single character-class check on the first non-space char after '[' (`c=='"' || c=='-' || Character.isDigit(c) || c=='t' || c=='f' || c=='n' || c==']'`), which is O(1) instead of O(6·|s|). Delete the two unused Pattern constants or wire them in.

### `RECON-15` — Anonymous TypeReference allocated per readValue call instead of a static constant

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:2499`
- **Cluster:** perf-avro | **Category:** allocation-pressure | **Effort:** trivial
- **Complexity:** 1 allocation + 1 generic-signature reflection per readValue
  - → 0 allocations; type resolved once at class init

**Evidence.** Three sites each allocate a fresh anonymous subclass instance per invocation:
- line 774 (PathNode.deserializeArrayStatic): `SHARED_OBJECT_MAPPER.readValue(strValue, new TypeReference<List<Object>>() {})`
- line 1616 (parseNestedArrayStructure): `objectMapper.readValue(trimmed, new TypeReference<List<Object>>() {})`
- line 2499 (deserializeArray): `objectMapper.readValue(strValue, new TypeReference<List<Object>>() {})`
Also GAvroSchemaFlattener line 675. Jackson resolves the generic type by reflecting on the anonymous class's generic superclass on each readValue; the commented-out JsonReconstructor at line 133 actually got this right with a static LIST_TYPE_REF.

**Impact.** Per array parse: one object allocation plus Jackson's TypeFactory.constructType reflection walk over the anonymous class's generic signature. Multiplied by every array-valued field of every record, and compounded by RECON-06 which invokes these m times over.

**Fix.** `private static final TypeReference<List<Object>> LIST_TYPE_REF = new TypeReference<List<Object>>() {};` at class level; reference it at all four sites. Equivalently use the pre-resolved `objectMapper.getTypeFactory().constructCollectionType(List.class, Object.class)` as a static JavaType, which skips resolution entirely.

### `RECON-16` — unwrapNullable and per-field schema metadata recomputed for every element of every array

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:1282`
- **Cluster:** perf-avro | **Category:** loop-invariant | **Effort:** small
- **Complexity:** O(m·f) unwrapNullable calls + O(m·f) hash lookups per array
  - → O(f) precomputation + O(m·f) array indexing

**Evidence.** reconstructArrayOfRecords, inside `for (int i = 0; i < arraySize; i++)` (line 1276):
```groovy
for (Schema.Field field : elementSchema.getFields()) {
    String fieldName = field.name()
    Schema fieldSchema = field.schema()
    Schema actualFieldSchema = unwrapNullable(fieldSchema)
    List<Object> fieldValues = parsedFieldValues.get(fieldName)
```
None of `field.name()`, `field.schema()`, `unwrapNullable(...)` or the `parsedFieldValues.get(fieldName)` map lookup depend on `i` — all four are recomputed m times for the same field. The same shape appears in reconstructNestedArrayOfRecordsAtIndex: the `for (Schema.Field field : recordSchema.getFields())` at line 1543 re-does `fieldValuesAtIndex.get(fieldName)` for every j.

**Impact.** For m elements x f fields: m·f redundant unwrapNullable calls (each a getType + getTypes + list indexing, all dynamically dispatched) and m·f redundant hash lookups, where f would suffice. On a 50-element array with 12 fields that is 600 operations instead of 12.

**Fix.** Before the `for i` loop, build parallel arrays once: `Schema.Field[] fields; Schema[] actualSchemas; List<Object>[] valuesByField;` indexed positionally, then the inner loop is a plain index walk with no map lookups and no unwrapping. This also removes the per-iteration Iterator allocation over getFields().

### `RECON-17` — log.debug arguments are evaluated eagerly, including getClass().getSimpleName(), on the per-element hot path

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:1039`
- **Cluster:** perf-avro | **Category:** logging-overhead | **Effort:** trivial
- **Complexity:** 1 Object[] + 2 boxings + 1 getSimpleName per field per element, unconditionally
  - → 1 boolean check per field per element when debug is off

**Evidence.** reconstructNestedRecordFromArray, inside the per-field loop:
```java
log.debug("Field: {}, Index: {}, ValueIndex: {}, RawValue: {}, RawValue.class: {}",
        nestedFieldName, index, valueIndex, rawValue, rawValue != null ? rawValue.getClass().getSimpleName() : "null");
```
and again at lines 1087-1088. SLF4J's placeholder substitution is lazy, but the ARGUMENTS are not: the varargs Object[] is allocated, `index` and `valueIndex` are boxed to Integer, and `rawValue.getClass().getSimpleName()` executes unconditionally — even when DEBUG is disabled, which is the production case.

**Impact.** Per field per array element: one Object[5] allocation, two Integer boxings, one getSimpleName() string derivation, plus (under dynamic Groovy) several indy dispatches — all discarded. Inside reconstructNestedRecordFromArray, the largest method in the file at 1932 bytecode bytes, these two calls also inflate the method well past the inlining threshold (RECON-08).

**Fix.** Guard with `if (log.isDebugEnabled())` so the argument expressions are not evaluated, or delete these two debug statements outright — they are clearly leftover instrumentation (note the un-indented `// DEBUG:` comments at lines 1038, 1045, 1086).

### `RECON-18` — GAvroSchemaFlattener's ARRAY_PARSE_CACHE has a structural 0% hit rate — pure overhead plus a ThreadLocal leak surface

- **Location:** `src/main/groovy/io/github/pierce/GAvroSchemaFlattener.groovy:667`
- **Cluster:** perf-avro | **Category:** ineffective-cache | **Effort:** trivial
- **Complexity:** O(1) ThreadLocal.get + 2 clears per record + O(1) failed lookup per array field, 0% hit rate
  - → Zero — code removed

**Evidence.** The cache is keyed on the field name:
```groovy
String cacheKey = fieldType.getFlattenedName();
if (parseCache.containsKey(cacheKey)) {
    return formatArrayForOutput(parseCache.get(cacheKey), fieldType);
}
```
But applyTypes clears it on entry (line 583 `parseCache.clear()`), iterates `flattenedSchema.entrySet()` exactly once per field (line 586) — and the schema map is keyed by that same flattened name, so each cacheKey is visited exactly once — then clears it again in the finally (line 630). A hit is impossible by construction. Even on a hypothetical hit, the return path still calls formatArrayForOutput, which re-serializes (RECON-09), so it would save nothing.

**Impact.** Per record: one ThreadLocal.get(), two HashMap.clear() calls. Per array field: one containsKey, one put, and retention of the typed List until the next clear. All of it dead weight. The ThreadLocal (line 49) is also never removed except via the manual clearCaches() at line 865 — on long-lived Spark executor threads the map object stays reachable per thread indefinitely.

**Fix.** Delete ARRAY_PARSE_CACHE, the parseCache parameter threading through convertValue/convertSerializedArray, and clearCaches(). If caching is actually wanted, it must be keyed on the serialized VALUE (identical array strings recurring across records), not on the field name, and scoped across records rather than cleared per record.

### `RECON-19` — genericRecordToMap uses LinkedList as its BFS queue

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:2862`
- **Cluster:** perf-avro | **Category:** data-structure-choice | **Effort:** trivial
- **Complexity:** O(nodes) LinkedList.Node allocations per record, pointer-chasing traversal
  - → O(1) amortized array-backed enqueue, zero per-element allocation

**Evidence.** ```java
Map<String, Object> result = new LinkedHashMap<>();
Queue<ConversionTask> queue = new LinkedList<>();
queue.add(new ConversionTask(record, result));
```
The queue is also fed from convertList (line 2907) and convertMap (line 2930). LinkedList allocates a 24-byte Node wrapper per offer and gives poor cache locality on poll; ArrayDeque uses a flat array with amortized O(1) at both ends and zero per-element allocation. This method runs on every record via reconstructToMap line 243.

**Impact.** One extra heap object per node of the record tree, per record, plus pointer-chasing traversal. For a 500-node record over 10,000 records that is 5M throwaway LinkedList.Node objects on top of the ConversionTask objects themselves.

**Fix.** `Deque<ConversionTask> queue = new ArrayDeque<>(64);`. Note GAvroSchemaFlattener already gets this right (line 237 `Deque<SchemaNode> stack = new ArrayDeque<>()`), so this is an inconsistency rather than a considered choice. Combined with RECON-05, the whole method can often be skipped entirely.

### `JFLAT-02` — Pattern.compile inside a per-key x per-path-level nested loop in explodeFlattened — measured 12.3 us/key

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:310`
- **Cluster:** perf-flatten | **Category:** allocation-pressure | **Effort:** small
- **Complexity:** Structure is right — O(K·P) `Pattern.compile` calls per record per explosion path at line 310, plus 1 per call at line 262, all hoistable to O(P) once per instance. But the constant is overstated by ~25x. Measured on JDK 21 with the exact patterns the code builds for a 3-segment path (`^\Qpayload\E(?:\[(\d+)\])?\.([^\[.]+).*$` etc.): pure Pattern.compile of all three = 903 ns total (~301 ns each), and the full realistic inner-loop pass (StringBuilder build + compile + matches, with the line-339 break) = 352-532 ns/key versus 53-68 ns/key cached — ~8x, not 100x. Corrected per-record cost at K=200: ~70-110 us/record, i.e. ~0.07-0.11 s per 1,000 records, NOT 2.46 ms/record / 2.5 s per 1,000. Also note P is an upper bound, not the actual count: line 339 `break`s on the first matching parentLevel, and line 294 `continue`s past every key already assigned to a group, so many keys never reach the loop at all.
  - → O(P) Pattern.compile calls once per instance; O(K·P) matcher applications only, measured 122 ns per key

**Evidence.** Lines 284-310: `for (Map.Entry<String, Object> entry : flattened.entrySet())` { ... `for (int parentLevel = 0; parentLevel < pathParts.length; parentLevel++)` { ... `Pattern pp = Pattern.compile(parentPattern.toString());` }. The regex is rebuilt with a StringBuilder and recompiled for every (key, parentLevel) pair, yet it depends only on `pathParts` — which derives from `explosionPath`, a constructor-time constant. Line 262 has the same problem one level out: `Pattern explosionPattern = Pattern.compile(pattern.toString());` recompiled per record per explosion path. I benchmarked compiling the three parent patterns for a 3-segment path: 12,319.6 ns per pass, versus 122.3 ns to reuse a precompiled Pattern (100x).

**Impact.** With K=200 flattened keys and P=3 path segments, that is 200 x 12.3 us = ~2.46 ms per record per explosion path — ~2.5 seconds per 1,000 records, spent compiling regexes that are byte-for-byte identical every time. Pattern.compile also allocates the full node graph each time, so this is simultaneously the largest GC contributor on the explosion path.

**Fix.** Precompute in the constructor: for each explosion path, store `String[] pathParts`, the single `explosionPattern`, and a `Pattern[] parentPatterns` of length pathParts.length. Reduces P+1 compiles per record to P+1 compiles per JsonFlattenerConsolidator instance.

### `JFLAT-07` — processGroupedValues recompiles a Pattern in a groups x array-fields nested loop, ignoring the static Pattern already declared for that exact regex

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:642`
- **Cluster:** perf-flatten | **Category:** redundant-recomputation | **Effort:** small
- **Complexity:** The O(G·A) loop structure is real and hoistable, but the cost attribution to Pattern.compile is wrong. Measured on JDK 21 against `payload.orders[i].items[3].sku`: `String.replaceAll("\\[\\d+\\]", "")` = 170.6 ns versus `ARRAY_INDEX_STRIP_PATTERN.matcher(s).replaceAll("")` = 123.5 ns — only 1.38x, so just ~47 ns (~28%) of the cost is the avoidable `Pattern.compile`; the remainder is Matcher + StringBuilder allocation that switching to the static Pattern does NOT remove. The claim's 263.8 vs 162.4 (1.62x) is the same shape but the framing 'O(G·A) Pattern.compile calls -> zero Pattern.compile' implies the whole 0.66 ms/record disappears. It does not: hoisting the compile saves ~0.12 ms of it; the real win is the G-fold hoist of an invariant computation, worth ~0.65 ms at G=50/A=50. Also A is an upper bound — line 644 `break`s on the first prefix match.
  - → O(A·L) once per record with zero Pattern.compile; measured 162.4 ns per normalization and G-fold fewer of them

**Evidence.** Lines 641-647: `for (String arrayField : arrayFieldsThreadLocal.get()) { String normalizedArrayField = arrayField.replaceAll("\\[\\d+\\]", ""); if (originalBaseKey.startsWith(normalizedArrayField)) { ... } }`. `String.replaceAll` compiles a fresh Pattern on every invocation — and line 39 already declares `private static final Pattern ARRAY_INDEX_STRIP_PATTERN = Pattern.compile("\\[\\d+\\]")`, the identical regex, used correctly at line 619 but not here. `processGroupedValues` is called once per group from line 631, so the whole loop re-derives the same normalized set for every group. Line 639 additionally allocates `consolidatedKey.replace("_", ".")` per group. Measured: 263.8 ns/key via String.replaceAll versus 162.4 ns/key reusing the static Pattern.

**Impact.** For a record with G=50 consolidation groups and A=50 tracked array fields this is 2,500 Pattern compilations plus 2,500 replaceAll passes per record — ~0.66 ms/record, ~0.66 s per 1,000 records — computing a set that is invariant across all groups in the record.

**Fix.** Two changes: (a) swap `arrayField.replaceAll(...)` for `ARRAY_INDEX_STRIP_PATTERN.matcher(arrayField).replaceAll("")`; (b) hoist the normalization out of processGroupedValues entirely — build the normalized array-field set once per record in `consolidateFlattened` (before the loop at line 627) and pass it in. The startsWith test then runs against a prebuilt collection.

### `JFLAT-09` — processBatch builds a thread pool per call, submits one task per item ignoring batchSize, and collects into a ConcurrentHashMap keyed by boxed dense indices

- **Location:** `src/main/groovy/io/github/pierce/JsonFlattener.groovy:1237`
- **Cluster:** perf-flatten | **Category:** data-structure-choice | **Effort:** medium
- **Complexity:** All four defects confirmed; two scoping corrections. (1) The whole block is opt-in: line 1114 defaults `parallelism = 1` and line 1211 gates on `parallelism > 1 && items.size() > parallelism`, so at default settings this branch never executes and the sequential path runs instead. The finding applies only to callers who explicitly configure parallelism. (2) The ~3N boxing figure assumes every item succeeds: `orderedResults.put(pr.index, ...)` at 1252 boxes once per SUCCESSFUL item, while `containsKey(i)` and `get(i)` at 1265-1266 box twice per item unconditionally — so 2N to 3N, not a flat 3N. Everything else stands, and under dynamic Groovy (see JFLAT-03) it is worse than the claim assumes, since these map operations are indy callsites too.
  - → O(N/batchSize) task submissions, zero boxing, O(1) indexed array writes, shared pool

**Evidence.** Four defects in one method. Line 1213: `ExecutorService executor = Executors.newFixedThreadPool(parallelism);` — a fresh pool built and torn down per processBatch call (shutdown at 1271 with a 60-second awaitTermination). Lines 1217-1233: one `new Callable<ProcessResult>(){...}` anonymous instance submitted per ITEM; `batchSize` (field 1116, setter 1150) is written but never read — grep confirms no read site — so the configured chunking never happens. Line 1237: `Map<Integer, Map<String, Object>> orderedResults = new ConcurrentHashMap<>();` keyed by a dense 0..N-1 int range. Lines 1264-1268: `if (orderedResults.containsKey(i)) { results.add(orderedResults.get(i)); }` — containsKey followed by get, each boxing `i` again.

**Impact.** For N=100,000 records: ~100,000 Callable + 100,000 FutureTask + 100,000 queue-node allocations, ~300,000 Integer boxes (indices above 127 miss the valueOf cache), 200,000 CHM lookups, and 100,000 dispatches through a single LinkedBlockingQueue whose lock becomes the bottleneck when per-item work is measured in microseconds. Pool churn adds ~50-100 us per thread per call, which matters if processBatch is invoked per Spark micro-batch. The index bookkeeping is entirely redundant — line 1239 already iterates `futures` in submission order, so results arrive ordered.

**Fix.** Accept a caller-supplied ExecutorService (or hold one on the instance). Chunk into `batchSize` sublists and submit N/batchSize tasks. Replace orderedResults with a preallocated `Object[] results = new Object[items.size()]` written by index, or drop it entirely and append `pr.result` directly while iterating futures in order — which also makes `ProcessResult.index` (line 1311) dead.

### `JFLAT-10` — buildKey rebuilds the full dotted path with an unfused two-step dynamic concat instead of threading a StringBuilder — comment claims StringBuilder, code does not use one

- **Location:** `src/main/groovy/io/github/pierce/MapFlattener.groovy:333`
- **Cluster:** perf-flatten | **Category:** allocation-pressure | **Effort:** small
- **Complexity:** Mechanism verified exactly; the 'fix' target is slightly optimistic. `javap -c -p` on buildKey reproduces the claimed disassembly verbatim: offset 2 `invokestatic ScriptBytecodeAdapter.compareEqual` for the null test, offset 9 `invokedynamic #18:invoke` + offset 14 `DefaultTypeTransformation.booleanUnbox` for `prefix.isEmpty()`, then TWO separate `invokedynamic #49:invoke` at offsets 32 and 38 followed by `invokedynamic #4:cast` at 43 — so no `makeConcatWithConstants` fusion, two full String allocations per key, dynamically dispatched. Two corrections: (a) on JDK 9+ compact strings these are String + byte[] pairs, not char[]; (b) '0 allocations for interior nodes' is not reachable by threading a StringBuilder alone, because line 311 calls `shouldIncludePath(newKey)` on every interior key and would also need to accept a CharSequence. Realistic win is roughly halving the concat cost (eliminating the never-observed `prefix + separator` intermediate) plus removing one indy dispatch per key.
  - → 0 allocations for interior nodes, 1 String per emitted leaf key; n_leaves·d·s characters copied (the unavoidable minimum)

**Evidence.** Lines 332-333: `// Use StringBuilder for efficiency` immediately followed by `return prefix + separator + key;` — there is no StringBuilder. Under dynamic Groovy this is worse than the equivalent Java: `javap -c -p` on buildKey shows the concatenation compiling to TWO separate `invokedynamic #49:invoke` callsites at offsets 32 and 38 (each returning Object, followed by a `cast` to String) rather than a single fused `makeConcatWithConstants`. So each key costs two full String allocations and two char-array copies, dispatched dynamically. The same disassembly shows `prefix == null` compiling to `ScriptBytecodeAdapter.compareEqual` and `prefix.isEmpty()` to an indy invoke plus `DefaultTypeTransformation.booleanUnbox`.

**Impact.** buildKey is called once per map entry at every level (line 309), plus twice more on array paths (lines 497, 522). Characters copied ≈ 2·Σ over all nodes of |path(node)| ≈ 2.5·n·d·s for a tree of n nodes, depth d, mean segment length s — versus the n·d·s floor of simply materialising the leaf keys. For 972 leaves at depth 6 with 10-char segments that is ~150 KB copied and ~2,800 String+char[] pairs allocated per record, ~150 MB and 2.8M objects per 1,000 records. The intermediate `prefix + separator` value is pure waste — it is never observed.

**Fix.** Fold into the JFLAT-04 refactor: thread a single StringBuilder down the recursion, `append(separator).append(key)` on descent and `setLength(mark)` on return, materialising `toString()` only at leaves. If keeping the current shape, at minimum use one explicit `new StringBuilder(prefix.length()+separator.length()+key.length()).append(...)` so there is a single allocation and a single copy.

### `JFLAT-11` — Circular-reference tracking boxes identityHashCode five times per node and removes from an ArrayDeque by linear scan

- **Location:** `src/main/groovy/io/github/pierce/MapFlattener.groovy:158`
- **Cluster:** perf-flatten | **Category:** data-structure-choice | **Effort:** small
- **Complexity:** 5 Integer allocations per node; O(d) linear scan per exitObject → O(n·d) total
  - → 0 allocations; O(1) pop → O(n) total

**Evidence.** FlattenContext lines 132-160. `enterObject` boxes `int id = System.identityHashCode(obj)` three times — line 144 `visitedIds.contains(id)`, line 149 `visitedIds.add(id)`, line 150 `visitStack.push(id)` — against `Set<Integer> visitedIds` and `Deque<Integer> visitStack`. `exitObject` boxes twice more (lines 158-159). identityHashCode values are essentially always outside the -128..127 Integer cache, so each box is a fresh allocation. Worse, line 158 `visitStack.remove(id)` is `ArrayDeque.remove(Object)` → `removeFirstOccurrence`, a LINEAR scan of the stack — even though exitObject is invoked from the `finally` at line 320 for the object just entered, i.e. strictly LIFO, so it is always the top element. `detectCircularReferences` defaults to true (line 1177), so this runs by default.

**Impact.** 5 Integer allocations per object node (~4,860 per depth-6/972-leaf record, ~5M per 1,000 records) plus O(d) scanning per exit for O(n·d) total scan work where O(n) suffices. The unboxing on every `contains` also defeats the JIT's ability to keep the identity hash in a register. Secondary correctness note: identityHashCode collisions cause spurious `[CIRCULAR_REFERENCE]` output.

**Fix.** Replace `Set<Integer>`/`Deque<Integer>` with an int-keyed open-addressing set and an `int[]` stack (or a primitive collection such as Eclipse Collections IntHashSet), and change line 158 to `visitStack.pop()` since exit order is guaranteed LIFO by the finally block at line 320.

### `JFLAT-12` — applyNamingStrategy compiles three Patterns per output key when SNAKE_CASE is enabled

- **Location:** `src/main/groovy/io/github/pierce/MapFlattener.groovy:1146`
- **Cluster:** perf-flatten | **Category:** allocation-pressure | **Effort:** small
- **Complexity:** 3 Pattern.compile + 4 intermediate Strings per key, plus a full map copy; O(K) compiles per record
  - → 0 Pattern.compile, 1 StringBuilder pass per key, no extra map copy

**Evidence.** Lines 1146-1149: `return key.replaceAll("([A-Z])", "_\$1").toLowerCase().replaceAll("^_", "").replaceAll("_+", "_");` — three `String.replaceAll` calls, each compiling a fresh Pattern, plus `toLowerCase`, producing four intermediate Strings per key. Reached from `transformKeys` line 1130, which runs for every entry of the final flattened map whenever `namingStrategy != AS_IS` (gate at line 236).

**Impact.** For 300 output keys/record x 1,000 records that is 900,000 Pattern.compile calls and 1.2M intermediate Strings. Using the measured ~100 ns compile overhead per replaceAll from the JFLAT-07 benchmark, that is roughly 0.27 s per 1,000 records in compilation alone, on top of the regex execution and allocation. `transformKeys` also builds a second full LinkedHashMap (line 1124) plus a HashSet (line 1125), so the entire result map is copied one extra time.

**Fix.** Hoist the three regexes to `private static final Pattern` fields. Better: for a key of this shape a single left-to-right char scan into one StringBuilder does all three transforms (insert underscore before uppercase, lowercase, collapse runs, strip leading) in one O(L) pass with one allocation. Also fold `transformKeys` into the emit path so the naming strategy is applied as keys are produced rather than by copying the finished map.

### `JFLAT-13` — matchesPattern allocates a Groovy Closure on every call, including cache hits

- **Location:** `src/main/groovy/io/github/pierce/MapFlattener.groovy:1101`
- **Cluster:** perf-flatten | **Category:** groovy-dynamic-dispatch | **Effort:** small
- **Complexity:** 1 Groovy Closure allocation per (key, excludePath) pair even on cache hit; O(n·E) allocations per record
  - → 0 allocations on the lookup path; patterns compiled once per instance

**Evidence.** Line 1101: `Pattern compiledPattern = patternCache.computeIfAbsent(pattern, p -> { ... });`. Because there is no @CompileStatic (JFLAT-03), Groovy compiles that lambda to a Closure class — `target/classes/io/github/pierce/MapFlattener$_matchesPattern_closure3.class` exists on disk — and `javap -c -p` on matchesPattern shows `22: new #1169 // class io/github/pierce/MapFlattener$_matchesPattern_closure3` followed by `invokespecial <init>` BEFORE the `computeIfAbsent` indy call at offset 31. The closure object is therefore constructed unconditionally as an argument, even when the ConcurrentHashMap already holds the compiled Pattern. The same disassembly shows a `cast` to Pattern and further indy calls for `.matcher(path)` and `.matches()`.

**Impact.** `matchesPattern` is called from `shouldIncludePath` line 1090 for every excludePath, and `shouldIncludePath` is called for every key at every level (line 311). With 2 wildcard excludePaths and 972 nodes that is ~1,944 Closure allocations per record purely as GC pressure on a pure cache-hit path — ~2M per 1,000 records. Each Closure also captures owner and thisObject references, so they are not trivially scalar-replaced.

**Fix.** Do an explicit `Pattern p = patternCache.get(pattern); if (p == null) { p = compile(...); patternCache.put(pattern, p); }` so nothing is allocated on the hit path. Adding @CompileStatic converts the closure to a real invokedynamic lambda, which helps but still constructs an argument object — the get-then-compute form fixes it outright. Since `excludePaths` is fixed at construction (line 187), the better fix is to precompile all wildcard patterns in the constructor and drop the cache entirely.

### `JFLAT-14` — explodeFlattened rescans every key against every group twice — two O(K·G) passes over data it just built

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:288`
- **Cluster:** perf-flatten | **Category:** algorithmic-complexity | **Effort:** medium
- **Complexity:** O(K·G) map lookups for the inGroup rescan plus O(K·G) prefix comparisons for parent assignment, per record per explosion path
  - → O(K) set lookups for inGroup; O(K + G) for parent assignment via a prefix index

**Evidence.** The first pass (lines 264-282) populates `recordGroups`. The second pass then re-derives what it already knows — lines 284-294: `for (Map.Entry<String, Object> entry : flattened.entrySet()) { ... for (Map<String, Object> group : recordGroups.values()) { if (group.containsKey(key)) { inGroup = true; break; } } }`. A third O(K·G) loop follows at lines 332-338: `for (Map.Entry<String, Map<String, Object>> group : recordGroups.entrySet()) { if (parentKeyStr.isEmpty() || groupKeyStr.startsWith(parentKeyStr)) { group.getValue().put(key, entry.getValue()); } }`. Separately, lines 368-374 loop over all `pathParts` but the body only executes when `i == pathParts.length - 1`, and rebuilds `pathParts[i] + "[" + indices[i] + "]"` (line 370) inside the per-field loop although it depends only on the group.

**Impact.** With K=200 keys and G=20 groups, the inGroup rescan alone is 4,000 map lookups per record per explosion path that a single HashSet populated during the first pass would answer in 200. Combined with the parent-assignment loop the method is ~3x the necessary traversals of the same flattened map. The `searchPattern` rebuild adds one String concat per field per group where one per group suffices.

**Fix.** Record assigned keys in a `Set<String> claimed` during the first pass (line 279-280) and replace lines 287-294 with a single `if (claimed.contains(key)) continue;`. Index recordGroups by group-key prefix so the parent-assignment loop touches only matching groups. Hoist the line 370 concat above the `for (Map.Entry<String, Object> field : groupFields.entrySet())` loop and drop the vestigial pathParts loop in favour of indexing `pathParts.length - 1` directly.

### `JFLAT-15` — processArrayValues makes four separate passes over the same values with three redundant collection wrappers

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:714`
- **Cluster:** perf-flatten | **Category:** redundant-traversal | **Effort:** small
- **Complexity:** 5 passes over m elements plus 3 wrapper allocations per array field
  - → 1 pass over m elements, 1 StringBuilder + 1 HashSet

**Evidence.** The caller at line 707 does `arrayValues.toArray(new String[0])`, converting a freshly built ArrayList to an array. `processArrayValues` then walks that array four times: line 714 `String.join(arrayDelimiter, values)`; line 718 `new HashSet<>(Arrays.asList(values))`; lines 724-729 the min/max/total length loop; line 736 `determineArrayType(Arrays.asList(values))`. `Arrays.asList` is called twice on the same array, creating two throwaway list views, and `determineArrayType` (line 756) walks it a fifth time internally.

**Impact.** For an array field of m=100 elements this is ~500 element visits and 4 wrapper objects where one pass and one wrapper suffice. Across 20 array fields x 1,000 records that is ~2M redundant element visits and 80,000 throwaway wrappers. The join at line 714 also builds a full concatenated String that is then re-walked for the length statistics, when the running lengths could be accumulated during the join itself.

**Fix.** Fuse into a single loop: iterate the List once accumulating into a StringBuilder for the joined value, tracking min/max/total length, feeding a HashSet for distinct count, and updating the allNumbers/allBooleans flags inline. Pass `List<String>` throughout instead of round-tripping through String[] at line 707.

### `JFLAT-16` — Every LinkedHashMap on the flattening path is default-sized, and the result is materialized into an intermediate ObjectNode tree before serialization

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:608`
- **Cluster:** perf-flatten | **Category:** allocation-pressure | **Effort:** small
- **Complexity:** ~5 rehash cascades per map per record plus a full JsonNode mirror of every output entry (~2n objects for n output keys)
  - → 0 rehashes with correct initial capacity; direct streaming serialization with 0 JsonNode allocations

**Evidence.** Unsized maps on the hot path: line 526 `Map<String, Object> flattenedOutput = new LinkedHashMap<>();`, line 388 (same in flattenJsonForExplosion), lines 608-609 `Map<String, Object> consolidatedOutput = new LinkedHashMap<>(); Map<String, List<KeyedValue>> groupedByBase = new LinkedHashMap<>();`, lines 246-247, line 362. Default capacity 16 at load factor 0.75 rehashes at 12, 24, 48, 96, 192, 384 entries. `consolidatedOutput` is guaranteed larger than its input because lines 731-736 add SIX statistics entries per array group. Separately, lines 119-124 build a full second representation: `ObjectNode result = OBJECT_MAPPER.createObjectNode(); for (...) { putValue(result, entry.getKey(), entry.getValue()); } return OBJECT_MAPPER.writeValueAsString(result);` — every consolidated entry is wrapped in a JsonNode and inserted into the ObjectNode's own (also unsized) map, then walked again by the serializer. `putValue` (lines 188-208) is an 8-branch instanceof chain per entry.

**Impact.** For a 300-key consolidated record: ~5 rehashes relinking ~370 entries in each of three maps, plus ~300 TextNode/LongNode/DoubleNode allocations and ~300 extra hash insertions for the ObjectNode — roughly 900 avoidable objects and 1,100 avoidable hash operations per record, ~1M objects per 1,000 records. In the explosion path this repeats per exploded record (lines 169-173), multiplying by the explosion fan-out.

**Fix.** Size the maps from known input: `new LinkedHashMap<>(flattened.size() * 2)` for consolidatedOutput (accounting for the statistics entries) and `new LinkedHashMap<>(jsonNode.size() * 4)` for flattenedOutput. Replace lines 119-124 with a direct `JsonGenerator` (`OBJECT_MAPPER.getFactory().createGenerator(segmentedWriter)`) writing `writeStringField`/`writeNumberField` straight from the consolidated map, eliminating the ObjectNode tree entirely.

### `JFLAT-17` — serializeArray copies the entire list to apply a transform that is a no-op for every element, on values normalizePrimitive already transformed

- **Location:** `src/main/groovy/io/github/pierce/MapFlattener.groovy:751`
- **Cluster:** perf-flatten | **Category:** redundant-recomputation | **Effort:** small
- **Complexity:** O(m) extra list allocation + O(m) redundant instanceof per array field, plus 1 Groovy Closure per delimited serialization
  - → O(1) extra allocation, 0 redundant checks, 0 closures

**Evidence.** Lines 751-754: `List<Object> serializedValues = new ArrayList<>(values.size()); for (Object value : values) { serializedValues.add(serializeValue(value)); }` — a full defensive copy built unconditionally so that `serializeValue` (line 842) can Base64-encode ByteBuffers, which for JSON-sourced data never occur. The work is also duplicated: `normalizePrimitive` line 873 already begins with `Object serialized = serializeValue(value);` and returns early if it changed, and `flattenList` line 477 runs every element through `normalizePrimitive` before handing the list to `serializeArray` at line 479. So each element passes the `instanceof ByteBuffer` check twice, and any real ByteBuffer is already a "B64:" String by the time serializeArray sees it.

**Impact.** One ArrayList (16-byte header + m-slot Object[]) plus m reference copies and m redundant instanceof checks per array field. At 20 array fields of 100 elements x 1,000 records that is 20,000 ArrayLists and 2M redundant checks per 1,000 records — all in dynamic Groovy callsites (serializeArray carries 50 invokedynamic instructions per javap).

**Fix.** Scan first and copy only on the first element that actually changes, or simply drop the pass: since flattenList line 477 and extractFieldsFromList line 739 already normalize every element through normalizePrimitive, serializeArray can consume `values` directly. Also note the COMMA_SEPARATED and PIPE_SEPARATED branches (lines 768-776) each allocate a Groovy Closure (MapFlattener$_serializeArray_closure1/2.class both exist on disk) — replace both with one StringBuilder loop parameterized by the delimiter char.

### `JFLAT-18` — Sorted pretty output sorts the map twice — once into a TreeMap, once inside Jackson

- **Location:** `src/main/groovy/io/github/pierce/JsonFlattener.groovy:934`
- **Cluster:** perf-flatten | **Category:** redundant-recomputation | **Effort:** trivial
- **Complexity:** 2 × O(n log n) sorts and 2 × O(n) TreeMap copies per record for pretty+sorted output
  - → 1 × O(n log n) sort, 1 × O(n) copy

**Evidence.** Line 244 configures the shared pretty mapper: `mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);`. Then `toJson(OutputOptions)` line 934-935 does `if (options.isSortKeys()) { result = new TreeMap<>(result); }` and line 943 selects `options.isPretty() ? PRETTY_MAPPER : STANDARD_MAPPER`. With pretty+sortKeys the map is sorted into a red-black tree, then Jackson's MapSerializer copies it into ANOTHER TreeMap to honour ORDER_MAP_ENTRIES_BY_KEYS. Even with sortKeys off, any pretty output pays an undocumented sort-and-copy of every record.

**Impact.** Two O(n log n) sorts plus two full map copies (n red-black nodes each) per record instead of one. For 300 keys that is ~2,470 comparisons and 600 tree nodes per record where 1,235 and 300 suffice — and callers using pretty output without sortKeys pay a sort they never asked for.

**Fix.** Drop line 934-935 and let ORDER_MAP_ENTRIES_BY_KEYS handle sorting for the pretty mapper; for the compact path, add a third static mapper with ORDER_MAP_ENTRIES_BY_KEYS enabled and select it when `options.isSortKeys()`. That gives one sort in all four combinations. Also reconsider enabling ORDER_MAP_ENTRIES_BY_KEYS unconditionally on PRETTY_MAPPER, since it silently costs a sort per record.

### `JFLAT-19` — from(InputStream) and from(Reader) slurp the entire document into a StringBuilder, then a String, then hand it to Jackson

- **Location:** `src/main/groovy/io/github/pierce/JsonFlattener.groovy:737`
- **Cluster:** perf-flatten | **Category:** space-complexity | **Effort:** small
- **Complexity:** O(n) time with ~3n bytes of transient allocation and ~3n bytes copied before parsing; peak memory O(n)
  - → O(n) time with O(buffer) allocation; peak memory O(parsed result) only

**Evidence.** Lines 737-748: `StringBuilder sb = new StringBuilder(); String line; while ((line = reader.readLine()) != null) { sb.append(line); } return from(sb.toString());` and the identical pattern at lines 753-766 for Reader. `from(String)` then calls `flattenToMap` → line 263 `objectMapper.readValue(json, MAP_TYPE_REF)`. The StringBuilder starts at capacity 16 and grows by doubling, so a document of size n incurs ~2n bytes of copying through the grow chain, then `toString()` copies all n bytes again into a fresh String, which Jackson then re-walks.

**Impact.** Peak memory is ~3x the document size (StringBuilder backing array + its grow-chain garbage + the final String) plus ~3n bytes of copying, all before a single token is parsed. For a 100 MB JSON file that is ~300 MB of transient allocation and a hard cap at the 2 GB String limit — on a path whose own javadoc advertises "Streaming support for large JSON files" (line 45). Jackson can parse straight from the stream with an 8 KB buffer.

**Fix.** Replace both bodies with `this.currentData = flattener.mapFlattener.flatten(STANDARD_MAPPER.readValue(inputStream, MAP_TYPE_REF));` (and the Reader overload equivalently). Jackson handles decoding, buffering, and incremental parsing. Note the readLine loop also silently strips newlines, so this change is behaviour-preserving for valid JSON.

### `SCHEMA-01` — get_json_object re-parses the whole flattened JSON once per numeric column per row

- **Location:** `src/main/java/io/github/pierce/spark/NexusPiercerSparkPipeline.java:524`
- **Cluster:** perf-schema | **Category:** redundant-parsing | **Effort:** medium
- **Complexity:** O(R × F_null × L) character scans, where F_null = the number of numeric/bool/date/timestamp columns whose parsed value is null on that row (0 for a fully-populated row, F_num only in the worst case) → O(R × L)
  - → O(R x L) — one parse per row

**Evidence.** Inside `for (StructField field : cachedSchema.sparkSchema.fields())` (line 515):
```java
Column fieldIsCorrupt = col("data." + fieldName).isNull()
        .and(get_json_object(col("flattened_json"), "$." + fieldName).isNotNull());
schemaErrorCondition = schemaErrorCondition.or(fieldIsCorrupt);
```

**Impact.** Spark's GetJsonObject parses the JSON document from scratch on every invocation; there is no shared parse across the OR-chain. A flattened schema with 120 numeric/boolean/date/timestamp columns therefore performs 120 full re-parses of the flattened document per row, on top of the from_json parse at line 506 and the from_json map-parse at line 494. This is the single largest cost in the batch path and it scales with column count, so wide schemas degrade linearly. It also builds a 120-deep nested Or expression tree that the codegen path must evaluate per row.

**Fix.** Do the type-compatibility check once per row inside the flatten UDF (the pattern already used in processJsonColumn at lines 587-600: one OBJECT_MAPPER.readTree plus a field loop) and return a boolean alongside the flattened JSON in a struct. Better still, drive it off the parsed `data` struct plus a single from_json with a PERMISSIVE columnNameOfCorruptRecord instead of re-reading raw text at all.

### `SCHEMA-02` — cache() then unpersist() before the returned Dataset is consumed forces the whole flatten pipeline to run twice

- **Location:** `src/main/java/io/github/pierce/spark/NexusPiercerSparkPipeline.java:420`
- **Cluster:** perf-schema | **Category:** redundant-computation | **Effort:** small
- **Complexity:** 2 × O(R × flatten) with metrics enabled → 1 × O(R × flatten) if unpersist is deferred; with metrics disabled the cache is a driver-side no-op, not a wasted materialisation
  - → 1 x O(R x flatten)

**Evidence.** ```java
allProcessedRecords.cache();          // line 420
...
successDataset = allProcessedRecords.filter(col("_error").isNull());
...
if (config.enableMetrics && mode == PipelineMode.BATCH) {
    collectMetrics(allProcessedRecords, metrics);   // line 464 - only action
}
metrics.processingTimeMs = System.currentTimeMillis() - startTime;
allProcessedRecords.unpersist();      // line 468
return new ProcessingResult(successDataset, errorDataset, metrics);
```

**Impact.** collectMetrics is the only action that materialises the cache; unpersist() at line 468 discards it immediately afterwards, before the caller has touched successDataset. The successDataset returned to the user is lazy, so when it is finally written or counted, Spark recomputes the source read, the flatten UDF, and every from_json/get_json_object from scratch. With metrics enabled (the default, config.enableMetrics = true at line 141) the expensive part of the job executes exactly twice. When metrics are disabled the cache() is pure overhead — a full materialisation that is never read.

**Fix.** Do not unpersist inside processDataset. Either return the cached Dataset and expose an explicit release() on ProcessingResult, or drop cache() entirely and compute metrics from the same action that writes the output (e.g. Spark accumulators inside the UDF, or an observe()/CollectMetrics listener), which costs nothing extra.

### `SCHEMA-11` — All three converter caches are unbounded static maps keyed on System.identityHashCode — they leak and almost never hit

- **Location:** `src/main/java/io/github/pierce/converter/SchemaBasedMapConverter.java:334`
- **Cluster:** perf-schema | **Category:** cache-design | **Effort:** small
- **Complexity:** Unbounded O(entries) heap retention with ~0% hit rate across separately-parsed schema objects and 1 key String allocated per lookup → bounded, content-keyed cache. The stated collision mechanism is wrong: collisions arise from the 31-bit identity-hash birthday bound among live objects (~65k), not from GC recycling
  - → bounded O(n) entries, ~100% hit rate on content-equal schemas, 0 key allocations

**Evidence.** ```java
private static final ConcurrentHashMap<String, SchemaBasedMapConverter> CONVERTER_CACHE = new ConcurrentHashMap<>();   // line 181

public static SchemaBasedMapConverter cached(Schema icebergSchema) {
    String key = "iceberg:" + System.identityHashCode(icebergSchema);
    return CONVERTER_CACHE.computeIfAbsent(key, k -> forIceberg(icebergSchema));
}
```
Also lines 343 and 352. AvroSchemaConverter.java:59-60 + :101 uses `ConcurrentHashMap<Integer, ...>` with `System.identityHashCode(schema)`; IcebergSchemaConverter.java:47-48 + :86 is identical.

**Impact.** Three problems compound. (a) identityHashCode means two equal schemas parsed from the same .avsc text on different executors, or re-parsed per batch, are distinct keys — so in the normal streaming pattern (parse schema per micro-batch) the hit rate is 0% and every batch builds a fresh converter tree. (b) Nothing is ever evicted: no maximumSize, no expiry, no weak keys. Each miss permanently retains a converter graph plus a strong reference to the Schema, so a long-lived executor accumulates them until OOM — and the string key `"iceberg:" + identityHashCode` allocates on every call including hits. (c) identityHashCode values are recycled after GC, so a new schema can collide with a dead one's slot and return the wrong converter. Meanwhile Caffeine 3.1.8 is version-managed in pom.xml:354 but never declared as a real dependency and never imported anywhere in src/main.

**Fix.** Key on schema content, not identity — Avro's `Schema.hashCode()` is memoised and content-based, Iceberg's `Schema.asStruct().hashCode()` likewise. Add Caffeine to the real `<dependencies>` block and replace all three ConcurrentHashMaps with `Caffeine.newBuilder().maximumSize(n).weakKeys().build()`, or at minimum a Guava CacheBuilder like FileFinder already uses. Avoid the string-concat key entirely by using the Schema object as the key.

### `SCHEMA-12` — ConversionConfig.defaults() builds a new Builder and a new config object on every call, including on cache hits

- **Location:** `src/main/java/io/github/pierce/converter/ConversionConfig.java:82`
- **Cluster:** perf-schema | **Category:** redundant-allocation | **Effort:** trivial
- **Complexity:** 2 objects + ~40 field stores per defaults() call, on every cached() lookup
  - → 0 allocations (one static singleton)

**Evidence.** ```java
public static ConversionConfig defaults() {
    return builder().build();     // new Builder(), then new ConversionConfig(this)
}
```
The damage is at the call sites, e.g. AvroSchemaConverter.java:93-95:
```java
public static AvroSchemaConverter cached(org.apache.avro.Schema schema) {
    return cached(schema, ConversionConfig.defaults());
}
```
Java evaluates the argument before the call, so defaults() runs even when the cache hits. Same shape at IcebergSchemaConverter.java:79, SchemaBasedMapConverter.java:206/228/262/290/298/316, and AbstractTypeConverter.java:14.

**Impact.** ConversionConfig has ~20 fields and its Builder another ~20; each defaults() call is two object allocations plus 40 field stores. On a per-record `cached(schema)` call path this is pure garbage. SchemaBasedMapConverter.AvroEnumConverter (line 925) additionally calls `ConversionConfig.defaults()` in its constructor instead of using the parent's config, so enum fields silently ignore trimStrings/coerceEmptyStringsToNull.

**Fix.** Add `private static final ConversionConfig DEFAULT = builder().build();` and return it — ConversionConfig is immutable, so a singleton is safe. Same for strict() and lenient(). Pass the parent's config into AvroEnumConverter.

### `SCHEMA-14` — StructConverter copies the entire input map before reading it, once per nested struct per record

- **Location:** `src/main/java/io/github/pierce/converter/StructConverter.java:56`
- **Cluster:** perf-schema | **Category:** defensive-copy | **Effort:** small
- **Complexity:** O(N_struct x F) extra map entries + O(N_struct) map allocations per record
  - → 0 copies when keys are already Strings

**Evidence.** ```java
if (value instanceof Map<?, ?> map) {
    sourceMap = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
        String key = entry.getKey() != null ? entry.getKey().toString() : null;
        sourceMap.put(key, entry.getValue());
    }
}
```
and at line 114, when extra fields are disallowed:
```java
Set<String> extraFields = new HashSet<>(sourceMap.keySet());
extraFields.removeAll(fields.keySet());
```

**Impact.** The copy exists solely to normalise Avro Utf8 keys to String, but it runs unconditionally — including for the overwhelmingly common case where the keys are already java.lang.String and `entry.getKey().toString()` returns the same instance. Cost is one LinkedHashMap plus one Entry node plus one rehash per field, per nested struct, per record. A record with 5 nested structs of 10 fields each pays 5 map allocations and 50 entry nodes per record on top of the 5 result maps. The line-114 path adds a second full copy of the key set.

**Fix.** Probe the first key: if it is a String, use the source map directly and skip the copy. Only build the normalised copy when a non-String key is actually present. For the extra-fields check, iterate `sourceMap.keySet()` and test `!fields.containsKey(k)` without materialising a HashSet.

### `SCHEMA-15` — FileFinder.performDiscovery leaks the Files.walk stream and recomputes the CWD absolute path for every discovered file

- **Location:** `src/main/java/io/github/pierce/files/FileFinder.java:831`
- **Cluster:** perf-schema | **Category:** resource-leak | **Effort:** trivial
- **Complexity:** O(paths) leaked directory handles; O(files) redundant CWD resolutions and full-path string materialisations
  - → 0 leaked handles; O(1) CWD resolution; filename-only string per file

**Evidence.** ```java
Files.walk(path, 2)
        .filter(p -> { try { return p.toString().endsWith(extension); } catch (Exception e) { return false; } })
        .forEach(p -> {
            try {
                Path currentDir = Paths.get(".").toAbsolutePath().normalize();
                Path absolutePath = p.toAbsolutePath().normalize();
                ...
```

**Impact.** Files.walk returns a Stream backed by an open DirectoryStream that must be closed; there is no try-with-resources here, so each of the ~34 search paths (Config.searchPaths, lines 95-142) leaks a directory handle per discovery call, relying on the finaliser to release it. Separately, `Paths.get(".").toAbsolutePath().normalize()` is loop-invariant but is recomputed inside the forEach — it touches the `user.dir` system property, allocates several Path objects and normalises them, for every single discovered file. `p.toString()` inside the filter also materialises the full path string just to test a suffix.

**Fix.** Wrap the walk in try-with-resources. Hoist `currentDir` above the CompletableFuture (it is the same for every path and every file). Replace `p.toString().endsWith(extension)` with `p.getFileName().toString().endsWith(extension)` to avoid building the full path string.

### `SCHEMA-16` — FileFinder does not cache negative lookups, so every miss triggers a full recursive walk of the working tree

- **Location:** `src/main/java/io/github/pierce/files/FileFinder.java:557`
- **Cluster:** perf-schema | **Category:** redundant-io | **Effort:** small
- **Complexity:** per miss: O(6) classpath probes + O(34) stat calls + O(tree) walk + O(files) discovery + O(files x n x m) edit distance — repeated on every identical miss
  - → first miss same, subsequent identical misses O(1)

**Evidence.** findFileHandle's last resort:
```java
FileHandle deepSearchHandle = performDeepSearch(fileName, searchedLocations);   // line 557
```
which runs `Files.walkFileTree(start, ..., config.maxSearchDepth, searcher)` from `Paths.get(".")` (lines 664-666). Guava's LoadingCache does not cache loader exceptions, so a not-found result is never memoised. fileExists is built directly on this:
```java
public static boolean fileExists(String fileName) {
    try { getInstance().fileCache.get(fileName); return true; }
    catch (Exception e) { return false; }
}
```

**Impact.** A single miss costs: up to 6 classpath resolutions (searchClasspath, line 571-599), ~34 Files.exists stat calls (searchLocalPaths, line 608-620), then a depth-5 recursive walk of the entire project tree, and finally createNotFoundException (line 682) which calls performDiscovery again and runs an O(n x m) Levenshtein computeEditDistance (line 938-957) against every discovered file. Every repeat query for the same missing name pays the whole bill again. Any code that probes several candidate names in a loop — or calls fileExists on a negative — turns each probe into a full filesystem walk.

**Fix.** Memoise negatives: store a sentinel FileHandle (or use `Optional<FileHandle>` as the cache value) so a miss is cached with a short TTL. Gate performDeepSearch behind a flag or a first-miss-only policy, since 34 explicit search paths have already been tried by that point.

### `SCHEMA-17` — FileFinder.searchClasspath resolves each candidate location twice and leaks the probe InputStream

- **Location:** `src/main/java/io/github/pierce/files/FileFinder.java:583`
- **Cluster:** perf-schema | **Category:** resource-leak | **Effort:** trivial
- **Complexity:** 2-3 classpath resolutions and 1 leaked InputStream per candidate location, 6 locations per lookup
  - → 1 classpath resolution per candidate location, 0 streams opened

**Evidence.** ```java
URL resource = getClass().getResourceAsStream(location) != null
        ? getClass().getResource(location) : null;

if (resource == null) {
    resource = Thread.currentThread().getContextClassLoader().getResource(location.startsWith("/")
            ? location.substring(1) : location);
}
```

**Impact.** `getResourceAsStream` opens a real stream (for a JAR entry, an inflater-backed stream) purely to test existence, and the reference is discarded without close() — one leaked stream per hit, six candidate locations per lookup. The classpath is then walked a second time by `getResource(location)` to obtain the URL that was already implicit in the first call. When the file is not on the classpath, all six locations are probed twice plus a third time through the context classloader, and `searchedLocations.add("classpath:" + location)` (line 581) allocates a string for each.

**Fix.** Call `getResource(location)` once and test the returned URL for null — it gives existence and the URL in a single lookup, and opens nothing.

### `SCHEMA-18` — Caffeine is version-managed but never a real dependency and never used; the only working cache in the cluster is Guava's in FileFinder

- **Location:** `pom.xml:354`
- **Cluster:** perf-schema | **Category:** cache-design | **Effort:** small
- **Complexity:** 8 unbounded caches, monotonic heap growth on long-lived executors
  - → bounded O(maxSize) per cache with eviction

**Evidence.** pom.xml declares `<caffeine.version>3.1.8</caffeine.version>` (line 139) and a dependencyManagement entry (lines 354-356), but there is no matching entry in the real `<dependencies>` block and `grep -rn -i caffeine src/main` returns nothing. The only bounded, evicting cache in the whole cluster is FileFinder.java:335-344 (`CacheBuilder.newBuilder().maximumSize(...).expireAfterWrite(...)`). Every other cache is a raw unbounded ConcurrentHashMap: AvroSchemaFlattener.java:38, AvroSchemaLoader.java:41-42, CreateSparkStructFromAvroSchema.java:24, NexusPiercerSparkPipeline.java:40, SchemaBasedMapConverter.java:181, AvroSchemaConverter.java:59, IcebergSchemaConverter.java:47, TypeConverterRegistry.java:14.

**Impact.** Eight unbounded static caches on long-lived executor JVMs, none with size limits, TTL, weak keys or eviction. In a streaming job that parses its schema per micro-batch (the documented pattern), SCHEMA_CACHE, schemaCache and structTypeCache all grow monotonically and retain Schema/StructType graphs forever. The infrastructure to fix this is already half-provisioned in the pom and simply not wired up.

**Fix.** Move Caffeine from dependencyManagement into `<dependencies>`, then convert the eight ConcurrentHashMaps to `Caffeine.newBuilder().maximumSize(...).expireAfterAccess(...).build()`. If adding a runtime dependency is undesirable, Guava's CacheBuilder is already on the compile classpath and shaded (pom.xml:1529-1531) and would do the same job.

### `SCHEMA-19` — AvroSchemaFlattener's Excel analytics path contains three separate quadratic scans

- **Location:** `src/main/java/io/github/pierce/AvroSchemaFlattener.java:661`
- **Cluster:** perf-schema | **Category:** quadratic-algorithm | **Effort:** small
- **Complexity:** O(A x W) with A x W string allocations, O(A x F) lookups, O(F^2) tree iteration
  - → O(W + A) with A allocations, O(A) lookups, O(F) tree walk

**Evidence.** Descendant counting inside the per-array-field loop of createArraySection:
```java
long descendants = fieldsWithinArrays.stream()
        .filter(f -> f.startsWith(arrayField + "_"))
        .count();
```
The `arrayField + "_"` concatenation is inside the lambda body, so it is re-allocated for every element. Lookups in the same loop are linear scans:
```java
private FieldMetadata findFieldMetadata(String fieldName) {
    return fieldMetadataList.stream().filter(f -> f.flattenedName.equals(fieldName)).findFirst().orElse(null);   // line 841
}
private ArrayDefinition findArrayDefinition(String fieldName) {
    return arrayDefinitions.stream().filter(a -> a.fieldName.equals(fieldName)).findFirst().orElse(null);        // line 848
}
```
And generateTreeRecursively (line 552-579) re-iterates the full depth-(d+1) field list once for every field at depth d.

**Impact.** For A array fields, W fields-within-arrays and F total fields: descendant counting is O(A x W) with A x W String concatenations; the two finders make createArraySection O(A x F); and generateTreeRecursively performs sum-over-d of |F_d| x |F_d+1| iterations, which is O(F^2) for a two-level schema. This is the offline exportToExcel path rather than the record hot path, so it does not affect per-record throughput — but on a 5,000-field schema it turns a report into a multi-minute operation.

**Fix.** Hoist `arrayField + "_"` out of the lambda. Build `Map<String,FieldMetadata>` and `Map<String,ArrayDefinition>` indexes once at the top of createArraySection instead of scanning lists. In generateTreeRecursively, precompute a parent→children multimap from originalPath so each field is visited once.

### `SCHEMA-20` — NexusPiercerPatterns collects to the driver, runs replaceAll in a loop, builds an N-literal isin, and reads the input twice

- **Location:** `src/main/java/io/github/pierce/spark/NexusPiercerPatterns.java:126`
- **Cluster:** perf-schema | **Category:** spark-plan | **Effort:** medium
- **Complexity:** 1 unbounded driver collect; O(S) regex compilations for S stat fields; O(R x |arrayBaseFields|) per-row literal comparisons; 2 full input reads + 2 shuffles
  - → 0 driver collects; 1 regex compilation; O(R) broadcast-hash probes; 1 read + 1 shuffle

**Evidence.** There are ZERO Pattern.compile calls in this file — regexes go through Spark's `rlike` (lines 126, 148-150), which compiles the literal once per expression. The costs are elsewhere:
```java
Set<String> statFields = new HashSet<>(
        flattened.filter(col("key").rlike(".*_count$|..."))
                .select("key").as(Encoders.STRING()).collectAsList());          // 126-128

for (String statField : statFields) {
    arrayBaseFields.add(statField.replaceAll("_(count|type|...)$", ""));       // 133
}
...
.withColumn("likely_array", col("key").isin(arrayBaseFields.toArray()))         // 155
.orderBy("field");                                                             // 158
```
And generateDataQualityReport reads the same inputPath twice — once at line 51 and again through the full pipeline at line 76.

**Impact.** Line 127 collects one row per matching key across the whole sample to the driver with no limit — unbounded driver memory. `String.replaceAll` at line 133 compiles the regex on every invocation (String.replaceAll is `Pattern.compile(regex).matcher(this).replaceAll(...)`), so the pattern is recompiled once per stat field in the loop. Line 155 splices |arrayBaseFields| literals into an `In` predicate, which Spark evaluates as a linear scan per row and which bloats the physical plan. `orderBy` at line 158 forces a full sort with an exchange for what is a report. generateDataQualityReport reads and parses the input file twice and shuffles twice (groupBy at line 61, plus collectMetrics' groupBy) for statistics that could come from one pass.

**Fix.** Hoist a `private static final Pattern SUFFIX = Pattern.compile("_(count|type|distinct_count|min_length|max_length|avg_length)$")` and call `SUFFIX.matcher(s).replaceAll("")`. Replace collectAsList with a broadcast join against a derived DataFrame, or bound it with `.limit(n)`. Replace the isin with a broadcast join on a small key DataFrame. Make orderBy opt-in. In generateDataQualityReport, read the text once into a cached Dataset and feed both the quality aggregation and processDataset from it.

### `SCHEMA-21` — GenericRecord validates via schema.findField on every setField and resolves positional access through a String key

- **Location:** `src/main/java/io/github/pierce/converter/GenericRecord.java:37`
- **Cluster:** perf-schema | **Category:** data-structure | **Effort:** medium
- **Complexity:** per field per record: 2 hash lookups on write, 1 list index + 1 hash lookup on read; >=1 rehash per record for F>12
  - → per field per record: 1 array store on write, 1 array load on read; 1 array allocation per record

**Evidence.** ```java
public GenericRecord setField(String name, Object value) {
    Types.NestedField field = schema.findField(name);
    if (field == null) throw new IllegalArgumentException("Unknown field: " + name);
    values.put(name, value);
    return this;
}

public Object get(int pos) {
    Types.NestedField field = schema.columns().get(pos);   // line 57
    return values.get(field.name());                       // line 58
}
```
The backing store is `new LinkedHashMap<>()` with default capacity (line 24).

**Impact.** IcebergSchemaConverter.convert calls setField once per field per record (lines 129, 138, 146), so each field costs an Iceberg findField hash lookup plus a LinkedHashMap put — two hash operations where the field index is already known from the enclosing `for (Types.NestedField field : schema.columns())` loop. The LinkedHashMap starts at capacity 16, so any schema wider than 12 columns rehashes at least once per record. `get(int pos)` is the API Iceberg writers use, and it turns an O(1) array index into a list index plus a String hash lookup — so writing R records of F fields costs R x F string hashes on the read side too.

**Fix.** Back the record with an `Object[] values` sized to `schema.columns().size()`, plus the name→position map that Iceberg's Schema already maintains. setField becomes one findField-derived index write; get(pos) becomes `values[pos]`. Keep the Map-shaped API as a view over the array for compatibility.

### `SCHEMA-22` — AvroSchemaFlattener's static cache is keyed by schema full name only, and computeIfAbsent runs file IO and mutates shared instance state under the bin lock

- **Location:** `src/main/java/io/github/pierce/AvroSchemaFlattener.java:96`
- **Cluster:** perf-schema | **Category:** cache-design | **Effort:** medium
- **Complexity:** content-blind key (wrong hits); IO + parse serialised under a bin lock; shared mutable state raced across threads
  - → content-keyed; lock held only for the map insert; stateless flattener

**Evidence.** ```java
private static final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();   // line 38

public Schema getFlattenedSchema(String schemaPath) throws IOException {
    String cacheKey = schemaPath + ":" + this.includeArrayStatistics + ":" + this.includeNonTerminalArrays;
    return schemaCache.computeIfAbsent(cacheKey, path -> {
        InputStream is = FileFinder.findFile(schemaPath);
        Schema schema = new Schema.Parser().parse(is);
        return flattenSchema(schema);
    });
}

public Schema getFlattenedSchema(Schema schema) {
    String cacheKey = schema.getFullName() + ":" + ... ;      // line 112 - content-blind key
    return schemaCache.computeIfAbsent(cacheKey, k -> flattenSchema(schema));
}
```
flattenSchema begins by clearing eleven instance collections (lines 118-128) and then repopulates them.

**Impact.** Three issues. (a) The line-112 key ignores schema content, so two different revisions of the same named record collide and the second silently receives the first's flattened schema. (b) The mapping function performs FileFinder.findFile — which may include a full recursive tree walk (SCHEMA-16) — and an Avro parse while holding a ConcurrentHashMap bin lock; concurrent lookups that hash to the same bin serialise behind it, and the InputStream from line 98 is never closed. (c) flattenSchema mutates instance state that loadSchema then reads back out (NexusPiercerSparkPipeline.java:874-877). Two threads calling computeIfAbsent for different keys on the same flattener instance run flattenSchema concurrently and corrupt each other's arrayFieldNames/terminalArrayFieldNames, so the cached StructType and the terminal/non-terminal sets can disagree.

**Fix.** Include `schema.hashCode()` (Avro memoises it and it is content-based) in the line-112 key. Load and parse outside computeIfAbsent, in try-with-resources, and pass the parsed Schema in. Make flattenSchema return a result object holding the flattened schema plus all the derived sets instead of writing to instance fields, so the flattener becomes stateless and safe to share.

### `SCHEMA-23` — processJsonColumn parses the flattened JSON twice per row and dereferences the broadcast inside the row loop

- **Location:** `src/main/java/io/github/pierce/spark/NexusPiercerSparkPipeline.java:586`
- **Cluster:** perf-schema | **Category:** redundant-parsing | **Effort:** medium
- **Complexity:** per row: 1 serialise + 2 full parses + 1 broadcast deref + F boxed values
  - → per row: 1 serialise + 1 parse (Spark's), 0 broadcast derefs, 0 boxing

**Evidence.** ```java
String flattenedJson = flattener.flattenAndConsolidateJson(rawJson);
...
StructType sparkSchema = finalBroadcastSchema.getValue();      // line 586 - per row
JsonNode jsonNode = OBJECT_MAPPER.readTree(flattenedJson);     // line 587 - parse #1
for (StructField field : sparkSchema.fields()) { ... }
return RowFactory.create(flattenedJson, schemaError);
```
and then, back in the plan:
```java
.withColumn("_parsed_data", from_json(col("_processing_output.flattened_json"), cachedSchema.sparkSchema));   // line 621 - parse #2
```

**Impact.** The flattener produces a Map, serialises it to a JSON String, the UDF parses that String back into a Jackson tree to validate types, discards the tree, returns the String, and Spark parses the same String a third time via from_json. Two of the three passes are avoidable. `finalBroadcastSchema.getValue()` and `sparkSchema.fields()` are loop-invariant across the whole partition but are executed per row; Broadcast.getValue involves a synchronized read and a lazy-deserialise check. `extractJsonNodeValue` (line 673) then boxes every numeric field into Integer/Long/Double just so isTypeCompatible can unbox it, and isTypeCompatible's numeric fallback calls `Double.parseDouble(value.toString())` (line 706), allocating a String per non-Number value.

**Fix.** Have the flattener expose the flattened Map so the UDF validates against the map directly with no serialise/parse. Hoist `finalBroadcastSchema.getValue()` and a precomputed array of (fieldName, DataType) pairs outside the lambda by capturing them once — the schema is already available on the driver at line 569, so the broadcast adds nothing here. Replace extractJsonNodeValue+isTypeCompatible with a direct JsonNode-vs-DataType check (node.isNumber(), node.isBoolean()) that never boxes.

### `SCHEMA-24` — SerializedArrayConverter and ListConverter walk arrays through java.lang.reflect.Array, boxing every primitive element

- **Location:** `src/main/java/io/github/pierce/converter/SchemaBasedMapConverter.java:1058`
- **Cluster:** perf-schema | **Category:** reflection-on-hot-path | **Effort:** small
- **Complexity:** per array value: N boxed wrappers + N Strings + 1 intermediate List (ListConverter)
  - → per array value: 0 boxing for primitive arrays, 1 StringBuilder, no intermediate List

**Evidence.** ```java
if (value.getClass().isArray()) {
    StringJoiner joiner = new StringJoiner(",");
    int length = java.lang.reflect.Array.getLength(value);
    for (int i = 0; i < length; i++) {
        joiner.add(convertElement(java.lang.reflect.Array.get(value, i)));
    }
    return joiner.toString();
}
```
Same pattern in ListConverter.arrayToList (lines 76-82):
```java
int length = Array.getLength(array);
List<Object> list = new ArrayList<>(length);
for (int i = 0; i < length; i++) { list.add(Array.get(array, i)); }
```

**Impact.** `Array.get` on a primitive array boxes each element into a wrapper object before returning it. For a long[] of 1,000 elements that is 1,000 Long allocations per value, and then convertElement calls toString() on each, allocating 1,000 more Strings before StringJoiner concatenates them. ListConverter then materialises a full intermediate ArrayList of boxed values that is immediately iterated and discarded. Both sit on the per-value path for array-typed columns.

**Fix.** Add explicit typed branches for the common primitive array types (long[], int[], double[], String[], Object[]) before the reflective fallback; for the string-joining case append directly to a StringBuilder with the primitive overload, which avoids both the boxing and the intermediate String. In ListConverter, iterate the array in place instead of copying it into an ArrayList first.

### `NP-018` — AUTO_DETECT timestamp precision misreads pre-1973 millisecond epochs as seconds

- **Location:** `src/main/java/io/github/pierce/converter/TimestampConverter.java:101`
- **Cluster:** quality | **Category:** correctness | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
} else if (absValue > 100_000_000_000L) {   // 1e11
    return l * MICROS_PER_MILLI;            // millis
} else {
    return l * MICROS_PER_SECOND;           // seconds
}
```
The millis/seconds boundary of 1e11 ms corresponds to 1973-03-03. `TimestampPrecision.AUTO_DETECT` is the builder default (ConversionConfig.Builder line 254).

**Impact.** An epoch-millis value for any date before March 1973 is multiplied by 1,000,000 instead of 1,000. `63072000000` (1972-01-01 in millis) is interpreted as 63 billion *seconds* → year 3968. Historical/backfill datasets, birthdates stored as millis, and sentinel values silently land thousands of years in the future, and partition pruning on the timestamp column then silently excludes them from every query.

**Fix.** Make AUTO_DETECT reject ambiguous magnitudes rather than guess — throw and require an explicit `inputTimestampPrecision` — or at minimum log a WARN when a value falls in the ambiguous band. Do not default to AUTO_DETECT.

### `NP-021` — Multi-branch Avro unions silently lose all but one branch, and the three code paths disagree on which

- **Location:** `src/main/java/io/github/pierce/AvroSchemaFlattener.java:734`
- **Cluster:** quality | **Category:** correctness | **Effort:** medium
- **Complexity:** N/A
  - → N/A

**Evidence.** AvroSchemaFlattener.getNonNullType and CreateSparkStructFromAvroSchema.getNonNullType both `return` the **first** non-NULL branch. SchemaBasedMapConverter.unwrapAvroNullableStatic also takes `.findFirst()`. But SchemaBasedMapConverter.flattenAvroSchema (line 1106) does:
```java
for (Schema unionType : currentSchema.getTypes()) {
    if (unionType.getType() == NULL) nullable = true;
    else currentSchema = unionType;   // keeps the LAST non-null
}
```

**Impact.** A legitimate union like `["null","string","long"]` is flattened as `string` by the schema flattener and the Spark converter, but as `long` by flattenAvroSchema — so the flattened schema and the flattened-schema-derived converter disagree on the column type for the same field, and `from_json` yields nulls for every row that used the other branch. A union of two record types `["null","Card","Bank"]` has only Card's fields emitted; every Bank payment record loses all of its fields with no warning.

**Fix.** Pick one policy and apply it everywhere: for genuine multi-branch unions, either widen to STRING with a WARN naming the discarded branches, or emit one flattened column per branch (`payment_card_*`, `payment_bank_*`). Make flattenAvroSchema use the same shared helper as the other two call sites so they cannot drift.

### `NP-022` — TypeConverterRegistry NPEs on a null config despite an explicit null guard

- **Location:** `src/main/java/io/github/pierce/converter/TypeConverterRegistry.java:18`
- **Cluster:** quality | **Category:** correctness | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
public TypeConverterRegistry(ConversionConfig config) {
    this.config = config != null ? config : ConversionConfig.defaults();
    this.converterCache = config.isCacheConverters()      // <-- parameter, not this.config
            ? new ConcurrentHashMap<>(config.getInitialCacheCapacity())
            : new ConcurrentHashMap<>();
}
```

**Impact.** `new TypeConverterRegistry(null)` throws NullPointerException on line 18 — the defaulting on line 17 is dead. The API visibly advertises null-tolerance (that is the only reason line 17 exists), so a caller passing an unset optional config gets a bare NPE from library internals instead of default behavior.

**Fix.** Use `this.config` on lines 18-19.

### `NP-023` — Deep-search depth counter is never decremented on SKIP_SUBTREE, disabling the search entirely

- **Location:** `src/main/java/io/github/pierce/files/FileFinder.java:976`
- **Cluster:** quality | **Category:** correctness | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
    currentDepth++;
    if (currentDepth > maxDepth) return FileVisitResult.SKIP_SUBTREE;
    String dirName = dir.getFileName().toString();
    if (dirName.startsWith(".") || dirName.equals("node_modules") || ... ) return FileVisitResult.SKIP_SUBTREE;
    return FileVisitResult.CONTINUE;
}
```
Per the FileVisitor contract, returning SKIP_SUBTREE means `postVisitDirectory` (the only place `currentDepth--` happens, line 1006) is **not** invoked for that directory. So every skipped directory permanently increments the counter.

**Impact.** maxSearchDepth defaults to 5. In a typical repo root the visitor skips `.git`, `.idea`, `.mvn`, `target`, `build` — five skips — after which `currentDepth` is stuck above 5 and *every* subsequent directory returns SKIP_SUBTREE from the depth check. Strategy 5 ("deep search as last resort") therefore finds nothing on any real project, and files that do exist are reported as missing with a misleading "searched N locations" exception. The counter is redundant anyway: `Files.walkFileTree(start, opts, config.maxSearchDepth, searcher)` already enforces depth.

**Fix.** Delete `currentDepth`/`maxDepth` from DeepFileSearcher and rely on the maxDepth argument already passed to walkFileTree.

### `NP-024` — Static schema cache is shared across loaders with different target directories

- **Location:** `src/main/java/io/github/pierce/AvroSchemaLoader.java:41`
- **Cluster:** quality | **Category:** correctness | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
private static final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
private static final Map<String, StructType> structTypeCache = new ConcurrentHashMap<>();
```
`loadAvroSchema` keys on `normalizeSchemaName(schemaName)` only (line 137-152); `loadFlattenedSchema` keys on `schemaName + ":" + includeArrayStatistics` (line 162). Neither key includes `targetDirectory` or the builder's `additionalSearchPaths`, even though the Builder exposes `withTargetDirectory(...)` precisely to select between schema locations.

**Impact.** `new Builder().withTargetDirectory("/schemas/prod").build().loadAvroSchema("user")` followed by `new Builder().withTargetDirectory("/schemas/staging").build().loadAvroSchema("user")` returns the **prod** schema to the staging loader. In a multi-tenant or multi-environment driver this silently applies one tenant's schema to another's data. The `containsKey`-then-`get` pattern is also a non-atomic check-then-act: a concurrent `clearCaches()` between the two calls makes `loadAvroSchema` return null instead of throwing, NPE-ing the caller.

**Fix.** Include targetDirectory and the resolved search-path list in both cache keys, and collapse the check-then-act into a single `Schema s = schemaCache.get(key); if (s != null) return s;` (or `computeIfAbsent`).

### `NP-025` — Library writes 15 println calls and 2 printStackTrace to stdout/stderr, including record contents

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:218`
- **Cluster:** quality | **Category:** logging | **Effort:** small
- **Complexity:** O(arrays × records) console writes under a global lock
  - → O(0) when TRACE disabled

**Evidence.** 14 `System.err.println` calls in JsonFlattenerConsolidator (lines 92, 93, 94, 100, 127, 179, 218, 219, 220, 223, 475, 481, 497, 503) plus `e.printStackTrace()` at lines 128 and 180; one more `System.err.println` in AvroReconstructor.groovy:778. The worst is per-record data dumping:
```java
System.err.println("Flattened data sample (first 10 keys):");
for (Map.Entry<String, Object> entry : flattened.entrySet()) {
    System.err.println("  " + entry.getKey() + " = " + entry.getValue());
```
and `shouldKeepAsArrayElements`, which prints 2 lines for **every array node of every record** (lines 475/481/497/503). The class already sits in a codebase where every other class uses SLF4J.

**Impact.** Two problems. (1) Privacy: field names and values — which for this library are customer records — are written verbatim to stderr with no level control, no redaction, and no way for an operator to turn them off; in Spark these land in YARN/K8s executor logs that are retained and often world-readable within the org. (2) Throughput: `System.err` is an unbuffered, globally synchronized PrintStream; emitting several lines per array per record serializes every executor thread on the console lock and can dominate runtime. `printStackTrace` also bypasses the log aggregation that operators rely on.

**Fix.** Replace all of them with the SLF4J logger the rest of the codebase uses; put the per-array trace behind `log.isTraceEnabled()`; delete the flattened-data dump entirely or reduce it to key names at DEBUG; replace `e.printStackTrace()` with `log.error("...", e)`. Add a checkstyle/PMD rule banning System.out/System.err/printStackTrace in src/main so this cannot regress.

### `NP-026` — Blanket catch returns "{}" and a substring check drops valid records

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:99`
- **Cluster:** quality | **Category:** correctness | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
if (trimmed.contains(": undefined") || trimmed.contains(": NaN")) {
    System.err.println("JSON validation failed: contains undefined or NaN");
    return "{}";
}
...
} catch (Exception e) {
    System.err.println("Error processing JSON: " + e.getMessage());
    e.printStackTrace();
    return "{}";
}
```
The substring test is applied to the raw document text, not to parsed tokens. (`MALFORMED_JSON_PATTERN`, the precise regex written for this at line 41, is compiled and never used.)

**Impact.** A perfectly valid record such as `{"note":"result: undefined behavior in v2"}` or `{"log":"score: NaN reported"}` is discarded and replaced by `{}`. Combined with the catch-all, *any* processing failure also degrades to `{}`. Downstream, `from_json` on `{}` yields a row of all-nulls that passes the pipeline's `_error IS NULL` success filter — so dropped records are counted as **successes** in ProcessingMetrics and land in the output table as null rows. Silent, self-concealing data loss.

**Fix.** Delete the substring check and let Jackson's parser reject genuinely malformed input (use the already-compiled MALFORMED_JSON_PATTERN only if lenient pre-cleaning is truly required). Replace the catch-all `return "{}"` with a distinguishable sentinel or a thrown exception so the pipeline's error-handling strategy — not the flattener — decides whether to skip, quarantine, or fail.

### `NP-027` — Reading a stream joins lines with no separator, corrupting comment-bearing JSON

- **Location:** `src/main/groovy/io/github/pierce/JsonFlattener.groovy:736`
- **Cluster:** quality | **Category:** correctness | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
while ((line = reader.readLine()) != null) {
    sb.append(line);          // no newline appended
}
return from(sb.toString());
```
Meanwhile the shared mapper is built with `mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true)` (line 235). This affects every File and Path input, since `from(File, InputOptions)` delegates here.

**Impact.** With comments enabled, `//` runs to end-of-line — but the newlines have just been stripped. A config-style JSON file containing `{ // user record\n "id": 1 }` is joined into `{ // user record "id": 1 }`, so the entire remainder of the document becomes a comment and parsing fails with an unexpected-EOF error that points at the wrong place, or (for a file ending in `}`) silently yields a truncated object. Reading the whole file into a StringBuilder also defeats the streaming design for large inputs.

**Fix.** Use `Files.readString(path, charset)` / `new String(inputStream.readAllBytes(), charset)` instead of line-joining, or append `'\n'` after each line. Consider disabling ALLOW_COMMENTS/ALLOW_SINGLE_QUOTES by default since non-standard JSON acceptance is a parsing-differential hazard when the same bytes are read by another engine.

### `NP-028` — toStream closes the caller's OutputStream; toFile truncates the target before serializing

- **Location:** `src/main/groovy/io/github/pierce/JsonFlattener.groovy:1023`
- **Cluster:** quality | **Category:** correctness | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
public void toStream(OutputStream outputStream, OutputOptions options) {
    OutputStream os = options.isGzipped() ? new GZIPOutputStream(outputStream) : outputStream;
    try (Writer writer = new OutputStreamWriter(os, options.getCharset())) { ... }
}
```
Closing the OutputStreamWriter closes the wrapped caller-owned stream. Separately, `toFile` (line 983) opens `new FileOutputStream(file)` — which truncates immediately — and only then calls `toJson(options)`, which can throw JsonValidationException or JsonFlattenException.

**Impact.** `flattener.toStream(httpResponse.getOutputStream())` closes the servlet response stream, so nothing can be appended and the container throws on commit; writing two documents to the same stream fails the second time with "Stream closed". For toFile: if validation rules reject the record, the destination file has already been truncated to zero bytes — running the pipeline over an existing output file destroys it and writes nothing. The GZIP wrap in both methods also leaks the underlying stream if the GZIPOutputStream constructor throws.

**Fix.** Do not close streams you do not own: flush the writer without closing (or wrap in a CloseShieldOutputStream), closing only the GZIPOutputStream you created. In toFile, serialize to a String first and only then open the file — or write to a temp file and atomically move it into place.

### `NP-029` — ConversionConfig's error-handling and null-handling modes are never read — strict()/lenient() are partly no-ops

- **Location:** `src/main/java/io/github/pierce/converter/ConversionConfig.java:118`
- **Cluster:** quality | **Category:** correctness | **Effort:** medium
- **Complexity:** N/A
  - → N/A

**Evidence.** Grepping the whole converter package for each getter shows `getErrorHandlingMode()`, `getNullHandlingMode()`, `isAllowMissingOptionalFields()`, and `isEnableTypePromotion()` have **zero** call sites outside ConversionConfig itself. Yet `ConversionConfig.strict()` sets `errorHandlingMode(FAIL_FAST)` and `lenient()` sets `errorHandlingMode(COLLECT_ERRORS)`, and the enum is documented with three distinct behaviours (FAIL_FAST / COLLECT_ERRORS / SKIP_ON_ERROR).

**Impact.** A user who builds `ConversionConfig.builder().errorHandlingMode(SKIP_ON_ERROR).build()` to keep a pipeline running past bad fields still gets fail-fast behaviour: the TypeConversionException propagates out of `convert()` and kills the Spark task. Likewise `nullHandlingMode(PASS_THROUGH)` and `nullHandlingMode(USE_DEFAULTS)` are ignored — `SchemaBasedMapConverter.convert` hardcodes "skip nulls for required fields" (line 483-489). The configuration surface promises safety controls it does not implement.

**Fix.** Either implement the modes in SchemaBasedMapConverter.convert / StructConverter (dispatch on getErrorHandlingMode to choose between throw / Notification-collect / skip, and on getNullHandlingMode for required-null handling) or delete the unimplemented options and the factory presets that set them, so the API cannot mislead.

### `NP-030` — Static ThreadLocal is cleared but never removed — retains state on pooled threads

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:36`
- **Cluster:** quality | **Category:** concurrency | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
private static final ThreadLocal<Set<String>> arrayFieldsThreadLocal =
        ThreadLocal.withInitial(HashSet::new);
```
Only ever `.get().clear()` at lines 110 and 157 — there is no `.remove()` anywhere. Note MapFlattener.groovy gets this right (line 251: `CONTEXT.remove()` in a finally block).

**Impact.** On pooled threads (Spark executor task threads, servlet worker threads) each thread's ThreadLocalMap permanently retains a HashSet whose entries are the array field paths of the largest record that thread has ever seen — unbounded in size for wide documents, and never reclaimed. In a container this also pins the JsonFlattenerConsolidator class and its classloader, blocking redeploy-time unloading (the classic ThreadLocal classloader leak). The `clear()`-at-entry design also means the set is live-but-stale between calls, so any future code path that reads it before `flattenAndConsolidateJson` runs sees the previous record's data.

**Fix.** Wrap the body of the public entry points in try/finally and call `arrayFieldsThreadLocal.remove()` in the finally, exactly as MapFlattener does.

### `NP-031` — Flattened schema of a namespace-less Avro record gets the literal namespace "null.flattened"

- **Location:** `src/main/java/io/github/pierce/AvroSchemaFlattener.java:142`
- **Cluster:** quality | **Category:** correctness | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
String flatNamespace = schema.getNamespace() + ".flattened";
String flatName = "Flattened" + schema.getName();
Schema flattenedSchema = Schema.createRecord(flatName, "...", flatNamespace, false);
```
`Schema.getNamespace()` returns null for records declared without a namespace, which is legal Avro. Java string concatenation renders that as the four characters `null`, and `null` happens to be a syntactically valid Avro name component, so Avro accepts it silently.

**Impact.** `{"type":"record","name":"User","fields":[...]}` flattens to a record whose full name is `null.flattened.FlattenedUser`. That name is then used as the cache key in getFlattenedSchema and CreateSparkStructFromAvroSchema, is embedded in any serialized Avro output, and appears in the schema registry — so every namespace-less schema in the system collides into the same `null.flattened.*` namespace and produces confusing, unqueryable identifiers.

**Fix.** `String flatNamespace = schema.getNamespace() == null ? "flattened" : schema.getNamespace() + ".flattened";`

### `NP-032` — Excel export throws NumberFormatException on any comma-decimal locale

- **Location:** `src/main/java/io/github/pierce/AvroSchemaFlattener.java:807`
- **Cluster:** quality | **Category:** correctness | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
private String calculateComplexityScore() {
    double score = ...;
    return String.format("%.1f", score);      // locale-sensitive
}
private String getComplexityLevel() {
    double score = Double.parseDouble(calculateComplexityScore());   // locale-INsensitive
    ...
}
```
`String.format` without a Locale uses `Locale.getDefault(FORMAT)`; `Double.parseDouble` only ever accepts '.' as the decimal separator.

**Impact.** On a JVM with a European default locale (de_DE, fr_FR, pt_BR, ...) calculateComplexityScore returns "12,5" and `Double.parseDouble("12,5")` throws `NumberFormatException: For input string: "12,5"`. That propagates out of getComplexityLevel → createComplexityAssessmentSection → createExecutiveSummarySheet → `exportToExcel`, so the entire schema-analysis export fails on those machines while working fine in CI under en_US — a classic works-on-my-machine failure. Round-tripping a double through a formatted string is also lossy and pointless.

**Fix.** Keep the numeric value: make `calculateComplexityScore()` return `double`, add a separate `formatComplexityScore()` for display, and pass `Locale.ROOT` to every `String.format` that produces machine-reparsed text.

### `NP-033` — Default flattener config silently truncates arrays >1000 and maps >10000 entries at DEBUG level

- **Location:** `src/main/groovy/io/github/pierce/MapFlattener.groovy:448`
- **Cluster:** quality | **Category:** correctness | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
int limit = Math.min(list.size(), maxArraySize);
if (list.size() > maxArraySize && log.isDebugEnabled()) {
    log.debug("Array size ({}) exceeds maxArraySize ({}), truncating at key: {}", ...);
}
```
and in flattenObject (line 301): `if (entryCount >= maxMapSize) { log.warn("Map size limit reached at depth {}", depth); break; }`. Defaults are maxArraySize=1000, maxMapSize=10000 (Builder lines 1171-1172). The depth path is similar — at maxDepth the remaining subtree is replaced by a JSON string with only a DEBUG line.

**Impact.** An input array of 5,000 order line items is silently flattened as if it had 1,000; the emitted `orders_id_count` statistic and the joined value string both reflect the truncated data, so a downstream reconciliation sees a plausible but wrong total with no error anywhere. Because production log levels are almost always INFO or above, the DEBUG guard means the loss leaves no trace at all. The `log.warn` for maps does not name the affected key, so it is not actionable either.

**Fix.** Emit a WARN (unconditionally, including the key path and both counts) on every truncation, and add a `failOnTruncation` option — defaulting to true for a data-integrity library — so silent loss must be opted into. Surface a truncation count on the result so callers can assert on it.

### `NP-034` — consolidateFlattened's lossy '.'↔'_' round-trip misclassifies array fields and drops statistics columns

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:639`
- **Cluster:** quality | **Category:** correctness | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** Flattening uses '.' as the path separator, then `consolidateFlattened` does `String consolidatedKey = key.replace(".", "_")` (line 615). `processGroupedValues` then attempts to invert that: `String originalBaseKey = consolidatedKey.replace("_", ".")` (line 639) and prefix-matches it against the recorded `arrayFieldsThreadLocal` paths to decide `wasTrackedAsArray`.

**Impact.** The inverse is wrong whenever an original JSON key contains an underscore. Path `data.user_ids` becomes `data_user_ids`, which inverts to `data.user.ids` and no longer prefix-matches the tracked array path `data.user_ids`, so `wasTrackedAsArray` is false and the `_count`, `_distinct_count`, `_min_length`, `_max_length`, `_avg_length`, `_type` columns are never emitted for that field. The Avro-derived Spark StructType *does* declare those columns (AvroSchemaFlattener.addArrayStatisticsFields), so `from_json` produces nulls for all six — statistics silently vanish for exactly the snake_case field names that dominate real schemas. The forward direction also collides: `{"a.b":1}` and `{"a":{"b":2}}` both map to `a_b` and the second overwrites the first.

**Fix.** Stop reconstructing the original path from the consolidated key. Carry the original dotted path alongside the consolidated key in `KeyedValue` (it is already stored as `originalFlattenedKey`) and match `arrayFieldsThreadLocal` against that, using `ARRAY_INDEX_STRIP_PATTERN` on the original rather than a `replace` on the mangled form.

### `NP-035` — Case-insensitive field lookup silently collapses distinct schema fields and is locale-dependent

- **Location:** `src/main/java/io/github/pierce/converter/SchemaBasedMapConverter.java:651`
- **Cluster:** quality | **Category:** correctness | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** `fieldNameLookup.put(fieldName.toLowerCase(), fieldName)` is executed once per field in all three initializers (lines 381, 394, 414) with no collision check, and `findMatchingField` does `fieldNameLookup.get(inputKey.toLowerCase())` (line 651). Neither call passes a Locale. Case-insensitive matching is the default in every factory method (`forIceberg(schema)` → `caseInsensitive = true`).

**Impact.** (a) An Avro record with both `userId` and `userid` — legal, and common after schema evolution or a flattening collision — produces one lookup entry; the field registered first becomes unreachable, and `convert()` silently `continue`s past its input values, dropping the column with no error. (b) Locale: on a Turkish-locale JVM `"ID".toLowerCase()` is "ıd" (dotless) while an input key of "id" lowercases to "id", so the field stops matching and is silently dropped — the pipeline produces different output on a tr-TR host than on an en-US host.

**Fix.** Use `toLowerCase(Locale.ROOT)` on both sides, and detect collisions at initialization: if `fieldNameLookup.put(...)` returns non-null, throw an IllegalArgumentException naming both fields rather than silently discarding one.

### `NP-036` — Avro schemas are decoded with the platform default charset

- **Location:** `src/main/java/io/github/pierce/AvroSchemaLoader.java:332`
- **Cluster:** quality | **Category:** correctness | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
String content = new String(Files.readAllBytes(schemaPath));
return new Schema.Parser().parse(content);
```
No Charset argument, so `Charset.defaultCharset()` applies. Same defect at line 345: `new InputStreamReader(fs.open(hdfsPath))` for the HDFS path.

**Impact.** Avro schema files are UTF-8 by specification. On a Windows JVM without `-Dfile.encoding=UTF-8` (default cp1252) or an ASCII-locale container (ANSI_X3.4-1968), any non-ASCII byte in a `doc` string, a string field `default`, or an enum symbol is mojibake'd. Non-ASCII enum symbols and defaults become part of the parsed schema, so records validate against subtly different values than on a UTF-8 host, and the schema fingerprint differs between environments — breaking schema-registry compatibility checks in a way that only reproduces on some machines.

**Fix.** `new String(Files.readAllBytes(schemaPath), StandardCharsets.UTF_8)` and `new InputStreamReader(fs.open(hdfsPath), StandardCharsets.UTF_8)`. Better: pass the InputStream directly to `Schema.Parser.parse(InputStream)`, which handles encoding itself.

### `TA-007` — Stated fact is wrong: JsonFlattenerConsolidatorPerformanceTest is NOT entirely commented out — 4 of 6 tests are live and ran today; the 2 commented ones are flaky wall-clock ratio assertions, not API breakage

- **Location:** `src/test/java/io/github/pierce/FlattenConsolidatorTests/JsonFlattenerConsolidatorPerformanceTest.java:26`
- **Cluster:** tests | **Category:** fact-correction | **Effort:** small
- **Complexity:** 4 flaky timing tests in the default build + 2 commented
  - → 0 timing assertions in the default build; deterministic cache/behaviour assertions instead

**Evidence.** The file has 4 ACTIVE @Test methods (testDeepNestingPerformance L111, testComplexStructurePerformance L145, testThroughput L200, testMemoryEfficiency L231) and only 2 commented (testStatisticsOverhead L26-67, testArraySizePerformance L69-109). Proof it executes: target/surefire-reports/TEST-io.github.pierce.FlattenConsolidatorTests.JsonFlattenerConsolidatorPerformanceTest.xml records 4 tests, 0 failures, 1.96s, timestamped 2026-08-09. Git shows why the confusion: b1023d0 commented the WHOLE file, then c7b3dde restored 4 of 6. The 2 still-commented tests are NOT API-broken — testStatisticsOverhead uses the 6-arg constructor `new JsonFlattenerConsolidator(",", null, 50, 1000, false, true)` which still exists at JsonFlattenerConsolidator.java:44-57. Their actual problem is the assertions: `assertThat(overhead).isLessThan(50.0)` (relative % between two timed loops) and `assertThat(times.get(times.size()-1) / times.get(0)).isLessThan(20.0)` (ratio of first vs last timing). The 4 LIVE ones are equally non-deterministic: `assertThat(avgMs).isLessThan(5.0)` (L197), `assertThat(count).isGreaterThan(100)` ops/sec over a `while (System.currentTimeMillis() < end)` busy loop (L219-228), and `assertThat(memDelta).isLessThan(10*1024*1024)` after two `System.gc(); Thread.sleep(100);` pairs (L242-264).

**Impact.** The audit premise 'the whole performance suite is dead code' is false and would misdirect remediation. The real problem is inverted: the 4 tests that ARE running are the flakiest in the repo — absolute millisecond thresholds, a throughput floor, and a `System.gc()`-based memory delta all measured on shared CI hardware. Repo-wide there are 8 wall-clock/memory assertions: AvroSchemaLoaderTest.java:229 `assertThat(secondLoadTime).isLessThan(firstLoadTime / 2)`, CreateSparkStructFromAvroSchemaTest.java:325 `assertThat(secondTime).isLessThan(firstTime / 10)`, FileFinderComprehensiveTest.java:734 `<5000ms`, JsonFlattenerExplosionTest.java:323 `<1000ms` and :724 `<5000ms`, plus the three above. The two cache-warmup ratio assertions (`/2`, `/10`) are the most fragile — JIT warmup alone can invert them.

**Fix.** Do not simply uncomment the 2. Move all 6 timing tests behind a `performance` Maven profile or a JUnit `@Tag("perf")` excluded from the default surefire run, and convert the assertions from absolute wall-clock to invariants that are actually deterministic (e.g. assert the cache returns the identical object reference instead of asserting it is 10× faster; assert allocation counts or result equality rather than ms). If real benchmarking is wanted, use JMH in a separate module.

### `TA-008` — AvroSchemaFlattenerCompatibilityTest is tautological — it claims to compare two implementations but compares one implementation against itself

- **Location:** `src/test/java/io/github/pierce/avroTesting/AvroSchemaFlattenerCompatibilityTest.java:212`
- **Cluster:** tests | **Category:** assertion-quality | **Effort:** small
- **Complexity:** 2 tests with 4 always-true assertions
  - → 2 tests pinned to checked-in reference output

**Evidence.** Class javadoc (lines 16-19): 'Verifies that the optimized iterative implementation produces identical results to the original recursive implementation'. The helper it relies on, `verifyIdenticalOutput(String schemaJson)` at lines 212-235, does: `Schema flattened1 = flattener.getFlattenedSchema(schema); ... AvroSchemaFlattener.clearCache(); Schema flattened2 = flattener.getFlattenedSchema(schema); ... assertThat(fields1).isEqualTo(fields2);` — the SAME `flattener` instance and the SAME method on both sides. The 'original recursive implementation' no longer exists in src/main. Both non-parameterized tests (`testIdenticalOutputForAllTestCases` L32, `testEdgeCases` L93) consist solely of calls to this helper (4 call sites total).

**Impact.** Two of the repo's 636 active tests assert a proposition that is true by construction and cannot fail for any implementation, correct or not. They inflate the test count and the 93.3% AvroSchemaFlattener coverage figure without providing a single behavioural guarantee. Any mutation to getFlattenedSchema survives both. Note the third method in the file, `testWithAndWithoutStatistics` (@ParameterizedTest, L160), IS a real test — it asserts specific generated field names like `tags_count`, `tags_min_length`, `nested_values_distinct_count`.

**Fix.** Either delete the two tautological tests and the helper, or convert them into golden-file tests: check the expected flattened field-name lists into src/test/resources as reference fixtures and assert against those. That converts a self-comparison into an actual regression detector and simultaneously addresses the missing-golden-dataset gap (TA-012).

### `TA-009` — Dead test-support code: SchemaTestUtils has zero callers, SparkTestBase is referenced only from commented files, and 4 example/debug files (748 lines) match no surefire include

- **Location:** `src/test/java/io/github/pierce/SchemaTestUtils.java:12`
- **Cluster:** tests | **Category:** dead-code | **Effort:** small
- **Complexity:** ~1270 lines of never-executed test-tree code
  - → test tree contains only executable tests plus live helpers

**Evidence.** `grep -rn SchemaTestUtils src/test | grep -v SchemaTestUtils.java` → zero hits. 100 lines of unused helpers (schemasAreEquivalent, extractFieldNames, countStatisticsFields, assertSparkSchemaContainsFields, getFieldType). `grep -rn SparkTestBase` → only 3 hits, ALL inside commented lines (SchemaCompatibilityIntegrationTest.java:41, NexusPiercerSparkPipelineTest.java:3 and :18) — so this 29-line base class is currently unreachable. Seven test-source files match none of the surefire include patterns and therefore never execute: AvroReconstructorDebugger.groovy (251 lines, 0 @Test), MapFlattenerExamples.groovy (109, 0), StreamingProcessorExample.groovy (87, 0), AvroSchemaLoaderUsageExamples.java (301, 0), TypeConverterProperties.java (392, 26 @Property — see TA-003), SchemaTestUtils.java (100), SparkTestBase.java (29). Additionally JsonReconstructorTest.groovy, being 100%-commented Groovy, still compiles to a real class — target/test-classes/JsonReconstructorTest.class exists (an empty groovy.lang.Script shell) and is scanned by surefire's `**/*Test.class` each run, contributing nothing.

**Impact.** ~1270 lines of test-tree code that is compiled on every build, counted in nothing, and drifts silently. SchemaTestUtils in particular duplicates capability that the live tests hand-roll inline. AvroReconstructorDebugger.groovy is a 251-line debugging harness (FieldTrace class, tree-view generator) shipped in the test tree with no entry point.

**Fix.** Delete SchemaTestUtils.java unless the revived Spark tests will use it (check during TA-002). Keep SparkTestBase — it becomes live again the moment NexusPiercerSparkPipelineTest is uncommented, and NexusPiercerFunctionsTest should be migrated onto it (TA-002). Move the four *Examples/*Debugger files to a `src/test/examples` non-compiled directory or into docs, or convert them into real tests with assertions.

### `TA-010` — No shared fixtures or golden datasets — 91 distinct Avro schemas are inlined as text blocks across tests, and the only two checked-in .avsc fixtures generate classes nobody uses

- **Location:** `src/test/avro/user_schema.avsc:1`
- **Cluster:** tests | **Category:** fixtures | **Effort:** medium
- **Complexity:** 91 inline schemas, 0 golden files, 2 unused .avsc, 1 orphan properties file
  - → shared .avsc fixtures + golden reference outputs, one loader

**Evidence.** The entire non-source content of src/test is four files: src/test/avro/product_schema.avsc, src/test/avro/user_schema.avsc, src/test/resources/application-test.properties, src/test/resources/logback-test.xml. There are ZERO golden/reference output files. Programmatic scan found 91 DISTINCT inline Avro record-schema text blocks (`"""..."type"..."record"..."""`, >120 normalized chars) across the Java tests, of which only 2 are shared by more than one file (['AvroSchemaFlattenerTest.java','CreateSparkStructFromAvroSchemaTest.java'] and ['AvroSchemaFlattenerCompatibilityTest.java','AvroSchemaFlattenerOrderingTest.java','AvroSchemaFlattenerTest.java']) — i.e. 89 one-off schemas. The Groovy side inlines schemas via SchemaBuilder instead (e.g. `SchemaBuilder.record("Address")` appears at 4 sites in AvroFlattenReconstructStressTest.groovy alone and 5+ in AvroSchemaFlattenerAlignmentTest.groovy). The avro-maven-plugin `test-schemas` execution (pom.xml:1061-1072) generates Address, UserRecord, ProductRecord, ProductSpecs into target/generated-test-sources/avro — grep for those type names in test sources returns only unrelated SchemaBuilder string literals; the generated classes are never imported or instantiated. src/test/resources/application-test.properties is loaded by nothing (`grep -rn application-test src/` → no code reference) and its `schema.names=user_schema,product_schema,complex_schema` names a `complex_schema` that does not exist.

**Impact.** Every schema-shaped behaviour is re-specified from scratch in each test, so a change to the flattening contract requires editing ~91 places; there is no single artifact that pins 'what correct flattened output looks like'. Absence of golden files is why TA-008's compatibility test degenerated into a self-comparison. The unused avro codegen adds a build step and 8 generated classes for nothing, and the orphan properties file is actively misleading (it advertises a config-driven test setup that does not exist).

**Fix.** Introduce src/test/resources/schemas/*.avsc for the recurring schemas and a src/test/resources/golden/*.json set holding expected flattened output and expected flattened Avro schemas, loaded through one shared fixture helper (a good home for the resurrected SchemaTestUtils). Either wire the generated Avro test classes into a test or drop the `test-schemas` execution. Delete or actually load application-test.properties.

### `TA-011` — JaCoCo gates are set far below actual coverage and the class-miss ceiling has only 14 classes of headroom

- **Location:** `pom.xml:187`
- **Cluster:** tests | **Category:** quality-gate | **Effort:** small
- **Complexity:** gate 20% vs actual 60.26%; 36/167 classes at 0%, ceiling 50
  - → ratcheted gate at ~58%; zero-coverage classes reduced by reviving spark + 3 new test classes

**Evidence.** pom.xml:187 sets `<jacoco.minimum.coverage>0.20</jacoco.minimum.coverage>` and pom.xml:1164-1180 enforces BUNDLE INSTRUCTION COVEREDRATIO >= 0.20 plus CLASS MISSEDCOUNT <= 50. Measured from target/site/jacoco/jacoco.csv (run of 2026-08-09): actual instruction coverage is **60.26% (26711/44323)** — 3× the gate — and **36 of 167 classes have zero instruction coverage**, i.e. 14 classes below the MISSEDCOUNT ceiling of 50. The 36 zero-coverage classes are dominated by the untested spark package (7 classes/inner classes) and converter internals: SchemaConversionUtil (0/636 incl. FieldType 0/117, FieldDefinition 0/23, FieldInfo 0/18), TimestampNanoConverter (0/330), AvroSchemaUtilWrapper (0/275), AvroSchemaConverter$AvroUnionConverter (0/82), SchemaBasedMapConverter$AvroUnionConverter (0/71), SchemaBasedMapConverter$AvroEnumConverter (0/40).

**Impact.** The INSTRUCTION gate cannot detect a two-thirds regression in coverage — it would pass at 20%. The CLASS gate is the opposite problem: it is 14 classes from breaking the build, so the next handful of new untested classes will fail `mvn verify` for a reason unrelated to the change that triggered it. Three main classes have no test file AND no test reference at all: AvroSchemaUtilWrapper.java, SchemaConversionUtil.java, TimestampNanoConverter.java (cross-checked: zero mentions in any active test source). AbstractTypeConverter.java also has no dedicated test file (47.4% incidental coverage via subclasses).

**Fix.** Raise `jacoco.minimum.coverage` to a ratchet just under current (0.58) so regressions are caught, and raise CLASS MISSEDCOUNT to ~45 only after reviving the spark tests drops the zero-coverage count. Add dedicated test classes for the three fully-untested converter/utility classes (SchemaConversionUtil at 636 instructions is the largest single untested non-Spark unit). Note the `development` profile is activeByDefault (pom.xml:1306-1317) and disables checkstyle/pmd/spotbugs, so those three quality gates are off unless `-Pquality` is passed.

### `TA-012` — Two active tests contain no assertion at all, and one of them tests Jackson rather than project code while swallowing every exception

- **Location:** `src/test/groovy/NestedArrayDiagnosticTest.groovy:165`
- **Cluster:** tests | **Category:** assertion-quality | **Effort:** small
- **Complexity:** 2 assertion-free tests, 9 diagnostic tests, 10 swallowing catches, 409 prints
  - → every active test asserts; console output routed through logback

**Evidence.** Parsing all 653 active test methods (636 @Test + 17 @ParameterizedTest) and treating delegation to an assertion-bearing helper as valid, exactly 2 have no assertion path: NestedArrayDiagnosticTest.groovy:165 `testDeserializeArrayStatic` and DoublyNestedArrayTest.groovy:105 `debugShowFlattenedStructure`. `testDeserializeArrayStatic` is the worse of the two — its body constructs 6 JSON strings, calls `mapper.readValue(testCase, List.class)` on Jackson's own ObjectMapper (no NexusPiercer code involved), prints the results, and wraps everything in `catch (Exception e) { println("  Error: ${e.message}") }`, so it cannot fail under any circumstance. A first-pass scan over-reported 81 assertion-free tests; that was a false positive — 46 tests in AvroFlattenReconstructStressTest.groovy and 30 in AvroSchemaFlattenerAlignmentTest.groovy delegate to `verifyRoundTrip(...)`/`verifyAlignment(...)` helpers that do assert, which is a legitimate pattern. Broader smells confirmed: 9 tests are @DisplayName'd 'Diagnostic:' or 'Debug:' (4 in DeepNestingDiagnosticTest, 4 in NestedArrayDiagnosticTest, 1 in DoublyNestedArrayTest); 10 catch blocks in active test code swallow the exception with no assert/fail/throw (5 in SchemaBasedMapConverterTest.java, 1 each in AvroSchemaLoaderUsageExamples.java, EdgeCaseTest.java, FileFinderComprehensiveTest.java, JsonFlattenerConsolidatorComprehensiveTest.java, JsonFlattenerTest.groovy); and there are 409 print statements across 19 test files (61 in NestedArrayDiagnosticTest.groovy, 59 in NexusPiercerExamplesTest.java, 50 in DeepNestingDiagnosticTest.groovy, 38 in BracketAwareSplitTest.groovy). Overall assertion density is healthy: 1375 assertion calls over 653 active tests ≈ 2.1 per test. ANSWER TO THE DIRECT QUESTION ON System.out: **zero** tests assert on stdout — the only two ByteArrayOutputStream usages (JsonFlattenerTest.groovy:396, FileFinderComprehensiveTest.java:386) are legitimate API sinks passed to `JsonFlattener.toStream(baos)` and `FileFinder.Util.copyTo(path, baos)`, not System.setOut captures. No System.setOut/setErr anywhere.

**Impact.** Two tests contribute to the green count while proving nothing; 9 'diagnostic' tests exist to produce console output rather than to detect regressions; 10 swallowing catches can hide genuine failures inside otherwise-real tests. 409 print statements make the surefire output unreadable and slow the build, which is exactly the condition under which a real failure gets scrolled past.

**Fix.** Delete `testDeserializeArrayStatic` (it tests a third-party library) and `debugShowFlattenedStructure`, or give them assertions. Convert the 9 Diagnostic/Debug tests either into asserting tests or into disabled-by-default `@Tag("diagnostic")` methods. Replace println/System.out.println with SLF4J at debug level — logback-test.xml already exists to control it. Audit the 10 swallowing catches: each should either assert on the caught exception or let it propagate.


## LOW

### `NP-021` — Dead validation regex replaced by naive substring checks that miss most malformed input

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:41`
- **Cluster:** arch | **Category:** correctness | **Effort:** trivial

**Evidence.** Line 41 declares `private static final Pattern MALFORMED_JSON_PATTERN = Pattern.compile("[:,\\[]\\s*(undefined|NaN)\\s*[,\\}\\]]")` — a well-formed check that grep confirms is referenced nowhere. It was replaced by two naive substring tests: line 99 `trimmed.contains(": undefined") || trimmed.contains(": NaN")` and the duplicate at line 518 inside preprocessJson. Those miss `:undefined` with no space, `[undefined, 1]` in arrays, `,undefined`, and any whitespace variant the abandoned regex handled. Note also that the two validation paths (flattenAndConsolidateJson lines 84-102 and preprocessJson lines 507-523) are near-identical copies that have already drifted: only the first prints diagnostics.

**Impact.** Malformed JSON containing `undefined`/`NaN` in any form other than the exact `": undefined"` sequence passes validation, reaches Jackson, throws, is caught at line 126, and is silently converted to `"{}"` — a dropped record. The dead Pattern is a compiled-and-retained object documenting an intent the code no longer implements.

**Fix.** Delete the duplicated inline validation from flattenAndConsolidateJson and call preprocessJson from both entry points. Inside preprocessJson, use MALFORMED_JSON_PATTERN as originally intended, or drop the pre-check entirely and let Jackson be the single source of truth on well-formedness — the pre-check exists only to avoid an exception that the code catches anyway.

### `NP-022` — A 70-jar Groovy 5.0.0 distribution is vendored into lib/ while the build resolves Groovy 4.0.21

- **Location:** `pom.xml:150`
- **Cluster:** arch | **Category:** build | **Effort:** trivial

**Evidence.** `git status` reports `?? lib/` — untracked but present in the working tree — containing 70 files: the complete Apache Groovy 5.0.0 binary and sources distribution (groovy-5.0.0.jar, groovy-ant, groovy-console, groovy-groovysh, groovy-swing, groovy-servlet, groovy-testng, and so on, each with its -sources sibling). Meanwhile pom.xml:150 sets `<groovy.version>4.0.21</groovy.version>` and the dependencies at lines 383-392 resolve groovy and groovy-json 4.0.21 from Maven. No <systemPath> or repository declaration references lib/, so nothing in the build consumes it.

**Impact.** A major-version-ahead Groovy distribution sitting in the project root is a live footgun: any IDE that auto-adds lib/ to the module classpath (IntelliJ does this for a directory named lib) will compile and run against Groovy 5 while Maven builds against Groovy 4, producing local-vs-CI behaviour divergence that is very hard to diagnose. It also inflates the working tree by tens of megabytes and would be an accidental license/redistribution issue if ever committed.

**Fix.** Delete lib/ and add it to .gitignore. If the Groovy 5 upgrade is genuinely intended, do it by bumping the pom property and letting Maven resolve it. This becomes moot once NP-004 removes Groovy from the project entirely.

### `OSS-22` — docs/ mixes governance markdown with 160KB of unrenderable React/HTML visualizations in a Java library

- **Location:** `docs/flattening-visualization.jsx:1`
- **Cluster:** hygiene | **Category:** docs-placement | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** docs/ contains 9 markdown files plus flattening-visualization.jsx (33KB), business-focused-visualization.jsx (39KB), flattening-documentation.html (42KB), and business-focused-documentation.html (45KB). The .jsx files open with `import React, { useState, useEffect } from 'react';` but the repo has no package.json, no node tooling, and no bundler — nothing can build them. The HTML files pull webfonts from fonts.googleapis.com, so they render incompletely offline. They were added in the two duplicate 'Added detailed visualization for the logic and design decisions' commits (e698ae4, ab98da0).

**Impact.** GitHub renders .jsx as plain source and does not execute committed HTML, so neither asset is viewable where the docs live — the visualizations are effectively write-only. Their presence also implies a JavaScript toolchain that contributors will look for and not find, and it dilutes docs/ as the place to look for governance documents.

**Fix.** Either publish the HTML via GitHub Pages (move to a docs site branch or /docs Pages source) and link it from the new README, or move both pairs to an assets/ or design/ directory clearly marked as non-buildable reference material. If the .jsx sources have no build path, drop them and keep only the rendered HTML. Self-host or inline the fonts so the HTML renders offline.

### `OSS-23` — pom.xml defines gmavenplus.version twice in two separate property blocks

- **Location:** `pom.xml:206`
- **Cluster:** hygiene | **Category:** build-config | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** `<gmavenplus.version>3.0.2</gmavenplus.version>` appears at line 151 in the 'Groovy Configuration' block and again at line 206 in a second, later 'Testing versions' block that also holds `<spock.version>` (line 205). Maven silently takes the last definition. The pom is ~1,700 lines with several such overlapping property groupings.

**Impact.** Duplicate properties are a maintenance trap: editing the first occurrence has no effect, so a future Groovy-plugin upgrade (relevant given the Groovy 4-vs-5 conflict in OSS-06) can appear to be applied while the build keeps using the stale value. The second block's placement after the skip-configuration section also suggests properties are being appended ad hoc rather than maintained in one ordered list.

**Fix.** Delete the duplicate at line 206, fold spock.version into the Groovy Configuration block so all Groovy/Spock versions live together, and add the maven-enforcer `banDuplicatePomDependencyVersions` rule (already available — the enforcer plugin is configured) to catch recurrences.

### `GIT-001` — .gitattributes has only `* text=auto` — no binary or eol rules for Avro/Excel/jar test fixtures

- **Location:** `.gitattributes:1`
- **Cluster:** infra | **Category:** repo-hygiene | **Effort:** trivial

**Evidence.** The entire file is two lines: a comment and `* text=auto`. The repo contains `src/test/avro/product_schema.avsc` and `user_schema.avsc`, and the toolkit's own domain is Avro/Parquet/Excel — `poi-ooxml` is a compile-scope dependency, so `.xlsx` fixtures are a natural future addition. There are no `binary`, `-text`, or `eol=lf` declarations, and no `*.sh text eol=lf`. Development is on Windows (`OS name: "windows 11"` from `mvn -v`) while the intended CI is `ubuntu-latest`.

**Impact.** `text=auto` autodetection is heuristic. Any binary fixture git misclassifies gets CRLF-mangled on a Windows checkout, producing byte-level test failures that reproduce on one platform only — the hardest class of bug to diagnose, and directly in this project's problem domain. Shell scripts committed from Windows would also get CRLF and fail on the Linux runner.

**Fix.** Add explicit rules: `*.avro binary`, `*.parquet binary`, `*.jar binary`, `*.xlsx binary`, `*.avsc text eol=lf`, `*.json text eol=lf`, `*.sh text eol=lf`, `mvnw text eol=lf` (once BLD-002 adds it), `*.cmd text eol=crlf`.

### `LIC-002` — .reuse/dep5 has a wrong upstream URL, uses the deprecated format, and contradicts the POM's inceptionYear

- **Location:** `.reuse/dep5:4`
- **Cluster:** infra | **Category:** licensing | **Effort:** trivial

**Evidence.** `Source: https://github.com/piercelonergan/nexuspiercer` — the real repository, per the POM's `<url>` and `<scm>` (25, 66-68), is `https://github.com/pierce-lonergan/NexusPiercer` (missing hyphen, wrong case). Every stanza declares `Copyright: 2025 Pierce Lonergan` while the POM declares `<inceptionYear>2024</inceptionYear>` (26) and `project.build.outputTimestamp` is `2024-01-01T00:00:00Z` (114). The `.reuse/dep5` format is deprecated as of REUSE 3.3 in favour of `REUSE.toml`. The per-directory stanzas cover `src/main/java/*`, `src/test/java/*`, `src/main/groovy/*` and `docs/*` but not `src/test/groovy/*`, `src/main/resources`, `src/test/resources`, `src/test/avro`, `src/main/checkstyle`, `src/main/pmd`, `maven-publish.yml` or `NOTICE` — those fall through to the `Files: *` catch-all, so compliance holds but the file gives a false impression of being exhaustive.

**Impact.** A dead URL in the machine-readable licence metadata, an inconsistent copyright year across three files, and a deprecated format. Nothing lints it because there is no CI (CI-001).

**Fix.** Fix the Source URL, reconcile the year to 2024 (or update inceptionYear to 2025), migrate to `REUSE.toml` via `reuse convert-dep5`, and add a `reuse lint` step to the new CI workflow.

### `TST-004` — Five compiled test-tree classes never execute, and two distinct test classes share the simple name AvroSchemaFlattenerTest

- **Location:** `pom.xml:1092`
- **Cluster:** infra | **Category:** test-execution | **Effort:** small

**Evidence.** Compiled into `target/test-classes` but absent from all 126 surefire reports: `io/github/pierce/AvroReconstructorDebugger.class`, `io/github/pierce/examples/MapFlattenerExamples.class`, `io/github/pierce/examples/StreamingProcessorExample.class`, `io/github/pierce/avroTesting/AvroSchemaLoaderUsageExamples.class`, `io/github/pierce/converter/TypeConverterProperties.class`. Checked each for annotations: the first four have `@Test`=0 and `@Property`=0 — they are `public static void main` demo/utility classes (`MapFlattenerExamples.groovy` line 9: `public static void main(String[] args)`), so excluding them is correct, but they are compiled on every build and counted nowhere. `TypeConverterProperties` is the genuine casualty (see TST-001, 26 `@Property`). Separately, `src/test/groovy/AvroSchemaFlattenerTest.groovy` compiles to `io.github.pierce.avro.AvroSchemaFlattenerTest` while `src/test/java/io/github/pierce/avroTesting/AvroSchemaFlattenerTest.java` compiles to `io.github.pierce.avroTesting.AvroSchemaFlattenerTest` — both appear in surefire-reports. Note also that all 17 files in `src/test/groovy/` sit flat in the directory root while declaring packages (`package io.github.pierce.examples`), so the directory layout does not mirror the package structure.

**Impact.** Minor build waste and real navigational confusion: two same-named test classes for the same production class in different packages, plus example code living in the test tree where it is compiled but never run or verified. A reader cannot tell which of the 46 test files are actually tests.

**Fix.** Move the four demo/utility classes to `src/test/java/.../examples/` with a clear naming convention, or better to a `src/examples` directory excluded from test compilation. Rename one of the two `AvroSchemaFlattenerTest` classes. Reorganise `src/test/groovy/` into package-matching subdirectories.

### `RECON-20` — GAvroSchemaFlattener constructs its own ObjectMapper per instance instead of sharing one

- **Location:** `src/main/groovy/io/github/pierce/GAvroSchemaFlattener.groovy:118`
- **Cluster:** perf-avro | **Category:** allocation-pressure | **Effort:** trivial
- **Complexity:** O(1) but with a cold Jackson type cache per instance
  - → One warm shared mapper per JVM

**Evidence.** ```groovy
public GAvroSchemaFlattener(AvroFlatteningConfig config) {
    this.config = config;
    this.objectMapper = new ObjectMapper();
}
```
AvroReconstructor by contrast uses `private static final ObjectMapper SHARED_OBJECT_MAPPER` (line 85). ObjectMapper construction builds the serializer/deserializer provider and its caches; a fresh instance starts with cold caches, so the first readValue/writeValueAsString for each type re-resolves the (de)serializer.

**Impact.** The class implements Serializable (line 41) for Spark shipping, so a new instance — and a new ObjectMapper with cold Jackson caches — materializes on each executor, and on each construction if callers build one per partition or per batch. Bounded, but free to fix.

**Fix.** Make it `private static final ObjectMapper SHARED_MAPPER = new ObjectMapper();` and mark the field transient/static, matching AvroReconstructor. ObjectMapper is thread-safe once configured.

### `RECON-21` — JsonReconstructor.groovy is 100% commented out — the schema-less reconstruction path does not exist

- **Location:** `src/main/groovy/io/github/pierce/JsonReconstructor.groovy:1`
- **Cluster:** perf-avro | **Category:** dead-code | **Effort:** trivial
- **Complexity:** N/A — no bytecode generated
  - → N/A

**Evidence.** `grep -c -v -E '^\s*//' src/main/groovy/io/github/pierce/JsonReconstructor.groovy` returns 0 — every one of the 1,294 lines is prefixed with `//`, starting with line 1 `//package io.github.pierce;`. Confirmed at the artifact level: `ls target/classes/io/github/pierce/ | grep -i JsonReconstructor` returns nothing, while JsonFlattener produces 40+ class files. The file emits no bytecode.

**Impact.** No runtime cost — but the second file in this cluster contributes nothing, so any JSON reconstruction the pipeline claims to do must be going through AvroReconstructor (which requires a schema) or is simply unavailable. Also relevant to the audit: the commented-out code contains the genuinely O(n·d) prefix-grouping loop at lines 452-458 that AvroReconstructor lacks, and a static LIST_TYPE_REF (line 133) doing correctly what RECON-15 flags as wrong in the live file.

**Fix.** Delete the file, or restore it and put it under test. If restored, note that analyzeStructure (line 425) as written builds `prefixToSuffixes` AND `prefixToValues` maps holding every value once per prefix level — O(n·d) memory per record — before it decides anything; that would need reworking before it is production-viable.

### `RECON-22` — Dead private methods totalling ~1.5KB of bytecode, including a full duplicate of the array-sizing logic

- **Location:** `src/main/groovy/io/github/pierce/AvroReconstructor.groovy:2737`
- **Cluster:** perf-avro | **Category:** dead-code | **Effort:** small
- **Complexity:** ~1.5KB dead bytecode; per-schema construction of an unread trie and an unread pathSchemas map
  - → Removed; or the trie becomes load-bearing and eliminates RECON-04's quadratic prefix building

**Evidence.** Verified by grep for each name across the file — these have exactly one occurrence, their own declaration:
- `calculateArraySize` (line 2737), 837 bytecode bytes — a near-duplicate of the live `determineArraySize` (line 1335, 422 bytes), with extra format-specific parsing that determineArraySize lacks
- `reconstructNestedArrayOfRecords` (line 1708), 653 bytecode bytes — dead; the live variant is reconstructNestedArrayOfRecordsAtIndex (line 1469)
- `unwrapUnion` (line 1779) — dead; unwrapNullable (line 2634) is the live one
- `ARRAY_INDEX_PATTERN` / `JSON_ARRAY_PATTERN` (lines 95-96) — see RECON-14
Also the entire SchemaPathTrie Node tree: `root` (line 595), `Node.isValidEndpoint`, `Node.isArrayPath` (line 591), `contains()` (line 627) and `getSchema()` (line 631) have no readers — the trie's only live use is the flat HashSet in containsArrayPath (line 623).

**Impact.** Not a throughput cost directly, but it inflates the class past 2,979 lines, hides which sizing logic is authoritative (calculateArraySize handles BRACKET_LIST/COMMA_SEPARATED formats that the live determineArraySize silently ignores — a latent behavioral gap), and the dead pathSchemas map (line 598) means the one data structure that could give O(1) path→Schema lookups is populated per schema and never read.

**Fix.** Delete calculateArraySize, reconstructNestedArrayOfRecords, unwrapUnion, and the two unused Patterns — but first diff calculateArraySize against determineArraySize and port over the format-specific branches if BRACKET_LIST/COMMA_SEPARATED are supported configurations. Then either wire the trie Node structure into isArrayFieldPattern (RECON-04) and pathSchemas into per-path schema lookup, or reduce SchemaPathTrie to the two HashSet/HashMap fields it actually uses.

### `JFLAT-08` — shouldKeepAsArrayElements recompiles a Pattern and re-splits the constant explosion path on every array node

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:477`
- **Cluster:** perf-flatten | **Category:** redundant-recomputation | **Effort:** trivial
- **Complexity:** All cited code verified, but the magnitude is trivial and the arithmetic is garbled. Per call: exactly ONE Pattern.compile (line 477's `replaceAll`, ~170 ns measured), plus 2 splits per explosion-path iteration — so with E=2 that is 4 splits per call and ~80 per record at 20 array nodes, not the claimed 40 (and fewer in practice, since lines 480 and 496 return early on a match). Importantly the splits do NOT each compile a Pattern: `"\\."` hits String.split's two-char fastpath (backslash followed by a non-alphanumeric), so the cost there is purely the String[] plus per-segment String allocations. Total realistic cost: roughly 20 x (170 ns + a few hundred ns of split allocation) = ~10 us/record — two orders of magnitude below the System.err.println traffic in the very same method (JFLAT-06, ~40-80 us/record). The '~1,200 wasted objects per 1,000 records per array node' figure has incoherent units.
  - → 0 Pattern.compile, 1 split per array node; explosion path segments resolved once per instance

**Evidence.** Line 477: `String normalizedPath = currentPath.replaceAll("\\[\\d+\\]", "");` — again String.replaceAll (a Pattern.compile per call) when `ARRAY_INDEX_STRIP_PATTERN` exists at line 39. Then inside the loop over explosionPaths, lines 485-486: `String[] explosionParts = explosionPath.split("\\."); String[] currentParts = normalizedPath.split("\\.");`. `explosionPath` is a constructor-time constant (line 55) yet is re-split on every iteration of every call, allocating a String[] plus one String per segment each time. The same `explosionPath.split("\\.")` appears again at line 244 in `explodeFlattened`.

**Impact.** Called from flattenJsonForExplosion line 430 for every non-empty array node in every record. With 20 array nodes/record and 2 explosion paths that is 20 Pattern.compile + 40 redundant splits per record, each split allocating an array plus P Strings — ~1,200 wasted objects per 1,000 records per array node. Together with JFLAT-06 (which lives in the same method) this makes shouldKeepAsArrayElements one of the most expensive predicates in the codebase.

**Fix.** Use ARRAY_INDEX_STRIP_PATTERN for the normalization, and precompute `String[][] explosionPartsByPath` in the constructor alongside the parent patterns from JFLAT-02. Split `normalizedPath` at most once per call rather than once per explosion path.

### `JFLAT-20` — Pattern.quote + String.split recompiles a literal-delimiter Pattern on the single-value consolidation path

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:671`
- **Cluster:** perf-flatten | **Category:** allocation-pressure | **Effort:** trivial
- **Complexity:** 1 Pattern.quote String + 1 Pattern.compile + 1 Matcher per delimiter-containing single-value group
  - → 0 allocations, 0 compiles; O(L) indexOf scan reusing the line 670 result

**Evidence.** Line 671: `processArrayValues(consolidatedKey, value.split(Pattern.quote(arrayDelimiter), -1), consolidatedOutput);`. `Pattern.quote` allocates a `\Q,\E` String on every call, and because that regex is 5 characters it does NOT hit `String.split`'s single-char fast path — it falls through to `Pattern.compile("\\Q,\\E")` plus a full Matcher run, per call. `arrayDelimiter` is a constructor-time constant (line 49). Line 670 also does `value.contains(arrayDelimiter)` immediately before, so the string is scanned twice.

**Impact.** One String allocation plus one Pattern.compile plus a Matcher run per single-value array group that contains the delimiter. Bounded by the number of such groups per record, so materially smaller than JFLAT-02/07, but it is the same avoidable-recompilation bug in a third location and the fix is one line.

**Fix.** Add `private final Pattern arrayDelimiterPattern = Pattern.compile(Pattern.quote(this.arrayDelimiter));` to the constructor and use `arrayDelimiterPattern.split(value, -1)`. Since the delimiter is always a literal, a hand-rolled indexOf split avoids the regex engine entirely and can reuse the scan already performed by line 670.

### `JFLAT-21` — NDJSON writers build a full intermediate String per record instead of serializing into the writer

- **Location:** `src/main/groovy/io/github/pierce/JsonFlattener.groovy:1727`
- **Cluster:** perf-flatten | **Category:** allocation-pressure | **Effort:** trivial
- **Complexity:** 2 full copies + 1 SegmentedStringWriter per record written
  - → 1 copy into the existing writer buffer, 0 extra allocations

**Evidence.** Three sites use the same anti-pattern. Line 1727: `writer.write(STANDARD_MAPPER.writeValueAsString(map)); writer.write(options.getLineSeparator());` inside the per-record `toMapStream().forEach(...)`. Line 1427 in BatchResult.toNdjsonFile: `writer.write(STANDARD_MAPPER.writeValueAsString(result));`. Line 1680 in toJsonStream. `writeValueAsString` allocates a SegmentedStringWriter, fills it, and copies out a complete String, which is then copied again into the BufferedWriter's char buffer.

**Impact.** Two full copies of every serialized record plus a per-record SegmentedStringWriter. At 1 KB/record x 1,000,000 records that is ~2 GB of transient char data purely to move bytes from the serializer into an already-buffered writer. Also worth noting: line 1670 `lineCounter.incrementAndGet()` performs an AtomicLong CAS per line on a sequential stream (~20 ns each, ~20 ms per 1M lines) where a plain long field would do.

**Fix.** Use `STANDARD_MAPPER.writeValue(writer, map)` at lines 1727 and 1427 — Jackson writes directly into the BufferedWriter, skipping both copies. Keep the explicit line-separator write (Jackson does not emit one). For line 1680 the String is the stream's actual product, so it must stay.

### `JFLAT-22` — Pervasive containsKey-then-get double lookups and a Stream pipeline to take the max of a handful of ints

- **Location:** `src/main/groovy/io/github/pierce/MapFlattener.groovy:542`
- **Cluster:** perf-flatten | **Category:** redundant-recomputation | **Effort:** small
- **Complexity:** 2·m·f hash lookups per array-of-maps node; 1 Stream pipeline + ~4 allocations per padding pass
  - → m·f hash lookups; O(f) for-loop with 0 allocations

**Evidence.** The containsKey/get/add idiom appears at seven sites on the array-of-maps path: lines 524-528 (`if (!fieldValues.containsKey(fieldKey)) { fieldValues.put(fieldKey, new ArrayList<>()); } fieldValues.get(fieldKey).add(...)`), 532-537, 585-588, 592-597, 611-615, 620-625, 702-705, 710-715 — each hashing the key twice or three times where `computeIfAbsent(k, x -> new ArrayList<>()).add(v)` hashes once. Separately, lines 542-545 and 720-723: `int maxSize = fieldValues.values().stream().mapToInt(List::size).max().orElse(0);` builds a ReferencePipeline, a spliterator, an IntPipeline and an OptionalInt to find the max of typically 3-10 ints.

**Impact.** For an array of m maps with f fields, 2·m·f hash computations instead of m·f — and under dynamic Groovy each is an invokedynamic callsite (flattenList carries 93 per javap), so the doubling is on the expensive kind of call. The stream pipeline costs ~150-200 ns and ~4 allocations per array-of-maps node versus ~5 ns for a for-loop; it is also unnecessary, since the maximum is `limit` whenever any field appears in every element.

**Fix.** Replace all seven sites with `map.computeIfAbsent(k, x -> new ArrayList<>()).add(v)`. Replace both stream max computations with a plain for-loop over `fieldValues.values()`, or track the running maximum while populating.

### `JFLAT-23` — MapFlattener constructs its own ObjectMapper per instance instead of sharing a static one

- **Location:** `src/main/groovy/io/github/pierce/MapFlattener.groovy:126`
- **Cluster:** perf-flatten | **Category:** redundant-recomputation | **Effort:** trivial
- **Complexity:** 1 ObjectMapper + cold serializer caches per MapFlattener instance (per Spark task)
  - → 1 shared ObjectMapper with warm caches per JVM

**Evidence.** Line 126: `private final ObjectMapper objectMapper = new ObjectMapper();` — an instance field. JsonFlattener.groovy gets this right at lines 155 and 158 (`private static final ObjectMapper STANDARD_MAPPER = createStandardMapper();`), so the two classes disagree. The mapper is used on hot paths: `serializeArray` line 760, `stringifyObject` line 947, `tryParseJson` lines 987 and 992.

**Impact.** ObjectMapper construction costs on the order of a millisecond and, more importantly, each instance carries its own SerializerProvider and DeserializerCache. In Spark, where a flattener is typically built per partition or per task, every instance starts with a cold class-to-serializer cache and re-resolves serializers for the same types instead of sharing warm lookups across the executor. With thousands of tasks this is thousands of redundant cache warm-ups.

**Fix.** Change to `private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();`. ObjectMapper is thread-safe once configured, and this class never reconfigures it. This also removes a non-trivial field from a Serializable class.

### `JFLAT-24` — Two full scans of the raw JSON per record for a validation check, plus three dead members left on the class

- **Location:** `src/main/java/io/github/pierce/JsonFlattenerConsolidator.java:99`
- **Cluster:** perf-flatten | **Category:** redundant-traversal | **Effort:** trivial
- **Complexity:** 2 full O(n) scans + 1 O(n) String copy per record before parsing; 3 dead members including one class-init Pattern.compile
  - → 0 pre-scans, 0 copies; validation folded into the single parse pass

**Evidence.** Line 99: `if (trimmed.contains(": undefined") || trimmed.contains(": NaN"))` — two full linear scans of the entire raw JSON document before Jackson ever sees it, duplicated in `preprocessJson` at line 518. Line 84 `String trimmed = jsonString.trim();` allocates a copy of the whole document (and `preprocessJson` line 508 does it again). Meanwhile line 41 declares `private static final Pattern MALFORMED_JSON_PATTERN = Pattern.compile("[:,\\[]\\s*(undefined|NaN)\\s*[,\\}\\]]");` — grep confirms it is never referenced anywhere; the substring checks replaced it. Two more dead members: `MapFlattener.flattenSingleValue` (line 816, no callers) and `JsonFlattener.BatchOperation.batchSize` (written at line 1151, never read — see JFLAT-09).

**Impact.** For a 10 KB record, ~20 KB of redundant scanning plus a 10 KB String copy per record — ~30 MB per 1,000 records — for a check Jackson performs correctly during parsing anyway (`undefined` and `NaN` are not valid JSON literals under the default configuration). Low severity because indexOf is intrinsified, but it is pure waste on every record and the check is also unsound: it misses `:undefined` with no space and false-positives on the literal text inside a string value.

**Fix.** Delete the substring checks and let `OBJECT_MAPPER.readTree` reject invalid input, catching JsonProcessingException where the current code returns "{}". Pass the untrimmed string to readTree (Jackson skips leading whitespace itself), keeping only the BOM strip. Remove MALFORMED_JSON_PATTERN, flattenSingleValue, and the batchSize field.

### `SCHEMA-25` — AbstractTypeConverter.parseNumber allocates a lowercased copy of every string it inspects, then boxes the result

- **Location:** `src/main/java/io/github/pierce/converter/AbstractTypeConverter.java:105`
- **Cluster:** perf-schema | **Category:** allocation-pressure | **Effort:** trivial
- **Complexity:** per string value: 1 String allocation + 1 full copy + 1 boxing
  - → per string value: 0 allocations for the scan; boxing removable with primitive helpers

**Evidence.** ```java
if (str.contains(".") || str.toLowerCase().contains("e")) {
    return Double.parseDouble(str);
}
long l = Long.parseLong(str);
if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
    return (int) l;
}
return l;
```
The same `str.toLowerCase().contains("e")` idiom is duplicated in LongConverter.java:52.

**Impact.** toLowerCase() allocates a new String for every value that lacks a '.', purely to look for a single character. The method's Number return type then forces autoboxing of the int/long/double on every return. Neither is catastrophic in isolation, but parseNumber sits behind the numeric converters on a per-value path, so at tens of millions of values it is a measurable contribution to young-gen pressure.

**Fix.** Replace `str.toLowerCase().contains("e")` with `str.indexOf('e') >= 0 || str.indexOf('E') >= 0` — no allocation, one pass. Since the callers immediately narrow the Number back to a primitive, consider replacing parseNumber with type-specific primitive helpers so the boxing disappears.

### `SCHEMA-26` — The reported "unchecked or unsafe operations" warning is the unsuppressed cast at line 194 — benign for correctness, but it marks a fourth redundant copy of the schema map

- **Location:** `src/main/java/io/github/pierce/converter/SchemaBasedMapConverter.java:194`
- **Cluster:** perf-schema | **Category:** redundant-copy | **Effort:** medium
- **Complexity:** 4 LinkedHashMaps x F entries per converter, 4 hash lookups per field during init, 1 full defensive copy
  - → 1 LinkedHashMap x F entries + 1 lowercase index, 2 hash lookups per field, 0 copies

**Evidence.** Every other unchecked cast in the file carries @SuppressWarnings (lines 406, 422, 675, 741). The constructor does not:
```java
this.fieldConverters = new LinkedHashMap<>();
this.fieldNameLookup = new LinkedHashMap<>();
this.fieldNullability = new LinkedHashMap<>();
this.flattenedSchemaFields = schemaType == SchemaType.FLATTENED
        ? new LinkedHashMap<>((Map<String, FlattenedFieldType>) schema)     // line 194: unchecked
        : Collections.emptyMap();
```

**Impact.** The cast itself hides no real problem — `schema` is only ever a Map<String,FlattenedFieldType> when schemaType is FLATTENED, guaranteed by the private constructor's only caller path (forFlattened, line 282). What it does mark is a structural inefficiency: the constructor builds four parallel LinkedHashMaps over the identical key set (fieldConverters, fieldNameLookup, fieldNullability, flattenedSchemaFields), and flattenedSchemaFields is a full defensive copy of a map the caller just built and does not retain. That is 4x the entry nodes and 4x the hash lookups per field for what is one logical per-field record, and it is exactly the data that SCHEMA-13's compiled plan would collapse.

**Fix.** Introduce a single `record FieldPlan(String name, TypeConverter<Object,Object> converter, boolean nullable, FlattenedFieldType type)` and store one `Map<String,FieldPlan>` plus one lowercase-name index. Drop the defensive copy (the input map is not retained by the caller) or wrap it with Collections.unmodifiableMap instead of copying. Then annotate the constructor @SuppressWarnings("unchecked") to silence the build warning.

### `NP-037` — Enum converter discards the caller's ConversionConfig

- **Location:** `src/main/java/io/github/pierce/converter/SchemaBasedMapConverter.java:925`
- **Cluster:** quality | **Category:** correctness | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
private static class AvroEnumConverter extends AbstractTypeConverter<String> {
    public AvroEnumConverter(org.apache.avro.Schema enumSchema) {
        super(ConversionConfig.defaults(), "enum<" + enumSchema.getName() + ">");
```
It is constructed at line 772 as `case ENUM -> new AvroEnumConverter(avroSchema)` — the enclosing instance's `config` is in scope and simply not passed.

**Impact.** Enum-typed fields silently ignore every configured option. With `trimStrings(true)`, the input `" ACTIVE "` is not trimmed and fails validation with "Invalid enum symbol"; with `coerceEmptyStringsToNull(true)`, an empty string throws instead of becoming null. The failure is confusing because the same settings demonstrably work on every other field in the record.

**Fix.** Add a ConversionConfig parameter and pass `config` at the construction site.

### `NP-038` — GenericRecord is mutable but defines equals/hashCode over its mutable value map

- **Location:** `src/main/java/io/github/pierce/converter/GenericRecord.java:126`
- **Cluster:** quality | **Category:** correctness | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
public GenericRecord setField(String name, Object value) { ...; values.put(name, value); return this; }
public void set(int pos, Object value) { ...; values.put(field.name(), value); }
@Override public int hashCode() { return Objects.hash(schema, values); }
@Override public boolean equals(Object o) { ... Objects.equals(values, that.values); }
```

**Impact.** Standard mutable-key hazard: `set.add(record); record.setField("status","SHIPPED"); set.contains(record)` returns false, and the entry is unreachable and unremovable — it leaks for the lifetime of the collection. Dedup logic (`new HashSet<>(records)`) and `Map<GenericRecord, ...>` grouping produce wrong results whenever a record is touched after insertion, which is easy given the fluent `setField` returns `this` and invites post-construction mutation.

**Fix.** Either drop equals/hashCode (identity semantics, matching the mutable design) or make the class immutable with a builder and keep value semantics. Document explicitly that instances must not be mutated after being used as a key.

### `NP-039` — Cached DataFrame is unpersisted before the returned lazy Datasets are ever consumed

- **Location:** `src/main/java/io/github/pierce/spark/NexusPiercerSparkPipeline.java:468`
- **Cluster:** quality | **Category:** efficiency | **Effort:** small
- **Complexity:** 3 full passes over the input (metrics + success + error)
  - → 1 pass materialized and reused

**Evidence.** ```java
allProcessedRecords.cache();          // line 420
...
successDataset = allProcessedRecords.filter(...);   // lazy
...
metrics.processingTimeMs = System.currentTimeMillis() - startTime;
allProcessedRecords.unpersist();      // line 468
return new ProcessingResult(successDataset, errorDataset, metrics);
```
Both returned Datasets are lazy derivations of `allProcessedRecords`, and the cache is dropped before the caller performs any action on them.

**Impact.** The `cache()` never pays off: by the time the caller writes `successDataset`, the cached blocks are gone and the entire lineage — including the expensive flattening UDF and both `from_json` calls — is recomputed from source. If the caller consumes both successDataset and errorDataset, the UDF runs the full input twice more. The `cache()`/`unpersist()` pair costs a full materialization for collectMetrics and delivers no reuse.

**Fix.** Return the cached Dataset (or a cached derivation) and expose an `unpersist()`/close on ProcessingResult for the caller to invoke, so the cache spans the actions that actually consume it.

### `NP-040` — isTypeCompatible accepts fractional strings for integral columns, so bad rows are silently nulled instead of quarantined

- **Location:** `src/main/java/io/github/pierce/spark/NexusPiercerSparkPipeline.java:703`
- **Cluster:** quality | **Category:** correctness | **Effort:** small
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
if (dataType instanceof org.apache.spark.sql.types.NumericType) {
    if (value instanceof Number) return true;
    try { Double.parseDouble(value.toString()); return true; }
    catch (NumberFormatException e) { return false; }
}
```
The check is the same for LongType, IntegerType, and DoubleType, and `Double.parseDouble` also accepts "Infinity", "NaN", "1e400", and hex-float forms.

**Impact.** A record whose `user_id` arrives as `"3.7"` against a LongType column passes the compatibility check, so no `Schema validation failed` error is raised; `from_json` then cannot coerce it and emits null for that column. The row lands in the success dataset with a null primary key instead of being quarantined — precisely the outcome the quarantine feature exists to prevent, and it inflates `metrics.successfulRecords`.

**Fix.** Branch on the concrete numeric type: for IntegerType/LongType/ShortType/ByteType require `Long.parseLong` (or a BigDecimal with scale 0 in range); reserve `Double.parseDouble` for Float/Double, and reject non-finite literals.

### `NP-041` — YAML is loaded with the default Yaml constructor rather than an explicit SafeConstructor

- **Location:** `src/main/java/io/github/pierce/files/FileFinder.java:1134`
- **Cluster:** quality | **Category:** security | **Effort:** trivial
- **Complexity:** N/A
  - → N/A

**Evidence.** ```java
public static Object readYaml(String fileName) throws IOException {
    try (InputStream is = findFile(fileName)) {
        Yaml yaml = new Yaml();
        return yaml.load(is);
    }
}
```
No LoaderOptions, no SafeConstructor. The POM pins snakeyaml 2.2, whose default LoaderOptions rejects global tags, so this is not currently exploitable — but the safety is entirely implicit in the dependency version.

**Impact.** The code carries no local evidence that it is safe: a downgrade to snakeyaml 1.x (via a transitive dependency-management change or a shaded uber-jar picking a different version) silently restores CVE-2022-1471 arbitrary object instantiation, turning any attacker-supplied `.yaml` reachable through readYaml into remote code execution. There are also no explicit `codePointLimit`/`maxAliasesForCollections` bounds, so resource limits likewise depend on library defaults.

**Fix.** Be explicit and version-independent: `LoaderOptions opts = new LoaderOptions(); opts.setCodePointLimit(...); opts.setMaxAliasesForCollections(...); Yaml yaml = new Yaml(new SafeConstructor(opts));` Add an enforcer/dependency-convergence rule pinning snakeyaml ≥ 2.x.

### `NP-042` — Static StructType and CachedSchema caches grow without bound

- **Location:** `src/main/java/io/github/pierce/CreateSparkStructFromAvroSchema.java:24`
- **Cluster:** quality | **Category:** efficiency | **Effort:** small
- **Complexity:** O(distinct schemas) retained forever
  - → O(maximumSize)

**Evidence.** `private static final Map<String, StructType> structTypeCache = new ConcurrentHashMap<>();` keyed by `avroSchema.getFullName() + ":" + avroSchema.hashCode()` (line 34) with eviction only via the manual `clearCache()`. Same shape in NexusPiercerSparkPipeline (`SCHEMA_CACHE`, line 40), AvroSchemaLoader (`schemaCache`/`structTypeCache`, lines 41-42), and AvroReconstructor.groovy:213, where the constant named `DEFAULT_MAX_CACHE_SIZE` is passed as ConcurrentHashMap's *initial capacity* and enforces no maximum at all.

**Impact.** A long-lived driver that handles many distinct or per-tenant schemas accumulates one StructType (plus, in the pipeline cache, the original Schema, flattened Schema, and four field-name Sets) per schema forever — a steady driver heap leak that ends in OOM after enough distinct schemas. The `fullName + hashCode` key is also collision-prone: two schemas sharing a name and hash return the wrong StructType.

**Fix.** Replace the raw ConcurrentHashMaps with bounded caches (Guava `CacheBuilder.maximumSize(n).expireAfterAccess(...)`, already a dependency and already used inside FileFinder) and key on `SchemaNormalization.parsingFingerprint64` rather than `hashCode`.

### `TA-013` — Count correction: 636 active @Test (not 524) and 115 commented-out @Test (not 117) — the published figures conflate substring matches on @TestInstance/@TestMethodOrder

- **Location:** `pom.xml:1092`
- **Cluster:** tests | **Category:** fact-correction | **Effort:** trivial
- **Complexity:** reported 524 active / 117 commented
  - → verified 636 active / 115 commented / 737 executed invocations

**Evidence.** Exact per-file parse of all 46 test-source files (17 .groovy + 29 .java), classifying each `@Test` line as active vs comment-prefixed. ACTIVE: 449 in Java + 187 in Groovy = **636**, plus 17 @ParameterizedTest = 653 test methods. COMMENTED: 70 in Java + 45 in Groovy = **115**. Reconciling the stated numbers: a naive `grep -o '@Test' *.java` returns 524 because it also matches the 5 substring occurrences of @TestInstance/@TestMethodOrder (NexusPiercerExamplesTest.java:38, NexusPiercerPatternsTest.java:30, FileFinderComprehensiveTest.java:33, JsonFlattenerConsolidatorPerformanceTest.java:19, JsonFlattenerConsolidatorComprehensiveTest.java:22); 449 active + 70 commented = 519 true @Test, + 5 = 524. Likewise the '117 commented' figure includes two commented `//@TestInstance(TestInstance.Lifecycle.PER_CLASS)` lines (NexusPiercerExamplesTest.java:38, NexusPiercerPatternsTest.java:30), so NexusPiercerExamplesTest has 8 commented @Test (not 9) and NexusPiercerPatternsTest has 2 (not 3). Corrected per-file commented tally: JsonReconstructorTest.groovy 45, SchemaCompatibilityIntegrationTest.java 35, NexusPiercerFunctionsTest.java 12, NexusPiercerSparkPipelineTest.java 11, NexusPiercerExamplesTest.java 8, NexusPiercerPatternsTest.java 2, JsonFlattenerConsolidatorPerformanceTest.java 2 = 115. Ground truth from the live run (target/surefire-reports, 2026-08-09 15:11-15:14): 126 report XMLs, **737 test invocations, 0 failures, 0 errors, 3 skipped** — 737 > 653 because the 17 @ParameterizedTest methods expand to multiple invocations and 109 @Nested containers report separately.

**Impact.** Materially changes the remediation arithmetic: 115 dead tests against 636 live ones is 15.3% of the suite dormant (not 117/524 = 22%). More importantly the per-file split determines who owns what — the two files whose counts were inflated (Examples, Patterns) are the two smallest revival jobs, and JsonFlattenerConsolidatorPerformanceTest is not a dead file at all (TA-007).

**Fix.** Use 636 active / 115 commented / 653 methods / 737 invocations as the baseline. Reviving all 115 (TA-001, TA-002, TA-004, TA-007) plus enabling the 26 jqwik properties (TA-003) and the 3 @Disabled (TA-006) would take the suite to ~779 methods and lift the spark package from 0% and JsonReconstructor from 0%.

### `TA-014` — Groovy test files declare packages that do not match their directory, and one test sits in the default package

- **Location:** `src/test/groovy/AvroReconstructorTest.groovy:1`
- **Cluster:** tests | **Category:** layout | **Effort:** trivial
- **Complexity:** 15/17 groovy files in wrong directory, 2 in default package
  - → directory matches package throughout

**Evidence.** All 17 files live flat in src/test/groovy/ with no package subdirectories, yet 15 declare a package on line 1: 11 declare `package io.github.pierce`, 2 declare `package io.github.pierce.avro` (AvroSchemaFlattenerAlignmentTest, AvroSchemaFlattenerTest), 2 declare `package io.github.pierce.examples` (MapFlattenerExamples, StreamingProcessorExample). Two declare no package at all and land in the DEFAULT package: AvroReconstructorTest.groovy (9 @Test) and JsonReconstructorTest.groovy — confirmed by target/test-classes/AvroReconstructorTest.class and JsonReconstructorTest.class sitting at the root of test-classes, and by the surefire report literally named TEST-AvroReconstructorTest.xml (9 tests, 2.00s). Groovy tolerates the dir/package mismatch; javac would not. Note also src/test/groovy contains BOTH an `AvroSchemaFlattenerTest.groovy` (package io.github.pierce.avro) and src/test/java has `AvroSchemaFlattenerTest.java` (package io.github.pierce.avroTesting) — same simple name, different packages.

**Impact.** Low functional risk today (everything runs), but it blocks any future move to joint javac/groovyc compilation, makes IDE navigation unreliable, and the default-package classes cannot be imported by any packaged test. The duplicated `AvroSchemaFlattenerTest` simple name across two languages and two packages is a live source of confusion when reading surefire output — reports show `io.github.pierce.avro.AvroSchemaFlattenerTest` (9 tests, Groovy) and `io.github.pierce.avroTesting.AvroSchemaFlattenerTest` (10 tests, Java) side by side.

**Fix.** Move each .groovy file into src/test/groovy/<package-path>/ matching its declaration, and give AvroReconstructorTest.groovy a package (io.github.pierce). Rename one of the two AvroSchemaFlattenerTest classes. Do this in the same change as TA-001, which already needs JsonReconstructorTest.groovy relocated to src/test/groovy/io/github/pierce/.

### `TA-015` — Only one duplicated test helper repo-wide (getUsedMemory, copy-pasted into 4 files) — helper duplication is otherwise not a problem

- **Location:** `src/test/java/io/github/pierce/FlattenConsolidatorTests/JsonFlattenerConsolidatorPerformanceTest.java:315`
- **Cluster:** tests | **Category:** duplication | **Effort:** trivial
- **Complexity:** 4 copies of getUsedMemory, 3 copies of an identical setUp
  - → one shared support class

**Evidence.** Signature-level scan of every method defined in the 46 test files: exactly ONE non-lifecycle helper name is defined in more than one file — `getUsedMemory()` (`return runtime.totalMemory() - runtime.freeMemory()`), duplicated verbatim across JsonFlattenerConsolidatorComprehensiveTest.java, JsonFlattenerConsolidatorEdgeCaseTest.java, JsonFlattenerConsolidatorPerformanceTest.java:315 and JsonFlattenerExplosionTest.java. Body-level MD5 comparison over all methods >4 lines found exactly ONE cross-file identical block: the 5-line `setUp()` shared by AvroSchemaFlattenerCompatibilityTest.java, AvroSchemaFlattenerOrderingTest.java and AvroSchemaFlattenerTest.java (`AvroSchemaFlattener.clearCache(); flattener = new AvroSchemaFlattener(false); flattenerWithStats = new AvroSchemaFlattener(true);`).

**Impact.** Minimal. Reported for completeness against the brief: helper duplication is NOT a significant problem in this suite — the real fixture problem is data duplication (91 inline schemas, TA-010), not code duplication. The four getUsedMemory copies all belong to the flaky memory assertions covered by TA-007.

**Fix.** Fold `getUsedMemory()` and the shared AvroSchemaFlattener `setUp()` into a single `src/test/java/io/github/pierce/TestSupport.java` (or the revived SchemaTestUtils). Lower priority than TA-010.

