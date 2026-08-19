# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| 2.1.0 (unreleased, on `main`) | Yes — 26 behaviour changes since 2.0.0; see `CHANGELOG.md` |
| 2.0.x | Yes |
| 1.0.8 | Security fixes only |
| < 1.0.8 | No |

## Reporting a vulnerability

**Do not open a public issue for a security problem.**

Report privately via
[GitHub Security Advisories](https://github.com/pierce-lonergan/NexusPiercer/security/advisories/new),
or by email to **lonerganpierce@gmail.com** with `SECURITY` in the subject.

Please include the affected version, a minimal reproducing input, and the impact you believe it
has. You will get an acknowledgement within 3 business days and an assessment within 10.

Coordinated disclosure is preferred: we will agree a publication date with you and credit you in
the advisory unless you ask otherwise.

## Known issues in current releases

These are documented rather than hidden. The `NP-0xx` numbering comes from `docs/audit/FINDINGS.md`, but that register is FROZEN at 2026-08-09 and has no status column, so it carries no row for `recon/NP-022` through `NP-026`, `files/NP-027` or `perf/NP-028`, all of which postdate it. **This table plus `CHANGELOG.md` and `docs/BACKLOG.md` are the current state**; `docs/audit/ROADMAP.md` schedules the original audit's phases and is likewise a 2026-08-09 document.

| ID | Issue | Impact |
|---|---|---|
| ~~`quality/NP-001`~~ | ~~`FileFinder` declares `validatePaths`, `allowedExtensions`, and `maxFileSize` but never reads them.~~ **FIXED and RELEASED in 2.0.0.** Re-verified 2026-08-17 against `FileFinder.java`: `validatePaths` (default `true`) is read and rejects a caller-supplied name containing `../` or starting `..`; `allowedExtensions` is read and enforced; `maxFileSize` is read and throws on breach. This row contradicted `README.md`, which was the correct document. **RESIDUAL CORRECTED AGAIN, 2026-08-19 — the sentence that stood here was FALSE.** It said "`..`, `../..` and `../../..` remain in the DEFAULT search paths". They do not, and have not since 2.1.0: `FileFinder.Config.searchPaths` contains no parent directory and carries an in-code comment recording the removal and the measurement that forced it (a `.json` miss walked 67 files across sibling checkouts and 24 in the user home directory, and embedded the listing in an exception message Spark logs). What survives is narrower still: `validatePaths(false)` disables the `..`-prefix rejection for a caller-supplied NAME, so a caller who both writes that name and disables the check can still escape the working tree. | Was: arbitrary local file read if a schema name is caller-influenced. Now: only if the caller disables `validatePaths`. |
| `quality/NP-002` | `AvroSchemaFlattener.collectRecordDefinitions` recurses with no depth or cycle guard. | A self-referential `.avsc` causes `StackOverflowError`. |
| `quality/NP-003` | `SchemaBasedMapConverter.flattenAvroSchema` tracks a depth counter it never checks. | `record Node { Node next }` loops until heap exhaustion — an unkillable hang rather than a fast failure. |
| `quality/NP-013` | JSON explosion produces an unbounded cross-product. | Three array paths of 1,000 elements yields 10⁹ records — OOM on adversarial input. |
| `arch/NP-002` | ~~Flattened key encoding is not injective~~ | **Fixed and RELEASED in 2.0.0** — see below. |
| `perf/NP-021` | ~~Reconstruction cost superlinear in separator count~~ | **Fixed and RELEASED in 2.0.0** — see below. |
| `recon/NP-022` | A field literally named `___` collides with the reconstructor's `__*__` sentinel namespace and is silently dropped. | Silent field loss for that specific name shape. |
| `recon/NP-023` | ~~A defaulted field absent from the input arrives with the wrong Java type~~ | **Fixed in 2.1.0.** Defaults now go through `GenericData.getDefaultValue`, deep-copied, so ENUM yields `EnumSymbol`, FIXED yields `GenericData.Fixed`, BYTES yields `ByteBuffer`, STRING yields `Utf8` and `"default": null` yields a **real Java null** rather than the `JsonProperties.NULL_VALUE` singleton — a case this row did not list and which a consumer testing `value == null` saw as a non-null object of an Avro-internal type. The root record now validates and binary-encodes at the shipped default. TWO LIMITS REMAIN AND ARE NOT THIS ROW: a RECORD-typed default is correct in `reconstructToMap` but still a `LinkedHashMap` in `reconstruct()` (see the nested-record row below), and a defaulted LOGICAL-TYPE field arrives as its raw underlying type — new row `recon/NP-026`. |
| `recon/NP-024` | ~~`allowMissingFields` allows no missing fields at either value~~ | **Fixed in 2.1.0.** The default flipped to `false`, which fails with a `ReconstructionException` naming every missing field by its FLATTENED PATH, aggregated and thrown before `build()`; `true` substitutes the Avro type default with an aggregated WARN, and refuses for ENUM, FIXED, RECORD and a null-free UNION where no type default exists. NARROWER THAN THIS ROW SAID: the lost-path half applied only to ROOT-level fields — a nested `reconstructRecord` runs inside the parent's per-field try and was already wrapped with a path. A FOURTH DEFECT IN THE SAME LADDER, named nowhere and masked only because the old default was `true`: `useSchemaDefaults` was tested before `hasDefaultValue`, so `.useSchemaDefaults(false).allowMissingFields(false)` reported "Required field missing and no default" about a field that HAS one. |
| `recon/NP-025` | ~~An empty flattened map bypasses the reconstruction path entirely~~ | **Fixed in 2.1.0.** `createEmptyRecord` and the short-circuit are deleted; an empty or null map now builds an empty root `PathNode` and every field runs the same ladder as the one-unrelated-key case. STATED MORE PRECISELY THAN THIS ROW DID: "returns `{}`" was true only of a schema with no defaulted and no nullable field — `createEmptyRecord` did return those. The defect was the SILENT OMISSION of required no-default fields, not the emptiness of the result. |
| `recon/NP-026` | A defaulted **logical-type** field absent from the input arrives as its raw underlying type — a `ByteBuffer` for a `decimal`, an `int` for a `date` — while the same field supplied in the input arrives converted through the class's own `Conversions`. | Two code paths for one field disagree about its Java type. The datum is valid and writable, so nothing fails; a caller switching between "present" and "absent" sees the type change under them. Created by the `recon/NP-023` fix (`GenericData.getDefaultValue` builds a plain `GenericDatumReader` with no conversions) and disclosed here rather than left to be discovered. |
| `perf/NP-028` | A wide SPARSE array of maps makes `MapFlattener` emit `(union of distinct keys) x (element count)` cells, and until 2.1.0 nothing bounded that product. | **Measured proof of concept:** a 4,057,897-character WELL-FORMED JSON document (1000 array elements, 300 distinct keys each) exhausts a 1 GB heap inside `MapFlattener.columnFor`, at `new ArrayList<>(Collections.nCopies(size, null))`. None of the four existing bounds covers it: `maxArraySize` caps the SLOT axis (and the column axis only incidentally, when each element carries one distinct key), `maxMapSize` caps keys PER ELEMENT rather than their union, `maxDepth` is all-or-nothing at the array boundary with no breadth effect, and `maxJsonStringLength` is unrelated. Per-array ceiling at the shipped defaults was `maxArraySize x maxMapSize` = 1e10 cells, and nothing at all across SIBLING arrays - 50 arrays of 500 sparse elements are 250,000 cells each and 62,778,390 output characters together. **2.1.0 caps the total** at `maxArrayCells`, per invocation, default 1,048,576, refusing rather than truncating. **The default still permits roughly 391x amplification** - 12,787 input characters legitimately producing 5,005,780 output characters is under budget by deliberate choice, because refusing it would invalidate a published release note. Heap exhaustion is closed; amplification is not. **Lower `maxArrayCells` if you accept untrusted documents.** **THE CEILING IS ON TOTAL CELLS REGARDLESS OF SPARSITY**, which this row's framing around a "wide SPARSE array" does not convey: an ordinary DENSE batch document with no amplification at all is refused on the same arithmetic. Measured at the default - `50 arrays x 1000 elements x 20 dense keys` passes at 1,000,000 cells, `60 arrays x 1000 elements x 20 dense keys` is refused at `arr52`, `300 arrays x 200 elements x 20 keys` at `arr262`, `1000 arrays x 100 elements x 12 keys` at `arr873`. The refusal names the knob and says to raise it, so it is loud and actionable, but a batch user should recognise their own shape here rather than in production. **2.1.0 also charges the nested-array site's hole cells**, which the cardinality repair (CHANGELOG item 27) would otherwise have left free: an inner list that flattens to no columns creates no inner column, so nothing billed it - measured, 13,901 input bytes producing 10,012,005 output characters, accepted. Same DoS family as `quality/NP-013`, different code path; fix them together or neither. |
| ~~`files/NP-027`~~ | `FileFinder.maxFileSize` is unenforced for every name that does not resolve as a regular file **relative to the CWD**. `enforceSafetyOptions` calls `Paths.get(fileName)` with no base path, while `findFileHandle` resolves against `config.getAllSearchPaths()` — which by default include `..`, `../..` and `../../..`. Classpath and HDFS names are not size-checked at all. | **FIXED IN 2.1.0, and BOTH halves of the cause as stated above are now false.** (1) The default search paths no longer include any parent directory, so the resolution asymmetry the row describes cannot reach outside the working tree in the first place. (2) The size gate moved to where the size is actually known: `enforceResolvedSize` caps the RESOLVED handle however it was located, and `SizeLimitedInputStream` caps the byte stream during the read, so a compressed file that expands past the bound now throws MID-READ. CHANGELOG behaviour changes 14 and 15 record both. The original finding stands as history: it was found while auditing an empty catch whose comment claimed "size is checked on open instead", and there was no size check on open. |

### `arch/NP-002` / `perf/NP-021` — fixed by an injective key encoding

**Still present in the released 1.0.8. Fixed and released in 2.0.0 — upgrade to 2.0.0.**

The old encoding concatenated path segments without escaping, so `{"user_id": 1}` and
`{"user": {"id": 1}}` both produced the key `user_id`. That was known to be lossy. The JMH harness
then found it was also a denial-of-service vector: because the reconstructor could not tell a
structural separator from a literal one, it grouped by every candidate prefix, and each extra
underscore in a field name multiplied both the groupings and the paths falsely detected as arrays.

Structure held completely fixed at 40 flattened keys (5 record arrays × N records × 8 fields),
varying only underscores per field name — JDK 21, 1 GB heap:

| Field name | 25 recs | 50 recs | 75 recs | 150 recs |
|---|---:|---:|---:|---:|
| `nested_field_{n}` — **before** | 1,198 ms | 3,435 ms | **OOM** | — |
| `nested_field_{n}` — **after** | 3 ms | 4 ms | 4 ms | 7 ms |
| `deep_nested_field_{n}` (3 `_`) — after | 3 ms | 3 ms | 6 ms | 6 ms |

`FlattenedPath` escapes separator characters inside segments, so `record_array_0` is one segment
rather than three. Reconstruction cost is now **independent of how many separator characters a
field name contains** — that independence, rather than the ~860x speedup, is the property that
makes the input space safe.

**If you are on 1.0.8**, configure a separator that cannot occur in your field names and bound
document size before reconstruction.

### `recon/NP-022` — sentinel namespace collision

`JsonReconstructor` reserves the `__*__` shape for internal bookkeeping (`__isArray__`,
`__arrayPath__`) and skips any key matching `startsWith("__") && endsWith("__")`. A field named
`___` satisfies both — characters 0-1 and 1-2 are each `"__"` — so it is dropped.

The encoding round-trips the name correctly; the loss is downstream, so the fix is to replace the
magic-string sentinels with a private holder type. Asserted as present by
`SeparatorInFieldNameRegressionTest` rather than left latent.

**Until these are fixed, treat schema paths and JSON documents passed to this library as trusted
input.** If you must accept untrusted input, validate and bound it before it reaches NexusPiercer.

## Scope

In scope: anything in `src/main` that ships in the published artifact.

Out of scope: issues requiring an attacker to already control the JVM or filesystem; findings in
dependencies without a demonstrated path through this library's API (report those upstream, though
we do want to hear about them); and results from automated scanners without a working
proof-of-concept.

## Our security process

Every pull request runs CodeQL and an OWASP dependency-check CVE scan. A CycloneDX SBOM is
published with each release. Dependabot proposes updates weekly, with security updates ungrouped
so they arrive as individually reviewable PRs.

**GitHub dependency review is configured but has never actually run, and this paragraph used to
claim otherwise.** It previously said every pull request runs "GitHub dependency review (blocking
on high-severity advisories in newly-added dependencies)". Measured on the most recent pull-request
run of `quality.yml`: the `actions/dependency-review-action` step is **skipped**, the fallback
"Report that dependency review is unavailable" step runs in its place, and the job's conclusion is
nonetheless **success** — a green check on an analysis that did not happen. The cause is that this
repository's **Dependency graph** is disabled, which both `dependency-graph/snapshots` and
`dependency-graph/sbom` confirm by returning 404. The workflow already detects this and writes a
`::warning::` plus a step summary rather than pretending, so no code change will fix it: it needs
the repository owner to enable Dependency graph under *Settings → Code security and analysis*.
Until then, treat newly-added dependencies as unscreened by this control and rely on the OWASP CVE
scan, which does run.

**Releases cannot be signed or published from CI.** No Actions secrets are configured, so
`release.yml`'s GPG and Maven Central steps have no `MAVEN_GPG_PRIVATE_KEY`,
`MAVEN_CENTRAL_USERNAME` or `MAVEN_CENTRAL_TOKEN` to use. The workflow guards on their presence
rather than failing obscurely, but the effect is that a release must be cut by hand. This is also
a repository-settings item for the owner.
