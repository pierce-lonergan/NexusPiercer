# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| 1.0.8 | Yes — security fixes only |
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

These are documented rather than hidden. All are tracked in
[docs/audit/FINDINGS.md](docs/audit/FINDINGS.md) and scheduled in
[docs/audit/ROADMAP.md](docs/audit/ROADMAP.md).

| ID | Issue | Impact |
|---|---|---|
| `quality/NP-001` | `FileFinder` declares `validatePaths`, `allowedExtensions`, and `maxFileSize` but never reads them. `..`, `../..`, and `../../..` are in the default search paths. | Arbitrary local file read if a schema name is caller-influenced. |
| `quality/NP-002` | `AvroSchemaFlattener.collectRecordDefinitions` recurses with no depth or cycle guard. | A self-referential `.avsc` causes `StackOverflowError`. |
| `quality/NP-003` | `SchemaBasedMapConverter.flattenAvroSchema` tracks a depth counter it never checks. | `record Node { Node next }` loops until heap exhaustion — an unkillable hang rather than a fast failure. |
| `quality/NP-013` | JSON explosion produces an unbounded cross-product. | Three array paths of 1,000 elements yields 10⁹ records — OOM on adversarial input. |
| `arch/NP-002` | ~~Flattened key encoding is not injective~~ | **Fixed on `main`** — see below. Unreleased. |
| `perf/NP-021` | ~~Reconstruction cost superlinear in separator count~~ | **Fixed on `main`** — see below. Unreleased. |
| `recon/NP-022` | A field literally named `___` collides with the reconstructor's `__*__` sentinel namespace and is silently dropped. | Silent field loss for that specific name shape. |

### `arch/NP-002` / `perf/NP-021` — fixed by an injective key encoding

**Still present in the released 1.0.8. Fixed on `main`, unreleased.**

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

Every pull request runs CodeQL, GitHub dependency review (blocking on high-severity advisories in
newly-added dependencies), and an OWASP dependency-check CVE scan. A CycloneDX SBOM is published
with each release. Dependabot proposes updates weekly, with security updates ungrouped so they
arrive as individually reviewable PRs.
