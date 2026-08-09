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
| `arch/NP-002` | Flattened key encoding is not injective; the separator is not escaped. | Silent data corruption, not a crash. Fields whose names contain the separator collide. |
| `perf/NP-021` | **Reconstruction cost is superlinear in the number of separator characters inside field names.** See below. | Heap exhaustion. A document with ordinary snake_case field names can hang a driver. |

### `perf/NP-021` — separator-driven reconstruction blow-up

Found by the JMH harness on its first run and reproduced deterministically. Structure held
completely fixed at 5 sibling record arrays × N records × 8 fields, producing an identical
**40 flattened keys** in every case. The only variable is how many literal underscores appear in
each field name (JDK 21, 1 GB heap):

| Field name | 25 records | 50 records | 75 records |
|---|---:|---:|---:|
| `field_{n}` — one `_` | 196 ms | 174 ms | 233 ms *(flat through 150 records)* |
| `nested_field_{n}` — two `_` | 1,198 ms | 3,435 ms | **OutOfMemoryError** |

One extra underscore per field name takes reconstruction from flat-and-linear to heap exhaustion.
The reconstructor cannot distinguish a structural separator from a literal one, so the number of
candidate groupings it must consider grows with underscores-per-name and field count.

This matters because `nested_field_x` is an unremarkable name — and so are `user_id`,
`created_at`, and `order_total`, which dominate this library's target domain. **A document that
looks entirely ordinary can exhaust the heap.**

Mitigation until the encoding is fixed: configure a separator that cannot occur in your field
names, and bound document size before reconstruction. Pinned by
`SeparatorInFieldNameRegressionTest` so it cannot silently worsen.

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
