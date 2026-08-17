<div align="center">

# NexusPiercer

**Flatten, consolidate, and reconstruct deeply nested JSON and Avro for Apache Spark.**

[![CI](https://github.com/pierce-lonergan/NexusPiercer/actions/workflows/ci.yml/badge.svg)](https://github.com/pierce-lonergan/NexusPiercer/actions/workflows/ci.yml)
[![Quality](https://github.com/pierce-lonergan/NexusPiercer/actions/workflows/quality.yml/badge.svg)](https://github.com/pierce-lonergan/NexusPiercer/actions/workflows/quality.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.pierce-lonergan/nexus-piercer.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.pierce-lonergan/nexus-piercer)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://adoptium.net/)

</div>

---

## What it does

Nested JSON and Avro do not fit comfortably into columnar warehouses. NexusPiercer turns a nested
record into a flat, analysable one — and, for Avro, turns it back again.

```
{"user": {"name": "ada", "tags": ["x","y"]}}
        │
        │  flatten
        ▼
{"user.name": "ada", "user.tags": "x,y"}
        │
        │  reconstruct  (Avro path)
        ▼
{"user": {"name": "ada", "tags": ["x","y"]}}
```

Four things ship in the box:

| | |
|---|---|
| **JSON flattener / consolidator** | Collapse nested documents to flat maps, with array consolidation or explosion |
| **Avro schema flattener + reconstructor** | Flatten a schema to columns, flatten records to rows, rebuild records from rows |
| **Enriched schema API** (`io.github.pierce.schema`) | Structured provenance, custom-property preservation, type mapping, governance hooks |
| **Spark integration** | UDFs, `Column` wrappers, pipeline helpers and profiling reports |

## What makes it different

Most flatteners will tell you what they produce. This one tells you **what you lose**.

Every release ships a [round-trip fidelity guarantee](docs/ROUND_TRIP_FIDELITY.md) generated from a
corpus of **156 fixtures** that are executed on every build:

| Classification | Count | Meaning |
|---|---:|---|
| `LOSSLESS` | 51 | Round-trips exactly, and that is correct |
| `ACCEPTED_LOSS` | 23 | Does not round-trip; the reason is stated and defensible |
| `DEFECT` | 82 | Does not round-trip, and that is a bug we have not fixed |

A `DEFECT` fixture asserts the defect is **still present**, so repairing one turns the build red and
forces a deliberate update to the published contract. The document cannot drift from the corpus —
it is generated from the manifest and a test asserts the committed bytes match.

If you need to know whether *your* data survives, read that document before adopting. The honest
summary is that structural flattening is reliable and full JSON round-tripping is not.

## Install

```xml
<dependency>
    <groupId>io.github.pierce-lonergan</groupId>
    <artifactId>nexus-piercer</artifactId>
    <version>2.0.0</version>
</dependency>
```

Requires **Java 17+**. Spark integration is built against **Spark 3.5.x / Scala 2.12**.

No Maven Central access? Four verified offline install routes are documented in
[docs/INSTALL.md](docs/INSTALL.md), including a self-contained shaded jar.

## Quick start

### Flatten a JSON document

```java
import io.github.pierce.JsonFlattenerConsolidator;

// arrayDelimiter, nullPlaceholder, maxNestingDepth, maxArraySize, preserveArrayOrder
var flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false);

String flat = flattener.flattenAndConsolidateJson("""
    {"user": {"name": "ada", "tags": ["x", "y"]}}
    """);
// {"user.name":"ada","user.tags":"x,y"}
```

To emit one record per array element instead of consolidating:

```java
List<String> exploded = flattener.flattenAndExplodeJson(json);
```

### Flatten an Avro schema

```java
import io.github.pierce.AvroSchemaFlattener;

var flattener = new AvroSchemaFlattener();
Schema flattened = flattener.getFlattenedSchema(originalSchema);

Set<String> arrayFields = flattener.getArrayFieldNames();
Schema roundTripped   = flattener.reconstructOriginalSchema(flattened);
```

### Enriched flattening — the schema pipeline API

`AvroSchemaFlattener` emits `Schema.Field`s, which carry a name and a type and nothing else.
Everything downstream — custom properties, documentation, whether an array was crossed, what the
parent record was called — has to be recovered by parsing the name, and a name is not a reliable
place to recover structure from. `user_id` and the nested path `user` → `id` are indistinguishable
once rendered.

`io.github.pierce.schema` keeps that information instead of throwing it away:

```java
import io.github.pierce.schema.*;

var options = FlattenOptions.builder()
        .separator("_")
        .arrayBoundarySeparator("__")
        .collisionPolicy(NameCollisionPolicy.FAIL)     // refuse ambiguity, don't mangle it
        .typeMapper(field -> switch (field.avroType()) {
            case STRING -> "VARCHAR(255)";
            case LONG   -> "BIGINT";
            default     -> "VARCHAR(MAX)";
        })
        .leafInterceptor(field -> field.properties().put("x-scanned", true))
        .injectField(1, auditColumn())                 // fixed position, source order preserved
        .maxDepth(64)
        .maxFields(100_000)
        .build();

List<FlattenedField> fields = new EnrichedSchemaFlattener(options).flatten(schema);

for (FlattenedField f : fields) {
    f.flattenedName();        // the column name
    f.pathSegments();         // structured ancestry — not a parsed string
    f.arrayBoundaries();      // which arrays were crossed, in order
    f.doc();                  // inherited from the nearest ancestor record if absent
    f.isDocInherited();       // ...and whether it was
    f.mappedType();           // whatever your TypeMapper returned
    f.isNullable();
    f.isWithinArray();
    f.isPrimaryKeyEligible(); // necessary, not sufficient — schemas cannot know uniqueness
    f.properties();           // every x- property the producer declared, preserved
    f.position();
    f.isSynthetic();
}
```

For schemas too wide to materialise, stream instead — injections and guards apply identically:

```java
new EnrichedSchemaFlattener(options).stream(schema, field -> sink.accept(field));
```

Migrating from `GAvroSchemaFlattener`? `FlattenOptions.gAvroParity()` reproduces its naming
conventions, and its javadoc names the four structural cases where parity does not hold.
`FlattenOptions.defaults()` carries this library's own conventions instead.

Failures are typed, not stack overflows:

```java
try {
    new EnrichedSchemaFlattener().flatten(schema);
} catch (RecursiveSchemaException e) {      // a named type contains itself
    log.error("{} at {}", e.getSchemaName(), e.getPath());
} catch (SchemaLimitExceededException e) {  // maxDepth or maxFields hit
    ...
} catch (SchemaFlattenException e) {        // not a record, or a name collision
    ...
}
```

### Use it from Spark

```java
import io.github.pierce.spark.NexusPiercerFunctions;
import static org.apache.spark.sql.functions.col;

Dataset<Row> flattened = df.withColumn(
    "flat", NexusPiercerFunctions.flattenJson(col("raw_json")));
```

`NexusPiercerFunctions` exposes both `UserDefinedFunction` fields (`flattenJson`,
`extractJsonArray`, `explodeJsonArray`, `isValidJson`, …) and `Column`-returning wrappers
(`flattenJson(Column)`, `arrayCount(Column, String)`, `isValid(Column)`, …).

`NexusPiercerPatterns` provides two higher-level reports:

```java
Dataset<Row> quality = NexusPiercerPatterns.generateDataQualityReport(df, "raw_json");
Dataset<Row> profile = NexusPiercerPatterns.profileJsonStructure(df, "raw_json");
```

`FlattenOptions` is `Serializable`, so a configured flattener can be captured into a Spark closure
and shipped to executors.

## Capability reference

<details>
<summary><b>Flattening</b></summary>

- Nested objects to a configurable depth, with a fail-closed `maxDepth`
- Arrays consolidated with a delimiter, or exploded one record per element
- Array-boundary marking in rendered names, plus structured `arrayBoundaries()` that is reliable
- Injective key encoding ([`FlattenedPath`](src/main/java/io/github/pierce/path/FlattenedPath.java))
  so `user_id` and `user.id` no longer collide
- Name-collision detection under both `FAIL` and `ESCAPE` policies, for source and injected columns
- Positional column injection, with source field order never reordered
- Streaming emission for wide schemas
- Bounded, instrumented schema cache with `SchemaCacheStats.hitRate()`

</details>

<details>
<summary><b>Reconstruction</b></summary>

- Avro records rebuilt from flattened rows, with the original or a supplied schema
- Original schema rebuilt from a flattened schema
- Configurable separator, array format, null preservation and array-path hints
- Depth-bounded to stop hostile input exhausting the heap

</details>

<details>
<summary><b>Type handling</b></summary>

- Avro primitives, records, enums, fixed, bytes, maps, arrays, unions
- Nullable-union unwrapping with `isNullable()` preserved
- Logical types (decimal, date, time, timestamp, uuid) — see the fidelity doc for which survive
- `BigInteger` beyond `long` range preserved exactly rather than wrapped
- Optional `BigDecimal` text preservation

</details>

<details>
<summary><b>Operational</b></summary>

- Typed exceptions carrying schema name and path
- `FileFinder` path-traversal, null-byte, extension and size enforcement
- JMH benchmark harness with allocation-based (machine-independent) regression gates
- Ratcheted Checkstyle / PMD / SpotBugs ceilings that may only decrease
- CycloneDX SBOM, OWASP dependency-check, CodeQL, reproducible-build verification

</details>

## Documentation

| Document | What's in it |
|---|---|
| [Round-trip fidelity](docs/ROUND_TRIP_FIDELITY.md) | **What survives a round trip, fixture by fixture** — generated, drift-guarded |
| [Install](docs/INSTALL.md) | Maven Central plus four offline install routes |
| [Performance](docs/PERFORMANCE.md) | Benchmark methodology and measured results |
| [Anti-regression](docs/ANTI_REGRESSION.md) | How the gates and ratchets work |
| [Audit findings](docs/audit/FINDINGS.md) | Full engineering audit — 200 verified findings, ranked |
| [Roadmap](docs/audit/ROADMAP.md) | Phased remediation plan and benchmark design |
| [Backlog](docs/BACKLOG.md) | Known issues not yet scheduled |
| [Architecture](docs/ARCHITECTURE_GRAPH.md) | Component and data-flow diagrams |
| [API surface](docs/API_SURFACE.md) | Generated API inventory |
| [Contributing](CONTRIBUTING.md) | Build, test, and PR workflow |
| [Security](SECURITY.md) | Reporting a vulnerability |
| [Changelog](CHANGELOG.md) | Release history |

## Building from source

```bash
./mvnw verify
```

Useful variations:

```bash
./mvnw -Pfast package
```
```bash
./mvnw -Pquality verify
```
```bash
./mvnw -Psecurity verify
```

The build is pure Java — Java 17, single-language, no Groovy toolchain. Every PR runs the full
suite on JDK 17 and 21 across Linux and Windows.

## Project status — read this before adopting

2.0.0 is the first tagged, reproducible release from this repository. A full audit
([docs/audit/FINDINGS.md](docs/audit/FINDINGS.md)) is complete and remediation is phased in
[docs/audit/ROADMAP.md](docs/audit/ROADMAP.md).

**Fixed and released in 2.0.0**, all previously listed here as limitations:

- Flattened keys are now injectively encoded, so `user_id` and `user.id` are distinct and the
  separator-driven heap exhaustion is gone
- `FileFinder`'s `validatePaths`, `allowedExtensions` and `maxFileSize` are enforced — they were
  settable and read nowhere, behind a builder that implied otherwise
- Recursive Avro schemas fail with a typed `RecursiveSchemaException` on the enriched API

**Still true today.** These are the headline entries from the fidelity manifest; the full list with
reproducible fixtures is in [docs/ROUND_TRIP_FIDELITY.md](docs/ROUND_TRIP_FIDELITY.md):

- **An array of objects does not survive a JSON round trip**, and an array of scalars turns its
  parent object into an array
- **`{}`, `[]` and `null` are the same value** after flattening
- **A non-finite number is indistinguishable from a string of the same text**, and an integer past
  `Long.MAX_VALUE` becomes a quoted string
- **Declared decimal scale is destroyed**, and the flag that claims to prevent it cannot fire on
  the JSON stack
- **Field names made of underscores collide with internal markers and are silently deleted** —
  the whole `__*__` namespace is swallowed, including `__meta__` and `__type__` headers that event
  envelopes routinely carry
- **Anything whose serialised text starts with a bracket is turned into an array**
- **Recursive schemas still exhaust the stack on the legacy `AvroSchemaFlattener`** — use the
  enriched API if you accept untrusted schemas
- **`AvroReconstructor.reconstruct()` returns a datum that fails `GenericData.validate`** — nested
  values come back as `LinkedHashMap`. Use `reconstructToMap` unless you need a writable record

Use the Avro path where fidelity matters. Treat the JSON round trip as lossy unless a fixture says
otherwise for your shape.

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). Every PR runs the full test
suite on JDK 17 and 21 across Linux and Windows, plus static analysis, CVE scanning, and CodeQL.

If you fix something the fidelity corpus classifies as a `DEFECT`, the build will go red. That is
intended: update the manifest and regenerate the guarantee in the same change.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
The repository is [REUSE](https://reuse.software/)-compliant; per-file licensing is in
[.reuse/dep5](.reuse/dep5).
