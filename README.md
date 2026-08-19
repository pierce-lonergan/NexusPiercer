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
corpus of **161 fixtures** that are executed on every build:

| Classification | Count | Meaning |
|---|---:|---|
| `LOSSLESS` | 56 | Round-trips exactly, and that is correct |
| `ACCEPTED_LOSS` | 24 | Does not round-trip; the reason is stated and defensible |
| `DEFECT` | 81 | Does not round-trip, and that is a bug we have not fixed |

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

No Maven Central access? Three verified routes that need no Central at all — a release jar, a
source build, and a fully air-gapped install — are documented in
[docs/INSTALL.md](docs/INSTALL.md), including a self-contained shaded jar.

## Quick start

### Flatten a JSON document

<!-- snippet: body env=core -->
```java
import io.github.pierce.JsonFlattenerConsolidator;

// arrayDelimiter, nullPlaceholder, maxNestingDepth, maxArraySize,
// consolidateWithMatrixDenotorsInValue
var flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false);

String flat = flattener.flattenAndConsolidateJson("""
    {"user": {"name": "ada", "tags": ["x", "y"]}}
    """);
// {"user.name":"ada","user.tags":"x,y"}
```

To emit one record per array element instead of consolidating:

<!-- snippet: body env=core -->
```java
List<String> exploded = new JsonFlattenerConsolidator(",", null, 50, 1000, false)
        .flattenAndExplodeJson(json);
```

### Flatten an Avro schema

<!-- snippet: body env=core -->
```java
import io.github.pierce.AvroSchemaFlattener;
import java.util.Set;

var schemaFlattener = new AvroSchemaFlattener();

// getFlattenedSchemaNoCache, not getFlattenedSchema: the cached factory keys on the schema's
// full name and will hand back another schema's columns when the name repeats.
Schema flattened = schemaFlattener.getFlattenedSchemaNoCache(schema);

Set<String> arrayFields = schemaFlattener.getArrayFieldNames();
Schema roundTripped     = schemaFlattener.reconstructOriginalSchema(flattened);
```

### Enriched flattening — the schema pipeline API

`AvroSchemaFlattener` emits `Schema.Field`s, which carry a name and a type and nothing else.
Everything downstream — custom properties, documentation, whether an array was crossed, what the
parent record was called — has to be recovered by parsing the name, and a name is not a reliable
place to recover structure from. `user_id` and the nested path `user` → `id` are indistinguishable
once rendered.

`io.github.pierce.schema` keeps that information instead of throwing it away:

<!-- snippet: body env=core -->
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
        .injectField(1, FlattenedField.builder()       // fixed position, source order preserved
                .name("ingested_at")
                .avroType(Schema.Type.LONG)
                .synthetic(true)
                .build())
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

<!-- snippet: body env=core -->
```java
new EnrichedSchemaFlattener(FlattenOptions.defaults())
        .stream(schema, field -> System.out.println(field.flattenedName()));
```

Migrating from `GAvroSchemaFlattener`? `FlattenOptions.gAvroParity()` reproduces its naming
conventions, and its javadoc names the four structural cases where parity does not hold.
`FlattenOptions.defaults()` carries this library's own conventions instead.

Failures are typed, not stack overflows:

<!-- snippet: body env=core -->
```java
import io.github.pierce.schema.RecursiveSchemaException;
import io.github.pierce.schema.SchemaFlattenException;
import io.github.pierce.schema.SchemaLimitExceededException;

try {
    new EnrichedSchemaFlattener().flatten(schema);
} catch (RecursiveSchemaException e) {      // a named type contains itself
    System.err.println(e.getSchemaName() + " at " + e.getPath());
} catch (SchemaLimitExceededException e) {  // maxDepth or maxFields hit
    System.err.println(e.getMessage());
} catch (SchemaFlattenException e) {        // not a record, or a name collision
    System.err.println(e.getMessage());
}
```

### Use it from Spark

<!-- snippet: body env=spark -->
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

<!-- snippet: body env=spark -->
```java
// Both take the SparkSession and a path, not an already-loaded Dataset. Between 2.0.0 and
// 2.1.0 this block published a two-argument (Dataset, String) form that has never existed.
Dataset<Row> quality = NexusPiercerPatterns.generateDataQualityReport(
        spark, "schema.avsc", "input/*.json");
Dataset<Row> profile = NexusPiercerPatterns.profileJsonStructure(
        spark, "input/*.json", 100);   // sample size
```

`FlattenOptions` is `Serializable`, so a configured flattener can be captured into a Spark closure
and shipped to executors.

### Which flattener do I use?

Six public types have "Flattener" in the name and the names do not tell you them apart. Three
flatten DATA; two flatten a SCHEMA only; and `GAvroSchemaFlattener` flattens a schema **and**
carries `applyTypes`, the per-record type-casting step the Spark streaming path calls — so "the
schema ones never touch a record" is true of two of the three, not three. Renaming them is a
breaking change and is deferred to 3.0.0 ([BL-016]), so until then this table is the selection
rule.

| Class | Flattens | Reach for it when |
|---|---|---|
| **`MapFlattener`** | data — `Map` → flat `Map` | **Default choice.** This is the engine; everything else here is a facade, a different input type, or schema-only. |
| `JsonFlattener` | data — fluent facade over `MapFlattener` | Prefer `MapFlattener`. It holds a `MapFlattener` internally and adds a fluent chain; before 2.1.0 no caller could even obtain one, since the constructor is private and every factory returned `FluentOperation` ([BL-010]). |
| `JsonFlattenerConsolidator` | data — independent Jackson implementation | Only from the Spark layer, which is what calls it. It contains **no** `MapFlattener`, so its output is not guaranteed to match, and the fidelity corpus does not cover it. |
| `EnrichedSchemaFlattener` | schema only | **Default choice for schemas.** The current one: `final`, configured by `FlattenOptions`, and the target of the enriched pipeline API above. |
| `AvroSchemaFlattener` | schema only | Legacy. Emits Avro `Schema.Field`s directly. |
| `GAvroSchemaFlattener` | schema **+ per-record type casting** | Legacy, and independent of `AvroSchemaFlattener` despite the name. `flattenSchema(Schema)` emits names shaped to match `MapFlattener`'s data output; `applyTypes(flattenedData, flattenedSchema)` then casts a flattened RECORD against that schema, and its own javadoc calls it "the hot path method called for every record in streaming". That method is the reason to reach for this class — it is how flattened data gets typed against a GAvro-flattened schema, and neither of the other two schema flatteners has an equivalent. |

For reading a schema file off disk, use **`SchemaFiles`**, not `FileFinder` — the latter is
deprecated in 2.1.0.

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
- `SchemaFiles` path-traversal, null-byte and size enforcement on every schema read
  (`FileFinder` is deprecated in 2.1.0; see the note below for what its guards did and did
  not cover in 2.0.0)
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
| [Architecture](docs/ARCHITECTURE_GRAPH.md) | Component, dependency and data-flow diagrams — every class-to-class edge is asserted against the source by a test |
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

**Repaired in 2.1.0**, and each one changes what a 2.0.0 caller gets back — see
[CHANGELOG.md](CHANGELOG.md) for the behaviour-change list:

- An Avro array of records whose element fields all live inside a nested record returned **one**
  element instead of N, under every array format including the default
- `AvroReconstructor.arrayFormat` was inert for arrays of records; it is now honoured, and columns
  whose lengths disagree raise `ArrayCardinalityException` instead of being padded or duplicated
- A missing required field fails with the flattened path instead of leaking Avro's own exception,
  and an empty flattened map is no longer special-cased

**Fixed and released in 2.0.0**, all previously listed here as limitations:

- Flattened keys are now injectively encoded, so `user_id` and `user.id` are distinct and the
  separator-driven heap exhaustion is gone
- `FileFinder`'s `validatePaths`, `allowedExtensions` and `maxFileSize` began to be enforced —
  they were settable and read nowhere, behind a builder that implied otherwise. **Stated more
  precisely in 2.1.0, because that sentence claimed more than the code did:** in 2.0.0 the
  size gate covered only names resolving as a regular file relative to the working directory,
  and `fileExists`/`getFileMetadata` bypassed all three guards by calling the cache loader
  directly. `AvroSchemaLoader` also caught and discarded the traversal `SecurityException`
  and then read the file through an unvalidated fallback. All four are closed in 2.1.0, and
  `FileFinder` is deprecated in favour of `SchemaFiles`
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
- **`AvroReconstructor.reconstruct()` returns a datum that fails `GenericData.validate`** when the
  record has nesting — nested values come back as `LinkedHashMap`. Use `reconstructToMap` unless
  you need a writable record. The **flat** case is repaired in 2.1.0: a defaulted enum, fixed,
  bytes or `null` field absent from the input used to break even a flat record at the shipped
  default configuration, and now arrives as its schema-correct Avro type
  ([`recon/NP-023`](SECURITY.md))
- **An Avro union still takes the first branch that will accept the value**, and for a union of
  records the first branch sharing any field name wins with the rest of the data dropped. Repaired
  in 2.1.0 for one position only: inside an *array element*, a union of three or more branches used
  to be dropped to `null` in complete silence and is now resolved by branch — chosen by the value's
  Java type first and declaration order second, so a string in the document stops being coerced
  into an earlier numeric or boolean branch — or refused by name when two record branches match the
  same columns
- **An array of records read at the JSON default no longer accepts unbracketed delimited text.**
  It used to return one record holding the concatenation of all of them, silently; it now raises
  `ArrayFormatMismatchException` naming the format that *would* read the data. Set `arrayFormat`
  to match your producer

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
