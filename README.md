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

It ships three things: a JSON flattener/consolidator, an Avro schema flattener with a matching
record reconstructor, and a set of Spark UDFs and pipeline helpers built on top.

## Install

```xml
<dependency>
    <groupId>io.github.pierce-lonergan</groupId>
    <artifactId>nexus-piercer</artifactId>
    <version>1.0.8</version>
</dependency>
```

Requires **Java 17+**. Spark integration is built against **Spark 3.5.x / Scala 2.12**.

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

## Documentation

| Document | What's in it |
|---|---|
| [Audit findings](docs/audit/FINDINGS.md) | Full engineering audit — 200 verified findings, ranked |
| [Roadmap](docs/audit/ROADMAP.md) | Phased remediation plan and benchmark design |
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

## Project status — read this before adopting

This project is being brought up to production standard in the open, and it is more useful to say
where it actually stands than to imply it is finished. A full audit
([docs/audit/FINDINGS.md](docs/audit/FINDINGS.md)) is complete and the remediation is phased in
[docs/audit/ROADMAP.md](docs/audit/ROADMAP.md).

Known limitations you should weigh today:

- **Flattened keys are not escaped in 1.0.8.** A field literally named `user_id` and a nested
  `user.id` produce the same flattened key, so reconstruction is lossy for schemas whose field
  names contain the separator — which is most snake_case schemas. The same ambiguity also made
  reconstruction exhaust the heap on ordinary input. **Both are fixed on `main`** by an injective
  encoding, but are not yet released. If you are on 1.0.8, configure a separator that cannot occur
  in your field names. Details in [SECURITY.md](SECURITY.md).
- **A field literally named `___` is silently dropped** during reconstruction — it collides with
  an internal sentinel namespace. Not yet fixed.
- **Recursive Avro schemas are not guarded.** A self-referential `.avsc` will exhaust the stack
  or the heap rather than failing with a typed error.
- **`FileFinder`'s `validatePaths`, `allowedExtensions`, and `maxFileSize` options are not
  enforced.** Do not pass untrusted input to schema-discovery APIs.

Versions before 2.0 should be treated as pre-production.

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). Every PR runs the full test
suite on JDK 17 and 21 across Linux and Windows, plus static analysis, CVE scanning, and CodeQL.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
The repository is [REUSE](https://reuse.software/)-compliant; per-file licensing is in
[.reuse/dep5](.reuse/dep5).
