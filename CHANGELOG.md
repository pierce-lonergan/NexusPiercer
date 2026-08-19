# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Version on `main` is `2.1.0-SNAPSHOT`. `2.0.0` is released and staged on Maven Central and `main`
must not sit on a released coordinate.

The public API surface is additive-only and enforced by
`PublicApiIsAdditiveOnlySinceReleaseTest` against a baseline in `src/test/resources`. **OUTPUT
BEHAVIOUR IS NOT.** The section immediately below lists **twenty-nine** places where a `2.0.0`
caller gets a different answer, several of them at the default configuration. Read it before
upgrading.

**Ten of the twenty-nine turn a previously-successful call into a throw**, across nine items:
a bracketed JSON array read under a delimited format, and its mirror, an unbracketed delimited
column read under the JSON default (item 3, two cases); disagreeing column counts (item 4); an
oversized read, which for a compressed file can throw MID-READ (item 14); a bare name that only
resolved through a parent-directory search path (item 15); a `.avsc` outside the working tree
that `AvroSchemaLoader` used to reach through an unvalidated fallback (item 17); a bracketed
column a caller has named in `arrayPaths()` that is not parseable JSON (item 18); and a bare
schema name passed to `AvroSchemaFlattener.getFlattenedSchema(String)` (item 19); and a
document whose array-element cell count exceeds `maxArrayCells` (item 23); and a
flattened map holding a key that is also an intermediate path of a longer key -
`a` beside `a_b` - which `JsonReconstructor` used to resolve by map iteration order
(item 28).

Items 3 and 6 each carry a sub-entry recording a defect that the first version of this release's
own repair INTRODUCED and that was caught in adversarial review before release, and item 18
carries one recording a false claim its own error message made about `MapFlattener`. All three
are stated in full rather than quietly folded into the surrounding text, because a repair that
creates the fault it removes is the single most useful thing this changelog can tell a reader.

### Behaviour changes

**These change what the library returns at the SHIPPED DEFAULT configuration.** Items 1–12 are
`AvroReconstructor`; items 13, 21, 22, 23, 27 and 29 are `MapFlattener` and the flattener's schema read; items 24-26 are
`JsonFlattener`; items
14–16 and 20 are `FileFinder`; item 17 is `AvroSchemaLoader`; items 18 and 28 are `JsonReconstructor`;
item 19 is `AvroSchemaFlattener`. They are listed here rather than under *Fixed* because a caller
pinning a snapshot of today's output will see a diff, and because ten previously-successful
calls now throw. No public signature, return type, parameter list or visibility changes — the new
exception types and the added `ReconstructionException(String)` constructor are additive and
clear the 2.0.0 additive-only gate.

1. **A schema default absent from the input now arrives as its schema-correct Avro type.**
   ENUM: `java.lang.String` → `GenericData.EnumSymbol`. FIXED: `byte[]` → `GenericData.Fixed`.
   BYTES: `byte[]` → `ByteBuffer`. STRING: `java.lang.String` → `org.apache.avro.util.Utf8`.
   A `"default": null` on a nullable field: the `org.apache.avro.JsonProperties.NULL_VALUE`
   singleton → a **real Java null**, so `value == null` starts being true where it was false.
   Numeric and boolean defaults are unchanged. The point of the change is that
   `GenericData.validate` now returns true and the datum binary-encodes, where before it returned
   false and the write threw `AvroTypeException` or `ClassCastException` in an executor,
   downstream of an apparently successful reconstruction. Every default now goes through
   `GenericData.getDefaultValue`, deep-copied — the copy is not optional, because getDefaultValue
   memoises one shared mutable instance per field. A caller who worked around this with
   `useSchemaDefaults(false)` no longer needs to. (`recon/NP-023`)

   TWO LIMITS, stated so nobody reads more into this than it carries. A RECORD-typed default is
   repaired leaf-by-leaf in `reconstructToMap`, but `reconstruct()` still hands it back as a
   `LinkedHashMap` and still fails validate, because `mapToGenericRecord` rebuilds only the root
   record — that is the separate `avro-generic-record-unwritable` defect. And a defaulted
   LOGICAL-TYPE field arrives as its raw underlying type (a `ByteBuffer` for a decimal) while the
   same field supplied in the input arrives converted: a new, smaller inconsistency created by
   this fix and disclosed rather than left to be discovered.

2. **An array of records whose element fields all live inside a nested record now returns N
   elements.** It returned exactly one, and the other N-1 were gone — with no exception, no log
   above debug, and a datum that validates against its own schema. Measured identically under all
   four array formats including the JSON default, so this was not confined to an exotic
   configuration. (`BL-013`)

3. **`AvroReconstructor.arrayFormat` now changes the result for arrays of records.** It was
   provably inert there — all four settings produced byte-identical output on a document built to
   reach the branch — because the split ran in a `static` method that could not read the instance
   field and inferred the delimiter from the data, comma before pipe. A caller who set
   `PIPE_SEPARATED` and has commas in their values now gets fewer, correct elements instead of
   more, corrupted ones. A caller who set a delimited format but is feeding it JSON-shaped text
   now gets `ArrayFormatMismatchException` where they previously got a correct answer by luck;
   that is the one regression risk here, and it is deliberate — JSON's grammar is self-delimiting
   and the comma/pipe writers cannot emit a bracketed quoted list, so it is a detectable
   contradiction rather than a guess. (`BL-013`)

   **THE CONTRADICTION IS NOW CHECKED IN BOTH DIRECTIONS, and for one release it was not.** The
   first version of this split guarded only the direction above. In the other direction — an
   UNBRACKETED delimited column read under `arrayFormat=JSON`, which is the SHIPPED DEFAULT — it
   introduced a new silent N-to-1 collapse, the exact defect class this item exists to remove.
   Measured with one probe against both builds:

   ```
   {items_sku=S1,S2,S3, items_name=N1,N2,N3}  against  Order{id, items: array<Item{sku,name}>}
     before this split : 3 elements  [{sku=S1,name=N1},{sku=S2,name=N2},{sku=S3,name=N3}]
     first version     : 1 element   [{sku=S1,S2,S3, name=N1,N2,N3}]        no throw, no log
   ```

   `MapFlattener`'s JSON writer always brackets a column — a single-element one arrives as
   `["a"]` — so an unbracketed column cannot have been produced by it, which is precisely the
   reasoning that justifies throwing in the opposite direction. Such a column now raises
   `ArrayFormatMismatchException` naming the configured format, the format that WOULD read the
   data, and how many records the other reading finds. **A caller feeding unbracketed delimited
   text at the default configuration therefore moves from a wrong answer to a thrown exception.**
   The guard is narrowed to the case where the two readings DISAGREE: an unbracketed column with
   no delimiter in it reads as one element under every rule and is left alone.

4. **Columns of unequal length now throw `ArrayCardinalityException`**, naming every column, its
   count and the configured format. The longest column used to win: short scalar columns were
   padded with `""` and `0`, and short nested-record columns had their LAST value duplicated into
   every remaining row. Measured: `sku=S1,S2,S3` beside `meta_code=C1,C2` produced a third row
   whose code repeated the second, and the reverse direction silently discarded `C3`. A
   previously-successful call becomes a failure — deliberately, because the success was fictional.
   Not gated on `strictValidation` or `allowMissingFields`: those knobs already select *which*
   exception fires rather than whether one does, and overloading them again would repeat the
   defect. (`BL-013`)

5. **An array node with no element data returns `[]`** instead of one record of type-defaults.
   The old sizing routine ended in `maxSize > 0 ? maxSize : 1` and so could never return zero,
   which made the existing empty-array branch dead code. (`BL-013`)

6. **A union of three or more branches inside an array element is now resolved instead of being
   dropped.** `reconstructArrayOfRecords` never had a UNION arm: `unwrapNullable` collapses only
   `[null, T]`, so a wider union matched neither the RECORD nor the ARRAY test and fell through to
   `handleMissingField`, which saw a null branch and wrote a plain null — in total silence, with
   the child node holding the real data never read. A consumer that has been null-checking that
   field and skipping it now receives data. On the flat-column side of the same field the value
   changes from unconverted pass-through to a value converted against the SELECTED BRANCH, so
   `GenericData.validate` against the FIELD's union schema returns true where it returned false —
   a Jackson-boxed `Integer` in a `["null","long","string"]` slot resolves to no branch at all,
   and a `Long` resolves to one.

   **This does NOT make the enclosing datum writable, and an earlier version of this bullet said
   it did.** Measured at the public `reconstruct()` entry point: the record still fails
   `GenericData.validate` and a real `GenericDatumWriter` still throws — with a
   `ClassCastException` naming the array element as a `java.util.LinkedHashMap`, not an
   `UnresolvedUnionException`. A different blocker sits in front of the union one and is untouched
   here: `mapToGenericRecord` rebuilds only the ROOT record, so array elements remain
   `LinkedHashMap`. That is the same `avro-generic-record-unwritable` limit already disclosed
   under item 1, and `AvroReconstructorArrayElementUnionTest` has asserted it as a pinned limit
   all along — the test was honest and this bullet was not. The VALUE is repaired; the datum is
   not.

   **The branch is chosen by Java type first, declaration order second, and the first version of
   this arm got that wrong.** Trying the branches in pure declaration order and taking the first
   that did not throw destroyed values the flattener had NOT made ambiguous, because at the
   array-element position the JSON column keeps the quotes and Jackson boxes the element as a
   `String`. Measured against the shipped default with a real `MapFlattener`:

   | union | document value | before | now |
   |---|---|---|---|
   | `["null","int","long","string"]` | `"0007"` | `Integer 7` | `String "0007"` |
   | `["null","long","string"]` | `"123"` | `Long 123` | `String "123"` |
   | `["null","boolean","string"]` | `"hello"` | `Boolean false` | `String "hello"` |
   | `["null","string","long"]` | `123` | `String "123"` | `Long 123` |

   The BOOLEAN row is the worst of them and was not in the original filing: `Boolean.parseBoolean`
   never throws, so a boolean branch declared before a string branch accepted *anything* and made
   the string branch unreachable. A branch carrying a logical type is never demoted, and where the
   value's Java type has no native branch — `"0007"` under `["null","int","long"]` — the coercion
   is unavoidable and still happens, but now logs a WARN naming the before and after text. This is
   distinct from the TOP-LEVEL union, which is untouched: there the flat map holds `meta=0007` as
   a bare unquoted scalar, so the type really has been erased and any choice is a guess.

   Where the flattened form genuinely cannot disambiguate — two record
   branches in the same union sharing the columns that are present — the result changes from a
   silent null to a thrown exception naming both candidates under the default
   `strictValidation`, or to a null plus a WARN under `strictValidation(false)`. That half will
   break a pipeline that today merely loses data, and it is intended: a failure that cannot be
   repaired must at least stop being invisible. `unwrapNullable`'s `[null, T]` contract is
   unchanged, so every two-branch union anywhere in the codebase behaves exactly as before.
   (`BL-014`)

7. **`allowMissingFields` now defaults to `false` and means something at both values.** The
   OUTCOME at the shipped default is unchanged — a missing required field failed before and fails
   now — but it fails with a `ReconstructionException` naming every such field by its FLATTENED
   PATH, aggregated, instead of letting `org.apache.avro.AvroMissingFieldException` escape from
   `GenericRecordBuilder.build()` with only the field name. Anyone catching
   `AvroMissingFieldException` by class must change. At `true` the Avro TYPE default is
   substituted (`""`, `0`, `false`, `[]`, `{}`, empty bytes) with one aggregated WARN naming every
   path filled — except ENUM, FIXED, RECORD and a null-free UNION, where no type default exists
   and reconstruction fails saying exactly that. Keeping `true` as the default while giving it a
   tolerant meaning would have turned today's loud failure into a silently invented `""` at the
   shipped configuration. (`recon/NP-024`)

   KNOWN INCONSISTENCY, disclosed rather than discovered: the flag still does not reach
   `handleMissingField`, which fills `""` and `0` for a missing required field one level down
   inside an array element regardless of the setting. It is at least audible now — it logs a WARN
   — and gating it would turn array-of-records reconstructions that succeed today into throws.

8. **`useSchemaDefaults(false)` now genuinely suppresses a schema default.** It could always do so
   on a NULLABLE field; on a non-nullable one it left the slot unset and `build()` re-supplied the
   same value, so the knob's behaviour depended silently on nullability. The field is now treated
   as MISSING and handed to `allowMissingFields`. This also removes a lie: because
   `useSchemaDefaults` was tested BEFORE `hasDefaultValue`, the combination
   `.useSchemaDefaults(false).allowMissingFields(false)` used to report `"Required field missing
   and no default: color"` about a field that HAS a default.

9. **An empty or null flattened map is no longer special-cased.** `reconstructToMap({}, schema)`
   for a schema with a required no-default field now throws, naming the field, where it previously
   returned a partial map with that field silently dropped — while the same schema with one
   unrelated key present failed loudly. For an all-optional schema it still returns a populated
   map, with the corrected types from (1). `reconstructToMap` and `reconstruct` now agree about
   empty input; they did not before. `null` is treated exactly as empty. (`recon/NP-025`)

10. **Unparseable date, time and timestamp strings throw a different message and now carry a
    cause.** `new DateConverter().convert("not a date")` threw
    `Cannot convert not a date (String) to date: Cannot parse date from string: 'not a date'`
    with `getCause() == null`; the message now appends
    `. Tried: ISO date (yyyy-MM-dd), ISO datetime, ISO instant, M/d/yyyy, epoch millis` and the
    cause is the first `DateTimeParseException` the cascade discarded. The exception TYPE and the
    `Cannot parse X from string: '…'` prefix are unchanged, so anyone matching on either is
    unaffected; anyone equality-matching the whole message, or asserting the cause is null, sees a
    diff.

11. **The Avro FIXED size-mismatch message gains a decode trace**, naming which of the five decode
    strategies were attempted and why each declined — including the two that decline WITHOUT
    throwing and therefore used to leave no trace at all. Separately, a FIXED value that no
    strategy decodes but whose raw platform-charset bytes happen to be the right length — until
    now an entirely silent success returning fabricated bytes — logs a WARN. The returned value is
    unchanged; only its visibility is.

12. **New log output on previously silent paths, with no value change.** `JsonReconstructor` warns
    when bracket-wrapped text is not parseable JSON and is about to be returned as a one-element
    array holding the raw text. `AvroReconstructor` warns when a column is bracketed but not JSON
    under the JSON format. For anyone with log-volume alerting that is itself a visible change,
    and for anyone who has been silently losing array elements it is the first notice they have
    ever had.

13. **`MapFlattener` now writes every array-element column BY INDEX, so a value stays under the
    element that carried it.** This is a silent-corruption fix, not a loss fix, and it is the
    largest behaviour change in this release.

    Case 3 of `flattenList` and the array-of-maps arm of `extractFieldsFromList` both built their
    columns by APPENDING each value as they met it, then equalised the columns with a TAIL pad. A
    column first seen at element *k* therefore landed at index 0, and every later value for it was
    shifted left by the number of earlier elements that lacked it. **The tail pad is why nothing
    downstream could see this**: it already made every column the same LENGTH, so a length check —
    including this release's own `ArrayCardinalityException` — passed while the values sat under
    the wrong elements. Both sites now pre-size each column to the element count and write with
    `set(i, …)`; the equaliser is deleted rather than kept beside the indexed writes, so a future
    append path cannot silently re-shift and still satisfy every length assertion.

    What a caller sees. (a) Values re-associated with the correct element — measured on
    `real-world/order-optional-discount-absent`, a 15% discount that the document put on line item
    2 was being applied to line item 1, with a correct field count and a plausible shape.
    (b) Columns that were short now carry one slot per source element, so reconstructed arrays
    that used to collapse now report their true length. (c) An element that did not carry a column
    now leaves an explicit `null` at its own index rather than no entry at all.
    **(d) OUTPUT SIZE. For a SPARSE array of maps, the emitted cell count is now `columns ×
    elements`, unconditionally.** Pre-sizing is what puts a value back under its own element, and
    it also makes the padding unconditional, where the old tail pad only fired if some other
    column was already dense. MEASURED on an array of N maps each carrying one distinct key, at
    the default `MapFlattener` with `maxArraySize=1000`: N=100 goes from 390 emitted value
    characters to 49,890; N=500 from 2,390 to 1,249,890; N=1000 from 4,890 to **4,999,890 — a
    1022× growth**, and the shape moves from `O(elements + columns)` to `O(elements × columns)`.
    A dense array, where every element carries every column, is unaffected: it was already
    `columns × elements`. If your documents contain wide sparse arrays of maps, size the consumer
    accordingly before upgrading — this can turn a job that fit in memory into one that does not.
    The figures are pinned by `SparseArrayOfMapsOutputSizeTest`. Bounding the column count of a
    sparse array is deferred; see *Known issues*.

    HONEST ABOUT WHAT THIS DOES **NOT** FIX, because two published rows read better afterwards
    without losing less. `structural/heterogeneous-array-object-first` still deletes both object
    elements and `avro/avro-array-of-records-null-element-annihilates-array` still annihilates its
    record; both now return the true element count with explicit nulls where the data was, instead
    of a shorter array that looked like clean data. The array length being honest is a smaller lie,
    not a repair — and for the Avro row it means a caller who would have caught the loss by
    checking the element count no longer can. An absent key inside an array element still becomes
    a present null on the way back, and nothing marks a positional hole as distinct from a value
    genuinely written as null.

    Seven corpus rows were re-recorded and
    `avro/avro-array-element-multi-branch-union-mixed-branches` moved **DEFECT → LOSSLESS**: its
    residual fault was never union resolution, it was this misalignment, so the corpus counts go
    55/24/82 → **56/24/81**. A third site, `extractFieldsPreservingStructure`, had the same
    defect and is fixed in item 22 below.

14. **`FileFinder`'s `maxFileSize` is enforced against the RESOLVED file and against the bytes
    actually read.** The gate used to call `Paths.get(fileName)` with no base path while resolution
    went through the configured search paths, so it covered only names that happened to resolve as
    a regular file relative to the working directory. A file reached through the classpath, HDFS,
    the deep search or any non-CWD search path was not size-checked at all. Two further holes are
    closed with it: a classpath handle whose content length is unknown reports `-1`, which a
    `size > max` comparison waves through, and `.gz` is an allowed extension that is inflated
    transparently, so the handle's COMPRESSED size was being compared against the cap. The cap is
    now applied twice — once against a known resolved size before anything is opened, and once as
    a hard byte count on the outermost, decompressed stream. **A previously-successful read of an
    oversized file now throws**, and for a compressed file the throw can land mid-read.

15. **`FileFinder` no longer searches parent directories.** `".."`, `"../.."` and `"../../.."`
    were default search paths, and `performDiscovery` — invoked on EVERY miss to build the
    not-found message — applies `Files.walk(path, 2)` to each of them and embeds the results in the
    exception text. Measured from this repository's root, a `.json` miss walked 67 files across
    sibling checkouts and 24 in the user's home directory, and `AvroSchemaFlattener` rewraps that
    message into a `RuntimeException` that Spark logs. On an executor the same walk enters sibling
    containers' scratch space. `discoverFiles`, `discoverAvroSchemas` and
    `FileFinderException.getAvailableFiles()` now return a strictly smaller set — for
    `discoverFiles` that is a change in returned DATA, not only in an error string. Two duplicate
    entries were removed at the same time, so each lookup issues two fewer `Files.exists` calls.
    The traversal `SecurityException` message was rewritten in the same change: it claimed
    FileFinder "searches parent directories by default" (no longer true) and told the caller to
    "disable validatePaths if the input is trusted", which was never possible, since `Config` has
    no setters and no injection point.

16. **`FileFinder.getFileMetadata` and `fileExists` now validate before searching.** Both called
    the cache loader directly, so neither ever reached `enforceSafetyOptions` — the class javadoc
    claiming `getInputStream` was "the single choke point every public accessor funnels through"
    was false, and that claim has been corrected in place. `getFileMetadata` on a traversal name
    now throws `SecurityException` where it previously threw
    `IOException("Failed to get file metadata")` after running the full search. `fileExists` still
    returns `false`, but stops performing the search first; the difference is observable through
    `getStatistics().searchAttempts`.

17. **`AvroSchemaLoader.loadAvroSchema` propagates `SecurityException` instead of silently falling
    back to an unvalidated read.** This closed a live bypass of the 2.0.0 traversal fix. The loader
    wrapped its `FileFinder` call in `catch (Exception e)` with a DEBUG log; `SecurityException` is
    a `RuntimeException`, so the traversal guard fired and was DISCARDED, and control fell through
    to `loadFromLocalFileSystem` — `Paths.get(basePath, schemaName)` plus `Files.readAllBytes`,
    with no traversal check, no extension check and no size cap, over a search-path list beginning
    with `"."`. The guard was proven at the `FileFinder` boundary and enforced nowhere at the
    library boundary. `normalizeSchemaName` appends `.avsc`, which narrowed the reachable target
    set but did not close it. A 2.0.0 caller relying on that fallback to read a `.avsc` from
    outside the working tree will now fail.

18. **`JsonReconstructor` refuses an unparseable bracketed column where the caller has already
    committed to it being an array.** Under `arrayFormat=JSON`, a bracket-wrapped value Jackson
    cannot parse used to be returned as a one-element list holding the raw text — indistinguishable
    from a legitimate one-element array. In `reconstructArray` it was worse: that single unparsed
    element was then REPLICATED into every element of an N-element array by the last-value clamp,
    so one piece of garbage was presented as N successfully parsed values. It now throws
    `ArrayParseException`, naming the flattened key, the configured format and the raw value.

    **The structure-inference probe is explicitly UNCHANGED and still treats such a value as
    "not an array", without throwing and without warning above DEBUG.** That is half the design,
    not an oversight: inference asks this of every value in every prefix group, where "no" is the
    ordinary answer, and making it throw would turn
    `limits/circular-map-reference-is-marked-and-the-guard-is-live` red by rejecting a benign
    `[CIRCULAR_REFERENCE]` marker — converting a working reconstruction into a hard failure. That
    row is the only fixture in the corpus that reaches this path at all, which is also why no
    fixture moved. The probe is implemented as a thin wrapper that CALLS the committed converter
    and catches its refusal, rather than as a second copy of the cascade, so the two cannot drift
    into disagreeing about what an array is.

    `BRACKET_LIST`, `COMMA_SEPARATED` and `PIPE_SEPARATED` are untouched — there the JSON attempt
    is the first leg of a genuine try-this-then-that cascade and a decline is the cascade working.
    Separately, a `ReconstructionException` raised inside `reconstruct` is no longer re-wrapped as
    `"Failed to reconstruct flattened map"`; a typed refusal now survives to the caller.

    This aligns `JsonReconstructor` with the answer `AvroReconstructor` already gives to the same
    misconfiguration. One library should not answer it two different ways.

    **A false claim inside this repair's own error message, corrected before release.** The first
    version of `ArrayParseException`'s text read "MapFlattener's JSON writer always emits
    parseable JSON, so this value did not come from it", and the surrounding comment used that as
    the argument for throwing rather than warning — "a detectable contradiction rather than a
    guess". It is not a contradiction. `MapFlattener` writes at least three bracketed values that
    are not parseable JSON: the literal `[CIRCULAR_REFERENCE]` cycle marker, `stringifyObject`'s
    `toString()` fallback, and its last-resort `[OBJECT:SimpleName]`. A caller who named a column
    holding the cycle marker in `arrayPaths()` would have been told the value "did not come from"
    the class that wrote it, and sent hunting for a misconfigured `arrayFormat` instead of for a
    cycle in their input. The throw is unchanged — replicating a marker into N elements is
    exactly the failure this guard exists to stop — but the message now names those three first
    and makes only the claim that is true: MapFlattener's *array* writer emits parseable JSON.

19. **`AvroSchemaFlattener.getFlattenedSchema(String)` no longer SEARCHES for the schema — it reads
    the path it is given.** Flagged on its own because it is the one repoint in this release with
    no fallback behind it, and because nothing in this repository exercises that overload, so no
    test went red to announce it.

    The parameter is named `schemaPath` and is now treated as one. Previously the call went through
    `FileFinder`, so a BARE NAME like `"product_schema.avsc"` could resolve out of any of roughly
    28 configured directories, the classpath, or a depth-five walk. It now resolves relative to the
    working directory and stops. **A caller passing a bare name and relying on the search will get
    `FileNotFoundException` where they previously got a schema** — pass a path, or resolve the name
    yourself before calling.

    `AvroSchemaLoader` is NOT affected in the same way: it keeps its own search paths and classpath
    fallback at steps 2 and 3, so only the traversal refusal is new there.
    `NexusPiercerSparkPipeline` takes an operator-supplied `config.schemaPath`, which was always a
    path.

    Two defects go with it: the stream at this call site was opened and never closed on the success
    path, leaking a descriptor per distinct cache key, and the not-found message embedded the
    filenames discovery had found — which this method rewraps into a `RuntimeException` that Spark
    logs.

20. **`FileFinder.fileExists` now returns `false` for a name that fails validation, even if the
    file is there.** It validates before consulting the cache, so a disallowed extension, a
    traversal segment or a null byte answers `false` rather than `true`. This is the consistent
    answer — `findFile` refuses those names, so reporting that they exist was an invitation to a
    call that cannot succeed — but it is a changed return value for an existing input.

21. **`AvroSchemaFlattener.getFlattenedSchema(String)` and `NexusPiercerSparkPipeline`'s schema
    read no longer enforce an extension allow-list.** This is the other half of the `SchemaFiles`
    repoint, and it goes the opposite way from item 19: that item made a call FAIL that used to
    succeed; this one makes a call SUCCEED that used to fail.

    `FileFinder` refused any name whose extension fell outside a 13-entry set — `.avsc`, `.json`,
    `.csv`, `.txt`, `.xml`, `.yaml`, `.yml`, `.conf`, `.config`, `.properties`, `.gz`, `.avro`,
    `.parquet`. `SchemaFiles` has no such check, so a valid schema body in a file named
    `schema.exe`, `schema.sh` or `key.pem` is now read where it was previously refused with
    `IOException: Extension '.exe' is not in the allowed set […]`. **`AvroSchemaLoader` is
    unaffected** — its `normalizeSchemaName` appends `.avsc`, so the allow-list was already moot
    there.

    NOT REINSTATED, and here is the reasoning on the record rather than in a commit message. The
    control was already partial: an extensionless name always passed it, deliberately. It guards
    nothing this path can be attacked through — the path comes from the caller, not from the
    file's content, and every byte read still has to parse as an Avro schema before it is used.
    And a deployment that names its schemas `.tmpl` or ships them suffixless has a legitimate
    read that an allow-list turns into an outage. The omission is stated in `SchemaFiles`'
    javadoc under *What it deliberately does NOT enforce* and pinned by a test, so the next
    person to add one has to change both. What `SchemaFiles` does enforce is unchanged from
    item 14's list: null byte, traversal, regular-file, and a 100 MB cap applied twice.

22. **The nested-array flattening site now pads its columns to the outer element count**
    (`MapFlattener.extractFieldsPreservingStructure`; closes [BL-018]). This is the THIRD and
    last array-element site. Items 13 fixed the other two and this one was deliberately left,
    with a recorded reason. Both halves of that reason are now spent: the sequencing half
    ("bundling it would make the seven-row corpus diff impossible to attribute") no longer
    applies to a dedicated pass whose corpus diff is ONE row, and the shape half ("it would place
    a bare null where a nested LIST has always been") was dissolved by choosing the right filler
    rather than by ignoring it.

    Measured before → after, at the shipped default:

    | document | column | 2.0.0 | 2.1.0 |
    |---|---|---|---|
    | `{"g":[[{"a":1}],[{"b":2}]]}` | `g_a` | `[[1]]` | `[[1],[null]]` |
    | | `g_b` | `[[2]]` | `[[null],[2]]` |
    | `{"g":[[{"a":1},{"b":2}],[{"a":3}]]}` | `g_a` | `[[1,null],[3]]` | unchanged |
    | | `g_b` | `[[null,2]]` | `[[null,2],[null]]` |
    | `{"data":[[{"name":"A"}],"text"]}` | `data_name` | `[["A"]]` | `[["A"],null]` |
    | | `data` | `["text"]` | `[[null],"text"]` |
    | `{"grid":{"rows":[[1,2],[],[3]]}}` | `grid_rows` | `[[1,2],[],[3]]` | unchanged (pinned) |

    The first row is the whole defect in one line: `b` came from outer position 1 and sat at index
    0, so a consumer zipping `g_a` and `g_b` by outer index read `a=1` and `b=2` as one nested
    group. They came from different groups. Same silent corruption as item 13, one level deeper.

    **THE FILLER IS SHAPE-AWARE, and the choice is load-bearing.** At the two array-of-maps sites
    a column entry is a scalar, so a hole is a scalar `null`. Here a column entry is an INNER
    LIST, so a hole is an inner list of that outer position's inner cardinality — `[]` exactly
    when the inner array was empty. A bare `null` appears only where the outer position holds no
    nested list at all. Both alternatives were rejected against measurements, not preferences: a
    bare null everywhere changes the slot's TYPE in a column whose entries have always been
    lists, which is precisely what the deferral note objected to; and `[]` everywhere collides
    with the genuinely-empty-inner-array case that
    `structural/array-of-arrays-with-empty-inner` pins as `grid_rows="[[1,2],[],[3]]"`. That
    fixture re-recording would have been the signal the rule was implemented wrong. It did not
    move.

    **A CORRECTION TO THE ANALYSIS THAT ORDERED THIS.** It predicted `data="[null,\"text\"]"` —
    a bare null at outer position 0 of the sentinel column. Shipped is `"[[null],\"text\"]"`,
    because the rule is uniform: at a position that DOES hold a nested list, a missing column is
    an inner list, and the sentinel column is not special. The uniform rule keeps the stronger
    invariant — at every outer position, every column agrees on inner length too — which the
    bare-null form would have broken in the column most likely to be read beside the others.

    **A KEY-SET CHANGE, and the original wording of this paragraph was WRONG about who can reach
    it.** An outer position whose inner list flattens to no columns used to vanish from every
    column: `extractFieldsFromList` returned an empty map, nothing was appended, so the outer
    position disappeared and everything after it shifted left. It now registers its position
    under the base key, which can introduce a base-key column where a document previously had
    none.

    This shipped saying the change was "only reachable from a `Map` source, not from JSON" — an
    empty Java array, `Object[0]` — and `docs/BACKLOG.md` and the pin's own assertion message
    repeated it. **Measured, it is reachable from plain JSON**, because the same branch fires
    whenever an inner list flattens to no columns and an inner list of empty objects does exactly
    that. Four documents with no Java array anywhere, before (0962a56) and after:

    | Document | before | after |
    |---|---|---|
    | `{"g":[[{}]]}` | *no keys at all* | `g="[[null]]"` |
    | `{"g":[[{}],[{"a":1}]]}` | `g_a="[[1]]"` | `g="[[null],[null]]"`, `g_a="[[null],[1]]"` |
    | `{"g":[[{"a":1}],[{}]]}` | `g_a="[[1]]"` | `g="[[null],[null]]"`, `g_a="[[1],[null]]"` |
    | `{"g":[[{"a":null}],[{}]]}` | `g_a="[[null]]"` | `g="[[null],[null]]"`, `g_a="[[null],[null]]"` |

    The `[[null]]` values rather than `[[]]` are item 27, which corrects the second half of this
    same branch. Pinned by
    `MapFlattenerNestedArrayAlignmentTest#theBaseKeyColumnIsReachableFromPlainJson`.

    **A LOSS THIS CREATES, recorded rather than discovered later.** In `COMMA_SEPARATED` and
    `PIPE_SEPARATED` the new positional hole renders as the EMPTY STRING, so `data_name` gains a
    trailing delimiter and a splitter cannot tell a hole from an empty value. That is the same
    ambiguity this class's javadoc already documents for scalars, now reaching nested-array
    columns for the first time. Pinned by `MapFlattenerNestedArrayAlignmentTest#siteCUnderEachArrayFormat`.

    Output SIZE grows here as it did for item 13 — columns × outer positions × inner length.

    Corpus reach was measured by enumerating all 161 fixture inputs: exactly three route through
    this site and exactly one moved.
    `structural/mixed-nested-array-sentinel-collision` is re-recorded and STAYS `DEFECT` — the
    positional half of that row is closed, the sentinel-maps-to-the-base-key half and the
    `setNestedValue` leaf-versus-branch collision are untouched, and its prose now says which is
    which. `structural/array-of-arrays-with-empty-inner` and
    `schema/enriched-gavro-parity-diverges-on-an-array-of-arrays-of-records` were verified
    unchanged rather than assumed. Counts stay 56 / 24 / 81.

    NOT A BEHAVIOUR CHANGE, but shipped with it: the index clamp in
    `AvroReconstructor.reconstructNestedArrayOfRecordsAtIndex` —
    `outerIndex < rawValues.size() ? rawValues.get(outerIndex) : rawValues.get(0)` — is gone, and
    its comment, which read "KEY FIX: Use outerIndex to select the correct element" directly above
    a line that does the opposite out of range, is corrected. **The analysis that ordered this
    called the clamp a live silent duplicator; measured, it is not.** `agreedElementCount` refuses
    a disagreeing column count first: the exact shape described throws
    `ArrayCardinalityException` naming both counts before any index is taken.
    `AvroNestedArrayOuterIndexClampTest` pins that refusal, because the refusal is what makes the
    clamp unreachable — not the clamp's own shape. It is removed as unreachable defence so a
    future ragged producer cannot re-enter it, and nothing observable changes.

23. **`MapFlattener` refuses a document that would emit more than `maxArrayCells` array-element
    cells, instead of returning a very large map.** New knob
    `MapFlattener.Builder.maxArrayCells(int)` (and `JsonFlattener.Builder.maxArrayCells(int)`),
    default `1_048_576`; exceeding it throws the new
    `MapFlattener.FlattenLimitExceededException`, an `IllegalStateException`. Both are additive.

    **The measurement.** A 4,057,897-character WELL-FORMED JSON document — 1000 array elements,
    300 distinct keys each — exhausts a 1 GB heap inside `MapFlattener.columnFor`. Not a
    pathological document; a wide sparse one.

    **THE FILING NAMED THE WRONG AXIS, and the correction is the interesting part.** It said
    "nothing bounds the emitted column count". Quadratic is right; unbounded as stated is wrong.
    Measured: `maxArraySize` bounds the SLOT axis absolutely, and bounds the column axis too
    whenever each element carries exactly one distinct key — 1500, 2000 and 4000 elements all
    plateau at exactly 1000 columns and 5,005,780 characters. The genuinely unbounded quantity is
    the UNION OF DISTINCT KEYS across elements. The filing also said `MapFlattener` "already
    carries `maxArraySize` and `maxDepth`"; it carries FOUR bounds, and the omitted one —
    `maxMapSize` — is the one that actually bites on the second axis, capping keys PER ELEMENT
    but never their union. Per-array ceiling at the shipped defaults: 1000 × 10,000,000 = 1e10
    cells, and nothing at all across sibling arrays.

    Documents that succeed today and are now refused at the default, measured:

    | document | cells | 2.0.0 output | 2.1.0 |
    |---|---:|---:|---|
    | 1000 elements × 20 distinct keys | 20,000,000 | 100,146,690 chars | refused |
    | 400 elements × 100 distinct keys | 16,000,000 | 79,956,000 chars | refused |
    | 50 sibling arrays × 500 sparse elements | 12,500,000 | 62,778,390 chars | refused |
    | 1000 elements × 1000 distinct keys | 1,000,000 | 5,005,780 chars | **unchanged** |

    **The budget is PER INVOCATION, not per array**, and the third row is why: each of those fifty
    arrays is only 250,000 cells, comfortably under any sane per-array budget, and together they
    are 62 MB of output.

    **1,048,576 is anchored, not picked.** The worst shape CHANGELOG item 13 and
    `SparseArrayOfMapsOutputSizeTest` both publish is exactly 1,000,000 cells, so 2^20 sits just
    above it with 48,576 cells of headroom and no knife edge. **Be honest about what this does not
    fix:** the default still permits roughly 391× amplification — 12,787 input characters
    legitimately producing 5,005,780 output characters is UNDER budget by design, because refusing
    it would invalidate a published release note. This bound stops heap exhaustion. It does not
    make the library safe on untrusted input at the default; lower `maxArrayCells` if you accept
    untrusted documents. `SECURITY.md` says so under `perf/NP-028`.

    **It REFUSES rather than truncating, and that is the load-bearing decision.** Dropping columns
    past a budget leaves a flat map whose surviving columns are all still exactly the right
    length, so `ArrayCardinalityException`, `agreedElementCount` and every length assertion in the
    reconstructors stay green while whole fields have vanished. That is bit-for-bit the defect
    class of item 13 — a length invariant satisfied while the data is wrong. The reasoning is in
    the exception's own javadoc so a future maintainer cannot soften it without reading it.

    **WHAT NEW SILENT FAILURE THIS CREATES, and what stops it.** `flatten()` ends in
    `catch (Exception e) { throw new RuntimeException("Failed to flatten map", e); }`, which would
    rewrap the typed exception — so a caller writing `catch (FlattenLimitExceededException)`,
    exactly what the javadoc tells them to write, would catch nothing. Not silent in the log;
    silent to the type system, which is where the guarantee lives. A rethrow arm is placed FIRST
    and `MapFlattenerArrayCellBudgetTest#theTypedExceptionEscapesFlattenUnwrapped` is the only
    thing that can see it. The refusal is logged at WARN with the message and no stack trace,
    because one stack per hostile request is itself an amplification vector.

    **AND WHAT ITEM 22 SILENTLY CREATED.** The analysis that designed this budget excluded the
    nested-array site because it APPENDED, making its cost linear in present values. Item 22
    changed that: it now pre-sizes to the outer element count exactly like the other two, so it is
    quadratic exactly like them — measured, 1000 sparse nested positions emit 6,999,890 characters
    against 4,999,890 for the flat equivalent. A budget enforced only in `columnFor` would have
    left the WIDEST of the three sites unbounded. All three are charged.

    **THE CEILING IS ON TOTAL CELLS REGARDLESS OF SPARSITY, and the examples above are all
    sparse ones.** A dense batch document with no amplification at all is refused on the same
    arithmetic once it crosses the bound. Measured at the default 1,048,576: `50 arrays x 1000
    elements x 20 dense keys` passes at 1,000,000 cells, `60 arrays x 1000 elements x 20 dense
    keys` is refused at `arr52`, `300 arrays x 200 elements x 20 keys` at `arr262`, and `1000
    arrays x 100 elements x 12 keys` at `arr873`. In each of those the cell count equals the
    number of actual values — nothing is being amplified. The `maxArrayCells` javadoc does say
    the budget is "across ALL arrays in the document" and the exception names the knob and says
    to raise it, so the failure is loud and actionable; a batch user should still be able to
    recognise their own shape here rather than in production.

    Corpus: three new `limits/` fixtures, `array-cells-below-max` and `array-cells-exactly-max`
    (LOSSLESS) and `array-cells-one-over-max` (ACCEPTED_LOSS, recording the refusal). Counts move
    **56 / 24 / 81 over 161** to **58 / 25 / 81 over 164**.

24. **`JsonFlattenerConfig.sortKeys` is honoured by the no-argument `toJson()` and
    `toBytes()`.** A caller who set `sortKeys(true)` previously got insertion
    order. Measured for `{"z":1,"a":{"b":null,"c":"x"}}`: before `{"z":1,"a_b":null,"a_c":"x"}`,
    after `{"a_b":null,"a_c":"x","z":1}`. `toJson(OutputOptions)` is unaffected — an explicitly
    passed options object still wins.

    **NOT `toPrettyJson()`, and this entry shipped saying otherwise.** `PRETTY_MAPPER` enables
    `ORDER_MAP_ENTRIES_BY_KEYS` unconditionally at construction, so pretty output was ALREADY
    sorted before 2.1.0 and is byte-identical at both settings — measured, both emit
    `{ "a_b" : null, "a_c" : "x", "z" : 1 }`. The knob has no observable effect on that terminal
    and `sortKeys(false)` does not restore insertion order there. Pinned by
    `JsonFlattenerConfigKnobsTest#sortKeysDoesNotMoveToPrettyJsonBecauseItAlreadySorts`.

25. **`JsonFlattenerConfig.preserveNulls` is honoured by the same terminals, and
    `preserveNulls(false)` now REMOVES null-valued keys.** Measured on the same document: before
    `{"z":1,"a_b":null,"a_c":"x"}`, after `{"z":1,"a_c":"x"}`. **This is the highest-surprise
    change in the release** — it is the only one of the four wired knobs that makes output smaller
    by deleting data, and "a knob that silently did nothing now deletes fields" is a bad sentence
    to have to write. It ships anyway because the only callers who see it are the ones who wrote
    `preserveNulls(false)` and got nulls regardless.

26. **`JsonFlattenerConfig.charset` is honoured by the input overloads that take no explicit
    charset** — `from(InputStream)`, `from(byte[])` and `fromJsonArrayFile(Path)` — and supplies
    the charset of the `OutputOptions` the no-argument terminals synthesise. A caller who set a
    non-UTF-8 charset and used those overloads gets different decoded text: measured,
    ISO-8859-1 bytes for `{"k":"é"}` read back as `é` where they previously arrived as `\uFFFD`.
    `from(InputStream, Charset)` and `from(byte[], Charset)` are unaffected.

    **TWO CORRECTIONS TO THIS ENTRY AS SHIPPED.**

    *It also ENCODES OUTPUT, and that is lossy.* `engineDefaults()` feeds the config charset into
    the `OutputOptions` the no-argument terminals synthesise, and `toBytes()`/`toFile()` write
    through it. Before this release `toBytes()` used `OutputOptions.defaults()`, i.e. UTF-8
    always. So a caller who set ISO-8859-1 meaning only "decode my input that way" also changed
    how output is written, and a character outside that charset becomes `'?'` — measured,
    a two-CJK-character document emits `7b226b223a22e697a5e69cac227d` under UTF-8 and
    `7b226b223a223f3f227d` under ISO-8859-1. To decode input as one charset and write output as
    another, pass an explicit `OutputOptions`. Pinned by
    `JsonFlattenerConfigKnobsTest#theEngineCharsetEncodesOutputAndCanLoseCharacters`.

    *The replacement-character claim above is wrong for the `Path` overload.*
    `batch().fromJsonArrayFile(Path)` goes through `Files.readString(path, charset)`, which
    THROWS on undecodable bytes rather than substituting — measured, a UTF-8 config over
    ISO-8859-1 file bytes raises `JsonFlattenException: Failed to read file`. Substitution is
    what the `InputStream` and `byte[]` overloads did.

    NOT a behaviour change, shipped alongside: `JsonFlattenerConfig.bufferSize` is now honoured on
    `from(InputStream)`, `from(Reader)` and `toFile(..)`. Output is byte-identical either way — it
    is an allocation, not a value — which is exactly why the existing inertness probe was blind to
    it by construction and why it needed a test that watches the read requests instead.

    **WHY WIRING FOUR KNOBS ON RELEASED API IS SAFE.** [BL-015] said "do not simply make them
    live". The general argument is right; the specific one is narrower and survives it. Every
    `JsonFlattenerConfig` default is byte-identical to the per-call default it now feeds — charset
    UTF-8, bufferSize 8192, preserveNulls/includeNulls true, sortKeys false — and an explicitly
    passed `InputOptions`/`OutputOptions` still wins. So the only caller whose behaviour moves is
    the one who explicitly set the knob and previously got nothing.

    **`failOnError` STAYS INERT, and that is a decision rather than an omission.** Its NAME gives
    a direction; its EFFECT is defined nowhere. "Do not fail" on `flattenToMap(String)` could mean
    return an empty map, return null, return a partial map, or log and continue, and nothing picks
    one. A knob whose effect is undetermined cannot be honoured without inventing semantics on
    released API. What DID change is its javadoc, which sent callers to
    `InputOptions.lenient(boolean)` as the live alternative — **measured, that knob is inert too.**

    **THE COUNT IN [BL-015] WAS WRONG: SEVEN KNOBS WERE INERT, NOT FIVE.**
    `InputOptions.isLenient()` and `isSkipInvalid()` are read nowhere in `src/main` either — a
    grep finds them only at their own declarations. Both are now labelled, and pinned by a test so
    the seven is falsifiable rather than a grep result in a report. Removing them is [BL-016] /
    3.0.0, same as `failOnError`.

27. **A nested-array position whose inner elements carry no columns now emits its true inner
    cardinality instead of `[]`.** Item 22 established the rule that at the nested-array site a
    hole is an inner list of that position's inner cardinality, and that `[]` means one specific
    thing: "this position's inner array was EMPTY", distinguishable from "it had elements, none
    of which carried this column". **The implementation did not keep that rule.** The branch
    guarded on `nested.isEmpty()` — the FLATTENED result — so an inner list of N empty objects
    took the empty-array path and recorded an inner size of 0, collapsing four different
    documents into one output:

    | Document | before (b48e177) | after |
    |---|---|---|
    | `{"g":[[],[{"a":1}]]}` | `g_a="[[],[1]]"` | unchanged — the inner array really was empty |
    | `{"g":[[{}],[{"a":1}]]}` | `g_a="[[],[1]]"` | `g_a="[[null],[1]]"` |
    | `{"g":[[{},{}],[{"a":1}]]}` | `g_a="[[],[1]]"` | `g_a="[[null,null],[1]]"` |
    | `{"g":[[{},{},{}],[{"a":1}]]}` | `g_a="[[],[1]]"` | `g_a="[[null,null,null],[1]]"` |

    Byte-identical across all four before; the distinction the class javadoc, item 22 and the
    corpus row `structural/array-of-arrays-with-empty-inner` all say is preserved was being
    destroyed. The inner cardinality is a property of the SOURCE, so it is now read from the
    source list (capped by `maxArraySize`, which is the count the inner extraction would have
    produced columns for). A genuinely empty inner list yields 0 from the same expression, so the
    empty-Java-array repair from item 22 still holds with no second branch.

    **THE PIN THAT WAS SUPPOSED TO RULE THIS OUT COULD NOT FAIL.**
    `MapFlattenerNestedArrayAlignmentTest`'s declared pin says any fix that pads with `[]`
    "without distinguishing" the two facts changes its string. Its input,
    `{"grid":{"rows":[[1,2],[],[3]]}}`, is all scalars and never reaches the map arm — the only
    arm where both facts can occur. It was a scalar-only control for a rule about maps. No corpus
    fixture and no other test covered an empty-object nested element either. The pin now carries
    the map-arm inputs above and fails against the old code.

    **AND THE REPAIR'S OWN NEW GAP, checked before shipping rather than after.** Making those
    positions cost their real cardinality also made them expensive, and nothing charged them: an
    inner list that flattens to no columns creates no inner column, so `columnFor` never runs.
    Measured with the repair and no charge, `{"g":[[1000 distinct-key maps],[{} x1000]]}` is
    13,901 input bytes and 10,012,005 output characters, accepted. `materialiseNestedColumns` now
    charges those hole cells against `maxArrayCells`. **A first attempt charged the whole inner
    axis on the premise that it was uncounted; that premise was drilled and refuted** — it is
    counted, by `columnFor` inside the recursive extraction — and the draft would have
    double-billed every ordinary nested document and halved its ceiling. The control in
    `MapFlattenerArrayCellBudgetTest#aNestedPositionThatFlattensToNoColumnsIsChargedForItsHoles`
    is the assertion that caught it.

    Corpus: no fixture reaches an empty-object nested element, so no row moved and the counts
    stay **58 / 25 / 81 over 164**.

28. **A flattened map holding a key that is also an intermediate path of a longer key is now
    REFUSED instead of being resolved by map iteration order.** `{"a":"2","a_b":"1"}` asks for a
    node at `a` that is simultaneously the string `"2"` and the object `{"b":"1"}`. JSON has no
    such node. `JsonReconstructor.buildHierarchy` iterated `flattenedMap.entrySet()` in the
    caller's order and let whichever key arrived LAST decide, with no exception and no log at any
    level. **The first two rows below are the SAME two entries and produce structurally different
    documents**; the remaining rows are other pairs that lose a value outright in either order.
    All measured against `24dc5a5`:

    | flattened map | before | after (default) |
    |---|---|---|
    | `{"a_b":"1","a":"2"}` | `{"a":"2"}` — the subtree destroyed | `KeyCollisionException` |
    | `{"a":"2","a_b":"1"}` | `{"a":{"_value":"2","b":"1"}}` — a key the source never had | `KeyCollisionException` |
    | `{"a":"scalar","a_\_value":"real"}` | `{"a":{"_value":"real"}}` — the scalar destroyed, and the survivor byte-identical to the fabrication | `KeyCollisionException` |
    | `{"a":null,"a_b":"1"}` | `{"a":{"b":"1"}}`; reversed, `{"a":null}` | `KeyCollisionException` |

    A `HashMap` of the same pair picked an outcome by `String.hashCode`. The corpus already
    contained the proof and nobody had connected it:
    `structural/heterogeneous-array-object-first` and
    `structural/heterogeneous-array-scalar-first` differ only in the element order of the source
    array and recorded structurally different reconstructions.

    **NO COLUMN WAS RENAMED, and that is a decision rather than an omission.** The obvious repair
    — emit the flattener's non-map array element under `data_value` instead of the base key `data`
    — was traced and REFUSED ([BL-022]). `joinEncodedKey` extends an already-encoded path without
    re-escaping it, so a user field literally named `value` produces the segment `value` and the
    key `data_value`; a suffixed sentinel produces the identical string, and the collision moves
    from the reconstruct side, where it is recoverable, to the FLATTEN side, where the bytes are
    gone:

    | document | today, unchanged | under the rename |
    |---|---|---|
    | `{"data":[[{"value":"A"}],"text"]}` | `data_value` and `data`, both kept | one `result.put` overwrites the other; a whole column lost |
    | `{"mixed":[{"value":1},"x"]}` | `mixed_value="[1,null]"`, `mixed="[null,\"x\"]"` | `columnFor` returns the SAME column; `mixed_value="[1,\"x\"]"` |

    A blanket rename would also touch every array-of-arrays-of-scalars —
    `{"grid":{"rows":[[1,2],[],[3]]}}` emits `grid_rows` today, a sentinel-sourced base key with
    **no sibling and no collision** — and would desynchronise the data column name from the SCHEMA
    column name, since `AvroSchemaFlattener` and `GAvroSchemaFlattener` both emit `basePath` for
    array-of-primitives and array-of-arrays. **Old key names: `data`, `grid_rows`, `mixed`,
    `attachments`. New key names: `data`, `grid_rows`, `mixed`, `attachments` — unchanged, every
    one of them.**

    **THE COLLISION IS NOT A SENTINEL PROBLEM, measured.** Decoding every flat key in the 164
    fixtures the corpus held at `24dc5a5` into segment lists and searching for a key whose path is
    a strict prefix of another
    finds **8 rows, of which only 5 involve the sentinel** — re-measured over all **166** fixtures
    with the production decoder after this release's two new rows landed, **10 rows, 6 of them
    sentinel-involved**, one of each kind added. Three of the original eight reproduce it with no
    sentinel
    anywhere: a nullable nested record inside an array of records
    (`{"orders":[{"id":1,"ship":{"city":"NY"}},{"id":2,"ship":null}]}` emitted `orders_ship`
    beside `orders_ship_city` and returned both `ship` fields as `null`, deleting `{"city":"NY"}`
    from an entirely ordinary document); the `LOWER_CASE` dedup suffix (`id` beside `id_2`); and
    a caller-built flat map, since `reconstruct(Map)` is public. One of the eight is already
    `LOSSLESS` on the Avro stack with the same colliding key set, which is what settles that emit
    is not at fault.

    **The fix.** The colliding keys are computed BEFORE any write, as
    `flattenedMap.keySet() ∩ analysis.allPaths` — a set intersection, so the answer cannot depend
    on iteration order. `StructureAnalysis.allPaths` was already populated and read nowhere: the
    detector was built and switched off. Detection compares an ENCODED key against an ENCODED
    intermediate path, so an escaped literal separator is never mistaken for a nesting level —
    `{"a":"1","a\_b":"2"}` still reconstructs to two sibling fields and does not throw.

    **The `_value` wrapper is DELETED, not kept as a fallback.** It fabricated a key the source
    never carried, it did not survive a re-flatten (the node re-encoded to `a_\_value`, not `a`),
    and it silently duelled with a genuine field named `_value`. Keeping it behind the detector
    would have preserved the exact nondeterminism the detector exists to remove. The branch it
    occupied now throws, so a future gap in detection fails loudly instead of inventing a key.

    **Opt out by name, never by accident.** `JsonReconstructor.builder().onKeyCollision(...)`
    takes `FAIL` (default), `PREFER_LEAF` or `PREFER_BRANCH`. Both non-failing members drop the
    same side for every iteration order and log at WARN naming what was discarded, so the worst
    available outcome is now a loud, reproducible loss. `PREFER_LEAF` reproduces the pre-2.1.0
    result for the majority of colliding documents.

    `FAIL` is the default for the reason `ArrayParseException` was added in this same release
    (item 18): the alternatives all discard one side of a collision, a caller who wants that can
    ask for it by name, and a consumer who hits the throw on upgrade is a consumer who was already
    losing data silently.

    **The refusal reaches four other public entry points**, all of which route through
    `reconstruct(Map)`: `reconstructToJson`, `reconstructToPrettyJson`, both `quickReconstruct`
    overloads, and `create().from(...).toMap()`. It also reaches **`verifyRoundTrip`**, which
    flattens, reconstructs and compares — a colliding document now throws there instead of
    returning a verification that lists differences. That is the intended shape: a verification
    reporting "one difference" for a document with an entire subtree deleted was understating it.

    **Not the fix, and rejected explicitly so it is not re-proposed:** merging into a node holding
    both. Any merge must invent a key for the scalar — which IS the `_value` wrapper, which is the
    defect. Sorting the flat map before iterating is worse than every other option: a stable wrong
    answer is harder to notice than an unstable one.

    `AvroReconstructor` is unaffected and was verified so. `PathNode.addPath` sets the leaf value
    and the children side by side, so neither write can clobber the other and its tree is
    order-independent by construction; it still drops one side on a genuine collision, but
    deterministically and without fabricating a key. The three AVRO-stack colliding fixtures did
    not move when the corpus was re-recorded, which is the check that the change did not leak.

    Corpus: **164 → 166**, counts **58 / 25 / 83**. Six rows changed their recorded
    reconstruction (`structural/mixed-nested-array-sentinel-collision`,
    `structural/heterogeneous-array-object-first`, `structural/heterogeneous-array-scalar-first`,
    `real-world/event-envelope-mixed-attachments`,
    `naming/lower-case-collision-suffix-corrupts-structure`, plus the two new rows) and all stay
    `DEFECT`: a refusal is deterministic and loud, and it still does not reproduce the source. The
    repair that would make them `LOSSLESS` — zipping the base column with the field columns by
    outer index — is not implemented. Two rows are new:
    `structural/mixed-array-sentinel-vs-user-value-field`, which pins the flatten-side collisions
    the rename would have created, and `structural/nullable-nested-record-shadowed-json-stack`,
    which pins that the collision reproduces with no sentinel.

    **FOUR PREMISES THIS ENTRY'S BACKLOG ITEM CARRIED WERE FALSE, and are corrected rather than
    inherited.** [BL-022] asserted in the present perfect that the `MapFlattener` class javadoc
    documents the suffixed pair; commit `6bb66d1` — the item-22 padding commit itself — had
    already rewritten that javadoc to describe the measured output. The claim survived in the
    backlog entry and in the fixture's own `rationale` field, and six passes inherited it from
    one of those two places. `SentinelKeyProseMatchesTheCodeTest` now bans the exact sentences by
    substring and measures the real key set in the same class, so prose cannot be edited to match
    a behaviour change after the fact. **The historical tables in this file are exempt by path and
    are NOT rewritten**: they record what was believed and published at the time, and correcting
    them would destroy the evidence rather than the error. The fixture's `cannotCatch` also
    instructed that "the fix must not be to edit the javadoc to match the code" — which `6bb66d1`
    did, and did well; an instruction already violated by a good change is replaced, not obeyed.

    **ONE UNDOCUMENTED SHAPE FOUND WHILE TRACING, now characterised rather than left unwritten.**
    `isNestedList` is false for a `Map`, so a map at an outer position beside a nested list is
    never field-extracted: `{"mixed":[{"a":1},[2,3]]}` emits ONE column `mixed` whose slot 0 is
    the JSON TEXT of the map, and there is no `mixed_a`. No fixture reaches this shape and no test
    asserted it; `MapFlattenerSentinelKeyContractTest` now does. It is also a second, independent
    reason `_value` would misdescribe that column — it is not type-homogeneous.

29. **The three `MapFlattener` naming strategies are pinned to `Locale.ROOT`, so the emitted
    COLUMN NAME stops being a function of the JVM the job runs on.** Before this,
    `applyNamingStrategy` called `toLowerCase()` and `toUpperCase()` with no locale, which uses
    the DEFAULT locale. On a `tr-TR`, `az` or `lt` JVM that is a different case mapping for ASCII
    `i`/`I`, so the same document produced a different key set on a Turkish executor than on an
    American one. Measured on two separately compiled builds under
    `-Duser.language=tr -Duser.country=TR`:

    | strategy and input | `2.0.0` on a tr-TR JVM | now, on every JVM |
    |---|---|---|
    | `LOWER_CASE` `{"ID":1}` | `ıd` — U+0131 U+0064, DOTLESS i | `id` — U+0069 U+0064 |
    | `UPPER_CASE` `{"id":1}` | `İD` — U+0130 U+0044, I WITH DOT ABOVE | `ID` — U+0049 U+0044 |
    | `SNAKE_CASE` `{"userID":1}` | `user_ı_d` | `user_i_d` |
    | `LOWER_CASE` `{"user":{"ID":1,"NAME":"x"}}` | `user_ıd`, `user_name` | `user_id`, `user_name` |

    **This is a column rename in released output, which is why it is here and not under
    *Fixed*.** It is a no-op on every locale whose ASCII case mapping agrees with `ROOT` — which
    is every locale a CI matrix normally runs — and that is exactly what made it survivable for
    two releases and exactly what makes it worth naming: the caller who is affected is the one
    whose Spark executor happens to be started with a Turkish default locale, and their symptom
    is a schema field-not-found rather than an error anyone can trace back to here. A column name
    is a data-format decision; Athena, Spark and Avro all resolve it by exact string.
    `AS_IS` is unchanged and touches nothing.

    **It had no behavioural test and now has one.** Adversarial review reverted the three
    `Locale.ROOT` arguments and ran the whole suite: **2684 tests, 0 failures, BUILD SUCCESS** —
    nothing noticed. The only thing that did was SpotBugs, through `DM_CONVERT_CASE`, which is a
    static-analysis proxy: it fires on any locale-less case conversion anywhere in the class, so
    it pins no key, and any refactor that routes case conversion through a helper silences it
    while the column name goes back to being environment-dependent.
    `MapFlattenerNamingLocaleTest` sets the default locale to `tr-TR`, asserts the emitted keys by
    CODE POINT — the two spellings are visually near-identical — and restores in a `finally`.

    Filed as the second half of [BL-024]. The same hazard survives in
    `converter/SchemaBasedMapConverter`, which lowercases field names at four sites to build a
    case-insensitive lookup; that one is a missed field rather than a renamed column, and it is
    left for its own change rather than folded in here.

### Added

- **`JsonReconstructor.ArrayParseException`**, a nested public class extending
  `ReconstructionException`. Additive; the 2.0.0 baseline gate is unaffected.

- **`JsonReconstructor.KeyCollisionException`**, a nested public class extending
  `ReconstructionException`, with `getCollidingKey()` and `getShadowedKeys()`. Its message
  names the key, every longer key it shadows, and the escaped form that would have
  disambiguated. Additive; the 2.0.0 baseline gate is unaffected.

- **`JsonReconstructor.CollisionPolicy`** (`FAIL`, `PREFER_LEAF`, `PREFER_BRANCH`) and
  **`JsonReconstructor.Builder.onKeyCollision(CollisionPolicy)`**. Additive; the 2.0.0
  baseline gate is unaffected. See behaviour change 28 for why `FAIL` is the default.

- **`io.github.pierce.files.SchemaFiles`** — a static, dependency-free reader for a schema file
  whose path the caller already knows. `open(String)` and `readString(String)` reject a null byte,
  reject a relative escape, resolve the LITERAL path with no searching, stat, size-cap, and return
  a stream capped at the same limit so a file that grew between the stat and the read still cannot
  exceed it. No singleton, no thread pools, no caches, no fuzzy matching. This is the replacement
  named in `FileFinder`'s deprecation notice, and all three of the library's own call sites
  (`AvroSchemaFlattener`, `AvroSchemaLoader`, `NexusPiercerSparkPipeline`) now use it. Additive;
  the 2.0.0 baseline gate is unaffected.

### Deprecated

- **`FileFinder`, `FileFinder.Config`, `FileFinder.Util`, `discoverFiles`, `discoverAvroSchemas`,
  `getStatistics` and `clearCaches`.** `@Deprecated` is additive and clears the baseline gate;
  nothing is removed at 2.1.0. The notice discloses two things a caller reading it needs: a single
  static `findFile` call constructs a singleton whose thread pools nothing ever shuts down (the
  scheduled pool used NON-daemon threads, so a CLI or `spark-submit` driver would finish its work
  and then hang — both pools are daemon as of this release, but there is still no lifecycle and no
  `close()`), and `Config` configures nothing, because every field is private with no accessor and
  the only construction anywhere is inside `getInstance()`. Removal and the rework into an
  instantiable `AutoCloseable` component are filed as [BL-017] for 3.0.0; 64 baseline entries name
  this class or one of its seven nested types (8 TYPE, 4 CTOR, 23 FIELD, 29 METH; 9 declared
  directly on `FileFinder` itself), so none of it can go additively. The figures are asserted
  against `src/test/resources/api/public-api-2.0.0.txt` by `FileFinderBaselineFootprintTest`.

- **`AvroReconstructor.ArrayCardinalityException` and `AvroReconstructor.ArrayFormatMismatchException`**,
  both nested public classes extending `ReconstructionException`, plus a
  `ReconstructionException(String)` constructor. Additive; the 2.0.0 baseline gate is unaffected.

- **`JsonFlattener.Builder.buildFlattener()` and `JsonFlattener.newOperation()`** — the reusable
  engine. Before this, no consumer could obtain a `JsonFlattener` at all: the constructor is
  private and `create()`, both `with(...)` overloads and `Builder.build()` all return
  `FluentOperation`. A caller who configured `builder().maxDepth(64)` therefore had to either
  re-run the whole builder chain per document or reuse one `FluentOperation` across documents —
  and `FluentOperation` holds unsynchronised mutable state, so two threads calling `from(...)`
  on a shared one race on the loaded document. `buildFlattener()` yields the immutable,
  shareable engine; `newOperation()` yields a fresh per-document pipeline.

<!-- snippet: body env=core -->
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

- **`FileFinder` leaked an OS file handle on every CLASSPATH resolution.**
  `createClasspathHandle` called `resource.openConnection()` and then read `getLastModified()`
  and `getContentLengthLong()`, both of which force `connect()` and open an underlying stream.
  `URLConnection` has no `close()`, so the descriptor stayed open — on the HIT path, whether or
  not the caller ever asked for a stream, and handles are cached for 60 minutes. MEASURED on
  Windows with a probe that calls only `getFileMetadata`: `Files.delete` on the resolved file
  fails with "The process cannot access the file because it is being used by another process",
  while a control copy nothing touched deletes fine. Under a jar the leaked connection pinned an
  `Inflater` for the same hour. The 2.1.0 pass fixed the MISS-path leak in `searchClasspath` and
  described the classpath path as handled; this one is larger and was untouched. The connection's
  stream is now taken and closed once the two values are read, and
  `ClasspathHandleReleasesTheFileTest` gates it two ways — the delete on Windows, the
  `/proc/self/fd` count on POSIX — so neither platform runs a test that only ever passes.

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

- **A 26th empty catch that PMD was exempting by variable name.** The audit that took PMD's
  `EmptyCatchBlock` from 25 to 0 cleared PMD's list; a regex over `src/main` finds **26** genuinely
  empty catch blocks at that commit. The delta is `SchemaBasedMapConverter.AvroUnionConverter`,
  written `catch (Exception ignored)` — and `^(ignored|expected)$` is exactly the rule's own escape
  hatch, which the audit recorded as *rejected* at 24 of the 25 sites it did classify. The one site
  already using the hatch was never classified, because the gate never reported it. It carried the
  identical defect the four date/time cascades were repaired for: the terminal named no branch and
  `getCause()` was null. It now holds the first branch failure and throws `Value does not match any
  union branch. Tried: <types>` with that failure as the cause. A caller catching
  `TypeConversionException` and reading `getMessage()` sees a longer message; `getCause()` becomes
  non-null where it was null.

- **A sibling that laundered failures without an empty catch at all.**
  `AvroSchemaConverter.AvroUnionConverter` appended every branch failure to a
  `List<Exception> errors` that nothing ever read, then threw with `getCause() == null` — an empty
  catch that PMD cannot see, because the bin is not empty, it is merely never emptied. The dead
  list is gone and the cause is attached. The message is unchanged; it already named the branches.
  Both sites also narrow `catch (Exception)` to `catch (RuntimeException)`: no branch converter can
  throw a checked exception, so the wider catch could only ever have caught something unreachable.

### Changed

- `JsonFlattener`'s class javadoc no longer claims "thread-safe for concurrent use" without
  qualification. That was true of `JsonFlattener` and false of `FluentOperation`, which was the
  only object the API could hand out. The two halves are now stated separately.

### Documentation

- **[BL-009] is closed: README carries a flattener-family diagram, and it is gated.** Three lanes
  by what each class flattens — DATA, SCHEMA only, and SCHEMA plus per-record casting — with each
  node stating what it takes, what it emits, whether it is the default choice, whether it is
  legacy, and which fidelity stack covers it. `FlattenerFamilyDiagramTest` asserts the name set
  equals the `src/main` Flattener types, that every drawn edge is a dependency the source has
  (word-boundary matched, so `AvroSchemaFlattener` does not match inside `GAvroSchemaFlattener`),
  that each coverage marker agrees with the harness, and that renaming / deleting / adding a node
  each fail.

  **It is NOT in `docs/ARCHITECTURE_GRAPH.md`, deliberately.** That would have inherited
  `ArchitectureGraphEdgesAreRealTest` for free — and that gate builds ONE alias map for the whole
  file, last write wins, so a second diagram reusing an id silently retargets the first diagram's
  edges while the gate keeps passing. A cross-reference goes there instead.

  **A premise in the task did not hold and the diagram says so.** It asked which of "the two
  round-trip stacks" each flattener belongs to. There are FOUR stack keys in the manifest, and the
  letters COLLIDE with the audit register's: the manifest's Stack A is `MapFlattener`, while
  `docs/audit/FINDINGS.md` NP-001 calls `MapFlattener` Stack B. The diagram uses the manifest's
  letters, because the manifest is the published contract, and captions the collision.

- **A documentation sweep, with three of its numbers now under test.** New
  `PublishedProjectFactsMatchTheSourceTest` asserts that `docs/ANTI_REGRESSION.md` publishes the
  ceilings `.github/quality-baseline.json` enforces, that every document stating a suite size
  states the same one, and that `ANTI_REGRESSION` names every gate. All three had drifted:

  - **The ceilings row said `(0 / 323 / 231)`** while the baseline enforced PMD at 322 — the
    document that exists to explain the ratchets publishing a value the ratchet does not use. Both
    were brought to 318 at that pass; PMD is 315 today.
  - **Three documents published three suite sizes** — 2,372, 2,401 and 2,530 — none current. All
    were set to 2,634 at that pass; the figure is 2,689 today and EVERY occurrence of it is now
    gated, not just the first in each file. `CONTRIBUTING.md`'s surefire-XML undercount is
    re-measured too: it is **exactly
    532**, not "roughly 500", and has been 532 at every measurement since 2026-08-17.
  - **The gate inventory named none of the gates.** README sends readers to that document for
    "how the gates and ratchets work" and sixteen gates were absent from it.

  Corrections that could not be gated and were made by hand, each naming the false claim rather
  than overwriting it:

  - `docs/PERFORMANCE.md` published `invokedynamic` as **378 and ratcheted**; `ANTI_REGRESSION`
    published **413 and an observation**, for the same control, in the same tree. 413, observed,
    dated, and marked not re-run against a `src/main` that has grown from ~19,860 to 24,432 lines.
  - `ANTI_REGRESSION` said the **dependency-review** job was "Live, blocking"; `SECURITY.md` had
    already measured it as SKIPPED because the repository's Dependency graph is disabled. Two
    documents in one tree contradicting each other about whether a control runs.
  - `SECURITY.md`'s `quality/NP-001` residual claimed `..`, `../..` and `../../..` "remain in the
    DEFAULT search paths". **They do not** — `FileFinder.Config.searchPaths` has held no parent
    directory since 2.1.0. And the whole `files/NP-027` row was doubly stale: `enforceResolvedSize`
    caps the resolved handle however it was located and `SizeLimitedInputStream` caps the byte
    stream, so both halves of its stated cause are closed. Struck through with the fix named.
  - `docs/CONCERNS.md` said **"No critical concerns identified"** while `SECURITY.md` published a
    `StackOverflowError`, an unkillable heap-exhaustion hang and an unbounded cross-product. The
    most quotable contradiction in the tree; corrected in place with the three named.
  - `docs/CLASS_REGISTRY.md` and `docs/DEPENDENCY_MAP.md` still named **`FileFinder`** as the
    collaborator of `NexusPiercerSparkPipeline`, `AvroSchemaFlattener` and `AvroSchemaLoader`. All
    three import `SchemaFiles`; the first two never name `FileFinder` at all. This is the same
    false edge `ArchitectureGraphEdgesAreRealTest` deleted from the graph a pass ago, still live
    in the registry because no test read it.
  - `docs/MODULE_INDEX.md` listed `FileFinder` at **"~100" lines**. It is 1,462 — a 14x
    understatement — and `SchemaFiles` was missing entirely.
  - `docs/PROJECT_OVERVIEW.md` claimed **"Perfect Reconstruction"** and that `verify()` confirms
    "the reconstructed data matches the original exactly". 83 of 166 fixtures are `DEFECT`, and
    `verify()` compares doubles with a 1e-6 tolerance and treats String and Number as compatible —
    wired to that oracle the corpus reports two money-losing rows as perfect. Its line counts were
    stale by 942 lines on `AvroReconstructor` alone.
  - `docs/audit/FINDINGS.md` now opens with a header declaring it FROZEN at 2026-08-09, naming
    four findings it still publishes present-tense that are long fixed, mapping its
    `src/main/groovy` locations onto the Java tree, and flagging its dead `docs/API_SURFACE.md`
    citations. **The 200 findings are not rewritten** — the register's value is that it is a
    snapshot, and 200 status edits would be stale next pass while a header cannot rot.
  - `README.md`'s Documentation table said "four offline install routes"; `docs/INSTALL.md` has
    four routes TOTAL of which three need no Central, which is what README's own body says six
    paragraphs earlier. `INSTALL.md` was the correct document and was left alone.
  - `README.md` listed `SchemaCacheStats.hitRate()` under Flattening; it is declared on
    `AvroReconstructor` and caches on the reconstruction side. And "reproducible-build
    verification" overstated what CI does — no workflow compares two builds byte for byte, and
    `<Built-By>${user.name}</Built-By>` would defeat it across machines.


- **`OSS-01` is closed, and the fix is a compiler rather than a correction.** Every published
  Java block in every git-tracked markdown file is now compiled on every build by
  `DocumentedJavaSnippetsCompileTest`. Measured before the change: **83** Java blocks in **8**
  files, of which exactly **4** — the fidelity stack recipes — were gated by anything. The other
  79 were prose.

  What the gate found immediately, beyond the six filed phantom calls:

  - `docs/SPARK_PIPELINE.md` line 333 passed a `String` to `withRepartition(int)`, and used the
    Scala property form `spark.conf.get(...)` where Java needs `spark.conf().get(...)`. An
    eighth non-compiling site with nothing to do with the four phantom methods.
  - `README.md` published `NexusPiercerPatterns.generateDataQualityReport(df, "raw_json")` and
    `profileJsonStructure(df, "raw_json")`. Neither compiles: both methods take a `SparkSession`
    and a path. The class javadoc on `NexusPiercerPatterns` published the same non-existent
    two-argument shape. **Both were introduced by `8483b7c`, the commit that added the warning
    banner about the four phantom methods** — and the entry below at *"The class javadoc on
    `NexusPiercerPatterns` … was corrected in 2.1.0"* recorded it as a fix. It was not. A phantom
    method was replaced by a phantom signature. That is the whole argument for the gate.
  - `README.md` called `flattener.flattenAndExplodeJson(json)` on a variable declared in a
    different code block, referenced a phantom `auditColumn()` helper, and published a `catch`
    body written as a literal `...`.
  - `README.md` line 86 named the fifth `JsonFlattenerConsolidator` constructor argument
    `preserveArrayOrder`; the parameter is `consolidateWithMatrixDenotorsInValue`. **That snippet
    compiles**, so no compile gate will ever catch it. Corrected by hand, and recorded here as
    the limit of what the gate proves.
  - `CHANGELOG.md` itself carries a Java block (the `buildFlattener()` example) that no scan had
    ever counted, because its fence is INDENTED and every previous count anchored the
    fence marker at column 0. It is gated now.
  - `docs/audit/FINDINGS.md` opens several fences MID-LINE (`**Evidence.** ` followed by a fence
    on the same line). That is malformed markdown: the opener is invisible to any line-anchored
    scanner, so the eventual closing fence reads as an opener and every block after it shifts. A
    structural parse of that file reports 9 java blocks where it holds 20. The file is exempt
    from compilation, so the count is taken with a permissive scanner and the gate refuses a
    mid-line fence in any non-exempt file.

  The six phantom snippets are **rewritten against the API that exists, not implemented**. Under
  the additive-only rule anything shipped to satisfy that document would be permanent until
  3.0.0, and two of the four cannot be written without inventing semantics: there is no
  parent-child key concept behind `explodeArrays`, and no event-time or state concept anywhere in
  `NexusPiercerSparkPipeline`. Both are filed in `docs/BACKLOG.md` as feature requests so the
  capability is retracted deliberately rather than forgotten. The two that *are* one-liners —
  `jsonToParquet` and `jsonToDelta` — were rejected for a different reason: any signature narrow
  enough to be a convenience must return `void` or a `Dataset`, and both discard the
  `ProcessingResult` that carries `getErrorDataset()`. On a QUARANTINE pipeline the error dataset
  is the product.

  The warning banner is deleted in the same commit that rewrites the snippets, never before: a
  banner is disclosure, and this one was itself wrong about the signatures of the two real
  methods it named.

  **What the gate does not prove.** Type-correctness only. The output tables in the comments, the
  claim that a value will be 2048.5, and every other semantic assertion in these documents are
  invisible to javac. `README.md:86` is the worked example.

- **The escape hatch is counted.** A block that genuinely is not Java may be marked
  `<!-- snippet: pseudo reason="..." -->`; the reason must be at least 30 characters and the
  TOTAL number of pseudo blocks is asserted equal to a recorded constant, so adding one turns the
  build red until a human raises the number in a diff. There are **3**, all in
  `docs/JSON_FLATTENER_CONSOLIDATOR.md`, all DO/DON-T contrasts written with a literal ellipsis.
  Two files are exempt wholesale — `docs/audit/FINDINGS.md`, which quotes code as found, and
  `src/main/java/io/github/pierce/converter/RESEARCH_README.md`, whose thirteen blocks are
  implementation sketches — asserted by SET EQUALITY, not containment.

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
- **The round-trip fidelity corpus moved: 156 → 161 fixtures, `LOSSLESS`/`ACCEPTED_LOSS`/`DEFECT`
  51/23/82 → 55/24/82, and then to 56/24/81** when the array-element alignment fix (item 13) moved
  `avro/avro-array-element-multi-branch-union-mixed-branches` out of `DEFECT`. **56/24/81 is the
  figure this release ships**; the 55/24/82 above is the intermediate state, kept because the two
  steps had different causes. Five fixtures added and three rows reclassified as the defects above
  were repaired; `DEFECT` held at 82 through that first step because the added rows are `LOSSLESS`
  controls and the reclassified rows moved out of `DEFECT` as new ones arrived. Every number in this
  changelog, in `README.md` and in `docs/ROUND_TRIP_FIDELITY.md` is taken from
  `src/test/resources/fidelity/manifest.json`, which is the contract; the document is generated
  from it and the README is now gated against it (see below). The `156` figures elsewhere in this
  file are inside the released **2.0.0** section and are correct as history.

- **The SpotBugs ratchet ceiling drops 236 → 231.** The five points were left on the table by the
  previous commit, which lowered the count and did not touch
  `.github/quality-baseline.json`; measured on both trees with the CI invocation rather than
  inferred. This pass's own contribution was +1 — a `LOG.trace` in `FileFinder.closeQuietly` that
  interpolated `getMessage()` into the format string — and it was paid for rather than banked
  against the headroom, so the final count is 231 and the ceiling is set to it. Checkstyle is
  unmoved at 0. PMD measures 322 against a ceiling of 323 and the ceiling is deliberately NOT
  lowered: the one-violation delta is in the tail, could not be attributed to a named rule, and a
  ratchet moved on an unattributed measurement is bookkeeping. Reasoning in the baseline file.

- **`docs/API_SURFACE.md` is DELETED.** It was hand-written, generated by nothing, read by no
  test, and last touched in December 2025. Four of its claims were already refuted in this
  repository's own audit and were still being published: `<version>1.0.5</version>` against a
  `2.1.0-SNAPSHOT` pom, `DataStreamWriter processStream(...)` against a method whose return type
  is `ProcessingResult`, and three `NexusPiercerPatterns` methods that have never existed
  (`OSS-01`, `OSS-09` and the release-tagging finding in `docs/audit/FINDINGS.md`). The generated
  javadoc and `src/test/resources/api/public-api-2.0.0.txt` — which IS generated and IS gated —
  replace it. Reasoning recorded under [BL-002], which a 2.1.0 review had reported as tracking
  this file; it never did, and that mismatch is corrected in the entry itself.

- **`NexusPiercerPatterns`' class javadoc showed two methods that do not exist.** The example
  called `NexusPiercerPatterns.jsonToDelta(...)` and `NexusPiercerPatterns.kafkaToParquetStream(...)`;
  the class declares exactly two public methods, `generateDataQualityReport` and
  `profileJsonStructure`. A reader following the published example got a compile error. The
  javadoc now documents what the class does. `docs/SPARK_PIPELINE.md` carries six snippets with
  the same defect and is **not** fixed here — it now opens with a warning naming the four phantom
  methods and pointing at `NexusPiercerSparkPipeline`, and `OSS-01` stays open.

- **`docs/ARCHITECTURE_GRAPH.md` is corrected and is now GATED.** Five class-to-class edges were
  false: `AvroSchemaFlattener --> FileFinder` (falsified by this release's own `SchemaFiles`
  repoint and left standing), `GAvroSchemaFlattener --> AvroSchemaFlattener`,
  `CreateSparkStructFromAvroSchema --> AvroSchemaFlattener`,
  `NexusPiercerSparkPipeline --> NexusPiercerFunctions` and
  `AvroSchemaConverter --> TypeConverterRegistry`; fifteen registry rows claimed the converters
  implement `TypeConverter` directly when every one of them extends `AbstractTypeConverter`; and
  `EnrichedSchemaFlattener` and `SchemaFiles` were absent entirely while the classes they replace
  were drawn. `ArchitectureGraphEdgesAreRealTest` now asserts every edge whose two ends are real
  classes, so an edge cannot rot silently again. The header no longer claims the file is
  auto-generated, because it is not.

- **README's flattener table said three of the six "never touch a record". Two do not.**
  `GAvroSchemaFlattener.applyTypes(Map, Map)` is a public per-record data method — its own javadoc
  calls it "the hot path method called for every record in streaming" — and it is the one thing a
  reader choosing between the schema flatteners needs to know about that class. Corrected in
  README and in [BL-008]'s closure note.

- **Five `@deprecated` notices in `FileFinder` were added as a SECOND javadoc comment stacked on
  the existing one, which deletes the original description from the published javadoc.** Javac
  attaches only the last comment before a declaration. Measured against
  `target/apidocs` after `./mvnw javadoc:javadoc`: "Discover files with specific extension",
  "Discover all Avro schema files", "Clear all caches", "Get detailed statistics" and "Utility
  methods for common operations" each returned **0** hits, while un-stacked neighbours returned 2.
  Worse, inserting `ArrayParseException` above `ReconstructionException` in `JsonReconstructor`
  stranded the latter's only sentence, so a public exception type had no documentation at all.
  All seven pairs in `src/main/java` are merged, and `NoStackedJavadocCommentsTest` fails the
  build on a new one.

- The `unwrapUnion` dead-code verdict is corrected wherever it was recorded — see **Known issues**
  below for why the correction matters rather than being cosmetic.

- **`SECURITY.md`'s process section claimed a control that has never run.** It stated that every
  pull request runs "GitHub dependency review (blocking on high-severity advisories in newly-added
  dependencies)". Measured against the most recent pull-request run of `quality.yml`: the
  `actions/dependency-review-action` step is **skipped**, the workflow's own fallback step runs in
  its place, and the job's conclusion is still **success** — a green check on an analysis that did
  not happen. Both `dependency-graph/snapshots` and `dependency-graph/sbom` return 404 for this
  repository, so the Dependency graph is disabled. The workflow was already honest about it in a
  step summary; `SECURITY.md` was not. Corrected, together with the fact that no Actions secrets
  exist, so `release.yml` cannot sign or publish. Both are repository-settings items for the
  owner, not code.

- **A fidelity fixture published a claim its own assert mode cannot measure.**
  `avro-empty-datum-cannot-build-a-record` was titled "both entry points now agree that it is not
  representable", and its `detail` and `cannotCatch` said the same. The row is `assert DATUM`, and
  `FidelityRunner`'s DATUM arm calls only `reconstruct()` — `reconstructToMap` appears in the DATA
  branch and the recipe arm, neither of which a DATUM row executes. `cannotCatch` is the one field
  whose entire job is to be honest about what a row does not measure, so this was the worst
  possible place for it. The row now says what it actually pins — `reconstruct()`'s answer and its
  exact message. Classification and measurement are unchanged; only the published claim moved.

  **And checking where that claim really lived turned up that it lived nowhere.** The corrected
  row pointed at `AvroReconstructorEmptyInputTest`, whose three tests all compare an empty map
  against a one-unrelated-key map — **both through `reconstructToMap`**. So the two-entry-point
  disagreement, which is the whole of `recon/NP-025` and of behaviour change 9 above, was asserted
  in the fixture title, in the fixture's `cannotCatch`, and in this changelog, and tested in none
  of them. `AvroReconstructorEmptyInputTest.reconstructToMapAndReconstructGiveTheSameAnswerForAnEmptyMap`
  is added and asserts that both entry points throw the same class with the same message, naming
  the field. This is the second untested behaviour-change claim found in this pass; the other was
  behaviour change 5.

- **`FidelityRunner`'s "HONEST LIMIT" comment on the Avro DATA defaults arm was false in both of
  its factual claims.** It said no DATA fixture sets `avro.reconstructor` and that the comparison
  "cannot presently fail on any of the eleven DATA rows". Measured: there are **sixteen** DATA
  rows and **two** of them tune the reconstructor, one of which
  (`avro-array-of-records-pipe-format-comma-inside-element`) records `avroDefaultsMatch: false`.
  The arm was published as a dormant tripwire while it was carrying present-tense evidence.

- **A drift guard now covers `README.md`'s fidelity counts.** `RoundTripFidelityDocTest` guarded
  `docs/ROUND_TRIP_FIDELITY.md`, which is generated from the manifest and therefore the one file
  that could not go stale by hand; `README.md` hand-carries the same four numbers, is the first
  thing a prospective consumer reads, and had no gate at all. `ReadmeFidelityCountsTest` asserts
  the front-page total and the three classification rows against `manifest.json`, checks they sum,
  and drills its own anchors so a reworded headline fails rather than silently matching nothing.

- **The `TRIED_FORMATS` javadoc paragraph was pasted verbatim into four converters and was false
  in three of them.** It claimed "five (or three) branches ... two of them consult config".
  Measured: `DateConverter` has five branches and exactly ONE consults config;
  `TimeConverter` has three and NONE do; the two timestamp converters have five each and reach
  config only indirectly, through the shared local-datetime helper. Each class now states its own
  branch count and its own config dependency; only the part that is true everywhere is shared.

### Known issues

Confirmed present in the released **2.0.0**. Recorded here so that a consumer reading release
notes on upgrade learns of them without opening the backlog. Full detail in
[docs/BACKLOG.md](docs/BACKLOG.md); consumer-facing rows in [SECURITY.md](SECURITY.md).

**This block was wrong and is corrected.** It was headed "still open on `main`" and published four
defects as open that this same release repairs — `recon/NP-023`, `recon/NP-024`, `recon/NP-025`
and `BL-014` — contradicting the *Behaviour changes* section a hundred lines above it,
`SECURITY.md`, `docs/BACKLOG.md` and the corpus manifest, all of which had been updated. The
NP-023 row was the damaging one: it told an upgrading consumer to set `useSchemaDefaults(false)`
as a workaround for a defect that is gone and whose behaviour *at that setting* also changed in
this release. Struck-through rows are kept rather than deleted so that a reader who followed the
old advice can find out what happened to it.

#### Repaired in 2.1.0 — do not act on these

- ~~**`AvroReconstructor.reconstruct()` produces an unwritable datum at the shipped default
  configuration** (`recon/NP-023`, BL-012), workaround `useSchemaDefaults(false)`.~~ **Fixed in
  2.1.0**; see behaviour change 1. The workaround is no longer needed and is no longer inert:
  `useSchemaDefaults(false)` now genuinely suppresses a default on a non-nullable field, so a
  caller who set it as a workaround gets a *different* answer than they did in 2.0.0 (behaviour
  change 8). The `reconstruct()` datum is still unwritable for a record with **nesting** — that
  is the separate `avro-generic-record-unwritable` limit, still open and listed below.
- ~~**`allowMissingFields` does not allow missing fields at either value** (`recon/NP-024`).~~
  **Fixed in 2.1.0**; see behaviour change 7. The flag still does not reach `handleMissingField`
  inside an array element — that residue is listed below as open.
- ~~**An empty flattened map silently returns `{}`** (`recon/NP-025`).~~ **Fixed in 2.1.0**; see
  behaviour change 9.
- ~~**3+ branch unions inside Avro array elements are silently dropped** (BL-014).~~ **Fixed in
  2.1.0**; see behaviour change 6. The framing correction stands and is worth keeping: this was a
  gap that had **always** been present, not a regression — the `unwrapUnion` method it was
  originally attributed to never had a declaration in any revision where it was called, so it
  never executed and no behaviour was lost when it was deleted.

#### Genuinely still open on `main`

- **`AvroReconstructor.reconstruct()` returns an unwritable datum whenever the record has
  nesting** (`avro-generic-record-unwritable`). `mapToGenericRecord` rebuilds only the ROOT
  record, so nested records and array elements come back as `LinkedHashMap` and
  `GenericData.get().validate(schema, record)` returns `false`. This is the residue of NP-023
  above and is pinned by assertion in both `AvroReconstructorDefaultValueTypeTest` and
  `AvroReconstructorArrayElementUnionTest` so that "NP-023 fixed" cannot be read as "the datum
  writes". Use `reconstructToMap` unless you need a writable record.
- **A RECORD-typed schema default is repaired leaf-by-leaf in `reconstructToMap` only.** Through
  `reconstruct()` it is still a `LinkedHashMap`; same cause as the row above.
- **`allowMissingFields` does not reach `handleMissingField` inside an array element**, which
  fills `""` and `0` for a missing required field one level down regardless of the setting. It
  logs a WARN now, so it is audible; gating it would turn array-of-records reconstructions that
  succeed today into throws.
- **A defaulted LOGICAL-TYPE field arrives as its raw underlying type** (a `ByteBuffer` for a
  decimal) while the same field supplied in the input arrives converted. A new, smaller
  inconsistency created by the NP-023 repair and disclosed rather than left to be discovered.
- **`failOnError` is inert, deliberately and permanently at 2.x** (BL-015). Four of the five
  knobs this bullet used to name were wired up in 2.1.0 — see items 24-26. This one was not: its
  effect is UNDEFINED rather than unimplemented, so honouring it would mean inventing semantics
  on released API. `failOnError(false)` does **not** make parsing lenient and never will at 2.x.
  **`InputOptions.lenient` and `InputOptions.skipInvalid` are inert too** — the count in BL-015
  was five and is measured at seven. All three are pinned by tests and filed for 3.0.0 removal
  under BL-016.
- **`FileFinder` carried a false compensating-control claim** (`files/NP-027`). A catch block
  asserted that "size is checked on open instead" and there is no size check on open anywhere in
  the file. The comment is corrected in place; the missing check itself is not added here.
- **A sparse array of maps emits `columns × elements` cells, unconditionally, and the AMPLIFICATION
  is still there even though the CEILING is not.** The resource cost of item 13's alignment fix:
  N elements with N distinct keys produce N columns of N slots, measured at 4,999,890 value
  characters for N=1000 against 4,890 before, a 1022× growth. Item 23 caps the total at
  `maxArrayCells`, so heap exhaustion is closed. The 391× amplification UNDER that cap is not,
  and it is under the cap by deliberate choice, because refusing it would invalidate item 13's
  published figure. A job sized against 2.0.0 output can still run out of memory on the same
  input at the shipped default. Lower `maxArrayCells` if you accept untrusted documents; the
  shape stays pinned by `SparseArrayOfMapsOutputSizeTest`.

- **Two repository-owner items that cannot be fixed in code.** The GitHub **Dependency graph** is
  disabled for this repository, so the `dependency review` job has never actually run — it
  reports success without analysing anything. And no **Actions secrets** are configured, so
  `release.yml` cannot sign or publish. Both need a change in repository settings by the owner.

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
