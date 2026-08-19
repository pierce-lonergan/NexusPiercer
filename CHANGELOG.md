# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Version on `main` is `2.1.0-SNAPSHOT`. `2.0.0` is released and staged on Maven Central and `main`
must not sit on a released coordinate.

The public API surface is additive-only and enforced by
`PublicApiIsAdditiveOnlySinceReleaseTest` against a baseline in `src/test/resources`. **OUTPUT
BEHAVIOUR IS NOT.** The section immediately below lists **twenty-one** places where a `2.0.0`
caller gets a different answer, several of them at the default configuration. Read it before
upgrading.

**Eight of the twenty-one turn a previously-successful call into a throw**, across seven items:
a bracketed JSON array read under a delimited format, and its mirror, an unbracketed delimited
column read under the JSON default (item 3, two cases); disagreeing column counts (item 4); an
oversized read, which for a compressed file can throw MID-READ (item 14); a bare name that only
resolved through a parent-directory search path (item 15); a `.avsc` outside the working tree
that `AvroSchemaLoader` used to reach through an unvalidated fallback (item 17); a bracketed
column a caller has named in `arrayPaths()` that is not parseable JSON (item 18); and a bare
schema name passed to `AvroSchemaFlattener.getFlattenedSchema(String)` (item 19).

Items 3 and 6 each carry a sub-entry recording a defect that the first version of this release's
own repair INTRODUCED and that was caught in adversarial review before release, and item 18
carries one recording a false claim its own error message made about `MapFlattener`. All three
are stated in full rather than quietly folded into the surrounding text, because a repair that
creates the fault it removes is the single most useful thing this changelog can tell a reader.

### Behaviour changes

**These change what the library returns at the SHIPPED DEFAULT configuration.** Items 1–12 are
`AvroReconstructor`; items 13 and 21 are `MapFlattener` and the flattener's schema read; items
14–16 and 20 are `FileFinder`; item 17 is `AvroSchemaLoader`; item 18 is `JsonReconstructor`;
item 19 is `AvroSchemaFlattener`. They are listed here rather than under *Fixed* because a caller
pinning a snapshot of today's output will see a diff, and because eight previously-successful
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
    55/24/82 → **56/24/81**. A third site, `extractFieldsPreservingStructure`, has the same defect
    and is deliberately NOT fixed here — see *Known issues*.

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

### Added

- **`JsonReconstructor.ArrayParseException`**, a nested public class extending
  `ReconstructionException`. Additive; the 2.0.0 baseline gate is unaffected.

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
- **Five `JsonFlattenerConfig` knobs are inert** (BL-015): `charset`, `bufferSize`, `failOnError`,
  `preserveNulls` and `sortKeys` are read nowhere in `src/main`. `failOnError(false)` in
  particular does **not** make parsing lenient. They are documented rather than repaired because
  making them live would silently change behaviour for released callers; they are now pinned as
  inert by a test so that wiring one up cannot happen unnoticed.
- **`FileFinder` carried a false compensating-control claim** (`files/NP-027`). A catch block
  asserted that "size is checked on open instead" and there is no size check on open anywhere in
  the file. The comment is corrected in place; the missing check itself is not added here.
- **A sparse array of maps now emits `columns × elements` cells, unconditionally** — the resource
  cost of item 13's alignment fix. Nothing bounds the column count of a sparse array: N elements
  with N distinct keys produce N columns of N slots, measured at 4,999,890 value characters for
  N=1000 against 4,890 before, a 1022× growth. The correctness win is not in question — values
  were landing under the wrong element — but a job sized against 2.0.0 output can run out of
  memory on the same input. A cap on the column count, or an opt-out that trades alignment back
  for the old shape, is deferred; the shape is pinned by `SparseArrayOfMapsOutputSizeTest` so any
  future bound has a measured baseline to move.

- **`extractFieldsPreservingStructure` still has the array-element misalignment** the other two
  sites had ([BL-018]). Deliberately not fixed with them: it would place a bare `null` where a
  nested `LIST` has always been, a shape no reconstructor has been exercised against, and
  bundling it would have made the seven-row corpus diff impossible to attribute.

- **`docs/SPARK_PIPELINE.md` documents four `NexusPiercerPatterns` methods that do not exist**
  (`OSS-01`): `jsonToParquet`, `jsonToDelta`, `jsonToNormalizedTables` and `processIncremental`,
  across six snippets. The document now opens with a warning naming them; the snippets themselves
  are unchanged, because rewriting them needs a decision about whether to implement the recipes
  or drop the sections.

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
