<!-- GENERATED FILE - DO NOT EDIT BY HAND. -->
<!-- Source of truth: src/test/resources/fidelity/manifest.json -->
<!-- Regenerate with FidelityDocGenerator; see the last section. -->
<!-- RoundTripFidelityDocTest fails the build when this file and the manifest disagree. -->

# NexusPiercer round-trip fidelity guarantee

**What a document loses, and does not lose, when NexusPiercer flattens and reconstructs it.**

Measured, not asserted. Every number and every row below is generated from the corpus manifest
recorded on 2026-08-10, and the corpus is executed on every build.

| | count |
| --- | ---: |
| documents in the corpus | 108 |
| reproduce the source exactly (`LOSSLESS`) | 32 |
| lose something, by accepted design (`ACCEPTED_LOSS`) | 10 |
| lose something, wrongly (`DEFECT`) | 66 |
| measured under non-default configuration | 32 |
| `LOSSLESS` rows that do **not** hold through the default reconstruction entry point | 3 |

**10 + 66 = 76 of 108 documents do not survive a round trip.** That ratio is the headline fact about
this library. It is high because the corpus was built adversarially - it hunts for the shapes that
break rather than sampling shapes at random - but every one of those rows is a shape real data has.

## 1. Read this before you depend on the library

Stated without softening, because the point of the list is that you meet these here rather than in
production. Each item names the fixtures that hold it in place; if the behaviour is ever repaired,
those fixtures fail and the item has to be withdrawn from this page.

**1. An array of objects does not survive the round trip.**

This is the most common shape in real data - order line items, event attachments, CDC row images - and under default configuration it comes back wrong. Elements are duplicated, elements are deleted, or the array is rebuilt as an object keyed by index. A two-element array of objects at the document root does round-trip, and so does any array whose path is named explicitly to JsonReconstructor.arrayPaths(...) with inferArraysFromValues(false); nothing else in this corpus does. Treat arrayPaths as mandatory, not optional.

<sub>Pinned by: `nested-array-of-objects-default`, `array-element-with-nested-object-duplicated`, `array-of-objects-single-element`, `order-single-line-item`, `order-line-item-nested-address`, `order-optional-discount-absent`, `line-item-tag-arrays`, `cdc-update-with-array-column`, `customer-preferences-object-with-array`</sub>

**2. An array of scalars turns its parent object into an array.**

A list of strings one level below the root does not come back as a list of strings inside an object: the containing object itself is reconstructed as a JSON array. At the document root an array of scalars never reconstructs at all.

<sub>Pinned by: `nested-array-of-scalars`, `order-top-level-tags-array`, `top-level-array-of-scalars`, `array-of-arrays-with-empty-inner`</sub>

**3. An empty object, an empty array and an explicit null are the same value afterwards.**

{} and [] and null all flatten to null and all reconstruct as null. Three distinct authored values arrive as one. If your schema distinguishes "no items" from "field not set", this library cannot carry that distinction. The nesting DEPTH of an empty chain is preserved, which means you get keys back with no leaves under them.

<sub>Pinned by: `vd-empty-containers-end-to-end`, `empty-containers-at-depth`, `empty-cart`, `empty-chains-yield-keys-with-zero-leaves`</sub>

**4. A number that overflows a double becomes the text "Infinity".**

A finite JSON number too large for a double is emitted as the JSON string "Infinity", not as a number and not as an error. A number too small underflows to 0.0. NaN and Infinity collapse to their string forms. A consumer parsing the output sees a string where the source had a number, and no exception is raised anywhere on the path.

<sub>Pinned by: `vd-exponent-overflow-and-underflow`</sub>

**5. An integer past Long.MAX_VALUE stops being a number and becomes a quoted string.**

The digits survive exactly - nothing is truncated and no value wraps negative, which an earlier version of this library did do - but the JSON type changes from number to string. Anything downstream that type-checks before parsing will reject it.

<sub>Pinned by: `vd-integer-past-long`, `financial-transaction-huge-minor-units`</sub>

**6. Declared decimal scale is destroyed, and the flag that claims to prevent that cannot fire on the JSON stack.**

37.7740 comes back as 37.774; an 18-digit FX rate is rounded to a double. preserveBigDecimalPrecision keeps exact text when you hand MapFlattener a real BigDecimal, but the JSON stack parses fractional numbers to Double before the flattener ever runs, so on that stack the flag is inert. Do not carry money through this library as a JSON number.

<sub>Pinned by: `vd-decimal-scale-and-precision`, `financial-transaction-high-precision`, `vd-preserve-bigdecimal-flag-inert`, `avro-decimal-bytes-scale-mismatch`</sub>

**7. Field names made of underscores collide with the library's internal markers and are silently deleted.**

A field named ___ is dropped outright - no exception, no warning, no null placeholder (NP-022). The whole __*__ namespace is swallowed, including entire subtrees beneath it, which takes out the __meta__ and __type__ headers that event envelopes routinely carry. A field named __isArray__ converts its parent object into a JSON array. An empty-string key is renamed to empty_key and overwrites any real sibling of that name.

<sub>Pinned by: `triple-underscore-key-dropped`, `double-underscore-namespace-swallowed`, `reserved-marker-key-dropped`, `is-array-field-name-changes-json-type`, `empty-string-key-renamed-into-collision`, `event-envelope-sentinel-metadata-keys`, `boundary-separator-on-double-underscore-field`</sub>

**8. A string whose text merely looks like a JSON array is turned into an array.**

The value domain is not injective: a genuine array and a scalar string containing that array's text flatten to byte-identical output, so at most one of the two can ever round-trip. The misdetection does not only mis-type the value, it fabricates padding elements that were never in the input.

<sub>Pinned by: `vd-string-that-looks-like-array`, `vd-array-lookalike-fabricates-padding`, `vd-real-array-collides-with-array-text`</sub>

**9. ArraySerializationFormat.COMMA_SEPARATED invents arrays out of ordinary strings.**

A comma anywhere inside a scalar string - a product description, an address - makes that string reconstruct as an array. Element types are coerced, and an empty or trailing-null element is lost entirely rather than merely retyped. This format is not safe for free text.

<sub>Pinned by: `vd-comma-format-scalar-with-comma`, `vd-comma-format-atomic-coercion`, `vd-comma-format-empty-and-trailing-null`, `order-line-items-comma-separated`</sub>

**10. The naming strategies restructure the document rather than only renaming fields.**

SNAKE_CASE injects unescaped separator characters, which turns a flat scalar field into a nested object on the way back. The case-collision disambiguator emits an unescaped _2 that the decoder reads as another level of nesting. The same field name is sanitised differently at the root than deeper down, so one name yields two column names. UPPER_CASE is the one strategy that renames without damaging the encoding.

<sub>Pinned by: `snake-case-injects-unescaped-separators`, `lower-case-collision-suffix-corrupts-structure`, `top-level-double-sanitization-asymmetry`</sub>

**11. maxDepth, maxArraySize and maxMapSize discard data with no marker, no warning and no exception.**

Past maxDepth the subtree is replaced by its JSON text, which is indistinguishable from a user string that happens to contain that text - the corpus pins that collision as a pair invariant. Past maxArraySize the extra elements are gone. Past maxMapSize the trailing fields are gone. maxArraySize=1 does not shorten the array, it changes the reconstructed type from array to object, so a resource bound silently rewrites your schema. maxDepth(0) is the one bound that fails closed, at configuration time.

<sub>Pinned by: `depth-one-over-max`, `array-size-one-over-max`, `map-size-limit-truncates-fields-silently`, `flattener-maxdepth-truncation`, `array-size-max-one-collapses-the-type`, `depth-bound-map-vs-list-asymmetry`, `depth-bound-bypassed-by-empty-container`, `sparse-array-of-maps-padding-misaligns-elements`, `single-element-array-at-the-arity-lower-bound`</sub>

**12. preserveNulls(false) erases present nulls, and a ragged array turns an absent field into a present null.**

Absent-key versus present-null is preserved by default and is one of the guarantees this corpus does make - but turning preserveNulls off voids it silently, and inside an array of objects a field missing from one element comes back as an explicit null on that element.

<sub>Pinned by: `vd-preserve-nulls-false-erases-present-null`, `ragged-array-of-objects-absent-vs-null`</sub>

**13. A mixed-type array loses elements or leaks an internal sentinel into your data.**

With objects before the scalars the object elements are silently deleted. With scalars first the flattener's internal marker appears in the reconstructed document as if it were a value.

<sub>Pinned by: `heterogeneous-array-object-first`, `heterogeneous-array-scalar-first`, `mixed-nested-array-sentinel-collision`, `event-envelope-mixed-attachments`</sub>

**14. An Avro union takes the first branch that will accept the value and coerces to it.**

A string branch listed before int turns every value in that field into text. [long, double] truncates a fractional value to an integer. For a union of records, the first branch sharing any field name wins and the remaining data is dropped. A nullable string whose value is the literal text "null" comes back as an actual null.

<sub>Pinned by: `avro-union-string-branch-swallows-int`, `avro-union-long-before-double-truncates`, `avro-union-of-records-overlapping-fields`, `avro-nullable-string-literal-null-collapses`</sub>

**15. One null element destroys an entire Avro array of records.**

A single null inside an array of records annihilates the whole array, and one null nested record erases the nested records of every other element in that array.

<sub>Pinned by: `avro-array-of-records-null-element-annihilates-array`, `avro-array-of-records-nullable-nested-record-shadowed`</sub>

**16. Avro schema flattening erases logical types, field defaults, and enum and fixed identity.**

Every logical type on a field inside an array becomes an untyped STRING column. Field defaults are gone from the reconstructed schema. A fixed-backed decimal loses precision and scale and arrives as uninterpretable bytes. reconstructOriginalSchema never reads its flattenedSchema argument at all - it replays definitions stored during the forward pass, so it will reproduce an original schema even when the flattened schema has thrown the information away. The flattened SCHEMA and the flattened ROWS disagree on column names for any field containing an underscore.

<sub>Pinned by: `avro-logical-types-erased-inside-arrays`, `avro-record-defaults-dropped-in-reconstructed-schema`, `avro-enum-accepted-fixed-decimal-erased`, `avro-bytes-b64-sentinel-and-fixed-charset`, `avro-reconstruct-original-schema-ignores-argument`, `avro-underscore-field-name-column-divergence`, `avro-array-statistics-suffix-silent-overwrite`, `avro-flattened-name-collision-guard-fires`</sub>

**17. Two controls do not do what their names say.**

useArrayBoundarySeparator marks no array boundaries; it is a global rename of the separator from _ to __, and switching it on collapses __ inside field names and manufactures collisions that did not exist. strictKeyValidation performs no validation and rejects nothing; it silently rewrites names, and three distinct names can fold into one column.

<sub>Pinned by: `array-boundary-separator-manufactures-a-collision`, `strict-key-validation-folds-three-names-into-one`</sub>

**18. Two settings are wired, documented, readable - and cannot change the output.** *(a setting that does nothing, not a data loss)*

JsonReconstructor.maxDepth is a dead control: maxDepth(1) and maxDepth(1000) produce byte-identical results, so it cannot fail closed on a deep document. preserveBigDecimalPrecision cannot fire on the JSON stack. Each of these is pinned by a probe that asserts the two configurations produce IDENTICAL output - the day either control is wired up, that probe fails and this warning must be withdrawn.

<sub>Pinned by: `reconstructor-maxdepth-is-a-dead-control`, `vd-preserve-bigdecimal-flag-inert`</sub>

## 2. What round-trip fidelity means here

THIS FILE IS THE CONTRACT. For every fixture below it states what a JSON or Avro document loses, or does not lose, when it is flattened and reconstructed. A consumer can read a line here and rely on it.

The repository cannot change any of these behaviours quietly. Each fixture file carries the exact recorded rendering of its flattened intermediate and its reconstructed document, and RoundTripFidelityCorpusTest asserts both. Changing behaviour - including FIXING a defect - makes a recording stale and turns the build red until someone updates this manifest on purpose. That is the intended cost: the published guarantee may not drift by accident.

The oracle is strict recursive equality with runtime types attached, not JsonReconstructor verify(), which treats String and Number as compatible and compares doubles with a 1e-6 tolerance and would report several of these fixtures as PERFECT.

Concretely, two documents count as equal only when every scalar has the same runtime type, every
list has the same length and order, every key set matches, and an absent key is distinct from a
present null. `1`, `1L` and `"1"` are three different values; `-0.0` is distinct from `0.0`;
`37.7740` is distinct from `37.774`.

## 3. The stacks this applies to

A fixture declared `BOTH` is measured on Stack A and Stack B independently and must satisfy both.

### Stack A - Map level  <sub>`MAP`</sub>

```java
MapFlattener f = MapFlattener.builder().build();
Map<String, Object> flat = f.flatten(sourceMap);
Map<String, Object> back = JsonReconstructor.quickReconstruct(flat);
// lossless means back.equals(sourceMap)
```

The source is an in-memory Java Map. Anything a JSON parser would have destroyed before flatten() was called is already absent from the source AND from the result, so this stack is structurally blind to parse-time loss. Fixtures that measure parse-time loss are declared JSON-only for that reason.

### Stack B - JSON level  <sub>`JSON`</sub>

```java
JsonFlattener jf = JsonFlattener.builder().build();
Map<String, Object> flat = jf.flattenToMap(jsonString);
String back = JsonReconstructor.builder().build().reconstructToJson(flat);
// lossless means back is semantically equal to jsonString
```

The source is JSON text. Jackson parses it before the flattener runs, so this stack sees parse-time loss - declared decimal scale, exponent overflow - that Stack A cannot see. The corpus compares the two documents with BigDecimal-exact parsing on both sides: comparing the raw text would fail on 1e2 versus 100.0, and comparing with a default mapper would collapse both sides to the same Double and hide the money loss.

### Stack C - Avro  <sub>`AVRO`</sub>

```java
AvroSchemaFlattener sf = new AvroSchemaFlattener(false, true);
Schema flatSchema = sf.getFlattenedSchema(schema);
Map<String, Object> flat = MapFlattener.builder().build().flatten(datum);
Map<String, Object> back = AvroReconstructor.builder().build().reconstructToMap(flat, schema);
```

Two separate things are measured and they do not agree with each other: the DATA path (flatten a datum, reconstruct it against the writer schema) and the SCHEMA path (flatten a schema, then ask reconstructOriginalSchema to invert it). The schema inverse replays definitions captured during the forward pass rather than reading the flattened schema, so it reproduces the original even when the flattened schema has discarded the information. The corpus therefore never accepts the inverse alone as a verdict; it conjoins it with direct checks on what the flattened schema still carries.

## 4. What the three classifications mean

### `LOSSLESS` - 32 documents

The round trip reproduces the source exactly under the corpus oracle: identical runtime types for every scalar, identical list lengths and order, identical key sets, and absent-vs-present-null preserved. The harness asserts equality against the source AND against the recorded rendering.

> **Repair status.** Not a repair category. These rows are the guarantee. They may not silently get worse; a change here fails the build.

### `ACCEPTED_LOSS` - 10 documents

The round trip does NOT reproduce the source, the loss is understood, bounded, and judged the right trade. The harness asserts the loss is still present AND that its exact shape matches the recording, so the deal cannot silently get worse - or silently get better without someone updating this file.

> **Repair status.** ACCEPTED DESIGN LIMIT. Not scheduled for repair. The loss is understood and bounded, and the corpus pins its exact shape so it cannot silently widen. If one of these is unacceptable for your data, the answer is not to wait for a fix - it is to avoid the shape or configure around it.

### `DEFECT` - 66 documents

The round trip does NOT reproduce the source and the loss is wrong. The harness asserts the exact wrong output. Fixing the defect makes the recorded output stale and the test FAILS - deliberately. A fix must land together with a manifest update, because the guarantee published to consumers changes at that moment.

> **Repair status.** UNDER ACTIVE REPAIR. The behaviour is wrong, not merely limited. The corpus records the exact wrong output that comes out today, so the day a defect is repaired the recording goes stale and the build turns red on purpose - a fix has to land together with a rewrite of this contract, because what is promised to consumers changes at that moment. Do not read a DEFECT row as a fixed date; read it as a hazard that is currently live.

## 5. Which rows are only true under a non-default configuration

Every row in the table below was measured under a stated configuration. 32 of 108 rows
turn some knob away from its default, and the `config` column says which.

The sharper question is whether a row still describes what happens through the library's *default*
reconstruction entry point - `JsonReconstructor.quickReconstruct`, `quickReconstructToJson`, or a
default-built `AvroReconstructor`. The harness reconstructs a second time that way and compares, and the
`defaults` column publishes the answer. `NOT_APPLICABLE` means the row reconstructs no data at all -
the Avro schema-only rows.

**9 rows behave differently through the default entry point, 3 of them `LOSSLESS` ones.**
If you use the defaults, these rows do not describe what you will get:

| fixture | classification | required configuration |
| --- | --- | --- |
| `nested-array-of-objects-explicit-hints` | `LOSSLESS` | JsonReconstructor.builder().inferArraysFromValues(false).arrayPaths("order_items") |
| `boundary-separator-on-round-trip` | `LOSSLESS` | MapFlattener(true, 50, 1000); JsonReconstructor.builder().separator("__") |
| `vd-preserve-nulls-false-erases-present-null` | `ACCEPTED_LOSS` | JsonReconstructor.builder().preserveNulls(false); probe asserts the default run DIFFERS |
| `vd-comma-format-scalar-with-comma` | `DEFECT` | arrayFormat COMMA_SEPARATED on both sides |
| `vd-comma-format-atomic-coercion` | `DEFECT` | arrayFormat COMMA_SEPARATED on both sides |
| `vd-comma-format-empty-and-trailing-null` | `DEFECT` | arrayFormat COMMA_SEPARATED on both sides |
| `multichar-separator-with-single-separator-char-in-name` | `LOSSLESS` | MapFlattener(true, 50, 1000); JsonReconstructor.builder().separator("__") - quickReconstruct hard-codes "_" and would fail for the wrong reason |
| `top-level-double-sanitization-asymmetry` | `DEFECT` | MapFlattener(true, 50, 1000); JsonReconstructor.builder().separator("__") |
| `order-line-items-comma-separated` | `DEFECT` | arrayFormat COMMA_SEPARATED on both sides |

## 6. Every fixture

`covers` is what the document is; `what happens` is precisely what the round trip does to it.
`defaults` is `YES` when the row also holds through the default reconstruction entry point.

### `structural` - 18 documents (4 lossless, 1 accepted loss, 13 defect)

| id | stack | covers | class | what happens | config | defaults | issue |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `object-chain-depth-8` | BOTH | Pure object chain, depth 8, scalar leaf | `LOSSLESS` | Flattens to the single key l1_l2_l3_l4_l5_l6_l7_l8 = "leaf". No prefix is flagged as an array because the leaf does not start with a bracket; buildHierarchy rebuilds all 8 levels. | defaults | YES | - |
| `array-of-objects-two-elements-top-level` | BOTH | Top-level array of two homogeneous objects | `LOSSLESS` | Flattens to users_name = "[\"Alice\",\"Bob\"]" and users_age = "[30,25]". Prefix users sees two serialized values of parsed size 2, is flagged as an array, and reconstructArray zips the columns back into two elements. | defaults | YES | - |
| `array-of-objects-single-element` | BOTH | Single-element array of objects - the array-vs-scalar ambiguity | `DEFECT` | Both the array wrapper and the value types are lost. Flattens correctly, but both columns parse to size 1, fail the > 1 test, so users becomes an object path and the leaves are never parsed. | defaults | YES | - |
| `nested-array-of-objects-default` | BOTH | Objects inside an array inside an object, default config | `DEFECT` | Flatten is correct. analyzeStructure attributes the whole key value to every prefix, so both order and order_items are flagged as array paths, and the enclosing object becomes an array of identical elements while the real array becomes an object of raw text. | defaults | YES | - |
| `nested-array-of-objects-explicit-hints` | BOTH | Same nested shape, rescued by explicit arrayPaths with inference off | `LOSSLESS` | With inference disabled only the configured path order_items enters analysis.arrayPaths; order stays an object path and the array rebuilds exactly. | JsonReconstructor.builder().inferArraysFromValues(false).arrayPaths("order_items") | NO | - |
| `top-level-array-of-scalars` | BOTH | Arrays of scalars at the document root never reconstruct | `DEFECT` | Both arrays survive flattening intact and come back as those same Strings. The information is fully present in the flattened form; only the reconstructor discards it. | defaults | YES | - |
| `nested-array-of-scalars` | BOTH | Array of scalars one level down turns its parent object into an array | `DEFECT` | Prefix user sees a size-2 serialized array and is flagged as the array path, so the object becomes a 2-element array and the array becomes a scalar at every position. | defaults | YES | - |
| `array-of-arrays-with-empty-inner` | BOTH | Array of arrays, including an empty inner array, at depth | `DEFECT` | Flattening is correct via the _value sentinel. On reconstruct, prefix grid is flagged and the outer object is turned into an array of objects, transposing one nesting level away. | defaults | YES | - |
| `mixed-nested-array-sentinel-collision` | BOTH | Nested array mixed with a scalar - the documented output does not exist | `DEFECT` | Actual flatten output uses the base key "data" rather than the documented data_value, and there is no positional null padding, so index alignment is already destroyed before reconstruction. | defaults | YES | - |
| `heterogeneous-array-scalar-first` | BOTH | Heterogeneous array, scalars before the object - internal sentinel leaks out | `DEFECT` | Reconstruct processes the base key first as a leaf String, then the suffixed key finds a non-map value already at that node and wraps it, inserting a literal "_value" key into user output. | defaults | YES | - |
| `heterogeneous-array-object-first` | BOTH | Heterogeneous array, objects before the scalars - object elements silently deleted | `DEFECT` | The one-segment key overwrites the array holder built from the longer keys, producing a syntactically perfect array with both object elements silently deleted and no error, warning, or log. | defaults | YES | - |
| `ragged-array-of-objects-absent-vs-null` | BOTH | Array of objects with a missing field - absent becomes present-null | `DEFECT` | The second element gains a key it never had. Array length, ordering and all present values are correct, so only a key-set comparison catches it. | defaults | YES | - |
| `array-element-with-nested-object-duplicated` | BOTH | Array of objects whose elements contain a nested object - elements duplicated | `DEFECT` | Both people and people_addr are flagged as arrays from one value; index 1 falls into the use-last-value fallback and reuses the same nested holder, so LA is replaced by a duplicate of a wrong value. | defaults | YES | - |
| `empty-containers-at-depth` | BOTH | Empty object, empty array and explicit null at depth, plus an empty chain | `DEFECT` | Three distinct container states collapse to one null. The ACCEPTED half holds: nesting depth of the empty chain is preserved exactly. | defaults | YES | - |
| `boundary-separator-on-round-trip` | BOTH | useArrayBoundarySeparator ON: index-like keys and an array of objects | `LOSSLESS` | Semantically identical to the flag-OFF run - only the character count of the keys differs. | MapFlattener(true, 50, 1000); JsonReconstructor.builder().separator("__") | NO | - |
| `boundary-separator-on-double-underscore-field` | BOTH | useArrayBoundarySeparator ON mangles a field name containing __ and collides with its sibling | `DEFECT` | sanitizeKey rewrites a__b to a_b; the genuine sibling then collides and generateUniqueKey renames it a_b_2. One field silently renamed, the other given an invented numeric suffix, no error and no warning. | MapFlattener(true, 50, 1000); JsonReconstructor.builder().separator("__") | YES | - |
| `flattener-maxdepth-truncation` | BOTH | Flattener maxDepth reached mid-document - subtree preserved as verbatim JSON text | `ACCEPTED_LOSS` | Re-nesting below the limit is lost; the content is not. The reconstructor leaves the truncated String alone because looksLikeSerializedArray requires a leading bracket. | MapFlattener(false, 3, 1000) | YES | - |
| `reserved-marker-key-dropped` | BOTH | Field names matching the reconstructor internal marker syntax are silently deleted | `DEFECT` | Flattening is correct and lossless; processArrays then drops both __meta__ and ___ by the marker-skip test. No error, no warning, no count discrepancy reported anywhere. | defaults | YES | NP-022 |

### `value-domain` - 18 documents (4 lossless, 2 accepted loss, 12 defect)

| id | stack | covers | class | what happens | config | defaults | issue |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `vd-string-literal-lookalikes` | BOTH | Strings that impersonate JSON literals stay strings, and stay distinct from the literals | `LOSSLESS` | Every value is a String, Boolean or Number that normalizePrimitive returns unchanged. No value promotes the wrap prefix to an array path. | defaults | YES | - |
| `vd-integer-boundaries-within-long` | BOTH | Every integer boundary up to Long.MAX_VALUE survives with its exact type | `LOSSLESS` | Jackson yields Integer for the small values and Long for the rest; normalizePrimitive returns any Number untouched, so 9007199254740993 comes back exactly. | defaults | YES | - |
| `vd-integer-past-long` | BOTH | One integer past Long.MAX_VALUE stops being a number and becomes a quoted string | `DEFECT` | i64max survives as a number; i64maxPlus1 and huge are parsed as BigInteger, hit longValueExact, and fall back to decimal text. No digit is lost; the TYPE is. | defaults | YES | NP-015 |
| `vd-decimal-scale-and-precision` | JSON | Declared decimal scale and beyond-double precision are destroyed before the flattener ever runs | `DEFECT` | Declared scale is gone on trailingZero and oneFive; highScale drops to double precision; beyondDouble becomes indistinguishable from the literal 1.0. The exact digit strings are Jackson/JDK details - if measurement differs, correct the literal, not the classification. | defaults; comparison uses exact-decimal parsing | YES | - |
| `vd-preserve-bigdecimal-flag-inert` | JSON | preserveBigDecimalPrecision is wired, readable, documented - and cannot fire on the JSON stack | `DEFECT` | Output is byte-identical to the same run with the flag false. The probe proves the flag has zero observable effect on this stack while the MAP twin fed a real BigDecimal does yield the exact text. | MapFlattener.builder().preserveBigDecimalPrecision(true); probe compares against the same run with the flag off, plus a MAP twin fed a real BigDecimal | YES | - |
| `vd-exponent-overflow-and-underflow` | BOTH | A finite JSON number silently becomes the string "Infinity", and a tiny one becomes exactly zero | `DEFECT` | over/negOver overflow to Infinity and are emitted as JSON strings; under underflows to 0.0; nearMax survives with exponent reformatting only. | defaults | YES | - |
| `vd-signed-zero` | BOTH | IEEE-754 negative zero survives; the JSON integer -0 does not | `LOSSLESS` | negDouble keeps its sign bit; negInt loses its minus sign. Defensible: no Java integral type can represent a signed zero and preserving it would require promoting every -0 to a double. \|\| MEASURED: CORRECTION. The designer marked this JSON-only on the grounds that the MAP stack cannot observe the integer arm. Measurement shows the JSON stack cannot observe EITHER arm, so as declared the fixture was vacuous: the integer sign is gone at Jackson parse time so it is absent from the baseline as well as the result, and the double sign is invisible under exact-decimal comparison because BigDecimal has no signed zero (both -0.0 and 0.0 render as 0.0). Widened to BOTH. The MAP arm is now the live one: it renders Double.toString, so -0.0 and 0.0 stay distinct and the fixture goes red the moment reconstruction normalises the sign away. Measured LOSSLESS, not ACCEPTED_LOSS: the integer -0 is lost before flatten() is ever called, which is a property of JSON parsing and not of the round trip under test. \|\| CLASSIFIED: MEASUREMENT OVERRIDES THE PREDICTION. Predicted ACCEPTED_LOSS on the grounds that the JSON integer -0 loses its sign. It does - but at Jackson parse time, before flatten() is called, so it is absent from the baseline as well as from the result and no part of the round trip under test destroys it. The IEEE-754 double -0.0 is preserved end to end and the MAP arm pins it (rendered via Double.toString, so -0.0 and 0.0 stay distinct). Declared LOSSLESS for the round trip, with the parse-time integer loss recorded as out of scope. | defaults | YES | - |
| `vd-number-text-normalization` | JSON | Exponent notation is not preserved, and that is fine - this fixture calibrates the oracle | `ACCEPTED_LOSS` | The three fractional spellings converge on one canonical form; the integer stays an integer. Nothing about the VALUE is lost. | defaults | YES | - |
| `vd-empty-containers-end-to-end` | BOTH | The empty-container collapse, followed all the way to the reconstructed JSON document | `DEFECT` | Three different authored values arrive as the same null; nonEmpty is untouched. | defaults | YES | - |
| `vd-preserve-nulls-false-erases-present-null` | BOTH | preserveNulls(false) converts a present null into an absent key, silently voiding the absent-vs-null guarantee | `ACCEPTED_LOSS` | The present key is gone entirely, not nulled. Defensible because it is explicitly opt-in and the sparse-output use case is legitimate; what is not defensible is the silence in the documentation. | JsonReconstructor.builder().preserveNulls(false); probe asserts the default run DIFFERS | NO | - |
| `vd-string-that-looks-like-array` | BOTH | A scalar string whose text resembles a JSON array turns its parent object into an array | `DEFECT` | The string is destroyed, its characters are retyped as integers, and the enclosing object becomes an array of three synthetic objects. | defaults | YES | - |
| `vd-array-lookalike-fabricates-padding` | BOTH | The same misdetection then FABRICATES values that were never in the input | `DEFECT` | Only c triggers detection but arrayPaths is keyed on the shared prefix, so b is pulled in and its single 7 is replicated into three records. | defaults | YES | - |
| `vd-real-array-collides-with-array-text` | BOTH | A genuine array and a string containing its text flatten to byte-identical output | `DEFECT` | Flattened output is identical to the string fixture. Both the array nesting and the element grouping are wrong; the correct answer is an object holding an array, not an array of three objects. | defaults; paired flat-map equality against vd-string-that-looks-like-array | YES | - |
| `vd-comma-format-scalar-with-comma` | BOTH | Under COMMA_SEPARATED, a comma inside an ordinary scalar string invents an array out of nothing | `DEFECT` | Flattening does not touch either value; the damage is entirely on the reconstruct side, where the comma splits the name into two array elements and the sibling is padded by repetition. | arrayFormat COMMA_SEPARATED on both sides | NO | - |
| `vd-comma-format-atomic-coercion` | BOTH | Under COMMA_SEPARATED, four of five strings come back as different types | `DEFECT` | Quoting is gone after flattening, so nothing downstream can know these were strings; parseAtomicValue then returns a boolean, a null, an int with leading zeros dropped, a double with the trailing zero gone, and one surviving String. | arrayFormat COMMA_SEPARATED on both sides | NO | - |
| `vd-comma-format-empty-and-trailing-null` | BOTH | Under COMMA_SEPARATED an array loses an element, not just a distinction | `DEFECT` | Two separate failures: empty-string-to-null (accepted and documented) and three elements becoming two (undocumented, and the reason this is DEFECT rather than ACCEPTED_LOSS). | arrayFormat COMMA_SEPARATED on both sides | NO | - |
| `vd-nested-json-string-length-gate` | BOTH | With parseNestedJsonStrings on, an identical value keeps or loses its type depending on its LENGTH | `DEFECT` | shortJson is expanded into structure; longJson is kept verbatim. Neither field round-trips to its input, and they fail in opposite directions from the same authored type. | MapFlattener.builder().parseNestedJsonStrings(true).maxJsonStringLength(20) | YES | - |
| `vd-control-chars-and-long-string` | BOTH | Control characters, escapes, astral-plane text and a long string all survive intact | `LOSSLESS` | Jackson decodes every escape on the way in and re-escapes correctly on the way out; the surrogate pair stays paired. MapFlattener.escapeString is never reached. | defaults | YES | - |

### `naming` - 18 documents (8 lossless, 1 accepted loss, 9 defect)

| id | stack | covers | class | what happens | config | defaults | issue |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `separator-in-name-vs-nesting` | BOTH | A literal separator in a field name stays distinct from real nesting (NP-021 pin) | `LOSSLESS` | Decode yields the literal one-segment name and the two-segment nested path separately; reconstruct returns the source exactly. | defaults | YES | NP-021 |
| `triple-underscore-key-dropped` | BOTH | A field named ___ is silently deleted by the reconstructor (NP-022) | `DEFECT` | The ___ entry is dropped entirely: a document with one fewer field, no exception, no warning, no null placeholder. | defaults | YES | NP-022 |
| `double-underscore-namespace-swallowed` | BOTH | The whole __*__ namespace is dropped, not just ___ - including entire subtrees | `DEFECT` | Two of three top-level fields are gone, one of them a whole subtree; the _x_ control survives. | defaults | YES | NP-022 |
| `is-array-field-name-changes-json-type` | BOTH | A user field named __isArray__ converts its parent object into a JSON array | `DEFECT` | An object becomes an array and the __isArray__ field vanishes. Attacker-controlled key names can change the reconstructed JSON type, which is a schema-inference hazard, not just a fidelity gap. | defaults | YES | NP-022 |
| `empty-string-key-renamed-into-collision` | BOTH | An empty-string key is renamed to empty_key and steals a real sibling name | `DEFECT` | Both fields end up under wrong names and the first value now sits under a name the source assigned to the second. Renaming "" is arguably defensible; a substitute that is not reserved, not reported, and resolves in favour of the synthetic name is not. | defaults | YES | - |
| `unicode-names-must-not-normalize` | BOTH | NFC and NFD forms, RTL text, and an astral emoji stay four distinct keys | `LOSSLESS` | No name contains a separator or backslash so escapeSegment returns the identical instance; all four keys and values round-trip exactly and the NFC and NFD spellings stay separate map entries. | defaults | YES | - |
| `punctuation-and-control-chars-in-names` | BOTH | Dots, brackets, quotes, spaces and a newline in field names pass through untouched | `LOSSLESS` | All five are single unescaped segments and reconstruct verbatim, including the embedded double-quote and the literal newline. | defaults | YES | - |
| `backslash-and-preencoded-lookalike-names` | BOTH | Five names that all look like a_b under naive concatenation stay five distinct fields | `LOSSLESS` | The five encode to distinct keys and each decodes back to its exact source name. Escaping the backslash first is what keeps the pre-encoded lookalike from decoding as the two-level path. | defaults | YES | - |
| `multichar-separator-with-single-separator-char-in-name` | BOTH | Separator __ with names containing a single _ - the property-test case from the javadoc | `LOSSLESS` | Both keys decode to their exact segments. Neither name contains "__" so sanitizeKey collapse is a no-op here, which isolates the escaping from that control. | MapFlattener(true, 50, 1000); JsonReconstructor.builder().separator("__") - quickReconstruct hard-codes "_" and would fail for the wrong reason | NO | - |
| `array-boundary-separator-manufactures-a-collision` | BOTH | useArrayBoundarySeparator collapses __ inside names and creates the exact collision escaping prevents | `DEFECT` | Neither source name survives, the first value now sits under the name that belonged to the second, and no warning is emitted. Turning the flag off makes both names round-trip perfectly. | MapFlattener(true, 50, 1000); JsonReconstructor.builder().separator("__") | YES | - |
| `top-level-double-sanitization-asymmetry` | BOTH | The same field name gets two different column names depending on its nesting depth | `DEFECT` | Two occurrences of one source name land under two different names, and neither is the original. | MapFlattener(true, 50, 1000); JsonReconstructor.builder().separator("__") | NO | - |
| `snake-case-injects-unescaped-separators` | BOTH | SNAKE_CASE turns a flat scalar field into a nested object | `DEFECT` | A scalar at depth 1 is reconstructed as an object at depth 2. Case-folding is a requested transform and would be ACCEPTED_LOSS on its own; inventing structure is not. | MapFlattener.builder().namingStrategy(SNAKE_CASE); reconstructor default | YES | - |
| `case-only-differing-names-preserved` | BOTH | Names differing only by case stay three distinct fields under AS_IS | `LOSSLESS` | No escaping is triggered, the collision set is case-sensitive, AS_IS skips transformKeys entirely, and all three round-trip exactly. | defaults | YES | - |
| `lower-case-collision-suffix-corrupts-structure` | BOTH | The case-collision disambiguator emits an unescaped _2 that the decoder reads as a nesting level | `DEFECT` | Two scalars become one object with a fabricated _value key and a numeric-string key. Case folding is an ACCEPTED consequence of the requested strategy; the structural corruption and the invented _value are not. | MapFlattener.builder().namingStrategy(LOWER_CASE); reconstructor default | YES | - |
| `strict-key-validation-folds-three-names-into-one` | BOTH | strictKeyValidation is not validation - it silently rewrites and collides | `DEFECT` | All three fold to one name and the clashes are resolved with invented numeric suffixes. The folding is defensible for SQL-safe identifiers; emitting no mapping, no warning and no exception is not. | MapFlattener.builder().strictKeyValidation(true); reconstructor default | YES | - |
| `user-field-named-value-vs-flattener-sentinel` | BOTH | A genuine field named _value stays distinct from the flattener _value sentinel | `LOSSLESS` | No sentinel comparison matches, because every user-supplied segment carries its escape. | defaults | YES | - |
| `underscore-boundary-around-the-np022-trap` | BOTH | Underscore names either side of the NP-022 trap all survive - pinning exactly where it starts | `LOSSLESS` | All four decode to their exact source names; __x fails endsWith, x__ fails startsWith, and _ fails both, so processArrays keeps all of them. | defaults | YES | NP-022 |
| `upper-case-strategy-renames-without-restructuring` | BOTH | UPPER_CASE changes names as requested and does NOT damage the encoding | `ACCEPTED_LOSS` | Names uppercased, structure and nesting intact, the escaped underscore inside a_b still decoded as part of the name. Defensible because the caller explicitly asked for a name transform. | MapFlattener.builder().namingStrategy(UPPER_CASE); reconstructor default | YES | - |

### `avro` - 18 documents (3 lossless, 2 accepted loss, 13 defect)

| id | stack | covers | class | what happens | config | defaults | issue |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `avro-underscore-field-name-column-divergence` | AVRO | Flattened schema and flattened rows disagree on column names for any field containing an underscore | `DEFECT` | The schema names a column no row contains. The DATA assertion passes in isolation, so a corpus that only checks DATA reports this document as fully lossless. | assert KEYSET | NOT_APPLICABLE | - |
| `avro-flattened-name-collision-guard-fires` | AVRO | Collision guard rejects a document the data path round-trips losslessly | `ACCEPTED_LOSS` | Refusing is defensible: with a non-escapable separator the flattening is genuinely non-injective and silently overwriting one field would be worse. What is not defensible is the asymmetry - the data half round-trips this exact datum with perfect fidelity, so the library has no single answer to "is this document representable". | assert SCHEMA (expected to throw) | NOT_APPLICABLE | - |
| `avro-array-statistics-suffix-silent-overwrite` | AVRO | Array-statistics columns silently swallow a real field of the same name | `DEFECT` | The declared field is dropped and the surviving column is the synthetic statistics one. Assert on the doc string, not merely on the presence of the name, or the fixture cannot tell the two columns apart. | includeArrayStatistics=true; assert SCHEMA | NOT_APPLICABLE | - |
| `avro-reconstruct-original-schema-ignores-argument` | AVRO | reconstructOriginalSchema never reads its flattenedSchema parameter | `DEFECT` | Handed an unrelated, field-less record as the flattened schema, the method still returns the original schema. The argument is inert. | assert SCHEMA_ARG_IGNORED - feeds an unrelated schema and shows the original comes back anyway | NOT_APPLICABLE | - |
| `avro-record-defaults-dropped-in-reconstructed-schema` | AVRO | Field default values are erased by schema reconstruction | `DEFECT` | The reconstructed schema reports no default where the original had one. The DATA path is unaffected, so the loss is confined to the schema half. | assert SCHEMA | NOT_APPLICABLE | - |
| `avro-nullable-scalars-and-logical-types-control` | AVRO | Control: nullable scalars and the six supported logical types round-trip exactly | `LOSSLESS` | Each logical type reaches convertLogicalType with a value already in its epoch-integer encoding and takes the identity fast path; the uuid is already canonical. The int/long split matters - do not normalise numeric types in the comparator. | assert DATA | YES | - |
| `avro-nullable-string-literal-null-collapses` | AVRO | A nullable string whose value is the text "null" reconstructs as null | `DEFECT` | status and padded both reconstruct as null; safe survives, which is what lets the fixture distinguish "the sentinel fires too eagerly" from "all strings are broken". No configuration disables this. | assert DATA | YES | NP-022 |
| `avro-union-string-branch-swallows-int` | AVRO | Multi-branch union: a string branch listed before int converts every value to text | `DEFECT` | Both values come back as Strings. No member ordering makes both directions correct - the defect is the absence of a branch discriminator, not the ordering. | assert DATA | YES | - |
| `avro-union-long-before-double-truncates` | AVRO | Union [long, double] silently truncates a fractional value to an integer | `DEFECT` | amount reconstructs as an integer with the fraction gone, silently, with no exception anywhere. count is included so the fixture can distinguish "truncates fractions" from "always picks long". | assert DATA | YES | - |
| `avro-union-of-records-overlapping-fields` | AVRO | Union of records: the first branch sharing any field name wins and the rest of the data is dropped | `DEFECT` | The wrong branch is selected and two children go unconsumed. Total silence: no exception, and a structurally valid record of the wrong type. \|\| MEASURED: CONFIRMED, PLUS A NEW FINDING THE PREDICTION MISSED. The wrong branch is selected exactly as traced and routing/account are dropped. But the losing branch's defaulted field does not come back as a Java null: it comes back as org.apache.avro.JsonProperties.NULL_VALUE, a singleton OBJECT. Any consumer testing `value == null` on a defaulted nullable field sees a non-null object of an Avro-internal type. The corpus renders it as AVRO_NULL_DEFAULT so the distinction is visible and so the fixture does not depend on an identity hash code. Filed as a new defect for triage: useSchemaDefaults should materialise Avro's null default as a real null. | assert DATA | YES | - |
| `avro-decimal-bytes-scale-mismatch` | AVRO | A conforming bytes-backed decimal fails to reconstruct because only precision overflow triggers rescaling | `ACCEPTED_LOSS` | Reconstruction throws. Bytes-backed decimals do not round-trip from JSON-shaped input at all - and the inversion is the part worth recording: only decimals that were already out of range can be processed. Reading the datum with Avro jsonDecoder hides the whole defect, which is why the harness pins the plain-JSON path. \|\| MEASURED: CORRECTION - THE PREDICTED DEFECT DOES NOT REPRODUCE. Reconstruction does NOT throw. Measured output is two ByteBuffers holding unscaled 1250 and 50, i.e. exactly 12.50 and 0.50 at the declared scale 2. The reason is that Avro itself normalises scale: Conversions.DecimalConversion.validate() calls setScale(declaredScale, RoundingMode.UNNECESSARY) whenever the value scale differs, and 12.5 -> 12.50 is exact. The precision-based guard in convertDecimal is therefore dead code for this input rather than a live fault, and the fixture is reclassified ACCEPTED_LOSS: the value survives exactly and the ByteBuffer is the representation the schema mandates. TWO RESIDUAL FINDINGS THIS DATUM DOES NOT COVER, both needing their own fixture: (1) a value with MORE decimal places than the declared scale still reaches Avro with RoundingMode.UNNECESSARY and throws, and the precision guard cannot help because it tests the wrong property; (2) when precision genuinely overflows, the guard rounds HALF_UP silently, so the only decimals this code alters are the ones it should reject. \|\| CLASSIFIED: MEASUREMENT OVERRIDES THE PREDICTION. Predicted DEFECT on the grounds that reconstruction throws. It does not: Avro's own DecimalConversion normalises scale with RoundingMode.UNNECESSARY, so 12.5 becomes 12.50 exactly and both fields encode correctly. What survives is the exact numeric value at the declared scale; what changes is the Java representation, from Double to the ByteBuffer the schema mandates. That is an accepted representation change, not a fidelity defect. The two residual faults this datum does NOT reach are recorded in the fixture and are unfixtured gaps. | assert DATA | YES | - |
| `avro-logical-types-erased-inside-arrays` | AVRO | Every logical type on a field inside an array becomes an untyped STRING column | `DEFECT` | The declared decimal precision and scale are unrecoverable from the flattened schema. The DATA assertion on this same datum passes, so only SCHEMA can see the loss. | assert SCHEMA | NOT_APPLICABLE | - |
| `avro-enum-accepted-fixed-decimal-erased` | AVRO | Enum to bare STRING is defensible; fixed-backed decimal to bare BYTES is not | `DEFECT` | The enum half is ACCEPTED - SQL and Spark columns have no enum type and the symbol text is the only faithful projection. The fixed-decimal half is a DEFECT and is filed here only because a single classification is required; it should be split if the cap allows. \|\| CLASSIFIED: DELIBERATE CLASSIFICATION CHANGE, not a measurement disagreement. The designer filed this ACCEPTED_LOSS while stating in the same breath that the fixed-decimal half is a DEFECT and should be split out. It cannot be split without changing the fixture count, so the aggregate label must reflect the worse half. Enum-to-string is genuinely accepted: SQL and Spark have no enum type and the symbol text is a faithful projection. Fixed-decimal-to-bare-bytes is not: precision 18 and scale 4 are gone and the eight bytes are uninterpretable by any consumer of the flattened schema. | assert SCHEMA | NOT_APPLICABLE | - |
| `avro-bytes-b64-sentinel-and-fixed-charset` | AVRO | A bytes payload literally starting with a Base64 marker is decoded; a fixed value with a high byte fails on platform charset | `DEFECT` | The literal ASCII payload is replaced by decoded bytes with no diagnostic, and the fixed value throws a size mismatch on a UTF-8-default JVM while the same fixture would PASS on an ISO-8859-1-default JVM. A fixture whose outcome depends on file.encoding is exactly what a corpus must pin. \|\| MEASURED: PARTIAL CORRECTION. The fixed/charset half reproduces exactly: reconstruction throws "Fixed size mismatch at sig: expected 4, got 5" because the high byte encodes to two bytes under the JVM default charset (UTF-8 on JDK 18+). The B64 half is NOT observable in this datum: reconstructRecord processes fields in schema order and aborts on sig, so payload is never converted and the sentinel misinterpretation cannot be seen in the output. The fixture pins one of the two defects it was designed to pin; the B64 sentinel needs its own datum with no fixed field beside it. | assert DATA | YES | NP-022 |
| `avro-map-of-records-control` | AVRO | Control: a map with record values, including a key that contains the separator | `LOSSLESS` | Both keys escape and decode correctly. The result is a HashMap where the input was a LinkedHashMap, but Avro maps are unordered and key insertion order is genuinely not part of the value here. | assert DATA | YES | - |
| `avro-array-of-records-control` | AVRO | Control: array of records with a required nested record in each element | `LOSSLESS` | There is no bare array key, so the array node is not a leaf and the column-wise path is taken; elements come back in order. | assert DATA | YES | - |
| `avro-array-of-records-null-element-annihilates-array` | AVRO | A single null element destroys the entire array of records | `DEFECT` | Both the record element and the array length are lost; the extracted field values are silently discarded. No exception at any point. | assert DATA | YES | - |
| `avro-array-of-records-nullable-nested-record-shadowed` | AVRO | One null nested record erases the nested records of every other element | `DEFECT` | The array length is right, the ids are right, and the nested record content is gone from every element. Silent, no exception, plausible-looking output. | assert DATA | YES | - |

### `limits` - 18 documents (8 lossless, 4 accepted loss, 6 defect)

| id | stack | covers | class | what happens | config | defaults | issue |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `depth-below-max` | BOTH | Nesting one level below maxDepth expands fully | `LOSSLESS` | Two map levels, deepest flattenObject runs at depth 1, no truncation; neither value triggers array inference. | MapFlattener.builder().maxDepth(3) | YES | - |
| `depth-exactly-max` | BOTH | Nesting at exactly maxDepth expands fully | `LOSSLESS` | Three map levels, deepest flattenObject runs at depth 2, the check does not fire, and the exact three-level structure rebuilds. | MapFlattener.builder().maxDepth(3) | YES | - |
| `depth-one-over-max` | BOTH | Nesting one level past maxDepth stringifies the subtree instead of expanding it | `ACCEPTED_LOSS` | Bounding depth necessarily truncates, and embedding the exact JSON text is the least-destructive truncation available: every byte survives. What is not defensible is that the type change carries no marker. | MapFlattener.builder().maxDepth(3); flat map must equal depth-overflow-aliases-a-real-string | YES | - |
| `depth-overflow-aliases-a-real-string` | BOTH | A genuine JSON-text string at maxDepth is byte-identical to a truncated subtree past maxDepth | `LOSSLESS` | This document alone round-trips exactly. The defect is that its flattened form is byte-identical to depth-one-over-max, and the paired assertion is what records it. | MapFlattener.builder().maxDepth(3); flat map must equal depth-one-over-max | YES | - |
| `depth-bound-map-vs-list-asymmetry` | BOTH | At one structural position, maxDepth truncates the map and spares the list | `DEFECT` | Sibling keys at the same nesting position get opposite treatment, and reconstruction then compounds it by inferring an array and duplicating the stringified map into an element it never occupied. | MapFlattener.builder().maxDepth(2) | YES | - |
| `depth-bound-bypassed-by-empty-container` | BOTH | An empty map at the depth limit is admitted where a non-empty map is truncated | `DEFECT` | Two sibling values at the identical structural position, one exempt from the bound and one not: {} becomes null without consulting maxDepth while the non-empty sibling is stringified. | MapFlattener.builder().maxDepth(1) | YES | - |
| `empty-chains-yield-keys-with-zero-leaves` | BOTH | Deeply nested empty chains: three keys from a document with no leaves at all | `DEFECT` | Zero leaves in, three keys out. Depth is preserved exactly - the already-pinned ACCEPTED behaviour - but every {} became null, the already-pinned DEFECT, now shown to propagate at every depth. | defaults | YES | - |
| `array-size-below-max` | BOTH | A three-element array of maps under maxArraySize=4 round-trips exactly | `LOSSLESS` | limit = min(3,4) = 3, no truncation, all columns the same length so no padding occurs and the zip-back is exact. | MapFlattener.builder().maxArraySize(4) | YES | - |
| `array-size-exactly-max` | BOTH | An array of exactly maxArraySize elements is not truncated | `LOSSLESS` | limit = min(3,3) = 3, all elements retained, identical behaviour to the below-max fixture. | MapFlattener.builder().maxArraySize(3) | YES | - |
| `array-size-one-over-max` | BOTH | An array one element past maxArraySize loses the extra element silently | `ACCEPTED_LOSS` | Discarding elements past a configured array bound is precisely what the bound is for, and critically the TYPE survives - the field is still a list of the right element shape. | MapFlattener.builder().maxArraySize(2) | YES | - |
| `array-size-max-one-collapses-the-type` | BOTH | maxArraySize=1 does not shorten the array, it turns it into an object | `DEFECT` | An array of three objects comes back as an object with string fields: element type, container type and value type all wrong. Set maxArraySize to 2 and the same document returns a proper array - two configurations of one bound, two different reconstructed schemas. | MapFlattener.builder().maxArraySize(1) | YES | - |
| `single-element-array-at-the-arity-lower-bound` | BOTH | A one-element array round-trips to a string even with limits wide open | `DEFECT` | Flattening is exactly right; reconstruction fails on arity alone. At arity 1 the array degrades to a string while at arity 2 the PARENT is wrongly promoted, so no single-sided fix resolves both. | defaults | YES | - |
| `sparse-array-of-maps-padding-misaligns-elements` | BOTH | Sparse array of maps: cells grow quadratically and values move to the wrong element | `DEFECT` | Every sparse value is RELOCATED into element 0 and the later elements are emptied. Not loss - silent corruption that reads as valid data. Every element also gains explicit nulls for keys it never had. | defaults | YES | - |
| `wide-record-210-fields-under-one-parent` | BOTH | A 210-field record under a single parent round-trips exactly | `LOSSLESS` | Every value fails the String check so no array inference runs and the record rebuilds field-for-field. | defaults (210 < maxMapSize 10000) | YES | - |
| `wide-separator-heavy-np021-regression` | BOTH | Separator-laden field names at width: the NP-021 blow-up shape, now expected to be linear | `LOSSLESS` | The literal underscores inside the names are escaped and the structural ones are not; decodeSegments recovers exactly three segments per key. | defaults | YES | NP-021 |
| `map-size-limit-truncates-fields-silently` | MAP | A record wider than maxMapSize loses its trailing fields | `ACCEPTED_LOSS` | Capping record width is the stated purpose of maxMapSize, iteration order is deterministic so the truncation is reproducible, and nothing about the surviving data type or position changes. One inconsistency worth recording: this bound announces itself at WARN while the array bound announces itself only at DEBUG. | MapFlattener.builder().maxMapSize(8) | YES | - |
| `reconstructor-maxdepth-is-a-dead-control` | BOTH | JsonReconstructor.maxDepth(2) happily reconstructs eight levels | `LOSSLESS` | The document round-trips perfectly under maxDepth(2), and the probe proves the setting is inert by showing maxDepth(1) and maxDepth(1000) produce byte-identical results. Operational note: the flattener converts StackOverflowError to IllegalStateException while the reconstructor catches only Exception, so it cannot fail closed on depth. | JsonReconstructor.builder().maxDepth(2); probe compares maxDepth(1) against maxDepth(1000) on the identical flat map | YES | - |
| `invalid-maxdepth-fails-closed` | MAP | maxDepth(0) is rejected at configuration time, before any document is seen | `ACCEPTED_LOSS` | Throws IllegalArgumentException during configuration - the input document is never parsed or flattened. Failing closed is right: a depth budget of zero has no coherent meaning and both alternatives (clamping, honouring) are worse. | MapFlattener.builder().maxDepth(0) - expected to throw at build time | YES | - |

### `real-world` - 18 documents (5 lossless, 0 accepted loss, 13 defect)

| id | stack | covers | class | what happens | config | defaults | issue |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `order-multi-line-items` | BOTH | E-commerce order with two line items, nested shipping address and an explicit null discount | `LOSSLESS` | line_items is detected as an array; shipping_address has no array-valued leaf so it stays an object. Both discount_pct slots are physically present in both elements so the padding path is never entered. | defaults | YES | - |
| `order-single-line-item` | BOTH | E-commerce order with exactly one line item | `DEFECT` | The array becomes an object AND its leaves become JSON text: both the container type and the leaf types are wrong. | defaults | YES | - |
| `order-line-item-nested-address` | BOTH | E-commerce order whose line items each carry a nested ship_from block | `DEFECT` | Warehouse identity is destroyed for both items: the nested block is duplicated across elements and its leaves stay raw text. | defaults | YES | - |
| `order-optional-discount-absent` | BOTH | E-commerce order where only the second line item carries a discount | `DEFECT` | discount_pct migrates from line item 2 to line item 1 and item 2 gains an explicit null it never had. The document round-trips with the correct field count and a plausible shape - only the association is wrong. | defaults | YES | - |
| `empty-cart` | BOTH | Abandoned cart with no line items, no promo codes and an uncomputed totals block | `DEFECT` | A consumer iterating line_items now has to null-guard a field the source schema declares as a non-nullable array. | defaults | YES | - |
| `line-item-tag-arrays` | BOTH | Order whose line items each carry a tag array of differing length | `DEFECT` | Element identity and count are right; the tags type is wrong - inner arrays degrade to JSON text and one nesting level is lost. | defaults | YES | - |
| `order-top-level-tags-array` | BOTH | Order with two top-level arrays of plain strings | `DEFECT` | Both arrays come back as JSON strings. Flattening is correct; the single-segment keys never enter the prefix loop, so inference is never offered the path. | defaults | YES | - |
| `order-line-items-comma-separated` | BOTH | Order exported in COMMA_SEPARATED format with a description containing commas | `DEFECT` | Defensible because the format exists to feed CSV/Athena SERDEs that cannot represent an embedded delimiter, the collision is documented verbatim, and a lossless alternative is one builder call away. NOT defensible, and worth separate triage: the reconstructor fabricates extra rows and duplicates values rather than reporting that the element counts disagree. | arrayFormat COMMA_SEPARATED on both sides | NO | - |
| `customer-profile-addresses-and-preferences` | BOTH | Customer profile with two addresses, a preferences map and non-ASCII names | `LOSSLESS` | addresses is detected as an array; preferences holds only scalars and booleans so it stays an object. The postal code survives as a String because serializeArray writes it with JSON quotes. | defaults | YES | - |
| `customer-profile-legacy-key-shadow` | BOTH | Customer profile carrying both a preferences object and a legacy flat preferences_theme column | `LOSSLESS` | The nested path encodes to two segments joined by the separator while the literal legacy field encodes to one segment with the separator escaped; decodeSegments splits only on unescaped separators. | defaults | YES | NP-021 |
| `customer-preferences-object-with-array` | BOTH | Customer profile whose preferences map contains a notification_channels array | `DEFECT` | preferences becomes a three-element array with the scalars duplicated. Genuinely ambiguous on the wire, so a correct fix is a format change - but the same class of ambiguity in the KEY domain was treated as a bug worth a breaking change, and the value domain deserves the same verdict. | defaults | YES | - |
| `event-envelope-sentinel-metadata-keys` | BOTH | Event envelope with a __meta__ header and a __type__ payload discriminator | `DEFECT` | Flattening is faithful; the loss is entirely in the reconstructor internal-marker guard. Correct fix is to stop putting markers in the user namespace, not to tighten the name pattern. | defaults | YES | NP-022 |
| `event-envelope-mixed-attachments` | BOTH | Event envelope whose attachments array mixes objects with a legacy inline string | `DEFECT` | Two of three attachments are silently deleted. No error, no warning, and the field count at root is unchanged, so nothing downstream can detect the deletion. | defaults | YES | - |
| `financial-transaction-high-precision` | JSON | Financial transaction with a sub-cent amount, an 18-digit FX rate and money-as-text alongside | `DEFECT` | amount.value and fx_rate lose precision at PARSE time; amount_text survives exactly. Marked JSON-only because on the MAP stack the source Map already holds the rounded Double and the fixture would report LOSSLESS while silently destroying money. | defaults; comparison uses exact-decimal parsing | YES | - |
| `financial-transaction-huge-minor-units` | BOTH | Financial transaction with an amount in minor units beyond long range, next to a 2^53+1 sequence | `DEFECT` | Exact-but-retyped beats silently-wrong: the alternative it replaced truncated the low 64 bits and produced a plausible NEGATIVE number nothing downstream could detect. Still open and worth separate triage: the flattener could keep the BigInteger, or the reconstructor could re-widen. | defaults | YES | NP-015 |
| `telemetry-nested-optional-groups` | BOTH | Telemetry record with four-level optional groups, null sensor readings and a sparse label map | `LOSSLESS` | Every prefix sees only scalars and nulls so none is misread as an array; preserveNulls defaults true so both nulls come back explicit. | defaults | YES | - |
| `cdc-create-before-null` | BOTH | CDC insert event with a null before-image and a populated after-image | `LOSSLESS` | before reconstructs as an explicit null; source and after are scalar-only so both stay objects; the epoch-millis long is never routed through double. | defaults | YES | - |
| `cdc-update-with-array-column` | BOTH | CDC update where both before and after images contain a tag_list array of different lengths | `DEFECT` | A downstream diff now compares a 2-row array with a 3-row array and reports every field as changed, when the only real change was status plus one added tag. | defaults | YES | - |

## 7. Cross-fixture invariants

Some findings are not visible from any single document - each fixture round-trips or fails on its own
terms, and only the relationship between two of them is the defect. These are asserted separately.

| invariant | kind | holds over | statement |
| --- | --- | --- | --- |
| `pair-array-text-collides-with-real-array` | FLAT_EQUAL | `vd-string-that-looks-like-array` + `vd-real-array-collides-with-array-text` | A scalar string holding the text of an array and a genuine array flatten to byte-identical output. The value domain is not injective, so at most one of the two can ever round-trip without a wire-format change. This equality is the value-domain analogue of the FlattenedPath key-injectivity proof. |
| `pair-depth-truncation-aliases-a-real-string` | FLAT_EQUAL | `depth-one-over-max` + `depth-overflow-aliases-a-real-string` | A subtree truncated by maxDepth and a genuine user string containing that same JSON text produce the same flattened map. The depth bound output is indistinguishable from ordinary data; a truncation marker would remove the collision, and the day one is added this assertion flips. |
| `pair-array-bound-preserves-type-at-two` | RECON_TYPE_AT_PATH | `array-size-exactly-max` at `r` | At maxArraySize=3 the reconstructed field is still a list. Baseline for the type-flip assertion below. |
| `pair-array-bound-preserves-type-when-truncating` | RECON_TYPE_AT_PATH | `array-size-one-over-max` at `r` | Truncating from 3 elements to 2 loses data but preserves the container type. This is the well-behaved side of the cliff. |
| `pair-array-bound-of-one-flips-the-type` | RECON_TYPE_AT_PATH | `array-size-max-one-collapses-the-type` at `r` | At maxArraySize=1 the same field reconstructs as an OBJECT, not a shorter list. A resource bound has changed the reconstructed schema. Paired with the two assertions above, this names the type flip directly rather than leaving it inside a general inequality. |
| `pair-single-element-array-degrades-to-scalar` | RECON_TYPE_AT_PATH | `single-element-array-at-the-arity-lower-bound` at `r.tags` | With every limit wide open, a one-element array still reconstructs as a scalar string. This isolates the size>1 detection gate from maxArraySize entirely. |

## 8. Where measurement contradicted the designer

Each fixture carries a hand-traced prediction made before it was run. These are the rows where the
prediction was wrong, kept visible rather than quietly corrected. Every disagreement between a
prediction and a published classification must appear here, and every entry here must correspond to a
real disagreement - the corpus asserts that in both directions.

**`vd-signed-zero` -> `LOSSLESS`**

MEASUREMENT OVERRIDES THE PREDICTION. Predicted ACCEPTED_LOSS on the grounds that the JSON integer -0 loses its sign. It does - but at Jackson parse time, before flatten() is called, so it is absent from the baseline as well as from the result and no part of the round trip under test destroys it. The IEEE-754 double -0.0 is preserved end to end and the MAP arm pins it (rendered via Double.toString, so -0.0 and 0.0 stay distinct). Declared LOSSLESS for the round trip, with the parse-time integer loss recorded as out of scope.

**`avro-decimal-bytes-scale-mismatch` -> `ACCEPTED_LOSS`**

MEASUREMENT OVERRIDES THE PREDICTION. Predicted DEFECT on the grounds that reconstruction throws. It does not: Avro's own DecimalConversion normalises scale with RoundingMode.UNNECESSARY, so 12.5 becomes 12.50 exactly and both fields encode correctly. What survives is the exact numeric value at the declared scale; what changes is the Java representation, from Double to the ByteBuffer the schema mandates. That is an accepted representation change, not a fidelity defect. The two residual faults this datum does NOT reach are recorded in the fixture and are unfixtured gaps.

**`avro-enum-accepted-fixed-decimal-erased` -> `DEFECT`**

DELIBERATE CLASSIFICATION CHANGE, not a measurement disagreement. The designer filed this ACCEPTED_LOSS while stating in the same breath that the fixed-decimal half is a DEFECT and should be split out. It cannot be split without changing the fixture count, so the aggregate label must reflect the worse half. Enum-to-string is genuinely accepted: SQL and Spark have no enum type and the symbol text is a faithful projection. Fixed-decimal-to-bare-bytes is not: precision 18 and scale 4 are gone and the eight bytes are uninterpretable by any consumer of the flattened schema.

**`order-line-items-comma-separated` -> `DEFECT`**

DELIBERATE RECLASSIFICATION, ACCEPTED_LOSS -> DEFECT, on review of the loss claims. The fixture was filed as an accepted delimiter collision while its own detail said the reconstructor 'fabricates extra rows and duplicates values' and called that not defensible. The measurement is worse than the label: a two-line order reconstructs as FOUR line items, with SKU-312 repeated three times and quantity 8 attached to two lines that were never ordered. Losing the comma inside 'Bolt, hex, M8' is an accepted consequence of asking for a comma-delimited format; inventing two billable order lines is not, and publishing it under a classification whose repair status reads 'not scheduled for repair' would be indefensible. This follows the precedent already set by avro-enum-accepted-fixed-decimal-erased: where a fixture straddles both categories, the aggregate label must reflect the worse half.

**`financial-transaction-huge-minor-units` -> `DEFECT`**

DELIBERATE RECLASSIFICATION, ACCEPTED_LOSS -> DEFECT, on review of the loss claims. This row and vd-integer-past-long measure the identical behaviour - a BigInteger past Long.MAX_VALUE is emitted as a quoted string, every digit intact and the JSON type gone - and the corpus was publishing them under different classifications, so the same fact appeared to a consumer as a defect under repair in one family and an accepted design limit in another. The stated rationale, 'exact-but-retyped beats silently-wrong', compares the behaviour to the older implementation that wrapped to a negative number; that is an argument for why it is no longer a catastrophe, not an argument for why it is correct. The manifest text already conceded it was 'still open and worth separate triage'. Aligned with vd-integer-past-long as DEFECT.

## 9. Running the corpus against your own data

The corpus is not a fixed list you have to accept - it is a harness you can point at your own
documents, which is the only way to find out what *your* data loses.

### Run the published corpus

```bash
./mvnw -o test -Dtest=RoundTripFidelityCorpusTest
```

All 108 documents, on every declared stack, plus the cross-fixture invariants and the
control probes. A green run means the library still behaves exactly as this page says - including still
being broken in exactly the ways it says.

### Add one of your own documents

1. Drop a fixture at `src/test/resources/fidelity/<family>/<your-id>.json`. Copy the nearest existing
   file and replace `input`, `title`, `rationale`, `catchesBugClass`, `cannotCatch` and `predicted`.
   Write `predicted` **before** you run anything - a prediction made after the fact measures nothing.
2. Add a matching entry to `src/test/resources/fidelity/manifest.json` and bump the counts. The corpus fails if a fixture
   file exists that no manifest entry claims, or the reverse, so this step cannot be skipped.
3. Record the measured behaviour:
```bash
./mvnw -o test-compile dependency:build-classpath -Dmdep.outputFile=target/test-cp.txt
java -cp "target/classes;target/test-classes;$(cat target/test-cp.txt)" \
     io.github.pierce.fidelity.FidelityCorpusRecorder src/test/resources/fidelity
```
   The recorder writes only the `expected` block. It never edits the manifest and never decides a
   classification: it prints the fixtures whose measurement disagrees with the manifest and stops.
   Whether a measured loss is `ACCEPTED_LOSS` or `DEFECT` is a judgement a person makes and signs.
4. Compare your prediction with the recording. Where they differ, add a `classificationOverrides`
   entry saying so; the corpus refuses to let a disagreement go unexplained.

### Just check whether one document survives

If you only want a yes/no answer for a document, you do not need a fixture:

```java
Map<String, Object> src  = new ObjectMapper().readValue(json, new TypeReference<>() { });
Map<String, Object> flat = MapFlattener.builder().build().flatten(src);
Map<String, Object> back = JsonReconstructor.quickReconstruct(flat);
boolean survives = back.equals(src);   // Map.equals, not JsonReconstructor.verify
```

Use `Map.equals`, not `JsonReconstructor.verify()`. `verify()` treats String and Number as a compatible
type pair and compares doubles with an absolute tolerance of `1e-6`; wired to that oracle this corpus
reports the 30-digit-integer row and the decimal-precision row as perfect while the data is demonstrably
changed. It is not a fidelity check.

## 10. Regenerating this document

This page is generated. Editing it by hand is pointless - `RoundTripFidelityDocTest` compares it
against a fresh render of the manifest and fails on any difference. Change `src/test/resources/fidelity/manifest.json`,
then:

```bash
./mvnw -o test-compile dependency:build-classpath -Dmdep.outputFile=target/test-cp.txt
java -cp "target/classes;target/test-classes;$(cat target/test-cp.txt)" \
     io.github.pierce.fidelity.FidelityDocGenerator \
     src/test/resources/fidelity/manifest.json docs/ROUND_TRIP_FIDELITY.md
```
