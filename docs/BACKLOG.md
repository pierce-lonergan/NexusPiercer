# Discovery-Generated Backlog — NexusPiercer
> Improvements, refactors, and enhancements identified during exploration
> Last Updated: 2026-08-17
> Last verified against: `05982a4` (pre-change HEAD); items closed on 2026-08-17 were measured,
> not assumed — [BL-007]'s premise was refuted rather than fixed.

## Backlog Item Format
```
### [BL-XXX] Title
- **Type:** [Refactor | Feature | Fix | Enhancement | Chore]
- **Priority:** [Critical | High | Medium | Low]
- **Effort:** [XS | S | M | L | XL]
- **Related Concern:** [C-XXX or N/A]
- **Affected Files:** [list]
- **Description:** [What and why]
- **Acceptance Criteria:** [Definition of done]
- **Discovered:** Session X
```

---

## Critical Priority

*None yet*

---

## High Priority

### [BL-010] JsonFlattener is dead public surface — CLOSED 2026-08-17 ✅

**Resolved in 2.1.0, additively.** `JsonFlattener.Builder.buildFlattener()` returns the immutable
engine and `JsonFlattener.newOperation()` returns a fresh per-document `FluentOperation`. `build()`
is untouched and still returns `FluentOperation`, so every existing chain recompiles and re-links
unchanged. Both terminals route through one private `resolveFlattener()` so the two doors cannot
drift into configuring different engines.

The diagnosis below was right about the shape and understated the cost. The real gap was not
"a type you cannot name" — it was that **the only object the API could hand out was the unsafe
one**. `JsonFlattener` is immutable over a thread-safe `MapFlattener`; `FluentOperation` holds
unsynchronised mutable `currentData`, `transformers`, `validationRules` and `filter`. Two threads
calling `from(...)` on a shared `FluentOperation` race on the loaded document and can both observe
the same input — a silent wrong answer, not a crash. Meanwhile the class javadoc advertised
"Thread-safe for concurrent use", a published safety claim about an object no caller could get.
That sentence is now split into its two true halves.

**Fixed alongside, because every factory routes through it:** the constructor at line 221 guarded
`config` into `this.config` and then dereferenced the raw parameter on the next line, so
`JsonFlattener.with(flattener, null)` threw `NullPointerException: Cannot invoke
"…JsonFlattenerConfig.isUsePrettyPrint()" because "config" is null`. Shipping `buildFlattener()`
over an unfixed constructor would have widened that surface.

**CORRECTION to this entry, stated loudly.** It claimed below that "the class has **zero** tests
anywhere in the repository. A full grep of `src/test` finds exactly one reference to it, in
`FidelityRunner`." That was **false when written**.
`src/test/java/io/github/pierce/JsonFlattenerTest.java` has 63 tests across 12 nested classes in
1010 lines, ported from `src/test/groovy/JsonFlattenerTest.groovy`, which `git ls-tree` confirms
existed at the commit where this entry was filed. The grep evidently covered only `src/test/java`,
which at the time held no Groovy. The accurate and sharper statement is: **zero** tests touched
`flattenToMap`, `flattenToJson` or `flattenMapToJson`, because none could.

**Still open, deliberately deferred** — see [BL-015] (five inert config knobs) and [BL-016]
(3.0.0 breaking cleanup).

---

**Original entry, retained:**

**Found by:** the P2.2 published-recipe gate (`PublishedSnippetsCompileTest`), while making
`manifest.stacks[*].code` compile.

`JsonFlattener` is a 2000-line public class that no caller outside `io.github.pierce` can hold a
reference to. Its only constructor is private; `create()`, both `with(...)` overloads and
`Builder.build()` all return `FluentOperation`; no public member anywhere in the file returns
`JsonFlattener`. Its public instance methods `flattenToMap`, `flattenToJson` and
`flattenMapToJson` are therefore unreachable from outside the package.

The manifest's published Stack B recipe declared a `JsonFlattener` variable and so could never have
compiled in a consumer's project. That snippet has been corrected to the fluent form, which reaches
the same code because `FluentOperation.from(String)` delegates to `flattenToMap` — note that
`from(Map)` and `from(JsonNode)` do **not**, so "FluentOperation always routes through
flattenToMap" is false and must not be written down.

Additional evidence: the class has **zero** tests anywhere in the repository. A full grep of
`src/test` finds exactly one reference to it, in `FidelityRunner`.

**Not fixed here, deliberately.** Making the constructor or a factory return `JsonFlattener` is a
public-API change that needs a review and a migration path, and doing it in the same pass would
silently invalidate the corrected Stack B snippet the moment it landed. Cross-reference
[BL-008], whose description ("a fluent wrapper around MapFlattener") is now known to be incomplete:
it is a wrapper nobody can name.

**Re-verified 2026-08-11** during the corpus gap-closing pass, against
`src/main/java/io/github/pierce/JsonFlattener.java` as it stands: the constructor at line 221 is
`private`; `create()` (179), both `with(...)` overloads (192, 206) and `Builder.build()` (378) all
return `FluentOperation`; no public member returns `JsonFlattener`. The instance methods
`flattenToMap` (255), `flattenToJson` (276) and `flattenMapToJson` (289) remain unreachable from
outside the package. Still not fixed here — same reason. This entry is the one the Phase 5
facade-reduction task must inherit.

---

### [BL-011] Two independently-set array-format knobs that must agree, with no cross-check

**Found by:** the P2.2 gap-closing pass, while looking for an `AvroReconstructor` setting that would
make the corpus's Avro defaults arm falsifiable (it had been comparing a value with itself).

`MapFlattener.ArraySerializationFormat` and `AvroReconstructor.ArraySerializationFormat` are separate
enums on separate builders, both defaulting to `JSON`. Nothing validates that a reconstructor's
format matches the flattener that produced the data.

> **CORRECTION 2026-08-17, two ways.** *(Analysis complete; the fix is NOT implemented — see
> "Status" at the end of this entry.)*
>
> **(1) "the failure … is a `NumberFormatException`" is wrong about what the caller sees.**
> `convertPrimitive` CATCHES the `NumberFormatException` and, because `strictValidation` defaults
> to true, rethrows `IllegalArgumentException("Cannot convert '1,2,3' to INT at: ids[0]")`, which
> is then wrapped twice in `ReconstructionException`. The NFE survives only as the ROOT cause.
> That distinction matters for fixture design: `FidelityRender.thrown` unwinds to the root cause
> and records only class + message, so a corpus row WOULD record the `NumberFormatException` —
> the original claim is true of the harness and false of the API. It also means the improved
> diagnostic **cannot be pinned by a fixture** and must be pinned by a unit test, or the fix
> ships with a control that appears present and does nothing.
>
> **(2) The measured table below covers `array<int>` only, and therefore misses the worse half.**
> On a `array<string>` field the same mismatch throws NOTHING: flattener `COMMA_SEPARATED` into
> reconstructor `JSON` with input `["a","b","c"]` produces the flat leaf `a,b,c`, which
> `deserializeArray` wraps as a single-element list, and `convertPrimitive` returns unchanged.
> The result is `["a,b,c"]` — **one element instead of three, no exception, no log**. Silent data
> corruption. "Throws" is the benign outcome of this defect, not the defect.

**Measured**, directly against `target/classes` with a hand-built flattened map and schema
`record Batch { array<int> ids; string label }`:

| flattened `ids` leaf | `JSON` | `COMMA_SEPARATED` | `PIPE_SEPARATED` | `BRACKET_LIST` |
| --- | --- | --- | --- | --- |
| `[1,2,3]` | `[1, 2, 3]` | `[1, 2, 3]` | `[1, 2, 3]` | `[1, 2, 3]` |
| `1,2,3` | throws | `[1, 2, 3]` | throws | throws |
| `1\|2\|3` | throws | throws | `[1, 2, 3]` | throws |

So the setting is **live, but a no-op on anything `MapFlattener` emits at its own default**: the
reconstructor attempts a JSON parse first and succeeds, and only consults `arrayFormat` when that
parse fails. A caller who sets it on one side only gets either silence (if the producer stayed at
JSON) or an exception naming a number (if it did not).

**Not a corpus row yet, and it could be.** The pair is expressible today as
`config.mapFlattener.arrayFormat` beside `config.avro.reconstructor.arrayFormat` on one Avro
fixture; the gap-closing pass added
`avro-boundary-separator-datum-does-not-hold-under-defaults` for the separator knob instead and
recorded this one rather than expanding scope further. Whoever picks it up should add both the
matched and the mismatched fixture — the mismatched one is the consumer-facing defect.

**Related:** this is the same shape as the `useArrayBoundarySeparator` /
`JsonReconstructor.separator` pair the fidelity manifest already documents under `misnamed-controls`
— two halves of one wire format, configured independently. A facade that owns both halves is the
structural fix, which is why this belongs to Phase 5 rather than to a local patch.

**Status 2026-08-17: STILL OPEN. Analysed, corrected above, NOT implemented.** The designed fix is
additive and shippable in a 2.x — accessors on both builders (neither class exposes any today, so
a cross-check is not even *expressible* by a caller), a `FlattenRoundTrip` facade that owns both
halves and rejects a mismatched pair, an `AvroReconstructor.Builder.matching(MapFlattener)` that
copies the format by `name()`, and a tri-state `arrayFormatMismatchPolicy(IGNORE|WARN|FAIL)`
defaulting to `IGNORE` so today's silent behaviour is preserved byte-for-byte unless opted out of.
It was scoped out of the 2026-08-17 change deliberately: it is a new public type plus a new enum
plus three new fixtures, and bundling it into a static-analysis cleanup is exactly how the stale
exclude-block comment came to exist. Unifying the two enums into one shared type is the correct
design and is a hard break — deferred to 3.0.0 with a migration note.

---

### [BL-012] Four `AvroReconstructor` knobs with no observed effect — **SETTLED, AND THREE REPAIRED IN 2.1.0**

> **REPAIRS 1, 3, 4 AND 5 ARE DONE (2026-08-18).** `useSchemaDefaults` now supplies a
> schema-correct default and genuinely suppresses one at `false`; `allowMissingFields`
> defaults to `false` and means FAIL-vs-FILL; the empty-map bypass is deleted; and all four
> builder setters have javadoc. Repair 2 (make `useSchemaDefaults(false)` honest rather
> than deprecating it) was taken rather than declined, precisely so the fix would not
> MANUFACTURE a fourth member of the manifest's `inert-controls` block — after routing both
> settings through `GenericData.getDefaultValue` they would otherwise have produced
> identical output at both values.
>
> **TWO CORRECTIONS TO THE MEASUREMENT BELOW, made while repairing it.** (a) The claim that
> `useSchemaDefaults` "cannot do what its name says" is true only for a NON-NULLABLE
> defaulted field, where the unset slot is refilled by `build()`. On a NULLABLE field it
> always could suppress — the ladder set null explicitly — so the knob's behaviour depended
> silently on nullability. (b) A FOURTH defect in the same ladder, named nowhere: because
> `useSchemaDefaults` was tested BEFORE `hasDefaultValue`,
> `.useSchemaDefaults(false).allowMissingFields(false)` emitted "Required field missing and
> no default: color" about a field that HAS a default. It was masked only by the old
> `allowMissingFields` default, so flipping that default would have detonated it — which is
> why the ladder was reordered around `hasDefaultValue` rather than patched at the leaves.
>
> **STILL OPEN:** `handleMissingField` invents `""` and `0` for a missing required field
> inside an ARRAY ELEMENT, ungated by `allowMissingFields`. It logs a WARN now, but gating
> it would turn array-of-records reconstructions that succeed today into throws at the
> shipped default and puts the `avro-array-of-records-*` fixtures in play. Deferred loudly
> rather than silently. `enableVerification` still gates one method only.

> **THE TARGETED PROBE THIS ENTRY DEMANDED HAS BEEN RUN.** The entry's own instruction was
> "Anyone acting on this entry must build that document first and let it decide". Documents were
> built to reach each branch and executed against `target/classes`. **The result is not
> inertness. Two knobs are live; the other two are live and lying.** Every statement below is
> measured, and each is pinned by a test in
> `src/test/java/io/github/pierce/AvroReconstructorKnobEffectTest.java`.
>
> **`strictValidation` — LIVE. Move it out of the four.** Ten reader sites. On
> `record R{int n}` with `{"n":"abc"}`: `true` throws
> `IllegalArgumentException("Cannot convert 'abc' to INT at: n")` (wrapped in
> `ReconstructionException`, root `NumberFormatException`); `false` returns `{n=0}`. A
> well-formed document is identical at both settings — **which is exactly why the original probe
> found nothing: all five of its documents were well-formed, so no error branch was entered.**
>
> **`enableVerification` — LIVE, but gates ONE method.** Its single reader guards
> `verifyReconstruction`. It does NOT gate `compareFlattenedMaps`, the other public verification
> entry point, and has zero effect on `reconstructToMap` — which is all the original probe ever
> called. Measured: with the flag false, `verifyReconstruction` throws `IllegalStateException`
> while `compareFlattenedMaps` keeps working and `reconstructToMap` output is identical at both
> settings.
>
> **`useSchemaDefaults` — LIVE BRANCH, BUT IT CANNOT DO WHAT ITS NAME SAYS.** Both values put the
> schema default into the record. At `true` the reconstructor sets `field.defaultVal()` itself;
> at `false` it leaves the field unset and `GenericRecordBuilder.build()` re-supplies the default
> anyway via `RecordBuilderBase.defaultValue` → `GenericData.getDefaultValue`. There is no way to
> tell `GenericRecordBuilder` not to. **Measured on `record R3{string s="unknown"; int n}` with
> `{"n":"1"}`: `true` → `s` is a `java.lang.String`; `false` → `s` is an
> `org.apache.avro.util.Utf8`.** Same text, different runtime type — so "byte-identical output"
> was a TRUE observation of a REAL difference the renderer cannot see.
>
> **THE STING, and it is a live defect at the SHIPPED DEFAULT configuration.** `field.defaultVal()`
> goes through `JacksonUtils.toObject`, which returns a plain `String` for an **ENUM** default
> (and `byte[]` for FIXED/BYTES, `LinkedHashMap` for a record-typed default).
> `GenericRecordBuilder.set` does not type-check. **Measured on
> `record R4{string id; Color color = "RED"}` with `{"id":"x"}` at default configuration:
> `color` is a `java.lang.String` and `GenericData.get().validate(schema, rec)` returns
> `FALSE`** — the record cannot be binary-encoded. That is the "unwritable datum" defect class
> the corpus already tracks. `useSchemaDefaults(false)` reconstructs it correctly. **The default
> setting is the broken one.**
>
> **`allowMissingFields` — DOES NOT ALLOW MISSING FIELDS, at either value.** Measured on
> `record R2{string id; string other}` with `{"other":"x"}`: `false` throws
> `IllegalStateException("Required field missing and no default: id")`; `true` lets
> `AvroMissingFieldException("Field id type:STRING pos:0 not set and has no default value")`
> escape from `GenericRecordBuilder.build()`, which sits OUTSIDE the per-field try — so the
> caller loses the field path too. **The flag selects which exception you get, not whether
> reconstruction succeeds.** So "no observed effect" was the wrong conclusion, and "it works"
> would have been equally wrong.
>
> **THE ROOT CAUSE OF THE WHOLE NULL RESULT, and the finding to lead with.** The probe's fifth
> document — "empty flattened map against a schema with a required field" — is **the one document
> that provably cannot reach either missing-field branch**. `reconstructToMap` short-circuits an
> empty map into `createEmptyRecord`, which consults NEITHER `useSchemaDefaults` NOR
> `allowMissingFields`, never builds a `GenericRecord`, and silently omits required no-default
> fields. **Measured: an empty map against `R2` returns `{}` with no error at BOTH settings,
> while the SAME schema with one unrelated key present fails loudly.** The document chosen to
> reach the branches is the one document that bypasses them. That is a third defect, not merely
> a shallow probe.
>
> **Status: the diagnosis is settled and pinned; the REPAIRS are NOT implemented.** Every one of
> them changes behaviour for a released caller — the enum-default repair changes DEFAULT
> configuration output and would move any Avro fixture whose schema has a defaulted enum, fixed
> or record-typed field absent from the input, so it needs an audit of all 29 Avro fixtures and a
> deliberate re-record. See "Repairs owed" below.
>
> **Repairs owed, none additive-free:**
> 1. Route non-primitive defaults through `GenericData.get().getDefaultValue(field)` so the datum
>    is writable. Default-config behaviour change; corpus-moving; the highest-value item here.
> 2. Make `useSchemaDefaults(false)` honour its name (null where nullable, named failure
>    otherwise). Blast radius is only callers who explicitly opted out.
> 3. Add a `MissingFieldPolicy{FAIL, FILL_TYPE_DEFAULT}`, honouring an EXPLICIT
>    `allowMissingFields(true)` as `FILL_TYPE_DEFAULT` while an untouched builder keeps 2.0.0
>    behaviour. Note `getDefaultValue(Schema.Type)` returns null for ENUM/FIXED/RECORD, so that
>    case must fail with a message saying so rather than quietly setting null — otherwise the fix
>    ships the very pathology it removes.
> 4. Make the empty-map short-circuit consult both knobs.
> 5. Javadoc all four setters, three of which have none at all today. For `useSchemaDefaults` and
>    `allowMissingFields` the only promise a caller has is the method name, and in both cases the
>    name is false.

**Original entry, retained:**

**Found by:** the same pass. Recorded because it is a lead, **not** because it is established.

Probing `reconstructToMap` across five documents (array of int, array of string, nested record,
empty flattened map against a schema with a required field, and a schema with a field default),
these four builder settings produced **byte-identical output at both values** every time:
`strictValidation`, `allowMissingFields`, `useSchemaDefaults`, `enableVerification`. Two others did
change behaviour and are therefore live: `useArrayBoundarySeparator` (separator rename, now a corpus
row) and `maxDepth` (throws `IllegalStateException` past the bound).

**This is not yet a finding of inertness.** Five documents is a small probe set and each of those
four knobs plausibly guards a branch none of them reached — `allowMissingFields`, for instance, may
only matter for a partially-populated flattened map rather than an empty one. The repository's rule
is that an inert control is proven by a probe that measures two settings producing identical output
on a document chosen to reach the branch, and no such document was constructed here. Anyone acting
on this entry must build that document first and let it decide; do not cite this table as evidence
that the knobs are dead.

---

### [BL-013] Array-of-records element sizing — **FIXED IN 2.1.0, AND THE FILED CAUSE WAS WRONG**

**Filed 2026-08-17. Measured and repaired 2026-08-18. Three of this entry's factual claims were
false, and they are corrected here rather than deleted, because leaving them standing is how the
previous round's premises propagated.**

**WHAT THE ENTRY CLAIMED.** That `calculateArraySize` carried `BRACKET_LIST` / `COMMA_SEPARATED` /
`PIPE_SEPARATED` branches which `determineArraySize` lacks, that consequently "under those three
array formats an N-element array of records collapses to one", and that the fix was to "add the
format fallback to `reconstructArrayOfRecords` Step 1".

**WHAT WAS MEASURED.** All three are refuted.

1. **No collapse occurred under any format** for an element with a scalar field at its root.
   `PathNode.addArrayFieldValue` did not store the raw column text — it stored the output of a
   `static` deserializer that already JSON-parsed, and failing that stripped brackets and split on
   comma then pipe. By the time Step 1 ran, the column was ALREADY a multi-element list, so Step
   1's `rawValues.size() == 1 && startsWith("[")` guard was false and Step 1 was a no-op.
   **Porting the format branches back would have produced a commit that changed no output at
   all**, while reintroducing `DE_MIGHT_IGNORE`, `REC_CATCH_EXCEPTION` and `SF_SWITCH_NO_DEFAULT`
   against a ratchet that may only go down. `docs/audit/FINDINGS.md:2935`'s instruction to port
   them is hereby **discharged as based on a false premise**, not satisfied.

2. **The real collapse was FORMAT-INDEPENDENT.** It fired when every field of the array element
   lived inside a NESTED RECORD, so the array node carried no `arrayFieldValues` and every column
   hung off a child node. `determineArraySize`'s child-node loop only counted a child's values
   when the FIRST value was a String starting with `"[["` — a test that could never fire for a
   column the deserializer had already parsed into a list of plain strings. `maxSize` stayed 0 and
   the trailing `return maxSize > 0 ? maxSize : 1` FABRICATED a size of one. Measured: three
   elements returned as one under all four formats **including the JSON default**. Because that
   floor made zero unreachable, the `if (arraySize == 0) return new ArrayList<>()` branch was dead
   code and an array node with no element data produced one record of type-defaults.

3. **`arrayFormat` was a DEAD KNOB on this path.** The split ran in a `static` method that is
   structurally incapable of reading the instance field. Measured on a document built to reach the
   branch: all four settings produced byte-identical output. And because comma was sniffed before
   pipe, a legal comma inside a `PIPE_SEPARATED` element was split as a delimiter and **fabricated
   a row** — the opposite direction from this entry's claim, and the direction nobody looks for.

4. **A third defect the entry did not name.** `determineArraySize` took `Math.max` over the
   columns, so short scalar columns were padded with `""` and `0` by `handleMissingField` and
   short nested-record columns had their LAST value duplicated by a `Math.min(index, size - 1)`
   clamp. Measured: `sku=S1,S2,S3` beside `meta_code=C1,C2` produced a third row whose code
   repeated the second; the reverse direction silently discarded `C3`. Neither logged anything.

**WHAT WAS DONE.** `determineArraySize` is replaced by `collectElementCounts` (walks the ELEMENT
SCHEMA, descends into nested records through the child node, does not descend through ARRAY-typed
fields because their inner cardinality is legitimately ragged) plus `agreedElementCount` (throws
`ArrayCardinalityException` naming every column and its count and the configured format, rather
than picking a winner). The column split moved to the instance side and is driven by the
configured format with a bracket-aware splitter — and for `BRACKET_LIST` it uses that format's own
reader, because `MapFlattener`'s `BRACKET_LIST` writer quotes and escapes its strings and a raw
bracket split returns values with literal backslashes in them. Under `COMMA_SEPARATED` and
`PIPE_SEPARATED`, text that is well-formed JSON array syntax now raises
`ArrayFormatMismatchException`: those two writers structurally cannot emit a bracketed quoted list,
so it is a detectable contradiction rather than a sniff.

**THE FIRST VERSION OF THAT SPLIT REINTRODUCED THIS ENTRY'S OWN DEFECT, in the other direction and
at the SHIPPED DEFAULT. Caught in adversarial review, before release, and recorded here rather than
quietly patched.** The contradiction check above was written for one direction only. Under
`arrayFormat=JSON` an UNBRACKETED delimited column fell to a bare
`Collections.singletonList(strValue)` with no exception and no log of any level, so N array
elements became one element whose fields held the raw concatenated text. Measured with a single
probe binary against both commits:

```
{items_sku=S1,S2,S3, items_name=N1,N2,N3}  vs  Order{id, items: array<Item{sku,name}>}
  before the split : items size = 3   [{sku=S1,name=N1},{sku=S2,name=N2},{sku=S3,name=N3}]
  after  the split : items size = 1   [{sku=S1,S2,S3, name=N1,N2,N3}]
```

Identical for pipe text. This is D1 and D2 of this very entry, recreated by the repair for them,
which is why it is written up at length: the split's justification — "the configured writer could
not have produced this text" — applies symmetrically, and `MapFlattener`'s JSON writer always
brackets a column, including a single-element one (`["a"]`). The guard is now symmetric, and
narrowed to the case where the two readings DISAGREE: an unbracketed column containing no
delimiter reads as one element under every rule, so it is left alone rather than refused.
`AvroArrayOfRecordsSizingTest` pins all three of those: the comma direction, the pipe direction,
and the no-delimiter control.

**THE EMPTY-ARRAY BRANCH NOW HAS A TEST, and shipped without one.** The claim that
`if (arraySize == 0) return new ArrayList<>()` had become reachable was correct but ungated.
Drilled by reinstating the exact pre-fix floor — `agreedElementCount`'s
`if (distinct.isEmpty()) { return 0; }` patched to `return 1;` — which leaves the entire suite
green, so this was a coverage gap and not an unreachable branch. Against that drill build the new
`arrayNodeWithNoElementDataReturnsAnEmptyList` fails with `expected: <0> but was: <1>` and prints
the fabricated row `line_items=[{sku=, qty=0}]`.

**CORPUS PREMISE ALSO REFUTED.** This entry said `order-line-items-comma-separated` is
`ACCEPTED_LOSS` and would move. It was already `DEFECT`, and it **cannot** move on an
`AvroReconstructor` change: its stack is `BOTH`, and `FidelityRunner` routes `config.reconstructor`
to `JsonReconstructor` while `AvroReconstructor` is configured only from
`config.avro.reconstructor`. Its recorded output is `JsonReconstructor` behaviour and is untouched.
The two owed fixtures were written against the Avro path instead:
`avro-array-of-records-pipe-format-comma-inside-element` and, as a declared control,
`avro-array-of-records-bracket-list-round-trip`, beside the headline
`avro-array-of-records-nested-only-collapses-to-one`.

**RESIDUE, unfixable at the reconstructor and written into the new fixtures' `cannotCatch`.** Under
`COMMA_SEPARATED`, `"Bolt, hex, M8"` and three separate elements are byte-identical after
flattening; `MapFlattener` neither quotes nor escapes on that path. The cardinality check catches
it whenever the split leaves the columns disagreeing, but a document where every column happened to
contain the same number of stray delimiters would split consistently and pass while being wrong.

**ALSO OWED AND NOT DONE:** the disagreeing-count case is pinned in `AvroArrayOfRecordsSizingTest`
rather than in the corpus.

> **CORRECTION, 2.1.0 — the reason given above was FALSE and is the reason nobody looked again.**
> This paragraph used to read "`MapFlattener` always emits equal-length columns from a well-formed
> one; ragged columns come only from externally produced flat maps (Athena, Spark, CDC)". Measured,
> not argued: `{"g":[[{"a":1},{"b":2}],[{"a":3}]]}` — a perfectly well-formed document — emits
> `g_a="[[1,null],[3]]"` with **two** outer entries beside `g_b="[[null,2]]"` with **one**. The
> nested-array arm, `extractFieldsPreservingStructure`, had no padding step at all, so the
> equal-length property held only at the two array-of-maps sites.
>
> **SUPERSEDED, 2026-08-19, by [BL-018].** The third site is now padded, and the same document
> gives `g_a="[[1,null],[3]]"` beside `g_b="[[null,2],[null]]"` — two outer entries each, agreeing
> on inner length position by position as well. So the sentence above ("the 2.1.0 alignment repair
> does NOT close this") is now false, and **a disagreeing-count fixture is NO LONGER expressible
> from a well-formed source document through `MapFlattener`.** The disagreeing-count case is
> therefore correctly pinned in `AvroArrayOfRecordsSizingTest` and in
> `AvroNestedArrayOuterIndexClampTest` — both drive the reconstructor with an EXTERNALLY produced
> flat map, which is the only way that shape now arrives — and the corpus row that was "also owed"
> is withdrawn rather than left open: a fixture is a source document plus a recording, and there
> is no source document that produces it.
>
> The open question the entry posed is ANSWERED. `collectElementCounts`
> (`AvroReconstructor.java`) deliberately does not descend into an ARRAY field's INNER
> cardinality, but it DOES take an array-of-records child's own column sizes as a signal for the
> OUTER level. So ragged columns reach `agreedElementCount`, which refuses them —
> `orders_id=2, orders_items_sku=1` throws `ArrayCardinalityException` naming both counts. They
> do NOT reach the index clamp in `reconstructNestedArrayOfRecordsAtIndex`, which the BL-018
> analysis believed was silently duplicating outer position 0. Measured: it never fires. The
> clamp is removed anyway, as unreachable defence, and the refusal that makes it unreachable is
> what the new test pins.

---

### [BL-014] Multi-branch unions inside array elements — **FIXED IN 2.1.0**

**Filed 2026-08-17, premise corrected the same day, repaired 2026-08-18.**

The corrected framing held up under measurement and is worth restating because the obvious repair
is the wrong one: this is a **never-implemented gap, not a regression**. `unwrapUnion` had four
calls and zero declarations through `ef625f2`, and a declaration with zero callers from `cad816b`.
It never executed, so no arity-3+ behaviour was ever lost and none was owed restoration.
`NoDeadPrivateMethodsInTheFormerlySuppressedClassesTest` asserts by reflection that no method of
that name is declared, and re-adding it would correctly turn the build red.

**THE DEFECT.** `reconstructArrayOfRecords` had no UNION arm at all. Its field dispatch recognised
exactly three shapes — flat column present, RECORD, ARRAY — and `unwrapNullable` collapses only
`[null, T]`, so a union of arity three or more arrived still typed UNION, matched nothing, and fell
off the end into `handleMissingField`, which saw a NULL branch and wrote a plain null. Measured:
`{"items":[{"sku":"a","meta":{"src":"web"}},{"sku":"b","meta":{"src":"pos"}}]}` came back as
`[{sku=a, meta=null}, {sku=b, meta=null}]` while `items_meta_src=["web","pos"]` sat in the tree
unread by anything.

**A SECOND FAULT ON THE SAME FIELD, not in the filing.** When the element DID have a flat column,
`convertPrimitive` was handed the UNION, `unwrapNullable` gave it back unchanged, the switch had no
UNION case and `default: return value` returned it UNCONVERTED. For `["null","long","string"]` with
a Jackson-parsed number that set an `Integer` into a union with no `int` branch:
`GenericData.validate` false, binary encode `UnresolvedUnionException` — the "succeeds until
someone writes it" shape, fixed at the call site by passing the SELECTED BRANCH.

**AN HONEST NARROWING OF THE FILING, measured while testing.** The SILENT drop is specific to
unions that CONTAIN a null branch. With no null branch, `handleMissingField` falls to its switch
default, log.warns, never sets the field, and `GenericRecordBuilder.build()` throws
`AvroRuntimeException`. A null-free 3+ union was already loud, just uselessly so.

**WHAT WAS DONE.** A new `reconstructArrayElementUnion` computes the element-local signals — the
scalar at this index, and the child columns available — reuses `reconstructUnionValue`'s selection
ordering, and dispatches through the index-aware helpers the method already calls, so index
handling is inherited rather than reinvented. `reconstructUnionValue` itself could NOT be called:
it reads `node.value`, `node.isLeaf`, `node.children` and `node.arrayFieldValues` as the content of
ONE value and delegates through an index-free `reconstructValue`, while the only candidate node is
column-wise across all N elements. Handing it over would return the same value for every element
and feed a whole JSON-array column to a scalar field.

Guarded at **arity > 2 deliberately**: every currently-passing `[null, T]` shape keeps its exact
code path, which is why no existing fixture moved.

**THE FIRST VERSION OF THE SCALAR ARM DESTROYED VALUES, and the review that caught it was right.**
Reusing `reconstructUnionValue`'s ordering meant trying the branches in DECLARATION ORDER and
returning the first whose `convertPrimitive` did not throw. At the top level that is unavoidable —
the flat map holds `meta=0007` as a bare unquoted scalar, so the type really has been erased. At
the ARRAY-ELEMENT position it is not: the JSON column keeps the quotes, `items_meta=["0007"]`, and
Jackson boxes the element as a `String`. Declaration order was therefore discarding information
that was present in the input. Measured against a real `MapFlattener` at the shipped default:

| union | document value | first version | HEAD~1 (pass-through) | now |
|---|---|---|---|---|
| `["null","int","long","string"]` | `"0007"` | `Integer 7` | `String "0007"` | `String "0007"` |
| `["null","long","string"]` | `"123"` | `Long 123` | `String "123"` | `String "123"` |
| `["null","boolean","string"]` | `"hello"` | `Boolean false` | `String "hello"` | `String "hello"` |
| `["null","double","string"]` | `"1e999"` | `Double Infinity` | `String "1e999"` | `String "1e999"` |
| `["null","string","long"]` | `123` | `String "123"` | `Integer 123` | `Long 123` |

The BOOLEAN row is worse than the review filed and is the reason this is a general reorder rather
than a numeric special case: `Boolean.parseBoolean` **never throws**, so a boolean branch declared
before a string branch accepts anything at all and the string branch is unreachable. `""` became
`Boolean false` too.

Branch selection is now **type-directed first, declaration order second**: a `CharSequence` tries
STRING/ENUM/BYTES/FIXED first, a `Number` tries the numeric branches first, a `Boolean` tries
BOOLEAN first, and a branch carrying a logical type is never demoted. It is a REORDER and not a
FILTER — when the value's Java type has no native branch (`"0007"` under `["null","int","long"]`)
every branch stays in declaration order and the answer is unchanged, which is pinned by its own
control test. That residual coercion is unavoidable and is now audible: it logs a WARN naming the
before and after text whenever the lexical form changes.

**WHERE THE REPAIR IS IMPOSSIBLE IT IS LOUD INSTEAD.** Two record branches matching the same child
columns are information-theoretically unresolvable from column names — the existing fixture's own
`cannotCatch` says so. Under the default `strictValidation` that now throws naming both branches;
under `strictValidation(false)` it warns and takes the first. A chosen branch that leaves columns
unconsumed warns.

**STILL OPEN, deliberately not bundled:**

- **Arity-2 null-free unions.** `unwrapNullable` returns `types.get(0)` for `["string","int"]` —
  "first branch wins", the answer this entry rules out at higher arity, live today at arity 2.
  Widening the guard would also route `avro-array-of-records-nullable-nested-record-shadowed`
  through the new selector and could change it. Own entry, own fixture move.
- **`reconstructUnionValue`'s own behaviour is byte-identical.** Only the selection ORDERING is
  shared; the loudness was not taken there, so `avro-union-of-records-overlapping-fields` stays
  `DEFECT` and its repair is filed separately.
- **`convertPrimitive`'s `default: return value`** is still reachable from
  `tryReconstructArrayFromFields` and `reconstructNestedRecordFromArray` with a 3+ union. An
  explicit `case UNION:` now logs before returning, so the next caller to do it is heard, but those
  paths need their own fixtures.
- **A NEW FINDING, in `MapFlattener` rather than here.** For a mixed-branch union the flattener
  emits the columns MISALIGNED: `{"items":[{"meta":{"src":"web"}},{"meta":"plain"}]}` produces
  `items_meta=["plain",null]` — element 1's value at index 0 — beside a correctly-aligned
  `items_meta_src=["web",null]`. No reconstructor can undo that. Recorded as
  `avro-array-element-multi-branch-union-mixed-branches`, which is `DEFECT` and now throws where it
  used to be silently wrong for both elements.

---

### [BL-015] Inert `JsonFlattenerConfig` knobs — **FOUR WIRED IN 2.1.0, ONE PERMANENTLY INERT, AND THE COUNT WAS WRONG**

> **NARROWED 2026-08-19, not closed.** Four of the five are honoured as of 2.1.0 — see CHANGELOG
> items 24-26. `failOnError` stays inert, deliberately. And the title below is wrong twice over:
> **seven** knobs were inert, not five.
>
> **THE COUNT.** `InputOptions.isLenient()` and `InputOptions.isSkipInvalid()` are read nowhere in
> `src/main` either; a grep finds each only at its own declaration. That matters beyond
> bookkeeping: `failOnError`'s javadoc directed callers to
> `InputOptions.InputOptionsBuilder#lenient(boolean)` as the live alternative, so the remediation
> for the most misleading of the five pointed at a knob that does nothing. Both are now labelled
> and pinned by `JsonFlattenerConfigKnobsTest`, so the seven is falsifiable rather than a grep
> result in a report. Removal is [BL-016] / 3.0.0, same as `failOnError`.
>
> **WHY WIRING FOUR OF THEM WAS SAFE, against this entry's own advice.** The text below says "do
> not simply make them live" and gives a general argument, which is right. The SPECIFIC argument is
> narrower and survives it: every `JsonFlattenerConfig` default is byte-identical to the per-call
> default it now feeds — charset UTF-8, bufferSize 8192, preserveNulls/includeNulls true, sortKeys
> false — and each is wired as an ENGINE-LEVEL DEFAULT that an explicitly passed
> `InputOptions`/`OutputOptions` still overrides. So the only caller whose behaviour moves is the
> one who explicitly set the knob and previously got nothing: the caller who asked for it.
>
> **WHY `failOnError` COULD NOT BE.** Its NAME gives a direction; its EFFECT is defined nowhere.
> "Do not fail" on `flattenToMap(String)` could mean return an empty map, return null, return a
> partial map, or log and continue, and nothing in the name, the javadoc or any caller picks one.
> A knob whose effect is undetermined cannot be honoured without inventing semantics on released
> API. It is the one knob where "documented, not repaired" is the honest answer rather than the
> convenient one.
>
> **THE PIN BELOW WAS RESTATED, NOT DELETED.** `InertConfigKnobsArePinned`'s all-five assertion
> went red the moment `sortKeys` and `preserveNulls` went live — which is exactly what it existed
> for — and its own failure message said "do not delete this assertion". It is now one assertion
> per knob stating that knob's contract, with both vacuity controls and the `failOnError` throwing
> pin unchanged. Note the prediction it recorded from a temporary wiring drill,
> `expected: <{"z":1,"a_b":null,"a_c":"x"}> but was: <{"a_b":null,"a_c":"x","z":1}>`, reproduced
> exactly when the wiring became permanent.

#### Original entry (2026-08-17), retained

**Filed 2026-08-17 while adding `buildFlattener()`** — because that method now hands consumers an
engine carrying these, and shipping it without saying so would ratify them.

Of `JsonFlattenerConfig`'s six getters only `isUsePrettyPrint` is ever consumed. `getCharset`,
`getBufferSize`, `isFailOnError`, `isPreserveNulls` and `isSortKeys` are read by **nothing** in
`src/main`. Three of the five are settable from the released public `JsonFlattener.Builder` —
`charset`, `bufferSize`, `failOnError` — so `builder().failOnError(false)` is a 2.0.0 control that
does nothing. The live equivalents all live on `InputOptions`/`OutputOptions` instead, and the
buffer size is hardcoded at each use.

**Deliberately NOT `@Deprecated` in 2.1.0.** Unlike genuinely unreachable members, these three ARE
callable today, and a downstream consumer compiling with `-Werror` on deprecation would newly fail
to build. Javadoc now, `@Deprecated` at 3.0.0.

**DISCHARGED IN PART, 2026-08-17.** The two things that could be done without changing behaviour
are done:

1. **Javadoc.** Every one of the five setters on both `Builder` and `ConfigBuilder` now states
   plainly that it is inert and names the live equivalent. `buildFlattener()`'s javadoc no longer
   promises "the configured, shareable engine" outright — it names the `MapFlattener` half as
   honoured in full and lists the `JsonFlattenerConfig` caveats.
2. **A pin, so this cannot change unnoticed.** `JsonFlattenerReusableEngineTest`'s
   `InertConfigKnobsArePinned` asserts byte-identical output across all five knobs, plus the
   engine-surface inertness of `prettyPrint` and the fact that `failOnError(false)` still throws.
   Drilled three ways, including a **vacuity control** — the same comparison is run over `maxDepth`
   and over `prettyPrint`-through-`newOperation()`, both of which are live, and is required to
   report a DIFFERENCE. Without that leg the inertness assertions would be unfalsifiable. Verified
   by wiring `sortKeys` live temporarily; the pin failed with
   `expected: <{"z":1,"a_b":null,"a_c":"x"}> but was: <{"a_b":null,"a_c":"x","z":1}>`.

A measurement correction worth carrying: an adversarial review described these knobs as inert "on
the engine surface" while working "through `newOperation()`". That is true only of `prettyPrint`.
The other five are read nowhere in `src/main` on **any** path — the reads that a naive grep finds
at `JsonFlattener.java:1021` and `:1057` are `OutputOptions.isSortKeys()` and
`InputOptions.getCharset()`, different classes with identically named getters.

**Still open:** honour-or-remove, which is [BL-016] / 3.0.0.

**Do not simply make them live either** — that is a semantic change to released settings. A caller
who set `failOnError(false)` and relies on today's throwing behaviour would silently change
behaviour. Honour-or-remove is a 3.0.0 decision.

---

### [BL-016] 3.0.0 breaking cleanup for `JsonFlattener`

Not doable additively; recorded so it is not lost.

- `Builder.build()` should return `JsonFlattener`, with `buildOperation()` taking over the fluent
  entry. This is the shape the class should have had. It cannot be done at 2.x: every existing
  caller writes `builder().build().from(...)` against `FluentOperation`. `buildFlattener()` is the
  additive stand-in.
- Remove `flattenToJson(String, boolean)` and `flattenMapToJson(Map, boolean)`. Both have zero call
  sites anywhere in `src/main` or `src/test`, and both are strictly weaker duplicates of
  `newOperation().from(x).toJson(opts)`: they bypass transform/validate/filter entirely and ignore
  `OutputOptions.includeNulls` and `config.usePrettyPrint`.
- Resolve [BL-015] one way or the other. **NARROWED 2026-08-19:** four of the knobs are live;
  what remains for 3.0.0 is removing `JsonFlattenerConfig.failOnError`, whose effect is undefined
  rather than unimplemented, plus `InputOptions.lenient` and `InputOptions.skipInvalid`, which
  the original filing did not count.
- **Rename the six flatteners.** README and [BL-008]'s closure note both file the rename here and
  this entry did not carry it until 2026-08-19. The names to settle: `MapFlattener`,
  `JsonFlattener`, `JsonFlattenerConsolidator`, `AvroSchemaFlattener`, `GAvroSchemaFlattener`
  (the `G` prefix is a fossil of the Groovy port) and `EnrichedSchemaFlattener`. Six public types
  whose names do not distinguish DATA from SCHEMA from SCHEMA-PLUS-CASTING; until 3.0.0 the
  family diagram and the selection table in README are the answer.

---

### [BL-019] Multi-table normalization: `Map<String, Dataset<Row>>` from one document

**Filed because a capability was RETRACTED from the documentation, not because it was requested.**
`docs/SPARK_PIPELINE.md` published `NexusPiercerPatterns.jsonToNormalizedTables(spark, schema,
input, "items", "payments", "shipping_events")` returning a map of related tables. That method has
never existed. The section is rewritten in 2.1.0 to describe `explodeArrays(String...)`, which is
what the library actually does - and which is a different thing: it yields **one** dataset with
one row per element of the named array, carrying `_explosion_index`.

Turning that into N related tables needs two concepts that exist nowhere in this codebase:

- a **surrogate key** per parent record, stable across the child tables;
- a **parent-child linkage** so `order_items.order_id` can point back at `orders.id`.

`_explosion_index` is an ordinal within one array, not a key. Until those exist there is no
specification for this feature except the prose that was under suspicion, which is exactly the
circular reasoning that produced the phantom in the first place.

Purely additive whenever it is designed, so it is a feature request and not a 3.0.0 item.
Origin: `OSS-01`.

---

### [BL-020] Incremental processing with an event-time watermark

**Same origin, same retraction.** `docs/SPARK_PIPELINE.md` published
`NexusPiercerPatterns.processIncremental(spark, schema, input, output, "2024-01-15T00:00:00")`.
It has never existed, and unlike [BL-019] there is not even a partial mechanism to point at:

- there is no event-time concept in `NexusPiercerSparkPipeline` - nothing reads a timestamp field
  out of the record;
- there is no state store and no watermark;
- `includeMetadata()` writes `_processing_time`, which is when the pipeline ran, not when the
  event happened. Filtering on it gives you "records this job has not seen", not "records newer
  than T", and the two differ on every late arrival.

The `"2024-01-15T00:00:00"` argument in the deleted snippet had no counterpart in the code and no
defined semantics. Purely additive whenever it is designed. Origin: `OSS-01`.

---

### [BL-021] Semantic accuracy of published documentation has no gate

`DocumentedJavaSnippetsCompileTest` proves TYPE-CORRECTNESS and nothing else. Two measured
examples of what it cannot see:

- `README.md:86` named the fifth `JsonFlattenerConsolidator` constructor argument
  `preserveArrayOrder` when the parameter is `consolidateWithMatrixDenotorsInValue`. It compiled
  perfectly. Fixed by hand in 2.1.0; nothing stops it coming back.
- `docs/SPARK_PIPELINE.md` publishes commented output tables (`orderId | items_sku | items_qty |
  _explosion_index`, and a quality report claiming `avg_json_size 2048.5`). Nothing has ever
  executed them.

The corpus already solves this shape for the fidelity recipes - `PublishedSnippetsCompileTest`
compiles them, asserts byte-identity against the manifest, AND runs them against recorded answers.
Extending the third leg to the Spark examples needs a Spark session in the test tree and fixture
data, which is a real cost. Filed rather than pretended away: the honest current answer is that
documentation accuracy beyond types is a human read.

---

### [BL-017] 3.0.0 removal or extraction of `FileFinder`

Deprecated in 2.1.0; cannot be removed before 3.0.0 because **64 entries in
`src/test/resources/api/public-api-2.0.0.txt` name this class or one of its seven nested types**
and `PublicApiIsAdditiveOnlySinceReleaseTest` asserts released ⊆ current by design.

CORRECTED 2026-08-19. This was published as "34 members" in three places — here, `CHANGELOG.md`
and `FileFinder`'s class javadoc — and the figure does not reproduce against the file it cites.
MEASURED: 64 entries, being 8 TYPE, 4 CTOR, 23 FIELD and 29 METH; 9 of them are declared directly
on `FileFinder` itself (1 TYPE plus the eight static methods) and the other 55 belong to the
nested types `FileLocation`, `FileLocation.Type`, `FileMetadata`, `Statistics`, `Config`,
`FileFinderException` and `Util`. No subsetting of the baseline yields 34. The conclusion was
right and understated. All four figures are now asserted by
`src/test/java/io/github/pierce/gates/FileFinderBaselineFootprintTest.java`, so they cannot drift
from the file again. (A 2.1.0 review that caught the 34 reported the direct-member sub-figure as
12; that does not reproduce either — it is 9.)

**The verdict, and it is stronger than "1,257 lines for three call sites."** `FileFinder.Config` is
public API that configures nothing. Every field is private with no accessor except
`getAllSearchPaths()`; `searchPaths` is a fixed-size `Arrays.asList`; and the only construction
anywhere in `src/` is `INSTANCE = new FileFinder(new Config())` inside `getInstance()`. So
`maxFileSize`, `validatePaths`, `allowedExtensions`, `maxSearchDepth` and the search-path list are
not defaults — they are hard-coded constants wearing the costume of configuration, and `CTOR public
...FileFinder.Config()` is a baseline entry for a type that constructs an object nothing consumes.
That is what made the 2.0.0 `SecurityException` message ("disable validatePaths if the input is
trusted") an instruction no caller could follow; the message was rewritten in 2.1.0.

**Still true at 2.1.0, and not fixable additively:**

- A single static `findFile` call constructs a singleton that starts two thread pools nothing ever
  shuts down. Both are daemon threads as of 2.1.0, so the JVM-exit hazard is gone, but there is
  still no lifecycle, no `close()`, and no way to have two differently-configured finders.
- `performDeepSearch` still walks the working directory to depth 5 on every miss. Making it opt-in
  needs a `Config` setter, which is additive **and inert**, because `Config` cannot be injected.
- `DeepFileSearcher.currentDepth` is incremented in `preVisitDirectory` and decremented in
  `postVisitDirectory`, so it tracks a running count of open directories rather than the depth of
  the current node; the `currentDepth > maxDepth` guard therefore fires on breadth. The real bound
  is the `maxDepth` argument to `walkFileTree`. Delete the counter.
- `initializeEnvironmentPaths()` probes the CWD for `mvnw`, `.idea` and `.project` at `Config`
  construction, so the search-path set depends on where the JVM happened to be started. A Spark
  driver launched from a directory containing `.idea` searches `out/production/classes`; one
  launched elsewhere does not.
- `maxContentCacheBytes` is 50 MB while `maxFileSize` is 100 MB, so any file between the two is
  loaded, returned, and immediately evicted by Guava's `maximumWeight`. The content cache is pure
  overhead for exactly the sizes where caching would matter most.

**Recommended 3.0.0 shape:** delete `FileFinder`, keep `SchemaFiles`; or extract the finder into a
separate optional artifact for callers who genuinely want fuzzy discovery. `docs/audit/ROADMAP.md`
already reached the same conclusion at Phase 4 — line 130 says the fix means changing FileFinder
"from a static singleton with hidden global lifecycle into an instantiable AutoCloseable component
— an API change, not a patch" — and that conclusion had never been carried into this file.

---

### [BL-018] `extractFieldsPreservingStructure` has the array-element misalignment that the other two sites had — **FIXED IN 2.1.0**

**CLOSED 2026-08-19 with a measurement, not a tick.**

REPRODUCED EXACTLY AS FILED. `{"g":[[{"a":1}],[{"b":2}]]}` emitted `g_a="[[1]]"` beside
`g_b="[[2]]"`, so `b` — which came from outer position 1 — sat at index 0. A consumer zipping the
two columns by outer index read `a=1` and `b=2` as one nested group. They came from different
groups: silent corruption from a well-formed document, the same class as the discount landing on
the wrong line item that motivated the repair at the other two sites.

**BOTH HALVES OF THE DEFERRAL REASON WERE DISCHARGED, not overruled.**

- *"Bundling it would make the seven-row corpus diff impossible to attribute."* A sequencing
  reason, and it was a good one. Spent: this was a dedicated pass and the diff is ONE row.
- *"It would place a bare `null` where a nested LIST has always been, a shape no reconstructor has
  been exercised against."* Correct, and it SELECTED the filler rather than blocking the fix. The
  hole at this site is an inner list of the outer position's inner cardinality — `columnFor`'s own
  rule one level down — so no column of lists gains a bare null. A bare null appears only where
  the outer position holds no nested list at all, which is one already-DEFECT shape rather than
  every nested array.

**MEASURED CORPUS REACH: three fixtures, one moved.** All 161 inputs were enumerated.
`structural/mixed-nested-array-sentinel-collision` is re-recorded and stays `DEFECT` — the
positional half is closed, the sentinel-maps-to-the-base-key half and the `setNestedValue`
leaf-versus-branch collision are not.
`structural/array-of-arrays-with-empty-inner` and
`schema/enriched-gavro-parity-diverges-on-an-array-of-arrays-of-records` were verified unchanged
rather than assumed; the first is the load-bearing pin that rules out filling holes with `[]`.
Counts stay 56 / 24 / 81.

**ONE THING THE ANALYSIS GOT WRONG, and it mattered.** It described the index clamp at
`AvroReconstructor.reconstructNestedArrayOfRecordsAtIndex` as a live silent duplicator being fed
by this site. It is not: `agreedElementCount` refuses a disagreeing column count first. See the
[BL-013] correction above for the trace. The clamp is removed as unreachable defence and the
refusal is pinned; no behaviour changes from that half.

**A SECOND DEFECT FOUND WHILE FIXING THE FIRST, one branch away.** The Java-array arm had no
empty check at all, so an empty `Object[]` as a nested element vanished from every column and
everything after it shifted left — while the List arm explicitly preserved the position. It has
its own named test.

**CORRECTED 2026-08-19:** this paragraph called that "the one key-set change in the whole fix",
and CHANGELOG item 22 said the branch was reachable only from a `Map` source. Both are wrong.
The same branch fires whenever an inner list flattens to no columns, which plain JSON reaches
with an inner list of empty objects — `{"g":[[{}]]}` gains a `g` column. The branch also
collapsed inner cardinality 0, 1, 2 and 3 into one output, which is CHANGELOG item 27.

Filed as a follow-up rather than folded in: the `_value` sentinel maps to the BASE key rather than
to a `_value`-suffixed column, which is what kept
`structural/mixed-nested-array-sentinel-collision` a DEFECT after this repair. **RESOLVED
2026-08-19 — see [BL-022], where the rename was REFUSED on a trace and the residual defect was
retargeted to the reconstruct side as [BL-023].** The clause that used to sit here, "what the
class javadoc has always mis-documented", was already false when written: commit 6bb66d1 — the
BL-018 padding commit itself — rewrote that javadoc block to describe the measured output.

---

### [BL-022] The `_value` sentinel maps to the base key — **CLOSED 2026-08-19: the base key is the contract, the suffix is REFUSED** ✅

**WHAT SHIPPED, in one paragraph, so nobody has to reconstruct it from the trace below.** No
emitted key changed: `data`, `grid_rows`, `mixed` and `attachments` are all byte-identical to
`2.0.0`. The suffix rename was refused on a trace, not deferred, and the two documents that would
have broken under it are now corpus rows rather than paragraphs
(`structural/mixed-array-sentinel-vs-user-value-field` and
`structural/nullable-nested-record-shadowed-json-stack`). The data loss the entry was filed
against was real but lived one layer downstream, in `JsonReconstructor`, and was split out and
fixed as [BL-023]. The published contract for the sentinel's column is *the emitted key set is
`{P, P_field}`, two columns of ONE array to be read together* — deliberately not "`P` is a leaf",
because that form goes stale the day a reconstruct-side zip lands. Corpus at close: **166
fixtures, 58 / 25 / 83**. Two things this entry raised are NOT closed and are restated at the
bottom: the schema-side key-set gate, and the zip repair that would make the colliding rows
`LOSSLESS`.

**THE PREMISE THIS ENTRY CARRIED FOR SIX PASSES WAS FALSE, and it was false in this file.** The
entry opened by asserting, in the present perfect, that the `MapFlattener` class javadoc *has
documented* the suffixed pair since before 2.0.0. Commit 6bb66d1 rewrote that javadoc block; at HEAD the
javadoc describes the measured output. The stale claim survived here, and in the fixture's own
`rationale` field, and every pass since inherited it from one of those two places. Both are
corrected, and `SentinelKeyProseMatchesTheCodeTest` now bans the two exact sentences by substring
so the next pass cannot inherit them again. `CHANGELOG.md` is exempt by path: its tables record
what was believed and published at the time, and rewriting them to match today would destroy the
evidence rather than the error.

**What the code does, measured.** `VALUE_SENTINEL = "_value"` is an INTERNAL column name that
never reaches output under that spelling. There are TWO emit sites, not one: `flattenList` Case 2
(nested arrays), where the sentinel is mapped by `fieldKey = key`; and `flattenList` Case 3 (array
of maps), where a non-map element is written under the base key by `columnFor` and the constant is
not consulted at all. **Case 3 is a site this entry never named.** For
`{"data":[[{"name":"A"}],"text"]}` the key set is exactly `{data, data_name}`.

**Option (a), emit `data_value` — REFUSED ON A TRACE, not a preference.** `joinEncodedKey` extends
an already-encoded path and does not re-escape it, so a user field literally named `value` produces
the segment `value` and the key `data_value`. A suffixed sentinel produces the identical string:

| document | today | under the rename |
| --- | --- | --- |
| `{"data":[[{"value":"A"}],"text"]}` | `data_value` and `data`, both kept | one `result.put` overwrites the other — a whole column lost at FLATTEN time |
| `{"mixed":[{"value":1},"x"]}` | `mixed_value="[1,null]"` beside `mixed="[null,\"x\"]"` | `columnFor` returns the SAME column — `mixed_value="[1,\"x\"]"`, the distinction destroyed inside one column |

The rename does not resolve the collision by construction; it MOVES it from the reconstruct side,
where a schema-aware reader demonstrably recovers, to the flatten side, where the bytes are gone.
No suffix escapes this: `FlattenedPath` is a bijection over user segment lists with no reserved
namespace, `_value` escapes to `\_value` — exactly what a user field named `_value` produces — and
the only unforgeable form `__value__` is dropped outright by `JsonReconstructor.processArrays`.
Separately, a blanket rename touches every array-of-arrays-of-scalars: `{"grid":{"rows":[[1,2],[],[3]]}}`
emits `grid_rows` today, a sentinel-sourced base key with **no sibling and no collision**, and
would become `grid_rows_value` in released 2.0.0 output for a shape with no defect at all. It also
desynchronises the data column name from the SCHEMA column name — `AvroSchemaFlattener` and
`GAvroSchemaFlattener` both emit `basePath` for array-of-primitives and array-of-arrays — which is
a silent Avro/Spark field-not-found rather than a loud failure. Scoping the rename to "only when a
sibling column exists" is strictly worse: the column name would then depend on the sibling key set,
so adding one map element to an array silently renames the scalar column and no schema generator
could predict it.

**Option (c), a configurable suffix defaulting to the base key — REFUSED.** A knob whose default is
the bug leaves the defect shipped for everyone who does not read the release notes, and its
non-default value emits a column name the two Avro schema flatteners do not produce — so it offers
two defects rather than a fix and a status quo. Precedent is specific: e6451dd's headline was
wiring four knobs that did nothing. `PublicApiIsAdditiveOnlySinceReleaseTest` would have permitted
the builder method; the API rule was never the obstacle.

**Also considered and refused:** a runtime collision-avoiding suffix (probe `_value`, then
`_value_2`, …). Deterministic per document but not per schema, so the same logical column gets
different names in different rows of one table. Unusable for Athena, Spark or Avro, which is the
published use case.

**Option (b), keep the base key — TAKEN.** No emitted key changes. The published contract is
stated as *the emitted key set is `{P, P_field}`, two columns of ONE array to be read together* —
deliberately NOT as "`P` is a leaf", because that second form goes stale the moment a
reconstruct-side zip lands.

**"The base-key choice causes the silent data loss" — OVERSTATED, and measured.** Decoding every
flat key in the 164 fixtures the corpus held at `24dc5a5` into segment lists and searching for a
key whose path is a strict
prefix of another gives **8 rows, of which only 5 involve the sentinel**. Re-measured over all
**166** fixtures with the production `FlattenedPath` decoder once this release's two new rows had
landed: **10 rows, 6 of them sentinel-involved** — one row of each kind was added, so the ratio is
essentially unchanged and the conclusion is not sensitive to the corpus growing. Three of the
original eight reproduce the identical leaf/branch collision with no sentinel anywhere:

* `avro/avro-array-of-records-nullable-nested-record-shadowed` — `orders_ship` beside
  `orders_ship_city`, pure user data, DEFECT;
* `avro/avro-array-element-multi-branch-union-mixed-branches` — `items_meta` beside
  `items_meta_src`, and **LOSSLESS**, because the Avro reconstructor is schema-aware. Proof that
  the colliding key set is recoverable and that emit is not at fault;
* `naming/lower-case-collision-suffix-corrupts-structure` — `id` beside `id_2` from the
  `LOWER_CASE` disambiguator.

Renaming the sentinel would repair the KEY SET of 5 rows and leave 3 emitting byte-identical
collisions. It is therefore not the repair for the loss. The loss is retargeted to [BL-023].

**Undocumented shape found while tracing, now characterised.** `isNestedList` is false for a Map,
so a MAP at an outer position beside a nested list is never field-extracted: `{"mixed":[{"a":1},[2,3]]}`
emits ONE column `mixed` whose slot 0 is the JSON TEXT of the map, and there is no `mixed_a`. The
base-key column is not type-homogeneous — a second, independent reason the name `_value` would
misdescribe it. Zero fixtures reached this shape and zero tests asserted it;
`MapFlattenerSentinelKeyContractTest` now does.

**`cannotCatch` was self-contradictory and is rewritten.** The fixture instructed that "the fix
must not be to edit the javadoc to match the code". Commit 6bb66d1 did exactly that, and did it
well — it kept the discrepancy legible rather than erasing it. An instruction that is already
violated by a good change needs rewriting, not obeying.

**Still open, filed rather than folded in — the two things this closure does NOT cover:**

1. **The schema-side key-set gate.** For 3.0.0, if the rename is ever revisited, the schema side
   must move in the same commit (`AvroSchemaFlattener` ARRAY case, `GAvroSchemaFlattener`
   array-of-primitives and array-of-arrays). Nothing in the current gate set flattens a schema and
   a document of the same shape and compares key sets. That gate is worth adding regardless of
   which option wins, because a desynchronised data column and schema column is a silent
   field-not-found rather than a loud failure.
2. **The zip repair.** The two columns of one array are recoverable by zipping the base column
   with the field columns by outer index, and that is not implemented. Until it is, the colliding
   corpus rows stay `DEFECT` and no `CollisionPolicy` member returns the source document. Tracked
   under [BL-023].

---

### [BL-023] `JsonReconstructor.setNestedValue` resolved a leaf/branch key collision by map iteration order — **FIXED IN 2.1.0** ✅

Split out of [BL-022] because it is an independent defect with three producers, only one of which
is the flattener sentinel — and because leaving it filed as a sub-item guaranteed it would be
deferred whenever the rename was deferred.

**The defect.** A flattened map can hold `a` beside `a_b`. The first decodes to `["a"]`, the second
to `["a","b"]`, so `a` is both a leaf and an intermediate of the same tree. `buildHierarchy`
iterated `flattenedMap.entrySet()` in the caller's order and let whichever key arrived LAST decide,
with no log, no exception and no marker. **Three outcomes were reachable, all measured against
unfixed HEAD:**

| input | output before the fix |
| --- | --- |
| `{"a_b":"1","a":"2"}` | `{"a":"2"}` — subtree DESTROYED |
| `{"a":"2","a_b":"1"}` | `{"a":{"_value":"2","b":"1"}}` — key FABRICATED |
| `{"a":"scalar","a_\_value":"real"}` | `{"a":{"_value":"real"}}` — the scalar destroyed, and the survivor byte-identical to the fabrication |
| `{"a":null,"a_b":"1"}` | `{"a":{"b":"1"}}`; reversed, `{"a":null}` — a null leaf took a third path and lost data in both directions |

The corpus already proved the order-dependence without anyone running anything:
`structural/heterogeneous-array-object-first` and `structural/heterogeneous-array-scalar-first`
differ only in the element order of the source array and record structurally different
reconstructions.

**Three producers of an unescaped separator, only one a sentinel.** `MapFlattener`'s
`VALUE_SENTINEL` base-key mapping; an ordinary nullable nested record inside an array of records —
`{"orders":[{"id":1,"ship":{"city":"NY"}},{"id":2,"ship":null}]}` emits `orders_ship` beside
`orders_ship_city` and returned `{"orders":[{"id":1,"ship":null},{"id":2,"ship":null}]}`, the
nested record deleted from a completely ordinary document; and the `LOWER_CASE` dedup suffix. A
caller-built flat map is a fourth, and `reconstruct(Map)` is public and accepts any map, so the
reconstructor has to be correct on input it did not produce.

**What does NOT reproduce, which bounds the claim honestly.** An ordinary field name containing the
separator does not collide: `FlattenedPath` escapes it, `a\_b` is one segment, and
`SeparatorInFieldNameRegressionTest` pins that. The collision needs a producer that writes an
UNESCAPED separator into a key.

**The fix.** The colliding keys are computed BEFORE any write as
`flattenedMap.keySet() ∩ analysis.allPaths` — a set intersection, so the answer cannot depend on
iteration order. `allPaths` was already populated at `analyzeStructure` and read nowhere: the
detector was built and switched off. The default policy REFUSES with
`JsonReconstructor.KeyCollisionException`, naming the key, everything it shadows, and the escaped
form that would have disambiguated. `Builder.onKeyCollision(PREFER_LEAF | PREFER_BRANCH)` keeps
going but drops the same side every time and says so at WARN. The `_value` wrapper is DELETED, not
kept as a fallback — keeping it would have preserved the exact nondeterminism the detection exists
to remove — and the branch it occupied now throws, so a future gap in detection fails loudly
instead of fabricating a key.

**Why `FAIL` is the default in a minor.** `ArrayParseException` was added in this same 2.1.0 cycle
for the same reason on the same class, so the precedent is not merely analogous, it is the
immediately preceding decision. The alternatives all discard one side of a collision; a caller who
wants that can ask for it by name. Consumers upgrading who hit the throw are consumers who were
already losing data silently.

**Not the fix, and rejected explicitly so it is not re-proposed:** merging into a node that holds
both. JSON has no node that is a scalar and an object at once, so any merge must invent a key to
hold the scalar — which is the `_value` wrapper, which is the defect. It is not a fourth option.
Sorting the flat map before iterating is worse than all of them: a stable wrong answer is harder to
notice than an unstable one.

**THE FIRST VERSION OF THE FIX SHIPPED TWO SILENT PATHS OF ITS OWN, found in adversarial review of
`bd5b070` and repaired in the follow-up commit.** They are written up here rather than folded into
the paragraph above, because a repair that reintroduces the class it removes is the single most
useful thing this entry can record. The backstops in `setNestedValue` tested the SHAPE OF THE
CALLER'S VALUE — `existing == null` on the way down, `occupant instanceof Map && !(value instanceof
Map)` at the leaf — so the answer depended on the runtime type of a value the caller supplied.
Measured at `bd5b070` through the public `reconstruct(Map)` at the default policy:

| flattened map | `bd5b070` | now |
| --- | --- | --- |
| `{"a\b_c":"KEEP-ME","a\b":{}}` | `{"a\b":{}}` — KEEP-ME gone, no exception, no WARN | `KeyCollisionException` |
| `{"a\b":{},"a\b_c":"KEEP-ME"}` | `{"a\b":{"c":"KEEP-ME"}}` — merged into the caller's own map | `KeyCollisionException` |
| `{"a\b":null,"a\b_c":"1"}` | `{"a\b":{"c":"1"}}` — the null leaf gone | `KeyCollisionException` |
| `{"a\b_c":"1","a\b":null}` | `KeyCollisionException` | `KeyCollisionException` |

Three silent answers and one throw, under a javadoc that asserted both orders were covered. The
reach was narrow — `MapFlattener` always emits canonical keys, so only a hand-built map reaches
it — but `reconstruct(Map)` is public and that is the whole reason the backstops exist. **The
pinning test only ever used the String leaf `"scalar"`, which is why a value-shaped hole survived
a test written to close it**; it now runs both orders over a String, a `null` and a `Map`.

**The repair is to stop asking about shape and start asking about identity.** `buildHierarchy`
carries one `IdentityHashMap`-backed set of the nodes THIS WALK created. A descent is allowed only
into a node the walk built; a leaf write is refused only when a node the walk built is already
there; neither question consults the caller's value. One set per `reconstruct`, one entry per
created node — not per key — so the hot path pays a reference add rather than a string encode.

**A second defect went with it: `reconstruct` used to MUTATE the map it was handed.**
`current = (Map) existing` adopted a caller's Map value as the write target, so a longer key wrote
into an object the caller still owned — `{"zz":9}` came back `{"zz":9,"c":1}` — which also made
two calls on the same input return two different answers. Never adopting a caller's Map fixes both
at once. `reconstructDoesNotMutateTheMapItWasHanded` pins it in the collision case and in the
ordinary no-collision case.

**NO POLICY PRESERVES BOTH COLUMNS, and no fourth member could.** Stated here because the deleted
`_value` wrapper accidentally did, for a subset of documents, and a `2.0.0` caller who was reading
`_value` to salvage the columns now gets an exception with no equivalent option. Measured at HEAD
for `{"mixed":["S",{"k":"V1"},{"k":"V2"}]}`, which flattens to
`{mixed=["S",null,null], mixed_k=[null,"V1","V2"]}`:

| policy | returns | values lost |
| --- | --- | --- |
| `FAIL` | `KeyCollisionException` | none — nothing is produced, the input is intact |
| `PREFER_LEAF` | `{"mixed":["S",null,null]}` | `V1`, `V2` |
| `PREFER_BRANCH` | `{"mixed":[{"k":null},{"k":"V1"},{"k":"V2"}]}` | `S` |

**RELAYED, NOT RE-MEASURED HERE:** adversarial review reports that over 892 colliding fuzz seeds
the `24dc5a5` build returned every scalar leaf value for **496** of them (56%) in the mangled
`{"mixed":{"_value":"[…]","k":"[…]"}}` shape, and had already lost at least one value for the
other 396. That harness is not in this repository and the figure has not been reproduced here; it
is recorded because it sizes what the deletion cost, not because it has been verified. A fourth
policy is REFUSED for the [BL-022] reason: `FlattenedPath` is a bijection over user segment lists
with no reserved namespace, so any node holding both sides must name the scalar under a key a user
field can already occupy — which is the `_value` wrapper. Instead `KeyCollisionException` now says
so in its message and points at `getCollidingKey()` and `getShadowedKeys()`, which name both sides
so a caller can zip them out of the map they still hold under a key of their own choosing. The
zip repair that would make the corpus rows `LOSSLESS` remains unimplemented.

**Still open.** `AvroReconstructor` is NOT in this fix's blast radius — `PathNode.addPath` sets the
leaf value and the children side by side, so neither write can clobber the other and the tree is
order-independent by construction. It still drops one side on a genuine collision (measured:
`{orders=[]}` for the shape above), which is the three AVRO corpus rows, and it does so
deterministically and without fabricating a key. `AvroReconstructorKeyCollisionParityTest` is the
fence that catches a leak.

---

### [BL-024] The `LOWER_CASE` dedup suffix emits an UNESCAPED separator into an encoded namespace

Found while fixing [BL-023] and filed rather than folded in, because it is a FLATTEN-side defect in
a different class from the reconstructor's handling of collisions.

`transformKeys` calls `generateUniqueKey` AFTER encoding, and `generateUniqueKey` appends a raw
separator with no escaping. `{"Id":1,"id":2}` under `namingStrategy(LOWER_CASE)` folds to one name
and the disambiguating suffix becomes the key `id_2`, which the decoder reads as a nesting level.
Case folding is an ACCEPTED consequence of the requested strategy; manufacturing a key that cannot
round-trip is not.

[BL-023] makes this LOUD rather than corrupting — the reconstructor now refuses `{id, id_2}`
instead of returning `{"id":{"2":2,"_value":1}}` — which may be enough for most callers, but the
strategy is still emitting a key it cannot round-trip and
`naming/lower-case-collision-suffix-corrupts-structure` will stay `DEFECT` until it stops. The fix
is to escape the suffix (`id\_2`, one segment) or to append it before encoding. Deliberately NOT
done in the [BL-023] commit: it is a key-set change in released output and deserves its own
decision, and a reconstruct-side repair that papered over it would be worse than either.

Its fixture exists and is already `DEFECT`, so no new fixture is owed.

**ALSO FOUND WHILE FIXING [BL-023], NOT FIXED HERE.** `MapFlattener.applyNamingStrategy` used the
DEFAULT locale for all three case conversions, so the emitted COLUMN NAME was a function of the
JVM the job ran on — under `tr-TR`, `toLowerCase()` maps `I` to the dotless `ı`, and an `ID`
field became the column `ıd` on a Turkish executor and `id` everywhere else. That one IS fixed,
pinned to `Locale.ROOT`, and it is what paid for the SpotBugs cost of the [BL-023] WARN. The same
hazard survives elsewhere and is deliberately left for its own change rather than folded in:
`converter/SchemaBasedMapConverter` builds a case-insensitive field-name lookup with
`fieldName.toLowerCase()` at four sites, which makes SCHEMA FIELD MATCHING locale-dependent in
exactly the same way — a Turkish executor would fail to match a field named `ID`. It is a
different class of consequence (a missed field rather than a renamed column) and it belongs to the
converter package, so it is named here and filed rather than repaired in a reconstructor commit.

**The locale pin now has a behavioural test, which it did not when it shipped.** Adversarial
review measured that reverting the three `Locale.ROOT` arguments and running the whole suite gives
**2684 tests, 0 failures, BUILD SUCCESS** — nothing noticed a released-output column rename. The
only thing that did was SpotBugs `DM_CONVERT_CASE`, and that is a proxy: it fires on any
locale-less case conversion anywhere in the class, pins no key, and is silenced by any refactor
that routes case conversion through a helper. `MapFlattenerNamingLocaleTest` sets the default
locale to `tr-TR` and asserts the emitted keys by CODE POINT. Drilled: with the pin reverted it
fails `expected: <U+0069 U+0064> but was: <U+0131 U+0064>`.

---

### [BL-025] `reconstructArray` duplicates one column's whole serialized text into every element

**Found by adversarial review of `bd5b070` while checking whether `PREFER_BRANCH` fabricates
values. It does not — this does, and it reproduces with NO collision anywhere.** Filed rather than
fixed because it is a different defect in a different method from the collision work, and because
it is already a standing corpus fact that would go red the moment it is repaired.

Measured identically at `24dc5a5` and at `bd5b070`, so it is not a regression of either pass:

```
in   {"orders":[{"id":1,"ship":{"city":"NY"}},{"id":2,"ship":{"city":"LA"}}]}
flat {orders_id=[1,2], orders_ship_city=["NY","LA"]}
out  {"orders":[{"id":1,"ship":{"city":"[\"NY\",\"LA\"]"}},
                {"id":2,"ship":{"city":"[\"NY\",\"LA\"]"}}]}
```

Every element's `city` holds the ENTIRE column text, as a string. The cause is in
`reconstructArray`: the nested object under `ship` is not recognised as a per-element column, so
`parsedValues` is a one-element list holding the raw serialized text, and the shorter-column
fallback (`values.get(values.size() - 1)`) then replicates that one value into every index. It is
the same fallback the corpus row `structural/array-element-with-nested-object-duplicated` was
written to isolate — "That fallback invents data rather than failing" — and that row is `DEFECT`
today, so no new fixture is owed and repairing this turns the build red by design.

**Why it matters beyond its own row.** `PREFER_BRANCH` steers callers into it. The member's whole
proposition is "keep the subtree", and for the nullable-nested-record shape the subtree it keeps is
this one: `{"orders":[{"id":1,"ship":{"city":"NY"}},{"id":2,"ship":null}]}` under `PREFER_BRANCH`
returns a `city` of `["NY",null]` on BOTH elements, including the one whose `ship` was `null` in
the source. The javadoc on `CollisionPolicy.PREFER_BRANCH` now says so and names the corpus row;
before this it said only "Keep the SUBTREE and discard the colliding short key."

**The fix, when it is taken:** the same zip-by-outer-index repair [BL-023] owes the collision rows.
Padding or last-value replication produces a schema-valid record that is wrong, which is what
`AvroReconstructor.ArrayCardinalityException` already refuses to do on the Avro stack — the two
sides should agree, and today the JSON stack fabricates where the Avro stack throws.

---

## Medium Priority

### [BL-007] Investigate and Resolve JsonReconstructor — CLOSED 2026-08-17, PREMISE REFUTED ✅
- **Type:** Chore
- **Priority:** Medium
- **Effort:** M
- **Related Concern:** C-001
- **Affected Files:** `src/main/java/io/github/pierce/JsonReconstructor.java` (was `src/main/groovy/.../JsonReconstructor.groovy` when filed)
- **Original description (FALSE, retained so the correction is legible):** "The entire
  JsonReconstructor class (~1294 lines) is commented out."
- **MEASURED 2026-08-17, and the premise does not reproduce.** The class is **1295 lines of live,
  compiled, exported Java**. It has 72 `//` comment lines and **zero** commented-out code lines —
  a scan for lines beginning `//` followed by `package|import|public|private|protected|class|
  static|final|return|if|for|while|}|@` returns 0 hits. Every `//` line is a section banner, a
  step comment, or the four-line note about Groovy `\$` escaping. There is nothing to delete and
  nothing to uncomment.
- **Resolution:** closed under acceptance criterion **(b)**, uncommented and tested — which had
  already happened, by `8433386` (revival), `25621f6` (wired to the shared `FlattenedPath`) and
  `19ff557` (ported to Java). Coverage is 45 `@Test` in
  `src/test/java/io/github/pierce/JsonReconstructorTest.java`, plus roughly 40 fidelity fixtures
  that run through it.
- **The ROADMAP hazard it was blocked on is also retired.** `docs/audit/ROADMAP.md:162` warned
  that reviving this class "as originally written" would create a fourth key-encoding convention.
  It was not revived as written: `25621f6` wired it to `FlattenedPath`, and the file calls
  `FlattenedPath.decodeSegments`, `FlattenedPath.encode` and `FlattenedPath.escapeSegment`. The
  single-encoding invariant holds.
- **Superseded findings:** audit findings `NP-009` and `OSS-16` recommended deleting both
  `JsonReconstructor` and its test outright. Both are obsolete and now actively wrong — the class
  is public in a released artifact. They must not be acted on.
- **What is genuinely still open in this file** is unrelated to the commented-out claim and is
  tracked separately as [BL-013] and [BL-014] below.

---

## Low Priority

### [BL-008] Clarify Flattener Naming Convention — CLOSED 2026-08-18 ✅

**Closed by the "Which flattener do I use?" table in README.md**, which names all SIX (the original
filing listed two) and splits them into three DATA flatteners and three that flatten a SCHEMA.
CORRECTED 2026-08-19: the first version of the closure note, and the README sentence above the
table, both said the three schema flatteners "never touch a record". Two of them do not.
`GAvroSchemaFlattener.applyTypes(Map, Map)` does — its own javadoc calls it "the hot path method
called for every record in streaming", and it reads the flattened DATA map, converts each value
and substitutes defaults. `AvroSchemaFlattener` (three `getFlattenedSchema` overloads plus
`getMapFieldPaths`) and `EnrichedSchemaFlattener` (`options`, `flatten`, `stream`) have no data
method at all, so the categorical claim was false for exactly one of the three. Measured
relationships rather than assumed ones: `JsonFlattener` holds a `MapFlattener` field and is a
facade over it; `JsonFlattenerConsolidator` contains **no** reference to `MapFlattener` and is an
independent Jackson implementation, so its output is not guaranteed to match and the corpus does
not cover it; `GAvroSchemaFlattener` is independent of `AvroSchemaFlattener` despite the name, its
only mention of it being a javadoc example that would not compile. Renaming is breaking and stays
filed under [BL-016].

Original filing follows.

- **Type:** Documentation
- **Priority:** Low
- **Effort:** XS
- **Related Concern:** C-002
- **Affected Files:** README.md, class JavaDocs
- **Description:** The naming of JsonFlattenerConsolidator vs JsonFlattener is confusing since they serve different purposes. (Filed when one was Java and the other Groovy; both are Java now, which removes the only cue distinguishing them and makes this *more* worth doing, not less.) JsonFlattener is actually a fluent wrapper around MapFlattener. Consider adding clarifying documentation or renaming for clarity.
- **Acceptance Criteria:** README section explaining the purpose and relationship of each flattener class
- **Discovered:** Session 2

### [BL-009] Add Flattener Family Diagram — **CLOSED 2026-08-19**

`docs/ARCHITECTURE_GRAPH.md` now carries `EnrichedSchemaFlattener` and `SchemaFiles`, both of
which were absent while the classes they replace were drawn, and every class-to-class edge in it
is asserted against the source by
`src/test/java/io/github/pierce/gates/ArchitectureGraphEdgesAreRealTest.java`. That gate found
five edges that had silently rotted: `ASF --> FileFinder` (falsified by the 2.1.0 repoint),
`GASF --> ASF`, `CSFS --> ASF`, `NPSP --> NPF` and `ASC --> TCR`, plus fifteen registry rows
claiming the converters implement `TypeConverter` directly when they extend
`AbstractTypeConverter`.

**The ORIGINAL ask is now met.** README carries a mermaid flowchart directly above the
"Which flattener do I use?" table, in three lanes by WHAT IT FLATTENS — data, schema only, schema
plus per-record casting — with each node stating what it takes, what it emits, whether it is the
default choice, whether it is legacy, and which fidelity stack covers it. It is gated by
`src/test/java/io/github/pierce/gates/FlattenerFamilyDiagramTest.java`, which asserts four things:

1. the set of names in the diagram EQUALS the set of top-level `src/main` types whose simple name
   contains "Flattener" — so adding a seventh flattener and not drawing it turns the build red;
2. every `A --> B` edge resolves to a dependency the source actually has, word-boundary matched so
   `AvroSchemaFlattener` does not match inside `GAvroSchemaFlattener` (which is exactly the false
   edge `docs/CLASS_REGISTRY.md` asserted);
3. each node's corpus-coverage marker agrees with whether the fidelity harness names that class —
   measured, `JsonFlattenerConsolidator` is named by ZERO files under
   `src/test/java/io/github/pierce/fidelity/` while the other five are named by 1 to 8, and README
   already claimed that in prose with nothing checking it;
4. a mutation drill: renaming a class, deleting a node and inventing a seventh each fail, and each
   mutation is asserted to have actually applied so the drill cannot prove nothing.

**WHERE IT LIVES, AND WHY NOT IN `ARCHITECTURE_GRAPH.md`.** Putting it there would have inherited
`ArchitectureGraphEdgesAreRealTest` for free, and that is a trap: that gate builds ONE alias map
for the whole file, last write wins, so a second diagram reusing `MF[…]` or `ASF[…]` with a
different label would silently retarget the FIRST diagram's edges while the gate kept passing. A
new silent failure of exactly the class this repository keeps hitting. `ARCHITECTURE_GRAPH.md`
carries a cross-reference instead, and the new gate reuses the technique rather than the file.

**A PREMISE IN THE TASK THAT DID NOT HOLD.** The brief asked the diagram to say "which of the TWO
round-trip stacks" each flattener belongs to. There are FOUR stack keys in
`src/test/resources/fidelity/manifest.json`, and the letters COLLIDE with the audit register's:
the manifest's Stack A is `MapFlattener`, while `docs/audit/FINDINGS.md` NP-001 calls
`MapFlattener` Stack B. The diagram uses the manifest's letters — it is the published contract —
and says so in a caption, because drawing "Stack A" without saying whose would have published a
new false claim inside the artefact commissioned to remove false claims.

**"Corpus-covered" and "in a stack recipe" are kept as SEPARATE claims.** `EnrichedSchemaFlattener`
and `GAvroSchemaFlattener` are exercised by the harness but named in no published stack recipe;
collapsing the two would publish a false negative about the class README calls the default choice
for schemas.

- **Type:** Documentation
- **Priority:** Low
- **Effort:** S
- **Related Concern:** C-002
- **Affected Files:** README.md, docs/
- **Description:** Create a visual diagram showing the relationship between all flattener classes: JsonFlattenerConsolidator, JsonFlattener, MapFlattener, and their different use cases.
- **Acceptance Criteria:** Mermaid diagram in README showing class relationships and when to use each — **MET**
- **Discovered:** Session 2
- **Completed:** 2026-08-19

---

## Ideas / Future Consideration

### [BL-002] API Reference Documentation
- **Type:** Documentation
- **Priority:** Low
- **Effort:** M
- **Related Concern:** N/A
- **Affected Files:** docs/
- **Description:** Generate comprehensive API reference documentation (Javadoc)
- **Acceptance Criteria:** Published API docs for all public classes
- **Discovered:** Session 1

> **Note added 2026-08-19.** A 2.1.0 review reported this entry as tracking
> `docs/API_SURFACE.md`. It never did — this entry is about generating javadoc, and says nothing
> about that file. `docs/API_SURFACE.md` has been DELETED rather than re-tracked: it was
> hand-written, generated by nothing, read by no test, and still advertised `<version>1.0.5</version>`
> against a `2.1.0-SNAPSHOT` pom, `DataStreamWriter processStream(...)` against a method that
> returns `ProcessingResult`, and three `NexusPiercerPatterns` methods that have never existed.
> Those are OSS-01, OSS-09 and the release-tagging finding in `docs/audit/FINDINGS.md` — known
> false and still published. The generated javadoc (`./mvnw javadoc:javadoc`, which builds
> clean) and `src/test/resources/api/public-api-2.0.0.txt`, which IS generated and IS gated, are
> the two artefacts that survive. If a human-readable inventory is wanted later, generate it from
> the same `PublicApiSurface` extractor and gate it; do not hand-write one again.

---

## Completed
*Moved here when addressed*

| ID | Title | Completed | Commit | By |
|----|-------|-----------|--------|-----|
| BL-003 | Remove org.json dependency (license compliance) | 2025-12-08 | *(no SHA recorded)* | Session 2 |
| BL-004 | Dependency Hygiene & Modernization | 2025-12-08 | *(no SHA recorded)* | Session 3 |
| BL-001 | Document Java vs Groovy Implementation Choice | 2026-08-11 | `4001d3b` | Groovy toolchain removal |
| BL-007 | Investigate and Resolve JsonReconstructor — **premise refuted, closed** | 2026-08-17 | see below | SpotBugs exclude-block removal |
| BL-010 | JsonFlattener is dead public surface | 2026-08-17 | see below | SpotBugs exclude-block removal |
| BL-008 | GAvroSchemaFlattener: unknown responsibility | 2026-08-18 | see below | measured, premise refuted |
| BL-012 | reconstruct() vs reconstructToMap() | 2026-08-18 | see below | settled, not fixed |
| BL-013 | Array-of-records cardinality | 2026-08-18 | `e65fec7` | array-element alignment |
| BL-014 | Multi-branch unions inside array elements | 2026-08-18 | `e65fec7` | array-element alignment |
| BL-018 | extractFieldsPreservingStructure misalignment | 2026-08-19 | `6bb66d1` | the third array-element site |
| BL-009 | Flattener family diagram | 2026-08-19 | this commit | README diagram + FlattenerFamilyDiagramTest |

ROADMAP Phase 2 required "a commit SHA on every future completion claim". BL-003 and BL-004
predate that rule and no SHA was recorded at the time; they are marked as such rather than
back-filled with a guess. The 2026-08-17 rows are closed by the commit that carries this file.

### [BL-001] Document Java vs Groovy Implementation Choice — CLOSED, NOT ANSWERED ✅
- **Type:** Documentation
- **Priority:** Low
- **Effort:** S
- **Related Concern:** I-001
- **Original description:** Clarify when to use JsonFlattenerConsolidator (Java) vs JsonFlattener
  (Groovy) and their differences.
- **Resolution:** **The choice was removed rather than documented.** On 2026-08-11 the Groovy
  toolchain was deleted — `gmavenplus-plugin`, the Groovy runtime, Spock, and the last 17
  `.groovy` test sources. `src/main` had already been ported. There is no longer a Java-vs-Groovy
  decision for a user or a contributor to make: every class in this project is Java 17, and
  `ci.yml`'s `no Groovy anywhere` job fails the build if a `.groovy` file returns.
- **Why closed rather than left open:** the item asked for documentation of a distinction that no
  longer exists. Leaving it open would keep an action item pointed at a language the project does
  not use, and would imply to a reader of this backlog that the choice is still live.
- **What survives:** the *naming* confusion between `JsonFlattenerConsolidator` and
  `JsonFlattener` is real and independent of language — they are two different Java classes with
  similar names. That was tracked separately by **BL-008** and **BL-009**; both are now closed
  (BL-008 measured and its premise refuted, BL-009 by the README family diagram and its gate).
- **Completed:** 2026-08-11

### [BL-004] Dependency Hygiene & Modernization — COMPLETED ✅
- **Type:** Chore
- **Priority:** Medium
- **Effort:** S
- **Related Concern:** Maintainability, Security
- **Affected Files:**
  - `pom.xml` — Version updates and property extraction
- **Description:** Performed comprehensive pom.xml audit following the "Pom Hygiene" protocol. Extracted hardcoded versions to properties and updated outdated test dependencies.
- **Changes Made:**
  - Extracted ByteBuddy version to property: `1.14.4` → `${bytebuddy.version}` (1.15.10)
  - Extracted Objenesis version to property: `3.3` → `${objenesis.version}` (3.4)
  - Extracted Delta-Spark version to property: `3.1.0` → `${delta-spark.version}`
  - Updated Mockito: `5.7.0` → `5.14.2`
  - Updated AssertJ: `3.24.2` → `3.26.3`
  - Updated Testcontainers: `1.19.3` → `1.20.4`
- **Acceptance Criteria:** ✅ All versions use properties, test dependencies modernized
- **Discovered:** Session 3
- **Completed:** Session 3

### [BL-003] Remove org.json Dependency — COMPLETED ✅
- **Type:** Refactor
- **Priority:** High
- **Effort:** L
- **Related Concern:** License Compliance (org.json uses JSON License which includes "The Software shall be used for Good, not Evil" clause)
- **Affected Files:**
  - `pom.xml` — Removed org.json dependency and version property
  - `JsonFlattenerConsolidator.java` — Migrated to Jackson (ObjectMapper, JsonNode, ArrayNode)
  - `NexusPiercerSparkPipeline.java` — Migrated to Jackson
  - `NexusPiercerFunctions.java` — Migrated to Jackson
  - `FileFinder.java` — Migrated to Jackson
  - **Test Files (Complete Refactoring):**
    - `JsonFlattenerConsolidatorTest.java` — Updated assertions
    - `JsonFlattenerConsolidatorEdgeCaseTest.java` — Complete rewrite
    - `JsonFlattenerExplosionTest.java` — Migrated all JSONObject/JSONArray
    - `JsonFlattenerConsolidatorComprehensiveTest.java` — Complete rewrite
    - `JsonFlattenerConsolidatorPerformanceTest.java` — Complete rewrite
    - `NexusPiercerFunctionsTest.java` — Updated type declarations
    - `SchemaCompatibilityIntegrationTest.java` — Updated type declarations
- **Description:** Replaced org.json (JSON License with controversial "good not evil" clause) with Jackson (Apache 2.0 License) to ensure full license compatibility in enterprise/commercial environments.
- **Migration Mapping:**
  - `JSONObject` → `JsonNode` / `ObjectNode`
  - `JSONArray` → `ArrayNode`
  - `new JSONObject(string)` → `MAPPER.readTree(string)`
  - `new JSONObject()` → `MAPPER.createObjectNode()`
  - `new JSONArray()` → `MAPPER.createArrayNode()`
  - `jsonObject.getString(key)` → `jsonNode.get(key).asText()`
  - `jsonObject.put(k, v)` → `objectNode.put(k, v)` or `objectNode.set(k, node)`
  - `JSONObject.NULL` → `objectNode.putNull(key)`
- **Acceptance Criteria:** ✅ All org.json imports removed from main and test sources, Jackson used consistently
- **Discovered:** Session 2
- **Completed:** Session 3
