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

### [BL-012] Four `AvroReconstructor` knobs with no observed effect — **PROBE RUN 2026-08-17, SETTLED**

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

### [BL-013] `determineArraySize` has no non-JSON arrayFormat fallback — array-of-records sizing collapses to 1

**Filed 2026-08-17, and it must exist because the code that documented it is now deleted.**

`AvroReconstructor.calculateArraySize` was removed as an uncalled private method (SpotBugs
`UPM_UNCALLED_PRIVATE_METHOD`). Its caller was not lost: commit `cad816b` replaced the single call
site with `determineArraySize`, introduced by the same commit. **But the replacement is strictly
weaker.** `calculateArraySize` carried `BRACKET_LIST` / `COMMA_SEPARATED` / `PIPE_SEPARATED`
branches for parsing a bracket-delimited leaf; `determineArraySize` has none, and
`reconstructArrayOfRecords` Step 1 only JSON-parses a value that starts `[` and ends `]`.

**Consequence:** under those three array formats an N-element array of records collapses to one
`parsedFieldValues` entry of size 1, `determineArraySize` returns 1, and N−1 records are lost.

**The fix is at the caller, not by resurrecting the dead method** — add the format fallback to
`reconstructArrayOfRecords` Step 1. It changes reconstructed output, so it is corpus-moving and
must not ride along with a static-analysis change.

**Corpus:** `order-line-items-comma-separated` is currently `ACCEPTED_LOSS` on the strength of a
detail saying the reconstructor "fabricates extra rows and duplicates values rather than reporting
that the element counts disagree". Repairing the sizing moves that row. Its own `cannotCatch` notes
it covers neither `PIPE_SEPARATED` nor `BRACKET_LIST`, so two new fixtures are owed alongside it.

**Also carried forward:** `docs/audit/FINDINGS.md:2935`'s instruction to "first diff
`calculateArraySize` against `determineArraySize` and port over the format-specific branches" is
NOT satisfied by the deletion. This entry is where it now lives.

---

### [BL-014] `unwrapNullable` drops unions of three or more branches inside array elements

**Filed 2026-08-17, same reason as [BL-013]** — the dead method that recorded the gap is gone.

`AvroReconstructor.unwrapUnion` was deleted as uncalled. Its three call sites were replaced by
`unwrapNullable` when `cad816b` rewrote `reconstructArrayOfRecords`, so nothing stopped running.
**But the substitution is lossy at arity 3+.** `unwrapNullable` returns the union UNCHANGED unless
`types.size() == 2`; `unwrapUnion` returned the first non-null branch at any arity.

**Consequence:** a field typed `["null","SomeRecord","string"]` inside an array element matches
neither the `RECORD` nor the `ARRAY` branch and falls through silently to `handleMissingField`.

**Restoring `unwrapUnion` is NOT the fix.** "First non-null branch" is a different wrong answer,
and the class already owns a real branch resolver in `reconstructUnionValue` — which the
array-element path simply does not consult.

**Corpus:** `avro-union-of-records-overlapping-fields` is already published as `DEFECT` with
almost this rationale, but it goes through `reconstructUnionValue`, not the array-element path. A
distinct fixture is owed for the array-element case.

---

### [BL-015] Five `JsonFlattenerConfig` knobs are stored and read by nothing

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
- Resolve [BL-015] one way or the other.

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

### [BL-008] Clarify Flattener Naming Convention
- **Type:** Documentation
- **Priority:** Low
- **Effort:** XS
- **Related Concern:** C-002
- **Affected Files:** README.md, class JavaDocs
- **Description:** The naming of JsonFlattenerConsolidator vs JsonFlattener is confusing since they serve different purposes. (Filed when one was Java and the other Groovy; both are Java now, which removes the only cue distinguishing them and makes this *more* worth doing, not less.) JsonFlattener is actually a fluent wrapper around MapFlattener. Consider adding clarifying documentation or renaming for clarity.
- **Acceptance Criteria:** README section explaining the purpose and relationship of each flattener class
- **Discovered:** Session 2

### [BL-009] Add Flattener Family Diagram
- **Type:** Documentation
- **Priority:** Low
- **Effort:** S
- **Related Concern:** C-002
- **Affected Files:** README.md, docs/
- **Description:** Create a visual diagram showing the relationship between all flattener classes: JsonFlattenerConsolidator, JsonFlattener, MapFlattener, and their different use cases.
- **Acceptance Criteria:** Mermaid diagram in README showing class relationships and when to use each
- **Discovered:** Session 2

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
  similar names. That is tracked separately by **BL-008** and **BL-009**, both still open.
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
