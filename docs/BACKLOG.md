# Discovery-Generated Backlog — NexusPiercer
> Improvements, refactors, and enhancements identified during exploration
> Last Updated: 2025-12-08

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

### [BL-010] JsonFlattener is dead public surface

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
format matches the flattener that produced the data, and the failure when they disagree is a
`NumberFormatException` from the value coercion, not a diagnostic naming either setting.

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

---

### [BL-012] Four `AvroReconstructor` knobs with no observed effect — UNPROVEN, needs a targeted probe

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

## Medium Priority

### [BL-007] Investigate and Resolve JsonReconstructor.groovy
- **Type:** Chore
- **Priority:** Medium
- **Effort:** M
- **Related Concern:** C-001
- **Affected Files:** `src/main/groovy/io/github/pierce/JsonReconstructor.groovy`
- **Description:** The entire JsonReconstructor class (~1294 lines) is commented out. Need to determine if this is dead code to be removed, an incomplete feature to be finished, or was disabled for a specific reason. The class appears to provide schema-less JSON reconstruction, complementing the schema-based AvroReconstructor.
- **Acceptance Criteria:** Either (a) remove the file and update any references, or (b) uncomment and test the implementation, or (c) document why it's disabled
- **Discovered:** Session 2

---

## Low Priority

### [BL-008] Clarify Flattener Naming Convention
- **Type:** Documentation
- **Priority:** Low
- **Effort:** XS
- **Related Concern:** C-002
- **Affected Files:** README.md, class JavaDocs
- **Description:** The naming of JsonFlattenerConsolidator (Java) vs JsonFlattener (Groovy) is confusing since they serve different purposes. JsonFlattener is actually a fluent wrapper around MapFlattener. Consider adding clarifying documentation or renaming for clarity.
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

### [BL-001] Document Java vs Groovy Implementation Choice
- **Type:** Documentation
- **Priority:** Low
- **Effort:** S
- **Related Concern:** I-001
- **Affected Files:** README.md, docs/
- **Description:** Clarify when to use JsonFlattenerConsolidator (Java) vs JsonFlattener (Groovy) and their differences
- **Acceptance Criteria:** README section explaining implementation choice and use cases
- **Discovered:** Session 1

### [BL-002] API Reference Documentation
- **Type:** Documentation
- **Priority:** Low
- **Effort:** M
- **Related Concern:** N/A
- **Affected Files:** docs/
- **Description:** Generate comprehensive API reference documentation (Javadoc/Groovydoc)
- **Acceptance Criteria:** Published API docs for all public classes
- **Discovered:** Session 1

---

## Completed
*Moved here when addressed*

| ID | Title | Completed | By |
|----|-------|-----------|-----|
| BL-003 | Remove org.json dependency (license compliance) | 2025-12-08 | Session 2 |
| BL-004 | Dependency Hygiene & Modernization | 2025-12-08 | Session 3 |

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
