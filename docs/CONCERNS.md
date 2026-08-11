# Concerns Registry — NexusPiercer
> Issues, technical debt, code smells, and architectural concerns
> Last Updated: 2025-12-08

## Severity Definitions
- **CRITICAL:** System stability/security at risk. Address immediately.
- **HIGH:** Significant impact on maintainability/performance. Address soon.
- **MEDIUM:** Notable issue but manageable. Plan to address.
- **LOW:** Minor concern. Address opportunistically.
- **INFO:** Observation, not necessarily a problem.

---

## Critical Concerns

| ID | Category | Description | Location | Discovered |
|----|----------|-------------|----------|------------|
| — | — | No critical concerns identified | — | — |

---

## High Concerns

| ID | Category | Description | Location | Discovered |
|----|----------|-------------|----------|------------|
| — | — | No high concerns identified | — | — |

---

## Medium Concerns

| ID | Category | Description | Location | Discovered |
|----|----------|-------------|----------|------------|
| C-001 | DEBT | JsonReconstructor was completely commented out (~1294 lines) when filed; the file is now `src/main/java/.../JsonReconstructor.java` following the Groovy port. Potential dead code or incomplete refactor that should be either removed or completed. | src/main/java/.../JsonReconstructor.java | Session 2 |

---

## Low Concerns

| ID | Category | Description | Location | Discovered |
|----|----------|-------------|----------|------------|
| C-002 | MAINTAINABILITY | Naming confusion: JsonFlattenerConsolidator vs JsonFlattener serve different purposes but similar names may confuse developers. Both are Java as of 2026-08-11 — the language difference that once distinguished them is gone | src/main/java | Session 2 |

---

## Observations (Info)

| ID | Category | Description | Location | Discovered |
|----|----------|-------------|----------|------------|
| I-001 | ARCHITECTURE | Dual implementations: JsonFlattenerConsolidator + JsonFlattener exist (both Java since 2026-08-11; the language split that framed this is gone — see BL-001, closed) | src/main | Session 1 |
| I-002 | DOCUMENTATION | Comprehensive READMEs with examples provided | Root | Session 1 |
| I-003 | ARCHITECTURE | Heavy use of Builder pattern across flatteners | Multiple | Session 1 |
| I-004 | PERFORMANCE | Schema caching implemented with ConcurrentHashMap | NexusPiercerSparkPipeline, AvroSchemaFlattener | Session 1 |
| I-005 | ARCHITECTURE | Thread-local context used for circular reference detection | MapFlattener | Session 1 |

---

## Concern Categories
- **SECURITY:** Vulnerabilities, unsafe practices
- **PERFORMANCE:** Inefficiencies, N+1 queries, missing indexes
- **ARCHITECTURE:** Layer violations, circular dependencies, coupling
- **MAINTAINABILITY:** Complex code, missing tests, poor naming
- **RELIABILITY:** Missing error handling, no retries, race conditions
- **SCALABILITY:** Bottlenecks, single points of failure
- **DOCUMENTATION:** Missing/outdated docs, unclear intent
- **TESTING:** Missing coverage, flaky tests, test smells
- **DEBT:** Shortcuts, TODOs, FIXMEs found in code
- **DEPRECATED:** Using outdated libraries/patterns
