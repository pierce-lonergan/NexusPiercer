package io.github.pierce;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BL-010: JsonFlattener was dead public surface, and the constructor every factory routes
 * through carried a live NPE.
 *
 * <p>THE SHAPE OF THE PROBLEM. Until 2026-08-17 no consumer could obtain a
 * {@link JsonFlattener}: the sole constructor is private, and {@code create()},
 * {@code with(MapFlattener)}, {@code with(MapFlattener, JsonFlattenerConfig)} and
 * {@code Builder.build()} all return {@code FluentOperation}. An exhaustive scan for any
 * member returning the type found none. So {@code flattenToMap}, {@code flattenToJson} and
 * {@code flattenMapToJson} were public methods on a type nobody could name.
 *
 * <p>THE CAPABILITY THAT WAS MISSING, which is bigger than "a type you cannot name".
 * JsonFlattener is immutable and shareable — three final fields over a thread-safe
 * MapFlattener. FluentOperation is the opposite: {@code currentData}, {@code transformers},
 * {@code validationRules} and {@code filter} are unsynchronised mutable instance fields, and
 * it is the only object the API ever handed out. A consumer who configured via
 * {@code builder()} therefore had to either re-run the whole builder chain per document or
 * reuse one non-thread-safe pipeline across documents. Two additive members fix that:
 * {@code Builder.buildFlattener()} yields the shareable engine, {@code newOperation()} yields
 * a fresh per-document pipeline.
 */
@DisplayName("BL-010 reusable JsonFlattener engine")
class JsonFlattenerReusableEngineTest {

    // ===================== the NPE, which fails at RUNTIME on released 2.0.0 API =====================

    @Nested
    @DisplayName("A null config is defaulted rather than dereferenced")
    class NullConfig {

        @Test
        @DisplayName("with(mapFlattener, null) works and produces a WORKING default")
        void nullConfigIsDefaultedNotDereferenced() {
            // FAILS BEFORE THE FIX at runtime, needing no new API. The constructor read
            //     this.config = config != null ? config : JsonFlattenerConfig.defaults();
            //     this.objectMapper = config.isUsePrettyPrint() ? PRETTY_MAPPER : STANDARD_MAPPER;
            // The guard installed on the first line is defeated by the second, which
            // dereferences the PARAMETER rather than the field. This is the repository's
            // signature pathology in its purest form: a null-defence that appears present and
            // does nothing.
            Map<String, Object> out = assertDoesNotThrow(() ->
                    JsonFlattener.with(MapFlattener.builder().build(), null)
                            .from("{\"a\":1}")
                            .toMap());

            // Asserting the RESULT, not merely the absence of a throw: a guard that swallowed
            // the failure and produced a broken flattener would pass a doesNotThrow-only test.
            assertEquals(1, out.size(), "expected exactly the one leaf, got " + out);
            assertEquals(1, out.get("a"), "the defaulted config must produce a working engine");
        }

        @Test
        @DisplayName("a null MapFlattener is defaulted too - the sibling guard on the line above")
        void nullFlattenerIsDefaulted() {
            Map<String, Object> out = assertDoesNotThrow(() ->
                    JsonFlattener.with(null, null).from("{\"a\":{\"b\":2}}").toMap());
            assertEquals(2, out.get("a_b"),
                    "the mapFlattener guard was already correct; this leg keeps it that way");
        }
    }

    // ===================== the reusable engine =====================

    @Nested
    @DisplayName("One engine, many documents")
    class ReusableEngine {

        @Test
        @DisplayName("buildFlattener() yields an engine that serves many documents")
        void builderYieldsAnEngineThatServesManyDocuments() {
            JsonFlattener engine = JsonFlattener.builder().buildFlattener();

            Map<String, Object> a = engine.newOperation().from("{\"a\":{\"b\":1}}").toMap();
            Map<String, Object> b = engine.newOperation().from("{\"c\":{\"d\":2}}").toMap();

            assertEquals(1, a.get("a_b"));
            assertEquals(2, b.get("c_d"));
            assertFalse(a.containsKey("c__d"), "the first document must not see the second");
            assertFalse(b.containsKey("a__b"), "the second document must not see the first");
        }

        @Test
        @DisplayName("newOperation() ALLOCATES rather than sharing - the pathology drill on this very fix")
        void newOperationAllocatesRatherThanSharing() {
            JsonFlattener engine = JsonFlattener.builder().buildFlattener();

            JsonFlattener.FluentOperation op1 = engine.newOperation();
            JsonFlattener.FluentOperation op2 = engine.newOperation();
            assertNotSame(op1, op2, "each call must hand back a distinct pipeline");

            // If newOperation() were implemented as a cached/shared FluentOperation - the single
            // most likely slip, and exactly this repository's signature pathology - the
            // transformer added to the first pipeline would leak into the second and this
            // assertion would go red.
            Map<String, Object> first = op1.addField("tag", 1).from("{\"a\":1}").toMap();
            assertTrue(first.containsKey("tag"), "the transformer must apply to its own pipeline");

            Map<String, Object> second = engine.newOperation().from("{\"b\":2}").toMap();
            assertFalse(second.containsKey("tag"),
                    "a FRESH operation must not carry the previous operation's transformers; "
                            + "got " + second);
        }

        @Test
        @DisplayName("CONTROL: a REUSED FluentOperation still accumulates - released 2.0.0 behaviour is unchanged")
        void reusedOperationStillAccumulates() {
            // This leg is as load-bearing as the one above. It pins behaviour that is
            // deliberately NOT changing: transformers accumulate on a reused pipeline. If a
            // later agent "fixes" that, it is a behaviour break on released 2.0.0 API and this
            // test is what says so.
            JsonFlattener.FluentOperation shared = JsonFlattener.builder().build().addField("tag", 1);
            shared.from("{\"a\":1}").toMap();

            Map<String, Object> again = shared.from("{\"b\":2}").toMap();
            assertTrue(again.containsKey("tag"),
                    "a REUSED FluentOperation accumulates transformers - that is 2.0.0 behaviour "
                            + "and must not change; got " + again);
        }
    }

    // ===================== the three-way drill on buildFlattener's configuration =====================

    @Nested
    @DisplayName("buildFlattener honours the builder configuration")
    class ConfigurationIsHonoured {

        @Test
        @DisplayName("GOOD INPUT: a configured naming strategy reaches the engine")
        void configuredNamingStrategyReachesTheEngine() {
            JsonFlattener engine = JsonFlattener.builder()
                    .namingStrategy(MapFlattener.FieldNamingStrategy.LOWER_CASE)
                    .buildFlattener();
            Map<String, Object> out = engine.newOperation().from("{\"TestKey\":1}").toMap();
            assertTrue(out.containsKey("testkey"),
                    "expected the configured naming strategy to be applied; got " + out.keySet());
        }

        @Test
        @DisplayName("SYNTHETIC VIOLATION: the two configurations must produce DIFFERENT output")
        void differentConfigurationsDiffer() {
            // The differ-assertion is the point. If buildFlattener() were written to construct
            // from MapFlattener.builder().build() instead of the builder's accumulated settings
            // - the most likely implementation slip - both branches produce byte-identical
            // output and this goes red. Asserting each side is "correct" in isolation would not
            // catch that.
            Map<String, Object> custom = JsonFlattener.builder()
                    .namingStrategy(MapFlattener.FieldNamingStrategy.LOWER_CASE)
                    .buildFlattener().newOperation().from("{\"TestKey\":1}").toMap();
            Map<String, Object> dflt = JsonFlattener.builder()
                    .buildFlattener().newOperation().from("{\"TestKey\":1}").toMap();

            assertNotEqualsMaps(custom, dflt);
        }

        private void assertNotEqualsMaps(Map<String, Object> a, Map<String, Object> b) {
            assertFalse(a.keySet().equals(b.keySet()),
                    "buildFlattener() is ignoring the builder's accumulated configuration: both "
                            + "settings produced the same keys " + a.keySet());
        }

        @Test
        @DisplayName("MISSING / EMPTY INPUT: empty JSON yields an empty map, and no from() throws")
        void emptyAndMissingInput() {
            JsonFlattener engine = JsonFlattener.builder().buildFlattener();

            assertTrue(engine.newOperation().from("").toMap().isEmpty(),
                    "empty input must yield an empty map rather than throwing");

            assertThrows(IllegalStateException.class,
                    () -> engine.newOperation().toMap(),
                    "calling toMap() with no from() must be a named failure, not a silent empty "
                            + "result");
        }
    }

    // ===================== the two doors must lead to the same room =====================

    @Nested
    @DisplayName("The additive path agrees with the released path")
    class PathsAgree {

        @Test
        @DisplayName("build() and buildFlattener().newOperation() produce identical output")
        void theTwoDoorsAgree() {
            String[] docs = {
                "{\"user\":{\"name\":\"John\",\"age\":30}}",
                "{\"items\":[1,2,3]}",
                "{\"a\":null,\"b\":{\"c\":null}}"
            };

            for (String doc : docs) {
                Map<String, Object> viaReleased = JsonFlattener.builder()
                        .useArrayBoundarySeparator(true)
                        .namingStrategy(MapFlattener.FieldNamingStrategy.LOWER_CASE)
                        .build().from(doc).toMap();
                Map<String, Object> viaAdditive = JsonFlattener.builder()
                        .useArrayBoundarySeparator(true)
                        .namingStrategy(MapFlattener.FieldNamingStrategy.LOWER_CASE)
                        .buildFlattener().newOperation().from(doc).toMap();

                assertEquals(viaReleased, viaAdditive,
                        "the two doors must lead to the same room for " + doc
                                + ". If they diverge, new callers silently get different answers "
                                + "from old ones - which is why both share one private resolver.");
            }
        }

        @Test
        @DisplayName("the engine itself exposes the core operation")
        void engineExposesFlattenToMap() {
            JsonFlattener engine = JsonFlattener.builder().buildFlattener();
            Map<String, Object> out = engine.flattenToMap("{\"a\":{\"b\":1}}");
            assertNotNull(out);
            assertEquals(1, out.get("a_b"),
                    "flattenToMap is the engine's core operation and the reason the reusable "
                            + "handle exists; it was public-but-unreachable before this change");
        }
    }

    // ===================== BL-015: what buildFlattener() does NOT carry =====================

    /**
     * BL-015. {@code buildFlattener()} hands consumers an engine carrying a
     * {@link JsonFlattener.JsonFlattenerConfig}, and five of that config's six knobs are read
     * <b>nowhere</b> in {@code src/main}. Shipping the engine without pinning that would ratify
     * the inertness silently.
     *
     * <p><b>UPDATED 2026-08-19.</b> Four of the five were wired up in 2.1.0 and this class now
     * pins each one's CONTRACT rather than its inertness. The alarm it used to be did its job:
     * the single all-five assertion below went red the moment {@code sortKeys} and
     * {@code preserveNulls} went live, and it was RESTATED rather than deleted, exactly as its
     * own message demanded. The vacuity controls and the {@code failOnError} pin are unchanged.
     *
     * <p>WHY WIRING THEM WAS SAFE, since the reasoning above said it would not be. The general
     * argument ("a caller who set the knob would silently change behaviour") is right, and the
     * SPECIFIC argument is narrower and survives it: every {@code JsonFlattenerConfig} default is
     * byte-identical to the per-call default it now feeds - charset UTF-8, bufferSize 8192,
     * preserveNulls/includeNulls true, sortKeys false - and an explicitly passed
     * {@code OutputOptions} still wins. So the only caller whose behaviour moved is the one who
     * explicitly set a knob and previously got nothing: the caller who asked for it. That is a
     * promise kept, and it is changelogged as a behaviour change anyway.
     *
     * <p>{@code failOnError} is STILL inert and its assertion below is unchanged. Its effect is
     * undefined rather than unimplemented - see {@code JsonFlattener.Builder#failOnError}.
     *
     * <p>THE VACUITY CONTROL IS THE IMPORTANT TEST. An assertion that two outputs are equal is
     * exactly the shape that passes when the comparison is broken, the documents are trivial, or
     * the builder was never applied — the repository's signature pathology. So
     * {@link #theProbeCanSeeADifferenceWhenThereIsOne()} runs the SAME comparison over a knob that
     * IS live and requires it to report a DIFFERENCE. Without that leg, every assertion below is
     * unfalsifiable.
     */
    @Nested
    @DisplayName("BL-015: the five inert JsonFlattenerConfig knobs are pinned as inert")
    class InertConfigKnobsArePinned {

        private static final String DOC = "{\"z\":1,\"a\":{\"b\":null,\"c\":\"x\"}}";

        /** Every JsonFlattenerConfig knob turned away from its default, in one config. */
        private JsonFlattener.JsonFlattenerConfig loudConfig() {
            return JsonFlattener.JsonFlattenerConfig.builder()
                    .charset(java.nio.charset.StandardCharsets.US_ASCII)
                    .bufferSize(1)
                    .failOnError(false)
                    .preserveNulls(false)
                    .sortKeys(true)
                    .build();
        }

        // ---------------- 1. good input passes: the knobs change nothing ----------------

        @Test
        @DisplayName("GOOD INPUT: each config knob now carries its own stated contract")
        void eachConfigKnobCarriesItsOwnContract() {
            // RESTATED, NOT DELETED. This used to be one assertion that all five knobs together
            // changed nothing, and its own failure message said "do not fix a failure here by
            // relaxing the assertion". It went red when sortKeys and preserveNulls went live in
            // 2.1.0, which is precisely what it was for. Honouring that instruction means saying
            // what each knob now does, one assertion at a time - a single lump assertion cannot
            // distinguish "four knobs were wired correctly" from "one knob broke everything".
            String dflt = JsonFlattener.with(MapFlattener.builder().build(),
                            JsonFlattener.JsonFlattenerConfig.defaults())
                    .from(DOC).toJson();
            assertEquals("{\"z\":1,\"a_b\":null,\"a_c\":\"x\"}", dflt,
                    "the UNCONFIGURED default must not have moved; every config default is "
                            + "byte-identical to the per-call default it feeds, and that equality "
                            + "is the entire safety argument for wiring them");

            assertEquals("{\"a_b\":null,\"a_c\":\"x\",\"z\":1}",
                    JsonFlattener.with(MapFlattener.builder().build(),
                            JsonFlattener.JsonFlattenerConfig.builder().sortKeys(true).build())
                            .from(DOC).toJson(),
                    "sortKeys is LIVE since 2.1.0 on the no-argument terminals");

            assertEquals("{\"z\":1,\"a_c\":\"x\"}",
                    JsonFlattener.with(MapFlattener.builder().build(),
                            JsonFlattener.JsonFlattenerConfig.builder()
                                    .preserveNulls(false).build())
                            .from(DOC).toJson(),
                    "preserveNulls is LIVE since 2.1.0 and REMOVES keys - the largest surprise "
                            + "of the four");

            assertEquals(dflt,
                    JsonFlattener.with(MapFlattener.builder().build(),
                            JsonFlattener.JsonFlattenerConfig.builder().bufferSize(1).build())
                            .from(DOC).toJson(),
                    "bufferSize is live but affects ALLOCATION only, so output must stay "
                            + "byte-identical. Nothing in this class can see whether it is wired; "
                            + "JsonFlattenerConfigKnobsTest watches the read requests instead, "
                            + "and that is the only reason the knob is falsifiable at all.");

            assertEquals(dflt,
                    JsonFlattener.with(MapFlattener.builder().build(),
                            JsonFlattener.JsonFlattenerConfig.builder().failOnError(false).build())
                            .from(DOC).toJson(),
                    "failOnError is STILL INERT by design; on good input it could not show a "
                            + "difference either way, which is why the throwing pin below is the "
                            + "assertion that actually holds it");
        }

        @Test
        @DisplayName("GOOD INPUT: the engine's own methods ignore prettyPrint entirely")
        void prettyPrintIsInertOnTheEngineSurface() {
            String yes = JsonFlattener.builder().prettyPrint(true)
                    .buildFlattener().flattenToJson(DOC, false);
            String no = JsonFlattener.builder().prettyPrint(false)
                    .buildFlattener().flattenToJson(DOC, false);

            assertEquals(no, yes,
                    "flattenToJson takes an explicit `pretty` argument and selects its mapper from "
                            + "THAT, so config.usePrettyPrint cannot reach it. The mapper the "
                            + "config selects is used only to READ input, where pretty-printing "
                            + "has no effect. This is why buildFlattener()'s javadoc names the "
                            + "MapFlattener half specifically rather than promising 'the "
                            + "configured engine' outright.");
        }

        @Test
        @DisplayName("GOOD INPUT: failOnError(false) does not make parsing lenient")
        void failOnErrorDoesNotSuppressTheParseFailure() {
            JsonFlattener engine = JsonFlattener.builder().failOnError(false).buildFlattener();

            assertThrows(JsonFlattener.JsonFlattenException.class,
                    () -> engine.flattenToMap("{not json"),
                    "failOnError(false) is the most misleading of the five: it reads as 'do not "
                            + "throw' and JsonFlattenerConfig.isFailOnError() is consulted by "
                            + "nothing, so malformed input still throws. Pinned so that making it "
                            + "live cannot happen silently.");
        }

        // -------- 2. synthetic violation blocks: the probe is not vacuous --------

        @Test
        @DisplayName("SYNTHETIC VIOLATION: the same comparison DOES report a difference for a live knob")
        void theProbeCanSeeADifferenceWhenThereIsOne() {
            // A MapFlattener-side knob, which IS honoured on the engine surface.
            String shallow = JsonFlattener.builder().maxDepth(2)
                    .buildFlattener().flattenToJson("{\"l1\":{\"l2\":{\"l3\":{\"l4\":\"v\"}}}}", false);
            String deep = JsonFlattener.builder().maxDepth(99)
                    .buildFlattener().flattenToJson("{\"l1\":{\"l2\":{\"l3\":{\"l4\":\"v\"}}}}", false);

            assertNotEquals(deep, shallow,
                    "THE VACUITY CONTROL. If this fails, every inertness assertion in this class "
                            + "is worthless, because it would mean the comparison cannot detect a "
                            + "difference that definitely exists - the builder is not being "
                            + "applied, or the engine is not being reconfigured at all. maxDepth "
                            + "is honoured, so the two MUST differ.");

            // And prettyPrint IS live through newOperation(), unlike on the engine's own methods.
            String p = JsonFlattener.builder().prettyPrint(true)
                    .buildFlattener().newOperation().from(DOC).toJson();
            String c = JsonFlattener.builder().prettyPrint(false)
                    .buildFlattener().newOperation().from(DOC).toJson();

            assertNotEquals(c, p,
                    "SECOND VACUITY CONTROL, and it pins the asymmetry that matters: prettyPrint "
                            + "reaches FluentOperation.toJson() but NOT the engine's own "
                            + "flattenToJson. The two terminals buildFlattener() and build() are "
                            + "therefore not interchangeable on this setting.");
        }

        // -------- 3. missing / empty input blocks rather than passing vacuously --------

        @Test
        @DisplayName("MISSING / EMPTY INPUT: a null config defaults, and empty input still yields empty")
        void nullAndEmptyInputsDoNotPassVacuously() {
            assertDoesNotThrow(
                    () -> JsonFlattener.with(MapFlattener.builder().build(), null).from(DOC).toJson(),
                    "a null config must default rather than NPE - the sibling guard pinned above");

            JsonFlattener engine = JsonFlattener.builder().failOnError(false).buildFlattener();
            assertTrue(engine.flattenToMap("").isEmpty(),
                    "empty input yields an empty map at every config; asserted so the inertness "
                            + "checks above cannot be satisfied by an engine that silently returns "
                            + "nothing for everything");
            assertFalse(engine.flattenToMap(DOC).isEmpty(),
                    "and the SAME engine must return something for real input - otherwise 'both "
                            + "configs produce identical output' would be true because both "
                            + "produce nothing");
        }
    }
}
