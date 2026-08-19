package io.github.pierce;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The leaf-versus-branch key collision in {@code JsonReconstructor.setNestedValue} - filed as
 * [BL-023] and fixed here.
 *
 * <h2>What was wrong</h2>
 *
 * <p>A flattened map can hold a key {@code a} beside a key {@code a_b}. The first decodes to the
 * one-segment path {@code ["a"]}, the second to {@code ["a","b"]}, so {@code a} is BOTH a leaf and
 * an intermediate node of the same tree. JSON has no node that is simultaneously a scalar and an
 * object, so one of the two has to go - and {@code buildHierarchy} iterated
 * {@code flattenedMap.entrySet()} in the caller's order and let whichever arrived LAST decide,
 * with no log, no exception and no marker. THREE outcomes were reachable, measured against
 * unfixed HEAD:</p>
 *
 * <pre>
 * branch then leaf : {"a_b":"1","a":"2"} -&gt; {"a":"2"}                      subtree DESTROYED
 * leaf then branch : {"a":"2","a_b":"1"} -&gt; {"a":{"_value":"2","b":"1"}}   key FABRICATED
 * both, with a real {@code _value} field:
 *                    {"a":"scalar","a_\_value":"real"} -&gt; {"a":{"_value":"real"}}
 *                                                                          scalar DESTROYED, and
 *                                                                          the survivor is
 *                                                                          indistinguishable from
 *                                                                          the fabrication
 * </pre>
 *
 * <p>The first two rows are the SAME two entries and produce structurally different documents;
 * the third is a different pair and loses a value outright, in either order. That is
 * nondeterminism sitting under silent data loss: {@code reconstruct} is a public method that
 * accepts any {@code Map}, and a {@code HashMap} of the first pair picks an outcome by
 * {@code String.hashCode}.</p>
 *
 * <h2>Why this is NOT a sub-item of the sentinel-key entry [BL-022]</h2>
 *
 * <p>It reproduces with no sentinel anywhere. Three producers of an unescaped separator are known
 * and only ONE is the flattener sentinel:</p>
 * <ul>
 *   <li>{@code MapFlattener}'s {@code VALUE_SENTINEL} base-key mapping - {@code data} beside
 *       {@code data_name};</li>
 *   <li>an ordinary nullable nested record inside an array of records -
 *       {@code {"orders":[{"id":1,"ship":{"city":"NY"}},{"id":2,"ship":null}]}} emits
 *       {@code orders_ship} beside {@code orders_ship_city}, pure user data, no sentinel;</li>
 *   <li>the {@code LOWER_CASE} naming strategy's unescaped {@code _2} dedup suffix - {@code id}
 *       beside {@code id_2}.</li>
 * </ul>
 *
 * <p>Renaming the sentinel column would leave the other two producing byte-identical key sets, so
 * the emitted name cannot be the repair. See {@link MapFlattenerSentinelKeyContractTest} for the
 * emit-side half and for why no suffix is available.</p>
 *
 * <h2>What the fix is</h2>
 *
 * <p>The colliding leaf keys are computed BEFORE any write, as the intersection of
 * {@code flattenedMap.keySet()} with the analysis' set of intermediate paths. A set intersection
 * cannot depend on iteration order, which is the property the old walk lacked. The default policy
 * then REFUSES with {@link JsonReconstructor.KeyCollisionException}; {@code PREFER_LEAF} and
 * {@code PREFER_BRANCH} keep going but drop the same side every time and say so at WARN.</p>
 */
@DisplayName("a leaf/branch key collision is decided before any write, not by map iteration order")
class JsonReconstructorKeyCollisionTest {

    private static JsonReconstructor defaults() {
        return JsonReconstructor.builder().build();
    }

    private static JsonReconstructor with(JsonReconstructor.CollisionPolicy policy) {
        return JsonReconstructor.builder().onKeyCollision(policy).build();
    }

    private static Map<String, Object> ordered(String k1, Object v1, String k2, Object v2) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(k1, v1);
        m.put(k2, v2);
        return m;
    }

    // ------------------------------------------------------------------ the invariant that
    //                                                                     outlives the policy

    @Test
    @DisplayName("THE INVARIANT: the same entries reconstruct the same way in either key order")
    void reconstructionIsIndependentOfFlattenedMapKeyOrder() {
        Map<String, Object> leafFirst = ordered("a", "2", "a_b", "1");
        Map<String, Object> branchFirst = ordered("a_b", "1", "a", "2");

        // This is the test that fails on the NONDETERMINISM rather than on the policy, so it
        // cannot be satisfied by quietly picking a winner. It is written for every policy,
        // because the property has to hold for all three or the knob is just a second way to be
        // order-dependent.
        for (JsonReconstructor.CollisionPolicy policy : JsonReconstructor.CollisionPolicy.values()) {
            JsonReconstructor r = with(policy);
            String one = outcome(r, leafFirst);
            String two = outcome(r, branchFirst);
            assertEquals(one, two,
                    "policy " + policy + " reconstructed the SAME two entries differently "
                            + "depending on which one the map happened to yield first");
        }
    }

    private static String outcome(JsonReconstructor r, Map<String, Object> flat) {
        try {
            return "RETURNED " + r.reconstruct(flat);
        } catch (JsonReconstructor.ReconstructionException e) {
            return "THREW " + e.getClass().getSimpleName();
        }
    }

    // ------------------------------------------------------------------ the three branches

    @Test
    @DisplayName("branch then leaf no longer silently deletes the subtree")
    void branchThenLeafNoLongerSilentlyDeletesTheSubtree() {
        Map<String, Object> flat = ordered("a_b", "1", "a", "2");

        JsonReconstructor.KeyCollisionException thrown = assertThrows(
                JsonReconstructor.KeyCollisionException.class,
                () -> defaults().reconstruct(flat));

        assertTrue(thrown.getMessage().contains("a"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("a_b"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("a\\_b"),
                "the message must name the escaped form that WOULD have disambiguated, because "
                        + "that is the caller's fix: " + thrown.getMessage());
        assertEquals("a", thrown.getCollidingKey());
        assertTrue(thrown.getShadowedKeys().contains("a_b"), thrown.getShadowedKeys().toString());

        assertEquals(Map.of("a", "2"), with(JsonReconstructor.CollisionPolicy.PREFER_LEAF)
                .reconstruct(flat));
    }

    @Test
    @DisplayName("leaf then branch no longer fabricates a _value key")
    void leafThenBranchNoLongerFabricatesAValueKey() {
        Map<String, Object> flat = ordered("a", "2", "a_b", "1");

        assertThrows(JsonReconstructor.KeyCollisionException.class,
                () -> defaults().reconstruct(flat));

        for (JsonReconstructor.CollisionPolicy policy : JsonReconstructor.CollisionPolicy.values()) {
            if (policy == JsonReconstructor.CollisionPolicy.FAIL) {
                continue;
            }
            Map<String, Object> out = with(policy).reconstruct(flat);
            assertFalse(containsKeyAnywhere(out, "_value"),
                    "policy " + policy + " invented a _value key the source never had: " + out);
        }

        // The reversibility leg. The old wrapper produced {"a":{"_value":"2","b":"1"}}, which
        // re-flattens to the keys a_\_value and a_b - NOT to a and a_b - so the round trip did
        // not close even when nothing appeared to be lost.
        Map<String, Object> branch = with(JsonReconstructor.CollisionPolicy.PREFER_BRANCH)
                .reconstruct(flat);
        assertEquals(Map.of("a", Map.of("b", "1")), branch);
        assertEquals(Map.of("a_b", "1"), MapFlattener.builder().build().flatten(branch),
                "PREFER_BRANCH must return a document that re-flattens to a subset of the input "
                        + "keys, never to a new key set");
    }

    @Test
    @DisplayName("a genuine field named _value is not destroyed by the wrapper")
    void aGenuineFieldNamedUnderscoreValueIsNotDestroyedByTheWrapper() {
        // Segments ["a","_value"] encode to a_\_value - the leading underscore of the segment is
        // escaped, which is exactly what MapFlattener emits for {"a":{"_value":1}} and what the
        // fixture naming/user-field-named-value-vs-flattener-sentinel records.
        String realValueField = "a_\\_value";
        Map<String, Object> leafFirst = ordered("a", "scalar", realValueField, "real");
        Map<String, Object> branchFirst = ordered(realValueField, "real", "a", "scalar");

        assertThrows(JsonReconstructor.KeyCollisionException.class,
                () -> defaults().reconstruct(leafFirst));
        assertThrows(JsonReconstructor.KeyCollisionException.class,
                () -> defaults().reconstruct(branchFirst));

        // Under either non-failing policy exactly ONE side survives, the SAME side in both orders,
        // and the survivor is never a value the source did not carry. Against unfixed HEAD
        // leaf-first returned {"a":{"_value":"real"}} - the string "scalar" destroyed without a
        // trace, and the surviving node byte-identical to the fabricated wrapper.
        assertEquals(Map.of("a", "scalar"),
                with(JsonReconstructor.CollisionPolicy.PREFER_LEAF).reconstruct(leafFirst));
        assertEquals(Map.of("a", "scalar"),
                with(JsonReconstructor.CollisionPolicy.PREFER_LEAF).reconstruct(branchFirst));
        assertEquals(Map.of("a", Map.of("_value", "real")),
                with(JsonReconstructor.CollisionPolicy.PREFER_BRANCH).reconstruct(leafFirst));
        assertEquals(Map.of("a", Map.of("_value", "real")),
                with(JsonReconstructor.CollisionPolicy.PREFER_BRANCH).reconstruct(branchFirst));
    }

    // ------------------------------------------------------------------ the discriminating leg

    @Test
    @DisplayName("DISCRIMINATOR: an escaped literal separator is not mistaken for a collision")
    void anEscapedLiteralSeparatorIsNotMistakenForACollision() {
        // PASSES BEFORE AND AFTER, and the set is worthless without it. Without this leg every
        // other test here is equally satisfied by an implementation that throws on any key
        // containing a separator - which would detonate SeparatorInFieldNameRegressionTest and
        // every escaped-name fixture in the corpus. Detection must compare an ENCODED key against
        // an ENCODED intermediate path; getting that comparison wrong is the FlattenedPath bug
        // class coming back.
        Map<String, Object> literal = ordered("a", "1", "a\\_b", "2");
        Map<String, Object> out = defaults().reconstruct(literal);

        assertEquals("1", out.get("a"));
        assertEquals("2", out.get("a_b"), "a\\_b is ONE segment, a field literally named a_b");
        assertEquals(2, out.size(), out.toString());
    }

    @Test
    @DisplayName("DISCRIMINATOR: a colliding key and an escaped literal in the same map")
    void aCollidingKeyAndAnEscapedLiteralInTheSameMapAreToldApart() {
        // Both forms present at once, which is the shape that catches a detector that compares
        // the wrong side of the escape. "a" collides with "a_b" (two segments); "a\_c" does not
        // collide with anything because it is one segment named a_c.
        Map<String, Object> mixed = new LinkedHashMap<>();
        mixed.put("a", "leaf");
        mixed.put("a_b", "branch");
        mixed.put("a\\_c", "literal");

        JsonReconstructor.KeyCollisionException thrown = assertThrows(
                JsonReconstructor.KeyCollisionException.class,
                () -> defaults().reconstruct(mixed));
        assertEquals(1, thrown.getShadowedKeys().size(),
                "only a_b shadows a; a\\_c is a literal field name and must not be listed: "
                        + thrown.getShadowedKeys());

        Map<String, Object> kept = with(JsonReconstructor.CollisionPolicy.PREFER_LEAF)
                .reconstruct(mixed);
        assertEquals("leaf", kept.get("a"));
        assertEquals("literal", kept.get("a_c"),
                "the literal field is not part of the collision and must survive every policy");
    }

    // ------------------------------------------------------------------ order independence,
    //                                                                     four inputs one outcome

    @Test
    @DisplayName("the collision set is computed before any write, for every map shape")
    void theCollisionSetIsComputedBeforeAnyWrite() {
        Map<String, Object> spread = new LinkedHashMap<>();
        spread.put("a_b", "1");
        spread.put("z", "9");
        spread.put("y", "8");
        spread.put("a", "2");

        Map<String, Object> hash = new HashMap<>();
        hash.put("a", "2");
        hash.put("a_b", "1");
        hash.put("z", "9");
        hash.put("y", "8");

        Map<String, Object> leafFirst = new LinkedHashMap<>();
        leafFirst.put("a", "2");
        leafFirst.put("a_b", "1");
        leafFirst.put("z", "9");
        leafFirst.put("y", "8");

        Map<String, Object> branchFirst = new LinkedHashMap<>();
        branchFirst.put("a_b", "1");
        branchFirst.put("a", "2");
        branchFirst.put("z", "9");
        branchFirst.put("y", "8");

        for (Map<String, Object> input : java.util.List.of(spread, hash, leafFirst, branchFirst)) {
            assertThrows(JsonReconstructor.KeyCollisionException.class,
                    () -> defaults().reconstruct(input),
                    "detection must not depend on how far apart the colliding keys sit, nor on "
                            + "the map implementation: " + input.keySet());
        }

        // ... and the surviving document is identical for all four under a non-failing policy.
        JsonReconstructor lenient = with(JsonReconstructor.CollisionPolicy.PREFER_LEAF);
        Map<String, Object> reference = lenient.reconstruct(spread);
        for (Map<String, Object> input : java.util.List.of(hash, leafFirst, branchFirst)) {
            assertEquals(reference, lenient.reconstruct(input), input.keySet().toString());
        }
    }

    @Test
    @DisplayName("a null leaf collides too - the value is not what makes it a collision")
    void aNullLeafValueStillCollidesBecauseTheKEYIsTheCollision() {
        // Measured against unfixed HEAD: {"a":null,"a_b":"1"} returned {"a":{"b":"1"}} and the
        // reverse order returned {"a":null}. A null leaf took a THIRD path through the old code -
        // `existing == null` reads as "nothing here yet" - so it never even reached the wrapper,
        // and it lost data in both directions.
        Map<String, Object> leafFirst = ordered("a", null, "a_b", "1");
        Map<String, Object> branchFirst = ordered("a_b", "1", "a", null);

        assertThrows(JsonReconstructor.KeyCollisionException.class,
                () -> defaults().reconstruct(leafFirst));
        assertThrows(JsonReconstructor.KeyCollisionException.class,
                () -> defaults().reconstruct(branchFirst));
    }

    @Test
    @DisplayName("a three-segment collision is detected at the level it happens")
    void aThreeSegmentCollisionIsDetectedAtTheLevelItHappens() {
        // Against unfixed HEAD {"a_b_c":"1","a_b":"2"} returned {"a":{"b":"2"}} - "c" gone.
        Map<String, Object> flat = ordered("a_b_c", "1", "a_b", "2");

        JsonReconstructor.KeyCollisionException thrown = assertThrows(
                JsonReconstructor.KeyCollisionException.class,
                () -> defaults().reconstruct(flat));
        assertEquals("a_b", thrown.getCollidingKey(),
                "the collision is at a_b, not at a - a is an intermediate on BOTH keys and "
                        + "collides with neither");
    }

    @Test
    @DisplayName("BACKSTOP: a NON-CANONICAL key the set intersection cannot see still refuses, in both orders")
    void aNonCanonicalKeyIsCaughtByTheBackstopsInEitherOrder() {
        // THE RESIDUAL PATH THIS FIX COULD HAVE LEFT SILENT, pinned rather than assumed away.
        // reconstruct(Map) is public and takes any map, and the detector is a SET INTERSECTION
        // over encoded strings. The raw key  a\b  decodes to the SINGLE segment  a\b  - a
        // backslash escaping nothing is a literal backslash - and re-encodes to  a\\b , so the
        // raw key is not equal to the intermediate path that  a\b_c  contributes and the
        // intersection cannot see it. No amount of care in the detector closes that without
        // canonicalising every key, which is an allocation per key on the hottest path in the
        // class for a shape only a hand-built map can reach.
        //
        // Both orders must still refuse, and for the reason the whole entry exists: before the
        // leaf-side guard was added, branch-then-leaf silently OVERWROTE the subtree here while
        // leaf-then-branch threw - the exact asymmetry the fix removes, reintroduced one layer
        // further down.
        //
        // AND IT RUNS OVER THREE LEAF SHAPES, NOT ONE. The first version of this test used only
        // the String "scalar", and that is exactly why the first version of the guards passed it
        // while still losing data. The guards asked `occupant instanceof Map && !(value
        // instanceof Map)` and `existing == null`, so the answer depended on the RUNTIME TYPE OF
        // THE VALUE, and two shapes walked straight through. Measured at bd5b070, before this
        // repair:
        //
        //   Map leaf,  branch-first : {a\b_c=KEEP-ME, a\b={}}  -> {a\b={}}       KEEP-ME GONE
        //   Map leaf,  leaf-first   : {a\b={}, a\b_c=KEEP-ME}  -> {a\b={c=KEEP-ME}}  merged
        //   null leaf, leaf-first   : {a\b=null, a\b_c=1}      -> {a\b={c=1}}    null LEAF GONE
        //   null leaf, branch-first : threw, as it does today
        //
        // Three silent answers and one throw, from a guard pair whose javadoc claimed both orders
        // were covered. A value-shaped test cannot find a value-shaped hole, so the shapes are
        // now the parameter.
        for (Object leaf : new Object[] {"scalar", null, new LinkedHashMap<String, Object>()}) {
            String shape = leaf == null ? "null" : leaf.getClass().getSimpleName();
            Map<String, Object> leafFirst = ordered("a\\b", leaf, "a\\b_c", "1");
            Map<String, Object> branchFirst = ordered("a\\b_c", "1", "a\\b", leaf);

            assertThrows(JsonReconstructor.KeyCollisionException.class,
                    () -> defaults().reconstruct(leafFirst),
                    "leaf-then-branch must refuse for a " + shape + " leaf");
            assertThrows(JsonReconstructor.KeyCollisionException.class,
                    () -> defaults().reconstruct(branchFirst),
                    "branch-then-leaf must refuse for a " + shape + " leaf; before the "
                            + "identity-based guard it silently overwrote the subtree");

            // And it refuses under every policy, because the policies only skip keys the DETECTOR
            // found - which here is none of them. A refusal is the correct answer for input the
            // detector cannot classify; guessing would be the silent option.
            for (JsonReconstructor.CollisionPolicy policy : JsonReconstructor.CollisionPolicy.values()) {
                assertThrows(JsonReconstructor.KeyCollisionException.class,
                        () -> with(policy).reconstruct(leafFirst), policy + " / " + shape);
                assertThrows(JsonReconstructor.KeyCollisionException.class,
                        () -> with(policy).reconstruct(branchFirst), policy + " / " + shape);
            }
        }
    }

    @Test
    @DisplayName("BACKSTOP: reconstruct does not mutate the map, or the nested values, it was handed")
    void reconstructDoesNotMutateTheMapItWasHanded() {
        // The old descent adopted a caller's Map as its write target - `current = (Map) existing`
        // - so a longer key wrote INTO an object the caller still owns. Measured at bd5b070:
        //
        //   inner BEFORE {zz=9}  ->  reconstruct  ->  inner AFTER {zz=9, c=1}
        //
        // Two consequences, both bad on their own. Running reconstruct twice on the same input
        // returned two different answers, and the guard that reports shadowedKeys was inspecting
        // an object whose contents the caller can change underneath it.
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("zz", 9);
        Map<String, Object> flat = ordered("a\\b", inner, "a\\b_c", "1");

        assertThrows(JsonReconstructor.KeyCollisionException.class,
                () -> defaults().reconstruct(flat));
        assertEquals(1, inner.size(),
                "reconstruct wrote into the caller's nested map: " + inner);
        assertEquals(9, inner.get("zz"));

        // The no-collision case is the other half: a caller Map that nothing descends into must
        // come back untouched too, and reconstruct must be idempotent on it.
        Map<String, Object> keep = new LinkedHashMap<>();
        keep.put("zz", 9);
        Map<String, Object> plain = ordered("a", keep, "b", "1");
        Map<String, Object> once = defaults().reconstruct(plain);
        Map<String, Object> twice = defaults().reconstruct(plain);
        assertEquals(1, keep.size(), "reconstruct mutated a caller Map it did not descend into");
        assertEquals(once.toString(), twice.toString(),
                "reconstruct is not idempotent on the same input map");
    }

    // ------------------------------------------------------------------ the three real producers

    @Test
    @DisplayName("PRODUCER 1: the flattener sentinel base key - {\"data\":[[{\"name\":\"A\"}],\"text\"]}")
    void theSentinelBaseKeyCollisionIsRefusedRatherThanDroppingTheFieldColumn() {
        Map<String, Object> flat = MapFlattener.builder().build()
                .flatten(Map.of("data", java.util.List.of(java.util.List.of(Map.of("name", "A")), "text")));

        // The emit side is unchanged and correct: two distinct, injective keys.
        assertEquals("[[\"A\"],null]", flat.get("data_name"));
        assertEquals("[[null],\"text\"]", flat.get("data"));

        // Against unfixed HEAD this returned {"data":[[null],"text"]} - data_name annihilated,
        // silently, because the live flat map yields data_name BEFORE data.
        assertThrows(JsonReconstructor.KeyCollisionException.class,
                () -> defaults().reconstruct(flat));
    }

    @Test
    @DisplayName("PRODUCER 2: NO SENTINEL - a nullable nested record inside an array of records")
    void aNullableNestedRecordInAnArrayOfRecordsCollidesWithNoSentinelInvolved() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("orders", java.util.List.of(
                orderedMap("id", 1, "ship", Map.of("city", "NY")),
                orderedMapWithNull("id", 2, "ship")));

        Map<String, Object> flat = MapFlattener.builder().build().flatten(source);
        assertNotNull(flat.get("orders_ship"), flat.keySet().toString());
        assertNotNull(flat.get("orders_ship_city"), flat.keySet().toString());

        // Against unfixed HEAD this returned {"orders":[{"id":1,"ship":null},{"id":2,"ship":null}]}
        // - the nested record {"city":"NY"} deleted from the document with no trace. This is the
        // reason the collision is filed on its own and not under the sentinel entry: there is no
        // sentinel anywhere in this document.
        assertThrows(JsonReconstructor.KeyCollisionException.class,
                () -> defaults().reconstruct(flat));
    }

    @Test
    @DisplayName("PRODUCER 3: NO SENTINEL - the LOWER_CASE dedup suffix emits an unescaped _2")
    void theLowerCaseDedupSuffixCollidesWithNoSentinelInvolved() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("Id", 1);
        source.put("id", 2);

        Map<String, Object> flat = MapFlattener.builder()
                .namingStrategy(MapFlattener.FieldNamingStrategy.LOWER_CASE).build()
                .flatten(source);
        assertEquals(java.util.Set.of("id", "id_2"), flat.keySet());

        // Against unfixed HEAD this returned {"id":{"_value":1,"2":2}} - a fabricated _value key
        // and a numeric-string key, neither of which appears in the source. The producer here is
        // generateUniqueKey appending a RAW separator after encoding; that is a flatten-side
        // defect of its own and is filed separately as [BL-024]. This test pins only that the
        // reconstructor no longer corrupts it silently.
        assertThrows(JsonReconstructor.KeyCollisionException.class,
                () -> defaults().reconstruct(flat));
    }

    // ------------------------------------------------------------------ the fence

    @Test
    @DisplayName("FENCE: reconstructToJson and quickReconstruct surface the refusal by type")
    void everyPublicEntryPointSurfacesTheRefusalByType() {
        Map<String, Object> flat = ordered("a_b", "1", "a", "2");

        assertThrows(JsonReconstructor.KeyCollisionException.class,
                () -> defaults().reconstructToJson(flat));
        assertThrows(JsonReconstructor.KeyCollisionException.class,
                () -> JsonReconstructor.quickReconstruct(flat));
        assertThrows(JsonReconstructor.KeyCollisionException.class,
                () -> JsonReconstructor.quickReconstructToJson(flat));
        assertThrows(JsonReconstructor.KeyCollisionException.class,
                () -> JsonReconstructor.create().from(flat).toMap());
    }

    @Test
    @DisplayName("FENCE: a map with no collision is untouched by any of this")
    void aMapWithNoCollisionIsUntouched() {
        Map<String, Object> flat = new LinkedHashMap<>();
        flat.put("user_name", "Alice");
        flat.put("user_age", 30);
        flat.put("active", true);

        Map<String, Object> out = defaults().reconstruct(flat);
        assertEquals(Map.of("user", Map.of("name", "Alice", "age", 30), "active", true), out);
    }

    // ------------------------------------------------------------------ helpers

    private static Map<String, Object> orderedMap(String k1, Object v1, String k2, Object v2) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(k1, v1);
        m.put(k2, v2);
        return m;
    }

    private static Map<String, Object> orderedMapWithNull(String k1, Object v1, String nullKey) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(k1, v1);
        m.put(nullKey, null);
        return m;
    }

    @SuppressWarnings("unchecked")
    private static boolean containsKeyAnywhere(Object node, String key) {
        if (node instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) node;
            if (map.containsKey(key)) {
                return true;
            }
            for (Object v : map.values()) {
                if (containsKeyAnywhere(v, key)) {
                    return true;
                }
            }
            return false;
        }
        if (node instanceof Iterable) {
            for (Object v : (Iterable<?>) node) {
                if (containsKeyAnywhere(v, key)) {
                    return true;
                }
            }
        }
        return false;
    }
}
