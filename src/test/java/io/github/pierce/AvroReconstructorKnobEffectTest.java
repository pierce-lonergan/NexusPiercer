package io.github.pierce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BL-012 SETTLED. The four AvroReconstructor knobs recorded as "no observed effect — UNPROVEN"
 * are measured here against documents chosen to REACH their branches.
 *
 * <p>WHY THE ORIGINAL PROBE FOUND NOTHING. It ran five documents — array of int, array of
 * string, nested record, empty flattened map against a schema with a required field, and a
 * schema with a field default — and saw byte-identical output at both settings for
 * {@code strictValidation}, {@code allowMissingFields}, {@code useSchemaDefaults} and
 * {@code enableVerification}. Three separate reasons, all now demonstrated below:
 * <ul>
 *   <li>All five documents were WELL-FORMED, so no error branch was ever entered. That is why
 *       strictValidation looked inert; it is not.</li>
 *   <li>The one document chosen to reach the missing-field branches — the empty flattened map —
 *       is the one document that provably CANNOT: {@code reconstructToMap} short-circuits an
 *       empty map into {@code createEmptyRecord}, which reads neither knob, builds no
 *       GenericRecord, and silently omits required no-default fields.</li>
 *   <li>useSchemaDefaults DID differ, in a way the renderer cannot see: the two settings produce
 *       the same VALUE as different JAVA TYPES.</li>
 * </ul>
 *
 * <p><b>THREE OF THE FOUR PINS WERE REPAIRED IN 2.1.0 AND THIS FILE MOVED WITH THEM.</b> The
 * file's own header used to say "when this is repaired, this assertion flips to assertTrue and the
 * corpus rows must be re-recorded in the same commit". That is what happened: NP-023 (a defaulted
 * ENUM arriving as a raw String, so the datum did not validate at the SHIPPED DEFAULT), NP-024
 * (allowMissingFields selecting WHICH exception fires rather than whether one does) and NP-025
 * (an empty flattened map bypassing both knobs) are fixed, and the assertions below are rewritten
 * to the repaired behaviour rather than deleted or weakened. Each one records what it used to
 * measure, so the measurement is not erased by the repair.
 *
 * <p>What is still pinned and NOT repaired: {@code enableVerification} gates one method only, and
 * {@code compareFlattenedMaps} is unaffected by it.</p>
 */
@DisplayName("BL-012 the four AvroReconstructor knobs, measured against branch-reaching documents")
class AvroReconstructorKnobEffectTest {

    private static Map<String, Object> flat(String... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static Schema parse(String json) {
        return new Schema.Parser().parse(json);
    }

    private static final Schema INT_RECORD = parse(
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"n\",\"type\":\"int\"}]}");

    private static final Schema TWO_REQUIRED = parse(
            "{\"type\":\"record\",\"name\":\"R2\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"other\",\"type\":\"string\"}]}");

    private static final Schema STRING_DEFAULT = parse(
            "{\"type\":\"record\",\"name\":\"R3\",\"fields\":["
                    + "{\"name\":\"s\",\"type\":\"string\",\"default\":\"unknown\"},"
                    + "{\"name\":\"n\",\"type\":\"int\"}]}");

    private static final Schema ENUM_DEFAULT = parse(
            "{\"type\":\"record\",\"name\":\"R4\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"color\",\"type\":{\"type\":\"enum\",\"name\":\"Color\","
                    + "\"symbols\":[\"RED\",\"BLUE\"]},\"default\":\"RED\"}]}");

    // ===================== strictValidation: LIVE. Backlog was wrong. =====================

    @Nested
    @DisplayName("strictValidation is LIVE - move it out of the 'unproven' four")
    class StrictValidation {

        @Test
        @DisplayName("SYNTHETIC VIOLATION: a malformed scalar diverges between the two settings")
        void malformedScalarDiverges() {
            // true: throws. Measured chain is
            //   ReconstructionException("Failed to reconstruct data for schema: R")
            //     <- IllegalArgumentException("Cannot convert 'abc' to INT at: n")
            //       <- NumberFormatException("For input string: \"abc\"")
            Throwable t = assertThrows(RuntimeException.class,
                    () -> AvroReconstructor.builder().strictValidation(true).build()
                            .reconstructToMap(flat("n", "abc"), INT_RECORD));
            Throwable root = t;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            assertInstanceOf(NumberFormatException.class, root,
                    "expected the coercion failure at the root of the chain, got " + root);

            // false: substitutes the type default. Asserting the EXACT value, not merely
            // "no exception" - a non-strict path that returned null or dropped the field would
            // pass a doesNotThrow-only test.
            Map<String, Object> lenient = AvroReconstructor.builder().strictValidation(false)
                    .build().reconstructToMap(flat("n", "abc"), INT_RECORD);
            assertEquals(0, lenient.get("n"),
                    "strictValidation(false) must substitute the INT type default, got " + lenient);
        }

        @Test
        @DisplayName("GOOD INPUT CONTROL: a well-formed document is identical at both settings")
        void wellFormedIsIdentical() {
            // This is the leg that explains the original null result. Every one of the five
            // probe documents looked like this one.
            Map<String, Object> strict = AvroReconstructor.builder().strictValidation(true)
                    .build().reconstructToMap(flat("n", "7"), INT_RECORD);
            Map<String, Object> lenient = AvroReconstructor.builder().strictValidation(false)
                    .build().reconstructToMap(flat("n", "7"), INT_RECORD);
            assertEquals(strict, lenient,
                    "a well-formed document must not distinguish the settings - if it does, the "
                            + "divergence test above is measuring the wrong thing");
        }
    }

    // ===================== allowMissingFields: does NOT allow missing fields =====================

    @Nested
    @DisplayName("allowMissingFields now means FAIL vs FILL - repaired in 2.1.0")
    class AllowMissingFields {

        @Test
        @DisplayName("false fails with the flattened path; true substitutes the Avro type default")
        void oneValueFailsAndTheOtherFills() {
            // WHAT THIS USED TO MEASURE, kept so the repair does not erase it: BOTH values threw.
            //   true  -> org.apache.avro.AvroMissingFieldException("Field id type:STRING pos:0
            //            not set and has no default value"), leaking from GenericRecordBuilder
            //            .build(), which sits OUTSIDE the per-field try, so the caller lost the
            //            flattened path entirely.
            //   false -> IllegalStateException("Required field missing and no default: id").
            // A flag named allowMissingFields that at neither value allows a missing field is a
            // control satisfied without being met. Now each value has a real, different outcome.
            Throwable strict = assertThrows(RuntimeException.class,
                    () -> AvroReconstructor.builder().allowMissingFields(false).build()
                            .reconstructToMap(flat("other", "x"), TWO_REQUIRED));
            assertInstanceOf(AvroReconstructor.ReconstructionException.class, strict);
            assertTrue(strict.getMessage().contains("id"),
                    "the failure must name the field; got " + strict.getMessage());
            assertFalse(chainOf(strict).contains("AvroMissingFieldException"),
                    "Avro's own builder exception must no longer escape; chain was "
                            + chainOf(strict));

            Map<String, Object> filled = AvroReconstructor.builder().allowMissingFields(true)
                    .build().reconstructToMap(flat("other", "x"), TWO_REQUIRED);
            assertEquals("", String.valueOf(filled.get("id")),
                    "allowMissingFields(true) substitutes the Avro STRING type default; got "
                            + filled);
        }

        @Test
        @DisplayName("the SHIPPED DEFAULT is the failing one, and it still fails")
        void theShippedDefaultStillFails() {
            // The default flipped from true to false, and the OUTCOME at the shipped default is
            // unchanged - it failed before and it fails now. Keeping true as the default while
            // giving true a tolerant meaning would have turned a loud failure into a silently
            // invented "" at the shipped configuration.
            assertThrows(AvroReconstructor.ReconstructionException.class,
                    () -> AvroReconstructor.builder().build()
                            .reconstructToMap(flat("other", "x"), TWO_REQUIRED));
        }

        @Test
        @DisplayName("true refuses to invent a value where Avro has no type default")
        void trueRefusesToInventAnEnum() {
            Schema requiredEnum = parse(
                    "{\"type\":\"record\",\"name\":\"REk\",\"fields\":["
                            + "{\"name\":\"id\",\"type\":\"string\"},"
                            + "{\"name\":\"color\",\"type\":{\"type\":\"enum\","
                            + "\"name\":\"ColorK\",\"symbols\":[\"RED\",\"BLUE\"]}}]}");
            Throwable t = assertThrows(RuntimeException.class,
                    () -> AvroReconstructor.builder().allowMissingFields(true).build()
                            .reconstructToMap(flat("id", "x"), requiredEnum));
            assertTrue(t.getMessage().contains("ENUM"),
                    "quietly picking the first symbol would ship the pathology the repair "
                            + "removes; got " + t.getMessage());
        }

        private String chainOf(Throwable t) {
            StringBuilder sb = new StringBuilder();
            for (Throwable c = t; c != null; c = c.getCause()) {
                sb.append(c.getClass().getName()).append(": ").append(c.getMessage()).append(" | ");
            }
            return sb.toString();
        }
    }

    // ===================== the empty-map bypass: the reason the probe found nothing ==============

    @Nested
    @DisplayName("An EMPTY flattened map now takes the same path as any other - repaired in 2.1.0")
    class EmptyMapBypass {

        @Test
        @DisplayName("empty and one-unrelated-key give the SAME answer at each setting")
        void emptyMapNoLongerBypassesTheKnobs() {
            // WHAT THIS USED TO MEASURE: at BOTH settings an empty map returned {} - the required
            // field silently dropped - while the SAME schema with one unrelated key present threw.
            // reconstructToMap short-circuited an empty map into createEmptyRecord, which
            // consulted neither knob and never built a GenericRecord. That is the finding that
            // explained the whole original null result: the one document the earlier probe chose
            // to reach the missing-field branches was the one document that provably could not.
            // createEmptyRecord is now deleted and the short-circuit with it.
            for (boolean allow : new boolean[]{true, false}) {
                AvroReconstructor r = AvroReconstructor.builder()
                        .allowMissingFields(allow).build();

                String fromEmpty = outcome(() -> r.reconstructToMap(
                        new LinkedHashMap<>(), TWO_REQUIRED));
                String fromOneKey = outcome(() -> r.reconstructToMap(
                        flat("other", "x"), TWO_REQUIRED));

                if (allow) {
                    assertTrue(fromEmpty.contains("id="),
                            "allowMissingFields(true) must fill, not drop; got " + fromEmpty);
                } else {
                    assertTrue(fromEmpty.startsWith("THREW") && fromEmpty.contains("id"),
                            "allowMissingFields(false) on an empty map must fail naming the "
                                    + "field; got " + fromEmpty);
                }
                // The parity that matters is what happens to `id` - the field that is missing in
                // both inputs. `other` legitimately differs (absent vs "x") and comparing whole
                // renderings would only measure that.
                assertEquals(fromOneKey.startsWith("THREW"), fromEmpty.startsWith("THREW"),
                        "empty and one-unrelated-key must agree at allowMissingFields(" + allow
                                + "): empty=" + fromEmpty + " oneKey=" + fromOneKey);
            }
        }

        private String outcome(java.util.function.Supplier<Map<String, Object>> call) {
            try {
                return String.valueOf(call.get());
            } catch (RuntimeException e) {
                return "THREW " + e.getClass().getSimpleName() + ": " + e.getMessage();
            }
        }
    }

    // ===================== useSchemaDefaults: cannot suppress, and the default is wrong =========

    @Nested
    @DisplayName("useSchemaDefaults means something at both values - repaired in 2.1.0")
    class UseSchemaDefaults {

        @Test
        @DisplayName("true supplies a schema-correct default; false genuinely suppresses it")
        void bothSettingsSupplyTheDefault() {
            // WHAT THIS USED TO MEASURE, and it is a MEASURED CORRECTION to BL-012's blanket
            // claim. It recorded that BOTH settings supplied "unknown", differing only in the
            // Java type (String at true from field.defaultVal(), Utf8 at false from Avro's own
            // path re-supplying it inside build()) - and concluded the knob could not suppress a
            // default. Measured while repairing this: it always COULD on a NULLABLE field, where
            // the ladder set null explicitly. It could not on a NON-nullable one, because leaving
            // the slot unset lets GenericRecordBuilder.build() fill it in. The knob's behaviour
            // therefore depended silently on nullability. It now means the same thing at both:
            // "do not consult the schema default", after which the field is simply MISSING and
            // allowMissingFields decides.
            Map<String, Object> on = AvroReconstructor.builder().useSchemaDefaults(true).build()
                    .reconstructToMap(flat("n", "1"), STRING_DEFAULT);
            assertEquals("unknown", on.get("s").toString(),
                    "useSchemaDefaults(true) supplies the default");
            assertInstanceOf(org.apache.avro.util.Utf8.class, on.get("s"),
                    "and it is now the SCHEMA-CORRECT type - Avro's Utf8, decoded through "
                            + "GenericData.getDefaultValue, not JacksonUtils' plain String");

            Throwable suppressed = assertThrows(RuntimeException.class,
                    () -> AvroReconstructor.builder().useSchemaDefaults(false).build()
                            .reconstructToMap(flat("n", "1"), STRING_DEFAULT),
                    "useSchemaDefaults(false) on a NON-nullable defaulted field now genuinely "
                            + "suppresses the default, which leaves the field missing");
            assertTrue(suppressed.getMessage().contains("useSchemaDefaults(false)"),
                    "and it must say WHY rather than claiming there is no default - the old "
                            + "message was 'Required field missing and no default: s' about a "
                            + "field that has one. Got: " + suppressed.getMessage());

            Map<String, Object> lenient = AvroReconstructor.builder()
                    .useSchemaDefaults(false).allowMissingFields(true).build()
                    .reconstructToMap(flat("n", "1"), STRING_DEFAULT);
            assertEquals("", String.valueOf(lenient.get("s")),
                    "with tolerance asked for, the TYPE default is used instead of the SCHEMA "
                            + "default - which is the distinction the knob exists to draw");
        }

        @Test
        @DisplayName("THE STING: at the SHIPPED DEFAULT, an enum default produces an UNWRITABLE datum")
        void enumDefaultIsUnwritableAtTheShippedDefault() {
            // PINNED LIVE DEFECT AT DEFAULT CONFIGURATION - the sharpest thing in this file.
            //
            // field.defaultVal() routes through JacksonUtils.toObject, which returns a plain
            // java.lang.String for an ENUM default (and byte[] for FIXED/BYTES, LinkedHashMap for
            // a record-typed default). GenericRecordBuilder.set does not type-check, so the
            // record is built carrying a raw String where an EnumSymbol belongs.
            //
            // Consequence: GenericData.validate returns FALSE and the record cannot be binary
            // encoded. This is the "unwritable datum" defect class the corpus already tracks.
            // Ironically useSchemaDefaults(FALSE) reconstructs this correctly - the DEFAULT
            // setting is the broken one.
            GenericRecord rec = AvroReconstructor.builder().build()
                    .reconstruct(flat("id", "x"), ENUM_DEFAULT);

            Object color = rec.get("color");
            assertInstanceOf(GenericData.EnumSymbol.class, color,
                    "REPAIRED: it used to land as a raw java.lang.String, because "
                            + "field.defaultVal() routes through JacksonUtils.toObject and "
                            + "GenericRecordBuilder.set does not type-check");
            assertTrue(GenericData.get().validate(ENUM_DEFAULT, rec),
                    "THE FLIP THIS FILE ASKED FOR. The old assertion was assertFalse with the "
                            + "note 'when this is repaired, this assertion flips to assertTrue "
                            + "and the corpus rows must be re-recorded in the same commit'. Both "
                            + "halves happened in 2.1.0.");
        }
    }

    // ===================== enableVerification: live, but gates one method only ==================

    @Nested
    @DisplayName("enableVerification gates verifyReconstruction ONLY")
    class EnableVerification {

        @Test
        @DisplayName("it blocks verifyReconstruction, leaves compareFlattenedMaps working, and never touches reconstruction")
        void gatesOnlyVerifyReconstruction() {
            AvroReconstructor off = AvroReconstructor.builder().enableVerification(false).build();
            AvroReconstructor on = AvroReconstructor.builder().enableVerification(true).build();

            assertThrows(IllegalStateException.class,
                    () -> off.verifyReconstruction(flat("n", "1"), flat("n", "1"), INT_RECORD),
                    "the flag must actually gate the one method it is wired to");

            // The other public verification entry point is NOT gated. That inconsistency is the
            // signature pathology inside the verification gate itself, and it is pinned rather
            // than repaired: extending the gate would turn a currently-working call into a throw
            // for anyone who set the flag false.
            assertTrue(off.compareFlattenedMaps(flat("n", "1"), flat("n", "1")) != null,
                    "compareFlattenedMaps is unaffected by enableVerification(false) - PINNED, "
                            + "see docs/BACKLOG.md");

            // The assertion the original probe should have made explicit: the flag provably does
            // not touch reconstruction, which is why five reconstruct-only documents saw nothing.
            assertEquals(on.reconstructToMap(flat("n", "1"), INT_RECORD),
                    off.reconstructToMap(flat("n", "1"), INT_RECORD),
                    "enableVerification has no effect on reconstructToMap at either value");
        }
    }
}
