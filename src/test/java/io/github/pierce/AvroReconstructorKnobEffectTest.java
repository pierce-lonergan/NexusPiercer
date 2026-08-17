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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
 * <p><b>THESE ARE PINS ON MEASURED BEHAVIOUR, NOT CLAIMS THAT THE BEHAVIOUR IS CORRECT.</b>
 * Three of the four knobs do not do what their names say, and one of them produces an invalid
 * datum at the SHIPPED DEFAULT configuration. Each test below states which it is. The repairs are
 * behaviour changes on released API and are tracked in docs/BACKLOG.md; the job of these tests is
 * to make the current behaviour impossible to re-discover by accident, and to fail loudly the
 * moment someone changes it without moving the paperwork.
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
    @DisplayName("allowMissingFields selects an exception, not an outcome - PINNED DEFECT")
    class AllowMissingFields {

        @Test
        @DisplayName("BOTH values fail on a missing required field; only the exception differs")
        void bothValuesFail() {
            // PINNED KNOWN DEFECT, not a claim of correctness. The flag is named
            // "allowMissingFields" and at neither value does it allow a missing field.
            //
            // true : AvroMissingFieldException("Field id type:STRING pos:0 not set and has no
            //        default value") escaping from GenericRecordBuilder.build(), which sits
            //        OUTSIDE the per-field try - so the caller also loses the field path.
            // false: IllegalStateException("Required field missing and no default: id"), which
            //        does carry the path.
            Throwable permissive = rootOf(assertThrows(RuntimeException.class,
                    () -> AvroReconstructor.builder().allowMissingFields(true).build()
                            .reconstructToMap(flat("other", "x"), TWO_REQUIRED)));
            Throwable strict = rootOf(assertThrows(RuntimeException.class,
                    () -> AvroReconstructor.builder().allowMissingFields(false).build()
                            .reconstructToMap(flat("other", "x"), TWO_REQUIRED)));

            assertEquals("org.apache.avro.AvroMissingFieldException",
                    permissive.getClass().getName(),
                    "allowMissingFields(true) currently leaks Avro's own exception from build()");
            assertInstanceOf(IllegalStateException.class, strict);
            assertTrue(strict.getMessage().contains("id"),
                    "the strict path names the field path; the permissive path does not, which "
                            + "is the second half of this defect");
            assertNotEquals(permissive.getClass(), strict.getClass(),
                    "the flag DOES have an observable effect - it picks which failure you get. "
                            + "That is why 'no observed effect' was the wrong conclusion, and "
                            + "'it works' would be equally wrong.");
        }

        private Throwable rootOf(Throwable t) {
            Throwable r = t;
            while (r.getCause() != null) {
                r = r.getCause();
            }
            return r;
        }
    }

    // ===================== the empty-map bypass: the reason the probe found nothing ==============

    @Nested
    @DisplayName("An EMPTY flattened map bypasses both knobs entirely - PINNED DEFECT")
    class EmptyMapBypass {

        @Test
        @DisplayName("a required no-default field is silently omitted at BOTH settings")
        void emptyMapSilentlyOmitsRequiredField() {
            // PINNED KNOWN DEFECT. reconstructToMap short-circuits an empty flattened map into
            // createEmptyRecord, which consults NEITHER useSchemaDefaults NOR allowMissingFields,
            // never builds a GenericRecord, and returns a partial map with no error.
            //
            // THIS IS THE FINDING THAT EXPLAINS THE WHOLE ORIGINAL NULL RESULT: the one document
            // the earlier probe chose to reach the missing-field branches is the one document
            // that provably cannot reach them.
            for (boolean allow : new boolean[]{true, false}) {
                Map<String, Object> out = AvroReconstructor.builder()
                        .allowMissingFields(allow).build()
                        .reconstructToMap(new LinkedHashMap<>(), TWO_REQUIRED);
                assertTrue(out.isEmpty(),
                        "allowMissingFields(" + allow + ") on an empty map returns a partial map "
                                + "with the required field silently dropped; got " + out);
            }

            // CONTRAST, and this is what makes the above a defect rather than a design: the SAME
            // schema with a single unrelated key present does fail loudly.
            assertThrows(RuntimeException.class,
                    () -> AvroReconstructor.builder().build()
                            .reconstructToMap(flat("other", "x"), TWO_REQUIRED),
                    "a non-empty map reaches the real path and fails; only the empty map is "
                            + "silently tolerated. Same schema, same missing field, two outcomes.");
        }
    }

    // ===================== useSchemaDefaults: cannot suppress, and the default is wrong =========

    @Nested
    @DisplayName("useSchemaDefaults cannot suppress a default - PINNED DEFECT")
    class UseSchemaDefaults {

        @Test
        @DisplayName("both settings supply the default; they differ only in the Java type")
        void bothSettingsSupplyTheDefault() {
            // PINNED KNOWN DEFECT. At true the reconstructor sets field.defaultVal() itself;
            // at false it leaves the field unset and GenericRecordBuilder.build() re-supplies
            // the default anyway via RecordBuilderBase.defaultValue -> GenericData.getDefaultValue.
            // There is no way to tell GenericRecordBuilder not to.
            Map<String, Object> on = AvroReconstructor.builder().useSchemaDefaults(true).build()
                    .reconstructToMap(flat("n", "1"), STRING_DEFAULT);
            Map<String, Object> off = AvroReconstructor.builder().useSchemaDefaults(false).build()
                    .reconstructToMap(flat("n", "1"), STRING_DEFAULT);

            assertEquals("unknown", on.get("s").toString(),
                    "useSchemaDefaults(true) supplies the default");
            assertEquals("unknown", off.get("s").toString(),
                    "useSchemaDefaults(FALSE) supplies it too - the knob does not suppress it");

            // The difference the renderer could not see, and which is why the original probe
            // recorded "byte-identical output": same text, different runtime type.
            assertInstanceOf(String.class, on.get("s"),
                    "true takes field.defaultVal(), which JacksonUtils returns as a plain String");
            assertInstanceOf(org.apache.avro.util.Utf8.class, off.get("s"),
                    "false goes through Avro's own default path, which yields the schema-correct "
                            + "Utf8. 'Byte-identical output' was a true observation of a real "
                            + "difference the renderer cannot see.");
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
            assertInstanceOf(String.class, color,
                    "measured: the enum default lands as a raw String, not a GenericData.EnumSymbol");
            assertFalse(GenericData.get().validate(ENUM_DEFAULT, rec),
                    "PINNED DEFECT: the reconstructed record does NOT validate against its own "
                            + "schema at the shipped default configuration, so it cannot be "
                            + "binary-encoded. When this is repaired, this assertion flips to "
                            + "assertTrue and the corpus rows for defaulted enum/fixed/record "
                            + "fields must be re-recorded in the same commit.");
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
