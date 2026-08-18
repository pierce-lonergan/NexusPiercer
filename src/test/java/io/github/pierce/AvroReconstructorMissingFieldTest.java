package io.github.pierce;

import org.apache.avro.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NP-024 and the fourth defect in the same ladder.
 *
 * <p>{@code allowMissingFields} promised tolerance and delivered it at neither value: {@code true}
 * left the slot unset so {@code GenericRecordBuilder.build()} - which sits OUTSIDE the per-field
 * try - leaked {@code org.apache.avro.AvroMissingFieldException} with no flattened path, and
 * {@code false} threw {@code IllegalStateException}. A flag that only ever selects WHICH exception
 * you get is a control satisfied without being met.</p>
 *
 * <p>The fourth defect: {@code useSchemaDefaults} was tested BEFORE {@code hasDefaultValue}, so
 * {@code useSchemaDefaults(false)} on a defaulted non-nullable field with
 * {@code allowMissingFields(false)} emitted "Required field missing and no default: color" for a
 * field that HAS a default. The message was a lie, masked only because allowMissingFields
 * defaulted to true.</p>
 */
@DisplayName("NP-024 missing required fields fail with the path, and allowMissingFields means something")
class AvroReconstructorMissingFieldTest {

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

    private static final Schema TWO_REQUIRED = parse(
            "{\"type\":\"record\",\"name\":\"R2\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"other\",\"type\":\"string\"}]}");

    private static final Schema NESTED_REQUIRED = parse(
            "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
                    + "{\"name\":\"user\",\"type\":{\"type\":\"record\",\"name\":\"User\",\"fields\":["
                    + "{\"name\":\"name\",\"type\":\"string\"},"
                    + "{\"name\":\"nick\",\"type\":\"string\"}]}}]}");

    private static final Schema ENUM_DEFAULT = parse(
            "{\"type\":\"record\",\"name\":\"R4\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"color\",\"type\":{\"type\":\"enum\",\"name\":\"Color\","
                    + "\"symbols\":[\"RED\",\"BLUE\"]},\"default\":\"RED\"}]}");

    private static final Schema REQUIRED_ENUM = parse(
            "{\"type\":\"record\",\"name\":\"RE\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"color\",\"type\":{\"type\":\"enum\",\"name\":\"Color\","
                    + "\"symbols\":[\"RED\",\"BLUE\"]}}]}");

    private static String chainText(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            sb.append(c.getClass().getName()).append(": ").append(c.getMessage()).append(" || ");
        }
        return sb.toString();
    }

    @Test
    @DisplayName("a missing nested required field names the FLATTENED PATH, not just the field name")
    void missingRequiredFieldNamesTheFlattenedPathAtTheShippedDefault() {
        Throwable t = assertThrows(RuntimeException.class,
                () -> AvroReconstructor.builder().build()
                        .reconstructToMap(flat("user_name", "x"), NESTED_REQUIRED));
        String chain = chainText(t);
        assertTrue(chain.contains("user_nick"),
                "the failure must name the flattened path user_nick; chain was " + chain);
    }

    @Test
    @DisplayName("a root-level missing required field no longer leaks Avro's own exception")
    void rootLevelMissingFieldNoLongerLeaksAvrosOwnException() {
        Throwable t = assertThrows(RuntimeException.class,
                () -> AvroReconstructor.builder().build()
                        .reconstructToMap(flat("other", "x"), TWO_REQUIRED));
        String chain = chainText(t);
        assertFalse(chain.contains("AvroMissingFieldException"),
                "Avro's builder exception must not escape unwrapped; chain was " + chain);
        assertInstanceOf(AvroReconstructor.ReconstructionException.class, t,
                "the caller must see our exception type; got " + t.getClass().getName());
        assertTrue(t.getMessage().contains("id"),
                "the outer message must name the missing field; got " + t.getMessage());
    }

    @Test
    @DisplayName("allowMissingFields(true) actually tolerates and substitutes the Avro type default")
    void allowMissingFieldsTrueActuallyTolerates() {
        Map<String, Object> out = AvroReconstructor.builder().allowMissingFields(true).build()
                .reconstructToMap(flat("other", "x"), TWO_REQUIRED);
        assertNotNull(out);
        assertTrue(out.containsKey("id"), "the tolerated field must be present; got " + out);
        assertEquals("", String.valueOf(out.get("id")),
                "the Avro STRING type default is the empty string; got " + out.get("id"));
    }

    @Test
    @DisplayName("allowMissingFields(true) refuses to invent an ENUM - no type default exists")
    void allowMissingFieldsTrueRefusesToInventAnEnum() {
        Throwable t = assertThrows(RuntimeException.class,
                () -> AvroReconstructor.builder().allowMissingFields(true).build()
                        .reconstructToMap(flat("id", "x"), REQUIRED_ENUM));
        String chain = chainText(t);
        assertTrue(chain.contains("color"), "must name the field; chain was " + chain);
        assertTrue(chain.contains("ENUM"), "must name the type that has no default; chain was " + chain);
    }

    @Test
    @DisplayName("the SHIPPED DEFAULT still fails on a missing required field - it is not tolerant")
    void theShippedDefaultStillFailsAndIsNotTolerant() {
        // DECLARED HONESTLY: passes before AND after. It is not a reproduction; it is the tripwire
        // that stops a later agent "simplifying" the flag back to a tolerant default and silently
        // inventing data at the shipped configuration.
        assertThrows(RuntimeException.class,
                () -> AvroReconstructor.builder().build()
                        .reconstructToMap(flat("other", "x"), TWO_REQUIRED));
    }

    @Test
    @DisplayName("useSchemaDefaults(false) does not claim there is no default when there is one")
    void useSchemaDefaultsFalseDoesNotClaimThereIsNoDefault() {
        Throwable t = assertThrows(RuntimeException.class,
                () -> AvroReconstructor.builder()
                        .useSchemaDefaults(false).allowMissingFields(false).build()
                        .reconstructToMap(flat("id", "x"), ENUM_DEFAULT));
        String chain = chainText(t);
        assertFalse(chain.contains("no default"),
                "the field HAS a default - the message must not say otherwise; chain was " + chain);
        assertTrue(chain.contains("useSchemaDefaults"),
                "the message must say the default was suppressed by configuration; chain was " + chain);
    }

    @Test
    @DisplayName("useSchemaDefaults(false) genuinely suppresses a default on a NULLABLE field")
    void useSchemaDefaultsFalseSuppressesOnANullableField() {
        Schema nullableDefault = parse(
                "{\"type\":\"record\",\"name\":\"RN\",\"fields\":["
                        + "{\"name\":\"n\",\"type\":\"int\"},"
                        // MEASURED CORRECTION: the default must match the FIRST union branch, so
                        // ["string","null"] not ["null","string"]. Written the other way round,
                        // defaultVal() recurses into the NULL branch and returns null, and the
                        // test measures Avro's own rule rather than this library's knob.
                        + "{\"name\":\"s\",\"type\":[\"string\",\"null\"],\"default\":\"seed\"}]}");

        Map<String, Object> on = AvroReconstructor.builder().useSchemaDefaults(true).build()
                .reconstructToMap(flat("n", "1"), nullableDefault);
        assertEquals("seed", String.valueOf(on.get("s")),
                "useSchemaDefaults(true) supplies the default");

        Map<String, Object> off = AvroReconstructor.builder().useSchemaDefaults(false).build()
                .reconstructToMap(flat("n", "1"), nullableDefault);
        assertEquals(null, off.get("s"),
                "useSchemaDefaults(FALSE) must actually suppress it; got " + off.get("s"));
    }
}
