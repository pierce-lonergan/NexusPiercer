package io.github.pierce;

import org.apache.avro.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NP-025. {@code reconstructToMap} short-circuited a null-or-empty flattened map into
 * {@code createEmptyRecord}, which read neither knob, built no GenericRecord, and OMITTED any
 * field that was neither defaulted nor nullable. The SAME schema with a single unrelated key
 * present failed loudly. One input shape was silently tolerated and its sibling was not.
 *
 * <p>NOTE, because the SECURITY row states this too narrowly: "returns {} with no error" is only
 * true of a schema that happens to have no defaulted and no nullable field. createEmptyRecord DID
 * return defaulted and nullable fields. The defect is the SILENT OMISSION of required no-default
 * fields, not the emptiness of the result.</p>
 */
@DisplayName("NP-025 an empty flattened map takes the same path as a non-empty one")
class AvroReconstructorEmptyInputTest {

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

    private static final Schema ALL_OPTIONAL = parse(
            "{\"type\":\"record\",\"name\":\"Opt\",\"fields\":["
                    + "{\"name\":\"region\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"color\",\"type\":{\"type\":\"enum\",\"name\":\"Color\","
                    + "\"symbols\":[\"RED\",\"BLUE\"]},\"default\":\"RED\"}]}");

    @Test
    @DisplayName("an empty map fails exactly like a one-unrelated-key map")
    void anEmptyMapFailsExactlyLikeAOneUnrelatedKeyMap() {
        AvroReconstructor r = AvroReconstructor.builder().build();

        Throwable fromEmpty = assertThrows(RuntimeException.class,
                () -> r.reconstructToMap(new LinkedHashMap<>(), TWO_REQUIRED),
                "an empty map must reach the real path");
        Throwable fromOneKey = assertThrows(RuntimeException.class,
                () -> r.reconstructToMap(flat("other", "x"), TWO_REQUIRED));

        assertEquals(fromOneKey.getClass(), fromEmpty.getClass(),
                "the two inputs must fail the same way; empty=" + fromEmpty + " oneKey=" + fromOneKey);
        assertTrue(fromEmpty.getMessage().contains("id"),
                "the empty case must name the missing field; got " + fromEmpty.getMessage());
        assertTrue(fromOneKey.getMessage().contains("id"));
    }

    @Test
    @DisplayName("an empty map against an all-optional schema still returns a populated map")
    void anEmptyMapAgainstAnAllOptionalSchemaStillReturnsAMap() {
        Map<String, Object> out = AvroReconstructor.builder().build()
                .reconstructToMap(new LinkedHashMap<>(), ALL_OPTIONAL);

        assertNull(out.get("region"), () -> "region must be a real null, got "
                + (out.get("region") == null ? "null" : out.get("region").getClass().getName()));
        assertInstanceOf(org.apache.avro.generic.GenericData.EnumSymbol.class, out.get("color"),
                "the enum default must be schema-correct here too - 'make the empty case loud' "
                        + "must not be implemented as 'make the empty case always throw'");
    }

    @Test
    @DisplayName("a null flattened map behaves exactly like an empty one")
    void aNullFlattenedMapBehavesExactlyLikeAnEmptyOne() {
        // Passes before AND after. It pins the decision that null is treated as empty rather
        // than smuggled into a separate argument-validation semantics, and it stops removing the
        // short-circuit from turning null into an accidental NullPointerException.
        AvroReconstructor r = AvroReconstructor.builder().build();
        Map<String, Object> fromNull = r.reconstructToMap(null, ALL_OPTIONAL);
        Map<String, Object> fromEmpty = r.reconstructToMap(new LinkedHashMap<>(), ALL_OPTIONAL);
        assertEquals(String.valueOf(fromEmpty), String.valueOf(fromNull));
    }
}
