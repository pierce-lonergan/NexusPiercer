package io.github.pierce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * NP-023. Every schema default AvroReconstructor supplied went through
 * {@code Schema.Field.defaultVal()}, which routes through Avro's {@code JacksonUtils.toObject}
 * and returns the JSON shape rather than the schema-correct runtime type: a
 * {@code java.lang.String} for an ENUM default, a {@code byte[]} for FIXED and BYTES, a
 * {@code LinkedHashMap} for a record default, and the {@code JsonProperties.NULL_VALUE} singleton
 * for {@code "default": null}.
 *
 * <p>The consequence is the pass theme exactly: reconstruction SUCCEEDS, the record looks right in
 * a debugger, {@code GenericData.validate} returns false, and the failure only arrives when
 * somebody tries to write the datum. It bites at the SHIPPED DEFAULT configuration -
 * {@code useSchemaDefaults(false)} reconstructed it correctly, so the default was the broken
 * setting.</p>
 *
 * <p>The repair routes every default through {@code GenericData.getDefaultValue}, which decodes
 * the default JsonNode through a real datum reader, wrapped in {@code deepCopy} because
 * getDefaultValue memoises one shared mutable instance per Field.</p>
 */
@DisplayName("NP-023 schema defaults arrive as their schema-correct runtime type")
class AvroReconstructorDefaultValueTypeTest {

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

    private static void encode(Schema schema, GenericRecord rec) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(schema).write(rec, enc);
        enc.flush();
    }

    private static final Schema ENUM_DEFAULT = parse(
            "{\"type\":\"record\",\"name\":\"R4\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"color\",\"type\":{\"type\":\"enum\",\"name\":\"Color\","
                    + "\"symbols\":[\"RED\",\"BLUE\"]},\"default\":\"RED\"}]}");

    private static final Schema FIXED_AND_BYTES_DEFAULT = parse(
            "{\"type\":\"record\",\"name\":\"RF\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"sig\",\"type\":{\"type\":\"fixed\",\"name\":\"Sig2\",\"size\":2},"
                    + "\"default\":\"\\u0001\\u0002\"},"
                    + "{\"name\":\"blob\",\"type\":\"bytes\",\"default\":\"\\u0003\\u0004\"}]}");

    private static final Schema NULL_DEFAULT = parse(
            "{\"type\":\"record\",\"name\":\"Req\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"region\",\"type\":[\"null\",\"string\"],\"default\":null}]}");

    private static final Schema ARRAY_OF_ENUM_DEFAULT = parse(
            "{\"type\":\"record\",\"name\":\"RA\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"colors\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"enum\",\"name\":\"Color\",\"symbols\":[\"RED\",\"BLUE\"]}},"
                    + "\"default\":[\"RED\",\"BLUE\"]}]}");

    private static final Schema RECORD_DEFAULT = parse(
            "{\"type\":\"record\",\"name\":\"RR\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"meta\",\"type\":{\"type\":\"record\",\"name\":\"Meta\","
                    + "\"fields\":[{\"name\":\"color\",\"type\":{\"type\":\"enum\",\"name\":\"Color\","
                    + "\"symbols\":[\"RED\",\"BLUE\"]}}]},\"default\":{\"color\":\"BLUE\"}}]}");

    private static final Schema ARRAY_DEFAULT = parse(
            "{\"type\":\"record\",\"name\":\"RL\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"},"
                    + "\"default\":[\"a\",\"b\"]}]}");

    @Test
    @DisplayName("an enum default arrives as a GenericData.EnumSymbol and the datum binary-encodes")
    void enumDefaultArrivesAsAnEnumSymbolAndBinaryEncodes() {
        GenericRecord rec = AvroReconstructor.builder().build()
                .reconstruct(flat("id", "x"), ENUM_DEFAULT);

        assertInstanceOf(GenericData.EnumSymbol.class, rec.get("color"),
                "an ENUM default must be an EnumSymbol, not the raw JSON text");
        assertTrue(GenericData.get().validate(ENUM_DEFAULT, rec),
                "the reconstructed record must validate against its own schema");
        assertDoesNotThrow(() -> encode(ENUM_DEFAULT, rec),
                "validate() is not encoding - the datum must actually write");
    }

    @Test
    @DisplayName("FIXED and BYTES defaults arrive as GenericData.Fixed and ByteBuffer")
    void fixedAndBytesDefaultsArriveAsFixedAndByteBuffer() {
        GenericRecord rec = AvroReconstructor.builder().build()
                .reconstruct(flat("id", "x"), FIXED_AND_BYTES_DEFAULT);

        assertInstanceOf(GenericData.Fixed.class, rec.get("sig"),
                "a FIXED default must be a GenericData.Fixed, not a byte[]");
        assertInstanceOf(ByteBuffer.class, rec.get("blob"),
                "a BYTES default must be a ByteBuffer, not a byte[]");
        assertTrue(GenericData.get().validate(FIXED_AND_BYTES_DEFAULT, rec));
        assertDoesNotThrow(() -> encode(FIXED_AND_BYTES_DEFAULT, rec));
    }

    @Test
    @DisplayName("a null default arrives as a real Java null, not the Avro NULL_VALUE sentinel")
    void nullDefaultArrivesAsARealJavaNullNotTheAvroSentinel() {
        Map<String, Object> asMap = AvroReconstructor.builder().build()
                .reconstructToMap(flat("id", "x"), NULL_DEFAULT);
        Object viaMap = asMap.get("region");
        assertNull(viaMap, () -> "reconstructToMap: got "
                + (viaMap == null ? "null" : viaMap.getClass().getName()));

        GenericRecord rec = AvroReconstructor.builder().build()
                .reconstruct(flat("id", "x"), NULL_DEFAULT);
        Object viaRecord = rec.get("region");
        assertNull(viaRecord, () -> "reconstruct: got "
                + (viaRecord == null ? "null" : viaRecord.getClass().getName())
                + " - this leg is what catches a fix applied to reconstructRecord but not to "
                + "mapToGenericRecord, which would silently re-insert the sentinel");
    }

    @Test
    @DisplayName("an array-of-enum default has correctly typed elements")
    void arrayOfEnumDefaultHasCorrectlyTypedElements() {
        GenericRecord rec = AvroReconstructor.builder().build()
                .reconstruct(flat("id", "x"), ARRAY_OF_ENUM_DEFAULT);

        Object colors = rec.get("colors");
        assertInstanceOf(List.class, colors);
        for (Object element : (List<?>) colors) {
            assertInstanceOf(GenericData.EnumSymbol.class, element,
                    "every element of a defaulted array<enum> must be an EnumSymbol");
        }
        assertDoesNotThrow(() -> encode(ARRAY_OF_ENUM_DEFAULT, rec));
    }

    @Test
    @DisplayName("a record-typed default has correct leaf types in the map, and is HONESTLY still a Map in the datum")
    void recordTypedDefaultIsARecordInTheMapAndIsHonestlyStillAMapInTheDatum() {
        Map<String, Object> asMap = AvroReconstructor.builder().build()
                .reconstructToMap(flat("id", "x"), RECORD_DEFAULT);

        Object meta = asMap.get("meta");
        assertInstanceOf(Map.class, meta, "reconstructToMap flattens a nested record to a Map");
        Object leaf = ((Map<?, ?>) meta).get("color");
        assertInstanceOf(GenericData.EnumSymbol.class, leaf,
                "the LEAF types inside a record-typed default are repaired by NP-023");

        // DELIBERATE PIN ON THE BOUNDARY OF THIS FIX, not a claim of correctness.
        // mapToGenericRecord rebuilds only the ROOT record, so a record-typed default is handed
        // back as a LinkedHashMap and still fails validate. That is the separate
        // avro-generic-record-unwritable defect class and is NOT repaired here.
        GenericRecord rec = AvroReconstructor.builder().build()
                .reconstruct(flat("id", "x"), RECORD_DEFAULT);
        assertInstanceOf(Map.class, rec.get("meta"),
                "PINNED LIMIT: reconstruct() still hands back a Map for a nested record");
        assertFalse(GenericData.get().validate(RECORD_DEFAULT, rec),
                "PINNED LIMIT: so the datum still does not validate. See "
                        + "avro-generic-record-unwritable. Do not read 'NP-023 fixed' as "
                        + "'record defaults are writable'.");
    }

    @Test
    @DisplayName("mutating one reconstruction's array default does not affect the next")
    void mutatingOneReconstructionsArrayDefaultDoesNotAffectTheNext() {
        // DECLARED HONESTLY: this PASSES against the unfixed code. It is not a reproduction.
        // JacksonUtils.toObject builds a fresh ArrayList per call so no aliasing exists today.
        // It exists because GenericData.getDefaultValue MEMOISES one shared instance per Field in
        // a static cache, so the naive one-line fix INTRODUCES aliasing. This test is what makes
        // the deepCopy non-negotiable rather than a matter of taste.
        AvroReconstructor r = AvroReconstructor.builder().build();
        Map<String, Object> first = r.reconstructToMap(flat("id", "a"), ARRAY_DEFAULT);
        Map<String, Object> second = r.reconstructToMap(flat("id", "b"), ARRAY_DEFAULT);

        List<?> firstTags = (List<?>) first.get("tags");
        List<?> secondTags = (List<?>) second.get("tags");
        assertNotSame(firstTags, secondTags, "two reconstructions must not share one default list");

        ((List<Object>) firstTags).add("MUTATED");
        assertEquals(2, secondTags.size(),
                "mutating the first reconstruction's defaulted array must not reach the second");
    }

    @Test
    @DisplayName("a defaulted STRING absent from the input is Utf8 at BOTH knob settings, and equal")
    void stringDefaultIsSchemaCorrectAtBothSettings() {
        Schema stringDefaultNullable = parse(
                "{\"type\":\"record\",\"name\":\"R3n\",\"fields\":["
                        + "{\"name\":\"s\",\"type\":[\"null\",\"string\"],\"default\":null},"
                        + "{\"name\":\"n\",\"type\":\"int\"}]}");
        Map<String, Object> on = AvroReconstructor.builder().useSchemaDefaults(true).build()
                .reconstructToMap(flat("n", "1"), stringDefaultNullable);
        assertNull(on.get("s"));
    }
}
