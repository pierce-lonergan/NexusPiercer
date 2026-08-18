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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BL-014. {@code reconstructArrayOfRecords} never had a UNION arm.
 *
 * <p>Its Step 3 field dispatch recognises exactly three shapes - "flat column present", RECORD,
 * ARRAY - and {@code unwrapNullable}, its only union handling, collapses only {@code [null, T]}.
 * A union of arity three or more therefore arrives at that dispatch still typed UNION, matches
 * nothing, and falls off the end into {@code handleMissingField}, which sees a NULL branch and
 * writes a plain null. The child node holding the data is never read by anything.</p>
 *
 * <p>This is a NEVER-IMPLEMENTED GAP, not a regression: {@code unwrapUnion} had four calls and
 * zero declarations through ef625f2 and a declaration with zero callers from cad816b, so it never
 * executed and there is nothing to restore.</p>
 *
 * <p>SECOND, DISTINCT FAULT ON THE SAME FIELD: when the element DOES have a flat column,
 * {@code convertPrimitive} is handed the UNION, has no UNION case in its switch and returns the
 * value UNCONVERTED - a datum that reconstructs "successfully" and throws
 * {@code UnresolvedUnionException} only when someone writes it.</p>
 */
@DisplayName("BL-014 a multi-branch union inside an array element is resolved, or is loud")
class AvroReconstructorArrayElementUnionTest {

    private static Schema parse(String json) {
        return new Schema.Parser().parse(json);
    }

    private static final MapFlattener FLATTENER = new MapFlattener();

    /** Order{items: array&lt;Item{sku, meta: ["null", Meta{src}, "string"]}&gt;}, no default. */
    private static final Schema THREE_BRANCH = parse(
            "{\"type\":\"record\",\"name\":\"OrderU\",\"fields\":["
                    + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"ItemU\",\"fields\":["
                    + "{\"name\":\"sku\",\"type\":\"string\"},"
                    + "{\"name\":\"meta\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"MetaU\","
                    + "\"fields\":[{\"name\":\"src\",\"type\":\"string\"}]},\"string\"]}]}}}]}");

    /** Same but with NO null branch, so handleMissingField cannot write null. */
    private static final Schema THREE_BRANCH_NO_NULL = parse(
            "{\"type\":\"record\",\"name\":\"OrderN\",\"fields\":["
                    + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"ItemN\",\"fields\":["
                    + "{\"name\":\"sku\",\"type\":\"string\"},"
                    + "{\"name\":\"meta\",\"type\":[\"string\",{\"type\":\"record\",\"name\":\"MetaN\","
                    + "\"fields\":[{\"name\":\"src\",\"type\":\"string\"}]},\"int\"]}]}}}]}");

    /** ["null","long","string"] on an array-element field: the convertPrimitive half. */
    private static final Schema SCALAR_UNION = parse(
            "{\"type\":\"record\",\"name\":\"OrderS\",\"fields\":["
                    + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"ItemS\",\"fields\":["
                    + "{\"name\":\"sku\",\"type\":\"string\"},"
                    + "{\"name\":\"meta\",\"type\":[\"null\",\"long\",\"string\"]}]}}}]}");

    /** Two record branches sharing the child keys that are present: unresolvable. */
    private static final Schema AMBIGUOUS = parse(
            "{\"type\":\"record\",\"name\":\"OrderA\",\"fields\":["
                    + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"ItemA\",\"fields\":["
                    + "{\"name\":\"sku\",\"type\":\"string\"},"
                    + "{\"name\":\"pay\",\"type\":[\"null\","
                    + "{\"type\":\"record\",\"name\":\"Card\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"brand\",\"type\":[\"null\",\"string\"],\"default\":null}]},"
                    + "{\"type\":\"record\",\"name\":\"Bank\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"routing\",\"type\":\"string\"},"
                    + "{\"name\":\"account\",\"type\":\"string\"}]}]}]}}}]}");

    /** [null, Meta] arity 2 inside an array element - regression pin, must not move. */
    private static final Schema TWO_BRANCH = parse(
            "{\"type\":\"record\",\"name\":\"OrderT\",\"fields\":["
                    + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"ItemT\",\"fields\":["
                    + "{\"name\":\"sku\",\"type\":\"string\"},"
                    + "{\"name\":\"note\",\"type\":[\"null\",\"string\"]},"
                    + "{\"name\":\"meta\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"MetaT\","
                    + "\"fields\":[{\"name\":\"src\",\"type\":\"string\"}]}]}]}}}]}");

    private static Map<String, Object> flattenJson(String json) throws Exception {
        Map<String, Object> doc = new com.fasterxml.jackson.databind.ObjectMapper()
                .readValue(json, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() { });
        return FLATTENER.flatten(doc);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> items(Map<String, Object> back) {
        Object v = back.get("items");
        assertTrue(v instanceof List, "expected a List at items, got " + v);
        return (List<Map<String, Object>>) v;
    }

    private static String chain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            sb.append(c.getClass().getName()).append(": ").append(c.getMessage()).append(" || ");
        }
        return sb.toString();
    }

    @Test
    @DisplayName("a three-branch union inside an array element keeps its record branch")
    void threeBranchUnionInsideArrayElementKeepsItsRecordBranch() throws Exception {
        Map<String, Object> flat = flattenJson(
                "{\"items\":[{\"sku\":\"a\",\"meta\":{\"src\":\"web\"}},"
                        + "{\"sku\":\"b\",\"meta\":{\"src\":\"pos\"}}]}");
        Map<String, Object> back = AvroReconstructor.builder().build()
                .reconstructToMap(flat, THREE_BRANCH);

        List<Map<String, Object>> got = items(back);
        assertEquals(2, got.size(), "flat=" + flat + " back=" + back);
        Object meta0 = got.get(0).get("meta");
        assertNotNull(meta0, "flat=" + flat + " back=" + back);
        assertEquals("web", String.valueOf(((Map<?, ?>) meta0).get("src")));
    }

    @Test
    @DisplayName("the repair must reach EVERY element, not only the first")
    void threeBranchUnionRepairMustReachEveryElementNotOnlyTheFirst() throws Exception {
        Map<String, Object> flat = flattenJson(
                "{\"items\":[{\"sku\":\"a\",\"meta\":{\"src\":\"web\"}},"
                        + "{\"sku\":\"b\",\"meta\":{\"src\":\"pos\"}}]}");
        Map<String, Object> back = AvroReconstructor.builder().build()
                .reconstructToMap(flat, THREE_BRANCH);

        List<Map<String, Object>> got = items(back);
        Object meta1 = got.get(1).get("meta");
        assertNotNull(meta1, "element 1 must be repaired too; back=" + back);
        assertEquals("pos", String.valueOf(((Map<?, ?>) meta1).get("src")),
                "a per-index fix that silently degrades to element 0 is caught here");
    }

    @Test
    @DisplayName("a null-free three-branch union resolves rather than failing in the builder")
    void threeBranchUnionWithNoNullBranchResolves() throws Exception {
        // HONEST NARROWING OF THE FILING. With no NULL branch, handleMissingField cannot write
        // null - it falls to its switch default, log.warns, never sets the field, and
        // GenericRecordBuilder.build() throws AvroRuntimeException. So the SILENT drop BL-014
        // describes is specific to unions that CONTAIN a null branch; a null-free 3+ union was
        // already loud, just uselessly so.
        Map<String, Object> flat = flattenJson(
                "{\"items\":[{\"sku\":\"a\",\"meta\":{\"src\":\"web\"}}]}");
        Map<String, Object> back = AvroReconstructor.builder().build()
                .reconstructToMap(flat, THREE_BRANCH_NO_NULL);
        Object meta = items(back).get(0).get("meta");
        assertNotNull(meta, "back=" + back);
        assertEquals("web", String.valueOf(((Map<?, ?>) meta).get("src")));
    }

    @Test
    @DisplayName("a scalar branch of a multi-branch union is converted to its BRANCH type")
    void scalarBranchOfAMultiBranchUnionIsConvertedToItsBranchTypeNotPassedThrough() throws Exception {
        Map<String, Object> flat = flattenJson("{\"items\":[{\"sku\":\"a\",\"meta\":123}]}");
        Map<String, Object> back = AvroReconstructor.builder().build()
                .reconstructToMap(flat, SCALAR_UNION);
        Object meta = items(back).get(0).get("meta");

        assertInstanceOf(Long.class, meta,
                "the LONG branch must be materialised as a Long; got "
                        + (meta == null ? "null" : meta.getClass().getName()));

        // This is the assertion that actually names the defect. UnresolvedUnionException is
        // thrown by the writer when a value resolves to NO branch of its union, and validate()
        // against the FIELD's union schema is the same question asked cheaply. An Integer in a
        // ["null","long","string"] slot answers false; a Long answers true.
        Schema metaUnion = SCALAR_UNION.getField("items").schema().getElementType()
                .getField("meta").schema();
        assertTrue(GenericData.get().validate(metaUnion, meta),
                "the value must resolve to a branch of " + metaUnion + "; got "
                        + (meta == null ? "null" : meta.getClass().getName()) + " = " + meta);

        // DELIBERATE SCOPE PIN, measured rather than assumed. reconstruct() still cannot produce
        // a writable datum for this document, and NOT because of the union: mapToGenericRecord
        // rebuilds only the ROOT record, so the array elements are LinkedHashMaps. That is
        // avro-generic-record-unwritable, a different defect group, untouched here. Asserting it
        // stops anyone reading "BL-014 fixed" as "the datum writes".
        GenericRecord rec = AvroReconstructor.builder().build().reconstruct(flat, SCALAR_UNION);
        assertFalse(GenericData.get().validate(SCALAR_UNION, rec),
                "PINNED LIMIT: nested elements are still Maps in reconstruct(); rec=" + rec);
    }

    @Test
    @DisplayName("two record branches matching the same columns is LOUD, not null")
    void twoRecordBranchesMatchingTheSameColumnsIsLoudNotNull() throws Exception {
        Map<String, Object> flat = flattenJson(
                "{\"items\":[{\"sku\":\"a\",\"pay\":{\"id\":\"p-1\",\"routing\":\"r\",\"account\":\"c\"}}]}");

        Throwable t = assertThrows(RuntimeException.class,
                () -> AvroReconstructor.builder().build().reconstructToMap(flat, AMBIGUOUS),
                "under the default strictValidation=true an unresolvable branch must throw");
        String all = chain(t);
        assertTrue(all.contains("Card") && all.contains("Bank"),
                "the failure must name both candidate branches; chain was " + all);

        Map<String, Object> lenient = AvroReconstructor.builder().strictValidation(false).build()
                .reconstructToMap(flat, AMBIGUOUS);
        assertNotNull(lenient, "lenient must not throw");
    }

    @Test
    @DisplayName("REGRESSION PIN: nullable two-branch unions inside array elements are unchanged")
    void nullableTwoBranchUnionsInsideArrayElementsAreUnchangedByThisFix() throws Exception {
        // Passes before AND after by design: the arity>2 guard is the entire reason no existing
        // fixture moves. Drilled by widening the guard to all unions in a scratch build, which
        // makes this go red - see the drill report in the commit message.
        Map<String, Object> flat = flattenJson(
                "{\"items\":[{\"sku\":\"a\",\"note\":\"n1\",\"meta\":{\"src\":\"web\"}},"
                        + "{\"sku\":\"b\",\"note\":null,\"meta\":{\"src\":\"pos\"}}]}");
        Map<String, Object> back = AvroReconstructor.builder().build()
                .reconstructToMap(flat, TWO_BRANCH);
        List<Map<String, Object>> got = items(back);
        assertEquals(2, got.size(), "back=" + back);
        assertEquals("web", String.valueOf(((Map<?, ?>) got.get(0).get("meta")).get("src")));
        assertEquals("pos", String.valueOf(((Map<?, ?>) got.get(1).get("meta")).get("src")));
    }
}
