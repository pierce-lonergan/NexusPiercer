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

    /** ["null","int","long","string"]: three scalar branches, string LAST in declaration order. */
    private static final Schema WIDE_UNION = parse(
            "{\"type\":\"record\",\"name\":\"OrderW\",\"fields\":["
                    + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"ItemW\",\"fields\":["
                    + "{\"name\":\"sku\",\"type\":\"string\"},"
                    + "{\"name\":\"meta\",\"type\":[\"null\",\"int\",\"long\",\"string\"]}]}}}]}");

    /** ["null","boolean","string"]: Boolean.parseBoolean accepts everything and never throws. */
    private static final Schema BOOL_UNION = parse(
            "{\"type\":\"record\",\"name\":\"OrderB\",\"fields\":["
                    + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"ItemB\",\"fields\":["
                    + "{\"name\":\"sku\",\"type\":\"string\"},"
                    + "{\"name\":\"meta\",\"type\":[\"null\",\"boolean\",\"string\"]}]}}}]}");

    /** ["null","int","long"]: NO string branch, so a String value has nowhere native to go. */
    private static final Schema NO_STRING_BRANCH = parse(
            "{\"type\":\"record\",\"name\":\"OrderX\",\"fields\":["
                    + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"ItemX\",\"fields\":["
                    + "{\"name\":\"sku\",\"type\":\"string\"},"
                    + "{\"name\":\"meta\",\"type\":[\"null\",\"int\",\"long\"]}]}}}]}");

    /** ["null","string","long"]: the reverse direction - a NUMBER with string declared first. */
    private static final Schema STRING_FIRST = parse(
            "{\"type\":\"record\",\"name\":\"OrderR\",\"fields\":["
                    + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"ItemR\",\"fields\":["
                    + "{\"name\":\"sku\",\"type\":\"string\"},"
                    + "{\"name\":\"meta\",\"type\":[\"null\",\"string\",\"long\"]}]}}}]}");

    private static Object metaOf(Schema schema, String json) throws Exception {
        Map<String, Object> back = AvroReconstructor.builder().build()
                .reconstructToMap(flattenJson(json), schema);
        return items(back).get(0).get("meta");
    }

    @Test
    @DisplayName("a numeric-looking STRING keeps its string branch instead of being coerced")
    void numericLookingStringKeepsItsStringBranchRatherThanBeingCoerced() throws Exception {
        // ADVERSARIAL REVIEW, CONFIRMED BY MEASUREMENT. The first version of step 4 tried the
        // branches in DECLARATION ORDER and returned the first whose convertPrimitive did not
        // throw, so a value that was a STRING in the source document was silently converted into
        // an earlier numeric branch. This is NOT the unavoidable top-level ambiguity: the JSON
        // column keeps the quotes and Jackson boxes the element as a String, so the string-ness
        // is information PRESENT IN THE INPUT that was being discarded.
        //
        // Measured against the unfixed build, this exact case:
        //   ["null","int","long","string"] doc meta="0007" -> Integer = 7   leading zeros gone
        // and it was correct at HEAD~1 before the BL-014 arm was added, so the arm regressed it.
        //
        // Drilled by restoring the declaration-order loop. Verbatim, all three new tests:
        //   numericLookingStringKeepsItsStringBranchRatherThanBeingCoerced: a String in the
        //     document must take the STRING branch even though int and long are declared first;
        //     got java.lang.Integer = 7 ==> Unexpected type, expected: <java.lang.String> but
        //     was: <java.lang.Integer>
        //   booleanBranchDeclaredBeforeStringNoLongerEatsEveryString: an arbitrary string must
        //     not be laundered into Boolean.FALSE ==> expected: <hello> but was: <false>
        //   aNumberIsNotStringifiedByAStringFirstUnion: a JSON number must take the LONG branch
        //     even though string is declared first; got java.lang.String ==> Unexpected type,
        //     expected: <java.lang.Long> but was: <java.lang.String>
        Object meta = metaOf(WIDE_UNION, "{\"items\":[{\"sku\":\"a\",\"meta\":\"0007\"}]}");
        assertInstanceOf(String.class, meta,
                "a String in the document must take the STRING branch even though int and long "
                        + "are declared first; got "
                        + (meta == null ? "null" : meta.getClass().getName() + " = " + meta));
        assertEquals("0007", meta, "the leading zeros are the whole point of this test");
    }

    @Test
    @DisplayName("a BOOLEAN branch declared before string no longer eats every string")
    void booleanBranchDeclaredBeforeStringNoLongerEatsEveryString() throws Exception {
        // WORSE THAN THE REVIEW FILED, and the reason this is a reorder rather than a numeric
        // special case: Boolean.parseBoolean NEVER throws. Under declaration order the BOOLEAN
        // branch therefore accepted absolutely anything and the string branch was unreachable.
        // Measured against the unfixed build: meta="hello" -> Boolean false, meta="" -> Boolean
        // false. No exception, no log, and a datum that validates against its own schema.
        assertEquals("hello", metaOf(BOOL_UNION, "{\"items\":[{\"sku\":\"a\",\"meta\":\"hello\"}]}"),
                "an arbitrary string must not be laundered into Boolean.FALSE");
        assertEquals("true", metaOf(BOOL_UNION, "{\"items\":[{\"sku\":\"a\",\"meta\":\"true\"}]}"),
                "even the string \"true\" is a String in the document, not a boolean");
        assertInstanceOf(Boolean.class,
                metaOf(BOOL_UNION, "{\"items\":[{\"sku\":\"a\",\"meta\":true}]}"),
                "CONTROL: a real JSON boolean still takes the BOOLEAN branch");
    }

    @Test
    @DisplayName("the reverse direction too: a NUMBER is not stringified by a string-first union")
    void aNumberIsNotStringifiedByAStringFirstUnion() throws Exception {
        // The same fault pointing the other way, and it was ALSO introduced by the BL-014 arm:
        // ["null","string","long"] with a JSON number 123 gave String "123" at HEAD and
        // Integer 123 at HEAD~1. Neither is right - the schema says long.
        Object meta = metaOf(STRING_FIRST, "{\"items\":[{\"sku\":\"a\",\"meta\":123}]}");
        assertInstanceOf(Long.class, meta,
                "a JSON number must take the LONG branch even though string is declared first; "
                        + "got " + (meta == null ? "null" : meta.getClass().getName()));
    }

    @Test
    @DisplayName("with NO native branch the declaration-order fallback still applies")
    void withNoNativeBranchTheDeclarationOrderFallbackStillApplies() throws Exception {
        // THE CONTROL THAT KEEPS THE FIX HONEST. The preference is a REORDER, not a filter: a
        // String under ["null","int","long"] has no string, enum, bytes or fixed branch to
        // prefer, so every branch stays in the fallback group in declaration order and the
        // answer is unchanged from before the fix. Without this, "prefer the native branch"
        // could have been implemented as "fail when there is none", which would turn working
        // reconstructions into throws.
        Object meta = metaOf(NO_STRING_BRANCH, "{\"items\":[{\"sku\":\"a\",\"meta\":\"123\"}]}");
        assertInstanceOf(Integer.class, meta,
                "int is declared first and there is nothing native to prefer; got "
                        + (meta == null ? "null" : meta.getClass().getName()));
        assertEquals(123, meta);

        // The LOSSY corner of the same fallback: no string branch exists, so "0007" must still be
        // coerced, and 7 is the only answer available. What changed is that it is no longer
        // SILENT - warnCoercedAcrossJavaType compares the lexical form and logs a WARN naming the
        // before and after. This asserts the value and exercises that path; the WARN TEXT itself
        // is not asserted, because Spark's log4j-slf4j-impl wins SLF4J provider resolution in
        // this module and a logback ListAppender would capture nothing. Stated rather than
        // faked with a capture that silently observes the wrong provider.
        Object lossy = metaOf(NO_STRING_BRANCH, "{\"items\":[{\"sku\":\"a\",\"meta\":\"0007\"}]}");
        assertEquals(7, lossy, "with no string branch the coercion is unavoidable, only audible");
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
