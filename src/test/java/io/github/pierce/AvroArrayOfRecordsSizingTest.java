package io.github.pierce;

import org.apache.avro.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BL-013, measured rather than inherited.
 *
 * <p>THE FILED CAUSE WAS REFUTED. BL-013 claimed that under three of the four array formats an
 * N-element array of records silently became one, because {@code reconstructArrayOfRecords} Step 1
 * only JSON-parses a value that starts '[' and ends ']'. Measured: no collapse under ANY format
 * for an element with a scalar field at its root, because {@code PathNode.addArrayFieldValue} had
 * already split the column upstream. Step 1 was a no-op and resurrecting the deleted
 * {@code calculateArraySize} format branches would have changed nothing.</p>
 *
 * <p>THE REAL COLLAPSE IS FORMAT-INDEPENDENT (D1): it fires when every field of the array element
 * lives inside a NESTED RECORD, so the array node carries no arrayFieldValues at all and
 * {@code determineArraySize} counts nothing and floors at 1. Every one of the 29 existing AVRO
 * fixtures puts a scalar at the element root, which is exactly why none of them catches it.</p>
 *
 * <p>THE CONFIGURED FORMAT NEVER REACHED THE CODE (D2): the split ran in a {@code static} method
 * that structurally could not read the instance {@code arrayFormat} field, and sniffed
 * JSON-then-comma-then-pipe instead. Comma before pipe meant a legal comma inside a
 * PIPE_SEPARATED element was split as a delimiter.</p>
 *
 * <p>DISAGREEING COUNTS WERE REPAIRED IN BOTH DIRECTIONS (D3): {@code Math.max} over the columns,
 * short scalar columns padded with "" and 0, short nested-record columns having their last value
 * DUPLICATED by a {@code Math.min(index, size-1)} clamp.</p>
 */
@DisplayName("BL-013 array-of-records element sizing is schema-guided, format-driven and loud")
class AvroArrayOfRecordsSizingTest {

    private static Schema parse(String json) {
        return new Schema.Parser().parse(json);
    }

    /**
     * The flattened prefix for the field named {@code line_items}. The separator is {@code _} and
     * the field name contains one, so {@link io.github.pierce.path.FlattenedPath} escapes it. A
     * hand-built key of {@code line_items_sku} decodes to THREE segments and silently misses the
     * array node entirely - measured, and worth a named constant so the next reader does not
     * repeat it.
     */
    private static final String LI = "line\\_items";

    /** Order{order_id, line_items: array&lt;LineItem{meta: Meta{code,label}}&gt;} - nested-only. */
    private static final Schema NESTED_ONLY = parse(
            "{\"type\":\"record\",\"name\":\"O3\",\"fields\":["
                    + "{\"name\":\"order_id\",\"type\":\"string\"},"
                    + "{\"name\":\"line_items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"LineItem\",\"fields\":["
                    + "{\"name\":\"meta\",\"type\":{\"type\":\"record\",\"name\":\"Meta\",\"fields\":["
                    + "{\"name\":\"code\",\"type\":\"string\"},"
                    + "{\"name\":\"label\",\"type\":\"string\"}]}}]}}}]}");

    /** Order{line_items: array&lt;LineItem{sku}&gt;} - one scalar at the element root. */
    private static final Schema SKU_ONLY = parse(
            "{\"type\":\"record\",\"name\":\"O1\",\"fields\":["
                    + "{\"name\":\"line_items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"LineItem1\",\"fields\":["
                    + "{\"name\":\"sku\",\"type\":\"string\"}]}}}]}");

    /** Order{line_items: array&lt;LineItem{sku, description, quantity}&gt;}. */
    private static final Schema THREE_SCALARS = parse(
            "{\"type\":\"record\",\"name\":\"O2\",\"fields\":["
                    + "{\"name\":\"line_items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"LineItem2\",\"fields\":["
                    + "{\"name\":\"sku\",\"type\":\"string\"},"
                    + "{\"name\":\"description\",\"type\":\"string\"},"
                    + "{\"name\":\"quantity\",\"type\":\"int\"}]}}}]}");

    /** Order{line_items: array&lt;LineItem{sku, meta: Meta{code}}&gt;} - mixed. */
    private static final Schema SKU_AND_META = parse(
            "{\"type\":\"record\",\"name\":\"O4\",\"fields\":["
                    + "{\"name\":\"line_items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"LineItem4\",\"fields\":["
                    + "{\"name\":\"sku\",\"type\":\"string\"},"
                    + "{\"name\":\"meta\",\"type\":{\"type\":\"record\",\"name\":\"Meta4\",\"fields\":["
                    + "{\"name\":\"code\",\"type\":\"string\"}]}}]}}}]}");

    /**
     * Order{order_id, line_items: array&lt;LineItem{sku:string, qty:int}&gt;}.
     *
     * <p>Both element fields are REQUIRED and of different types, so the pre-fix floor of 1 had
     * to fabricate both an empty string and a zero to fill the row it invented.</p>
     */
    private static final Schema SKU_AND_QTY = parse(
            "{\"type\":\"record\",\"name\":\"O5\",\"fields\":["
                    + "{\"name\":\"order_id\",\"type\":\"string\"},"
                    + "{\"name\":\"line_items\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"LineItem5\",\"fields\":["
                    + "{\"name\":\"sku\",\"type\":\"string\"},"
                    + "{\"name\":\"qty\",\"type\":\"int\"}]}}}]}");

    /** Leaf array of string, for the one-configuration-two-answers drill. */
    private static final Schema LEAF_STRINGS = parse(
            "{\"type\":\"record\",\"name\":\"OL\",\"fields\":["
                    + "{\"name\":\"names\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}");

    private static Map<String, Object> flat(String... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static MapFlattener.ArraySerializationFormat flattenerFormat(
            AvroReconstructor.ArraySerializationFormat f) {
        return MapFlattener.ArraySerializationFormat.valueOf(f.name());
    }

    private static AvroReconstructor reconstructor(AvroReconstructor.ArraySerializationFormat f) {
        return AvroReconstructor.builder().arrayFormat(f).build();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> items(Map<String, Object> back, String field) {
        Object v = back.get(field);
        assertTrue(v instanceof List, "expected a List at " + field + ", got " + v);
        return (List<Map<String, Object>>) v;
    }

    // ============================== D1: the real collapse ==============================

    @ParameterizedTest
    @EnumSource(AvroReconstructor.ArraySerializationFormat.class)
    @DisplayName("nested-only elements keep every element, under all four formats")
    void nestedOnlyElementsKeepEveryElement(AvroReconstructor.ArraySerializationFormat format) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("order_id", "O1");
        List<Object> lineItems = new ArrayList<>();
        for (String[] pair : new String[][]{{"C1", "L1"}, {"C2", "L2"}, {"C3", "L3"}}) {
            Map<String, Object> meta = new LinkedHashMap<>();
            meta.put("code", pair[0]);
            meta.put("label", pair[1]);
            Map<String, Object> element = new LinkedHashMap<>();
            element.put("meta", meta);
            lineItems.add(element);
        }
        doc.put("line_items", lineItems);

        Map<String, Object> flattened = MapFlattener.builder()
                .arrayFormat(flattenerFormat(format)).build().flatten(doc);
        Map<String, Object> back = reconstructor(format).reconstructToMap(flattened, NESTED_ONLY);

        List<Map<String, Object>> got = items(back, "line_items");
        assertEquals(3, got.size(),
                "three elements must survive; flat=" + flattened + " back=" + back);
        for (int i = 0; i < 3; i++) {
            Map<?, ?> meta = (Map<?, ?>) got.get(i).get("meta");
            assertEquals("C" + (i + 1), String.valueOf(meta.get("code")));
            assertEquals("L" + (i + 1), String.valueOf(meta.get("label")));
        }
    }

    // ============================== D2: the knob is live ==============================

    @Test
    @DisplayName("arrayFormat is LIVE for arrays of records - two settings, one document")
    void arrayFormatIsLiveForArraysOfRecords() {
        Map<String, Object> pipeText = flat(LI + "_sku", "A|B");

        List<Map<String, Object>> underPipe = items(
                reconstructor(AvroReconstructor.ArraySerializationFormat.PIPE_SEPARATED)
                        .reconstructToMap(pipeText, SKU_ONLY), "line_items");
        assertEquals(2, underPipe.size(), "PIPE_SEPARATED must split on the pipe; got " + underPipe);
        assertEquals("A", String.valueOf(underPipe.get(0).get("sku")));
        assertEquals("B", String.valueOf(underPipe.get(1).get("sku")));

        List<Map<String, Object>> underComma = items(
                reconstructor(AvroReconstructor.ArraySerializationFormat.COMMA_SEPARATED)
                        .reconstructToMap(pipeText, SKU_ONLY), "line_items");
        assertEquals(1, underComma.size(),
                "COMMA_SEPARATED must treat a pipe as ordinary data; got " + underComma);
        assertEquals("A|B", String.valueOf(underComma.get(0).get("sku")));

        Map<String, Object> commaText = flat(LI + "_sku", "A,B");

        List<Map<String, Object>> commaUnderComma = items(
                reconstructor(AvroReconstructor.ArraySerializationFormat.COMMA_SEPARATED)
                        .reconstructToMap(commaText, SKU_ONLY), "line_items");
        assertEquals(2, commaUnderComma.size(), "got " + commaUnderComma);

        List<Map<String, Object>> commaUnderPipe = items(
                reconstructor(AvroReconstructor.ArraySerializationFormat.PIPE_SEPARATED)
                        .reconstructToMap(commaText, SKU_ONLY), "line_items");
        assertEquals(1, commaUnderPipe.size(),
                "PIPE_SEPARATED must treat a comma as ordinary data; got " + commaUnderPipe);
        assertEquals("A,B", String.valueOf(commaUnderPipe.get(0).get("sku")));
    }

    @Test
    @DisplayName("PIPE_SEPARATED does not split on a comma inside an element value")
    void pipeFormatDoesNotSplitOnACommaInsideAnElement() {
        Map<String, Object> doc = new LinkedHashMap<>();
        List<Object> lineItems = new ArrayList<>();
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("sku", "SKU-311");
        a.put("description", "Bolt, hex, M8");
        a.put("quantity", 4);
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("sku", "SKU-312");
        b.put("description", "Washer");
        b.put("quantity", 8);
        lineItems.add(a);
        lineItems.add(b);
        doc.put("line_items", lineItems);

        Map<String, Object> flattened = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.PIPE_SEPARATED)
                .build().flatten(doc);
        Map<String, Object> back = reconstructor(
                AvroReconstructor.ArraySerializationFormat.PIPE_SEPARATED)
                .reconstructToMap(flattened, THREE_SCALARS);

        List<Map<String, Object>> got = items(back, "line_items");
        assertEquals(2, got.size(), "flat=" + flattened + " back=" + back);
        assertEquals("Bolt, hex, M8", String.valueOf(got.get(0).get("description")));
        assertEquals("Washer", String.valueOf(got.get(1).get("description")));
        assertEquals("SKU-311", String.valueOf(got.get(0).get("sku")));
        assertEquals("SKU-312", String.valueOf(got.get(1).get("sku")));
        assertEquals(4, got.get(0).get("quantity"));
        assertEquals(8, got.get(1).get("quantity"));
    }

    @Test
    @DisplayName("a leaf array and an array of records agree on the element count")
    void aLeafArrayAndAnArrayOfRecordsAgreeOnTheElementCount() {
        String text = "Bolt, hex|Washer";

        Map<String, Object> leafBack = reconstructor(
                AvroReconstructor.ArraySerializationFormat.PIPE_SEPARATED)
                .reconstructToMap(flat("names", text), LEAF_STRINGS);
        List<?> leaf = (List<?>) leafBack.get("names");
        assertEquals(2, leaf.size(), "leaf array of string; got " + leaf);

        List<Map<String, Object>> records = items(reconstructor(
                AvroReconstructor.ArraySerializationFormat.PIPE_SEPARATED)
                .reconstructToMap(flat(LI + "_sku", text), SKU_ONLY), "line_items");
        assertEquals(leaf.size(), records.size(),
                "one reconstructor, one configuration, one input shape - the answer must not "
                        + "depend on whether the element type is a record. leaf=" + leaf
                        + " records=" + records);
        assertEquals("Bolt, hex", String.valueOf(records.get(0).get("sku")));
        assertEquals("Washer", String.valueOf(records.get(1).get("sku")));
    }

    // ============================== D3: disagreeing counts ==============================

    @Test
    @DisplayName("disagreeing column counts throw instead of duplicating the last value")
    void disagreeingColumnCountsThrowInsteadOfDuplicating() {
        Throwable t = assertThrows(RuntimeException.class,
                () -> reconstructor(AvroReconstructor.ArraySerializationFormat.COMMA_SEPARATED)
                        .reconstructToMap(
                                flat(LI + "_sku", "S1,S2,S3", LI + "_meta_code", "C1,C2"),
                                SKU_AND_META));
        String all = chain(t);
        assertTrue(all.contains("ArrayCardinalityException"),
                "must be the named cardinality failure; chain was " + all);
        assertTrue(all.contains(LI + "_sku") && all.contains("3"), "chain was " + all);
        assertTrue(all.contains(LI + "_meta_code") && all.contains("2"), "chain was " + all);
        assertTrue(all.contains("COMMA_SEPARATED"),
                "the message must name the configured format; chain was " + all);
    }

    @Test
    @DisplayName("disagreeing column counts throw instead of truncating the long column")
    void disagreeingColumnCountsThrowInsteadOfTruncating() {
        Throwable t = assertThrows(RuntimeException.class,
                () -> reconstructor(AvroReconstructor.ArraySerializationFormat.COMMA_SEPARATED)
                        .reconstructToMap(
                                flat(LI + "_sku", "S1,S2", LI + "_meta_code", "C1,C2,C3"),
                                SKU_AND_META));
        String all = chain(t);
        assertTrue(all.contains("ArrayCardinalityException"), "chain was " + all);
        assertTrue(all.contains(LI + "_sku") && all.contains(LI + "_meta_code"),
                "chain was " + all);
    }

    @Test
    @DisplayName("padded columns throw rather than invent \"\" and 0")
    void paddedColumnsThrowRatherThanInventDefaults() {
        Throwable t = assertThrows(RuntimeException.class,
                () -> reconstructor(AvroReconstructor.ArraySerializationFormat.COMMA_SEPARATED)
                        .reconstructToMap(flat(
                                LI + "_sku", "A,B,C",
                                LI + "_description", "d1,d2",
                                LI + "_quantity", "1"), THREE_SCALARS));
        String all = chain(t);
        assertTrue(all.contains("ArrayCardinalityException"), "chain was " + all);
        assertTrue(all.contains(LI + "_sku") && all.contains(LI + "_description")
                && all.contains(LI + "_quantity"), "chain was " + all);
    }

    @Test
    @DisplayName("the specific cardinality message survives both wrappers")
    void theSpecificExceptionSurvivesBothWrappers() {
        Throwable t = assertThrows(RuntimeException.class,
                () -> reconstructor(AvroReconstructor.ArraySerializationFormat.COMMA_SEPARATED)
                        .reconstructToMap(
                                flat(LI + "_sku", "S1,S2,S3", LI + "_meta_code", "C1,C2"),
                                SKU_AND_META));
        assertTrue(t.getMessage() != null && t.getMessage().contains(LI + "_sku"),
                "a loud error muffled two frames up is not loud. getMessage() was: "
                        + t.getMessage());
    }

    // ============================== controls ==============================

    @Test
    @DisplayName("GOOD INPUT CONTROL: mixed scalar+nested elements are unchanged")
    void goodInputControlMixedElements() {
        Map<String, Object> back = reconstructor(
                AvroReconstructor.ArraySerializationFormat.COMMA_SEPARATED)
                .reconstructToMap(
                        flat(LI + "_sku", "S1,S2,S3", LI + "_meta_code", "C1,C2,C3"),
                        SKU_AND_META);
        List<Map<String, Object>> got = items(back, "line_items");
        assertEquals(3, got.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("S" + (i + 1), String.valueOf(got.get(i).get("sku")));
            assertEquals("C" + (i + 1),
                    String.valueOf(((Map<?, ?>) got.get(i).get("meta")).get("code")));
        }
    }

    @Test
    @DisplayName("GOOD INPUT CONTROL: a JSON-format document is unaffected")
    void goodInputControlJsonFormat() {
        Map<String, Object> back = AvroReconstructor.builder().build().reconstructToMap(
                flat(LI + "_sku", "[\"S1\",\"S2\"]", LI + "_meta_code", "[\"C1\",\"C2\"]"),
                SKU_AND_META);
        List<Map<String, Object>> got = items(back, "line_items");
        assertEquals(2, got.size(), "got " + got);
        assertEquals("S1", String.valueOf(got.get(0).get("sku")));
        assertEquals("C2", String.valueOf(((Map<?, ?>) got.get(1).get("meta")).get("code")));
    }

    @Test
    @DisplayName("a delimited format fed self-delimiting JSON text is a detectable contradiction")
    void jsonTextUnderADelimitedFormatIsRefusedRatherThanShredded() {
        // DECIDED (BL-013 openQuestion 1). Trusting the config means splitting ["S1","S2"] on its
        // internal comma, which would produce garbage. JSON's grammar is self-delimiting and
        // MapFlattener's COMMA/PIPE writers structurally cannot emit a bracketed, quoted list, so
        // this is a detectable CONTRADICTION rather than a sniff. Refuse it by name.
        Throwable t = assertThrows(RuntimeException.class,
                () -> reconstructor(AvroReconstructor.ArraySerializationFormat.COMMA_SEPARATED)
                        .reconstructToMap(flat(LI + "_sku", "[\"S1\",\"S2\"]"), SKU_ONLY));
        String all = chain(t);
        assertTrue(all.contains("ArrayFormatMismatchException"), "chain was " + all);
        assertTrue(all.contains("COMMA_SEPARATED"), "chain was " + all);
    }

    @Test
    @DisplayName("BRACKET_LIST is not caught by the JSON-contradiction check")
    void bracketListIsNotAContradiction() {
        // BRACKET_LIST is itself bracketed, so the contradiction check must exclude it or it
        // would refuse its own format's legitimate output.
        Map<String, Object> back = reconstructor(
                AvroReconstructor.ArraySerializationFormat.BRACKET_LIST)
                .reconstructToMap(flat(LI + "_sku", "[A, B]"), SKU_ONLY);
        List<Map<String, Object>> got = items(back, "line_items");
        assertEquals(2, got.size(), "got " + got);
        assertNotEquals("A, B", String.valueOf(got.get(0).get("sku")));
    }

    @Test
    @DisplayName("the JSON default fed unbracketed delimited text is a contradiction TOO")
    void unbracketedDelimitedTextUnderTheJsonDefaultIsRefusedRatherThanConcatenated() {
        // ADVERSARIAL REVIEW, BLOCKING, CONFIRMED BY MEASUREMENT AGAINST BOTH COMMITS. The
        // "trust the configured format" split introduced a NEW silent N-to-1 collapse at the
        // SHIPPED DEFAULT - the exact defect class it was written to remove - and it was
        // undisclosed and untested. Measured with the same probe binary against both builds:
        //
        //   {items_sku=S1,S2,S3, items_name=N1,N2,N3} against Order{id, items:array<Item>}
        //     HEAD~1 : items size = 3  -> [{sku=S1,name=N1},{sku=S2,name=N2},{sku=S3,name=N3}]
        //     HEAD   : items size = 1  -> [{sku=S1,S2,S3, name=N1,N2,N3}]
        //
        // with no exception and no log of any level, because the JSON branch's non-bracketed
        // return was a bare Collections.singletonList(strValue). The opposite misconfiguration
        // - bracketed JSON under COMMA/PIPE - already threw, so the guard was asymmetric.
        //
        // Drilled by deleting the guard call again. Verbatim, this test and its pipe sibling:
        //   an unbracketed comma-delimited column under arrayFormat JSON must not silently
        //   collapse to one element holding the concatenation ==> Expected
        //   java.lang.RuntimeException to be thrown, but nothing was thrown.
        Throwable t = assertThrows(RuntimeException.class,
                () -> AvroReconstructor.builder().build()
                        .reconstructToMap(flat(LI + "_sku", "S1,S2,S3"), SKU_ONLY),
                "an unbracketed comma-delimited column under arrayFormat JSON must not silently "
                        + "collapse to one element holding the concatenation");
        String all = chain(t);
        assertTrue(all.contains("ArrayFormatMismatchException"), "chain was " + all);
        assertTrue(all.contains("JSON"), "the message must name the configured format; " + all);
        assertTrue(all.contains("COMMA_SEPARATED"),
                "the message must name the format that WOULD read this data; " + all);
        assertTrue(all.contains("3"),
                "the message must say how many records the other reading finds; " + all);
    }

    @Test
    @DisplayName("the same contradiction for PIPE text, naming PIPE_SEPARATED")
    void unbracketedPipeTextUnderTheJsonDefaultNamesPipeSeparated() {
        Throwable t = assertThrows(RuntimeException.class,
                () -> AvroReconstructor.builder().build()
                        .reconstructToMap(flat(LI + "_sku", "S1|S2|S3"), SKU_ONLY));
        assertTrue(chain(t).contains("PIPE_SEPARATED"), "chain was " + chain(t));
    }

    @Test
    @DisplayName("CONTROL: an unbracketed column with NO delimiter is left alone")
    void unbracketedColumnWithNoDelimiterIsLeftAlone() {
        // THE NARROWING, and it is what stops this guard being a blanket refusal of every
        // hand-built flat map. The two readings only DISAGREE when a delimiter is present; with
        // none, JSON and comma and pipe all read one element and nothing is at stake. If this
        // test ever goes red the guard has been widened past the contradiction it detects.
        Map<String, Object> back = AvroReconstructor.builder().build()
                .reconstructToMap(flat(LI + "_sku", "S1"), SKU_ONLY);
        List<Map<String, Object>> got = items(back, "line_items");
        assertEquals(1, got.size(), "back=" + back);
        assertEquals("S1", String.valueOf(got.get(0).get("sku")));
    }

    @Test
    @DisplayName("an array node with NO element data returns an EMPTY list, not a fabricated row")
    void arrayNodeWithNoElementDataReturnsAnEmptyList() {
        // ADVERSARIAL REVIEW, MAJOR: CHANGELOG behaviour change 5 shipped with NO test anywhere
        // in the suite, and the claim that reachability "was proven, not assumed" had nothing
        // failing-before/passing-after behind it. Drilled by reinstating the exact pre-fix floor
        // - agreedElementCount's `if (distinct.isEmpty()) { return 0; }` patched to `return 1;` -
        // which leaves the whole 2470-test suite green, so this was a coverage gap rather than
        // an unreachable branch. Against that drill build this test fails with:
        //   expected: <0> but was: <1> ==> back={order_id=O, line_items=[{sku=, qty=0}]}
        // The fabricated row is the defect: "" and 0 are not data, they are inventions that
        // validate against their own schema.
        //
        // The order_id key is written escaped for the same reason LI is: FlattenedPath decodes an
        // unescaped underscore as a separator, so a raw "order_id" key names a two-segment path
        // and the required root field reads as missing.
        Map<String, Object> back = AvroReconstructor.builder().build().reconstructToMap(
                flat("order\\_id", "O", LI + "_sku", "[]", LI + "_qty", "[]"), SKU_AND_QTY);
        List<Map<String, Object>> got = items(back, "line_items");
        assertEquals(0, got.size(),
                "empty columns describe an empty array, not one row of type-defaults; back="
                        + back);
        assertTrue(got.isEmpty(), "back=" + back);
    }

    private static String chain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            sb.append(c.getClass().getName()).append(": ").append(c.getMessage()).append(" || ");
        }
        return sb.toString();
    }
}
