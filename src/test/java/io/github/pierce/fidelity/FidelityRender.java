package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The corpus oracle, expressed as a canonical rendering rather than as an equality predicate.
 *
 * <p>Every value is rendered to a string that carries its runtime type as a prefix, so two
 * renderings are equal exactly when the two values are equal <em>for fidelity purposes</em>:</p>
 *
 * <ul>
 *   <li>scalars must have identical runtime classes - {@code 1} never equals {@code 1L} and
 *       never equals {@code "1"};</li>
 *   <li>doubles are rendered with {@link Double#toString}, so {@code -0.0} is distinct from
 *       {@code 0.0} and {@code NaN} equals {@code NaN};</li>
 *   <li>{@link BigDecimal} is rendered with its own {@code toString}, so declared scale is
 *       part of the value: {@code 37.7740} is distinct from {@code 37.774};</li>
 *   <li>lists compare positionally and by length before element content;</li>
 *   <li>an absent key and a present null are different, because one produces no entry at all
 *       and the other produces an entry whose value renders as JSON null.</li>
 * </ul>
 *
 * <p>Map keys are sorted so the rendering is stable across hash orderings; key ORDER is
 * therefore deliberately not part of the comparison, matching {@code Map.equals} and semantic
 * JSON equality. Key SET is.</p>
 *
 * <p>This exists because {@code JsonReconstructor.verify()} cannot be used as the oracle: it
 * declares String-vs-Number a compatible type pair and compares doubles with an absolute
 * tolerance of 1e-6, so it reports several corpus fixtures as PERFECT while the data is
 * demonstrably changed. A gate that cannot fail is not a gate.</p>
 */
final class FidelityRender {

    static final String THROWN = "THROWN: ";

    private FidelityRender() {
    }

    /** Renders a Java object graph (the MAP stack domain) to its canonical form. */
    static Object java(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> sorted = new TreeMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                sorted.put(String.valueOf(e.getKey()), java(e.getValue()));
            }
            return new LinkedHashMap<String, Object>(sorted);
        }
        if (value instanceof Collection<?> col) {
            List<Object> out = new ArrayList<>(col.size());
            for (Object o : col) {
                out.add(java(o));
            }
            return out;
        }
        if (value instanceof Object[] arr) {
            List<Object> out = new ArrayList<>(arr.length);
            for (Object o : arr) {
                out.add(java(o));
            }
            return out;
        }
        return scalar(value);
    }

    private static Object scalar(Object value) {
        if (value instanceof String s) {
            return "S:" + s;
        }
        if (value instanceof Boolean b) {
            return "B:" + b;
        }
        if (value instanceof Integer i) {
            return "I:" + i;
        }
        if (value instanceof Long l) {
            return "L:" + l;
        }
        if (value instanceof Short s) {
            return "SH:" + s;
        }
        if (value instanceof Byte b) {
            return "BY:" + b;
        }
        if (value instanceof Double d) {
            return "D:" + Double.toString(d);
        }
        if (value instanceof Float f) {
            return "F:" + Float.toString(f);
        }
        if (value instanceof BigInteger bi) {
            return "BI:" + bi;
        }
        if (value instanceof BigDecimal bd) {
            return "BD:" + bd;
        }
        if (value instanceof byte[] bytes) {
            return "BYTES:" + Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof ByteBuffer buf) {
            ByteBuffer dup = buf.duplicate();
            byte[] bytes = new byte[dup.remaining()];
            dup.get(bytes);
            return "BYTES:" + Base64.getEncoder().encodeToString(bytes);
        }
        return avroAware(value);
    }

    /**
     * Avro values are compared the way the Avro harness contract requires: {@code Utf8} by text
     * so it is interchangeable with {@code String}, enum symbols by symbol name, fixed values by
     * byte content, and records by field name and value. Numeric types are NOT normalised into
     * each other - several fixtures turn on exactly that distinction.
     */
    private static Object avroAware(Object value) {
        String cls = value.getClass().getName();
        if ("org.apache.avro.util.Utf8".equals(cls)) {
            return "S:" + value;
        }
        if (value instanceof org.apache.avro.generic.GenericEnumSymbol<?>) {
            return "ENUM:" + value;
        }
        if (value instanceof org.apache.avro.generic.GenericFixed fixed) {
            return "FIXED:" + Base64.getEncoder().encodeToString(fixed.bytes());
        }
        if (value instanceof org.apache.avro.generic.GenericRecord rec) {
            Map<String, Object> sorted = new TreeMap<>();
            for (org.apache.avro.Schema.Field f : rec.getSchema().getFields()) {
                sorted.put(f.name(), java(rec.get(f.name())));
            }
            return new LinkedHashMap<String, Object>(sorted);
        }
        if ("org.apache.avro.JsonProperties$Null".equals(cls)) {
            // Avro's NULL_VALUE singleton, which is what reconstruction substitutes for a
            // nullable field defaulted to null. It is NOT a Java null, so a consumer testing
            // `== null` sees an object. Rendered as its own token so the distinction is visible
            // and so the fixture does not depend on an identity hash code.
            return "AVRO_NULL_DEFAULT";
        }
        // Anything unrecognised still has to render deterministically: Object.toString() bakes in
        // an identity hash code, which would make a fixture pass on one run and fail on the next.
        return value.getClass().getSimpleName() + ":"
                + String.valueOf(value).replaceAll("@[0-9a-fA-F]+", "@X");
    }

    /** Renders a Jackson tree (the JSON stack domain) to the same canonical form. */
    static Object json(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        if (node instanceof ObjectNode obj) {
            Map<String, Object> sorted = new TreeMap<>();
            Iterator<String> it = obj.fieldNames();
            while (it.hasNext()) {
                String name = it.next();
                sorted.put(name, json(obj.get(name)));
            }
            return new LinkedHashMap<String, Object>(sorted);
        }
        if (node instanceof ArrayNode arr) {
            List<Object> out = new ArrayList<>(arr.size());
            for (JsonNode child : arr) {
                out.add(json(child));
            }
            return out;
        }
        if (node.isTextual()) {
            return "S:" + node.textValue();
        }
        if (node.isBoolean()) {
            return "B:" + node.booleanValue();
        }
        if (node.isInt()) {
            return "I:" + node.intValue();
        }
        if (node.isLong()) {
            return "L:" + node.longValue();
        }
        if (node.isBigInteger()) {
            return "BI:" + node.bigIntegerValue();
        }
        if (node.isBigDecimal()) {
            return "BD:" + node.decimalValue();
        }
        if (node.isDouble()) {
            return "D:" + Double.toString(node.doubleValue());
        }
        if (node.isFloat()) {
            return "F:" + Float.toString(node.floatValue());
        }
        if (node.isBinary()) {
            try {
                return "BYTES:" + Base64.getEncoder().encodeToString(node.binaryValue());
            } catch (java.io.IOException e) {
                return "BYTES:<unreadable>";
            }
        }
        return node.getNodeType() + ":" + node.asText();
    }

    /** Canonical text form, used for the recorded expectations and for equality. */
    static String text(Object canonical) {
        StringBuilder sb = new StringBuilder();
        write(canonical, sb);
        return sb.toString();
    }

    private static void write(Object canonical, StringBuilder sb) {
        if (canonical == null) {
            sb.append("null");
            return;
        }
        if (canonical instanceof Map<?, ?> map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                quote(String.valueOf(e.getKey()), sb);
                sb.append(':');
                write(e.getValue(), sb);
            }
            sb.append('}');
            return;
        }
        if (canonical instanceof List<?> list) {
            sb.append('[');
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                write(list.get(i), sb);
            }
            sb.append(']');
            return;
        }
        quote(String.valueOf(canonical), sb);
    }

    private static void quote(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20 || (c >= 0x7f && c <= 0x9f)) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    /** Renders a thrown failure so that "it threw" is a first-class, comparable outcome. */
    static String thrown(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String msg = root.getMessage() == null ? "" : root.getMessage();
        // Strip anything that could vary between runs or machines.
        msg = msg.replaceAll("@[0-9a-fA-F]+", "@X").trim();
        if (msg.length() > 240) {
            msg = msg.substring(0, 240);
        }
        return THROWN + root.getClass().getSimpleName() + ": " + msg;
    }
}
