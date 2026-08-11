package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Builds a Java object graph from a fixture's {@code javaInput} spec.
 *
 * <h2>Why this exists</h2>
 *
 * <p>Stack A's signature is {@code flatten(Map<String,Object>)} and Spark hands it {@code Row ->
 * Map} carrying {@code Date}, {@code Instant}, {@code UUID}, enums, {@code byte[]},
 * {@code Object[]}, {@code Set} and non-{@code String} keys. Every MAP arm in this corpus was
 * previously fed a Jackson-parsed document, so the source domain was the strict JSON subset -
 * String, Boolean, Integer, Long, Double, BigInteger, List, Map, null. Whole branches of
 * {@code normalizePrimitive} had never been reached by a fixture, and a cyclic graph - the only
 * input {@code detectCircularReferences} can be observed on - is not expressible as JSON text
 * at all.</p>
 *
 * <h2>Why the kind set is closed and the default branch throws</h2>
 *
 * <p>A materializer that returned {@code null} for a mistyped {@code kind} would let a fixture
 * record a document full of nulls and pass forever. This repository's signature pathology is a
 * control that appears present and does nothing; a silent builder would plant one under every
 * fixture built on it. Every failure below names the fixture.</p>
 */
final class FidelityJavaInput {

    /** Enum types a spec may name. Restricted so a fixture cannot load arbitrary classes. */
    private static final String ENUM_PACKAGE_PREFIX = "java.";

    private FidelityJavaInput() {
    }

    /** The JVM-global pins a spec declares, and the values they replaced. */
    static final class Env implements AutoCloseable {
        private final TimeZone previousZone;
        private final Locale previousLocale;
        private final boolean pinned;

        private Env(TimeZone previousZone, Locale previousLocale, boolean pinned) {
            this.previousZone = previousZone;
            this.previousLocale = previousLocale;
            this.pinned = pinned;
        }

        @Override
        public void close() {
            if (!pinned) {
                return;
            }
            TimeZone.setDefault(previousZone);
            Locale.setDefault(previousLocale);
            if (!TimeZone.getDefault().equals(previousZone) || !Locale.getDefault().equals(previousLocale)) {
                throw new IllegalStateException("FidelityJavaInput: failed to restore the JVM "
                        + "default TimeZone/Locale after a pinned measurement");
            }
        }
    }

    /**
     * Applies a spec's {@code environment} pin.
     *
     * <p>{@code TimeZone.setDefault} is process-global. That is safe here only because surefire
     * runs one JVM with no parallel execution enabled; if anyone turns on
     * {@code junit.jupiter.execution.parallel.enabled} this becomes a source of cross-test flakes
     * that will look like anything but a timezone pin. The restore-and-assert in {@link Env#close}
     * bounds the blast radius; it does not remove it.</p>
     */
    static Env environment(JsonNode spec) {
        JsonNode env = spec == null ? null : spec.path("environment");
        if (env == null || !env.isObject() || env.size() == 0) {
            return new Env(null, null, false);
        }
        TimeZone previousZone = TimeZone.getDefault();
        Locale previousLocale = Locale.getDefault();
        if (env.has("timeZone")) {
            TimeZone.setDefault(TimeZone.getTimeZone(env.get("timeZone").asText()));
        }
        if (env.has("locale")) {
            Locale.setDefault(Locale.forLanguageTag(env.get("locale").asText()));
        }
        return new Env(previousZone, previousLocale, true);
    }

    /** Materialises the spec. The root must be a map, because {@code flatten} takes one. */
    static Object build(JsonNode spec, String fixtureId) {
        if (spec == null || !spec.isObject() || spec.size() == 0) {
            throw new IllegalStateException("fixture " + fixtureId + " declares an empty javaInput");
        }
        requireTimeZonePinIfDated(spec, fixtureId);
        Object root = build(spec, fixtureId, new LinkedHashMap<>());
        if (!(root instanceof Map)) {
            throw new IllegalStateException("fixture " + fixtureId + " javaInput root is a "
                    + root.getClass().getSimpleName() + "; MapFlattener.flatten takes a Map");
        }
        return root;
    }

    /**
     * {@code java.util.Date.toString()} formats through the default zone, so a fixture containing
     * one is machine-dependent unless the zone is pinned. Refusing is the only way that stays a
     * property of the harness rather than of whoever authored the fixture.
     */
    private static void requireTimeZonePinIfDated(JsonNode spec, String fixtureId) {
        if (containsKind(spec, "date") && !spec.path("environment").has("timeZone")) {
            throw new IllegalStateException("fixture " + fixtureId + " builds a java.util.Date but "
                    + "declares no javaInput.environment.timeZone. Date.toString() is a function of "
                    + "the JVM default zone, so the recording would be machine-dependent.");
        }
    }

    private static boolean containsKind(JsonNode node, String kind) {
        if (node == null) {
            return false;
        }
        if (node.isObject() && kind.equals(node.path("kind").asText(null))) {
            return true;
        }
        for (JsonNode child : node) {
            if (containsKind(child, kind)) {
                return true;
            }
        }
        return false;
    }

    private static Object build(JsonNode node, String id, Map<String, Object> anchors) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isObject()) {
            // A bare JSON literal is a convenience for the common scalars; anything structural
            // must be spelled out so that its runtime class is explicit in the fixture file.
            return literal(node, id);
        }
        String kind = node.path("kind").asText("");
        Object built = switch (kind) {
            case "map" -> buildMap(node, id, anchors);
            case "list" -> buildList(node, id, anchors, new ArrayList<>());
            case "set" -> buildSet(node, id, anchors);
            case "objectArray" -> buildObjectArray(node, id, anchors);
            case "bytes" -> buildBytes(node, id);
            case "string" -> node.path("value").asText();
            case "int" -> node.path("value").asInt();
            case "long" -> node.path("value").asLong();
            case "short" -> (short) node.path("value").asInt();
            case "byte" -> (byte) node.path("value").asInt();
            case "float" -> buildFloat(node);
            case "double" -> buildDouble(node);
            case "bigdecimal" -> new BigDecimal(node.path("value").asText());
            case "bigint" -> new BigInteger(node.path("value").asText());
            case "bool" -> node.path("value").asBoolean();
            case "char" -> node.path("value").asText().charAt(0);
            case "null" -> null;
            case "uuid" -> UUID.fromString(node.path("value").asText());
            case "instant" -> Instant.parse(node.path("value").asText());
            case "date" -> new java.util.Date(node.path("epochMillis").asLong());
            case "enum" -> buildEnum(node, id);
            case "ref" -> resolveRef(node, id, anchors);
            default -> throw new IllegalStateException("fixture " + id
                    + " javaInput declares unknown kind '" + kind + "'");
        };
        String anchor = node.path("id").asText("");
        if (!anchor.isEmpty() && !"ref".equals(kind)) {
            // Anchors are registered BEFORE children are built for containers (see buildMap /
            // buildList); this re-put is a no-op for them and covers scalars for completeness.
            anchors.put(anchor, built);
        }
        return built;
    }

    private static Object literal(JsonNode node, String id) {
        if (node.isTextual()) {
            return node.textValue();
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isInt()) {
            return node.intValue();
        }
        if (node.isLong()) {
            return node.longValue();
        }
        if (node.isDouble() || node.isFloat()) {
            return node.doubleValue();
        }
        throw new IllegalStateException("fixture " + id
                + " javaInput carries a bare literal of an unsupported type: " + node.getNodeType());
    }

    private static Object buildMap(JsonNode node, String id, Map<String, Object> anchors) {
        Map<Object, Object> out = new LinkedHashMap<>();
        register(node, anchors, out);
        JsonNode entries = node.path("entries");
        if (!entries.isArray()) {
            throw new IllegalStateException("fixture " + id + " javaInput map has no 'entries' array");
        }
        for (JsonNode e : entries) {
            Object key = build(e.path("key"), id, anchors);
            Object value = build(e.path("value"), id, anchors);
            out.put(key, value);
        }
        return out;
    }

    private static Object buildList(JsonNode node, String id, Map<String, Object> anchors,
                                    List<Object> out) {
        register(node, anchors, out);
        for (JsonNode item : items(node, id)) {
            out.add(build(item, id, anchors));
        }
        return out;
    }

    private static Object buildSet(JsonNode node, String id, Map<String, Object> anchors) {
        // LinkedHashSet only: HashSet's iteration order is unspecified and must never enter a
        // corpus whose recordings are compared byte-for-byte.
        Set<Object> out = new LinkedHashSet<>();
        register(node, anchors, out);
        for (JsonNode item : items(node, id)) {
            out.add(build(item, id, anchors));
        }
        return out;
    }

    private static Object buildObjectArray(JsonNode node, String id, Map<String, Object> anchors) {
        List<Object> tmp = new ArrayList<>();
        for (JsonNode item : items(node, id)) {
            tmp.add(build(item, id, anchors));
        }
        Object[] arr = tmp.toArray();
        register(node, anchors, arr);
        return arr;
    }

    private static Object buildBytes(JsonNode node, String id) {
        JsonNode values = node.path("value");
        if (!values.isArray()) {
            throw new IllegalStateException("fixture " + id + " javaInput bytes has no 'value' array");
        }
        byte[] out = new byte[values.size()];
        for (int i = 0; i < values.size(); i++) {
            out[i] = (byte) values.get(i).asInt();
        }
        return out;
    }

    private static Object buildFloat(JsonNode node) {
        JsonNode v = node.path("value");
        return v.isTextual() ? Float.valueOf(Float.parseFloat(v.asText())) : Float.valueOf((float) v.asDouble());
    }

    private static Object buildDouble(JsonNode node) {
        JsonNode v = node.path("value");
        // Text is how a fixture names NaN and the infinities, which JSON has no literal for and
        // which are the entire point of the non-finite fixtures.
        return v.isTextual() ? Double.valueOf(Double.parseDouble(v.asText())) : Double.valueOf(v.asDouble());
    }

    private static Object buildEnum(JsonNode node, String id) {
        String type = node.path("type").asText("");
        if (!type.startsWith(ENUM_PACKAGE_PREFIX)) {
            throw new IllegalStateException("fixture " + id + " javaInput names enum type '" + type
                    + "', which is outside the '" + ENUM_PACKAGE_PREFIX + "' allow-list");
        }
        try {
            Class<?> cls = Class.forName(type);
            if (!cls.isEnum()) {
                throw new IllegalStateException("fixture " + id + " javaInput type '" + type
                        + "' is not an enum");
            }
            for (Object constant : cls.getEnumConstants()) {
                if (((Enum<?>) constant).name().equals(node.path("value").asText())) {
                    return constant;
                }
            }
            throw new IllegalStateException("fixture " + id + " javaInput enum '" + type
                    + "' has no constant '" + node.path("value").asText() + "'");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("fixture " + id + " javaInput enum type '" + type
                    + "' does not exist", e);
        }
    }

    private static Object resolveRef(JsonNode node, String id, Map<String, Object> anchors) {
        String target = node.path("ref").asText("");
        if (!anchors.containsKey(target)) {
            throw new IllegalStateException("fixture " + id + " javaInput references anchor '"
                    + target + "', which is not an ancestor that has been opened. A ref is the only "
                    + "way to build a cycle and it must point at a container already under "
                    + "construction.");
        }
        return anchors.get(target);
    }

    private static JsonNode items(JsonNode node, String id) {
        JsonNode items = node.path("items");
        if (!items.isArray()) {
            throw new IllegalStateException("fixture " + id + " javaInput container has no 'items' array");
        }
        return items;
    }

    private static void register(JsonNode node, Map<String, Object> anchors, Object container) {
        String anchor = node.path("id").asText("");
        if (!anchor.isEmpty()) {
            anchors.put(anchor, container);
        }
    }

    /** Test seam: the identity map the cycle drill uses to prove {@code ref} shares one object. */
    static IdentityHashMap<Object, Object> identityProbe(Object root) {
        IdentityHashMap<Object, Object> seen = new IdentityHashMap<>();
        collect(root, seen);
        return seen;
    }

    private static void collect(Object value, IdentityHashMap<Object, Object> seen) {
        if (value == null || seen.put(value, value) != null) {
            return;
        }
        if (value instanceof Map<?, ?> map) {
            map.forEach((k, v) -> {
                collect(k, seen);
                collect(v, seen);
            });
        } else if (value instanceof Iterable<?> it) {
            it.forEach(o -> collect(o, seen));
        }
    }
}
