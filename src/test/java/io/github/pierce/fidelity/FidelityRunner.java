package io.github.pierce.fidelity;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pierce.AvroReconstructor;
import io.github.pierce.AvroSchemaFlattener;
import io.github.pierce.JsonFlattener;
import io.github.pierce.JsonReconstructor;
import io.github.pierce.MapFlattener;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Executes one corpus fixture against the declared stack(s) and returns every rendering the
 * manifest guarantee is checked against.
 *
 * <p>Two Jackson configurations are used on purpose and must not be merged.</p>
 *
 * <p>{@link #LENIENT} mirrors {@code JsonFlattener}'s own mapper exactly
 * ({@code USE_BIG_DECIMAL_FOR_FLOATS=false}). It builds the MAP-stack source document, which is
 * why the MAP stack is structurally blind to parse-time loss: any damage Jackson does while
 * reading the text has already happened before {@code flatten()} is called, so
 * {@code back.equals(source)} is trivially true. Fixtures that measure parse-time loss are
 * declared JSON-only for exactly this reason and must not be widened to BOTH.</p>
 *
 * <p>{@link #EXACT} keeps fractional numbers as {@link BigDecimal}. It is used only to build the
 * JSON-stack baseline and to read the reconstructed JSON back, so declared scale and
 * beyond-double precision are visible in the comparison. Comparing the two JSON texts as strings
 * would be too strict (it would fail on 1e2 vs 100.0); comparing them with a default mapper would
 * be too lenient (both sides collapse to the same Double and money loss disappears).</p>
 */
final class FidelityRunner {

    static final ObjectMapper LENIENT = lenient();
    static final ObjectMapper EXACT = exact();

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() { };

    private FidelityRunner() {
    }

    private static ObjectMapper lenient() {
        ObjectMapper m = new ObjectMapper();
        m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        m.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, false);
        m.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        m.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        m.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        return m;
    }

    private static ObjectMapper exact() {
        ObjectMapper m = new ObjectMapper();
        m.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        m.configure(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS, false);
        m.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        // USE_BIG_DECIMAL_FOR_FLOATS alone is NOT enough, and getting this wrong silently
        // disabled three fixtures on the first recording run: the default JsonNodeFactory calls
        // BigDecimal.stripTrailingZeros() when building a DecimalNode, so 37.7740 arrived as
        // 37.774 on BOTH sides of the comparison and a scale-loss fixture measured LOSSLESS.
        // withExactBigDecimals(true) keeps the declared scale, which is the whole point.
        m.setNodeFactory(com.fasterxml.jackson.databind.node.JsonNodeFactory.withExactBigDecimals(true));
        return m;
    }

    /** Answer to "does the row's recorded behaviour survive the library's default entry point?" */
    static final String DEFAULTS_HOLD = "YES";
    static final String DEFAULTS_DIVERGE = "NO";
    static final String DEFAULTS_NA = "NOT_APPLICABLE";

    /** All renderings produced for one fixture, plus the live objects the pair checks need. */
    static final class Measurement {
        final Map<String, Object> recorded = new LinkedHashMap<>();
        Object mapDocObject;
        List<String> notes = new ArrayList<>();
    }

    static Measurement run(FidelityFixture fx) {
        Measurement m = new Measurement();
        String stack = fx.stack();
        if ("AVRO".equals(stack)) {
            runAvro(fx, m);
        } else {
            boolean doMap = "BOTH".equals(stack) || "MAP".equals(stack);
            boolean doJson = "BOTH".equals(stack) || "JSON".equals(stack);
            if (doMap) {
                runMapStack(fx, m);
            }
            if (doJson) {
                // The MAP arm owns the "flat" key when both run; the JSON arm gets its own so the
                // two flattened intermediates are BOTH asserted. See runJsonStack.
                runJsonStack(fx, m, doMap ? "flatJson" : "flat");
            }
        }
        runProbe(fx, m);
        return m;
    }

    /**
     * Folds the per-arm defaults verdicts into the single tri-state a consumer needs.
     *
     * <p>{@code NO} on any arm wins: the row is then only true under the configuration the fixture
     * declares, and the published table has to say so.</p>
     */
    static String defaultsVerdict(Measurement m) {
        boolean any = false;
        for (String k : new String[] {"mapDefaultsMatch", "jsonDefaultsMatch", "avroDefaultsMatch"}) {
            Object v = m.recorded.get(k);
            if (v instanceof Boolean b) {
                any = true;
                if (!b) {
                    return DEFAULTS_DIVERGE;
                }
            }
        }
        return any ? DEFAULTS_HOLD : DEFAULTS_NA;
    }

    // ---------------------------------------------------------------- MAP stack

    private static void runMapStack(FidelityFixture fx, Measurement m) {
        Map<String, Object> source;
        try {
            source = LENIENT.readValue(fx.input(), MAP_TYPE);
        } catch (Exception e) {
            throw new IllegalStateException("fixture " + fx.id() + " input is not parseable JSON", e);
        }
        m.recorded.put("mapBaseline", FidelityRender.text(FidelityRender.java(source)));

        String flat;
        String doc;
        String defaults;
        try {
            MapFlattener flattener = flattener(fx.config());
            Map<String, Object> flattened = flattener.flatten(source);
            flat = FidelityRender.text(FidelityRender.java(flattened));
            Map<String, Object> back = reconstructor(fx.config()).reconstruct(flattened);
            m.mapDocObject = back;
            doc = FidelityRender.text(FidelityRender.java(back));
            // The DEFAULTS ARM. The row above is measured with whatever configuration the fixture
            // declares; a consumer who has read only the published table will call
            // JsonReconstructor.quickReconstruct. Where the two disagree the row is true only
            // under its stated configuration, and the table has to print that.
            defaults = mapDefaultsArm(flattened);
        } catch (Throwable t) {
            flat = FidelityRender.thrown(t);
            doc = flat;
            defaults = flat;
        }
        m.recorded.put("flat", flat);
        m.recorded.put("mapDoc", doc);
        m.recorded.put("losslessMap", doc.equals(m.recorded.get("mapBaseline")));
        m.recorded.put("mapDefaultsMatch", defaults.equals(doc));
    }

    private static String mapDefaultsArm(Map<String, Object> flattened) {
        try {
            return FidelityRender.text(FidelityRender.java(
                    JsonReconstructor.quickReconstruct(flattened)));
        } catch (Throwable t) {
            return FidelityRender.thrown(t);
        }
    }

    // ---------------------------------------------------------------- JSON stack

    /**
     * @param flatKey where to record the flattened intermediate. When only the JSON stack runs it
     *                is {@code flat}; when both run the MAP arm already owns that key and this one
     *                gets {@code flatJson}. Recording it unconditionally is the point:
     *                {@code JsonFlattener} parses with its own mapper, so its flattened map can
     *                legitimately differ in value typing from {@code MapFlattener.flatten}, and
     *                without this key that divergence would only ever surface downstream as a
     *                changed reconstruction rather than being blamed on the flattening step.
     */
    private static void runJsonStack(FidelityFixture fx, Measurement m, String flatKey) {
        JsonNode baseline;
        try {
            baseline = EXACT.readTree(fx.input());
        } catch (Exception e) {
            throw new IllegalStateException("fixture " + fx.id() + " input is not parseable JSON", e);
        }
        m.recorded.put("jsonBaseline", FidelityRender.text(FidelityRender.json(baseline)));

        String flat;
        String doc;
        String defaults;
        try {
            MapFlattener flattener = flattener(fx.config());
            Map<String, Object> flattened = JsonFlattener.with(flattener).from(fx.input()).toMap();
            flat = FidelityRender.text(FidelityRender.java(flattened));
            String backJson = reconstructor(fx.config()).reconstructToJson(flattened);
            doc = FidelityRender.text(FidelityRender.json(EXACT.readTree(backJson)));
            defaults = jsonDefaultsArm(flattened);
        } catch (Throwable t) {
            flat = FidelityRender.thrown(t);
            doc = flat;
            defaults = flat;
        }
        m.recorded.put(flatKey, flat);
        m.recorded.put("jsonDoc", doc);
        m.recorded.put("losslessJson", doc.equals(m.recorded.get("jsonBaseline")));
        m.recorded.put("jsonDefaultsMatch", defaults.equals(doc));
    }

    private static String jsonDefaultsArm(Map<String, Object> flattened) {
        try {
            return FidelityRender.text(FidelityRender.json(
                    EXACT.readTree(JsonReconstructor.quickReconstructToJson(flattened))));
        } catch (Throwable t) {
            return FidelityRender.thrown(t);
        }
    }

    // ---------------------------------------------------------------- AVRO stack

    private static void runAvro(FidelityFixture fx, Measurement m) {
        JsonNode avro = fx.config().path("avro");
        String mode = avro.path("assert").asText("DATA");
        m.recorded.put("avroAssert", mode);

        Map<String, Object> datum;
        try {
            datum = LENIENT.readValue(fx.input(), MAP_TYPE);
        } catch (Exception e) {
            throw new IllegalStateException("fixture " + fx.id() + " input is not parseable JSON", e);
        }

        // The cache is static and keyed on full name + flags; a hit skips flattenSchema entirely
        // and leaves the instance without record definitions, so reconstructOriginalSchema would
        // then throw. Clearing between fixtures is what keeps this corpus order-independent.
        AvroSchemaFlattener.clearCache();

        Schema schema = new Schema.Parser().parse(avro.path("avsc").toString());
        AvroSchemaFlattener schemaFlattener = schemaFlattener(avro);

        String flat;
        Map<String, Object> flattened = null;
        try {
            flattened = flattener(fx.config()).flatten(datum);
            flat = FidelityRender.text(FidelityRender.java(flattened));
        } catch (Throwable t) {
            flat = FidelityRender.thrown(t);
        }

        String baseline;
        String doc;
        switch (mode) {
            case "KEYSET" -> {
                baseline = renderNames(schemaFieldNames(schemaFlattener, schema));
                doc = flattened == null
                        ? FidelityRender.thrown(new IllegalStateException("flatten failed"))
                        : renderNames(new ArrayList<>(flattened.keySet()));
            }
            case "SCHEMA" -> {
                baseline = schema.toString();
                String flattenedSchemaRendering;
                Map<String, Object> checks = new LinkedHashMap<>();
                try {
                    Schema flatSchema = schemaFlattener.getFlattenedSchemaNoCache(schema);
                    flattenedSchemaRendering = describe(flatSchema);
                    doc = schemaFlattener.reconstructOriginalSchema(flatSchema).toString();
                    schemaChecks(schema, flatSchema, doc, checks);
                } catch (Throwable t) {
                    flattenedSchemaRendering = FidelityRender.thrown(t);
                    doc = flattenedSchemaRendering;
                    checks.put("threw", true);
                }
                m.recorded.put("flattenedSchema", flattenedSchemaRendering);
                m.recorded.put("schemaChecks", checks);
            }
            case "SCHEMA_ARG_IGNORED" -> {
                Schema unrelated = new Schema.Parser().parse(avro.path("avsc2").toString());
                baseline = unrelated.toString();
                String flattenedSchemaRendering;
                try {
                    Schema flatSchema = schemaFlattener.getFlattenedSchemaNoCache(schema);
                    flattenedSchemaRendering = describe(flatSchema);
                    // Feed the inverse a schema that has nothing to do with the forward pass.
                    // A real inverse would derive its answer from this argument.
                    doc = schemaFlattener.reconstructOriginalSchema(unrelated).toString();
                } catch (Throwable t) {
                    flattenedSchemaRendering = FidelityRender.thrown(t);
                    doc = flattenedSchemaRendering;
                }
                m.recorded.put("flattenedSchema", flattenedSchemaRendering);
            }
            default -> {
                baseline = FidelityRender.text(FidelityRender.java(datum));
                try {
                    Map<String, Object> back = avroReconstructor(avro).reconstructToMap(flattened, schema);
                    m.mapDocObject = back;
                    doc = FidelityRender.text(FidelityRender.java(back));
                } catch (Throwable t) {
                    doc = FidelityRender.thrown(t);
                }
                // Defaults arm for the Avro data path. The schema is data, not configuration, so
                // supplying it is not a departure from defaults; anything the fixture sets on
                // AvroReconstructor.Builder is. The SCHEMA and KEYSET modes reconstruct no data at
                // all, so they record no key here and the published table prints NOT_APPLICABLE
                // rather than a verdict nobody measured.
                //
                // HONEST LIMIT, stated so nobody reads more into the YES than it carries: no Avro
                // fixture currently sets avro.reconstructor, so avroReconstructor(avro) above
                // builds the same default instance this line does and the comparison cannot
                // presently fail. It is a tripwire for the first Avro fixture that DOES tune the
                // reconstructor, not present-tense evidence. The MAP and JSON arms are the ones
                // carrying real signal today - nine rows diverge there.
                m.recorded.put("avroDefaultsMatch", avroDefaultsArm(flattened, schema).equals(doc));
            }
        }
        m.recorded.put("flat", flat);
        m.recorded.put("avroBaseline", baseline);
        m.recorded.put("avroDoc", doc);
        boolean lossless = doc.equals(baseline);
        if ("SCHEMA".equals(mode)) {
            // The inverse alone is NOT a verdict on the SCHEMA stack: reconstructOriginalSchema
            // replays stored definitions from the forward pass, so it reproduces the original even
            // when the flattened schema has thrown information away. Three fixtures measured
            // LOSSLESS on the first recording run for exactly that reason. The verdict is
            // therefore the conjunction of the inverse and three checks on the flattened schema
            // itself.
            @SuppressWarnings("unchecked")
            Map<String, Object> checks = (Map<String, Object>) m.recorded.get("schemaChecks");
            for (Object v : checks.values()) {
                if (v instanceof Boolean b && !b) {
                    lossless = false;
                }
            }
            if (Boolean.TRUE.equals(checks.get("threw"))) {
                lossless = false;
            }
        }
        m.recorded.put("losslessAvro", lossless);
        AvroSchemaFlattener.clearCache();
    }

    private static String avroDefaultsArm(Map<String, Object> flattened, Schema schema) {
        if (flattened == null) {
            return FidelityRender.thrown(new IllegalStateException("flatten failed"));
        }
        try {
            return FidelityRender.text(FidelityRender.java(
                    AvroReconstructor.builder().build().reconstructToMap(flattened, schema)));
        } catch (Throwable t) {
            return FidelityRender.thrown(t);
        }
    }

    /**
     * Generic, schema-independent checks on what the FLATTENED schema kept.
     *
     * <ul>
     *   <li>{@code inverseReproducesOriginal} - catches field metadata dropped by the inverse
     *       (defaults, order, aliases).</li>
     *   <li>{@code logicalTypesPreserved} - every logical type reachable in the source must still
     *       be nameable in the flattened schema.</li>
     *   <li>{@code namedTypesPreserved} - every enum and fixed type name must survive.</li>
     *   <li>{@code declaredColumnsNotShadowed} - a flattened column that takes a declared root
     *       field's name must be that field, not a generated column that happens to collide with
     *       it. Comparing the documentation string is what separates the two.</li>
     * </ul>
     */
    private static void schemaChecks(Schema original, Schema flattened, String inverse,
                                     Map<String, Object> checks) {
        checks.put("inverseReproducesOriginal", inverse.equals(original.toString()));

        List<String> originalLogical = new ArrayList<>();
        List<String> originalNamed = new ArrayList<>();
        collectTypes(original, new java.util.HashSet<>(), originalLogical, originalNamed);
        List<String> flatLogical = new ArrayList<>();
        List<String> flatNamed = new ArrayList<>();
        collectTypes(flattened, new java.util.HashSet<>(), flatLogical, flatNamed);
        Collections.sort(originalLogical);
        Collections.sort(originalNamed);
        Collections.sort(flatLogical);
        Collections.sort(flatNamed);
        checks.put("originalLogicalTypes", String.join(",", originalLogical));
        checks.put("flattenedLogicalTypes", String.join(",", flatLogical));
        checks.put("logicalTypesPreserved", flatLogical.containsAll(originalLogical));
        checks.put("originalNamedTypes", String.join(",", originalNamed));
        checks.put("flattenedNamedTypes", String.join(",", flatNamed));
        checks.put("namedTypesPreserved", flatNamed.containsAll(originalNamed));

        boolean notShadowed = true;
        StringBuilder shadowed = new StringBuilder();
        for (Schema.Field declared : original.getFields()) {
            Schema.Field column = flattened.getField(declared.name());
            if (column == null) {
                continue;
            }
            String a = declared.doc() == null ? "" : declared.doc();
            String b = column.doc() == null ? "" : column.doc();
            if (!a.equals(b)) {
                notShadowed = false;
                shadowed.append(declared.name()).append("=>'").append(b).append("' ");
            }
        }
        checks.put("declaredColumnsNotShadowed", notShadowed);
        checks.put("shadowedColumns", shadowed.toString().trim());
    }

    private static void collectTypes(Schema s, java.util.Set<String> seen,
                                     List<String> logical, List<String> named) {
        if (s == null) {
            return;
        }
        if (s.getLogicalType() != null) {
            logical.add(s.getLogicalType().getName());
        }
        switch (s.getType()) {
            case RECORD -> {
                if (!seen.add(s.getFullName())) {
                    return;
                }
                for (Schema.Field f : s.getFields()) {
                    collectTypes(f.schema(), seen, logical, named);
                }
            }
            case ENUM, FIXED -> named.add(s.getType() + ":" + s.getName());
            case ARRAY -> collectTypes(s.getElementType(), seen, logical, named);
            case MAP -> collectTypes(s.getValueType(), seen, logical, named);
            case UNION -> {
                for (Schema b : s.getTypes()) {
                    collectTypes(b, seen, logical, named);
                }
            }
            default -> { }
        }
    }

    private static List<String> schemaFieldNames(AvroSchemaFlattener f, Schema schema) {
        List<String> names = new ArrayList<>();
        for (Schema.Field field : f.getFlattenedSchemaNoCache(schema).getFields()) {
            names.add(field.name());
        }
        return names;
    }

    private static String renderNames(List<String> names) {
        List<String> copy = new ArrayList<>(names);
        Collections.sort(copy);
        return FidelityRender.text(copy.stream().map(n -> (Object) ("S:" + n)).toList());
    }

    /** Field-by-field description of a flattened schema: type text, logical type, doc, default. */
    private static String describe(Schema schema) {
        Map<String, Object> out = new TreeMap<>();
        for (Schema.Field field : schema.getFields()) {
            String logical = field.schema().getLogicalType() == null
                    ? "none" : field.schema().getLogicalType().getName();
            out.put(field.name(), "S:type=" + field.schema()
                    + ";logical=" + logical
                    + ";doc=" + field.doc()
                    + ";hasDefault=" + field.hasDefaultValue());
        }
        return FidelityRender.text(new LinkedHashMap<String, Object>(out));
    }

    // ---------------------------------------------------------------- probes

    private static void runProbe(FidelityFixture fx, Measurement m) {
        JsonNode probe = fx.probe();
        if (probe == null || probe.isMissingNode() || probe.isNull()) {
            return;
        }
        Map<String, Object> source;
        try {
            source = LENIENT.readValue(fx.input(), MAP_TYPE);
        } catch (Exception e) {
            throw new IllegalStateException("fixture " + fx.id() + " input is not parseable JSON", e);
        }
        String kind = probe.path("kind").asText();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("kind", kind);
        result.put("expect", probe.path("expect").asText(""));

        if ("RECONSTRUCT_CONFIG_COMPARE".equals(kind)) {
            Map<String, Object> flat = flattener(fx.config()).flatten(source);
            String a = FidelityRender.text(FidelityRender.java(
                    reconstructor(probe.path("configA")).reconstruct(flat)));
            String b = FidelityRender.text(FidelityRender.java(
                    reconstructor(probe.path("configB")).reconstruct(flat)));
            result.put("a", a);
            result.put("b", b);
            result.put("equal", a.equals(b));
        } else if ("FLATTEN_CONFIG_COMPARE".equals(kind)) {
            String a = FidelityRender.text(FidelityRender.java(
                    flattener(probe.path("configA")).flatten(source)));
            String b = FidelityRender.text(FidelityRender.java(
                    flattener(probe.path("configB")).flatten(source)));
            result.put("a", a);
            result.put("b", b);
            result.put("equal", a.equals(b));
        } else {
            throw new IllegalStateException("unknown probe kind '" + kind + "' on fixture " + fx.id());
        }

        JsonNode twin = probe.path("twin");
        if (twin.isObject()) {
            if (!"BIGDECIMAL_TWIN".equals(twin.path("kind").asText())) {
                throw new IllegalStateException("unknown twin kind on fixture " + fx.id());
            }
            Object typed = substitute(source, twin.path("key").asText(),
                    new BigDecimal(twin.path("decimal").asText()));
            @SuppressWarnings("unchecked")
            Map<String, Object> typedMap = (Map<String, Object>) typed;
            result.put("twinFlat", FidelityRender.text(FidelityRender.java(
                    flattener(fx.config()).flatten(typedMap))));
        }
        m.recorded.put("probe", result);
    }

    /** Replaces every leaf reached by {@code key} with a Java-typed value the JSON stack cannot express. */
    private static Object substitute(Object node, String key, Object replacement) {
        if (node instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                String k = String.valueOf(e.getKey());
                out.put(k, k.equals(key) ? replacement : substitute(e.getValue(), key, replacement));
            }
            return out;
        }
        if (node instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object o : list) {
                out.add(substitute(o, key, replacement));
            }
            return out;
        }
        return node;
    }

    // ---------------------------------------------------------------- config

    static MapFlattener flattener(JsonNode config) {
        JsonNode c = config == null ? null : config.path("mapFlattener");
        MapFlattener.Builder b = MapFlattener.builder();
        if (c == null || c.isMissingNode() || c.isNull()) {
            return b.build();
        }
        if (c.has("maxDepth")) {
            b.maxDepth(c.get("maxDepth").asInt());
        }
        if (c.has("maxArraySize")) {
            b.maxArraySize(c.get("maxArraySize").asInt());
        }
        if (c.has("maxMapSize")) {
            b.maxMapSize(c.get("maxMapSize").asInt());
        }
        if (c.has("maxJsonStringLength")) {
            b.maxJsonStringLength(c.get("maxJsonStringLength").asInt());
        }
        if (c.has("useArrayBoundarySeparator")) {
            b.useArrayBoundarySeparator(c.get("useArrayBoundarySeparator").asBoolean());
        }
        if (c.has("detectCircularReferences")) {
            b.detectCircularReferences(c.get("detectCircularReferences").asBoolean());
        }
        if (c.has("strictKeyValidation")) {
            b.strictKeyValidation(c.get("strictKeyValidation").asBoolean());
        }
        if (c.has("parseNestedJsonStrings")) {
            b.parseNestedJsonStrings(c.get("parseNestedJsonStrings").asBoolean());
        }
        if (c.has("preserveBigDecimalPrecision")) {
            b.preserveBigDecimalPrecision(c.get("preserveBigDecimalPrecision").asBoolean());
        }
        if (c.has("namingStrategy")) {
            b.namingStrategy(MapFlattener.FieldNamingStrategy.valueOf(c.get("namingStrategy").asText()));
        }
        if (c.has("arrayFormat")) {
            b.arrayFormat(MapFlattener.ArraySerializationFormat.valueOf(c.get("arrayFormat").asText()));
        }
        return b.build();
    }

    static JsonReconstructor reconstructor(JsonNode config) {
        JsonNode c = config == null ? null : config.path("reconstructor");
        JsonReconstructor.Builder b = JsonReconstructor.builder();
        if (c == null || c.isMissingNode() || c.isNull()) {
            return b.build();
        }
        if (c.has("separator")) {
            b.separator(c.get("separator").asText());
        }
        if (c.has("useArrayBoundarySeparator")) {
            b.useArrayBoundarySeparator(c.get("useArrayBoundarySeparator").asBoolean());
        }
        if (c.has("inferArraysFromValues")) {
            b.inferArraysFromValues(c.get("inferArraysFromValues").asBoolean());
        }
        if (c.has("preserveNulls")) {
            b.preserveNulls(c.get("preserveNulls").asBoolean());
        }
        if (c.has("maxDepth")) {
            b.maxDepth(c.get("maxDepth").asInt());
        }
        if (c.has("arrayFormat")) {
            b.arrayFormat(JsonReconstructor.ArraySerializationFormat.valueOf(c.get("arrayFormat").asText()));
        }
        if (c.has("arrayPaths")) {
            List<String> paths = new ArrayList<>();
            for (JsonNode p : c.get("arrayPaths")) {
                paths.add(p.asText());
            }
            b.arrayPaths(paths.toArray(new String[0]));
        }
        return b.build();
    }

    private static AvroSchemaFlattener schemaFlattener(JsonNode avro) {
        JsonNode sf = avro.path("schemaFlattener");
        boolean stats = sf.path("includeArrayStatistics").asBoolean(false);
        boolean nonTerminal = sf.path("includeNonTerminalArrays").asBoolean(true);
        return new AvroSchemaFlattener(stats, nonTerminal);
    }

    private static AvroReconstructor avroReconstructor(JsonNode avro) {
        JsonNode rc = avro.path("reconstructor");
        AvroReconstructor.Builder b = AvroReconstructor.builder();
        if (rc.has("strictValidation")) {
            b.strictValidation(rc.get("strictValidation").asBoolean());
        }
        if (rc.has("allowMissingFields")) {
            b.allowMissingFields(rc.get("allowMissingFields").asBoolean());
        }
        if (rc.has("useSchemaDefaults")) {
            b.useSchemaDefaults(rc.get("useSchemaDefaults").asBoolean());
        }
        if (rc.has("useArrayBoundarySeparator")) {
            b.useArrayBoundarySeparator(rc.get("useArrayBoundarySeparator").asBoolean());
        }
        if (rc.has("arrayFormat")) {
            b.arrayFormat(AvroReconstructor.ArraySerializationFormat.valueOf(rc.get("arrayFormat").asText()));
        }
        return b.build();
    }
}
