package io.github.pierce.fidelity;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pierce.AvroReconstructor;
import io.github.pierce.AvroSchemaFlattener;
import io.github.pierce.GAvroSchemaFlattener;
import io.github.pierce.JsonFlattener;
import io.github.pierce.JsonReconstructor;
import io.github.pierce.MapFlattener;
import io.github.pierce.schema.FlattenOptions;
import io.github.pierce.schema.FlattenedField;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
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
        if (fx.javaInput() != null && !"MAP".equals(fx.stack())) {
            // The JSON, BOTH and AVRO arms all need parseable source TEXT. Falling through with a
            // null input would NPE somewhere downstream, or worse, be "helpfully" defaulted.
            throw new IllegalStateException("fixture " + fx.id() + " declares javaInput but stack "
                    + fx.stack() + "; a typed source document is only expressible on the MAP stack");
        }
        try (FidelityJavaInput.Env env = FidelityJavaInput.environment(fx.javaInput())) {
            assert env != null;
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
                    // The MAP arm owns the "flat" key when both run; the JSON arm gets its own so
                    // the two flattened intermediates are BOTH asserted. See runJsonStack.
                    runJsonStack(fx, m, doMap ? "flatJson" : "flat");
                }
            }
            runProbe(fx, m);
            return m;
        }
    }

    /**
     * The fixture's source document, built once so the MAP arm and the probe arms see the SAME
     * object graph. Identity matters: {@code detectCircularReferences} keys on object identity, so
     * a probe that re-parsed its own copy would be measuring a different graph.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> source(FidelityFixture fx) {
        if (fx.javaInput() != null) {
            return (Map<String, Object>) FidelityJavaInput.build(fx.javaInput(), fx.id());
        }
        try {
            return LENIENT.readValue(fx.input(), MAP_TYPE);
        } catch (Exception e) {
            throw new IllegalStateException("fixture " + fx.id() + " input is not parseable JSON", e);
        }
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
        Map<String, Object> source = source(fx);
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

        // ORDER-INDEPENDENCE BRACKET, and the one place it is deliberately absent.
        //
        // AvroSchemaFlattener.schemaCache is `private static final` and keyed on full name plus
        // two flags, with no schema CONTENT in the key; GAvroSchemaFlattener holds a static
        // ThreadLocal parse cache. Clearing both at each end of runAvro is what keeps a fixture's
        // result the same whether it runs first or last.
        //
        // What that bracket therefore means, stated because it is the audit finding and not the
        // fix: every SCHEMA, SCHEMA_ARG_IGNORED, KEYSET and ENRICHED_* row is measured through
        // getFlattenedSchemaNoCache, so NONE of them says anything about the cached entry point
        // getFlattenedSchema(Schema) that the published stack recipe used to name. Exactly one
        // fixture - assert SCHEMA_CACHED - calls the cached factory TWICE inside this bracket, and
        // the absence of a clear between those two calls IS that fixture. Adding a clearCache
        // there to "make it cleaner" deletes the measurement while leaving the row green.
        AvroSchemaFlattener.clearCache();
        GAvroSchemaFlattener.clearCaches();

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
                // AvroSchemaFlattener.processFieldRecursively throws on a flattened-name
                // collision. Until this catch existed that throw escaped run() and blew up the
                // parameterized test as an ERROR instead of being recorded as a comparable
                // outcome - the SCHEMA and DATA arms both caught, KEYSET did not.
                String keysetBaseline;
                try {
                    keysetBaseline = renderNames(schemaFieldNames(schemaFlattener, schema));
                } catch (Throwable t) {
                    keysetBaseline = FidelityRender.thrown(t);
                }
                baseline = keysetBaseline;
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
            case "SCHEMA_CACHED" -> {
                String[] pair = runSchemaCached(fx, avro, schema, schemaFlattener, m);
                baseline = pair[0];
                doc = pair[1];
            }
            case "ENRICHED_KEYSET" -> {
                String[] pair = runEnrichedKeyset(fx, avro, schema, schemaFlattener, flattened, m);
                baseline = pair[0];
                doc = pair[1];
            }
            case "ENRICHED_METADATA" -> {
                String[] pair = runEnrichedMetadata(fx, avro, schema, m);
                baseline = pair[0];
                doc = pair[1];
            }
            case "DATUM" -> {
                baseline = FidelityRender.text(FidelityRender.java(datum));
                doc = runDatum(avro, schema, flattened, m);
                // The AVRO disclosure gate must keep working on DATUM rows rather than silently
                // printing NOT_APPLICABLE, so the defaults arm compares the SAME entry point.
                //
                // THIS ARM IS LIVE, and it was not. Review found it recording a comparison that
                // could not fail: no Avro fixture set avro.reconstructor, so avroReconstructor(avro)
                // above and the default builder below were the identical call on every DATUM row,
                // and all six published holdsUnderDefaultReconstruction=YES on a tautology. The
                // DATA branch had disclosed that in a comment; this branch had not, so its YES read
                // as a measurement. avro-boundary-separator-datum-does-not-hold-under-defaults now
                // tunes the reconstructor and records FALSE here - measured, not asserted. Keep at
                // least one such row: delete it and this line silently reverts to a tautology.
                m.recorded.put("avroDefaultsMatch", datumDefaultsArm(flattened, schema).equals(doc));
            }
            case "DATA" -> {
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
                // THIS ARM IS LIVE ON THIS BRANCH TOO, and the comment that used to sit here was
                // false in both of its factual claims. It said "no fixture in DATA mode sets
                // avro.reconstructor ... the comparison cannot presently fail on any of the
                // eleven DATA rows", which left the arm published as a dormant tripwire. Measured
                // over the fixtures on disk: there are SIXTEEN DATA rows, and TWO of them tune
                // the reconstructor -
                //   avro-array-of-records-bracket-list-round-trip   {arrayFormat: BRACKET_LIST}
                //       records avroDefaultsMatch TRUE  (it agrees with the default anyway)
                //   avro-array-of-records-pipe-format-comma-inside-element {PIPE_SEPARATED}
                //       records avroDefaultsMatch FALSE (the row that makes this present-tense)
                // avroReconstructor() reads arrayFormat, so the two calls are genuinely different
                // instances on those rows. Keep at least one FALSE row: delete it and this line
                // silently reverts to a tautology, exactly as the DATUM sibling below warns.
                m.recorded.put("avroDefaultsMatch", avroDefaultsArm(flattened, schema).equals(doc));
            }
            default -> throw new IllegalStateException("unknown avro assert mode '" + mode
                    + "' on fixture " + fx.id() + ". A typo used to fall through to DATA, so the "
                    + "fixture measured something else entirely and still passed against its own "
                    + "recording.");
        }
        m.recorded.put("flat", flat);
        m.recorded.put("avroBaseline", baseline);
        m.recorded.put("avroDoc", doc);
        boolean lossless = doc.equals(baseline) && checksHold(m);
        m.recorded.put("losslessAvro", lossless);
        AvroSchemaFlattener.clearCache();
        GAvroSchemaFlattener.clearCaches();
    }

    /**
     * Folds every check map the mode may have recorded into the verdict.
     *
     * <p>The inverse alone is NOT a verdict on the SCHEMA stack: {@code reconstructOriginalSchema}
     * replays definitions captured during the forward pass, so it reproduces the original even
     * when the flattened schema has thrown the information away - three fixtures measured LOSSLESS
     * on the first recording run for exactly that reason. DATUM needs the same treatment for a
     * sharper reason: {@link FidelityRender} renders a {@code GenericRecord} and a
     * {@code LinkedHashMap} to byte-identical canonical text, so {@code doc.equals(baseline)} is
     * TRUE for a datum whose nested record is a raw map. A DATUM mode without this conjunction
     * would score its own defect fixtures LOSSLESS and be inert.
     *
     * <p>{@code enrichedChecks} is deliberately NOT in this loop and its absence is a decision,
     * not an omission: those entries are STRINGS precisely so that a boolean like
     * "the enriched arm threw" cannot make a correct parity control read as lossy. The strings are
     * still asserted byte-exactly by {@code reconstructionMatchesTheRecording}.</p>
     */
    private static boolean checksHold(Measurement m) {
        for (String key : new String[] {"schemaChecks", "datumChecks", "cacheChecks"}) {
            Object node = m.recorded.get(key);
            if (!(node instanceof Map<?, ?> checks)) {
                continue;
            }
            for (Object v : checks.values()) {
                if (v instanceof Boolean b && !b) {
                    return false;
                }
            }
            if (Boolean.TRUE.equals(checks.get("threw"))) {
                return false;
            }
        }
        return true;
    }

    // ---------------------------------------------------------------- AVRO: DATUM

    /**
     * The other Avro reconstruction entry point: {@code reconstruct(Map,Schema)}, which returns a
     * {@code GenericRecord} rather than a {@code Map}.
     *
     * <p>It is {@code reconstructToMap} followed by {@code mapToGenericRecord}, and that second
     * step is ONE LEVEL DEEP: it iterates the root schema's fields and calls
     * {@code builder.set(name, value)} with whatever the map held. {@code GenericRecordBuilder}
     * validates nullability and nothing else, so a {@code LinkedHashMap} lands in a record-typed
     * field and {@code build()} succeeds. The object looks right in a debugger and cannot be
     * written.</p>
     */
    private static String runDatum(JsonNode avro, Schema schema, Map<String, Object> flattened,
                                   Measurement m) {
        Map<String, Object> checks = new LinkedHashMap<>();
        String doc;
        try {
            GenericRecord rec = avroReconstructor(avro).reconstruct(flattened, schema);
            doc = FidelityRender.text(FidelityRender.java(rec));
            checks.put("reconstructReturnedARecord", true);
            checks.put("validatesAgainstSchema", GenericData.get().validate(schema, rec));
            String offenders = nonRecordNestedPaths(schema, rec, "");
            checks.put("nestedRecordsAreRecords", offenders.isEmpty());
            checks.put("nonRecordNestedPaths", offenders);
            checks.put("binaryEncodes", binaryEncodes(schema, rec, checks));
        } catch (Throwable t) {
            doc = FidelityRender.thrown(t);
            // Orientation matters: every boolean in a check map must read true == correct, because
            // the fold above turns any false into "not lossless". A key named "reconstructThrew"
            // would invert that and the row would go green on the failure.
            checks.put("reconstructReturnedARecord", false);
            checks.put("validatesAgainstSchema", false);
            checks.put("nestedRecordsAreRecords", false);
            checks.put("nonRecordNestedPaths", "");
            checks.put("binaryEncodes", false);
            checks.put("encodeFailure", "");
        }
        checks.putIfAbsent("encodeFailure", "");
        m.recorded.put("datumChecks", checks);
        return doc;
    }

    private static String datumDefaultsArm(Map<String, Object> flattened, Schema schema) {
        if (flattened == null) {
            return FidelityRender.thrown(new IllegalStateException("flatten failed"));
        }
        try {
            return FidelityRender.text(FidelityRender.java(
                    AvroReconstructor.builder().build().reconstruct(flattened, schema)));
        } catch (Throwable t) {
            return FidelityRender.thrown(t);
        }
    }

    /**
     * Can the datum actually be written?
     *
     * <p>Only the SHALLOWEST throwable's simple name is recorded, never
     * {@link FidelityRender#thrown}. {@code thrown()} walks to the ROOT cause, and under Avro 1.12
     * the writer wraps the cast in {@code TracingClassCastException} whose cause is a raw
     * {@code ClassCastException} whose message embeds module and classloader names. That text can
     * differ under a surefire isolated classloader, which would make the recording
     * machine-dependent - and a fixture that fails on someone else's laptop for no reason is a
     * fixture somebody weakens.</p>
     */
    private static boolean binaryEncodes(Schema s, GenericRecord r, Map<String, Object> checks) {
        try {
            ByteArrayOutputStream sink = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(sink, null);
            new GenericDatumWriter<GenericRecord>(s).write(r, encoder);
            encoder.flush();
            checks.put("encodeFailure", "");
            return true;
        } catch (Throwable t) {
            checks.put("encodeFailure", t.getClass().getSimpleName());
            return false;
        }
    }

    /**
     * Every place the schema says RECORD and the value is not one, rendered in the same
     * {@code path=SimpleClassName } convention {@code shadowedColumns} already uses.
     */
    private static String nonRecordNestedPaths(Schema s, Object value, String path) {
        StringBuilder out = new StringBuilder();
        walkForNonRecords(s, value, path, out);
        return out.toString().trim();
    }

    private static void walkForNonRecords(Schema s, Object value, String path, StringBuilder out) {
        if (s == null || value == null) {
            return;
        }
        switch (s.getType()) {
            case RECORD -> {
                if (!(value instanceof IndexedRecord rec)) {
                    out.append(path.isEmpty() ? "<root>" : path)
                            .append('=').append(value.getClass().getSimpleName()).append(' ');
                    return;
                }
                for (Schema.Field f : s.getFields()) {
                    String child = path.isEmpty() ? f.name() : path + "." + f.name();
                    walkForNonRecords(f.schema(), rec.get(f.pos()), child, out);
                }
            }
            case ARRAY -> {
                if (value instanceof List<?> list) {
                    for (int i = 0; i < list.size(); i++) {
                        walkForNonRecords(s.getElementType(), list.get(i), path + "[" + i + "]", out);
                    }
                }
            }
            case MAP -> {
                if (value instanceof Map<?, ?> map) {
                    for (Map.Entry<?, ?> e : map.entrySet()) {
                        walkForNonRecords(s.getValueType(), e.getValue(),
                                path + "{" + e.getKey() + "}", out);
                    }
                }
            }
            case UNION -> {
                for (Schema b : s.getTypes()) {
                    if (b.getType() == Schema.Type.RECORD || b.getType() == Schema.Type.ARRAY
                            || b.getType() == Schema.Type.MAP) {
                        walkForNonRecords(b, value, path, out);
                    }
                }
            }
            default -> { }
        }
    }

    // ---------------------------------------------------------------- AVRO: SCHEMA_CACHED

    /**
     * The CACHED factory, {@code getFlattenedSchema(Schema)} - the only entry point the published
     * stack recipe used to name and the only one no fixture had ever executed.
     */
    private static String[] runSchemaCached(FidelityFixture fx, JsonNode avro, Schema schema,
                                            AvroSchemaFlattener schemaFlattener, Measurement m) {
        Schema v2 = new Schema.Parser().parse(avro.path("avsc2").toString());
        boolean requireSameName = avro.path("requireSameFullName").asBoolean(true);
        if (requireSameName && !v2.getFullName().equals(schema.getFullName())) {
            throw new IllegalStateException("fixture " + fx.id() + " asserts SCHEMA_CACHED but its "
                    + "two schemas have different full names, so the cache key differs and the "
                    + "fixture measures nothing. Set avro.requireSameFullName=false only if that "
                    + "is deliberately the control.");
        }

        Map<String, Object> checks = new LinkedHashMap<>();
        // Recorded as a STRING on purpose: it is an exemption to make visible, not a correctness
        // boolean, and folding it would make the control row read as lossy.
        checks.put("sameFullNameRequired", String.valueOf(requireSameName));

        // NO clearCache between these two calls. That absence is the fixture.
        Schema f1 = schemaFlattener.getFlattenedSchema(schema);
        Schema f2 = schemaFlattener.getFlattenedSchema(v2);

        String baseline = describe(new AvroSchemaFlattener(
                avro.path("schemaFlattener").path("includeArrayStatistics").asBoolean(false),
                avro.path("schemaFlattener").path("includeNonTerminalArrays").asBoolean(true))
                .getFlattenedSchemaNoCache(v2));
        String doc = describe(f2);

        checks.put("firstFlattenedColumns", renderNames(fieldNames(f1)));
        checks.put("secondFlattenedColumns", renderNames(fieldNames(f2)));
        checks.put("cacheDistinguishesTheTwoSchemas", f1 != f2);
        checks.put("secondFlatteningMatchesItsOwnSchema", doc.equals(baseline));
        String inverse;
        try {
            inverse = schemaFlattener.reconstructOriginalSchema(f2).toString();
        } catch (Throwable t) {
            inverse = FidelityRender.thrown(t);
        }
        checks.put("inverseOfSecond", inverse);
        checks.put("inverseReproducesSecondSchema", inverse.equals(v2.toString()));
        m.recorded.put("cacheChecks", checks);
        return new String[] {baseline, doc};
    }

    private static List<String> fieldNames(Schema schema) {
        List<String> names = new ArrayList<>();
        for (Schema.Field f : schema.getFields()) {
            names.add(f.name());
        }
        return names;
    }

    // ---------------------------------------------------------------- AVRO: enriched schema API

    /**
     * Compares the enriched flattener's column names against another producer of the same flat
     * namespace. Comparative on purpose: the property "these two agree" can genuinely pass and
     * genuinely fail, which is what makes each row signal rather than a constant.
     */
    private static String[] runEnrichedKeyset(FidelityFixture fx, JsonNode avro, Schema schema,
                                              AvroSchemaFlattener legacy,
                                              Map<String, Object> flattened, Measurement m) {
        FlattenOptions options = FidelityEnriched.buildOptions(fx.id(), avro);
        String comparator = avro.path("enrichedCompare").asText("");
        m.recorded.put("enrichedCompare", comparator);
        m.recorded.put("enrichedOptions", FidelityEnriched.renderOptions(options));

        String baseline;
        String emissionOrder;
        try {
            List<FlattenedField> fields = FidelityEnriched.flatten(options, schema);
            List<String> names = FidelityEnriched.names(fields);
            baseline = renderNames(names);
            // renderNames SORTS, which destroys the emission order that position() and positional
            // injection are about. Recorded separately, unsorted, so an ordering change is visible.
            emissionOrder = FidelityRender.text(names.stream().map(n -> (Object) ("S:" + n)).toList());
        } catch (Throwable t) {
            baseline = FidelityRender.thrown(t);
            emissionOrder = baseline;
        }
        m.recorded.put("enrichedNames", emissionOrder);

        String doc;
        try {
            doc = switch (comparator) {
                case "MAP_FLATTENER" -> flattened == null
                        ? FidelityRender.thrown(new IllegalStateException("flatten failed"))
                        : renderNames(new ArrayList<>(flattened.keySet()));
                case "LEGACY_AVRO_SCHEMA_FLATTENER" -> renderNames(schemaFieldNames(legacy, schema));
                case "GAVRO_SCHEMA_FLATTENER" -> renderNames(
                        new ArrayList<>(new GAvroSchemaFlattener().flattenSchema(schema).keySet()));
                case "ENRICHED_STREAM" -> renderNames(FidelityEnriched.streamNames(options, schema));
                default -> throw new IllegalStateException("fixture " + fx.id()
                        + " declares unknown avro.enrichedCompare '" + comparator + "'");
            };
        } catch (IllegalStateException unknown) {
            throw unknown;
        } catch (Throwable t) {
            doc = FidelityRender.thrown(t);
        }
        return new String[] {baseline, doc};
    }

    /** Compares what the enriched flattener REPORTS about a leaf against what the schema DECLARES. */
    private static String[] runEnrichedMetadata(FidelityFixture fx, JsonNode avro, Schema schema,
                                                Measurement m) {
        FlattenOptions options = FidelityEnriched.buildOptions(fx.id(), avro);
        String comparator = avro.path("enrichedCompare").asText("");
        m.recorded.put("enrichedCompare", comparator);
        m.recorded.put("enrichedOptions", FidelityEnriched.renderOptions(options));

        List<FlattenedField> fields;
        try {
            fields = FidelityEnriched.flatten(options, schema);
        } catch (Throwable t) {
            String failed = FidelityRender.thrown(t);
            m.recorded.put("propertyPlacement", failed);
            return new String[] {failed + " (declared side not reached)", failed};
        }
        m.recorded.put("propertyPlacement", FidelityEnriched.propertyPlacement(fields));

        return switch (comparator) {
            // A SET comparison on purpose: with inheritance on, a record-level property reaches N
            // leaves, so a per-path comparison would report divergence for a reason that is a
            // feature. Placement is pinned separately, above, but is not the verdict.
            case "PROPERTY_SET" -> new String[] {
                    FidelityEnriched.declaredPropertySet(schema),
                    FidelityEnriched.emittedPropertySet(fields)};
            case "DECLARED_DOC" -> new String[] {
                    FidelityEnriched.declaredDocs(schema),
                    FidelityEnriched.emittedDocs(fields)};
            case "DECODED_PATH" -> new String[] {
                    FidelityEnriched.declaredPaths(fields),
                    FidelityEnriched.decodedPaths(fields, options)};
            default -> throw new IllegalStateException("fixture " + fx.id()
                    + " declares unknown avro.enrichedCompare '" + comparator + "'");
        };
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
        String kind = probe.path("kind").asText();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("kind", kind);
        result.put("expect", probe.path("expect").asText(""));

        // A schema-only probe must not depend on the datum parsing, and a javaInput fixture has no
        // input TEXT to re-parse - the source is built once and shared so that identity, which is
        // what detectCircularReferences keys on, is the same graph in both arms.
        if ("RECONSTRUCT_CONFIG_COMPARE".equals(kind)) {
            Map<String, Object> source = source(fx);
            String a;
            String b;
            try {
                Map<String, Object> flat = flattener(fx.config()).flatten(source);
                a = FidelityRender.text(FidelityRender.java(
                        reconstructor(probe.path("configA")).reconstruct(flat)));
                b = FidelityRender.text(FidelityRender.java(
                        reconstructor(probe.path("configB")).reconstruct(flat)));
            } catch (Throwable t) {
                a = FidelityRender.thrown(t);
                b = a;
            }
            result.put("a", a);
            result.put("b", b);
            result.put("equal", a.equals(b));
        } else if ("FLATTEN_CONFIG_COMPARE".equals(kind)) {
            // Each arm is captured independently: one configuration can throw where the other does
            // not, and "it threw" must be a comparable outcome rather than an aborted measurement.
            // The detection-off arm of the cycle fixtures drives MapFlattener into stringifyObject
            // on a self-referential container, which is exactly that case.
            String a = flattenArm(fx, probe.path("configA"));
            String b = flattenArm(fx, probe.path("configB"));
            result.put("a", a);
            result.put("b", b);
            result.put("equal", a.equals(b));
        } else if ("ENRICHED_CONFIG_COMPARE".equals(kind)) {
            Schema schema = new Schema.Parser().parse(fx.config().path("avro").path("avsc").toString());
            String a = enrichedArm(fx.id(), probe.path("configA"), schema);
            String b = enrichedArm(fx.id(), probe.path("configB"), schema);
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
            Object typed = substitute(source(fx), twin.path("key").asText(),
                    new BigDecimal(twin.path("decimal").asText()));
            @SuppressWarnings("unchecked")
            Map<String, Object> typedMap = (Map<String, Object>) typed;
            result.put("twinFlat", FidelityRender.text(FidelityRender.java(
                    flattener(fx.config()).flatten(typedMap))));
        }
        m.recorded.put("probe", result);
    }

    private static String flattenArm(FidelityFixture fx, JsonNode config) {
        try {
            return FidelityRender.text(FidelityRender.java(flattener(config).flatten(source(fx))));
        } catch (Throwable t) {
            return FidelityRender.thrown(t);
        }
    }

    /**
     * Renders every leaf's doc, inheritance flag, mapped type, properties, nullability, array
     * membership and position - NOT just its name. A probe that rendered only the name would
     * report EQUAL for {@code inheritDoc}, for {@code inheritRecordProperties} and for a live doc
     * control alike, which is the exact "appears present and does nothing" failure this corpus has
     * already hit four times inside its own harness.
     */
    private static String enrichedArm(String fixtureId, JsonNode config, Schema schema) {
        try {
            FlattenOptions options = FidelityEnriched.buildOptions(fixtureId,
                    config.isObject() ? config : LENIENT.createObjectNode());
            return FidelityEnriched.renderLeaves(FidelityEnriched.flatten(options, schema));
        } catch (Throwable t) {
            return FidelityRender.thrown(t);
        }
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
