package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.pierce.path.FlattenedPath;
import io.github.pierce.schema.EnrichedSchemaFlattener;
import io.github.pierce.schema.FlattenOptions;
import io.github.pierce.schema.FlattenedField;
import io.github.pierce.schema.NameCollisionPolicy;
import io.github.pierce.schema.PathSegment;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Everything the two enriched-schema assert modes need, kept out of {@code FidelityRunner}.
 *
 * <p>{@code runAvro} is already a long method with a multi-arm switch, and the ratchet in
 * {@code .github/quality-baseline.json} may only go down. Two hundred more lines inline is the
 * most likely way this work buys a cyclomatic-complexity finding that cannot then be paid for.</p>
 */
final class FidelityEnriched {

    private FidelityEnriched() {
    }

    // ------------------------------------------------------------------ options

    /**
     * Reads {@code avro.enriched}. Absent means default {@link FlattenOptions}.
     *
     * <p>Two ways to configure a row, and they are mutually exclusive. {@code "preset": "NAME"}
     * calls the named {@link FlattenOptions} FACTORY, so a fixture can pin the factory's own
     * knobs; anything else is spelled out knob by knob. Before the preset hatch existed, two rows
     * carried {@code gAvroParity} in their titles while measuring
     * {@code FlattenOptions.builder().build()} - the factory whose name makes the claim was
     * unobservable from the corpus, which is how its claim rotted.</p>
     *
     * <p>An unknown preset THROWS rather than falling through to the defaults, and a preset
     * combined with explicit knobs is refused rather than merged. Both for the same reason: a
     * fixture that declares one configuration and silently measures another still passes, and the
     * runner already carries a {@code default ->} guard for exactly that failure on the
     * comparator selector.</p>
     */
    static FlattenOptions buildOptions(String fixtureId, JsonNode avro) {
        java.util.Objects.requireNonNull(fixtureId, "fixtureId");
        JsonNode e = avro == null ? null : avro.path("enriched");
        FlattenOptions.Builder b = FlattenOptions.builder();
        if (e == null || !e.isObject() || e.size() == 0) {
            return b.build();
        }
        if (e.has("preset")) {
            return preset(fixtureId, e);
        }
        if (e.has("separator")) {
            b.separator(e.get("separator").asText());
        }
        if (e.has("arrayBoundarySeparator")) {
            b.arrayBoundarySeparator(e.get("arrayBoundarySeparator").asText());
        }
        if (e.has("inheritDoc")) {
            b.inheritDoc(e.get("inheritDoc").asBoolean());
        }
        if (e.has("inheritRecordProperties")) {
            b.inheritRecordProperties(e.get("inheritRecordProperties").asBoolean());
        }
        if (e.has("unwrapNullableUnions")) {
            b.unwrapNullableUnions(e.get("unwrapNullableUnions").asBoolean());
        }
        if (e.has("collisionPolicy")) {
            b.collisionPolicy(NameCollisionPolicy.valueOf(e.get("collisionPolicy").asText()));
        }
        if (e.has("maxDepth")) {
            b.maxDepth(e.get("maxDepth").asInt());
        }
        if (e.has("maxFields")) {
            b.maxFields(e.get("maxFields").asInt());
        }
        for (JsonNode inject : e.path("inject")) {
            b.injectField(inject.path("position").asInt(), FlattenedField.builder()
                    .flattenedName(inject.path("flattenedName").asText())
                    .name(inject.path("name").asText())
                    .avroType(Schema.Type.valueOf(inject.path("avroType").asText("STRING")))
                    .schema(Schema.create(Schema.Type.valueOf(inject.path("avroType").asText("STRING"))))
                    .synthetic(true)
                    .build());
        }
        return b.build();
    }

    /** Resolves {@code enriched.preset} to a named {@link FlattenOptions} factory. */
    private static FlattenOptions preset(String fixtureId, JsonNode enriched) {
        if (enriched.size() > 1) {
            java.util.Set<String> extra = new TreeSet<>();
            enriched.fieldNames().forEachRemaining(extra::add);
            extra.remove("preset");
            throw new IllegalStateException("fixture " + fixtureId + " declares a 'preset' "
                    + "alongside explicit knobs " + extra + ". A preset a neighbouring key can "
                    + "partially override is a preset whose recorded name no longer describes "
                    + "what was measured - choose one or the other.");
        }
        String name = enriched.path("preset").asText("").trim();
        return switch (name) {
            case "DEFAULTS" -> FlattenOptions.defaults();
            case "GAVRO_PARITY" -> FlattenOptions.gAvroParity();
            default -> throw new IllegalStateException("fixture " + fixtureId
                    + " declares unknown avro.enriched.preset '" + name + "'. Known presets: "
                    + "DEFAULTS, GAVRO_PARITY. Falling through to the defaults on a typo would let "
                    + "this fixture measure a configuration it does not declare.");
        };
    }

    /** Human-readable, recorded so the exact options are pinned byte-for-byte. */
    static String renderOptions(FlattenOptions o) {
        return "separator=" + o.separator()
                + ";arrayBoundarySeparator=" + o.arrayBoundarySeparator()
                + ";inheritDoc=" + o.inheritDoc()
                + ";inheritRecordProperties=" + o.inheritRecordProperties()
                + ";unwrapNullableUnions=" + o.unwrapNullableUnions()
                + ";collisionPolicy=" + o.collisionPolicy()
                + ";maxDepth=" + o.maxDepth()
                + ";maxFields=" + o.maxFields()
                + ";injected=" + new TreeSet<>(o.injectedFields().keySet());
    }

    // ------------------------------------------------------------------ name producers

    static List<FlattenedField> flatten(FlattenOptions options, Schema schema) {
        return new EnrichedSchemaFlattener(options).flatten(schema);
    }

    static List<String> names(List<FlattenedField> fields) {
        List<String> out = new ArrayList<>(fields.size());
        for (FlattenedField f : fields) {
            out.add(f.flattenedName());
        }
        return out;
    }

    static List<String> streamNames(FlattenOptions options, Schema schema) {
        List<String> out = new ArrayList<>();
        new EnrichedSchemaFlattener(options).stream(schema, f -> out.add(f.flattenedName()));
        return out;
    }

    // ------------------------------------------------------------------ metadata comparators

    /**
     * Every custom property the producer declared anywhere in the schema, as a sorted distinct set
     * of {@code key=value} pairs.
     *
     * <p>Harvested from Avro's own {@code getObjectProps}, deliberately NOT mirroring any
     * reserved-name filter the library might grow. That independence is what made the private
     * blocklist's silent drop measurable in the first place, and it is what would make a
     * reintroduced one measurable again.</p>
     */
    static String declaredPropertySet(Schema schema) {
        java.util.Set<String> out = new TreeSet<>();
        collectDeclared(schema, new java.util.HashSet<>(), out);
        return String.join(" ", out);
    }

    private static void collectDeclared(Schema s, java.util.Set<String> seen, java.util.Set<String> out) {
        if (s == null) {
            return;
        }
        switch (s.getType()) {
            case RECORD -> {
                if (!seen.add(s.getFullName())) {
                    return;
                }
                s.getObjectProps().forEach((k, v) -> out.add(k + "=" + v));
                for (Schema.Field f : s.getFields()) {
                    f.getObjectProps().forEach((k, v) -> out.add(k + "=" + v));
                    collectDeclared(f.schema(), seen, out);
                }
            }
            case ARRAY -> collectDeclared(s.getElementType(), seen, out);
            case MAP -> collectDeclared(s.getValueType(), seen, out);
            case UNION -> {
                for (Schema b : s.getTypes()) {
                    collectDeclared(b, seen, out);
                }
            }
            default -> { }
        }
    }

    /** The same shape, harvested from the leaves the flattener emitted. */
    static String emittedPropertySet(List<FlattenedField> fields) {
        java.util.Set<String> out = new TreeSet<>();
        for (FlattenedField f : fields) {
            f.properties().forEach((k, v) -> out.add(k + "=" + v));
        }
        return String.join(" ", out);
    }

    /**
     * Which leaf carries which property. Pinned byte-exactly but deliberately NOT folded into the
     * verdict: with inheritance on, one record-level property reaches N leaves, so a per-path
     * comparison would report divergence for a reason that is a feature.
     */
    static String propertyPlacement(List<FlattenedField> fields) {
        Map<String, Object> out = new TreeMap<>();
        for (FlattenedField f : fields) {
            out.put(f.sourcePath(), "S:" + new TreeSet<>(f.properties().keySet()));
        }
        return FidelityRender.text(new LinkedHashMap<String, Object>(out));
    }

    /** Declared documentation, keyed by source path, with inheritance always false. */
    static String declaredDocs(Schema schema) {
        Map<String, Object> out = new TreeMap<>();
        collectDeclaredDocs(schema, "", new java.util.ArrayDeque<>(), out);
        return FidelityRender.text(new LinkedHashMap<String, Object>(out));
    }

    private static void collectDeclaredDocs(Schema s, String prefix,
                                            java.util.Deque<String> open, Map<String, Object> out) {
        Schema resolved = unwrap(s);
        if (resolved.getType() != Schema.Type.RECORD) {
            return;
        }
        if (open.contains(resolved.getFullName())) {
            return;
        }
        open.push(resolved.getFullName());
        for (Schema.Field f : resolved.getFields()) {
            String path = prefix.isEmpty() ? f.name() : prefix + "." + f.name();
            Schema fs = unwrap(f.schema());
            if (fs.getType() == Schema.Type.RECORD) {
                collectDeclaredDocs(fs, path, open, out);
            } else if (fs.getType() == Schema.Type.ARRAY
                    && unwrap(fs.getElementType()).getType() == Schema.Type.RECORD) {
                collectDeclaredDocs(unwrap(fs.getElementType()), path, open, out);
            } else {
                out.put(path, "S:doc=" + f.doc() + ";inherited=false");
            }
        }
        open.pop();
    }

    /** Reported documentation, keyed by source path, with the library's inheritance flag. */
    static String emittedDocs(List<FlattenedField> fields) {
        Map<String, Object> out = new TreeMap<>();
        for (FlattenedField f : fields) {
            out.put(f.sourcePath(), "S:doc=" + f.doc().orElse(null)
                    + ";inherited=" + f.isDocInherited());
        }
        return FidelityRender.text(new LinkedHashMap<String, Object>(out));
    }

    /** The source path's own segment names, keyed by source path. */
    static String declaredPaths(List<FlattenedField> fields) {
        Map<String, Object> out = new TreeMap<>();
        for (FlattenedField f : fields) {
            List<String> segs = new ArrayList<>();
            for (PathSegment s : f.pathSegments()) {
                segs.add(s.name());
            }
            out.put(f.sourcePath(), "S:" + segs);
        }
        return FidelityRender.text(new LinkedHashMap<String, Object>(out));
    }

    /**
     * What decoding the flattened NAME back through {@link FlattenedPath} actually recovers.
     *
     * <p>DO NOT "FIX" THIS to be array-aware. It passes {@code options.separator()} and ignores
     * {@code arrayBoundarySeparator()} on purpose: that IS the divergence the contract discloses,
     * and teaching it the boundary marker would flip the disclosure row to LOSSLESS and delete the
     * warning rather than earn it. The rendered name is not a decodable structure - see
     * {@link io.github.pierce.schema.NameCollisionPolicy#ESCAPE}.</p>
     */
    static String decodedPaths(List<FlattenedField> fields, FlattenOptions options) {
        Map<String, Object> out = new TreeMap<>();
        for (FlattenedField f : fields) {
            out.put(f.sourcePath(),
                    "S:" + FlattenedPath.decodeSegments(f.flattenedName(), options.separator()));
        }
        return FidelityRender.text(new LinkedHashMap<String, Object>(out));
    }

    /**
     * The full per-leaf rendering used by the {@code ENRICHED_CONFIG_COMPARE} probe.
     *
     * <p>Name alone would report EQUAL for {@code inheritDoc}, for
     * {@code inheritRecordProperties} and for a genuinely live doc control alike - a probe that
     * appears present and does nothing. Doc, the inheritance flag and the property set are
     * therefore all in the rendering.</p>
     */
    static String renderLeaves(List<FlattenedField> fields) {
        Map<String, Object> out = new TreeMap<>();
        for (FlattenedField f : fields) {
            out.put(f.flattenedName(), "S:doc=" + f.doc().orElse(null)
                    + ";docInherited=" + f.isDocInherited()
                    + ";mappedType=" + f.mappedType().orElse(null)
                    + ";props=" + new TreeMap<>(f.properties())
                    + ";nullable=" + f.isNullable()
                    + ";withinArray=" + f.isWithinArray()
                    + ";position=" + f.position());
        }
        return FidelityRender.text(new LinkedHashMap<String, Object>(out));
    }

    private static Schema unwrap(Schema s) {
        if (s.getType() != Schema.Type.UNION) {
            return s;
        }
        for (Schema b : s.getTypes()) {
            if (b.getType() != Schema.Type.NULL) {
                return b;
            }
        }
        return s;
    }
}
