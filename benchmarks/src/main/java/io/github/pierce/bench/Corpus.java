package io.github.pierce.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Deterministic corpus generator for the NexusPiercer benchmarks.
 *
 * <p>Five shapes, each chosen to isolate a different complexity dimension. The point of separate
 * corpora rather than one "realistic" blob is attribution: when a number moves you need to know
 * which property of the input made it move. A single mixed corpus tells you throughput changed
 * but not whether the cause was depth, key count, or array size.</p>
 *
 * <table>
 *   <caption>Corpus design</caption>
 *   <tr><th>Shape</th><th>Isolates</th></tr>
 *   <tr><td>{@code wideFlat}</td><td>Cost linear in key count, independent of structure</td></tr>
 *   <tr><td>{@code deepNarrow}</td><td>Cost driven by depth — quadratic-in-depth effects</td></tr>
 *   <tr><td>{@code arrayHeavy}</td><td>Per-element cost and allocation rate</td></tr>
 *   <tr><td>{@code unionNullable}</td><td>Exception-driven control flow on type dispatch</td></tr>
 *   <tr><td>{@code mixedProduction}</td><td>Headline number; realistic shape</td></tr>
 * </table>
 *
 * <p><b>Determinism is mandatory.</b> Every generator is seeded from a fixed constant, so the
 * baseline run and the pull-request run measure byte-identical inputs. A corpus that varies
 * between runs turns the regression gate into a random number generator. {@link Random} with an
 * explicit seed is used rather than {@code Math.random()} for exactly this reason.</p>
 *
 * <p><b>Field names deliberately contain the separator.</b> {@code user_id}, {@code created_at}
 * and {@code order_total} are snake_case, which is what real schemas in this library's target
 * domain look like. That makes {@code mixedProduction} double as a live regression test for the
 * key-injectivity defect (arch/NP-002) rather than quietly avoiding the case that breaks.</p>
 */
public final class Corpus {

    /** Fixed so baseline and PR runs measure identical bytes. Do not derive from the clock. */
    private static final long SEED = 0x4E455855_53504943L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Corpus() {
    }

    // ------------------------------------------------------------------ shapes

    /**
     * 1 record, 1,000 scalar fields at depth 1, mixed types, ~45 KB serialized.
     *
     * <p>Everything here costs O(keys) and nothing costs O(depth), so a change that halves
     * per-key work shows up cleanly with no structural confound.</p>
     */
    public static ObjectNode wideFlat() {
        Random rnd = new Random(SEED);
        ObjectNode root = MAPPER.createObjectNode();
        for (int i = 0; i < 1_000; i++) {
            switch (i % 4) {
                case 0 -> root.put("field_str_" + i, randomToken(rnd, 12));
                case 1 -> root.put("field_long_" + i, rnd.nextLong(1_000_000L));
                case 2 -> root.put("field_dbl_" + i, Math.round(rnd.nextDouble() * 1e4) / 1e2);
                default -> root.put("field_bool_" + i, rnd.nextBoolean());
            }
        }
        return root;
    }

    /**
     * Depth 24, exactly one field per level, one leaf value, ~2 KB serialized.
     *
     * <p>Deliberately tiny so that anything measurable is depth-driven rather than size-driven.
     * This is the discriminating corpus for the per-node map allocation in {@code MapFlattener}
     * (a leaf at depth d is re-inserted ~2d times) and for prefix rebuilding in {@code buildKey}.</p>
     *
     * @param depth nesting depth; 24 is the standard, 64 exercises the maxDepth boundary
     */
    public static ObjectNode deepNarrow(int depth) {
        ObjectNode root = MAPPER.createObjectNode();
        ObjectNode cur = root;
        for (int i = 0; i < depth - 1; i++) {
            cur = cur.putObject("level_" + i);
        }
        cur.put("leaf_value", "terminal");
        return root;
    }

    /**
     * 20 scalar arrays x 500 elements, plus 5 arrays-of-records x 100 x 8 fields. ~600 KB.
     *
     * <p>The allocation-rate headline corpus: 14,000 leaves, which is what it is for.</p>
     *
     * <p>Half the scalar arrays hold all-digit strings and half hold {@code randomToken} output
     * over {@code [a-z0-9]}. This used to be described as "numeric-looking strings, which is
     * what drives exception-based type detection to construct one NumberFormatException per
     * element". That was wrong twice over, and the correction matters to anyone reasoning about
     * filter hit rates: the exception behaviour was removed in 843a461, and an 8-character token
     * over a 36-symbol alphabet survives a double-grammar character filter only about 2% of the
     * time, so the string arrays were always CHEAP to reject rather than expensive.</p>
     */
    public static ObjectNode arrayHeavy() {
        Random rnd = new Random(SEED);
        ObjectNode root = MAPPER.createObjectNode();

        for (int a = 0; a < 10; a++) {
            ArrayNode arr = root.putArray("numeric_array_" + a);
            for (int i = 0; i < 500; i++) {
                arr.add(rnd.nextInt(100_000));
            }
        }
        for (int a = 0; a < 10; a++) {
            ArrayNode arr = root.putArray("string_array_" + a);
            for (int i = 0; i < 500; i++) {
                arr.add(randomToken(rnd, 8));
            }
        }
        for (int a = 0; a < 5; a++) {
            ArrayNode arr = root.putArray("record_array_" + a);
            for (int i = 0; i < 100; i++) {
                ObjectNode rec = arr.addObject();
                for (int f = 0; f < 8; f++) {
                    rec.put("nested_field_" + f, randomToken(rnd, 6));
                }
            }
        }
        return root;
    }

    /**
     * 200 fields, each a 3-branch union, ~12 KB.
     *
     * <p>Value distribution is skewed on purpose: 15% null and 70% matching the LAST branch, so
     * trial-and-error branch selection pays its worst realistic cost. Includes a US-format
     * {@code M/d/yyyy} date column and a {@code ["null","long","string"]} union carrying
     * non-numeric strings.</p>
     *
     * <p><b>This corpus measures the worst realistic case, not the average one.</b> An all-ISO-8601,
     * first-branch-dominant workload would show near-zero cost on the same code. Both numbers
     * must be reported together; quoting only this one would overstate the win.</p>
     */
    public static ObjectNode unionNullable() {
        Random rnd = new Random(SEED);
        ObjectNode root = MAPPER.createObjectNode();
        for (int i = 0; i < 200; i++) {
            int roll = rnd.nextInt(100);
            String name = "union_field_" + i;
            if (roll < 15) {
                root.putNull(name);
            } else if (roll < 30) {
                root.put(name, rnd.nextLong(1_000_000L));
            } else {
                // Non-numeric string: forces the long branch to fail before string is reached.
                root.put(name, randomToken(rnd, 10));
            }
        }
        // Non-ISO date: exercises format-detection fallback.
        root.put("created_at", "3/14/2024");
        root.put("updated_at", "12/25/2023");
        root.put("mixed_union_value", "not-a-number-at-all");
        return root;
    }

    /**
     * The headline corpus: 250 fields, depth 4, 12 arrays with a realistic size distribution
     * (p50 = 8, mean ~30, p99 = 400), 40% nullable. ~35 KB per record.
     *
     * <p>Field names are snake_case and therefore contain the separator, which makes this a live
     * regression test for key injectivity as well as a performance benchmark.</p>
     */
    public static ObjectNode mixedProduction() {
        Random rnd = new Random(SEED);
        ObjectNode root = MAPPER.createObjectNode();

        root.put("order_id", "ord_" + randomToken(rnd, 10));
        root.put("created_at", "2026-03-14T09:26:53Z");
        root.put("order_total", Math.round(rnd.nextDouble() * 100_000) / 100.0);

        ObjectNode user = root.putObject("user");
        user.put("user_id", rnd.nextLong(1_000_000L));
        user.put("display_name", randomToken(rnd, 14));
        ObjectNode addr = user.putObject("billing_address");
        addr.put("street_line_1", randomToken(rnd, 18));
        addr.put("postal_code", String.format("%05d", rnd.nextInt(100_000)));
        ObjectNode geo = addr.putObject("geo_location");
        geo.put("latitude_deg", rnd.nextDouble() * 180 - 90);
        geo.put("longitude_deg", rnd.nextDouble() * 360 - 180);

        for (int i = 0; i < 220; i++) {
            String name = "attribute_" + i + "_value";
            if (rnd.nextInt(100) < 40) {
                root.putNull(name);
            } else if (i % 3 == 0) {
                root.put(name, rnd.nextLong(1_000_000L));
            } else {
                root.put(name, randomToken(rnd, 9));
            }
        }

        for (int a = 0; a < 12; a++) {
            ArrayNode arr = root.putArray("line_items_" + a);
            for (int i = 0; i < arraySizeSample(rnd); i++) {
                ObjectNode item = arr.addObject();
                item.put("sku_code", randomToken(rnd, 8));
                item.put("quantity_ordered", rnd.nextInt(50) + 1);
                item.put("unit_price", Math.round(rnd.nextDouble() * 10_000) / 100.0);
            }
        }
        return root;
    }

    // ------------------------------------------------------------------ helpers

    /** Skewed array-size draw: p50 = 8, occasional long tail to 400. */
    private static int arraySizeSample(Random rnd) {
        int roll = rnd.nextInt(100);
        if (roll < 50) return 1 + rnd.nextInt(15);
        if (roll < 90) return 15 + rnd.nextInt(50);
        if (roll < 99) return 65 + rnd.nextInt(135);
        return 200 + rnd.nextInt(200);
    }

    private static final char[] ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    private static String randomToken(Random rnd, int len) {
        char[] buf = new char[len];
        for (int i = 0; i < len; i++) {
            buf[i] = ALPHABET[rnd.nextInt(ALPHABET.length)];
        }
        return new String(buf);
    }

    /** Serializes a node once. Benchmarks that measure parsing should call this in setup. */
    public static String toJson(ObjectNode node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (Exception e) {
            throw new IllegalStateException("corpus serialization failed", e);
        }
    }

    /** Converts a node to a nested Map, for the Map-based flatteners. */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> toMap(ObjectNode node) {
        return MAPPER.convertValue(node, LinkedHashMap.class);
    }

    /** Builds a batch of distinct-but-deterministic records for throughput measurement. */
    public static List<String> batch(int count) {
        List<String> out = new ArrayList<>(count);
        Random rnd = new Random(SEED);
        for (int i = 0; i < count; i++) {
            ObjectNode n = mixedProduction();
            // Vary identity per record so per-record caches are exercised honestly rather than
            // measuring a 100% hit rate that production would never see.
            n.put("order_id", "ord_" + i + "_" + rnd.nextInt(1_000));
            out.add(toJson(n));
        }
        return out;
    }
}
