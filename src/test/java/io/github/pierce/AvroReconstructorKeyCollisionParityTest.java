package io.github.pierce;

import org.apache.avro.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE FENCE for the [BL-023] reconstruct-side repair: the AVRO stack must not move.
 *
 * <h2>Why the Avro path is already correct</h2>
 *
 * <p>{@code AvroReconstructor} has no {@code setNestedValue}. {@code PathNode.addPath} sets
 * {@code isLeaf}/{@code value} on the node AND separately does {@code children.computeIfAbsent},
 * so one node holds the leaf value and the subtree at the same time. Neither write can clobber
 * the other and the tree is order-independent by construction; the SCHEMA then decides which of
 * the two a field reads. One side is still dropped for a genuinely colliding document, but
 * deterministically and never with a fabricated key.</p>
 *
 * <h2>Honest statement of what this test is</h2>
 *
 * <p>It PASSES before and after the JSON-side repair. It is not a bug demonstration - it is the
 * canary. Three corpus rows carry colliding keys on the AVRO stack
 * ({@code avro-array-of-records-null-element-annihilates-array},
 * {@code avro-array-of-records-nullable-nested-record-shadowed},
 * {@code avro-array-element-multi-branch-union-mixed-branches}) and none of their recorded
 * strings may move when the corpus is re-recorded. If this test ever goes red, the JSON-side
 * change has leaked into shared code and the answer is to revert the leak, not to re-record the
 * three rows.</p>
 */
@DisplayName("the Avro stack was already order-independent on the same colliding keys")
class AvroReconstructorKeyCollisionParityTest {

    private static final String SCHEMA = """
            {"type":"record","name":"Root","fields":[
              {"name":"orders","type":{"type":"array","items":
                {"type":"record","name":"Order","fields":[
                  {"name":"id","type":"string"}
                ]}}}
            ]}
            """;

    @Test
    @DisplayName("the same colliding pair yields the same record in either key order")
    void theSameCollidingPairYieldsTheSameRecordInEitherKeyOrder() {
        Schema schema = new Schema.Parser().parse(SCHEMA);
        AvroReconstructor avro = AvroReconstructor.builder().build();

        Map<String, Object> branchFirst = new LinkedHashMap<>();
        branchFirst.put("orders_id", "[\"O1\",\"O2\"]");
        branchFirst.put("orders", "[null,null]");

        Map<String, Object> leafFirst = new LinkedHashMap<>();
        leafFirst.put("orders", "[null,null]");
        leafFirst.put("orders_id", "[\"O1\",\"O2\"]");

        Map<String, Object> hashOrder = new HashMap<>(leafFirst);

        Map<String, Object> a = avro.reconstructToMap(branchFirst, schema);
        Map<String, Object> b = avro.reconstructToMap(leafFirst, schema);
        Map<String, Object> c = avro.reconstructToMap(hashOrder, schema);

        assertEquals(String.valueOf(a), String.valueOf(b),
                "PathNode holds leaf and children side by side; if these ever differ, the JSON "
                        + "reconstructor's collision handling has leaked onto the Avro path");
        assertEquals(String.valueOf(a), String.valueOf(c));
    }

    @Test
    @DisplayName("MEASURED CORRECTION: the Avro stack loses the record branch too - deterministically")
    void theAvroStackAlsoLosesTheRecordBranchButWithoutOrderDependenceOrFabrication() {
        // A CORRECTION TO THIS CLASS'S FIRST DRAFT, stated rather than quietly absorbed. The
        // draft asserted "the schema disambiguates: orders_id wins over the bare orders column"
        // and expected {orders=[{id=O1}, {id=O2}]}. MEASURED, it returns {orders=[]}: the bare
        // base column is consulted for the array and the record branch is dropped. The Avro side
        // is NOT correct on this shape - the corpus already says so, in
        // avro/avro-array-of-records-null-element-annihilates-array, a DEFECT row whose title is
        // "A single null element destroys the records in an array".
        //
        // What is true, and all this fence claims, is the two properties the JSON side lacked:
        // the outcome does not depend on key order, and no key the source never carried is
        // invented. Those are the properties that must survive the [BL-023] repair untouched.
        Schema schema = new Schema.Parser().parse(SCHEMA);

        Map<String, Object> flat = new LinkedHashMap<>();
        flat.put("orders", "[null,null]");
        flat.put("orders_id", "[\"O1\",\"O2\"]");

        Map<String, Object> out = AvroReconstructor.builder().build().reconstructToMap(flat, schema);
        assertEquals("{orders=[]}", String.valueOf(out),
                "recorded as measured; if this moves, the JSON-side collision change has leaked "
                        + "onto the Avro path and must be reverted rather than re-recorded");
    }
}
