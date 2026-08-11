package io.github.pierce.examples;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Makes {@link StreamingProcessorExample} executable.
 *
 * <p>The example arrived as 87 lines of Groovy with no {@code main}, no caller anywhere in the
 * repository and no assertion — compiled on every build, run never. Its claims (a schema is
 * flattened once per schemaId and reused; a batch types every record) are exactly the kind of
 * thing that rots unobserved, so they are asserted here.</p>
 */
class StreamingProcessorExampleTest {

    /** The schema the processor is supposed to keep using once it has cached it. */
    private static Schema eventSchema() {
        return SchemaBuilder.record("Event").fields()
                .requiredString("orderId")
                .requiredInt("quantity")
                .endRecord();
    }

    /**
     * A schema with entirely different field names. If the processor ever re-flattens for a
     * schemaId it has already seen, the typed output switches to THIS schema's columns — which
     * is what makes the cache observable from outside.
     */
    private static Schema decoySchema() {
        return SchemaBuilder.record("Decoy").fields()
                .requiredString("somethingElse")
                .endRecord();
    }

    private static Map<String, Object> orderRecord(String orderId, int quantity) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", orderId);
        data.put("quantity", quantity);
        return data;
    }

    @Test
    @DisplayName("processRecord flattens the data and types it against the flattened schema")
    void processRecordAppliesSchemaTypes() {
        StreamingProcessorExample processor = new StreamingProcessorExample();

        Map<String, Object> typed = processor.processRecord(orderRecord("ORD-1", 7), eventSchema(), "v1");

        assertThat(typed)
                .as("a record-rooted schema yields unprefixed columns, typed per field")
                .containsOnlyKeys("orderId", "quantity")
                .containsEntry("orderId", "ORD-1")
                .containsEntry("quantity", 7);
    }

    @Test
    @DisplayName("a repeated schemaId reuses the cached flattened schema instead of re-flattening")
    void repeatedSchemaIdReusesTheCachedFlattenedSchema() {
        StreamingProcessorExample processor = new StreamingProcessorExample();

        processor.processRecord(orderRecord("ORD-1", 7), eventSchema(), "v1");

        // Same id, deliberately different schema. Caching by id means the decoy is never looked at.
        Map<String, Object> second = processor.processRecord(orderRecord("ORD-2", 9), decoySchema(), "v1");

        assertThat(second)
                .as("the schema cached under 'v1' is reused; the decoy schema is ignored")
                .containsOnlyKeys("orderId", "quantity")
                .containsEntry("orderId", "ORD-2")
                .containsEntry("quantity", 9);
    }

    @Test
    @DisplayName("processBatch types every record in the batch")
    void processBatchTypesEveryRecord() {
        CapturingProcessor processor = new CapturingProcessor();

        List<Map<String, Object>> batch = new ArrayList<>();
        batch.add(orderRecord("ORD-1", 1));
        batch.add(orderRecord("ORD-2", 2));
        batch.add(orderRecord("ORD-3", 3));

        processor.processBatch(batch, eventSchema(), "batch-v1");

        assertThat(processor.written)
                .as("every record in the batch reaches the sink")
                .hasSize(3);
        assertThat(processor.written)
                .allSatisfy(row -> assertThat(row).containsOnlyKeys("orderId", "quantity"));
        assertThat(processor.written)
                .extracting(row -> row.get("orderId"))
                .containsExactlyInAnyOrder("ORD-1", "ORD-2", "ORD-3");
        assertThat(processor.written)
                .extracting(row -> row.get("quantity"))
                .containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    @DisplayName("clearCaches() clears the shared flattener caches, NOT the processor's own schemaCache")
    void clearCachesDoesNotClearTheProcessorsOwnSchemaCache() {
        StreamingProcessorExample processor = new StreamingProcessorExample();

        processor.processRecord(orderRecord("ORD-1", 7), eventSchema(), "v1");
        processor.clearCaches();

        // If clearCaches() had emptied the processor's own map, this call would re-flatten and
        // the decoy's column would appear. It does not: clearCaches() only touches
        // GAvroSchemaFlattener's static caches, so the processor's per-schemaId map is unbounded
        // and never invalidated. Asserted because the method name suggests otherwise.
        Map<String, Object> afterClear =
                processor.processRecord(orderRecord("ORD-2", 9), decoySchema(), "v1");

        assertThat(afterClear)
                .as("the processor's own schemaCache survives clearCaches()")
                .containsOnlyKeys("orderId", "quantity");
    }

    /** Captures what the example would have written to Glue. */
    private static final class CapturingProcessor extends StreamingProcessorExample {
        private final Queue<Map<String, Object>> written = new ConcurrentLinkedQueue<>();

        @Override
        protected void writeToGlueTable(Map<String, Object> data) {
            written.add(data);
        }
    }
}
