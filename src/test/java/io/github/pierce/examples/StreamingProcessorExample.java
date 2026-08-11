package io.github.pierce.examples;

import io.github.pierce.GAvroSchemaFlattener;
import io.github.pierce.MapFlattener;
import org.apache.avro.Schema;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Example of using AvroSchemaFlattener in a streaming application.
 *
 * <p>Ported from {@code src/test/groovy/StreamingProcessorExample.groovy}. The Groovy source
 * relied on Groovy's implicit {@code java.util.*} import for {@code Map} and {@code List};
 * those are explicit here. {@code writeToGlueTable} was {@code private} and is now
 * {@code protected} so a test can observe what a batch actually produced — without that, the
 * class is unobservable and cannot be asserted on, which is why it never was.</p>
 *
 * <p>Exercised by {@link StreamingProcessorExampleTest}. It is deliberately not named
 * {@code *Test} itself: it is the example, not the test.</p>
 */
public class StreamingProcessorExample {

    // Schema cache (shared across all records)
    private final Map<String, Map<String, GAvroSchemaFlattener.FlattenedFieldType>> schemaCache =
            new ConcurrentHashMap<>();

    private final MapFlattener dataFlattener;
    private final GAvroSchemaFlattener avroFlattener;

    public StreamingProcessorExample() {
        // Initialize once
        this.dataFlattener = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.JSON)
                .maxDepth(20)
                .build();

        this.avroFlattener = new GAvroSchemaFlattener(
                GAvroSchemaFlattener.AvroFlatteningConfig.builder()
                        .strictTypeEnforcement(true)
                        .build());
    }

    /**
     * Process a single record (called for each message in stream)
     */
    public Map<String, Object> processRecord(Map<String, Object> jsonData,
                                             Schema avroSchema,
                                             String schemaId) {
        // 1. Get or compute flattened schema (cached)
        Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                schemaCache.computeIfAbsent(schemaId, id -> {
                    System.out.println("Flattening schema for: " + schemaId);
                    return avroFlattener.flattenSchema(avroSchema);
                });

        // 2. Flatten the data
        Map<String, Object> flattenedData = dataFlattener.flatten(jsonData);

        // 3. Apply correct types based on schema
        Map<String, Object> typedData = avroFlattener.applyTypes(flattenedData, flattenedSchema);

        return typedData;
    }

    /**
     * Batch processing example
     */
    public void processBatch(List<Map<String, Object>> records,
                             Schema schema,
                             String schemaId) {
        // Flatten schema once for entire batch
        Map<String, GAvroSchemaFlattener.FlattenedFieldType> flattenedSchema =
                schemaCache.computeIfAbsent(schemaId,
                        id -> avroFlattener.flattenSchema(schema));

        // Process each record
        records.parallelStream().forEach(record -> {
            Map<String, Object> flattenedData = dataFlattener.flatten(record);
            Map<String, Object> typedData = avroFlattener.applyTypes(flattenedData, flattenedSchema);

            // Write to Glue table, etc.
            writeToGlueTable(typedData);
        });
    }

    protected void writeToGlueTable(Map<String, Object> data) {
        // Your Glue writing logic here
        System.out.println("Writing to Glue: " + data);
    }

    /**
     * Clear caches periodically in long-running applications
     */
    public void clearCaches() {
        GAvroSchemaFlattener.clearCaches();
    }
}
