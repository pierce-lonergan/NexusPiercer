package io.github.pierce.docs;

// SNIPPET-BEGIN IMPORTS core
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pierce.AvroReconstructor;
import io.github.pierce.AvroSchemaFlattener;
import io.github.pierce.GAvroSchemaFlattener;
import io.github.pierce.JsonFlattener;
import io.github.pierce.JsonFlattenerConsolidator;
import io.github.pierce.JsonReconstructor;
import io.github.pierce.MapFlattener;
import io.github.pierce.schema.EnrichedSchemaFlattener;
import io.github.pierce.schema.FlattenOptions;
import io.github.pierce.schema.FlattenedField;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
// SNIPPET-END IMPORTS core

/**
 * Template for {@code env=core}: the data-flattening and reconstruction entry points.
 *
 * <p>VARIABLES AND IMPORTS ONLY. See {@link SnippetEnvironments} for why that rule is
 * load-bearing and where it is drilled.</p>
 */
@SuppressWarnings({"unused", "PMD"})
final class SnippetEnvCore {

    private SnippetEnvCore() {
    }

    static void locals() throws Exception {
        // SNIPPET-BEGIN LOCALS core
        String json = "{\"k\":1}";
        String jsonString = "{\"k\":1}";
        String complexJson = "{\"k\":1}";
        String docA = "{\"k\":1}";
        Map<String, Object> sourceMap = Map.of("k", 1);
        Map<String, Object> datum = Map.of("k", 1);
        Map<String, Object> flatInput = Map.of("k", 1);
        Schema schema = Schema.create(Schema.Type.STRING);
        ObjectMapper mapper = new ObjectMapper();
        // SNIPPET-END LOCALS core
        touch(json, jsonString, complexJson, docA, sourceMap, datum, flatInput, schema, mapper);
    }

    private static void touch(Object... ignored) {
        // Reads every local so javac cannot report one as definitely unused, and so a local
        // whose type stops resolving fails this file rather than silently leaving the gate's
        // wrapper short of a variable a published snippet depends on.
    }
}
