package io.github.pierce.docs;

// SNIPPET-BEGIN IMPORTS converter
import io.github.pierce.converter.AvroSchemaConverter;
import io.github.pierce.converter.ConversionConfig;
import io.github.pierce.converter.ConversionConfig.ErrorHandlingMode;
import io.github.pierce.converter.GenericRecord;
import io.github.pierce.converter.IcebergSchemaConverter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Map;
// SNIPPET-END IMPORTS converter

/**
 * Template for {@code env=converter}: the Iceberg and Avro record converters.
 *
 * <p>A separate file from {@link SnippetEnvCore} for a reason worth writing down: {@code core}
 * imports {@code org.apache.avro.Schema} and this one imports {@code org.apache.iceberg.Schema},
 * and a single compilation unit cannot import both. One template file per environment is what
 * lets each declare the imports its own documents actually use, instead of a shared preamble
 * that silently decides which {@code Schema} a published snippet meant.</p>
 *
 * <p>VARIABLES AND IMPORTS ONLY. See {@link SnippetEnvironments}.</p>
 */
@SuppressWarnings({"unused", "PMD"})
final class SnippetEnvConverter {

    private SnippetEnvConverter() {
    }

    static void locals() throws Exception {
        // SNIPPET-BEGIN LOCALS converter
        // No predeclared locals. Every published converter snippet builds its own schema,
        // its own converter and its own input map, so a shared local here would collide
        // with the document rather than support it - and "variable already defined" is a
        // confusing way for a gate to reject a correct example.
        // SNIPPET-END LOCALS converter
        touch();
    }

    private static void touch(Object... ignored) {
        // See SnippetEnvCore.touch.
    }
}
