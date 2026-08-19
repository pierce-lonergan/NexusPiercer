package io.github.pierce;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The traversal guard is enforced at the LIBRARY boundary, not merely at the FileFinder boundary.
 *
 * <p>{@code FileFinderSafetyOptionsTest} proves {@code FileFinder} refuses {@code ../..}. That
 * proof did not reach any caller. {@code AvroSchemaLoader.findAndLoadSchema} wrapped its
 * {@code FileFinder.findFile} call in {@code catch (Exception e)} and logged at DEBUG.
 * {@code SecurityException} is a {@code RuntimeException}, so the guard fired and was
 * DISCARDED - and control then fell to step 2, {@code loadFromLocalFileSystem}, which is
 * {@code Paths.get(basePath, schemaName)} plus {@code Files.readAllBytes} with no traversal check,
 * no extension check and no size cap, over a search-path list whose first entry is {@code "."}.</p>
 *
 * <p>STATED PRECISELY RATHER THAN OVERCLAIMED: {@code normalizeSchemaName} appends {@code .avsc}
 * to any name lacking it, so the reachable target set is narrowed to {@code .avsc}-suffixed files.
 * Narrowed, not closed - any attacker-plantable {@code .avsc} outside the tree was fully readable,
 * and the read was unbounded regardless of {@code maxFileSize}.</p>
 */
@DisplayName("AvroSchemaLoader does not swallow the traversal guard and read the file anyway")
class AvroSchemaLoaderSecurityBypassTest {

    private Path planted;

    @AfterEach
    void removePlantedFile() throws IOException {
        if (planted != null) {
            Files.deleteIfExists(planted);
            planted = null;
        }
        AvroSchemaLoader.clearCaches();
    }

    @Test
    @DisplayName("a traversal name is refused, not retried through the unvalidated fallback")
    void traversalIsNotSwallowedAndRetriedUnvalidated() throws IOException {
        // Plant a perfectly valid schema in the PARENT of the working directory - somewhere the
        // library has no business reading from.
        Path parent = Paths.get("..").toAbsolutePath().normalize();
        planted = parent.resolve("nexuspiercer_bypass_probe.avsc");
        Files.writeString(planted,
                "{\"type\":\"record\",\"name\":\"Planted\",\"fields\":"
                        + "[{\"name\":\"secret\",\"type\":\"string\"}]}",
                StandardCharsets.UTF_8);

        assertThat(Files.exists(planted))
                .as("the probe file must exist, or this test passes for the wrong reason")
                .isTrue();

        assertThatThrownBy(() -> AvroSchemaLoader.createDefault()
                .loadAvroSchema("../nexuspiercer_bypass_probe.avsc"))
                .as("the SecurityException FileFinder raises must reach the caller instead of "
                        + "being swallowed and the file read by the unvalidated fallback")
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Path traversal");
    }

    @Test
    @DisplayName("an ordinary schema name still loads")
    void goodInputControlAnOrdinarySchemaStillLoads() {
        // CAPABLE-OF-DISCRIMINATING LEG. Narrowing the catch must not turn every miss into a
        // failure: a name that simply is not there is still a miss, not a security error.
        assertThatCode(() -> {
            try {
                AvroSchemaLoader.createDefault().loadAvroSchema("user_schema.avsc");
            } catch (IOException expectedIfAbsent) {
                // A not-found is fine; a SecurityException here would mean the narrowing was
                // done with too broad a brush.
                assertThat(expectedIfAbsent).isNotInstanceOf(SecurityException.class);
            }
        }).doesNotThrowAnyException();
    }
}
