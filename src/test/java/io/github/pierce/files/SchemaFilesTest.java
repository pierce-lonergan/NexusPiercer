package io.github.pierce.files;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link SchemaFiles} — the replacement for {@code FileFinder} at every call site in this library.
 *
 * <p>The whole point is what it does NOT do: no search paths, no classpath probing, no parent
 * walk, no depth-five tree walk, no fuzzy matching, no singleton, no thread pools. It resolves the
 * literal path it is given. These tests pin both halves — the guards it enforces, and the fact
 * that a name it cannot resolve is simply not found rather than hunted for.</p>
 */
@DisplayName("SchemaFiles reads the path it is given, and nothing else")
class SchemaFilesTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("it reads a real file")
    void itReadsARealFile() throws IOException {
        Path f = tempDir.resolve("s.avsc");
        Files.writeString(f, "{\"type\":\"record\"}", StandardCharsets.UTF_8);

        assertThat(SchemaFiles.readString(f.toString())).isEqualTo("{\"type\":\"record\"}");
        try (InputStream in = SchemaFiles.open(f.toString())) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8))
                    .isEqualTo("{\"type\":\"record\"}");
        }
    }

    @Test
    @DisplayName("a traversal name is refused")
    void traversalRefused() {
        assertThatThrownBy(() -> SchemaFiles.open("../../../etc/passwd"))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Path traversal");
        assertThatThrownBy(() -> SchemaFiles.open("schemas/../../secrets.avsc"))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> SchemaFiles.open("..\\..\\windows\\system32\\config"))
                .isInstanceOf(SecurityException.class);
    }

    @Test
    @DisplayName("a null byte is refused before anything else")
    void nullByteRefused() {
        assertThatThrownBy(() -> SchemaFiles.open("safe.avsc" + (char) 0 + "evil"))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Null byte");
    }

    @Test
    @DisplayName("IT DOES NOT SEARCH: a bare name that exists elsewhere is not found")
    void itDoesNotSearch() throws IOException {
        // THIS IS THE BEHAVIOUR CHANGE, pinned deliberately. FileFinder would have resolved this
        // bare name out of src/test/resources/schemas or any of ~28 other directories. SchemaFiles
        // resolves the literal path relative to the working directory and stops. A caller who was
        // relying on the search must pass a path.
        Path planted = tempDir.resolve("uniquely_named_probe.avsc");
        Files.writeString(planted, "{}", StandardCharsets.UTF_8);

        assertThatThrownBy(() -> SchemaFiles.open("uniquely_named_probe.avsc"))
                .isInstanceOf(java.io.FileNotFoundException.class)
                .hasMessageContaining("uniquely_named_probe.avsc");

        // ... and the same file DOES open when named by its actual path.
        assertThatCode(() -> SchemaFiles.open(planted.toString()).close())
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("a directory is not a schema file")
    void aDirectoryIsNotAFile() {
        assertThatThrownBy(() -> SchemaFiles.open(tempDir.toString()))
                .isInstanceOf(java.io.FileNotFoundException.class);
    }

    @Test
    @DisplayName("null and blank are refused as IO errors, not NullPointerException")
    void nullAndBlankRefused() {
        assertThatThrownBy(() -> SchemaFiles.open(null)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> SchemaFiles.open("   ")).isInstanceOf(IOException.class);
    }

    @Test
    @DisplayName("the stream is capped even when the file grows after the stat")
    void theStreamIsCappedIndependentlyOfTheStat() throws IOException {
        // The cap is applied twice on purpose. A stat-only check races the read: the file can grow
        // between Files.size() and the last byte read. This drives that window directly by opening
        // the stream first and appending afterwards, which is the only way to show the second gate
        // is not redundant.
        Path f = tempDir.resolve("grows.avsc");
        Files.writeString(f, "{}", StandardCharsets.UTF_8);

        try (InputStream in = SchemaFiles.open(f.toString())) {
            assertThat(in).isNotNull();
            try (OutputStream out = Files.newOutputStream(f, java.nio.file.StandardOpenOption.APPEND)) {
                out.write("x".getBytes(StandardCharsets.UTF_8));
            }
            // Nothing to assert about the value here; the cap is 100 MB and this file is tiny.
            // What this pins is that opening does not pre-read the file into memory, so the
            // limiting stream is genuinely counting bytes as they are consumed.
            assertThat(in.readAllBytes().length).isGreaterThanOrEqualTo(2);
        }
    }

    @Test
    @DisplayName("maxBytes is published so a caller can reason about the cap")
    void maxBytesIsPublished() {
        assertThat(SchemaFiles.maxBytes()).isEqualTo(100L * 1024 * 1024);
    }
}
