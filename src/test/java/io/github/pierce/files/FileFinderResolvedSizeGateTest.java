package io.github.pierce.files;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code maxFileSize} is enforced against the RESOLVED file, and against the bytes actually read.
 *
 * <p>The gate used to live in {@code enforceSafetyOptions}, where it called
 * {@code Paths.get(fileName)} with NO base path while resolution went through
 * {@code config.getAllSearchPaths()}. It therefore gated only names that happened to resolve as a
 * regular file relative to the CWD. A bare name resolved from a search path, from the classpath,
 * from HDFS or from the depth-5 deep search was never size-checked at all. The block's own comment
 * said so.</p>
 *
 * <p>TWO HOLES A HANDLE-SIZE-ONLY GATE WOULD LEAVE, and the reason two of these tests exist
 * separately from the first:</p>
 * <ul>
 *   <li>a classpath handle carries {@code URLConnection.getContentLengthLong()}, which is
 *       <b>-1</b> when the length is unknown. A gate reading {@code size > max} passes -1
 *       silently, reproducing the original defect in the exact strategy this work is about.</li>
 *   <li>{@code .gz} is an allowed extension and {@code openInputStream} inflates it transparently.
 *       The handle's size is the COMPRESSED size, so a small archive passes a large cap and the
 *       caller receives however many bytes the archive decides to produce.</li>
 * </ul>
 *
 * <p>So the cap is applied twice on purpose: once against a known resolved size before anything is
 * opened, and once as a hard byte count on the OUTERMOST stream. Neither is redundant - the second
 * also covers a handle whose cached size went stale, since handles live for 60 minutes.</p>
 */
@DisplayName("maxFileSize is enforced at resolution and on the bytes read")
class FileFinderResolvedSizeGateTest {

    private long original;

    @BeforeEach
    void lowerTheCap() {
        FileFinder.clearCaches();
        original = FileFinder.maxFileSizeForTesting();
        FileFinder.maxFileSizeForTesting(64L);
    }

    @AfterEach
    void restoreTheCap() {
        FileFinder.maxFileSizeForTesting(original);
        FileFinder.clearCaches();
    }

    /** A default search path that is NOT the CWD, so the old gate could not have seen it. */
    private static final Path SEARCH_PATH = Paths.get("src", "test", "resources", "schemas");

    @Test
    @DisplayName("an oversized file resolved through a search path is rejected")
    void oversizedFileResolvedViaSearchPathIsRejected() throws IOException {
        Files.createDirectories(SEARCH_PATH);
        Path big = SEARCH_PATH.resolve("finder_size_gate_probe.avsc");
        Files.writeString(big, "x".repeat(4096), StandardCharsets.UTF_8);
        try {
            // A BARE NAME. The old gate did Paths.get("finder_size_gate_probe.avsc") relative to
            // the CWD, found no regular file there, and skipped the whole block - while
            // searchLocalPaths resolved it happily from the search path and returned a stream.
            assertThatThrownBy(() -> FileFinder.findFile("finder_size_gate_probe.avsc").close())
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("4096")
                    .hasMessageContaining("64");
        } finally {
            Files.deleteIfExists(big);
        }
    }

    @Test
    @DisplayName("a file within the cap resolved the same way still opens")
    void goodInputControlAFileUnderTheCapStillResolves() throws IOException {
        Files.createDirectories(SEARCH_PATH);
        Path small = SEARCH_PATH.resolve("finder_size_gate_small.avsc");
        Files.writeString(small, "{}", StandardCharsets.UTF_8);
        try {
            // CAPABLE-OF-DISCRIMINATING LEG. Without it the assertions above would also pass
            // against an implementation that rejected everything, which would be a far worse
            // regression than the one being fixed.
            assertThatCode(() -> {
                try (InputStream is = FileFinder.findFile("finder_size_gate_small.avsc")) {
                    assertThat(is.readAllBytes()).hasSize(2);
                }
            }).doesNotThrowAnyException();
        } finally {
            Files.deleteIfExists(small);
        }
    }

    @Test
    @DisplayName("a gzip whose INFLATED size exceeds the cap is refused on decompressed bytes")
    void gzipBombIsCappedOnDecompressedBytes() throws IOException {
        // This leg needs a cap the ARCHIVE fits under and the INFLATED content does not, so it
        // sets its own rather than using the class-wide 64 bytes.
        final long cap = 64L * 1024;
        FileFinder.maxFileSizeForTesting(cap);
        FileFinder.clearCaches();

        Files.createDirectories(SEARCH_PATH);
        Path gz = SEARCH_PATH.resolve("finder_size_gate_bomb.gz");

        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        try (GZIPOutputStream out = new GZIPOutputStream(raw)) {
            // Highly compressible: 4 MB of one byte squeezes into a few kilobytes, so the
            // COMPRESSED size sails under the cap while the inflated size does not.
            out.write("A".repeat(4_000_000).getBytes(StandardCharsets.UTF_8));
        }
        Files.write(gz, raw.toByteArray());
        try {
            assertThat(Files.size(gz))
                    .as("the archive itself must be under the cap, or this test proves nothing")
                    .isLessThan(cap);

            assertThatThrownBy(() -> FileFinder.getFileContent("finder_size_gate_bomb.gz"))
                    .as("handle.size is the COMPRESSED size, so a gate that only compares it "
                            + "against the cap lets the whole archive inflate")
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(String.valueOf(cap));
        } finally {
            Files.deleteIfExists(gz);
        }
    }
}
