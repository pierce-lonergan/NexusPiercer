package io.github.pierce.files;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code FileFinder} safety options are now enforced.
 *
 * <p>{@code validatePaths}, {@code allowedExtensions} and {@code maxFileSize} were settable and
 * never read anywhere in the class. That is worse than not offering them: a caller reads the
 * builder, concludes traversal is blocked, and passes operator-supplied names straight through.
 * The default search paths include {@code ..}, {@code ../..} and {@code ../../..}, so a relative
 * escape was an arbitrary local-file read.</p>
 *
 * <p>What these tests catch: a regression that makes any of the three inert again. What they
 * cannot catch: whether the allow-list is the right set for a given deployment — that is a
 * caller's policy decision, and the list is configurable for exactly that reason.</p>
 */
@DisplayName("FileFinder safety options")
class FileFinderSafetyOptionsTest {

    @TempDir
    Path tempDir;

    @BeforeEach
    @AfterEach
    void clearCaches() {
        FileFinder.clearCaches();
    }

    @Test
    @DisplayName("path traversal is rejected before any filesystem access")
    void traversalRejected() {
        assertThatThrownBy(() -> FileFinder.findFile("../../../etc/passwd"))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Path traversal");
    }

    @Test
    @DisplayName("a traversal segment anywhere in the path is rejected, not just at the start")
    void traversalRejectedMidPath() {
        assertThatThrownBy(() -> FileFinder.findFile("schemas/../../secrets.avsc"))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Path traversal");
    }

    @Test
    @DisplayName("Windows-style backslash traversal is rejected too")
    void backslashTraversalRejected() {
        assertThatThrownBy(() -> FileFinder.findFile("..\\..\\windows\\system32\\config"))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Path traversal");
    }

    /**
     * Some native layers truncate at NUL, so a name can pass an extension check and then open
     * something else. Rejected on the raw name, before normalisation.
     */
    @Test
    @DisplayName("a null byte in the name is rejected")
    void nullByteRejected() {
        assertThatThrownBy(() -> FileFinder.findFile("safe.avsc" + (char) 0 + "evil"))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Null byte");
    }

    /**
     * IOException rather than SecurityException, deliberately: FileFinderException extends
     * FileNotFoundException, so every existing caller already handles IOException from this call.
     * The pre-existing test asserting ".exe files should be blocked by default" passed only
     * because the file did not exist — the check it named did nothing.
     */
    @Test
    @DisplayName("a disallowed extension is rejected, in the IOException family")
    void disallowedExtensionRejected() {
        assertThatThrownBy(() -> FileFinder.findFile("payload.exe"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not in the allowed set");
    }

    /**
     * The guard must not break ordinary use. A permitted extension gets past validation and fails
     * — if at all — for the honest reason that the file is absent.
     */
    @Test
    @DisplayName("an allowed extension passes validation and resolves normally")
    void allowedExtensionPasses() throws IOException {
        Path schema = tempDir.resolve("product_schema.avsc");
        Files.writeString(schema, "{\"type\":\"record\",\"name\":\"P\",\"fields\":[]}");

        try (InputStream is = FileFinder.findFile(schema.toString())) {
            assertThat(is).isNotNull();
        }
    }

    @Test
    @DisplayName("a missing but well-formed name still fails as not-found, not as a security error")
    void missingFileIsNotASecurityError() {
        assertThatThrownBy(() -> FileFinder.findFile("definitely_absent_schema.avsc"))
                .isNotInstanceOf(SecurityException.class);
    }

    /**
     * An extensionless name is allowed on purpose: directory-ish and well-known config names are
     * legitimate, and rejecting them would break callers for no security gain.
     */
    @Test
    @DisplayName("an extensionless name is not rejected by the extension check")
    void extensionlessNameAllowed() {
        assertThatThrownBy(() -> FileFinder.findFile("Dockerfile"))
                .isNotInstanceOf(SecurityException.class);
    }

    @Test
    @DisplayName("a file under maxFileSize resolves normally")
    void underSizedFileResolves() throws IOException {
        // THIS TEST WAS CALLED oversizedFileRejected AND ASSERTED NO REJECTION. Its body wrote a
        // 2-byte file, asserted the open succeeded, and asserted 2 < 100 MB - so it could not
        // fail, under a name that made the suite look as though maxFileSize were covered while
        // nothing covered it. Renamed to what it actually measures. The rejection it used to
        // promise is now measured for real, against the RESOLVED file and against the bytes
        // read, in FileFinderResolvedSizeGateTest.
        Path small = tempDir.resolve("small.avsc");
        Files.writeString(small, "{}");

        assertThatCode(() -> FileFinder.findFile(small.toString()).close())
                .doesNotThrowAnyException();
        assertThat(Files.size(small)).isLessThan(100L * 1024 * 1024);
    }

    @Test
    @DisplayName("getFileMetadata rejects a traversal name instead of searching for it")
    void getFileMetadataRejectsTraversal() {
        // getFileMetadata called fileCache.get(fileName) DIRECTLY, so it never invoked
        // enforceSafetyOptions. It ran the full search on a traversal name and reported the
        // outcome as a plain not-found. The class javadoc claimed getInputStream was "the single
        // choke point every public accessor funnels through"; this test is what refutes it.
        assertThatThrownBy(() -> FileFinder.getFileMetadata("../../../etc/passwd"))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Path traversal");
    }

    @Test
    @DisplayName("fileExists does not perform a search on a traversal name")
    void fileExistsDoesNotSearchOnATraversalName() {
        // Asserting only the RETURN VALUE would be a test that can only pass: fileExists returns
        // false for this name before the fix too, because the file is not found. The observable
        // that actually changes is whether the search ran at all.
        long before = FileFinder.getStatistics().searchAttempts;

        assertThat(FileFinder.fileExists("../../../etc/passwd")).isFalse();

        assertThat(FileFinder.getStatistics().searchAttempts)
                .as("a traversal name must be refused before findFileHandle increments the "
                        + "search counter - otherwise the full 28-path, 6-classpath-probe, "
                        + "depth-5 walk ran on attacker-controlled input")
                .isEqualTo(before);
    }

    @Test
    @DisplayName("fileExists still searches, and still answers, for a legitimate name")
    void goodInputControlFileExistsStillWorks() throws IOException {
        // CAPABLE-OF-DISCRIMINATING LEG for the test above: routing fileExists through the
        // validator must not stop it doing its job.
        Path real = tempDir.resolve("present.avsc");
        Files.writeString(real, "{}");

        long before = FileFinder.getStatistics().searchAttempts;
        assertThat(FileFinder.fileExists(real.toString())).isTrue();
        assertThat(FileFinder.getStatistics().searchAttempts)
                .as("a legitimate name must still reach the search")
                .isGreaterThan(before);
    }
}
