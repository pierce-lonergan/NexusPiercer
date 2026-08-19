package io.github.pierce.files;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Resolving a file through the CLASSPATH must not leave an OS handle open on it.
 *
 * <p>{@code createClasspathHandle} calls {@code resource.openConnection()} and then reads
 * {@code getLastModified()} and {@code getContentLengthLong()}. Both force {@code connect()},
 * which opens an underlying stream. {@code URLConnection} has no {@code close()}, so unless the
 * stream it opened is taken and closed, the descriptor stays open — on the HIT path, for every
 * classpath resolution, whether or not the caller ever asks for a stream.</p>
 *
 * <p>WHY THAT MATTERED MORE THAN IT SOUNDS. Handles are cached for 60 minutes. On Windows the
 * resolved file could not be deleted or replaced for that long; under a jar the leaked connection
 * pinned an {@code Inflater} for the same period. The 2.1.0 pass fixed the MISS-path leak in
 * {@code searchClasspath} (six probes per miss) and described the classpath path as handled; the
 * hit-path leak was larger and untouched. Found by adversarial review, reproduced here.</p>
 *
 * <p>TWO ASSERTIONS, ONE PER PLATFORM, because either one alone is a test that only ever passes
 * somewhere. On Windows a leaked handle makes {@code Files.delete} throw, so the delete IS the
 * gate. On Linux an open descriptor does not block a delete, so the gate is the descriptor count
 * under {@code /proc/self/fd} across repeated resolutions. A control leg — deleting an untouched
 * copy — rules out the file being locked for any unrelated reason.</p>
 */
@DisplayName("a classpath resolution releases the file handle it opened")
class ClasspathHandleReleasesTheFileTest {

    /** {@code target/test-classes} is on the test classpath, so anything written here resolves. */
    private static final Path CLASSPATH_DIR = Paths.get("target", "test-classes");

    private static final String BODY =
            "{\"type\":\"record\",\"name\":\"HandleProbe\",\"fields\":[]}";

    private static Path writeProbe(String name) throws IOException {
        Files.createDirectories(CLASSPATH_DIR);
        Path p = CLASSPATH_DIR.resolve(name);
        Files.write(p, BODY.getBytes(StandardCharsets.UTF_8));
        return p;
    }

    private static long openDescriptors() {
        Path fd = Paths.get("/proc/self/fd");
        if (!Files.isDirectory(fd)) {
            return -1;
        }
        try (Stream<Path> s = Files.list(fd)) {
            return s.count();
        } catch (IOException unreadable) {
            return -1;
        }
    }

    @Test
    @DisplayName("the resolved file can still be deleted, and descriptors do not accumulate")
    void resolvingViaClasspathDoesNotPinTheFile() throws IOException {
        Path control = writeProbe("handle_probe_control.avsc");
        Path probe = writeProbe("handle_probe_leak.avsc");
        FileFinder.clearCaches();

        // CONTROL: nothing has touched this one. If the delete below fails, the test is telling
        // us about the environment, not about FileFinder.
        assertDoesNotThrow(() -> Files.delete(control),
                "control copy could not be deleted; something other than FileFinder holds it");

        FileFinder.FileMetadata meta =
                assertDoesNotThrow(() -> FileFinder.getFileMetadata("handle_probe_leak.avsc"));
        assertEquals(FileFinder.FileLocation.Type.CLASSPATH, meta.location.type,
                "the probe must resolve through the classpath strategy for this test to mean "
                        + "anything; it resolved as " + meta.location.type);

        // WINDOWS GATE. A leaked URLConnection stream makes this throw
        // "The process cannot access the file because it is being used by another process".
        assertDoesNotThrow(() -> Files.delete(probe),
                "the file resolved through the classpath is still held open — "
                        + "createClasspathHandle leaked the stream URLConnection.connect() opened");

        // POSIX GATE. Deleting proves nothing there, so count descriptors across repeats instead.
        long before = openDescriptors();
        if (before < 0) {
            return;
        }
        Path repeat = writeProbe("handle_probe_fd.avsc");
        try {
            for (int i = 0; i < 40; i++) {
                FileFinder.clearCaches();
                FileFinder.getFileMetadata("handle_probe_fd.avsc");
            }
            long after = openDescriptors();
            assertTrue(after - before < 20,
                    "open descriptors grew from " + before + " to " + after
                            + " across 40 classpath resolutions; createClasspathHandle is "
                            + "leaking the stream URLConnection.connect() opened");
        } finally {
            FileFinder.clearCaches();
            Files.deleteIfExists(repeat);
        }
    }
}
