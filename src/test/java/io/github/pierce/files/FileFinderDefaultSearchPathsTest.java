package io.github.pierce.files;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The default search paths do not leave the working tree.
 *
 * <p>{@code Config.searchPaths} listed {@code ".."}, {@code "../.."} and {@code "../../.."}.
 * {@code performDiscovery} applies {@code Files.walk(path, 2)} to every search path and is invoked
 * by {@code createNotFoundException} on EVERY miss, and its results go into
 * {@code FileFinderException}'s MESSAGE. Measured from this repo root before the change, a
 * {@code .json} miss walked 67 files across sibling checkouts in {@code ..} and 24 in
 * {@code ../../..}, which is the user's home directory. Those paths were then embedded verbatim in
 * an exception that {@code AvroSchemaFlattener} rewraps and Spark logs. On an executor the same
 * walk enters sibling containers' scratch space.</p>
 *
 * <p>The assertions here are STRUCTURAL - "no configured path escapes the working tree" - and
 * never count files. A count would depend on whatever happens to sit beside the checkout, which
 * makes a test that passes on CI and means nothing.</p>
 */
@DisplayName("FileFinder's default search paths stay inside the working tree")
class FileFinderDefaultSearchPathsTest {

    @BeforeEach
    @AfterEach
    void clearCaches() {
        FileFinder.clearCaches();
    }

    @Test
    @DisplayName("no default search path is a parent directory, and none is duplicated")
    void defaultsDoNotLeaveTheWorkingTree() {
        List<String> paths = new FileFinder.Config().getAllSearchPaths();

        assertThat(paths)
                .as("a parent-directory search path makes every miss walk outside the project")
                .doesNotContain("..", "../..", "../../..");

        Path root = Paths.get(".").toAbsolutePath().normalize();
        for (String p : paths) {
            Path resolved = Paths.get(p).toAbsolutePath().normalize();
            // Path.startsWith, not AssertJ's - AssertJ's variant calls toRealPath(), which throws
            // NoSuchFileException for a configured directory that does not happen to exist in this
            // checkout. Whether the directory exists is not what is being asserted.
            assertThat(resolved.startsWith(root))
                    .as("search path '%s' resolves to %s, which is outside %s", p, resolved, root)
                    .isTrue();
        }

        assertThat(paths)
                .as("a duplicated search path costs a redundant Files.exists on every lookup")
                .doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("the not-found message names no path outside the tree")
    void notFoundMessageDoesNotNamePathsOutsideTheTree() {
        assertThatThrownBy(() -> FileFinder.findFile("definitely_absent_schema_xyzzy.avsc"))
                .isInstanceOf(java.io.IOException.class)
                .satisfies(t -> {
                    String message = String.valueOf(t.getMessage());
                    assertThat(message)
                            .as("the not-found message lists discovered files; a parent-directory "
                                    + "walk puts paths from outside the checkout into it")
                            .doesNotContain("..\\")
                            .doesNotContain("../");
                });
    }

    @Test
    @DisplayName("the traversal refusal does not instruct the caller to do something impossible")
    void theTraversalMessageDoesNotPromiseAnUnreachableKnob() {
        assertThatThrownBy(() -> FileFinder.findFile("../../../etc/passwd"))
                .isInstanceOf(SecurityException.class)
                .satisfies(t -> {
                    String message = String.valueOf(t.getMessage());
                    assertThat(message).contains("Path traversal");
                    // Config has no setters and no injection point - getInstance() hard-codes
                    // new FileFinder(new Config()) - so "disable validatePaths" was a remediation
                    // instruction no caller could follow, printed inside a security error.
                    assertThat(message)
                            .as("the message must not tell the caller to turn off a knob that "
                                    + "cannot be reached")
                            .doesNotContain("disable validatePaths");
                    assertThat(message)
                            .as("parent directories are no longer searched, so the message must "
                                    + "not still claim they are")
                            .doesNotContain("searches parent");
                });
    }
}
