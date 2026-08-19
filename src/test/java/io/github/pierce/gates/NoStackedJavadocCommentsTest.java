package io.github.pierce.gates;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two javadoc comments in a row SILENTLY DELETE the first one from the published documentation.
 *
 * <p>Javac attaches only the LAST doc comment before a declaration. A second {@code /**} opened
 * on the line after a closing {@code *}{@code /} therefore discards everything above it, with no
 * warning from the compiler, no warning from the javadoc tool, and no visible difference in the
 * source — the text is still right there in the file, it just is not in the output.</p>
 *
 * <p>MEASURED, which is why this gate exists. Adding the 2.1.0 {@code @deprecated} notices to
 * {@code FileFinder} as a second stacked comment removed five descriptions from
 * {@code target/apidocs}: "Discover files with specific extension", "Discover all Avro schema
 * files", "Clear all caches", "Get detailed statistics" and "Utility methods for common
 * operations" each returned 0 hits in the generated HTML, while un-stacked neighbours such as
 * "Get file metadata" returned 2. Worse, inserting {@code ArrayParseException} above
 * {@code ReconstructionException} in {@code JsonReconstructor} stranded the latter's only
 * sentence, so a public exception type shipped with no documentation at all.</p>
 *
 * <p>The fix is always the same and takes one edit: merge the two comments, description first,
 * block tags after.</p>
 */
@DisplayName("no source file stacks one javadoc comment on top of another")
class NoStackedJavadocCommentsTest {

    private static final Path MAIN = Paths.get("src", "main", "java");

    @Test
    @DisplayName("a closing */ is never immediately followed by an opening /**")
    void noStackedComments() throws IOException {
        List<String> offenders = new ArrayList<>();

        try (Stream<Path> files = Files.walk(MAIN)) {
            for (Path file : (Iterable<Path>) files
                    .filter(p -> p.getFileName().toString().endsWith(".java"))::iterator) {
                String[] lines = new String(Files.readAllBytes(file), StandardCharsets.UTF_8)
                        .split("\r?\n");
                for (int i = 0; i + 1 < lines.length; i++) {
                    if ("*/".equals(lines[i].trim()) && lines[i + 1].trim().startsWith("/**")) {
                        offenders.add(file + ":" + (i + 1)
                                + "  (the comment ending here is discarded by javac)");
                    }
                }
            }
        }

        assertTrue(offenders.isEmpty(),
                "stacked javadoc comments found — the FIRST of each pair is silently dropped "
                        + "from the published javadoc:\n  " + String.join("\n  ", offenders)
                        + "\nMerge each pair into one comment: description first, @deprecated or "
                        + "other block tags after it.");
    }
}
