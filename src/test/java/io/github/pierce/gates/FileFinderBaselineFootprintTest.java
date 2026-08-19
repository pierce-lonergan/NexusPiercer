package io.github.pierce.gates;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * How much of the released 2.0.0 API surface {@code FileFinder} occupies, asserted rather than
 * remembered.
 *
 * <p>The figure is load-bearing: it is the whole argument in BL-017 that deprecating
 * {@code FileFinder} in 2.1.0 cannot become a removal before 3.0.0, and it is quoted in three
 * published places — the class javadoc, the changelog and the backlog. It was published as "34
 * members". MEASURED against the file it cites, {@code src/test/resources/api/public-api-2.0.0.txt},
 * the count is 64, and no subsetting of the baseline produces 34: not 64, not 56 (non-TYPE
 * entries), not 9 (declared directly on {@code FileFinder} rather than on a nested type). The
 * conclusion the number supports was right and in fact understated; only the number was
 * wrong.</p>
 *
 * <p>Project doctrine is to verify the count rather than the claim. This is that verification,
 * so the published figure cannot drift from the file it cites again.</p>
 */
@DisplayName("the FileFinder footprint quoted in the docs matches the 2.0.0 baseline file")
class FileFinderBaselineFootprintTest {

    private static final Path BASELINE =
            Paths.get("src", "test", "resources", "api", "public-api-2.0.0.txt");

    private static final String PREFIX = "io.github.pierce.files.FileFinder";

    private static List<String> baselineLines() throws IOException {
        return Files.readAllLines(BASELINE, StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("64 baseline entries name FileFinder or one of its nested types")
    void totalFootprintIs64() throws IOException {
        long naming = baselineLines().stream().filter(l -> l.contains(PREFIX)).count();

        assertEquals(64, naming,
                "the FileFinder footprint in the 2.0.0 baseline changed. The figure is quoted in "
                        + "FileFinder's class javadoc, CHANGELOG.md and docs/BACKLOG.md (BL-017); "
                        + "update all three, not this test.");
    }

    @Test
    @DisplayName("the published breakdown by kind matches: 8 TYPE, 4 CTOR, 23 FIELD, 29 METH")
    void breakdownByKindMatches() throws IOException {
        Map<String, Integer> byKind = new LinkedHashMap<>();
        for (String line : baselineLines()) {
            if (!line.contains(PREFIX)) {
                continue;
            }
            String kind = line.trim().split("\\s+")[0];
            byKind.merge(kind, 1, Integer::sum);
        }

        assertEquals(8, byKind.getOrDefault("TYPE", 0), "TYPE entries naming FileFinder");
        assertEquals(4, byKind.getOrDefault("CTOR", 0), "CTOR entries naming FileFinder");
        assertEquals(23, byKind.getOrDefault("FIELD", 0), "FIELD entries naming FileFinder");
        assertEquals(29, byKind.getOrDefault("METH", 0), "METH entries naming FileFinder");
    }

    @Test
    @DisplayName("9 of them are declared directly on FileFinder itself, not on a nested type")
    void nineAreDeclaredOnFileFinderItself() throws IOException {
        // The nested types carry the other 55: FileLocation.Type, Util, Statistics, FileMetadata,
        // FileLocation, FileFinderException and Config. Removing FileFinder means removing all
        // eight types, which is why this cannot be done additively.
        //
        // The 2.1.0 review that corrected the published "34 members" reported this sub-figure as
        // 12 (1 TYPE + 11 METH). MEASURED it is 9: 1 TYPE plus the eight static methods
        // findFile, getFileContent, getFileMetadata, fileExists, discoverFiles,
        // discoverAvroSchemas, clearCaches and getStatistics.
        Pattern declaredHere = Pattern.compile(
                "(^|\\s)" + Pattern.quote(PREFIX) + "\\.[a-z][A-Za-z0-9_]*(\\(|$|\\s)");

        long direct = baselineLines().stream()
                .filter(l -> l.contains(PREFIX))
                .filter(l -> declaredHere.matcher(l).find()
                        || l.trim().equals("TYPE public class " + PREFIX))
                .count();

        assertEquals(9, direct,
                "entries declared directly on FileFinder (1 TYPE + 8 METH)");
    }
}
