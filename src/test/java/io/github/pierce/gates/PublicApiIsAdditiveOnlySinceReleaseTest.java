package io.github.pierce.gates;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 2.0.0 is released and staged on Maven Central, so work on {@code main} must be ADDITIVE ONLY.
 * This is the control that enforces it.
 *
 * <p>WHY IT EXISTS. Until now the additive-only constraint was a review discipline with nothing
 * behind it: an adversarial review established compliance for one commit by hand, in a scratch
 * directory, using artifacts that vanished with the session. {@code grep -i "japicmp|revapi|
 * animal-sniffer|clirr" pom.xml .github/workflows/*.yml} returns nothing, and there was no
 * reflective surface test either. In a repository whose signature pathology is "a control that
 * appears present and does nothing", the additive-only rule was worse than that — the control was
 * simply absent, and the next round would have had to re-establish it by hand or not at all.
 *
 * <p>WHAT THE BASELINE IS. {@code src/test/resources/api/public-api-2.0.0.txt}, extracted from the
 * released {@code nexus-piercer-2.0.0.jar} itself rather than from a rebuild of the tag. The
 * extraction ran under a classloader whose parent is the platform loader; an earlier attempt used
 * the application loader, which already carries {@code target/classes}, so parent-first delegation
 * resolved every {@code io.github.pierce} name to the CURRENT build and reported the released and
 * current surfaces as byte-identical. That is worth recording because it is the same pathology in
 * a new place: the tool ran, produced a confident empty diff, and measured nothing.
 *
 * <p>WHAT IT CATCHES AND WHAT IT DOES NOT. It catches removals and signature changes of public and
 * protected members — the binary-incompatible changes. It deliberately does NOT capture generic
 * signatures, so a source-visible but binary-compatible generics change passes; and it does not
 * model exception clauses or annotations. It is a floor, not japicmp. japicmp is the better tool
 * and is the upgrade if a network-permitted verification job ever exists; it is not used here
 * because it resolves its baseline remotely and this build verifies with {@code -o}.
 */
@DisplayName("The public API has only grown since the released 2.0.0")
class PublicApiIsAdditiveOnlySinceReleaseTest {

    private static final String BASELINE = "api/public-api-2.0.0.txt";

    /** The released surface is ~1255 members; anything far below it means the file was gutted. */
    private static final int SANITY_FLOOR = 1000;

    private static Path moduleRoot() {
        Path p = Paths.get("").toAbsolutePath();
        while (p != null && !Files.isRegularFile(p.resolve("pom.xml"))) {
            p = p.getParent();
        }
        if (p == null) {
            throw new AssertionError("could not find the module root");
        }
        return p;
    }

    private static Path baselineFile() {
        return moduleRoot().resolve("src/test/resources/api").resolve("public-api-2.0.0.txt");
    }

    private static Path classesDir() {
        return moduleRoot().resolve("target/classes");
    }

    private static SortedSet<String> released() {
        Path f = baselineFile();
        assertTrue(Files.isRegularFile(f),
                "THE API BASELINE IS MISSING: " + f + ". Without it this gate cannot tell an "
                        + "additive change from a breaking one, and would pass either way.");
        SortedSet<String> b = PublicApiSurface.readBaseline(f);
        assertTrue(b.size() >= SANITY_FLOOR,
                "THE API BASELINE HAS ONLY " + b.size() + " entries, below the sanity floor of "
                        + SANITY_FLOOR + ". A truncated baseline makes every containment check "
                        + "below trivially true - which is exactly how this gate would come to "
                        + "'pass' while enforcing nothing.");
        return b;
    }

    private static SortedSet<String> current() {
        Path d = classesDir();
        assertTrue(Files.isDirectory(d),
                "target/classes is missing at " + d + "; the surface cannot be measured. Run "
                        + "the build before this gate rather than letting it pass on no data.");
        return PublicApiSurface.fromClassesDirectory(d,
                PublicApiIsAdditiveOnlySinceReleaseTest.class.getClassLoader());
    }

    /** Baseline entries absent from the current surface. Non-empty means a BREAKING change. */
    private static List<String> removals(SortedSet<String> base, SortedSet<String> now) {
        List<String> gone = new ArrayList<>();
        for (String s : base) {
            if (!now.contains(s)) {
                gone.add(s);
            }
        }
        return gone;
    }

    // ------------------------------------------------------------- 1. good input passes

    @Nested
    @DisplayName("The released surface is still entirely present")
    class NothingRemoved {

        @Test
        @DisplayName("every member of released 2.0.0 is still on the current build")
        void noReleasedMemberHasBeenRemoved() {
            SortedSet<String> base = released();
            SortedSet<String> now = current();
            List<String> gone = removals(base, now);

            assertTrue(gone.isEmpty(),
                    "BREAKING CHANGE ON A RELEASED COORDINATE. " + gone.size() + " member(s) "
                            + "present in the released 2.0.0 artifact are absent from this build. "
                            + "A consumer compiled against 2.0.0 will fail at link time. Add the "
                            + "new form ALONGSIDE the old one, or defer the removal to 3.0.0 with "
                            + "a migration note. Do NOT delete the line from the baseline to make "
                            + "this pass.\nMissing:\n  "
                            + String.join("\n  ", gone.subList(0, Math.min(25, gone.size()))));
        }

        @Test
        @DisplayName("the current surface is a strict superset - additions are reported, not failed")
        void additionsAreAllowedAndVisible() {
            SortedSet<String> base = released();
            SortedSet<String> now = current();

            SortedSet<String> added = new TreeSet<>(now);
            added.removeAll(base);

            // Additions are legal. This assertion documents what they currently are, so that a
            // reviewer sees the growing surface in the diff rather than discovering it later.
            assertTrue(added.size() < 200,
                    "the public surface grew by " + added.size() + " members since 2.0.0. That is "
                            + "allowed, but it is a lot to add in a minor line - confirm each is "
                            + "intended and documented in CHANGELOG.md.\nAdded:\n  "
                            + String.join("\n  ", added));
            assertTrue(now.size() >= base.size(),
                    "the current surface (" + now.size() + ") is smaller than released 2.0.0 ("
                            + base.size() + ")");
        }
    }

    // -------------------------------------------- 2. a synthetic violation blocks

    @Nested
    @DisplayName("The comparison actually detects a removal")
    class SyntheticViolationBlocks {

        @Test
        @DisplayName("a fabricated released member that no longer exists IS reported as a removal")
        void aRemovedMemberIsDetected() {
            SortedSet<String> now = current();
            SortedSet<String> tampered = new TreeSet<>(released());
            tampered.add("METH public void io.github.pierce.JsonFlattener.methodDeletedInThisRelease()");

            List<String> gone = removals(tampered, now);

            assertEquals(1, gone.size(),
                    "THE VACUITY CONTROL. If a baseline entry that definitely does not exist in "
                            + "target/classes is not reported as removed, the comparison is broken "
                            + "and the passing test above proves nothing.");
            assertTrue(gone.get(0).contains("methodDeletedInThisRelease"),
                    "the reported removal must be the fabricated one, not something else");
        }

        @Test
        @DisplayName("a real member's signature change reads as a removal plus an addition")
        void aSignatureChangeIsDetected() {
            SortedSet<String> now = current();
            // The real released signature, with its return type changed - what a breaking edit
            // to buildFlattener() would look like.
            String real = "METH public io.github.pierce.JsonFlattener "
                    + "io.github.pierce.JsonFlattener.Builder.buildFlattener()";
            assertTrue(now.contains(real),
                    "precondition: the current surface must contain " + real);

            SortedSet<String> tampered = new TreeSet<>();
            tampered.add(real.replace("public io.github.pierce.JsonFlattener ", "public java.lang.Object "));

            assertFalse(removals(tampered, now).isEmpty(),
                    "a changed return type must read as a removal - binary compatibility is "
                            + "defined over the descriptor, which includes it");
        }
    }

    // ------------------------------------ 3. missing / empty input blocks

    @Nested
    @DisplayName("Missing or empty inputs fail loudly rather than passing")
    class MissingInputBlocks {

        @Test
        @DisplayName("an empty baseline file is rejected by the sanity floor")
        void anEmptyBaselineIsRejected() throws IOException {
            Path tmp = Files.createTempFile("api-empty", ".txt");
            try {
                Files.writeString(tmp, "# only a comment\n\n", StandardCharsets.UTF_8);
                SortedSet<String> parsed = PublicApiSurface.readBaseline(tmp);
                assertTrue(parsed.isEmpty(), "comments and blanks must not become entries");
                assertTrue(parsed.size() < SANITY_FLOOR,
                        "an empty baseline must fall below the sanity floor, so that emptying the "
                                + "file cannot silently disable this gate");
            } finally {
                Files.deleteIfExists(tmp);
            }
        }

        @Test
        @DisplayName("scanning a directory with no classes fails rather than reporting an empty surface")
        void anEmptyClassesDirectoryIsRejected() throws IOException {
            Path empty = Files.createTempDirectory("api-noclasses");
            try {
                AssertionError e = assertThrows(AssertionError.class,
                        () -> PublicApiSurface.fromClassesDirectory(empty,
                                getClass().getClassLoader()),
                        "an empty scan must throw. If it returned an empty set, every removal "
                                + "check would report the entire baseline as removed - or, worse, "
                                + "a containment check written the other way round would pass.");
                assertTrue(e.getMessage().contains("NO CLASSES"), e.getMessage());
            } finally {
                Files.deleteIfExists(empty);
            }
        }

        @Test
        @DisplayName("the baseline on disk is the released one, not a copy of the current build")
        void theBaselineIsNotJustTodaysSurface() {
            SortedSet<String> base = released();
            SortedSet<String> now = current();

            // If someone regenerates the baseline from target/classes, this gate silently becomes
            // "today equals today" and can never fail again. The two additive members added after
            // 2.0.0 are the tell: they must be in the current surface and NOT in the baseline.
            String buildFlattener = "METH public io.github.pierce.JsonFlattener "
                    + "io.github.pierce.JsonFlattener.Builder.buildFlattener()";
            assertTrue(now.contains(buildFlattener),
                    "precondition: buildFlattener() exists on the current build");
            assertFalse(base.contains(buildFlattener),
                    "THE BASELINE HAS BEEN REGENERATED FROM THE CURRENT BUILD. buildFlattener() "
                            + "was added AFTER 2.0.0 and must not appear in a 2.0.0 baseline. If "
                            + "it does, the file was overwritten with today's surface and this "
                            + "gate now compares the build to itself - it can never fail. Restore "
                            + "it from the released artifact.");
        }
    }
}
