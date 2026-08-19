package io.github.pierce.docs;

import io.github.pierce.spark.NexusPiercerPatterns;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * No document and no javadoc example names a {@code NexusPiercerPatterns} method that does not
 * exist.
 *
 * <h2>Why this exists beside the compile gate rather than inside it</h2>
 *
 * <p>{@link DocumentedJavaSnippetsCompileTest} is the real gate and is strictly stronger - but it
 * depends on three things that can be silently misconfigured: javac being present,
 * {@code target/test-cp.txt} being correct, and the wrapper templates being sound. This test
 * depends on none of them. It reflects over the class, reads text, and compares two name sets. If
 * the compile gate is ever quietly disarmed, this still fails.</p>
 *
 * <p>It also reaches ground the compile gate does not: {@code <pre>} examples in {@code src/main}
 * javadoc. The class javadoc on {@code NexusPiercerPatterns} carried a phantom method for two
 * releases, was "corrected" into a phantom SIGNATURE in 2.1.0, and {@code CHANGELOG.md} recorded
 * that as a fix. An unchecked javadoc example is the same defect as an unchecked markdown
 * block.</p>
 *
 * <h2>Code only, never prose</h2>
 *
 * <p>A repository has to be able to WRITE about a defect. {@code CHANGELOG.md} and the frozen
 * audit register both name {@code jsonToDelta} and {@code kafkaToParquetStream} in sentences
 * saying they never existed, and a scanner that failed on those would push the history out of the
 * record. What must not survive is a call a reader could copy.</p>
 *
 * <h2>The name set is reflected, never listed</h2>
 *
 * <p>Four phantoms were named explicitly in a warning banner and the defect survived the pass that
 * named them. A hardcoded list of four would let a fifth through.</p>
 */
@DisplayName("Nothing published names a NexusPiercerPatterns method that does not exist")
class NoPhantomPatternsMethodIsPublishedAsCallableTest {

    private static final Pattern CALL =
            Pattern.compile("NexusPiercerPatterns\\s*\\.\\s*([A-Za-z_$][\\w$]*)\\s*\\(");

    private static final Pattern PRE = Pattern.compile("(?s)<pre>(.*?)</pre>");

    @Test
    @DisplayName("every NexusPiercerPatterns.x( in published code names a real method")
    void everyPublishedCallNamesARealMethod() {
        Set<String> real = realMethodNames();
        assertThat(real).as("reflection found no public methods on NexusPiercerPatterns at all, "
                + "which would make every comparison below vacuously interesting").isNotEmpty();

        List<String> offences = new ArrayList<>();
        int scanned = 0;
        for (DocSnippetSource.Snippet block : DocSnippetSource.blocks()) {
            scanned += scanText(block.code(), block.where(), real, offences);
        }
        for (Path java : mainJavaFiles()) {
            String label = DocSnippetSource.moduleRoot().relativize(java).toString()
                    .replace('\\', '/');
            for (String pre : javadocPreBlocks(read(java))) {
                scanned += scanText(pre, label + " (javadoc <pre>)", real, offences);
            }
        }

        assertThat(scanned)
                .as("VERIFY THE COUNT: this is a scan, and a scan that reads nothing passes. It "
                        + "must find the real calls before its silence about phantoms means "
                        + "anything.")
                .isGreaterThan(0);
        assertThat(offences)
                .as("A PUBLISHED CALL NAMES A METHOD THAT DOES NOT EXIST. The real public methods "
                        + "are %s. This is finding OSS-01: six such calls lived in "
                        + "docs/SPARK_PIPELINE.md across four releases, and the pass that added a "
                        + "banner naming four phantoms introduced a fifth instance in the README "
                        + "and a sixth in the class javadoc.", real)
                .isEmpty();
    }

    @Test
    @DisplayName("the class javadoc example is scanned, not merely present")
    void theClassJavadocExampleIsCoveredByThisScan() {
        Path patterns = DocSnippetSource.moduleRoot()
                .resolve("src/main/java/io/github/pierce/spark/NexusPiercerPatterns.java");
        assertThat(patterns).exists();
        String text = read(patterns);
        assertThat(text).as("the class javadoc must keep an example; deleting it is not how this "
                + "finding gets closed").contains("<pre>");

        List<String> named = new ArrayList<>();
        for (String pre : javadocPreBlocks(text)) {
            Matcher m = CALL.matcher(pre);
            while (m.find()) {
                named.add(m.group(1));
            }
        }
        assertThat(named).as("CONTROL: the scan must actually see calls in this file, or its "
                + "clean verdict on every other file means nothing").isNotEmpty();
        assertThat(new TreeSet<>(named))
                .as("the javadoc example names methods that are not on the class")
                .isSubsetOf(realMethodNames());
    }

    private static Set<String> realMethodNames() {
        Set<String> names = new TreeSet<>();
        for (Method m : NexusPiercerPatterns.class.getMethods()) {
            if (m.getDeclaringClass() == NexusPiercerPatterns.class) {
                names.add(m.getName());
            }
        }
        return names;
    }

    private static int scanText(String text, String label, Set<String> real, List<String> offences) {
        int found = 0;
        Matcher m = CALL.matcher(text);
        while (m.find()) {
            found++;
            if (!real.contains(m.group(1))) {
                offences.add(label + " calls NexusPiercerPatterns." + m.group(1) + "(");
            }
        }
        return found;
    }

    /** Every {@code <pre>...</pre>} region in a source file, javadoc leaders stripped. */
    private static List<String> javadocPreBlocks(String source) {
        List<String> out = new ArrayList<>();
        Matcher m = PRE.matcher(source);
        while (m.find()) {
            StringBuilder b = new StringBuilder();
            for (String line : m.group(1).split("\n")) {
                b.append(line.strip().replaceFirst("^\\*\\s?", "")).append('\n');
            }
            out.add(b.toString());
        }
        return out;
    }

    private static List<Path> mainJavaFiles() {
        Path root = DocSnippetSource.moduleRoot().resolve("src/main/java");
        try (var walk = Files.walk(root)) {
            return walk.filter(p -> p.toString().endsWith(".java")).toList();
        } catch (IOException e) {
            throw new AssertionError("cannot walk " + root, e);
        }
    }

    private static String read(Path p) {
        try {
            return Files.readString(p, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AssertionError("cannot read " + p, e);
        }
    }
}
