package io.github.pierce.gates;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * No javadoc may call a {@code JsonFlattenerConfig} knob inert while its getter is read.
 *
 * <h2>Why this exists</h2>
 *
 * <p>2.1.0 wired four of the six {@code JsonFlattenerConfig} knobs up. Each knob is described in
 * FIVE places - the class table, the getter, {@code Builder.x}, {@code ConfigBuilder.x} and the
 * {@code buildFlattener()} paragraph - and the pass that wired them updated some of each. Two
 * survived the whole release still saying the opposite of the code:</p>
 *
 * <ul>
 *   <li>{@code ConfigBuilder.charset} said "INERT ... stored and read by nothing" while
 *       {@code getCharset()} was read at four sites in {@code src/main}, three lines below a
 *       class table saying "honoured since 2.1.0" and a getter saying the same.</li>
 *   <li>{@code buildFlattener()} - public API, the method a caller reads to learn what the engine
 *       honours - said all five knobs "are read nowhere in {@code src/main} at all", and pointed
 *       at an inertness probe that had been rewritten to assert liveness.</li>
 * </ul>
 *
 * <p>Prose review found both. It found neither the pass before. The check is mechanical, so it
 * should be mechanical: a knob's status is a fact about whether its getter is called, and that
 * fact is greppable.</p>
 *
 * <h2>The rule</h2>
 *
 * <p>For each getter on {@code JsonFlattenerConfig}: if it is called anywhere in {@code src/main}
 * outside its own declaration, then no javadoc in {@code JsonFlattener.java} may attach an
 * inertness phrase to that knob's name. {@code failOnError} is exempt because it is genuinely
 * inert BY DESIGN and its javadoc must keep saying so - it is listed explicitly rather than
 * inferred, so making a knob inert again requires editing this gate and saying why.</p>
 */
@DisplayName("no knob is documented as inert while its getter is read in src/main")
class ConfigKnobJavadocMatchesItsWiringTest {

    private static final Path SOURCE =
            Paths.get("src", "main", "java", "io", "github", "pierce", "JsonFlattener.java");
    private static final Path MAIN = Paths.get("src", "main", "java");

    /** knob name -> its getter, as the config declares them. */
    private static final Map<String, String> KNOBS = new LinkedHashMap<>();

    static {
        KNOBS.put("usePrettyPrint", "isUsePrettyPrint");
        KNOBS.put("prettyPrint", "isUsePrettyPrint");
        KNOBS.put("charset", "getCharset");
        KNOBS.put("bufferSize", "getBufferSize");
        KNOBS.put("preserveNulls", "isPreserveNulls");
        KNOBS.put("sortKeys", "isSortKeys");
        KNOBS.put("failOnError", "isFailOnError");
    }

    /** Inert BY DESIGN, and its javadoc must keep saying so. */
    private static final String DELIBERATELY_INERT = "failOnError";

    private static final List<String> INERTNESS_PHRASES = List.of(
            "read by nothing", "stored and read by nothing", "read nowhere",
            "is inert", "inert.", "read at all");

    /** A javadoc comment and the line it starts on. */
    private static final Pattern JAVADOC = Pattern.compile("(?s)/\\*\\*.*?\\*/");

    private static String read(Path p) throws IOException {
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    /** Whether {@code getter()} is invoked anywhere in src/main other than where it is declared. */
    private static boolean getterIsRead(String getter) throws IOException {
        Pattern call = Pattern.compile("\\.\\s*" + Pattern.quote(getter) + "\\s*\\(");
        try (var walk = Files.walk(MAIN)) {
            for (Path p : walk.filter(f -> f.toString().endsWith(".java")).toList()) {
                if (call.matcher(read(p)).find()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Test
    @DisplayName("an inertness phrase may not name a knob whose getter is called")
    void inertnessClaimsMatchTheWiring() throws IOException {
        String source = read(SOURCE);
        List<String> offences = new ArrayList<>();
        int examined = 0;

        for (Map.Entry<String, String> knob : KNOBS.entrySet()) {
            if (DELIBERATELY_INERT.equals(knob.getKey())) {
                continue;
            }
            if (!getterIsRead(knob.getValue())) {
                continue;
            }
            examined++;

            Matcher doc = JAVADOC.matcher(source);
            while (doc.find()) {
                String body = doc.group().toLowerCase(java.util.Locale.ROOT);
                if (!body.contains(knob.getKey().toLowerCase(java.util.Locale.ROOT))) {
                    continue;
                }
                for (String phrase : INERTNESS_PHRASES) {
                    // A javadoc may narrate the history ("this said INERT for a whole pass") and
                    // must be able to, or the record cannot describe its own defect. What it may
                    // not do is state it in the present tense, so a past-tense marker clears it.
                    boolean narratesHistory = body.contains("used to")
                            || body.contains("said ") || body.contains("for the whole of")
                            || body.contains("previously") || body.contains("until");
                    if (body.contains(phrase) && !narratesHistory) {
                        int line = 1 + (int) source.substring(0, doc.start()).chars()
                                .filter(c -> c == '\n').count();
                        offences.add(SOURCE + ":" + line + " calls '" + knob.getKey()
                                + "' inert (\"" + phrase + "\") but " + knob.getValue()
                                + "() is read in src/main");
                        break;
                    }
                }
            }
        }

        assertTrue(examined >= 4,
                "VERIFY THE COUNT: only " + examined + " live knobs were examined. This gate is a "
                        + "loop over knobs whose getters are read, and if the getter search stops "
                        + "matching, the loop body never runs and the gate passes having checked "
                        + "nothing. Four knobs went live in 2.1.0; expect at least four.");

        assertTrue(offences.isEmpty(),
                "A JAVADOC CALLS A LIVE KNOB INERT:\n  " + String.join("\n  ", offences)
                        + "\nEach knob is described in five places and this is the second pass in "
                        + "a row where one of them did not follow the code. Fix the javadoc.");
    }

    @Test
    @DisplayName("CONTROL: failOnError really is inert, so its exemption is not cover")
    void theExemptedKnobIsGenuinelyInert() throws IOException {
        assertTrue(!getterIsRead(KNOBS.get(DELIBERATELY_INERT)),
                "isFailOnError() is now read in src/main, so failOnError is no longer inert by "
                        + "design and must come out of the exemption list above - otherwise this "
                        + "gate is quietly excusing the exact defect it exists to catch.");
    }
}
