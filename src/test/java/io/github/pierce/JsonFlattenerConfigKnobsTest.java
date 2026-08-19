package io.github.pierce;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * [BL-015]: four of the inert {@code JsonFlattenerConfig} knobs are now honoured, one is not, and
 * the count in the filing was wrong.
 *
 * <h2>Why making them live is safe, and what makes it safe</h2>
 *
 * <p>The filing said "do not simply make them live - it would silently change behaviour for
 * released callers". The general argument is right; the SPECIFIC argument is narrower and
 * survives. Every {@code JsonFlattenerConfig} default is byte-identical to the corresponding
 * per-call default: charset UTF-8 == UTF-8, bufferSize 8192 == DEFAULT_BUFFER_SIZE, preserveNulls
 * true == includeNulls true, sortKeys false == sortKeys false. Wire each as the ENGINE-LEVEL
 * DEFAULT that an explicitly-passed options object still overrides, and the only caller whose
 * behaviour changes is the one who explicitly set the knob - the caller who asked for it and got
 * nothing.</p>
 *
 * <h2>The one that stays inert, and why that is not laziness</h2>
 *
 * <p>{@code failOnError}'s NAME gives a direction; its EFFECT is defined nowhere. "Do not fail" on
 * {@code flattenToMap(String)} could mean return an empty map, return null, return a partial map,
 * or log and continue, and nothing in the name or the javadoc picks one. A knob whose effect is
 * undetermined cannot be honoured. It stays inert and is labelled as such - but the javadoc
 * pointing callers at {@code InputOptions.lenient(boolean)} as the live alternative had to go,
 * because that knob is inert too.</p>
 *
 * <h2>The count was five; measured, it is seven</h2>
 *
 * <p>{@code InputOptions.isLenient()} and {@code isSkipInvalid()} are read nowhere in
 * {@code src/main} either. Pinned below so the seven is falsifiable rather than a grep result in
 * a report.</p>
 */
@DisplayName("JsonFlattenerConfig knobs: four live, one deliberately inert, and two more found")
class JsonFlattenerConfigKnobsTest {

    private static final String DOC = "{\"z\":1,\"a\":{\"b\":null,\"c\":\"x\"}}";

    private static JsonFlattener.FluentOperation with(
            JsonFlattener.JsonFlattenerConfig config) {
        return JsonFlattener.with(MapFlattener.builder().build(), config);
    }

    // ------------------------------------------------------------------ now live

    @Test
    @DisplayName("sortKeys(true) sorts the no-argument toJson()")
    void sortKeysTrueSortsTheNoArgToJson() {
        String json = with(JsonFlattener.JsonFlattenerConfig.builder().sortKeys(true).build())
                .from(DOC).toJson();
        assertEquals("{\"a_b\":null,\"a_c\":\"x\",\"z\":1}", json);
    }

    @Test
    @DisplayName("preserveNulls(false) DROPS null-valued keys from the no-argument toJson()")
    void preserveNullsFalseDropsNullKeysFromTheNoArgToJson() {
        // The highest-surprise change of the four, and the only one that makes output smaller by
        // deleting data. The argument for shipping it anyway: the only callers who see a
        // difference are the ones who explicitly wrote preserveNulls(false) and got nulls anyway.
        String json = with(JsonFlattener.JsonFlattenerConfig.builder().preserveNulls(false).build())
                .from(DOC).toJson();
        assertEquals("{\"z\":1,\"a_c\":\"x\"}", json);
    }

    @Test
    @DisplayName("charset is honoured on the byte overload that takes no explicit charset")
    void charsetIsHonouredOnTheNoCharsetByteOverload() {
        byte[] latin1 = "{\"k\":\"é\"}".getBytes(StandardCharsets.ISO_8859_1);

        Object decoded = with(JsonFlattener.JsonFlattenerConfig.builder()
                .charset(StandardCharsets.ISO_8859_1).build())
                .from(latin1).toMap().get("k");

        // It MUST be the byte overload: the String overload cannot show this at all, because the
        // caller has already decoded.
        assertEquals("é", decoded,
                "from(byte[]) hardcoded UTF-8, so the ISO-8859-1 byte 0xE9 arrived as U+FFFD");
    }

    @Test
    @DisplayName("bufferSize reaches the reader")
    void bufferSizeReachesTheReader() {
        // NOTHING ELSE CAN SEE THIS KNOB. Wiring bufferSize leaves output byte-identical, so the
        // existing inertness probe is blind to it by construction and so is the corpus. Without
        // this recording Reader the knob could be wired or unwired undetectably.
        RecordingReader reader = new RecordingReader(DOC);
        with(JsonFlattener.JsonFlattenerConfig.builder().bufferSize(4096).build())
                .from(reader).toMap();

        assertTrue(!reader.requests.isEmpty(), "the reader was never read from");
        assertEquals(4096, reader.requests.get(0).intValue(),
                "the first read request must be the configured buffer size, not 8192");
    }

    // ------------------------------------------------- the two claims the records overstated

    @Test
    @DisplayName("sortKeys does NOT move toPrettyJson(), which sorts unconditionally")
    void sortKeysDoesNotMoveToPrettyJsonBecauseItAlreadySorts() {
        // CHANGELOG item 24 shipped saying sortKeys is "honoured by the no-argument toJson(),
        // toPrettyJson() and toBytes()" and that "a caller who set sortKeys(true) previously got
        // insertion order". For toPrettyJson that is false in both directions: PRETTY_MAPPER
        // enables ORDER_MAP_ENTRIES_BY_KEYS unconditionally at construction, so pretty output was
        // ALREADY sorted before 2.1.0 and setting sortKeys(false) does not restore insertion
        // order. The knob is real on the other two terminals; the third was a records error.
        String off = with(JsonFlattener.JsonFlattenerConfig.builder().sortKeys(false).build())
                .from(DOC).toPrettyJson().replaceAll("\\s+", "");
        String on = with(JsonFlattener.JsonFlattenerConfig.builder().sortKeys(true).build())
                .from(DOC).toPrettyJson().replaceAll("\\s+", "");

        assertEquals(off, on, "toPrettyJson must be byte-identical at both sortKeys settings");
        assertEquals("{\"a_b\":null,\"a_c\":\"x\",\"z\":1}", off,
                "and it must be SORTED even at sortKeys(false), which is the half that makes the "
                        + "changelog's 'previously got insertion order' wrong for this terminal");

        // CONTROL: the same knob on the same document DOES move the compact terminals, so the
        // equality above is a fact about toPrettyJson and not a broken comparison.
        assertNotEquals(
                with(JsonFlattener.JsonFlattenerConfig.builder().sortKeys(false).build())
                        .from(DOC).toJson(),
                with(JsonFlattener.JsonFlattenerConfig.builder().sortKeys(true).build())
                        .from(DOC).toJson(),
                "VACUITY CONTROL: sortKeys must still be observable somewhere");
    }

    @Test
    @DisplayName("the engine charset ENCODES output too, and that is lossy")
    void theEngineCharsetEncodesOutputAndCanLoseCharacters() {
        // Recorded because the disclosure did not say it. engineDefaults() feeds
        // config.getCharset() into the synthesised OutputOptions, and toBytes() writes through
        // it - so a caller who set ISO-8859-1 meaning "decode my input that way" also silently
        // changed how output is encoded. Characters outside the charset become '?'.
        byte[] utf8 = with(JsonFlattener.JsonFlattenerConfig.builder()
                .charset(StandardCharsets.UTF_8).build()).from("{\"k\":\"日本\"}").toBytes();
        byte[] latin1 = with(JsonFlattener.JsonFlattenerConfig.builder()
                .charset(StandardCharsets.ISO_8859_1).build()).from("{\"k\":\"日本\"}").toBytes();

        assertEquals("{\"k\":\"日本\"}", new String(utf8, StandardCharsets.UTF_8));
        assertEquals("{\"k\":\"??\"}", new String(latin1, StandardCharsets.ISO_8859_1),
                "two characters were replaced by '?' on the way OUT, after decoding fine on the "
                        + "way in. A caller who wants non-UTF-8 input and UTF-8 output must pass "
                        + "an explicit OutputOptions.");
    }

    // ------------------------------------------------------------------ the precedence control

    @Test
    @DisplayName("CONTROL: explicit per-call options still beat the engine default")
    void explicitPerCallOptionsStillBeatTheEngineDefault() {
        // Passes before AND after - stated as a control. Its job is forward: an implementation
        // that applies the config UNCONDITIONALLY instead of as a default would break every
        // existing caller who passes explicit options, and nothing else in the suite sees that.
        JsonFlattener.JsonFlattenerConfig config = JsonFlattener.JsonFlattenerConfig.builder()
                .sortKeys(true).preserveNulls(false).charset(StandardCharsets.ISO_8859_1).build();

        assertEquals("{\"z\":1,\"a_b\":null,\"a_c\":\"x\"}",
                with(config).from(DOC).toJson(JsonFlattener.OutputOptions.defaults()),
                "an explicitly passed OutputOptions must win over the engine default");

        assertEquals("é",
                with(config).from("{\"k\":\"é\"}".getBytes(StandardCharsets.UTF_8),
                        StandardCharsets.UTF_8).toMap().get("k"),
                "an explicitly passed charset must win over the engine default");
    }

    @Test
    @DisplayName("CONTROL: the defaults are unchanged, so an unconfigured caller sees nothing")
    void theDefaultsAreUnchanged() {
        assertEquals("{\"z\":1,\"a_b\":null,\"a_c\":\"x\"}",
                JsonFlattener.create().from(DOC).toJson(),
                "every JsonFlattenerConfig default equals the per-call default it now feeds, so "
                        + "wiring them must move nothing for a caller who configured nothing. "
                        + "That equality is the whole safety argument for this change.");
    }

    // ------------------------------------------------------------------ still inert

    @Test
    @DisplayName("failOnError is still inert, and nothing points at a live alternative that is not")
    void failOnErrorIsStillInertAndNothingPointsAtAnInertAlternative() {
        // (a) THE SURVIVING PIN, carried over verbatim. failOnError has no defined effect, so it
        //     is not honoured, and both settings must still throw.
        for (boolean setting : new boolean[] {true, false}) {
            JsonFlattener engine = JsonFlattener.builder().failOnError(setting).buildFlattener();
            assertThrows(RuntimeException.class, () -> engine.flattenToMap("{not json"),
                    "failOnError(" + setting + ") must behave identically: the knob is inert");
        }

        // (b) NEW CHARACTERIZATION PIN of a fact nobody had written down. BL-015 counts FIVE
        //     inert knobs. Measured, it is SEVEN: InputOptions.lenient and .skipInvalid are read
        //     nowhere in src/main either - and they are exactly what failOnError's javadoc used
        //     to advertise as the live alternative.
        java.nio.file.Path bad;
        try {
            bad = java.nio.file.Files.createTempFile("inert-knob-probe", ".json");
            java.nio.file.Files.writeString(bad, "{not json", StandardCharsets.UTF_8);
            bad.toFile().deleteOnExit();
        } catch (IOException e) {
            throw new AssertionError("could not write the probe file", e);
        }
        assertThrows(RuntimeException.class,
                () -> JsonFlattener.create().from(bad,
                        JsonFlattener.InputOptions.builder()
                                .lenient(true).skipInvalid(true).build()).toMap(),
                "lenient(true).skipInvalid(true) must still throw; both are inert, which is why "
                        + "the failOnError javadoc no longer names them");
    }

    /** Records the length of every read request so the buffer size is observable. */
    private static final class RecordingReader extends Reader {
        private final StringReader delegate;
        private final List<Integer> requests = new ArrayList<>();

        RecordingReader(String content) {
            this.delegate = new StringReader(content);
        }

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            requests.add(len);
            return delegate.read(cbuf, off, len);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
