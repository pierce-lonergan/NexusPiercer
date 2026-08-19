package io.github.pierce;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code parseArrayValue} served two callers with opposite contracts, and answered both the same
 * way.
 *
 * <p>Structure inference asks a PREDICATE - "is this value a serialized array?" - of every value in
 * every prefix group, and "no" is the ordinary answer on ordinary data. The other two callers have
 * already COMMITTED: either the caller named the path in {@code arrayPaths()}, or inference ruled
 * the prefix an array from its sibling columns. There, "I could not parse it" is not an answer, it
 * is a silent substitution.</p>
 *
 * <p>Both used to receive a one-element list holding the raw unparsed text, indistinguishable from
 * a legitimate one-element array. In {@code reconstructArray} that was worse still: the single
 * unparsed element was replicated into every element of an N-element array by the last-value
 * clamp, so one piece of garbage was presented as N successfully parsed values.</p>
 *
 * <p>The method is now split. The committed converter throws; the probe calls it and catches, so
 * the two cannot drift.</p>
 */
@DisplayName("an unparseable bracketed column is refused where the caller has committed")
class JsonReconstructorArrayParseContractTest {

    @Test
    @DisplayName("a committed array path with unparseable bracket text throws")
    void committedArrayPathWithUnparseableBracketTextThrows() {
        JsonReconstructor r = JsonReconstructor.builder()
                .arrayPaths("a_b")
                .arrayFormat(JsonReconstructor.ArraySerializationFormat.JSON)
                .build();

        assertThatThrownBy(() -> r.reconstruct(Map.of("a_b", "[oops]")))
                .isInstanceOf(JsonReconstructor.ArrayParseException.class)
                .hasMessageContaining("a_b")
                .hasMessageContaining("JSON")
                .hasMessageContaining("[oops]");
    }

    @Test
    @DisplayName("an unparseable column is not replicated across every element")
    void anUnparseableColumnIsNotReplicatedAcrossEveryElement() {
        // THE SITE THAT MATTERS. Before the split, users_note parsed to a one-element list holding
        // "[oops]" while users_name parsed to two, so maxSize was 2 and reconstructArray's
        // last-value clamp gave BOTH records note="[oops]".
        JsonReconstructor r = JsonReconstructor.builder()
                .arrayPaths("users")
                .arrayFormat(JsonReconstructor.ArraySerializationFormat.JSON)
                .build();

        assertThatThrownBy(() -> r.reconstruct(Map.of(
                "users_name", "[\"Alice\",\"Bob\"]",
                "users_note", "[oops]")))
                .isInstanceOf(JsonReconstructor.ArrayParseException.class)
                .hasMessageContaining("users_note");
    }

    @Test
    @DisplayName("GOOD INPUT CONTROL: a valid one-element JSON array still parses")
    void goodInputControlAValidOneElementJsonArrayStillParses() {
        // HONEST STATEMENT: this passes before AND after. It is the capable-of-discriminating leg.
        // Without it the two assertThrows above would also pass against an implementation that
        // threw on every bracketed value, which would be a far worse regression than the one being
        // repaired.
        JsonReconstructor r = JsonReconstructor.builder()
                .arrayPaths("a_b")
                .arrayFormat(JsonReconstructor.ArraySerializationFormat.JSON)
                .build();

        Map<String, Object> out = r.reconstruct(Map.of("a_b", "[\"only\"]"));

        @SuppressWarnings("unchecked")
        Map<String, Object> a = (Map<String, Object>) out.get("a");
        assertThat(a.get("b")).isInstanceOf(List.class);
        assertThat((List<Object>) a.get("b")).containsExactly("only");
    }

    @Test
    @DisplayName("the structure probe still treats unparseable bracket text as not-an-array")
    void theStructureProbeStillTreatsUnparseableBracketTextAsNotAnArray() {
        // HONEST STATEMENT: this passes before AND after, and it is the reason the method had to
        // be SPLIT rather than simply made to throw. Applying the throw at the inference call site
        // instead would turn limits/circular-map-reference-is-marked-and-the-guard-is-live RED, by
        // rejecting a benign "[CIRCULAR_REFERENCE]" marker - converting a working reconstruction
        // into a hard failure. That row is the only fixture in the whole corpus that reaches this
        // catch at all.
        assertThatCode(() -> {
            Map<String, Object> out =
                    JsonReconstructor.quickReconstruct(Map.of("a_a", "[CIRCULAR_REFERENCE]"));

            @SuppressWarnings("unchecked")
            Map<String, Object> a = (Map<String, Object>) out.get("a");
            assertThat(a.get("a"))
                    .as("the marker must come back as the String it is, not as a List")
                    .isEqualTo("[CIRCULAR_REFERENCE]");
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("the delimited formats are unchanged - a Jackson decline there is the cascade working")
    void delimitedFormatsStillFallThroughWithoutThrowing() {
        // Under BRACKET_LIST / COMMA_SEPARATED / PIPE_SEPARATED the JSON attempt is the first leg
        // of a genuine try-this-then-that cascade, so a decline is not a contradiction.
        JsonReconstructor bracket = JsonReconstructor.builder()
                .arrayPaths("a_b")
                .arrayFormat(JsonReconstructor.ArraySerializationFormat.BRACKET_LIST)
                .build();

        assertThatCode(() -> bracket.reconstruct(Map.of("a_b", "[x, y]")))
                .doesNotThrowAnyException();

        JsonReconstructor comma = JsonReconstructor.builder()
                .arrayPaths("a_b")
                .arrayFormat(JsonReconstructor.ArraySerializationFormat.COMMA_SEPARATED)
                .build();

        assertThatCode(() -> comma.reconstruct(Map.of("a_b", "x,y")))
                .doesNotThrowAnyException();
    }
}
