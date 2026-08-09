package io.github.pierce.path;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Label;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for the injective path encoding.
 *
 * <p><b>Why these live in their own flat class.</b> jqwik and JUnit Jupiter are separate engines.
 * jqwik does not understand Jupiter's {@code @Nested} — it has its own {@code @Group} — so a
 * {@code @Property} method inside a {@code @Nested} class is silently never discovered. It does
 * not error; the test count is simply lower than you expect. That is exactly the failure that
 * left 26 property tests unexecuted in this repository before, so the properties are kept flat
 * and the example-based tests stay in {@link FlattenedPathTest}.</p>
 *
 * <p>The central claim about this encoding is a bijection, which is a statement about all inputs.
 * Example tests can only cover the cases someone thought of, and the case nobody thought of — a
 * field name containing the separator — is precisely the one that shipped broken.</p>
 */
@Label("FlattenedPath encoding properties")
class FlattenedPathPropertyTest {

    /** The whole point: encoding then decoding must return exactly what went in. */
    @Property(tries = 2000)
    @Label("decode(encode(segments)) == segments")
    void isBijective(@ForAll("pathSegments") List<String> segments) {
        String encoded = FlattenedPath.encode(segments, "_");
        assertThat(FlattenedPath.decodeSegments(encoded, "_")).isEqualTo(segments);
    }

    @Property(tries = 1000)
    @Label("bijective for multi-character separators")
    void isBijectiveForMultiCharSeparator(@ForAll("pathSegments") List<String> segments) {
        String encoded = FlattenedPath.encode(segments, "__");
        assertThat(FlattenedPath.decodeSegments(encoded, "__")).isEqualTo(segments);
    }

    /**
     * Injectivity stated directly: distinct inputs must never collide.
     *
     * <p>This is the property the old concatenation encoding violated, and violating it is what
     * made reconstruction lossy and turned ordinary snake_case field names into a DoS vector.</p>
     */
    @Property(tries = 2000)
    @Label("distinct segment lists never encode to the same key")
    void isInjective(@ForAll("pathSegments") List<String> a,
                     @ForAll("pathSegments") List<String> b) {
        String ea = FlattenedPath.encode(a, "_");
        String eb = FlattenedPath.encode(b, "_");
        if (a.equals(b)) {
            assertThat(ea).isEqualTo(eb);
        } else {
            assertThat(ea).isNotEqualTo(eb);
        }
    }

    /**
     * Most real field names contain no separator, and those must encode byte-identically to the
     * old scheme — otherwise this change would break every consumer rather than only the ones
     * relying on the broken case.
     */
    @Property(tries = 500)
    @Label("segments without separators encode identically to the legacy scheme")
    @SuppressWarnings("deprecation")
    void unaffectedWhenNoSeparatorPresent(@ForAll("separatorFreeSegments") List<String> segments) {
        assertThat(FlattenedPath.encode(segments, "_"))
                .isEqualTo(FlattenedPath.encodeLegacy(segments, "_"));
    }

    /** Encoding is deterministic — the same input always yields the same key. */
    @Property(tries = 500)
    @Label("encoding is deterministic")
    void isDeterministic(@ForAll("pathSegments") List<String> segments) {
        assertThat(FlattenedPath.encode(segments, "_"))
                .isEqualTo(FlattenedPath.encode(segments, "_"));
    }

    @Provide
    Arbitrary<List<String>> pathSegments() {
        Arbitrary<String> segment = Arbitraries.oneOf(
                Arbitraries.strings().alpha().numeric().ofMaxLength(8),
                // The adversarial cases: separators and escape characters inside a field name.
                Arbitraries.of("user_id", "created_at", "a_b_c", "_", "__", "___",
                        "back\\slash", "\\", "\\\\", "\\_", "_\\", "", "a\\_b"));
        return segment.list().ofMinSize(1).ofMaxSize(6);
    }

    @Provide
    Arbitrary<List<String>> separatorFreeSegments() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                .list().ofMinSize(1).ofMaxSize(5);
    }
}
