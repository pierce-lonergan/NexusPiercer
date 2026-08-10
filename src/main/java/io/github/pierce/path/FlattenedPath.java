package io.github.pierce.path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An injective encoding of a nested field path into a single flat key.
 *
 * <h2>The problem this replaces</h2>
 *
 * <p>Until 2.0 a flattened key was built by plain concatenation:
 * {@code prefix + separator + fieldName}. With the default {@code "_"} separator that encoding is
 * not injective — two structurally different documents collapse to the same key:</p>
 *
 * <pre>
 *   {"user_id": 1}          -&gt;  "user_id"
 *   {"user": {"id": 1}}     -&gt;  "user_id"     // identical
 * </pre>
 *
 * <p>Reconstruction therefore cannot recover the original shape, and the two flatteners in this
 * library resolved the collision in <em>opposite</em> directions, so the generated Spark schema
 * disagreed with the generated row data about which field won.</p>
 *
 * <p>It was also a denial-of-service vector. Because the reconstructor could not tell a
 * structural separator from a literal one, it had to consider every candidate grouping. Holding
 * structure fixed at 40 flattened keys and varying only the underscores per field name,
 * reconstruction went from ~200 ms to heap exhaustion:</p>
 *
 * <pre>
 *   field_{n}         (one _)    196 ms    174 ms    233 ms   (flat through 150 records)
 *   nested_field_{n}  (two _)  1,198 ms  3,435 ms      OOM
 * </pre>
 *
 * <h2>The encoding</h2>
 *
 * <p>Backslash escaping, applied to each segment before joining:</p>
 *
 * <ol>
 *   <li>{@code \} becomes {@code \\}  — must be first, or step 2's output would be re-escaped</li>
 *   <li>each occurrence of the separator becomes {@code \} + separator</li>
 * </ol>
 *
 * <p>Decoding splits only on separators not preceded by an odd number of backslashes. The result
 * is a genuine bijection between segment lists and keys:</p>
 *
 * <pre>
 *   ["user_id"]      -&gt;  "user\_id"
 *   ["user", "id"]   -&gt;  "user_id"
 * </pre>
 *
 * <p>Decoding is a single left-to-right character scan with no backtracking and no regex, so it
 * is O(key length) — which is what removes the DoS as a side effect of removing the ambiguity.</p>
 *
 * <h2>Compatibility</h2>
 *
 * <p>This <b>changes emitted key names</b> for any field whose name contains the separator, and
 * is therefore a breaking change. Documents whose field names contain no separator character are
 * encoded byte-identically to before, so the common case is unaffected.</p>
 *
 * <p>{@link #encodeLegacy} preserves the old behaviour for callers that must read data written by
 * an earlier version. It is deliberately marked deprecated: it is lossy by construction.</p>
 *
 * <p>Instances are immutable and thread-safe.</p>
 */
public final class FlattenedPath {

    private static final char ESCAPE = '\\';

    private final List<String> segments;

    private FlattenedPath(List<String> segments) {
        this.segments = Collections.unmodifiableList(segments);
    }

    /** Creates a path from already-separated segments. */
    public static FlattenedPath of(List<String> segments) {
        Objects.requireNonNull(segments, "segments");
        if (segments.isEmpty()) {
            throw new IllegalArgumentException("A flattened path needs at least one segment");
        }
        for (String s : segments) {
            Objects.requireNonNull(s, "path segments must not be null");
        }
        return new FlattenedPath(new ArrayList<>(segments));
    }

    /** Convenience overload. */
    public static FlattenedPath of(String... segments) {
        return of(Arrays.asList(segments));
    }

    /** Parses an encoded key back into its segments. Inverse of {@link #encode}. */
    public static FlattenedPath decode(String key, String separator) {
        return new FlattenedPath(decodeSegments(key, separator));
    }

    /** The individual field names, unescaped. Immutable. */
    public List<String> segments() {
        return segments;
    }

    public int depth() {
        return segments.size();
    }

    /** Returns a new path with {@code child} appended. This instance is unchanged. */
    public FlattenedPath child(String child) {
        Objects.requireNonNull(child, "child");
        List<String> next = new ArrayList<>(segments.size() + 1);
        next.addAll(segments);
        next.add(child);
        return new FlattenedPath(next);
    }

    /** Encodes to a flat key. Inverse of {@link #decode}. */
    public String encode(String separator) {
        return encode(segments, separator);
    }

    // ------------------------------------------------------------------ statics

    /**
     * Encodes segments into a single key such that {@code decode(encode(x)) equals x} for every
     * possible segment list, including segments that contain the separator or a backslash.
     */
    public static String encode(List<String> segments, String separator) {
        requireSeparator(separator);
        if (segments.size() == 1) {
            return escape(segments.get(0), separator);
        }
        // Pre-size to the exact common case (no escapes) to avoid a grow-and-copy.
        int estimate = separator.length() * (segments.size() - 1);
        for (String s : segments) {
            estimate += s.length();
        }
        StringBuilder out = new StringBuilder(estimate);
        for (int i = 0; i < segments.size(); i++) {
            if (i > 0) {
                out.append(separator);
            }
            appendEscaped(out, segments.get(i), separator);
        }
        return out.toString();
    }

    /**
     * Escapes a single segment so it can be appended to a key without introducing a spurious
     * separator.
     *
     * <p>Exists so a recursive flattener can build keys incrementally — appending one escaped
     * segment per level — instead of collecting every segment and calling {@link #encode} at each
     * leaf. The results are identical:</p>
     *
     * <pre>{@code
     *   encode(List.of("a", "b", "c"), "_")
     *     .equals(escapeSegment("a","_") + "_" + escapeSegment("b","_") + "_" + escapeSegment("c","_"))
     * }</pre>
     *
     * <p>Returns the original instance unchanged when nothing needs escaping, which is the common
     * case and keeps the hot path allocation-free.</p>
     */
    public static String escapeSegment(String segment, String separator) {
        Objects.requireNonNull(segment, "segment");
        requireSeparator(separator);
        return escape(segment, separator);
    }

    /**
     * The pre-2.0 encoding: plain concatenation, no escaping.
     *
     * @deprecated Not injective — {@code ["user_id"]} and {@code ["user","id"]} both encode to
     *         {@code "user_id"}, so the result cannot be decoded reliably. Retained only for
     *         reading data written by an earlier version.
     */
    @Deprecated
    public static String encodeLegacy(List<String> segments, String separator) {
        requireSeparator(separator);
        return String.join(separator, segments);
    }

    /**
     * Splits an encoded key on unescaped separators.
     *
     * <p>Single left-to-right scan, no regex, no backtracking: O(key length).</p>
     */
    public static List<String> decodeSegments(String key, String separator) {
        Objects.requireNonNull(key, "key");
        requireSeparator(separator);

        List<String> out = new ArrayList<>();
        StringBuilder current = new StringBuilder(key.length());
        final char sep0 = separator.charAt(0);
        final int sepLen = separator.length();

        int i = 0;
        while (i < key.length()) {
            char c = key.charAt(i);

            if (c == ESCAPE && i + 1 < key.length()) {
                char next = key.charAt(i + 1);
                // The encoder escapes exactly two characters: the escape itself and the
                // separator's first character. Both are consumed one char at a time - note this
                // takes a single character even for multi-character separators, because that is
                // what appendEscaped emits.
                if (next == ESCAPE || next == sep0) {
                    current.append(next);
                    i += 2;
                    continue;
                }
                // A backslash that escapes nothing meaningful is a literal backslash.
                current.append(ESCAPE);
                i++;
                continue;
            }

            if (c == sep0 && key.startsWith(separator, i)) {
                out.add(current.toString());
                current.setLength(0);
                i += sepLen;
                continue;
            }

            current.append(c);
            i++;
        }
        out.add(current.toString());
        return out;
    }

    private static String escape(String segment, String separator) {
        // Fast path: nothing to escape, so hand back the original String with no allocation.
        if (!needsEscaping(segment, separator)) {
            return segment;
        }
        StringBuilder sb = new StringBuilder(segment.length() + 8);
        appendEscaped(sb, segment, separator);
        return sb.toString();
    }

    private static boolean needsEscaping(String segment, String separator) {
        return segment.indexOf(ESCAPE) >= 0 || segment.indexOf(separator.charAt(0)) >= 0;
    }

    /**
     * Escapes every occurrence of the separator's FIRST character, not merely every occurrence of
     * the complete separator string.
     *
     * <p>Escaping only complete separators is subtly wrong for multi-character separators, and a
     * property test caught it: with separator {@code "__"}, the segments {@code ["_", ""]} encode
     * to {@code "_" + "__" + ""} = {@code "___"}, which decodes as {@code ["", "_"]}. Neither
     * segment contains {@code "__"}, so neither was escaped, yet a trailing separator-prefix in
     * one segment combined with the following separator to form a separator match one character
     * early.</p>
     *
     * <p>Escaping the first character removes the class of bug entirely: an unescaped occurrence
     * of {@code sep0} in an encoded key can then only ever be the start of a real separator, so
     * the decoder never has to disambiguate.</p>
     *
     * <p>For the default single-character separator this is exactly equivalent to escaping the
     * complete separator, so the common case is unchanged.</p>
     */
    private static void appendEscaped(StringBuilder out, String segment, String separator) {
        if (!needsEscaping(segment, separator)) {
            out.append(segment);
            return;
        }
        final char sep0 = separator.charAt(0);
        for (int i = 0; i < segment.length(); i++) {
            char c = segment.charAt(i);
            if (c == ESCAPE || c == sep0) {
                out.append(ESCAPE);
            }
            out.append(c);
        }
    }

    private static void requireSeparator(String separator) {
        Objects.requireNonNull(separator, "separator");
        if (separator.isEmpty()) {
            throw new IllegalArgumentException("separator must not be empty");
        }
        if (separator.indexOf(ESCAPE) >= 0) {
            throw new IllegalArgumentException(
                    "separator must not contain a backslash; it is the escape character");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FlattenedPath other)) return false;
        return segments.equals(other.segments);
    }

    @Override
    public int hashCode() {
        return segments.hashCode();
    }

    @Override
    public String toString() {
        return "FlattenedPath" + segments;
    }
}
