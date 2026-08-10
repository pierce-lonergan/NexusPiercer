package io.github.pierce;

import io.github.pierce.path.FlattenedPath;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A deliberately naive, obviously-correct flattener used as a differential-testing oracle.
 *
 * <h2>Why this exists</h2>
 *
 * <p>{@code MapFlattener} is ~1,300 lines carrying circular-reference detection, depth and size
 * limits, four array serialisation formats, naming strategies, path exclusion and JSON string
 * parsing. Its correctness is currently asserted by example-based tests, which is how a
 * non-injective key encoding survived to a published release: no example covered a field name
 * containing the separator.</p>
 *
 * <p>This class is the opposite trade. It is a dozen lines of recursion with no configuration, no
 * limits and no optimisation, and it is slow. That is the point — it is small enough to read and
 * agree is correct by inspection, so it can serve as the oracle that the real implementation is
 * checked against over generated input.</p>
 *
 * <h2>Deliberate scope</h2>
 *
 * <p><b>Scalars, nested maps, empty maps and nulls only.</b> Arrays are excluded because their
 * flattened form is not a path-encoding question — it is a serialisation-format question with
 * four modes, element-type inference and structure-preserving nesting rules, none of which a
 * "obviously correct" reference can encode without becoming as complicated as the thing it
 * checks. Array behaviour is covered by the round-trip property tests instead.</p>
 *
 * <p>Two observed behaviours of the real implementation are reproduced here rather than
 * idealised, because the oracle's job is to detect <em>divergence</em>, not to assert an opinion
 * about what the contract ought to be:</p>
 *
 * <ul>
 *   <li>An empty map flattens to a single entry with a {@code null} value, which makes it
 *       indistinguishable from an actual null on the way back. That is a real fidelity gap and is
 *       recorded as such in the tests — but it is current, intended-looking behaviour, so the
 *       oracle matches it.</li>
 *   <li>A null value flattens to its path with a {@code null} value rather than being dropped.</li>
 * </ul>
 */
final class ReferenceFlattener {

    private final String separator;

    ReferenceFlattener(String separator) {
        this.separator = separator;
    }

    /** Flattens by straightforward recursion. No limits, no caching, no cleverness. */
    Map<String, Object> flatten(Map<String, Object> input) {
        Map<String, Object> out = new LinkedHashMap<>();
        walk(input, new ArrayList<>(), out);
        return out;
    }

    private void walk(Map<String, Object> node, List<String> path, Map<String, Object> out) {
        if (node.isEmpty() && !path.isEmpty()) {
            // Matches MapFlattener: an empty map becomes one entry with a null value.
            out.put(FlattenedPath.encode(path, separator), null);
            return;
        }
        for (Map.Entry<String, Object> e : node.entrySet()) {
            List<String> childPath = new ArrayList<>(path);
            childPath.add(e.getKey());
            Object value = e.getValue();

            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> child = (Map<String, Object>) value;
                walk(child, childPath, out);
            } else {
                out.put(FlattenedPath.encode(childPath, separator), value);
            }
        }
    }
}
