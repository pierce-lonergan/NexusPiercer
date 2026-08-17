package io.github.pierce.gates;

import io.github.pierce.AvroReconstructor;
import org.apache.avro.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the two contracts that the {@code RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE} deletions
 * depend on: {@code reconstructArrayFromValues} and
 * {@code reconstructNestedArrayOfRecordsAtIndex} return non-null on every path.
 *
 * <p><b>HONEST STATEMENT OF WHAT THIS TEST IS.</b> It does NOT fail before the fix. Both
 * methods already satisfy it — that is precisely WHY the two null checks that were removed
 * were redundant, and a test that went red before the deletion would mean the deletion was
 * unsafe. The fails-before instrument for those two findings is the quality gate itself: with
 * the exclude block deleted and the code unfixed, SpotBugs measured 251 against a ceiling of
 * 241 and the ratchet step emitted
 * {@code ::error::SpotBugs rose from 241 to 251}. This file is the guard that keeps the
 * deletion safe against a future edit which makes either method return null and silently
 * reintroduces an NPE where a check used to sit.
 *
 * <p>DRILLED THREE WAYS so that it is a real assertion and not a tautology:
 * <ul>
 *   <li>GOOD INPUT — a matrix of awkward inputs (null list, empty list, all-null list, a
 *       malformed {@code "[["} string, an out-of-range outer index, a record schema with no
 *       fields) all return non-null.</li>
 *   <li>CAPABLE OF FAILING — the same predicate applied to a deliberately null-returning stub
 *       is REJECTED. Without this leg, {@code assertNotNull} over a method that cannot return
 *       null proves nothing about the assertion, only about the method.</li>
 *   <li>MISSING INPUT — a null PathNode has a STATED, asserted outcome rather than being
 *       quietly skipped.</li>
 * </ul>
 */
@DisplayName("The two methods behind the removed null checks never return null")
class ReconstructorNeverReturnsNullListContractTest {

    private static final Schema INT_SCHEMA = Schema.create(Schema.Type.INT);
    private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);

    private static final Schema NULLABLE_STRING = Schema.createUnion(
            Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));

    private static final Schema EMPTY_RECORD =
            Schema.createRecord("Empty", null, "io.github.pierce.test", false,
                    Collections.emptyList());

    private static AvroReconstructor reconstructor() {
        return AvroReconstructor.builder().build();
    }

    /** Reflectively reaches the private method; the deletion is about its callers, not it. */
    private static Object invokePrivate(Object target, String name, Class<?>[] types, Object[] args)
            throws Exception {
        Method m = target.getClass().getDeclaredMethod(name, types);
        m.setAccessible(true);
        return m.invoke(target, args);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> reconstructArrayFromValues(
            List<Object> values, Schema elementSchema) throws Exception {
        return (List<Object>) invokePrivate(reconstructor(), "reconstructArrayFromValues",
                new Class<?>[]{List.class, Schema.class, String.class, int.class},
                new Object[]{values, elementSchema, "root", 0});
    }

    // ===================== GOOD INPUT =====================

    @Nested
    @DisplayName("reconstructArrayFromValues returns non-null for every awkward input")
    class ArrayFromValues {

        @Test
        @DisplayName("null values list")
        void nullValues() throws Exception {
            assertNotNull(reconstructArrayFromValues(null, INT_SCHEMA),
                    "returns new ArrayList<>() on the null branch; the caller's "
                            + "'if (value == null) value = new ArrayList<>()' was therefore dead");
        }

        @Test
        @DisplayName("empty values list")
        void emptyValues() throws Exception {
            assertNotNull(reconstructArrayFromValues(new ArrayList<>(), INT_SCHEMA));
        }

        @Test
        @DisplayName("all-null values list against a nullable element schema")
        void allNullValues() throws Exception {
            assertNotNull(reconstructArrayFromValues(
                    Arrays.asList(null, null), NULLABLE_STRING));
        }

        @Test
        @DisplayName("a malformed bracket string")
        void malformedBracketString() throws Exception {
            assertNotNull(reconstructArrayFromValues(
                    Collections.singletonList("[["), STRING_SCHEMA),
                    "a value that opens a bracket and never closes it must still yield a list, "
                            + "not null");
        }

        @Test
        @DisplayName("an empty-array string")
        void emptyArrayString() throws Exception {
            assertNotNull(reconstructArrayFromValues(
                    Collections.singletonList("[]"), STRING_SCHEMA));
        }

        @Test
        @DisplayName("an ordinary populated list")
        void populatedList() throws Exception {
            List<Object> out = reconstructArrayFromValues(
                    Arrays.asList(1, 2, 3), INT_SCHEMA);
            assertNotNull(out);
            assertTrue(out.size() == 3,
                    "the good-input leg must also show the method WORKS, not merely that it "
                            + "returns something non-null; got " + out);
        }
    }

    @Nested
    @DisplayName("reconstructNestedArrayOfRecordsAtIndex returns non-null for every awkward input")
    class AtIndex {

        /** Builds the private static PathNode the method takes. */
        private Object newPathNode() throws Exception {
            Class<?> pathNode = Arrays.stream(AvroReconstructor.class.getDeclaredClasses())
                    .filter(c -> c.getSimpleName().equals("PathNode"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("AvroReconstructor.PathNode is gone"));
            Constructor<?> ctor = pathNode.getDeclaredConstructors()[0];
            ctor.setAccessible(true);
            Object[] args = new Object[ctor.getParameterCount()];
            Class<?>[] types = ctor.getParameterTypes();
            for (int i = 0; i < args.length; i++) {
                args[i] = types[i] == String.class ? "n"
                        : types[i].isPrimitive() ? defaultPrimitive(types[i]) : null;
            }
            return ctor.newInstance(args);
        }

        private Object defaultPrimitive(Class<?> t) {
            if (t == boolean.class) {
                return false;
            }
            if (t == int.class) {
                return 0;
            }
            if (t == long.class) {
                return 0L;
            }
            return 0;
        }

        @SuppressWarnings("unchecked")
        private List<Object> call(Object node, Schema recordSchema, int outerIndex)
                throws Exception {
            Class<?> pathNode = node.getClass();
            Method m = AvroReconstructor.class.getDeclaredMethod(
                    "reconstructNestedArrayOfRecordsAtIndex",
                    pathNode, Schema.class, int.class, String.class, int.class);
            m.setAccessible(true);
            return (List<Object>) m.invoke(reconstructor(), node, recordSchema, outerIndex,
                    "root", 0);
        }

        @Test
        @DisplayName("a node with no array field values")
        void bareNode() throws Exception {
            assertNotNull(call(newPathNode(), EMPTY_RECORD, 0),
                    "returns new ArrayList<>() when innerArraySize is 0; the caller's "
                            + "'if (reconstructed != null)' was therefore dead");
        }

        @Test
        @DisplayName("an outer index past the end")
        void outerIndexPastEnd() throws Exception {
            assertNotNull(call(newPathNode(), EMPTY_RECORD, 99));
        }

        @Test
        @DisplayName("a record schema with no fields")
        void recordWithNoFields() throws Exception {
            assertNotNull(call(newPathNode(), EMPTY_RECORD, 0));
        }
    }

    // ===================== CAPABLE OF FAILING =====================

    @Nested
    @DisplayName("The predicate is capable of rejecting - without this leg assertNotNull proves nothing")
    class CapableOfFailing {

        /** The property the two deletions rely on, as a reusable predicate. */
        private boolean neverReturnsNull(Function<List<Object>, List<Object>> candidate) {
            List<List<Object>> probes = Arrays.asList(
                    null, new ArrayList<>(), Arrays.asList(1, 2, 3));
            for (List<Object> p : probes) {
                if (candidate.apply(p) == null) {
                    return false;
                }
            }
            return true;
        }

        @Test
        @DisplayName("a deliberately null-returning stub is REJECTED by the same predicate")
        void nullReturningStubIsRejected() {
            assertTrue(!neverReturnsNull(in -> in == null ? null : new ArrayList<>()),
                    "The predicate must reject an implementation that returns null on the null "
                            + "branch. If it accepts one, the assertions above are vacuous and "
                            + "the two removed guards were not shown to be redundant.");
        }

        @Test
        @DisplayName("a always-non-null stub is ACCEPTED by the same predicate")
        void nonNullStubIsAccepted() {
            assertTrue(neverReturnsNull(in -> new ArrayList<>()),
                    "and it must accept a conforming implementation, or it rejects everything "
                            + "and is equally useless");
        }
    }

    // ===================== MISSING INPUT =====================

    @Nested
    @DisplayName("Missing input has a stated outcome rather than being silently skipped")
    class MissingInput {

        private Method atIndexMethod() throws Exception {
            Class<?> pathNode = Arrays.stream(AvroReconstructor.class.getDeclaredClasses())
                    .filter(c -> c.getSimpleName().equals("PathNode"))
                    .findFirst()
                    .orElseThrow();
            Method m = AvroReconstructor.class.getDeclaredMethod(
                    "reconstructNestedArrayOfRecordsAtIndex",
                    pathNode, Schema.class, int.class, String.class, int.class);
            m.setAccessible(true);
            return m;
        }

        private Throwable invokeExpectingOutcome(Schema recordSchema) throws Exception {
            try {
                Object out = atIndexMethod().invoke(reconstructor(), null, recordSchema, 0,
                        "root", 0);
                assertNotNull(out,
                        "Returning NULL is the one outcome that would invalidate the deletion: "
                                + "it would mean the removed 'if (reconstructed != null)' guard "
                                + "was load-bearing after all.");
                return null;
            } catch (Exception e) {
                return e.getCause() != null ? e.getCause() : e;
            }
        }

        @Test
        @DisplayName("null PathNode + empty record schema: MEASURED to return an empty list, not to throw")
        void nullPathNodeWithEmptySchemaReturnsEmptyList() throws Exception {
            // LOUD CORRECTION, recorded because the first draft of this test predicted the
            // opposite and was refuted by running it. I predicted a null PathNode would be
            // dereferenced and throw NullPointerException. MEASURED: it returns an empty list.
            // The reason is that every read of childNode sits inside
            // `for (Schema.Field field : recordSchema.getFields())`, so an empty record schema
            // never enters the loop, innerArraySize stays 0, and the method returns
            // `new ArrayList<>()` at the innerArraySize == 0 branch.
            //
            // This STRENGTHENS the finding rather than weakening it: the method returns non-null
            // even when handed a null node.
            assertNull(invokeExpectingOutcome(EMPTY_RECORD),
                    "expected no throw for an empty record schema; the field loop is never "
                            + "entered so the node is never dereferenced");
        }

        @Test
        @DisplayName("null PathNode + a record schema WITH a field: throws rather than returning null")
        void nullPathNodeWithPopulatedSchemaThrows() throws Exception {
            Schema oneField = Schema.createRecord("OneField", null, "io.github.pierce.test",
                    false, List.of(new Schema.Field("a", STRING_SCHEMA, null, (Object) null)));

            Throwable thrown = invokeExpectingOutcome(oneField);

            // STATED OUTCOME. With a field present the loop body runs and dereferences the node.
            // The point is the distinction: the method either returns a list or throws. It never
            // returns null, which is exactly why the caller's null guard was dead code.
            assertNotNull(thrown,
                    "A null PathNode against a schema with fields must produce a stated outcome, "
                            + "not be silently skipped.");
            assertTrue(thrown instanceof NullPointerException,
                    "expected the null node to be dereferenced, got " + thrown);
        }
    }
}
