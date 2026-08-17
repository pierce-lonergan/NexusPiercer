package io.github.pierce.gates;

import io.github.pierce.AvroReconstructor;
import io.github.pierce.GAvroSchemaFlattener;
import io.github.pierce.MapFlattener;
import org.apache.avro.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The five dead private methods that the deleted SpotBugs blanket used to hide are gone, and
 * the live method that replaced each one is still here.
 *
 * <p>WHY BOTH HALVES. The negative half discharges the five
 * {@code UPM_UNCALLED_PRIVATE_METHOD} findings. The positive half is the more important one:
 * in three of the five cases a real caller WAS removed by a specific commit, which replaced it
 * with a differently-named method at the same decision point. A future agent reading only
 * "delete the dead twin" could delete the live one instead. Each supersessor is therefore
 * pinned by name.
 *
 * <p>VERDICTS, each established from the occurrence count of the symbol in every historical
 * revision of its file, following renames:
 *
 * <table>
 *   <caption>Dead private method verdicts</caption>
 *   <tr><th>method</th><th>verdict</th><th>evidence</th></tr>
 *   <tr><td>AvroReconstructor.reconstructNestedArrayOfRecords</td><td>BORN DEAD</td>
 *       <td>count 0 in every revision before cad816b, 1 (the declaration alone) in cad816b and
 *           every revision since. Never called, in any revision, ever. The live variant
 *           reconstructNestedArrayOfRecordsAtIndex was added by the SAME commit.</td></tr>
 *   <tr><td>MapFlattener.flattenSingleValue</td><td>BORN DEAD</td>
 *       <td>count 1 in all 8 revisions of the file since 5df36e2. Never called. Also strictly
 *           worse than the live path: it never reaches serializeValue, which exists to
 *           Base64-encode a ByteBuffer instead of letting toString() destroy it.</td></tr>
 *   <tr><td>AvroReconstructor.unwrapUnion</td><td>CALLER REPLACED</td>
 *       <td>count 4 (declaration + 3 call sites) through ef625f2, then 1 from cad816b onward.
 *           cad816b rewrote reconstructArrayOfRecords wholesale; the same decisions are made
 *           today by unwrapNullable. Nothing stopped running.</td></tr>
 *   <tr><td>AvroReconstructor.calculateArraySize</td><td>CALLER REPLACED</td>
 *       <td>count 2 (declaration + 1 call) through ef625f2, then 1 from cad816b onward.
 *           ef625f2:1192 {@code calculateArraySize(node, node.arrayFieldValues, elementSchema)}
 *           became {@code determineArraySize(...)}, introduced by the same commit.</td></tr>
 *   <tr><td>GAvroSchemaFlattener.isNullable(Schema)</td><td>CALLER REPLACED</td>
 *       <td>at 5df36e2:317 the caller was {@code boolean nullable = isNullable(currentSchema);}.
 *           Today that line is a COMMENT. node.nullable is computed by analyzeUnion during
 *           traversal and carries ancestor nullability the local check cannot see, because
 *           currentSchema has already been union-unwrapped by that point.</td></tr>
 * </table>
 *
 * <p>NOT one of the five: {@code FlattenedFieldType.isNullable()}, the public no-arg accessor,
 * is live and is asserted present below. Deleting the private {@code isNullable(Schema)} must
 * not take it with it.
 */
@DisplayName("The five dead private methods are gone and their replacements are not")
class NoDeadPrivateMethodsInTheFormerlySuppressedClassesTest {

    private static List<Method> declaredNamed(Class<?> type, String name) {
        List<Method> hits = new ArrayList<>();
        for (Method m : type.getDeclaredMethods()) {
            if (m.getName().equals(name) && !m.isSynthetic()) {
                hits.add(m);
            }
        }
        return hits;
    }

    private static void assertNoDeclaredMethodNamed(Class<?> type, String name, String verdict) {
        List<Method> hits = declaredNamed(type, name);
        assertTrue(hits.isEmpty(),
                "Expected no declared method named " + name + " on " + type.getName()
                        + " (" + verdict + ") but found " + hits);
    }

    private static void assertDeclaresMethodNamed(Class<?> type, String name, String why) {
        assertTrue(!declaredNamed(type, name).isEmpty(),
                "Expected " + type.getName() + " to still declare " + name + ". " + why);
    }

    // ===================== the five are gone =====================

    @Nested
    @DisplayName("Dead methods deleted")
    class Deleted {

        @Test
        @DisplayName("AvroReconstructor.reconstructNestedArrayOfRecords - born dead, never called in any revision")
        void reconstructNestedArrayOfRecordsIsGone() {
            assertNoDeclaredMethodNamed(AvroReconstructor.class, "reconstructNestedArrayOfRecords",
                    "BORN DEAD: introduced by cad816b with no caller and never acquired one. "
                            + "Restoring a call would be shipping never-executed code under cover "
                            + "of a bug fix, not repairing a regression.");
        }

        @Test
        @DisplayName("AvroReconstructor.unwrapUnion - caller replaced by unwrapNullable at cad816b")
        void unwrapUnionIsGone() {
            assertNoDeclaredMethodNamed(AvroReconstructor.class, "unwrapUnion",
                    "CALLER REPLACED: its 3 call sites became unwrapNullable when cad816b "
                            + "rewrote reconstructArrayOfRecords.");
        }

        @Test
        @DisplayName("AvroReconstructor.calculateArraySize - caller replaced by determineArraySize at cad816b")
        void calculateArraySizeIsGone() {
            assertNoDeclaredMethodNamed(AvroReconstructor.class, "calculateArraySize",
                    "CALLER REPLACED: its single call site became determineArraySize, "
                            + "introduced by the same commit.");
        }

        @Test
        @DisplayName("GAvroSchemaFlattener.isNullable(Schema) - caller replaced by node.nullable in the first commit")
        void privateIsNullableSchemaIsGone() {
            List<Method> singleSchemaArg = new ArrayList<>();
            for (Method m : declaredNamed(GAvroSchemaFlattener.class, "isNullable")) {
                if (m.getParameterCount() == 1 && m.getParameterTypes()[0] == Schema.class) {
                    singleSchemaArg.add(m);
                }
            }
            assertTrue(singleSchemaArg.isEmpty(),
                    "Expected no declared isNullable(Schema) on GAvroSchemaFlattener "
                            + "(CALLER REPLACED: 5df36e2:317's call became node.nullable, which "
                            + "carries ancestor nullability a local check cannot see) but found "
                            + singleSchemaArg);
        }

        @Test
        @DisplayName("MapFlattener.flattenSingleValue - born dead, and would reintroduce ByteBuffer data loss")
        void flattenSingleValueIsGone() {
            assertNoDeclaredMethodNamed(MapFlattener.class, "flattenSingleValue",
                    "BORN DEAD: count 1 in all 8 revisions, never called. It also bypasses "
                            + "serializeValue, so wiring it up would stringify a ByteBuffer "
                            + "instead of Base64-encoding it.");
        }
    }

    // ===================== the live replacements are not =====================

    @Nested
    @DisplayName("Live replacements still present - do not delete the twin that works")
    class Supersessors {

        @Test
        @DisplayName("determineArraySize replaced calculateArraySize")
        void determineArraySizeSurvives() {
            assertDeclaresMethodNamed(AvroReconstructor.class, "determineArraySize",
                    "This is the live sizing routine that calculateArraySize was replaced by at "
                            + "cad816b. NOTE, and this is a tracked gap rather than a defect in "
                            + "this test: determineArraySize has no BRACKET_LIST / "
                            + "COMMA_SEPARATED / PIPE_SEPARATED fallback and calculateArraySize "
                            + "did, so array-of-records sizing under those formats collapses to "
                            + "1. That is a behaviour change and does not belong in a "
                            + "static-analysis cleanup.");
        }

        @Test
        @DisplayName("unwrapNullable replaced unwrapUnion")
        void unwrapNullableSurvives() {
            assertDeclaresMethodNamed(AvroReconstructor.class, "unwrapNullable",
                    "This is the live branch-unwrapper that unwrapUnion was replaced by. NOTE: "
                            + "unwrapNullable returns the union UNCHANGED unless it has exactly "
                            + "two branches, where unwrapUnion returned the first non-null branch "
                            + "at any arity, so the substitution was lossy for 3+ branch unions. "
                            + "Restoring unwrapUnion is NOT the fix - 'first non-null branch' is "
                            + "a different wrong answer, and the class already owns a real branch "
                            + "resolver in reconstructUnionValue.");
        }

        @Test
        @DisplayName("reconstructNestedArrayOfRecordsAtIndex is the live variant")
        void atIndexVariantSurvives() {
            assertDeclaresMethodNamed(AvroReconstructor.class,
                    "reconstructNestedArrayOfRecordsAtIndex",
                    "Added by the same commit as the dead twin and called from two sites. This "
                            + "assertion exists so a future agent deleting 'the dead one' by name "
                            + "similarity deletes neither by accident.");
        }

        @Test
        @DisplayName("MapFlattener.serializeValue and stringifyObject are the live value paths")
        void mapFlattenerLivePathsSurvive() {
            assertDeclaresMethodNamed(MapFlattener.class, "serializeValue",
                    "serializeValue is why deleting flattenSingleValue is safe: it is the path "
                            + "that Base64-encodes a ByteBuffer rather than destroying it.");
            assertDeclaresMethodNamed(MapFlattener.class, "stringifyObject",
                    "The live stringification path flattenSingleValue duplicated badly.");
        }

        @Test
        @DisplayName("the PUBLIC FlattenedFieldType.isNullable() accessor is untouched")
        void publicAccessorSurvives() {
            Class<?> fieldType = Arrays.stream(GAvroSchemaFlattener.class.getDeclaredClasses())
                    .filter(c -> c.getSimpleName().equals("FlattenedFieldType"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError(
                            "GAvroSchemaFlattener.FlattenedFieldType is gone. Deleting the "
                                    + "private isNullable(Schema) must not touch the nested type "
                                    + "that carries the public no-arg accessor."));

            List<Method> noArg = new ArrayList<>();
            for (Method m : declaredNamed(fieldType, "isNullable")) {
                if (m.getParameterCount() == 0) {
                    noArg.add(m);
                }
            }
            assertTrue(!noArg.isEmpty(),
                    "FlattenedFieldType.isNullable() is the PUBLIC accessor and is live (called "
                            + "from the strict-type-enforcement branch). It shares a name with "
                            + "the deleted private isNullable(Schema) and must survive it.");
        }
    }
}
