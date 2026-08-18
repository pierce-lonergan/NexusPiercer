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
 * in two of the five cases a real caller WAS removed by a specific commit, which replaced it
 * with a differently-named method at the same decision point. A future agent reading only
 * "delete the dead twin" could delete the live one instead. Each supersessor is therefore
 * pinned by name.
 *
 * <p>CORRECTED 2026-08-17. This table and the messages below previously said THREE methods had
 * their callers replaced, counting {@code unwrapUnion} among them. That was wrong, and it is
 * corrected in its row. Re-measured per revision with {@code git grep -n unwrapUnion <rev>}:
 * the method had NO declaration in any revision in which a call to it existed, so it never
 * coexisted with a caller and cannot have had one "replaced". The deletion verdict is unchanged
 * and is in fact stronger than the original argument for it.
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
 *   <tr><td>AvroReconstructor.unwrapUnion</td><td>BORN DEAD, most strongly of the three</td>
 *       <td>count 4 through ef625f2 and all four are CALLS - there is no declaration in ANY
 *           revision where a call exists. The declaration first appears at cad816b, the same
 *           commit that deleted all four calls, and never acquired a caller. No superclass, and
 *           the repo-wide count equals the per-file count at every revision, so it was not
 *           inherited; the file was Groovy-compiled then, so the calls compiled and would have
 *           thrown MissingMethodException if reached. It never executed, in any revision.</td></tr>
 *   <tr><td>AvroReconstructor.calculateArraySize</td><td>CALLER REPLACED</td>
 *       <td>count 2 (declaration at ef625f2:2018 + 1 call at ef625f2:1192) through ef625f2, then
 *           1 from cad816b onward. ef625f2:1192
 *           {@code calculateArraySize(node, node.arrayFieldValues, elementSchema)}
 *           became {@code determineArraySize(...)}, introduced by the same commit.</td></tr>
 *   <tr><td>GAvroSchemaFlattener.isNullable(Schema)</td><td>CALLER REPLACED</td>
 *       <td>the caller {@code boolean nullable = isNullable(currentSchema);} was still LIVE at
 *           5df36e2:317 and at 1ba32af:317, and became a COMMENT at 4a49041 - the fourth
 *           revision of the file, not the first. That the swap was deliberate is visible in the
 *           4a49041 diff, which adds a {@code nullable} field to SchemaNode and threads it
 *           through the constructor rather than merely dropping the call. node.nullable is
 *           computed by analyzeUnion during traversal and carries ancestor nullability the local
 *           check cannot see, because currentSchema has already been union-unwrapped by that
 *           point.</td></tr>
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
        @DisplayName("AvroReconstructor.unwrapUnion - born dead: four calls to a method that was never declared")
        void unwrapUnionIsGone() {
            assertNoDeclaredMethodNamed(AvroReconstructor.class, "unwrapUnion",
                    "BORN DEAD, and in the strongest sense of the three born-dead verdicts here. "
                            + "Through ef625f2 the symbol occurs FOUR times and every one is a "
                            + "CALL: there is no declaration in any revision in which a call "
                            + "exists. The declaration first appears at cad816b, the same commit "
                            + "that deleted all four calls, and it never acquired a caller. The "
                            + "file was Groovy-compiled then, so those calls compiled and would "
                            + "have thrown MissingMethodException had they been reached. "
                            + "This test previously said 'its 3 call sites became unwrapNullable' "
                            + "- false twice: there were four, and unwrapNullable's own count rose "
                            + "only 7 -> 8 at cad816b. Do not restore it to recover behaviour; it "
                            + "never had any.");
        }

        @Test
        @DisplayName("AvroReconstructor.calculateArraySize - caller replaced by determineArraySize at cad816b")
        void calculateArraySizeIsGone() {
            assertNoDeclaredMethodNamed(AvroReconstructor.class, "calculateArraySize",
                    "CALLER REPLACED: its single call site became determineArraySize, "
                            + "introduced by the same commit.");
        }

        @Test
        @DisplayName("GAvroSchemaFlattener.isNullable(Schema) - caller replaced by node.nullable at 4a49041")
        void privateIsNullableSchemaIsGone() {
            List<Method> singleSchemaArg = new ArrayList<>();
            for (Method m : declaredNamed(GAvroSchemaFlattener.class, "isNullable")) {
                if (m.getParameterCount() == 1 && m.getParameterTypes()[0] == Schema.class) {
                    singleSchemaArg.add(m);
                }
            }
            assertTrue(singleSchemaArg.isEmpty(),
                    "Expected no declared isNullable(Schema) on GAvroSchemaFlattener "
                            + "(CALLER REPLACED at 4a49041, NOT at 5df36e2 as this message "
                            + "previously said: the call was still live at 5df36e2:317 and at "
                            + "1ba32af:317, and 4a49041 turned it into a comment. The 4a49041 "
                            + "diff adds a nullable field to SchemaNode and threads it through "
                            + "the constructor, so the replacement was deliberate rather than "
                            + "incidental; node.nullable carries ancestor nullability a local "
                            + "check cannot see) but found "
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
        @DisplayName("collectElementCounts + agreedElementCount replaced determineArraySize in 2.1.0")
        void arrayOfRecordsSizingRoutineSurvives() {
            // THE ASSERTION MOVED BECAUSE THE METHOD DID, DELIBERATELY, IN THE SAME COMMIT.
            // determineArraySize was the supersessor of calculateArraySize and is now itself
            // superseded, so this gate names the two methods that replaced it rather than
            // silently passing on a name nobody calls any more.
            //
            // THE OLD MESSAGE HERE WAS FACTUALLY WRONG AND IS CORRECTED RATHER THAN DELETED. It
            // said determineArraySize "has no BRACKET_LIST / COMMA_SEPARATED / PIPE_SEPARATED
            // fallback and calculateArraySize did, so array-of-records sizing under those formats
            // collapses to 1", inheriting BL-013's filed cause. MEASURED: no collapse occurred
            // under any format for an element with a scalar field at its root, because the column
            // had already been split upstream - porting those format branches back would have
            // changed no output at all. The collapse was real but FORMAT-INDEPENDENT: it fired
            // when every element field lived inside a nested record, so nothing was counted and a
            // trailing `maxSize > 0 ? maxSize : 1` fabricated a size of 1, under the JSON default
            // too. That is what collectElementCounts (schema-guided, per column) and
            // agreedElementCount (refuses to pick a winner when columns disagree) replace.
            assertDeclaresMethodNamed(AvroReconstructor.class, "collectElementCounts",
                    "The schema-guided element counter. It walks the ELEMENT SCHEMA rather than "
                            + "the PathNode tree, which is what lets it see fields that live only "
                            + "inside a nested record.");
            assertDeclaresMethodNamed(AvroReconstructor.class, "agreedElementCount",
                    "The half that refuses to guess. determineArraySize took Math.max over the "
                            + "columns and let handleMissingField pad the short ones while a "
                            + "Math.min clamp duplicated the last nested value; this throws "
                            + "ArrayCardinalityException instead.");
            assertNoDeclaredMethodNamed(AvroReconstructor.class, "determineArraySize",
                    "SUPERSEDED in 2.1.0 by collectElementCounts + agreedElementCount. Left "
                            + "declared it would be a dead private method and PMD would count it; "
                            + "this repository's ratchet may only go down.");
        }

        @Test
        @DisplayName("unwrapNullable is the live branch-unwrapper")
        void unwrapNullableSurvives() {
            assertDeclaresMethodNamed(AvroReconstructor.class, "unwrapNullable",
                    "This is the live branch-unwrapper at the decision point where the dead "
                            + "unwrapUnion was declared. NOTE, and this message previously stated "
                            + "it wrongly: unwrapNullable returns the union UNCHANGED unless it "
                            + "has exactly two branches, so a 3+ branch union inside an array "
                            + "element still matches neither the RECORD nor the ARRAY test. Its "
                            + "[null,T] scope is DELIBERATE and unchanged: seven call sites rely "
                            + "on it, and widening it to arity 3+ would re-point convertPrimitive, "
                            + "handleMissingField and tryReconstructArrayFromFields at a "
                            + "first-non-null-branch guess all at once. BL-014 IS FIXED, and not "
                            + "here: reconstructArrayOfRecords gained a real UNION arm "
                            + "(reconstructArrayElementUnion) that reuses reconstructUnionValue's "
                            + "selection rule with the element index in hand. This message "
                            + "previously described that gap in the present tense; nothing forced "
                            + "the update, because the assertion only checks the method is "
                            + "declared - which is exactly why a stale message survives here. "
                            + "unwrapUnion is still NOT the fix and must not come back.");
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
