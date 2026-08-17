package io.github.pierce.gates;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.pierce.AvroReconstructor;
import io.github.pierce.GAvroSchemaFlattener;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The three {@code SIC_INNER_SHOULD_BE_STATIC_ANON} findings, tested directly rather than by
 * invoking SpotBugs.
 *
 * <p>An anonymous class created inside an INSTANCE method captures its enclosing instance in a
 * synthetic {@code this$0} field. Created in a static field initializer it does not. Three
 * {@code new TypeReference<List<Object>>() {}} sites sat in instance methods —
 * AvroReconstructor.parseNestedArrayStructure, AvroReconstructor.deserializeArray and
 * GAvroSchemaFlattener.convertSerializedArray — compiling to AvroReconstructor$1,
 * AvroReconstructor$2 and GAvroSchemaFlattener$1, each holding a needless reference to the
 * whole reconstructor and allocating a fresh TypeReference on every parse. They were hoisted
 * into shared {@code private static final} constants.
 *
 * <p>POSITIVE CONTROL, and it is the reason this test is not merely "assert zero anonymous
 * classes". A FOURTH identical expression exists at AvroReconstructor:923, inside
 * {@code deserializeArrayStatic} in the STATIC nested class PathNode. It compiles to
 * {@code AvroReconstructor$PathNode$1} with no enclosing instance and SpotBugs correctly did
 * NOT report it. That absence is the control on the whole finding set: a test that also
 * flagged it would be testing the wrong property (anonymity) instead of the right one
 * (enclosing-instance capture). It is asserted to PASS both before and after.
 */
@DisplayName("No anonymous class in the reconstructors captures an enclosing instance")
class NoAnonymousClassCapturesAnEnclosingInstanceTest {

    /**
     * The synthetic field javac emits on an inner class that captures its enclosing instance.
     * Anonymous classes in a static context do not get one.
     */
    private static final String ENCLOSING_INSTANCE_FIELD_PREFIX = "this$";

    private static List<String> anonymousMembersCapturingEnclosingInstance(Class<?> host) {
        List<String> offenders = new ArrayList<>();
        for (Class<?> member : host.getNestMembers()) {
            if (!member.isAnonymousClass()) {
                continue;
            }
            for (Field f : member.getDeclaredFields()) {
                if (f.isSynthetic() && f.getName().startsWith(ENCLOSING_INSTANCE_FIELD_PREFIX)) {
                    offenders.add(member.getName() + " holds " + f.getName()
                            + " of type " + f.getType().getName());
                }
            }
        }
        return offenders;
    }

    private static List<Field> staticFinalTypeReferenceFields(Class<?> host) {
        List<Field> out = new ArrayList<>();
        for (Field f : host.getDeclaredFields()) {
            if (TypeReference.class.isAssignableFrom(f.getType())
                    && Modifier.isStatic(f.getModifiers())
                    && Modifier.isFinal(f.getModifiers())) {
                out.add(f);
            }
        }
        return out;
    }

    @Nested
    @DisplayName("Enclosing-instance capture")
    class Capture {

        @Test
        @DisplayName("AvroReconstructor declares no anonymous class holding this$0")
        void avroReconstructorHasNoCapturingAnonymousClass() {
            List<String> offenders =
                    anonymousMembersCapturingEnclosingInstance(AvroReconstructor.class);
            assertTrue(offenders.isEmpty(),
                    "SIC_INNER_SHOULD_BE_STATIC_ANON. An anonymous class created in an instance "
                            + "method pins the whole enclosing object and re-allocates per call. "
                            + "Hoist it into a private static final constant. Offenders: "
                            + offenders);
        }

        @Test
        @DisplayName("GAvroSchemaFlattener declares no anonymous class holding this$0")
        void gAvroSchemaFlattenerHasNoCapturingAnonymousClass() {
            List<String> offenders =
                    anonymousMembersCapturingEnclosingInstance(GAvroSchemaFlattener.class);
            assertTrue(offenders.isEmpty(),
                    "SIC_INNER_SHOULD_BE_STATIC_ANON in convertSerializedArray. Offenders: "
                            + offenders);
        }
    }

    @Nested
    @DisplayName("The hoisted constants exist and are shared")
    class HoistedConstants {

        @Test
        @DisplayName("AvroReconstructor declares a private static final TypeReference")
        void avroReconstructorDeclaresTheConstant() {
            List<Field> fields = staticFinalTypeReferenceFields(AvroReconstructor.class);
            assertTrue(!fields.isEmpty(),
                    "Expected a private static final TypeReference constant on "
                            + "AvroReconstructor. Without it the three parse sites are each "
                            + "allocating a fresh TypeReference on the per-record path, which is "
                            + "the performance half of the SIC finding.");
        }

        @Test
        @DisplayName("GAvroSchemaFlattener declares a private static final TypeReference")
        void gAvroSchemaFlattenerDeclaresTheConstant() {
            List<Field> fields = staticFinalTypeReferenceFields(GAvroSchemaFlattener.class);
            assertTrue(!fields.isEmpty(),
                    "Expected a private static final TypeReference constant on "
                            + "GAvroSchemaFlattener.");
        }
    }

    @Nested
    @DisplayName("Positive control - the static-context site was never a finding and must stay clean")
    class PositiveControl {

        @Test
        @DisplayName("an anonymous class in a static nested class's static method has no this$0, before or after")
        void staticContextAnonymousClassIsNotCollateral() {
            // PathNode is a STATIC nested class; anything anonymous declared inside its static
            // method carries no enclosing instance. If a future edit makes PathNode non-static,
            // or moves that expression into an instance method, this leg goes red - which is the
            // point. It is asserted over the whole nest, so it holds whether or not the
            // expression at line 923 is itself hoisted.
            for (Class<?> member : AvroReconstructor.class.getNestMembers()) {
                if (!member.getName().contains("PathNode")) {
                    continue;
                }
                for (Field f : member.getDeclaredFields()) {
                    assertTrue(!(f.isSynthetic()
                                    && f.getName().startsWith(ENCLOSING_INSTANCE_FIELD_PREFIX)),
                            member.getName() + " gained an enclosing-instance field (" + f.getName()
                                    + "). PathNode is static on purpose; making it an inner class "
                                    + "would give every anonymous class inside it a this$0 and "
                                    + "turn a correct non-finding into a real one.");
                }
            }
        }
    }
}
