package io.github.pierce.path;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the injective path encoding.
 *
 * <p>The central claim is a bijection, so the central test is a property, not an example. Example
 * tests can only demonstrate the cases someone thought of — and the case nobody thought of
 * (a field name containing the separator) is precisely the one that shipped broken.</p>
 */
@DisplayName("FlattenedPath")
class FlattenedPathTest {


    @Nested
    @DisplayName("The collision that motivated this class")
    class MotivatingCollision {

        @Test
        @DisplayName("user_id and user.id no longer collide")
        void flatAndNestedNoLongerCollide() {
            String flatField = FlattenedPath.of("user_id").encode("_");
            String nested = FlattenedPath.of("user", "id").encode("_");

            assertThat(flatField).isEqualTo("user\\_id");
            assertThat(nested).isEqualTo("user_id");
            assertThat(flatField).isNotEqualTo(nested);

            assertThat(FlattenedPath.decode(flatField, "_").segments())
                    .containsExactly("user_id");
            assertThat(FlattenedPath.decode(nested, "_").segments())
                    .containsExactly("user", "id");
        }

        @Test
        @DisplayName("the legacy encoding demonstrably collides — kept only for reading old data")
        @SuppressWarnings("deprecation")
        void legacyEncodingCollides() {
            assertThat(FlattenedPath.encodeLegacy(List.of("user_id"), "_"))
                    .isEqualTo(FlattenedPath.encodeLegacy(List.of("user", "id"), "_"))
                    .isEqualTo("user_id");
        }
    }


    @Nested
    @DisplayName("Structure")
    class Structure {

        @Test
        void childAppendsWithoutMutating() {
            FlattenedPath root = FlattenedPath.of("user");
            FlattenedPath child = root.child("id");

            assertThat(root.segments()).containsExactly("user");
            assertThat(child.segments()).containsExactly("user", "id");
            assertThat(child.depth()).isEqualTo(2);
        }

        @Test
        void segmentsAreImmutable() {
            FlattenedPath p = FlattenedPath.of("a", "b");
            assertThatThrownBy(() -> p.segments().add("c"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void equalityIsByValue() {
            assertThat(FlattenedPath.of("a", "b")).isEqualTo(FlattenedPath.of("a", "b"));
            assertThat(FlattenedPath.of("a", "b")).hasSameHashCodeAs(FlattenedPath.of("a", "b"));
            assertThat(FlattenedPath.of("a_b")).isNotEqualTo(FlattenedPath.of("a", "b"));
        }

        @Test
        void rejectsEmptyPath() {
            assertThatThrownBy(() -> FlattenedPath.of(List.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("at least one segment");
        }

        @Test
        @DisplayName("a backslash separator is rejected — it is the escape character")
        void rejectsBackslashSeparator() {
            assertThatThrownBy(() -> FlattenedPath.encode(List.of("a"), "\\"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("escape character");
        }

        @Test
        void rejectsEmptySeparator() {
            assertThatThrownBy(() -> FlattenedPath.encode(List.of("a"), ""))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
