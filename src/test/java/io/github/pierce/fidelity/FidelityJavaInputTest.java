package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The three-way drill on the {@code javaInput} hatch and the renderer changes it needs.
 *
 * <p>A materializer that silently built {@code null} for a mistyped {@code kind} would let every
 * fixture standing on it record a document full of nulls and pass forever - the fifth control in
 * this harness that appears present and does nothing. So: good input builds the declared runtime
 * class, every synthetic violation throws by name, and every empty input throws.</p>
 */
@DisplayName("The javaInput hatch: good input builds, violations throw, empty throws")
class FidelityJavaInputTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static JsonNode spec(String json) {
        try {
            return JSON.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Object build(String json) {
        return FidelityJavaInput.build(spec(json), "drill");
    }

    @SuppressWarnings("unchecked")
    private static Map<Object, Object> buildMap(String json) {
        return (Map<Object, Object>) build(json);
    }

    @Nested
    @DisplayName("1. good input builds the declared runtime class")
    class GoodInput {

        @Test
        @DisplayName("every scalar kind produces its exact runtime type, not something convertible")
        void scalarKindsBuildTheirRuntimeClass() {
            Map<Object, Object> m = buildMap("""
                {"kind":"map","entries":[
                  {"key":"i","value":{"kind":"int","value":1}},
                  {"key":"l","value":{"kind":"long","value":1}},
                  {"key":"sh","value":{"kind":"short","value":7}},
                  {"key":"by","value":{"kind":"byte","value":3}},
                  {"key":"f","value":{"kind":"float","value":1.5}},
                  {"key":"d","value":{"kind":"double","value":"NaN"}},
                  {"key":"bd","value":{"kind":"bigdecimal","value":"37.7740"}},
                  {"key":"bi","value":{"kind":"bigint","value":"12"}},
                  {"key":"c","value":{"kind":"char","value":"x"}},
                  {"key":"u","value":{"kind":"uuid","value":"00000000-0000-0000-0000-000000000001"}},
                  {"key":"t","value":{"kind":"instant","value":"1970-01-01T00:00:00Z"}},
                  {"key":"e","value":{"kind":"enum","type":"java.time.DayOfWeek","value":"MONDAY"}},
                  {"key":"n","value":{"kind":"null"}}
                ]}""");
            assertThat(m.get("i")).isInstanceOf(Integer.class);
            assertThat(m.get("l")).isInstanceOf(Long.class);
            assertThat(m.get("sh")).isInstanceOf(Short.class);
            assertThat(m.get("by")).isInstanceOf(Byte.class);
            assertThat(m.get("f")).isInstanceOf(Float.class);
            assertThat((Double) m.get("d")).isNaN();
            assertThat(m.get("bd")).hasToString("37.7740");
            assertThat(m.get("bi")).isInstanceOf(java.math.BigInteger.class);
            assertThat(m.get("c")).isEqualTo('x');
            assertThat(m.get("u")).isInstanceOf(UUID.class);
            assertThat(m.get("t")).isInstanceOf(Instant.class);
            assertThat(m.get("e")).isEqualTo(java.time.DayOfWeek.MONDAY);
            assertThat(m).containsKey("n");
            assertThat(m.get("n")).isNull();
        }

        @Test
        @DisplayName("containers nest, and set-ness and array-ness are the real runtime types")
        void containersBuildTheirRuntimeClass() {
            Map<Object, Object> m = buildMap("""
                {"kind":"map","entries":[
                  {"key":"lst","value":{"kind":"list","items":[{"kind":"int","value":1}]}},
                  {"key":"set","value":{"kind":"set","items":[{"kind":"string","value":"p"}]}},
                  {"key":"arr","value":{"kind":"objectArray","items":[{"kind":"int","value":1}]}},
                  {"key":"b","value":{"kind":"bytes","value":[1,2,3]}}
                ]}""");
            assertThat(m.get("lst")).isInstanceOf(List.class);
            assertThat(m.get("set")).isInstanceOf(LinkedHashSet.class);
            assertThat(m.get("arr")).isInstanceOf(Object[].class);
            assertThat(m.get("b")).isInstanceOf(byte[].class);
            assertThat((byte[]) m.get("b")).containsExactly(1, 2, 3);
        }

        @Test
        @DisplayName("ref produces true object identity, which is the only way to build a cycle")
        void refProducesIdentityNotACopy() {
            Map<Object, Object> m = buildMap(
                    "{\"kind\":\"map\",\"id\":\"root\",\"entries\":[{\"key\":\"a\","
                            + "\"value\":{\"kind\":\"ref\",\"ref\":\"root\"}}]}");
            assertThat(m.get("a")).isSameAs(m);
        }

        @Test
        @DisplayName("non-String map keys survive as their own runtime type")
        void mapKeysAreThemselvesTypedSpecs() {
            Map<Object, Object> m = buildMap(
                    "{\"kind\":\"map\",\"entries\":["
                            + "{\"key\":{\"kind\":\"int\",\"value\":1},\"value\":{\"kind\":\"string\",\"value\":\"one\"}},"
                            + "{\"key\":{\"kind\":\"string\",\"value\":\"1\"},\"value\":{\"kind\":\"string\",\"value\":\"txt\"}}]}");
            assertThat(m).hasSize(2).containsKey(1).containsKey("1");
        }
    }

    @Nested
    @DisplayName("2. every synthetic violation blocks")
    class Violations {

        @Test
        @DisplayName("an unknown kind throws naming the fixture and the kind")
        void unknownKindThrows() {
            assertThatThrownBy(() -> build(
                    "{\"kind\":\"map\",\"entries\":[{\"key\":\"a\",\"value\":{\"kind\":\"intt\",\"value\":1}}]}"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("drill")
                    .hasMessageContaining("intt");
        }

        @Test
        @DisplayName("input and javaInput together is an error, not a precedence rule")
        void bothSourcesThrows() {
            assertThatThrownBy(() -> FidelityFixture.from(spec(fixtureJson(
                    "\"input\": \"{}\", \"javaInput\": {\"kind\":\"map\",\"entries\":[]},"))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("BOTH");
        }

        @Test
        @DisplayName("neither source is an error")
        void neitherSourceThrows() {
            assertThatThrownBy(() -> FidelityFixture.from(spec(fixtureJson(""))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no source document");
        }

        @Test
        @DisplayName("javaInput on a non-MAP stack throws rather than NPE-ing downstream")
        void javaInputOnTheWrongStackThrows() {
            FidelityFixture fx = FidelityFixture.from(spec(fixtureJson(
                    "\"javaInput\": {\"kind\":\"map\",\"entries\":[]},").replace("\"MAP\"", "\"JSON\"")));
            assertThatThrownBy(() -> FidelityRunner.run(fx))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("only expressible on the MAP stack");
        }

        @Test
        @DisplayName("a Date with no timezone pin throws, because its rendering is machine-dependent")
        void datedSpecWithoutATimeZonePinThrows() {
            assertThatThrownBy(() -> build(
                    "{\"kind\":\"map\",\"entries\":[{\"key\":\"w\",\"value\":{\"kind\":\"date\",\"epochMillis\":0}}]}"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("timeZone");
        }

        @Test
        @DisplayName("a ref to an anchor that was never opened throws")
        void danglingRefThrows() {
            assertThatThrownBy(() -> build(
                    "{\"kind\":\"map\",\"entries\":[{\"key\":\"a\",\"value\":{\"kind\":\"ref\",\"ref\":\"nope\"}}]}"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("nope");
        }

        @Test
        @DisplayName("an enum type outside the java. allow-list throws")
        void enumOutsideTheAllowListThrows() {
            assertThatThrownBy(() -> build(
                    "{\"kind\":\"map\",\"entries\":[{\"key\":\"a\",\"value\":{\"kind\":\"enum\","
                            + "\"type\":\"io.github.pierce.MapFlattener$FieldNamingStrategy\",\"value\":\"UPPER_CASE\"}}]}"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("allow-list");
        }

        @Test
        @DisplayName("the renderer refuses two map keys that render to the same token")
        void rendererRefusesAKeyCollision() {
            Map<Object, Object> clashing = new LinkedHashMap<>();
            clashing.put("S:1", "a");
            clashing.put(1, "b");
            // "S:1" as a String renders verbatim as S:1; Integer 1 renders as the token I:1, so
            // these two do NOT clash - which is the point of the typed rendering. Force a genuine
            // clash to prove the guard fires at all.
            Map<Object, Object> forced = new LinkedHashMap<>();
            forced.put("I:1", "a");
            forced.put(1, "b");
            assertThat(FidelityRender.text(FidelityRender.java(clashing))).contains("I:1");
            assertThatThrownBy(() -> FidelityRender.java(forced))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("render to");
        }

        @Test
        @DisplayName("a self-referential graph renders as a stable CYCLE token instead of overflowing")
        void rendererEmitsAStableCycleToken() {
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("a", root);
            assertThat(FidelityRender.text(FidelityRender.java(root))).isEqualTo("{\"a\":\"CYCLE:^1\"}");
        }
    }

    @Nested
    @DisplayName("3. missing or empty input blocks")
    class Empty {

        @Test
        @DisplayName("an empty javaInput block throws")
        void emptySpecThrows() {
            assertThatThrownBy(() -> FidelityJavaInput.build(spec("{}"), "drill"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("empty javaInput");
        }

        @Test
        @DisplayName("a javaInput root that is not a map throws - flatten() takes a Map")
        void nonMapRootThrows() {
            assertThatThrownBy(() -> build("{\"kind\":\"list\",\"items\":[]}"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("MapFlattener.flatten takes a Map");
        }

        @Test
        @DisplayName("a container with no items array throws rather than building an empty one")
        void missingItemsThrows() {
            assertThatThrownBy(() -> build(
                    "{\"kind\":\"map\",\"entries\":[{\"key\":\"a\",\"value\":{\"kind\":\"list\"}}]}"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("'items'");
        }
    }

    @Test
    @DisplayName("a pinned measurement restores the JVM defaults it changed")
    void theEnvironmentPinIsRestored() {
        TimeZone zoneBefore = TimeZone.getDefault();
        Locale localeBefore = Locale.getDefault();
        try (FidelityJavaInput.Env env = FidelityJavaInput.environment(
                spec("{\"environment\":{\"timeZone\":\"Asia/Tokyo\",\"locale\":\"fr-FR\"}}"))) {
            assertThat(env).isNotNull();
            assertThat(TimeZone.getDefault().getID()).isEqualTo("Asia/Tokyo");
        }
        assertThat(TimeZone.getDefault()).isEqualTo(zoneBefore);
        assertThat(Locale.getDefault()).isEqualTo(localeBefore);
    }

    private static String fixtureJson(String sourceFields) {
        return "{\"id\":\"drill\",\"family\":\"f\",\"title\":\"t\",\"stack\":\"MAP\","
                + "\"rationale\":\"r\",\"catchesBugClass\":\"c\",\"cannotCatch\":\"n\","
                + sourceFields
                + "\"expected\":{}}";
    }
}
