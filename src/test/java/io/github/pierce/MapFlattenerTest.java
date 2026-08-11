package io.github.pierce;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test suite for MapFlattener.
 *
 * <p>Ported from src/test/groovy/MapFlattenerTest.groovy. The Groovy original contained
 * 6 {@code @Test} methods and no nested classes; this port contains the same 6 methods.
 */
public class MapFlattenerTest {

    @Test
    public void testBasicFlattening() {
        MapFlattener flattener = new MapFlattener();

        Map<String, Object> input = new HashMap<>();
        Map<String, Object> user = new HashMap<>();
        user.put("name", "John");
        user.put("age", 30);
        input.put("user", user);

        Map<String, Object> result = flattener.flatten(input);

        assertThat(result.get("user_name")).isEqualTo("John");
        assertThat(result.get("user_age")).isEqualTo(30);
    }

    @Test
    public void testNestedArrayFlattening() {
        MapFlattener flattener = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.JSON)
                .build();

        Map<String, Object> input = new HashMap<>();
        List<Map<String, Object>> accounts = new ArrayList<>();

        Map<String, Object> account = new HashMap<>();
        account.put("signingOrderCode", "10721557");

        Map<String, Object> electronicDelivery = new HashMap<>();
        electronicDelivery.put("electronicDeliveryConsentIndicator", true);
        account.put("electronicDelivery", electronicDelivery);

        accounts.add(account);
        input.put("accounts", accounts);

        Map<String, Object> result = flattener.flatten(input);

        assertThat(result).containsKey("accounts_signingOrderCode");
        assertThat(result).containsKey("accounts_electronicDelivery_electronicDeliveryConsentIndicator");

        assertThat(result.get("accounts_signingOrderCode")).isEqualTo("[\"10721557\"]");
        assertThat(result.get("accounts_electronicDelivery_electronicDeliveryConsentIndicator"))
                .isEqualTo("[true]");
    }

    @Test
    public void testCircularReference() {
        MapFlattener flattener = MapFlattener.builder()
                .detectCircularReferences(true)
                .build();

        Map<String, Object> input = new HashMap<>();
        input.put("self", input); // Circular reference

        Map<String, Object> result = flattener.flatten(input);

        // Circular reference is detected when we try to re-enter the same object
        // First level: processes "self" -> input (input not yet visited, so we enter it)
        // Second level: processes "self" -> input (input already visited, circular detected)
        assertThat(result.get("self_self")).isEqualTo("[CIRCULAR_REFERENCE]");
    }

    @Test
    public void testArrayFormats() {
        Map<String, Object> input = new HashMap<>();
        input.put("values", Arrays.asList(1, 2, 3));

        // JSON format
        MapFlattener jsonFlattener = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.JSON)
                .build();
        assertThat(jsonFlattener.flatten(input).get("values")).isEqualTo("[1,2,3]");

        // Comma separated
        MapFlattener commaFlattener = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.COMMA_SEPARATED)
                .build();
        assertThat(commaFlattener.flatten(input).get("values")).isEqualTo("1,2,3");

        // Pipe separated
        MapFlattener pipeFlattener = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.PIPE_SEPARATED)
                .build();
        assertThat(pipeFlattener.flatten(input).get("values")).isEqualTo("1|2|3");
    }

    @Test
    public void testMaxDepth() {
        MapFlattener flattener = MapFlattener.builder()
                .maxDepth(2)
                .build();

        Map<String, Object> level1 = new HashMap<>();
        Map<String, Object> level2 = new HashMap<>();
        Map<String, Object> level3 = new HashMap<>();
        level3.put("value", "deep");
        level2.put("level3", level3);
        level1.put("level2", level2);

        Map<String, Object> result = flattener.flatten(level1);

        // Should stringify level3 due to depth limit
        assertThat(result.get("level2_level3").toString()).contains("value");
    }

    @Test
    public void testNullHandling() {
        MapFlattener flattener = new MapFlattener();

        Map<String, Object> input = new HashMap<>();
        input.put("nullValue", null);
        input.put("emptyList", Collections.emptyList());
        input.put("emptyMap", Collections.emptyMap());

        Map<String, Object> result = flattener.flatten(input);

        assertThat(result.get("nullValue")).isNull();
        assertThat(result.get("emptyList")).isNull();
        assertThat(result.get("emptyMap")).isNull();
    }
}
