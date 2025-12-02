//package io.github.pierce;
//import io.github.pierce.MapFlattener;
//import org.junit.jupiter.api.Test;
//import java.util.*;
//import static org.junit.jupiter.api.Assertions.*;
//
//public class MapFlattenerTest {
//
//    @Test
//    public void testBasicFlattening() {
//        MapFlattener flattener = new MapFlattener();
//
//        Map<String, Object> input = new HashMap<>();
//        Map<String, Object> user = new HashMap<>();
//        user.put("name", "John");
//        user.put("age", 30);
//        input.put("user", user);
//
//        Map<String, Object> result = flattener.flatten(input);
//
//        assertEquals("John", result.get("user_name"));
//        assertEquals(30, result.get("user_age"));
//    }
//
//    @Test
//    public void testNestedArrayFlattening() {
//        MapFlattener flattener = MapFlattener.builder()
//                .arrayFormat(MapFlattener.ArraySerializationFormat.JSON)
//                .build();
//
//        Map<String, Object> input = new HashMap<>();
//        List<Map<String, Object>> accounts = new ArrayList<>();
//
//        Map<String, Object> account = new HashMap<>();
//        account.put("signingOrderCode", "10721557");
//
//        Map<String, Object> electronicDelivery = new HashMap<>();
//        electronicDelivery.put("electronicDeliveryConsentIndicator", true);
//        account.put("electronicDelivery", electronicDelivery);
//
//        accounts.add(account);
//        input.put("accounts", accounts);
//
//        Map<String, Object> result = flattener.flatten(input);
//
//        assertTrue(result.containsKey("accounts_signingOrderCode"));
//        assertTrue(result.containsKey("accounts_electronicDelivery_electronicDeliveryConsentIndicator"));
//
//        assertEquals("[\"10721557\"]", result.get("accounts_signingOrderCode"));
//        assertEquals("[true]", result.get("accounts_electronicDelivery_electronicDeliveryConsentIndicator"));
//    }
//
//    @Test
//    public void testCircularReference() {
//        MapFlattener flattener = MapFlattener.builder()
//                .detectCircularReferences(true)
//                .build();
//
//        Map<String, Object> input = new HashMap<>();
//        input.put("self", input); // Circular reference
//
//        Map<String, Object> result = flattener.flatten(input);
//
//        assertEquals("[CIRCULAR_REFERENCE]", result.get("self"));
//    }
//
//    @Test
//    public void testArrayFormats() {
//        Map<String, Object> input = new HashMap<>();
//        input.put("values", Arrays.asList(1, 2, 3));
//
//        // JSON format
//        MapFlattener jsonFlattener = MapFlattener.builder()
//                .arrayFormat(MapFlattener.ArraySerializationFormat.JSON)
//                .build();
//        assertEquals("[1,2,3]", jsonFlattener.flatten(input).get("values"));
//
//        // Comma separated
//        MapFlattener commaFlattener = MapFlattener.builder()
//                .arrayFormat(MapFlattener.ArraySerializationFormat.COMMA_SEPARATED)
//                .build();
//        assertEquals("1,2,3", commaFlattener.flatten(input).get("values"));
//
//        // Pipe separated
//        MapFlattener pipeFlattener = MapFlattener.builder()
//                .arrayFormat(MapFlattener.ArraySerializationFormat.PIPE_SEPARATED)
//                .build();
//        assertEquals("1|2|3", pipeFlattener.flatten(input).get("values"));
//    }
//
//    @Test
//    public void testMaxDepth() {
//        MapFlattener flattener = MapFlattener.builder()
//                .maxDepth(2)
//                .build();
//
//        Map<String, Object> level1 = new HashMap<>();
//        Map<String, Object> level2 = new HashMap<>();
//        Map<String, Object> level3 = new HashMap<>();
//        level3.put("value", "deep");
//        level2.put("level3", level3);
//        level1.put("level2", level2);
//
//        Map<String, Object> result = flattener.flatten(level1);
//
//        // Should stringify level3 due to depth limit
//        assertTrue(result.get("level2_level3").toString().contains("value"));
//    }
//
//    @Test
//    public void testNullHandling() {
//        MapFlattener flattener = new MapFlattener();
//
//        Map<String, Object> input = new HashMap<>();
//        input.put("nullValue", null);
//        input.put("emptyList", Collections.emptyList());
//        input.put("emptyMap", Collections.emptyMap());
//
//        Map<String, Object> result = flattener.flatten(input);
//
//        assertNull(result.get("nullValue"));
//        assertNull(result.get("emptyList"));
//        assertNull(result.get("emptyMap"));
//    }
//}