package io.github.pierce;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Minimal test to isolate the doubly-nested array issue
 */
public class DoublyNestedArrayTest {

    @Test
    @DisplayName("Minimal doubly nested array test - attributes in product")
    public void testMinimalDoublyNestedArray() {
        // Create minimal schema: Product with array of Attribute records
        Schema attributeSchema = SchemaBuilder.record("Attribute")
                .fields()
                .requiredString("name")
                .requiredString("value")
                .endRecord();

        Schema productSchema = SchemaBuilder.record("Product")
                .fields()
                .requiredString("productId")
                .name("attributes").type().array().items(attributeSchema).noDefault()
                .endRecord();

        Schema orderSchema = SchemaBuilder.record("Order")
                .fields()
                .name("products").type().array().items(productSchema).noDefault()
                .endRecord();

        // Create original data
        Map<String, Object> attr1 = new LinkedHashMap<>();
        attr1.put("name", "RAM");
        attr1.put("value", "32GB");

        Map<String, Object> attr2 = new LinkedHashMap<>();
        attr2.put("name", "Storage");
        attr2.put("value", "1TB");

        Map<String, Object> product1 = new LinkedHashMap<>();
        product1.put("productId", "PROD-001");
        product1.put("attributes", Arrays.asList(attr1, attr2));

        Map<String, Object> order = new LinkedHashMap<>();
        order.put("products", Arrays.asList(product1));

        System.out.println("=== ORIGINAL DATA ===");
        System.out.println("products[0].attributes[0].name = " +
                ((Map)((List)((Map)((List)order.get("products")).get(0)).get("attributes")).get(0)).get("name"));
        System.out.println("products[0].attributes[0].value = " +
                ((Map)((List)((Map)((List)order.get("products")).get(0)).get("attributes")).get(0)).get("value"));

        // Flatten
        MapFlattener flattener = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.BRACKET_LIST)
                .build();

        Map<String, Object> flattened = flattener.flatten(order);

        System.out.println("\n=== FLATTENED DATA ===");
        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder()
                .arrayFormat(AvroReconstructor.ArraySerializationFormat.BRACKET_LIST)
                .build();

        System.out.println("\n=== STARTING RECONSTRUCTION ===");

        try {
            Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, orderSchema);

            System.out.println("\n=== RECONSTRUCTED DATA ===");
            System.out.println("products[0].attributes[0].name = " +
                    ((Map)((List)((Map)((List)reconstructed.get("products")).get(0)).get("attributes")).get(0)).get("name"));
            System.out.println("products[0].attributes[0].value = " +
                    ((Map)((List)((Map)((List)reconstructed.get("products")).get(0)).get("attributes")).get(0)).get("value"));

            // Verify
            assertEquals("RAM",
                    ((Map)((List)((Map)((List)reconstructed.get("products")).get(0)).get("attributes")).get(0)).get("name"),
                    "First attribute name should be RAM");
            assertEquals("32GB",
                    ((Map)((List)((Map)((List)reconstructed.get("products")).get(0)).get("attributes")).get(0)).get("value"),
                    "First attribute value should be 32GB");

            System.out.println("\n✅ TEST PASSED!");

        } catch (Exception e) {
            System.out.println("\n❌ RECONSTRUCTION FAILED:");
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    @DisplayName("Debug: Show what the flattened data looks like")
    public void debugShowFlattenedStructure() {
        // Same schema and data as above
        Schema attributeSchema = SchemaBuilder.record("Attribute")
                .fields()
                .requiredString("name")
                .requiredString("value")
                .endRecord();

        Schema productSchema = SchemaBuilder.record("Product")
                .fields()
                .requiredString("productId")
                .name("attributes").type().array().items(attributeSchema).noDefault()
                .endRecord();

        Schema orderSchema = SchemaBuilder.record("Order")
                .fields()
                .name("products").type().array().items(productSchema).noDefault()
                .endRecord();

        // Create data with 2 products, each with 2 attributes
        Map<String, Object> attr1p1 = Map.of("name", "RAM", "value", "32GB");
        Map<String, Object> attr2p1 = Map.of("name", "Storage", "value", "1TB");
        Map<String, Object> product1 = Map.of(
                "productId", "PROD-001",
                "attributes", Arrays.asList(attr1p1, attr2p1)
        );

        Map<String, Object> attr1p2 = Map.of("name", "Connectivity", "value", "Bluetooth");
        Map<String, Object> attr2p2 = Map.of("name", "Battery", "value", "6 months");
        Map<String, Object> product2 = Map.of(
                "productId", "PROD-002",
                "attributes", Arrays.asList(attr1p2, attr2p2)
        );

        Map<String, Object> order = Map.of("products", Arrays.asList(product1, product2));

        // Flatten
        MapFlattener flattener = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.BRACKET_LIST)
                .build();

        Map<String, Object> flattened = flattener.flatten(order);

        System.out.println("\n=== FLATTENED STRUCTURE ===");
        System.out.println("This shows EXACTLY what the reconstructor receives:\n");

        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            System.out.println(String.format("%-35s : %s", entry.getKey(), entry.getValue()));
        }

        System.out.println("\n=== KEY INSIGHT ===");
        System.out.println("products_attributes_name should be:");
        System.out.println("  [[RAM, Storage], [Connectivity, Battery]]");
        System.out.println("");
        System.out.println("This is a DOUBLY NESTED array:");
        System.out.println("  - Outer array: products (2 elements)");
        System.out.println("  - Inner arrays: attributes per product (2 elements each)");
        System.out.println("");
        System.out.println("Reconstruction needs to:");
        System.out.println("  1. Extract products[0] → [RAM, Storage]");
        System.out.println("  2. Extract attributes[0] from [RAM, Storage] → RAM");
    }
}