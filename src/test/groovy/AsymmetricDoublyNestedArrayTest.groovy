package io.github.pierce;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test ASYMMETRIC doubly nested arrays - inner arrays of different sizes
 * This is what's failing in ComplexAvroReconstructionTest!
 */
public class AsymmetricDoublyNestedArrayTest {

    @Test
    @DisplayName("Asymmetric doubly nested arrays - Product 1 has 3 attrs, Product 2 has 2")
    public void testAsymmetricInnerArrays() {
        // Same schema as before
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

        // Product 1: 3 attributes
        Map<String, Object> attr1p1 = new LinkedHashMap<>();
        attr1p1.put("name", "RAM");
        attr1p1.put("value", "32GB");

        Map<String, Object> attr2p1 = new LinkedHashMap<>();
        attr2p1.put("name", "Storage");
        attr2p1.put("value", "1TB");

        Map<String, Object> attr3p1 = new LinkedHashMap<>();
        attr3p1.put("name", "Processor");
        attr3p1.put("value", "Intel i9");

        Map<String, Object> product1 = new LinkedHashMap<>();
        product1.put("productId", "PROD-001");
        product1.put("attributes", Arrays.asList(attr1p1, attr2p1, attr3p1)); // 3 attributes

        // Product 2: 2 attributes (FEWER than Product 1!)
        Map<String, Object> attr1p2 = new LinkedHashMap<>();
        attr1p2.put("name", "Connectivity");
        attr1p2.put("value", "Bluetooth");

        Map<String, Object> attr2p2 = new LinkedHashMap<>();
        attr2p2.put("name", "Battery");
        attr2p2.put("value", "6 months");

        Map<String, Object> product2 = new LinkedHashMap<>();
        product2.put("productId", "PROD-002");
        product2.put("attributes", Arrays.asList(attr1p2, attr2p2)); // 2 attributes (DIFFERENT SIZE!)

        Map<String, Object> order = new LinkedHashMap<>();
        order.put("products", Arrays.asList(product1, product2));

        System.out.println("=== ORIGINAL DATA ===");
        System.out.println("Product 1 has 3 attributes");
        System.out.println("Product 2 has 2 attributes (ASYMMETRIC!)");

        // Flatten
        MapFlattener flattener = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.BRACKET_LIST)
                .build();

        Map<String, Object> flattened = flattener.flatten(order);

        System.out.println("\n=== FLATTENED DATA ===");
        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

        System.out.println("\n=== KEY OBSERVATION ===");
        System.out.println("products_attributes_name: [[RAM, Storage, Processor], [Connectivity, Battery]]");
        System.out.println("                           ^----- 3 elements -----^  ^----- 2 elements ----^");
        System.out.println("");
        System.out.println("When reconstructing Product 2, there is NO attributes[2]!");
        System.out.println("The code must handle this correctly!");

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder()
                .arrayFormat(AvroReconstructor.ArraySerializationFormat.BRACKET_LIST)
                .build();

        System.out.println("\n=== STARTING RECONSTRUCTION ===");

        try {
            Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, orderSchema);

            System.out.println("\n=== RECONSTRUCTED DATA ===");
            List<Map<String, Object>> products = (List<Map<String, Object>>) reconstructed.get("products");

            System.out.println("Product 1:");
            Map<String, Object> recon1 = products.get(0);
            System.out.println("  productId: " + recon1.get("productId"));
            List<Map<String, Object>> attrs1 = (List<Map<String, Object>>) recon1.get("attributes");
            System.out.println("  attributes count: " + attrs1.size());
            for (int i = 0; i < attrs1.size(); i++) {
                System.out.println("    [" + i + "] name=" + attrs1.get(i).get("name") + ", value=" + attrs1.get(i).get("value"));
            }

            System.out.println("\nProduct 2:");
            Map<String, Object> recon2 = products.get(1);
            System.out.println("  productId: " + recon2.get("productId"));
            List<Map<String, Object>> attrs2 = (List<Map<String, Object>>) recon2.get("attributes");
            System.out.println("  attributes count: " + attrs2.size());
            for (int i = 0; i < attrs2.size(); i++) {
                System.out.println("    [" + i + "] name=" + attrs2.get(i).get("name") + ", value=" + attrs2.get(i).get("value"));
            }

            // Verify counts
            assertEquals(3, attrs1.size(), "Product 1 should have 3 attributes");
            assertEquals(2, attrs2.size(), "Product 2 should have 2 attributes");

            // Verify Product 1
            assertEquals("RAM", attrs1.get(0).get("name"));
            assertEquals("32GB", attrs1.get(0).get("value"));
            assertEquals("Storage", attrs1.get(1).get("name"));
            assertEquals("1TB", attrs1.get(1).get("value"));
            assertEquals("Processor", attrs1.get(2).get("name"));
            assertEquals("Intel i9", attrs1.get(2).get("value"));

            // Verify Product 2
            assertEquals("Connectivity", attrs2.get(0).get("name"));
            assertEquals("Bluetooth", attrs2.get(0).get("value"));
            assertEquals("Battery", attrs2.get(1).get("name"));
            assertEquals("6 months", attrs2.get(1).get("value"));

            System.out.println("\n✅ TEST PASSED!");

        } catch (Exception e) {
            System.out.println("\n❌ RECONSTRUCTION FAILED:");
            System.out.println("This is likely the same error as ComplexAvroReconstructionTest!");
            e.printStackTrace();
            throw e;
        }
    }
}