package io.github.pierce;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import io.github.pierce.AvroReconstructor
import io.github.pierce.MapFlattener
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test for complex real-world Avro schemas
 * Tests deep nesting, arrays, unions, logical types, and edge cases
 */
public class ComplexAvroReconstructionTest {

    @Test
    @DisplayName("Complex E-Commerce Order - Full Production Scenario")
    public void testComplexEcommerceOrder() {
        // Build complex nested schema representing a realistic e-commerce order
        Schema schema = buildComplexOrderSchema();

        // Create complex original data
        Map<String, Object> original = createComplexOrderData();

        System.out.println("=== ORIGINAL COMPLEX DATA ===");
        System.out.println("Data structure depth: 5 levels");
        System.out.println("Total fields when flattened: ~50+");
        System.out.println();

        // Flatten
        MapFlattener flattener = MapFlattener.builder()
                .arrayFormat(MapFlattener.ArraySerializationFormat.JSON)
                .build();

        long flattenStart = System.currentTimeMillis();
        Map<String, Object> flattened = flattener.flatten(original);
        long flattenTime = System.currentTimeMillis() - flattenStart;

        System.out.println("=== FLATTENED DATA ===");
        System.out.println("Flattened fields count: " + flattened.size());
        System.out.println("Flatten time: " + flattenTime + "ms");
        System.out.println("\nFlattened keys:");
        for (String key : flattened.keySet()) {
            Object value = flattened.get(key);
            String valueStr = value != null ? value.toString() : "null";
            if (valueStr.length() > 60) {
                valueStr = valueStr.substring(0, 57) + "...";
            }
            System.out.println("  " + key + ": " + valueStr);
        }
        System.out.println();

        // Reconstruct
        AvroReconstructor reconstructor = AvroReconstructor.builder()
                .arrayFormat(AvroReconstructor.ArraySerializationFormat.BRACKET_LIST)
                .build();

        long reconstructStart = System.currentTimeMillis();
        Map<String, Object> reconstructed = reconstructor.reconstructToMap(flattened, schema);
        long reconstructTime = System.currentTimeMillis() - reconstructStart;

        System.out.println("=== RECONSTRUCTION ===");
        System.out.println("Reconstruct time: " + reconstructTime + "ms");
        System.out.println();

        // Verify
        AvroReconstructor.ReconstructionVerification verification =
                reconstructor.verifyReconstruction(original, reconstructed, schema);

        System.out.println(verification.getReport());

        assertTrue(verification.isPerfect(),
                "Complex order reconstruction should be perfect");
    }

    /**
     * Build a complex schema representing an e-commerce order with:
     * - Deep nesting (5 levels)
     * - Multiple arrays at different levels
     * - Union types (nullables)
     * - Logical types (timestamps, decimals)
     * - All primitive types
     * - Records within arrays within records
     */
    private Schema buildComplexOrderSchema() {
        // Address schema (used in multiple places)
        Schema addressSchema = SchemaBuilder.record("Address")
                .fields()
                .requiredString("street")
                .requiredString("city")
                .requiredString("state")
                .requiredString("zipCode")
                .requiredString("country")
                .optionalString("apartmentNumber")
                .endRecord();

        // Geolocation schema
        Schema geoLocationSchema = SchemaBuilder.record("GeoLocation")
                .fields()
                .requiredDouble("latitude")
                .requiredDouble("longitude")
                .optionalDouble("altitude")
                .endRecord();

        // Price schema with decimal logical type
        Schema priceSchema = SchemaBuilder.record("Price")
                .fields()
                .name("amount").type(LogicalTypes.decimal(10, 2)
                .addToSchema(Schema.create(Schema.Type.BYTES))).noDefault()
                .requiredString("currency")
                .endRecord();

        // Dimension schema
        Schema dimensionsSchema = SchemaBuilder.record("Dimensions")
                .fields()
                .requiredDouble("length")
                .requiredDouble("width")
                .requiredDouble("height")
                .requiredString("unit")
                .endRecord();

        // Product attributes schema
        Schema attributeSchema = SchemaBuilder.record("ProductAttribute")
                .fields()
                .requiredString("name")
                .requiredString("value")
                .endRecord();

        // Product schema with nested details
        Schema productSchema = SchemaBuilder.record("Product")
                .fields()
                .requiredString("productId")
                .requiredString("name")
                .requiredString("sku")
                .name("price").type(priceSchema).noDefault()
                .name("dimensions").type(dimensionsSchema).noDefault()
                .name("attributes").type().array().items(attributeSchema).noDefault()
                .name("tags").type().array().items().stringType().noDefault()
                .optionalInt("stockQuantity")
                .endRecord();

        // Line item schema with nested product
        Schema lineItemSchema = SchemaBuilder.record("LineItem")
                .fields()
                .requiredInt("lineNumber")
                .name("product").type(productSchema).noDefault()
                .requiredInt("quantity")
                .name("unitPrice").type(priceSchema).noDefault()
                .name("totalPrice").type(priceSchema).noDefault()
                .optionalDouble("discountPercent")
                .endRecord();

        // Tracking event schema
        Schema trackingEventSchema = SchemaBuilder.record("TrackingEvent")
                .fields()
                .name("timestamp").type(LogicalTypes.timestampMillis()
                .addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                .requiredString("status")
                .requiredString("location")
                .optionalString("notes")
                .endRecord();

        // Shipment schema with tracking
        Schema shipmentSchema = SchemaBuilder.record("Shipment")
                .fields()
                .requiredString("trackingNumber")
                .requiredString("carrier")
                .name("estimatedDelivery").type(LogicalTypes.timestampMillis()
                .addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                .name("shippingAddress").type(addressSchema).noDefault()
                .name("trackingEvents").type().array().items(trackingEventSchema).noDefault()
                .endRecord();

        // Payment method schema with union type
        Schema creditCardSchema = SchemaBuilder.record("CreditCard")
                .fields()
                .requiredString("last4Digits")
                .requiredString("brand")
                .requiredString("expiryMonth")
                .requiredString("expiryYear")
                .endRecord();

        Schema paypalSchema = SchemaBuilder.record("PayPal")
                .fields()
                .requiredString("email")
                .requiredString("accountId")
                .endRecord();

        // Customer schema
        Schema customerSchema = SchemaBuilder.record("Customer")
                .fields()
                .requiredString("customerId")
                .requiredString("email")
                .requiredString("firstName")
                .requiredString("lastName")
                .requiredString("phone")
                .name("billingAddress").type(addressSchema).noDefault()
                .name("shippingAddress").type(addressSchema).noDefault()
                .optionalBoolean("isPremiumMember")
                .optionalInt("loyaltyPoints")
                .endRecord();

        // Metadata schema
        Schema metadataSchema = SchemaBuilder.record("Metadata")
                .fields()
                .requiredString("source")
                .requiredString("ipAddress")
                .requiredString("userAgent")
                .optionalString("referrer")
                .name("location").type(geoLocationSchema).noDefault()
                .endRecord();

        // Main Order schema - combines everything
        return SchemaBuilder.record("Order")
                .fields()
                .requiredString("orderId")
                .requiredString("orderNumber")
                .name("orderDate").type(LogicalTypes.timestampMillis()
                .addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                .requiredString("status")
                .name("customer").type(customerSchema).noDefault()
                .name("lineItems").type().array().items(lineItemSchema).noDefault()
                .name("subtotal").type(priceSchema).noDefault()
                .name("tax").type(priceSchema).noDefault()
                .name("shippingCost").type(priceSchema).noDefault()
                .name("total").type(priceSchema).noDefault()
                .name("shipments").type().array().items(shipmentSchema).noDefault()
                .name("paymentMethod").type(Schema.createUnion(creditCardSchema, paypalSchema)).noDefault()
                .name("metadata").type(metadataSchema).noDefault()
                .name("notes").type().array().items().stringType().noDefault()
                .optionalBoolean("isGift")
                .optionalString("giftMessage")
                .endRecord();
    }

    /**
     * Create complex test data matching the schema
     */
    private Map<String, Object> createComplexOrderData() {
        Map<String, Object> order = new LinkedHashMap<>();

        // Basic order info
        order.put("orderId", "ORD-2025-123456");
        order.put("orderNumber", "WEB-00123456");
        order.put("orderDate", Instant.parse("2025-01-15T10:30:00Z").toEpochMilli());
        order.put("status", "SHIPPED");

        // Customer
        Map<String, Object> customer = new LinkedHashMap<>();
        customer.put("customerId", "CUST-987654");
        customer.put("email", "john.doe@example.com");
        customer.put("firstName", "John");
        customer.put("lastName", "Doe");
        customer.put("phone", "+1-555-0123");

        Map<String, Object> billingAddress = new LinkedHashMap<>();
        billingAddress.put("street", "123 Main St");
        billingAddress.put("city", "San Francisco");
        billingAddress.put("state", "CA");
        billingAddress.put("zipCode", "94102");
        billingAddress.put("country", "USA");
        billingAddress.put("apartmentNumber", "Apt 4B");
        customer.put("billingAddress", billingAddress);

        Map<String, Object> shippingAddress = new LinkedHashMap<>();
        shippingAddress.put("street", "456 Oak Ave");
        shippingAddress.put("city", "Los Angeles");
        shippingAddress.put("state", "CA");
        shippingAddress.put("zipCode", "90001");
        shippingAddress.put("country", "USA");
        shippingAddress.put("apartmentNumber", null);
        customer.put("shippingAddress", shippingAddress);

        customer.put("isPremiumMember", true);
        customer.put("loyaltyPoints", 2500);
        order.put("customer", customer);

        // Line Items with nested products
        List<Map<String, Object>> lineItems = new ArrayList<>();

        // Line Item 1 - Laptop
        Map<String, Object> lineItem1 = new LinkedHashMap<>();
        lineItem1.put("lineNumber", 1);

        Map<String, Object> product1 = new LinkedHashMap<>();
        product1.put("productId", "PROD-LAPTOP-001");
        product1.put("name", "Premium Laptop Pro 15");
        product1.put("sku", "LPT-PRO-15-BLK");

        Map<String, Object> price1 = new LinkedHashMap<>();
        price1.put("amount", ByteBuffer.wrap(new BigDecimal("1299.99").unscaledValue().toByteArray()));
        price1.put("currency", "USD");
        product1.put("price", price1);

        Map<String, Object> dimensions1 = new LinkedHashMap<>();
        dimensions1.put("length", 14.5);
        dimensions1.put("width", 10.2);
        dimensions1.put("height", 0.75);
        dimensions1.put("unit", "inches");
        product1.put("dimensions", dimensions1);

        List<Map<String, Object>> attributes1 = new ArrayList<>();
        Map<String, Object> attr1 = new LinkedHashMap<>();
        attr1.put("name", "RAM");
        attr1.put("value", "32GB");
        attributes1.add(attr1);

        Map<String, Object> attr2 = new LinkedHashMap<>();
        attr2.put("name", "Storage");
        attr2.put("value", "1TB SSD");
        attributes1.add(attr2);

        Map<String, Object> attr3 = new LinkedHashMap<>();
        attr3.put("name", "Processor");
        attr3.put("value", "Intel Core i9");
        attributes1.add(attr3);
        product1.put("attributes", attributes1);

        product1.put("tags", Arrays.asList("electronics", "laptop", "premium", "business"));
        product1.put("stockQuantity", 15);
        lineItem1.put("product", product1);

        lineItem1.put("quantity", 1);

        Map<String, Object> unitPrice1 = new LinkedHashMap<>();
        unitPrice1.put("amount", ByteBuffer.wrap(new BigDecimal("1299.99").unscaledValue().toByteArray()));
        unitPrice1.put("currency", "USD");
        lineItem1.put("unitPrice", unitPrice1);

        Map<String, Object> totalPrice1 = new LinkedHashMap<>();
        totalPrice1.put("amount", ByteBuffer.wrap(new BigDecimal("1299.99").unscaledValue().toByteArray()));
        totalPrice1.put("currency", "USD");
        lineItem1.put("totalPrice", totalPrice1);

        lineItem1.put("discountPercent", null);
        lineItems.add(lineItem1);

        // Line Item 2 - Wireless Mouse
        Map<String, Object> lineItem2 = new LinkedHashMap<>();
        lineItem2.put("lineNumber", 2);

        Map<String, Object> product2 = new LinkedHashMap<>();
        product2.put("productId", "PROD-MOUSE-042");
        product2.put("name", "Ergonomic Wireless Mouse");
        product2.put("sku", "MOU-ERG-WL-BLK");

        Map<String, Object> price2 = new LinkedHashMap<>();
        price2.put("amount", ByteBuffer.wrap(new BigDecimal("79.99").unscaledValue().toByteArray()));
        price2.put("currency", "USD");
        product2.put("price", price2);

        Map<String, Object> dimensions2 = new LinkedHashMap<>();
        dimensions2.put("length", 5.0);
        dimensions2.put("width", 3.2);
        dimensions2.put("height", 1.8);
        dimensions2.put("unit", "inches");
        product2.put("dimensions", dimensions2);

        List<Map<String, Object>> attributes2 = new ArrayList<>();
        Map<String, Object> attr4 = new LinkedHashMap<>();
        attr4.put("name", "Connectivity");
        attr4.put("value", "Bluetooth 5.0");
        attributes2.add(attr4);

        Map<String, Object> attr5 = new LinkedHashMap<>();
        attr5.put("name", "Battery Life");
        attr5.put("value", "6 months");
        attributes2.add(attr5);
        product2.put("attributes", attributes2);

        product2.put("tags", Arrays.asList("electronics", "peripherals", "wireless"));
        product2.put("stockQuantity", 150);
        lineItem2.put("product", product2);

        lineItem2.put("quantity", 2);

        Map<String, Object> unitPrice2 = new LinkedHashMap<>();
        unitPrice2.put("amount", ByteBuffer.wrap(new BigDecimal("79.99").unscaledValue().toByteArray()));
        unitPrice2.put("currency", "USD");
        lineItem2.put("unitPrice", unitPrice2);

        Map<String, Object> totalPrice2 = new LinkedHashMap<>();
        totalPrice2.put("amount", ByteBuffer.wrap(new BigDecimal("159.98").unscaledValue().toByteArray()));
        totalPrice2.put("currency", "USD");
        lineItem2.put("totalPrice", totalPrice2);

        lineItem2.put("discountPercent", 10.0);
        lineItems.add(lineItem2);

        order.put("lineItems", lineItems);

        // Pricing
        Map<String, Object> subtotal = new LinkedHashMap<>();
        subtotal.put("amount", ByteBuffer.wrap(new BigDecimal("1459.97").unscaledValue().toByteArray()));
        subtotal.put("currency", "USD");
        order.put("subtotal", subtotal);

        Map<String, Object> tax = new LinkedHashMap<>();
        tax.put("amount", ByteBuffer.wrap(new BigDecimal("131.40").unscaledValue().toByteArray()));
        tax.put("currency", "USD");
        order.put("tax", tax);

        Map<String, Object> shippingCost = new LinkedHashMap<>();
        shippingCost.put("amount", ByteBuffer.wrap(new BigDecimal("15.00").unscaledValue().toByteArray()));
        shippingCost.put("currency", "USD");
        order.put("shippingCost", shippingCost);

        Map<String, Object> total = new LinkedHashMap<>();
        total.put("amount", ByteBuffer.wrap(new BigDecimal("1606.37").unscaledValue().toByteArray()));
        total.put("currency", "USD");
        order.put("total", total);

        // Shipments with tracking
        List<Map<String, Object>> shipments = new ArrayList<>();
        Map<String, Object> shipment1 = new LinkedHashMap<>();
        shipment1.put("trackingNumber", "1Z999AA10123456784");
        shipment1.put("carrier", "UPS");
        shipment1.put("estimatedDelivery", Instant.parse("2025-01-20T18:00:00Z").toEpochMilli());
        shipment1.put("shippingAddress", shippingAddress); // Reuse address

        List<Map<String, Object>> trackingEvents = new ArrayList<>();
        Map<String, Object> event1 = new LinkedHashMap<>();
        event1.put("timestamp", Instant.parse("2025-01-15T11:00:00Z").toEpochMilli());
        event1.put("status", "PICKED_UP");
        event1.put("location", "San Francisco, CA");
        event1.put("notes", "Package picked up from warehouse");
        trackingEvents.add(event1);

        Map<String, Object> event2 = new LinkedHashMap<>();
        event2.put("timestamp", Instant.parse("2025-01-16T08:30:00Z").toEpochMilli());
        event2.put("status", "IN_TRANSIT");
        event2.put("location", "Sacramento, CA");
        event2.put("notes", null);
        trackingEvents.add(event2);

        Map<String, Object> event3 = new LinkedHashMap<>();
        event3.put("timestamp", Instant.parse("2025-01-17T14:20:00Z").toEpochMilli());
        event3.put("status", "OUT_FOR_DELIVERY");
        event3.put("location", "Los Angeles, CA");
        event3.put("notes", "Out for delivery");
        trackingEvents.add(event3);

        shipment1.put("trackingEvents", trackingEvents);
        shipments.add(shipment1);
        order.put("shipments", shipments);

        // Payment Method (Credit Card union type)
        Map<String, Object> creditCard = new LinkedHashMap<>();
        creditCard.put("last4Digits", "4242");
        creditCard.put("brand", "Visa");
        creditCard.put("expiryMonth", "12");
        creditCard.put("expiryYear", "2028");
        order.put("paymentMethod", creditCard);

        // Metadata
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("source", "web");
        metadata.put("ipAddress", "192.168.1.100");
        metadata.put("userAgent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
        metadata.put("referrer", "https://google.com/search");

        Map<String, Object> geoLocation = new LinkedHashMap<>();
        geoLocation.put("latitude", 37.7749);
        geoLocation.put("longitude", -122.4194);
        geoLocation.put("altitude", 52.0);
        metadata.put("location", geoLocation);
        order.put("metadata", metadata);

        // Order notes
        order.put("notes", Arrays.asList(
                "Customer requested gift wrapping",
                "Include extra warranty documentation",
                "Fragile items - handle with care"
        ));

        order.put("isGift", true);
        order.put("giftMessage", "Happy Birthday! Hope you enjoy your new setup!");

        return order;
    }
}