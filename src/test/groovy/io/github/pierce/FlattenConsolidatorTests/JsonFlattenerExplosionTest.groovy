package io.github.pierce.FlattenConsolidatorTests

import io.github.pierce.JsonFlattenerConsolidator
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll
import java.util.concurrent.*

/**
 * Extended test suite for the array explosion feature in JsonFlattenerConsolidator
 * Includes comprehensive edge cases and complex use cases
 *
 * Groovy improvements:
 * - More concise test data creation with builders
 * - Better assertions with Groovy collections
 * - Cleaner async testing with GPars or standard concurrency
 */
class JsonFlattenerExplosionTest extends Specification {

    def "default behavior - no explosion returns single record"() {
        given: "JSON with arrays and no explosion configuration"
        def json = '''
            {
                "orderId": "123",
                "items": ["A", "B", "C"],
                "customer": {
                    "name": "John",
                    "addresses": [
                        {"city": "NYC"},
                        {"city": "LA"}
                    ]
                }
            }
        '''

        and: "flattener without explosion paths"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false)

        when: "processing normally and with explosion"
        def result = flattener.flattenAndConsolidateJson(json)
        def exploded = flattener.flattenAndExplodeJson(json)

        then: "normal result has consolidated arrays"
        result.contains('"items":"A,B,C"')
        result.contains('"customer_addresses_city":"NYC,LA"')

        and: "explosion returns same single record"
        exploded.size() == 1
        exploded[0] == result
    }

    def "explode terminal array (simple array of primitives)"() {
        given: "JSON with primitive array"
        def json = '''
            {
                "orderId": "123",
                "customer": "ABC Corp",
                "items": ["Widget", "Gadget", "Tool"]
            }
        '''

        and: "flattener with explosion on items"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "items")

        when: "exploding on items array"
        def exploded = flattener.flattenAndExplodeJson(json)

        then: "creates one record per item"
        exploded.size() == 3

        and: "each record has correct data"
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        records[0].items == "Widget"
        records[0].items_explosion_index == 0
        records[0].orderId == "123"
        records[0].customer == "ABC Corp"

        records[1].items == "Gadget"
        records[1].items_explosion_index == 1

        records[2].items == "Tool"
        records[2].items_explosion_index == 2
    }

    def "explode non-terminal array (array of objects)"() {
        given: "JSON with object array"
        def json = '''
            {
                "orderId": "123",
                "departments": [
                    {
                        "name": "Sales",
                        "manager": "Alice",
                        "budget": 100000
                    },
                    {
                        "name": "IT",
                        "manager": "Bob",
                        "budget": 150000
                    }
                ]
            }
        '''

        and: "flattener with explosion on departments"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "departments")

        when: "exploding on departments"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "creates one record per department"
        exploded.size() == 2

        and: "first department data"
        records[0].departments_name == "Sales"
        records[0].departments_manager == "Alice"
        records[0].departments_budget == 100000
        records[0].orderId == "123"

        and: "second department data"
        records[1].departments_name == "IT"
        records[1].departments_manager == "Bob"
        records[1].departments_budget == 150000
    }

    def "hierarchical explosion - parent and child arrays"() {
        given: "nested structure with multiple array levels"
        def json = '''
            {
                "company": "TechCorp",
                "departments": [
                    {
                        "name": "Engineering",
                        "employees": [
                            {"id": "E1", "name": "Alice", "skills": ["Java", "Python"]},
                            {"id": "E2", "name": "Bob", "skills": ["JavaScript", "React"]}
                        ]
                    },
                    {
                        "name": "Marketing",
                        "employees": [
                            {"id": "M1", "name": "Charlie", "skills": ["SEO", "Content"]}
                        ]
                    }
                ]
            }
        '''

        and: "explosion on nested employees"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "departments.employees")

        when: "exploding on departments.employees"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "creates record for each employee across all departments"
        exploded.size() == 3

        and: "employee records maintain department context"
        with(records[0]) {
            departments_name == "Engineering"
            departments_employees_id == "E1"
            departments_employees_name == "Alice"
            departments_employees_skills == "Java,Python"
        }

        with(records[2]) {
            departments_name == "Marketing"
            departments_employees_id == "M1"
        }
    }

    def "multiple explosion paths create cartesian product"() {
        given: "JSON with multiple arrays"
        def json = '''
            {
                "orderId": "123",
                "lineItems": [
                    {"product": "A", "quantity": 2},
                    {"product": "B", "quantity": 1}
                ],
                "shipments": [
                    {"carrier": "FedEx", "tracking": "123"},
                    {"carrier": "UPS", "tracking": "456"}
                ]
            }
        '''

        and: "explosion on both arrays"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "lineItems", "shipments")

        when: "exploding on multiple paths"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "creates cartesian product: 2 x 2 = 4 records"
        exploded.size() == 4

        and: "verify all combinations exist"
        def combinations = records.collect {
            "${it.lineItems_product}-${it.shipments_carrier}"
        }.sort()

        combinations == ["A-FedEx", "A-UPS", "B-FedEx", "B-UPS"]
    }

    def "empty array handling in explosion"() {
        given: "JSON with empty array"
        def json = '''
            {
                "orderId": "123",
                "items": [],
                "customer": "ABC Corp"
            }
        '''

        and: "explosion on empty array"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "items")

        when: "exploding on empty array"
        def exploded = flattener.flattenAndExplodeJson(json)

        then: "returns original record"
        exploded.size() == 1
        exploded[0].contains('"orderId":"123"')
        exploded[0].contains('"customer":"ABC Corp"')
    }

    def "complex real-world explosion scenario"() {
        given: "complex order structure"
        def json = '''
            {
                "orderId": "ORD-2024-001",
                "customer": {
                    "id": "CUST-123",
                    "name": "Global Corp"
                },
                "orderDate": "2024-01-15",
                "lineItems": [
                    {
                        "sku": "WIDGET-A",
                        "quantity": 10,
                        "unitPrice": 25.00,
                        "discounts": [
                            {"type": "VOLUME", "amount": 2.50},
                            {"type": "PROMO", "amount": 1.00}
                        ]
                    },
                    {
                        "sku": "GADGET-B",
                        "quantity": 5,
                        "unitPrice": 50.00,
                        "discounts": [
                            {"type": "VOLUME", "amount": 5.00}
                        ]
                    }
                ],
                "tags": ["URGENT", "PREMIUM", "INTERNATIONAL"]
            }
        '''

        and: "explosion on nested discounts"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "lineItems.discounts")

        when: "exploding on lineItems.discounts"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "creates record for each discount"
        exploded.size() == 3  // 2 + 1 discounts

        and: "first discount maintains context"
        with(records[0]) {
            lineItems_sku == "WIDGET-A"
            lineItems_quantity == 10
            lineItems_discounts_type == "VOLUME"
            lineItems_discounts_amount == 2.50
        }

        and: "last discount from second item"
        with(records[2]) {
            lineItems_sku == "GADGET-B"
            lineItems_discounts_type == "VOLUME"
        }

        and: "all records preserve parent fields"
        records.every { record ->
            record.orderId == "ORD-2024-001" &&
                    record.customer_name == "Global Corp" &&
                    record.tags == "URGENT,PREMIUM,INTERNATIONAL"
        }
    }

    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    def "performance test for explosion"() {
        given: "large array for explosion"
        def json = [data: (0..<100).collect { [id: it, value: "item_$it"] }]
        def jsonString = new JsonBuilder(json).toString()

        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "data")

        when: "exploding large array"
        def start = System.currentTimeMillis()
        def exploded = flattener.flattenAndExplodeJson(jsonString)
        def elapsed = System.currentTimeMillis() - start

        then: "creates correct number of records"
        exploded.size() == 100

        and: "completes quickly"
        println "Explosion of 100 records completed in ${elapsed}ms"
        elapsed < 1000
    }

    def "explosion with null values and missing fields"() {
        given: "JSON with nulls and missing fields"
        def json = '''
            {
                "orderId": "123",
                "items": [
                    {"id": "A", "name": "Item A", "price": 10.0},
                    {"id": "B", "name": null, "price": 20.0},
                    {"id": "C", "price": 30.0},
                    null,
                    {"id": "D", "name": "Item D"}
                ]
            }
        '''

        and: "flattener with null placeholder"
        def flattener = new JsonFlattenerConsolidator(",", "NULL", 50, 1000, false, false, "items")

        when: "exploding with nulls"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "handles all cases gracefully"
        exploded.size() == 5

        and: "verify each record"
        records[0].items_name == "Item A"
        records[0].items_price == 10.0

        records[1].items_name == "NULL"

        !records[2].containsKey("items_name")  // Missing field

        records[3].orderId == "123"  // Null array element

        !records[4].containsKey("items_price")  // Missing price
    }

    @Unroll
    def "deep nested explosion at level #level"() {
        given: "deeply nested structure"
        def json = '''
            {
                "company": "TechCorp",
                "regions": [
                    {
                        "name": "North",
                        "countries": [
                            {
                                "code": "US",
                                "offices": [
                                    {
                                        "city": "NYC",
                                        "employees": [
                                            {"id": "E1", "name": "Alice"},
                                            {"id": "E2", "name": "Bob"}
                                        ]
                                    },
                                    {
                                        "city": "LA",
                                        "employees": [
                                            {"id": "E3", "name": "Charlie"}
                                        ]
                                    }
                                ]
                            },
                            {
                                "code": "CA",
                                "offices": [
                                    {
                                        "city": "Toronto",
                                        "employees": [
                                            {"id": "E4", "name": "David"}
                                        ]
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "name": "South",
                        "countries": [
                            {
                                "code": "BR",
                                "offices": [
                                    {
                                        "city": "Rio",
                                        "employees": [
                                            {"id": "E5", "name": "Eve"}
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        '''

        and: "flattener for specific level"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, explosionPath)

        when: "exploding at different levels"
        def exploded = flattener.flattenAndExplodeJson(json)

        then: "creates expected number of records"
        exploded.size() == expectedCount

        where:
        level | explosionPath                           | expectedCount
        1     | "regions"                              | 2  // North, South
        2     | "regions.countries"                    | 3  // US, CA, BR
        3     | "regions.countries.offices"            | 4  // NYC, LA, Toronto, Rio
        4     | "regions.countries.offices.employees"  | 5  // E1-E5
    }

    def "explosion with mixed type arrays"() {
        given: "array with mixed types"
        def json = '''
            {
                "data": [
                    "string value",
                    123,
                    true,
                    {"type": "object", "value": "complex"},
                    ["nested", "array"],
                    null,
                    45.67
                ]
            }
        '''

        and: "flattener with explosion"
        def flattener = new JsonFlattenerConsolidator(",", "NULL", 50, 1000, false, false, "data")

        when: "exploding mixed array"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "handles each type correctly"
        exploded.size() == 7

        records[0].data == "string value"
        records[1].data == 123
        records[2].data == true

        with(records[3]) {
            data_type == "object"
            data_value == "complex"
        }

        records[4].data == "nested,array"  // Nested array consolidated
    }

    def "explosion with array size limits"() {
        given: "large array exceeding limit"
        def json = [items: (0..<150).collect { [id: it, value: "item_$it"] }]
        def jsonString = new JsonBuilder(json).toString()

        and: "flattener with 100 item limit"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 100, false, false, "items")

        when: "exploding large array"
        def exploded = flattener.flattenAndExplodeJson(jsonString)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "respects array size limit"
        exploded.size() == 100

        and: "processes first 100 items"
        records[0].items_id == 0
        records[99].items_id == 99
    }

    def "explosion with special characters and Unicode"() {
        given: "JSON with special characters"
        def json = '''
            {
                "company": "Tech & Co.",
                "products": [
                    {"name": "Widget™", "description": "Best widget €99"},
                    {"name": "Gadget®", "description": "Zürich-made 製品"},
                    {"name": "Tool<script>", "description": "'; DROP TABLE; 🔧"}
                ]
            }
        '''

        and: "explosion on products"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "products")

        when: "exploding with special characters"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "preserves all special characters"
        exploded.size() == 3

        records[0].products_name == "Widget™"
        records[0].products_description.contains("€")

        records[1].products_description.contains("Zürich")
        records[1].products_description.contains("製品")

        records[2].products_name.contains("<script>")
        records[2].products_description.contains("🔧")
    }

    def "complex cartesian product with three arrays"() {
        given: "JSON with three arrays for cartesian product"
        def json = '''
            {
                "orderId": "123",
                "warehouses": [
                    {"id": "W1", "location": "East"},
                    {"id": "W2", "location": "West"}
                ],
                "products": [
                    {"sku": "A", "name": "Product A"},
                    {"sku": "B", "name": "Product B"},
                    {"sku": "C", "name": "Product C"}
                ],
                "shippingOptions": [
                    {"carrier": "FedEx", "speed": "Express"},
                    {"carrier": "UPS", "speed": "Ground"}
                ]
            }
        '''

        and: "explosion on all three arrays"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false,
                "warehouses", "products", "shippingOptions")

        when: "creating cartesian product"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "creates 2 x 3 x 2 = 12 records"
        exploded.size() == 12

        and: "all combinations are unique"
        def combinations = records.collect { record ->
            "${record.warehouses_id}-${record.products_sku}-${record.shippingOptions_carrier}"
        } as Set

        combinations.size() == 12
        combinations.contains("W1-A-FedEx")
        combinations.contains("W2-C-UPS")
    }

    def "explosion with consolidateWithMatrixDenotorsInValue option"() {
        given: "matrix structure"
        def json = '''
            {
                "matrix": [
                    [1, 2, 3],
                    [4, 5, 6],
                    [7, 8, 9]
                ]
            }
        '''

        and: "flattener with matrix denotors"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, true, false, "matrix")

        when: "exploding matrix"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "creates record per row"
        exploded.size() == 3

        and: "matrix indices are preserved"
        records.every { record ->
            record.matrix.contains("[")
        }
    }

    def "explosion with non-existent paths"() {
        given: "JSON without the explosion path"
        def json = '''
            {
                "orderId": "123",
                "customer": "ABC Corp",
                "items": ["A", "B", "C"]
            }
        '''

        and: "explosion on non-existent path"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "nonexistent.path")

        when: "attempting explosion"
        def exploded = flattener.flattenAndExplodeJson(json)

        then: "returns single consolidated record"
        exploded.size() == 1
        exploded[0].contains('"items":"A,B,C"')
    }

    def "explosion with partial path matches"() {
        given: "JSON with similar field names"
        def json = '''
            {
                "data": {
                    "items": ["A", "B"],
                    "itemsExtra": ["X", "Y", "Z"]
                },
                "dataItems": ["1", "2", "3", "4"]
            }
        '''

        and: "explosion on specific path"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "data.items")

        when: "exploding on data.items"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "only explodes exact path match"
        exploded.size() == 2

        and: "other arrays remain consolidated"
        records[0].data_items == "A"
        records[0].data_itemsExtra == "X,Y,Z"
        records[0].dataItems == "1,2,3,4"
    }

    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    def "performance test with large cartesian product"() {
        given: "JSON creating 1000 combinations"
        def json = [
                array1: (0..<10).collect { [value: "array1_$it"] },
                array2: (0..<10).collect { [value: "array2_$it"] },
                array3: (0..<10).collect { [value: "array3_$it"] }
        ]
        def jsonString = new JsonBuilder(json).toString()

        and: "explosion on all arrays"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false,
                "array1", "array2", "array3")

        when: "creating large cartesian product"
        def start = System.currentTimeMillis()
        def exploded = flattener.flattenAndExplodeJson(jsonString)
        def elapsed = System.currentTimeMillis() - start

        then: "creates all combinations"
        exploded.size() == 1000

        and: "completes in reasonable time"
        println "Large cartesian product (1000 records) completed in ${elapsed}ms"
        elapsed < 5000
    }

    def "memory efficiency during explosion"() {
        given: "document with repeated metadata"
        def json = [
                metadata: "x" * 1000,  // 1KB of metadata
                items: (0..<100).collect { i ->
                    [id: i, data: "y" * 100]
                }
        ]
        def jsonString = new JsonBuilder(json).toString()

        and: "explosion on items"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "items")

        when: "measuring memory usage"
        System.gc()
        def memBefore = getUsedMemory()

        def exploded = flattener.flattenAndExplodeJson(jsonString)

        System.gc()
        def memAfter = getUsedMemory()
        def memUsed = (memAfter - memBefore) / 1024

        then: "creates expected records"
        exploded.size() == 100

        and: "each contains metadata"
        exploded.every { it.contains("x" * 1000) }

        and: "memory usage is reasonable"
        println "Memory used for explosion: ${memUsed} KB"
        memUsed < 10000  // Less than 10MB
    }

    def "explosion with circular-like structures up to max depth"() {
        given: "recursive-like structure"
        def json = '''
            {
                "level1": {
                    "items": [
                        {
                            "id": "1A",
                            "nested": {
                                "items": [
                                    {
                                        "id": "2A",
                                        "nested": {
                                            "items": [
                                                {"id": "3A"},
                                                {"id": "3B"}
                                            ]
                                        }
                                    },
                                    {
                                        "id": "2B",
                                        "nested": {
                                            "items": [
                                                {"id": "3C"}
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "id": "1B",
                            "nested": {
                                "items": [
                                    {"id": "2C"}
                                ]
                            }
                        }
                    ]
                }
            }
        '''

        and: "explosion at deepest level"
        def flattener = new JsonFlattenerConsolidator(",", null, 10, 1000, false, false,
                "level1.items.nested.items.nested.items")

        when: "exploding deep structure"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "explodes deepest items"
        exploded.size() == 3  // 3A, 3B, 3C

        and: "preserves full context"
        with(records[0]) {
            level1_items_id == "1A"
            level1_items_nested_items_id == "2A"
            level1_items_nested_items_nested_items_id == "3A"
        }
    }

    def "explosion with empty objects in arrays"() {
        given: "array with empty objects"
        def json = '''
            {
                "data": [
                    {"id": 1, "value": "A"},
                    {},
                    {"id": 3, "value": "C"},
                    {},
                    {"id": 5}
                ]
            }
        '''

        and: "explosion on data"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "data")

        when: "exploding with empty objects"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "creates record for each element"
        exploded.size() == 5

        and: "empty objects create minimal records"
        !records[1].containsKey("data_id")
        !records[1].containsKey("data_value")
        records[1].data_explosion_index == 1
    }

    def "concurrent explosion safety"() {
        given: "test JSON and shared flattener"
        def json = '''
            {
                "items": [
                    {"id": 1, "value": "A"},
                    {"id": 2, "value": "B"},
                    {"id": 3, "value": "C"}
                ]
            }
        '''

        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "items")

        when: "multiple threads explode simultaneously"
        def threadCount = 10
        def latch = new CountDownLatch(threadCount)
        def allResults = new CopyOnWriteArrayList()
        def executor = Executors.newFixedThreadPool(threadCount)

        threadCount.times {
            executor.submit {
                try {
                    def result = flattener.flattenAndExplodeJson(json)
                    allResults.add(result)
                } finally {
                    latch.countDown()
                }
            }
        }

        def completed = latch.await(10, TimeUnit.SECONDS)
        executor.shutdown()

        then: "all threads complete successfully"
        completed
        allResults.size() == threadCount

        and: "all results are identical"
        def firstResult = allResults[0]
        allResults.every { result ->
            result.size() == firstResult.size() &&
                    result.withIndex().every { record, idx ->
                        new JsonSlurper().parseText(record) ==
                                new JsonSlurper().parseText(firstResult[idx])
                    }
        }
    }

    def "explosion with same field names at different levels"() {
        given: "JSON with repeated field names"
        def json = '''
            {
                "items": [
                    {
                        "id": "parent-1",
                        "items": [
                            {
                                "id": "child-1",
                                "items": [
                                    {"id": "grandchild-1"},
                                    {"id": "grandchild-2"}
                                ]
                            }
                        ]
                    }
                ]
            }
        '''

        and: "explosion at grandchild level"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "items.items.items")

        when: "exploding nested items"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "creates grandchild records"
        exploded.size() == 2

        and: "maintains hierarchy with same field names"
        records[0].items_id == "parent-1"
        records[0].items_items_id == "child-1"
        records[0].items_items_items_id == "grandchild-1"
    }

    def "explosion behavior with primitive array inside object array"() {
        given: "mixed array structures"
        def json = '''
            {
                "orders": [
                    {
                        "id": "ORDER-1",
                        "items": ["A", "B", "C"],
                        "quantities": [1, 2, 3]
                    },
                    {
                        "id": "ORDER-2", 
                        "items": ["X", "Y"],
                        "quantities": [5, 10]
                    }
                ]
            }
        '''

        and: "explosion on orders"
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false, "orders")

        when: "exploding orders"
        def exploded = flattener.flattenAndExplodeJson(json)
        def records = exploded.collect { new JsonSlurper().parseText(it) }

        then: "creates order records"
        exploded.size() == 2

        and: "primitive arrays within are consolidated"
        records[0].orders_items == "A,B,C"
        records[0].orders_quantities == "1,2,3"
    }

    private long getUsedMemory() {
        def runtime = Runtime.runtime
        runtime.totalMemory() - runtime.freeMemory()
    }
}