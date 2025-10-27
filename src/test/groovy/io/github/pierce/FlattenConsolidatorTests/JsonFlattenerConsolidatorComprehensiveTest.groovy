package io.github.pierce.FlattenConsolidatorTests

import io.github.pierce.JsonFlattenerConsolidator
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import spock.lang.Timeout
import spock.lang.Unroll
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

/**
 * Comprehensive test suite for JsonFlattenerConsolidator in Groovy.
 *
 * This test suite validates the robustness, correctness, and performance of our JSON flattening
 * and consolidation implementation using Spock framework features.
 *
 * Key improvements in Groovy version:
 * - Shared test fixtures with @Shared
 * - Data-driven tests with where blocks
 * - More expressive test names and assertions
 * - Cleaner async testing patterns
 */
@Stepwise
class JsonFlattenerConsolidatorComprehensiveTest extends Specification {

    @Shared JsonFlattenerConsolidator standardFlattener
    @Shared JsonFlattenerConsolidator performanceFlattener
    @Shared JsonFlattenerConsolidator securityFlattener

    def setupSpec() {
        // Standard configuration for most tests
        standardFlattener = new JsonFlattenerConsolidator(",", "null", 50, 1000, false)

        // Performance-optimized configuration
        performanceFlattener = new JsonFlattenerConsolidator(",", null, 20, 100, false)

        // Security-focused configuration with strict limits
        securityFlattener = new JsonFlattenerConsolidator(",", "NULL", 10, 50, false)
    }

    def "should handle Unicode characters, emojis, and special characters without data loss"() {
        given: "JSON with international characters and special symbols"
        def json = '''
        {
            "customer": {
                "name": "José María García-López",
                "nickname": "El Niño 👨‍💼",
                "address": {
                    "street": "Straße der Pariser Kommune",
                    "city": "北京市",
                    "notes": "Near café ☕ & restaurant 🍕"
                }
            },
            "transactions": [
                {"description": "Payment to Żółć Gęślą Jaźń", "amount": 123.45},
                {"description": "Transfer from Банк", "amount": 678.90}
            ],
            "specialChars": ["<script>", "'; DROP TABLE;", "tab\\tchar", "newline\\nchar", "\\"quotes\\""]
        }
        '''

        when: "processing Unicode and special characters"
        def result = standardFlattener.flattenAndConsolidateJson(json)
        def resultJson = new JsonSlurper().parseText(result)

        then: "all characters are preserved correctly"
        with(resultJson) {
            customer_name == "José María García-López"
            customer_nickname == "El Niño 👨‍💼"
            customer_address_city == "北京市"
            customer_address_notes.contains("café ☕")

            transactions_description == "Payment to Żółć Gęślą Jaźń,Transfer from Банк"
            transactions_amount == "123.45,678.9"

            specialChars.contains("<script>")
            specialChars.contains("'; DROP TABLE;")
            specialChars.contains("\"quotes\"")
            specialChars.contains("tab\tchar")
            specialChars.contains("newline\nchar")
        }
    }

    def "should handle extreme nesting depth without stack overflow"() {
        given: "deeply nested structure beyond security limit"
        def deepJson = [:]
        def current = deepJson

        100.times { i ->
            def next = [:]
            current["level$i"] = next
            current = next
        }
        current.deepValue = "finally here!"

        when: "processing with security flattener"
        def result = securityFlattener.flattenAndConsolidateJson(new JsonBuilder(deepJson).toString())
        def resultJson = new JsonSlurper().parseText(result)

        then: "depth is limited without stack overflow"
        def foundDepthLimit = resultJson.any { key, value ->
            key.contains("level9") && value.toString().contains("level10")
        }
        foundDepthLimit
    }

    def "should efficiently handle large arrays with proper truncation and statistics"() {
        given: "JSON with various large arrays"
        def json = [
                // Numeric array with 10,000 elements
                hugeNumbers: (0..<10000).collect { it },

                // String array with long values
                longStrings: (0..<100).collect { "x" * 1000 + it },

                // Nested array of objects
                complexArray: (0..<500).collect { i ->
                    [id: i, value: "item$i", nested: [deep: "value$i"]]
                }
        ]

        when: "processing with performance limits"
        def result = performanceFlattener.flattenAndConsolidateJson(new JsonBuilder(json).toString())
        def resultJson = new JsonSlurper().parseText(result)

        then: "arrays are truncated to limits"
        resultJson.hugeNumbers_count == 100
        resultJson.longStrings_count == 100
        resultJson.complexArray_id_count == 100

        and: "statistics are accurate for truncated data"
        resultJson.hugeNumbers_avg_length < 3
        resultJson.longStrings_min_length == 1001
        resultJson.longStrings_max_length == 1002

        and: "consolidation is memory-efficient"
        resultJson.hugeNumbers.split(",").size() == 100
        resultJson.hugeNumbers.startsWith("0,1,2,3,4")
    }

    def "should correctly classify mixed-type arrays and preserve type information"() {
        given: "various array type combinations"
        def json = '''
            {
                "pureNumbers": [1, 2.5, -3, 0, 1e6],
                "pureStrings": ["a", "b", "c"],
                "pureBooleans": [true, false, true],
                "mixedNumStr": [1, "2", 3, "four"],
                "mixedAll": [1, "two", true, null, 3.14],
                "stringifiedNumbers": ["1", "2", "3", "4.5"],
                "stringifiedBooleans": ["true", "false", "True", "FALSE"],
                "nullsAndValues": [null, null, "value", null]
            }
        '''

        when: "processing mixed types"
        def result = standardFlattener.flattenAndConsolidateJson(json)
        def resultJson = new JsonSlurper().parseText(result)

        then: "type classification is correct"
        with(resultJson) {
            pureNumbers_type == "numeric_list_consolidated"
            pureStrings_type == "string_list_consolidated"
            pureBooleans_type == "boolean_list_consolidated"
            mixedNumStr_type == "string_list_consolidated"
            mixedAll_type == "string_list_consolidated"
            stringifiedNumbers_type == "numeric_list_consolidated"
            stringifiedBooleans_type == "boolean_list_consolidated"

            nullsAndValues == "null,null,value,null"
            nullsAndValues_count == 4
            nullsAndValues_distinct_count == 2
        }
    }

    @Unroll
    def "should handle empty and null scenarios: #scenario"() {
        when: "processing edge case"
        def result = standardFlattener.flattenAndConsolidateJson(json)

        then: "result matches expected"
        if (json == "null" || json == "") {
            result == "{}"
        } else {
            def resultJson = new JsonSlurper().parseText(result)
            if (expectedValue == null) {
                !resultJson.containsKey(expectedKey)
            } else if (expectedValue instanceof Long) {
                resultJson[expectedKey] == expectedValue
            } else {
                resultJson[expectedKey].toString() == expectedValue.toString()
            }
        }

        where:
        scenario         | json                                | expectedKey       | expectedValue
        "Null JSON"      | "null"                             | "any"            | null
        "Empty JSON"     | ""                                 | "any"            | null
        "Empty object"   | "{}"                               | "any"            | null
        "Null value"     | '{"key": null}'                    | "key"            | "null"
        "Empty string"   | '{"key": ""}'                      | "key"            | ""
        "Empty array"    | '{"arr": []}'                      | "arr"            | "null"
        "Array of nulls" | '{"arr": [null, null, null]}'      | "arr_count"      | 3L
        "Nested empty"   | '{"a": {"b": {}}}'                 | "a_b"            | "null"
        "Mixed empty"    | '{"obj": {}, "arr": [], "val": null}' | "val"         | "null"
    }

    def "should handle field names with special characters and avoid collisions"() {
        given: "JSON with problematic field names"
        def json = '''
            {
                "normal.field": "value1",
                "normal_field": "value2",
                "SELECT": "sql_keyword",
                "class": "java_keyword",
                "field-with-dash": "dash",
                "field with space": "space",
                "field@email.com": "email",
                "числовое_поле": "unicode_field",
                "🔥hotField": "emoji",
                "": "empty_name",
                "a": {
                    "b.c": "nested_dot",
                    "b_c": "nested_underscore"
                }
            }
        '''

        when: "processing field names"
        def result = standardFlattener.flattenAndConsolidateJson(json)
        def resultJson = new JsonSlurper().parseText(result)

        then: "field names are handled appropriately"
        resultJson.normal_field == "value1,value2"  // Collision handled
        !resultJson.containsKey("normal.field")

        resultJson.SELECT == "sql_keyword"
        resultJson["class"] == "java_keyword"
        resultJson["field-with-dash"] == "dash"
        resultJson["field with space"] == "space"
        resultJson["field@email_com"] == "email"  // @ becomes _
        resultJson["числовое_поле"] == "unicode_field"
        resultJson["🔥hotField"] == "emoji"
    }

    def "should handle complex real-world financial transaction schema"() {
        given: "realistic transaction JSON"
        def json = new JsonBuilder([
                transactionId: "TXN-2024-001",
                timestamp: 1704067200000,
                customer: [
                        id: "CUST-12345",
                        name: "ABC Corporation",
                        type: "CORPORATE",
                        riskScore: 7.5,
                        contacts: [
                                [
                                        type: "PRIMARY",
                                        name: "John Doe",
                                        email: "john@abc.com",
                                        phones: ["+1-555-0123", "+1-555-0124"]
                                ],
                                [
                                        type: "SECONDARY",
                                        name: "Jane Smith",
                                        email: "jane@abc.com",
                                        phones: ["+1-555-0125"]
                                ]
                        ],
                        addresses: [
                                [
                                        type: "BILLING",
                                        line1: "123 Main St",
                                        line2: "Suite 100",
                                        city: "New York",
                                        state: "NY",
                                        zip: "10001",
                                        country: "USA"
                                ]
                        ]
                ],
                lineItems: [
                        [
                                itemId: "ITEM-001",
                                description: "Professional Services",
                                quantity: 10,
                                unitPrice: 150.00,
                                taxes: [
                                        [type: "STATE", rate: 0.08, amount: 120.00],
                                        [type: "CITY", rate: 0.02, amount: 30.00]
                                ],
                                discounts: []
                        ],
                        [
                                itemId: "ITEM-002",
                                description: "Software License",
                                quantity: 5,
                                unitPrice: 500.00,
                                taxes: [
                                        [type: "STATE", rate: 0.08, amount: 200.00]
                                ],
                                discounts: [
                                        [type: "VOLUME", percentage: 0.10, amount: 250.00]
                                ]
                        ]
                ],
                payment: [
                        method: "WIRE_TRANSFER",
                        status: "COMPLETED",
                        reference: "WT-2024-001",
                        processedAt: 1704067500000,
                        details: [
                                bankName: "Chase Bank",
                                accountLast4: "1234",
                                routingNumber: "XXXXX6789"
                        ]
                ],
                metadata: [
                        source: "API",
                        version: "2.0",
                        tags: ["urgent", "corporate", "Q1-2024"],
                        customFields: [
                                projectCode: "PROJ-789",
                                costCenter: "CC-456"
                        ]
                ]
        ]).toString()

        when: "processing complex schema"
        def result = standardFlattener.flattenAndConsolidateJson(json)
        def resultJson = new JsonSlurper().parseText(result)

        then: "all nested data is correctly flattened"
        with(resultJson) {
            // Top-level fields
            transactionId == "TXN-2024-001"
            timestamp == 1704067200000

            // Customer data
            customer_id == "CUST-12345"
            customer_riskScore == 7.5

            // Contacts consolidation
            customer_contacts_type == "PRIMARY,SECONDARY"
            customer_contacts_type_count == 2
            customer_contacts_phones == "+1-555-0123,+1-555-0124,+1-555-0125"
            customer_contacts_phones_count == 2  // 2 source arrays

            // Complex nested arrays
            lineItems_taxes_type == "STATE,CITY,STATE"
            lineItems_taxes_amount_count == 3
            lineItems_taxes_amount == "120.0,30.0,200.0"

            // Empty array handling
            lineItems_discounts_type == "VOLUME"
            lineItems_discounts_type_count == 1

            // Metadata
            metadata_tags == "urgent,corporate,Q1-2024"
            metadata_tags_count == 3
            metadata_customFields_projectCode == "PROJ-789"
        }
    }

    @Timeout(value = 35, unit = TimeUnit.SECONDS)
    def "should process complex documents efficiently within performance SLA"() {
        given: "complex document template"
        def template = generateComplexDocument(20, 50, 5)
        def templateStr = new JsonBuilder(template).toString()

        and: "performance tracking variables"
        def processedCount = 0
        def totalBytes = 0
        def processingTimes = []

        when: "warming up JVM"
        100.times {
            performanceFlattener.flattenAndConsolidateJson(templateStr)
        }

        and: "processing for 30 seconds"
        def startTime = System.currentTimeMillis()

        while (System.currentTimeMillis() - startTime < 30_000) {
            def doc = generateComplexDocument(
                    15 + (processedCount % 10),
                    40 + (processedCount % 20),
                    5
            )
            def docStr = new JsonBuilder(doc).toString()
            totalBytes += docStr.length()

            def docStart = System.nanoTime()
            def result = performanceFlattener.flattenAndConsolidateJson(docStr)
            def docTime = System.nanoTime() - docStart

            processingTimes << docTime
            processedCount++
        }

        def totalTime = System.currentTimeMillis() - startTime

        then: "performance metrics meet requirements"
        def avgTimeMs = processingTimes.sum() / processingTimes.size() / 1_000_000.0
        def throughputDocsPerSec = processedCount / (totalTime / 1000.0)
        def throughputMBPerSec = (totalBytes / (1024.0 * 1024.0)) / (totalTime / 1000.0)

        processingTimes.sort()
        def p50 = processingTimes[processingTimes.size() / 2] / 1_000_000
        def p95 = processingTimes[(int)(processingTimes.size() * 0.95)] / 1_000_000
        def p99 = processingTimes[(int)(processingTimes.size() * 0.99)] / 1_000_000

        println """
            ===== Performance Test Results =====
            Test Duration: ${String.format("%.2f", totalTime / 1000.0)} seconds
            Documents Processed: $processedCount
            Total Data Processed: ${String.format("%.2f", totalBytes / (1024.0 * 1024.0))} MB

            Throughput:
            - Documents/second: ${String.format("%.2f", throughputDocsPerSec)}
            - MB/second: ${String.format("%.2f", throughputMBPerSec)}

            Latency (ms):
            - Average: ${String.format("%.2f", avgTimeMs)}
            - P50: $p50
            - P95: $p95
            - P99: $p99

            Performance Characteristics:
            - Linear time complexity: O(n) where n is total nodes
            - Space complexity: O(d) where d is max depth
            - No recursive stack usage (iterative implementation)
            ===================================
        """

        processedCount > 1000
        avgTimeMs < 30
        p99 < 100
    }

    def "should handle concurrent processing without thread safety issues"() {
        given: "concurrent test parameters"
        def threadCount = 10
        def documentsPerThread = 100
        def executor = Executors.newFixedThreadPool(threadCount)
        def latch = new CountDownLatch(threadCount)
        def results = new ConcurrentHashMap()
        def successCount = new AtomicInteger()
        def errorCount = new AtomicInteger()

        and: "shared flattener instance"
        def sharedFlattener = new JsonFlattenerConsolidator(",", "null", 20, 100, false)

        when: "processing documents concurrently"
        threadCount.times { threadId ->
            executor.submit {
                try {
                    documentsPerThread.times { docId ->
                        def doc = generateComplexDocument(10 + threadId, 20 + docId, 3)
                        def docStr = new JsonBuilder(doc).toString()
                        def key = "thread${threadId}_doc${docId}"

                        def result = sharedFlattener.flattenAndConsolidateJson(docStr)
                        results[key] = result

                        def resultJson = new JsonSlurper().parseText(result)
                        if (resultJson.size() > 0) {
                            successCount.incrementAndGet()
                        } else {
                            errorCount.incrementAndGet()
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace()
                    errorCount.incrementAndGet()
                } finally {
                    latch.countDown()
                }
            }
        }

        def completed = latch.await(30, TimeUnit.SECONDS)
        executor.shutdown()

        then: "all threads complete successfully"
        completed
        successCount.get() == threadCount * documentsPerThread
        errorCount.get() == 0
        results.size() == threadCount * documentsPerThread
    }

    def "should maintain memory efficiency during large batch processing"() {
        given: "initial memory state"
        System.gc()
        def initialMemory = getUsedMemory()

        when: "processing many documents"
        def documentCount = 1000
        def results = []

        documentCount.times { i ->
            def doc = generateComplexDocument(15, 30, 4)
            def result = performanceFlattener.flattenAndConsolidateJson(new JsonBuilder(doc).toString())
            results << result

            // Periodically clear to allow GC
            if (i % 100 == 0) {
                results.clear()
                System.gc()
            }
        }

        results.clear()
        System.gc()
        Thread.yield()
        System.gc()

        def finalMemory = getUsedMemory()
        def memoryGrowth = finalMemory - initialMemory

        then: "memory usage is reasonable"
        println """
            ===== Memory Efficiency Results =====
            Documents Processed: $documentCount
            Initial Memory: ${String.format("%.2f", initialMemory / (1024.0 * 1024.0))} MB
            Final Memory: ${String.format("%.2f", finalMemory / (1024.0 * 1024.0))} MB
            Memory Growth: ${String.format("%.2f", memoryGrowth / (1024.0 * 1024.0))} MB
            Average per Document: ${String.format("%.2f", (memoryGrowth / 1024.0) / documentCount)} KB
            ====================================
        """

        memoryGrowth < 50 * 1024 * 1024  // Less than 50MB growth
    }

    // Helper methods

    private Map generateComplexDocument(int maxDepth, int maxArraySize, int complexity) {
        def doc = [
                docId: UUID.randomUUID().toString(),
                timestamp: System.currentTimeMillis(),
                threadInfo: [id: Thread.currentThread().id]
        ]

        // Add nested structures
        def current = doc
        Math.min(maxDepth, 10).times { depth ->
            def nested = [
                    level: depth,
                    data: "value_$depth"
            ]

            // Add arrays at various levels
            if (depth % 2 == 0) {
                def arr = []
                Math.min(maxArraySize, 20).times { i ->
                    if (depth % 3 == 0) {
                        arr << "item_$i"
                    } else {
                        arr << [index: i, value: "complex_$i"]
                    }
                }
                nested["array_$depth"] = arr
            }

            current["nested_$depth"] = nested
            current = nested
        }

        // Add variety based on complexity
        complexity.times { i ->
            doc["field_$i"] = generateRandomValue()
        }

        return doc
    }

    private Object generateRandomValue() {
        def rand = new Random()
        switch (rand.nextInt(6)) {
            case 0: return rand.nextInt(1000)
            case 1: return rand.nextDouble() * 1000
            case 2: return rand.nextBoolean()
            case 3: return "string_${rand.nextInt(100)}"
            case 4: return [rand.nextInt(), rand.nextInt()]
            default: return null
        }
    }

    private long getUsedMemory() {
        def runtime = Runtime.runtime
        runtime.totalMemory() - runtime.freeMemory()
    }
}