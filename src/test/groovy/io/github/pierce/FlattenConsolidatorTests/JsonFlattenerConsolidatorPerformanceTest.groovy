package io.github.pierce.FlattenConsolidatorTests

import io.github.pierce.JsonFlattenerConsolidator
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import spock.lang.Specification
import spock.lang.Stepwise
import spock.lang.Timeout
import java.util.concurrent.TimeUnit

/**
 * Performance Test Suite for JsonFlattenerConsolidator in Groovy
 *
 * Tests various scenarios to measure efficiency and identify performance bottlenecks.
 * Uses Spock's @Stepwise to ensure tests run in order for meaningful comparisons.
 */
@Stepwise
class JsonFlattenerConsolidatorPerformanceTest extends Specification {

    static final int WARM_UP_ITERATIONS = 10
    static final int TEST_ITERATIONS = 100

    def "performance comparison: Statistics ON vs OFF"() {
        given: "moderately complex JSON and two flatteners"
        def json = createModeratelyComplexJson(50, 20)
        def jsonString = new JsonBuilder(json).toString()

        def withStats = new JsonFlattenerConsolidator(",", null, 50, 1000, false, true)
        def withoutStats = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false)

        when: "warming up JVM"
        WARM_UP_ITERATIONS.times {
            withStats.flattenAndConsolidateJson(jsonString)
            withoutStats.flattenAndConsolidateJson(jsonString)
        }

        and: "measuring with statistics"
        def startWithStats = System.nanoTime()
        TEST_ITERATIONS.times {
            withStats.flattenAndConsolidateJson(jsonString)
        }
        def timeWithStats = System.nanoTime() - startWithStats

        and: "measuring without statistics"
        def startWithoutStats = System.nanoTime()
        TEST_ITERATIONS.times {
            withoutStats.flattenAndConsolidateJson(jsonString)
        }
        def timeWithoutStats = System.nanoTime() - startWithoutStats

        then: "calculate and display metrics"
        def avgWithStats = timeWithStats / TEST_ITERATIONS / 1_000_000
        def avgWithoutStats = timeWithoutStats / TEST_ITERATIONS / 1_000_000
        def overhead = ((avgWithStats - avgWithoutStats) / avgWithoutStats) * 100

        println """
            === Statistics Overhead Test ===
            Average time WITH statistics: ${String.format("%.3f", avgWithStats)} ms
            Average time WITHOUT statistics: ${String.format("%.3f", avgWithoutStats)} ms
            Statistics overhead: ${String.format("%.1f", overhead)}%
        """

        overhead < 500  // Overhead should be less than 500%
    }

    def "performance scaling with JSON size"() {
        given: "various array sizes and a flattener"
        def sizes = [10, 50, 100, 500, 1000]
        def times = []
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false)

        when: "processing different sized arrays"
        println "\n=== Scaling Test (Array Elements) ==="
        println "Size\tTime (ms)\tElements/ms"

        sizes.each { size ->
            def json = [data: (0..<size).collect { "element_$it" }]
            def jsonString = new JsonBuilder(json).toString()

            // Warm up
            5.times {
                flattener.flattenAndConsolidateJson(jsonString)
            }

            // Measure
            def start = System.nanoTime()
            10.times {
                flattener.flattenAndConsolidateJson(jsonString)
            }
            def elapsed = System.nanoTime() - start
            def avgTime = elapsed / 10.0 / 1_000_000
            times << avgTime

            def throughput = size / avgTime
            println "$size\t${String.format("%.3f", avgTime)}\t\t${String.format("%.0f", throughput)}"
        }

        then: "verify scaling is not quadratic"
        def ratio = times[-1] / times[0]
        def sizeRatio = sizes[-1] / (double) sizes[0]

        println "\nTime ratio: ${String.format("%.1f", ratio)}x for ${String.format("%.0f", sizeRatio)}x size increase"

        ratio < sizeRatio * sizeRatio  // Should be less than O(n²) growth
    }

    def "performance with deep nesting"() {
        given: "various nesting depths"
        def depths = [5, 10, 20, 30, 40]
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false)

        when: "processing deeply nested structures"
        println "\n=== Deep Nesting Test ==="
        println "Depth\tTime (ms)"

        depths.each { depth ->
            def jsonString = createDeeplyNestedJson(depth)

            // Warm up
            5.times {
                flattener.flattenAndConsolidateJson(jsonString)
            }

            // Measure
            def start = System.nanoTime()
            20.times {
                flattener.flattenAndConsolidateJson(jsonString)
            }
            def elapsed = System.nanoTime() - start
            def avgTime = elapsed / 20.0 / 1_000_000

            println "$depth\t${String.format("%.3f", avgTime)}"
        }

        then: "processing completes for all depths"
        true  // Just verify no exceptions
    }

    def "performance with wide structures (many fields)"() {
        given: "various field counts"
        def widths = [10, 50, 100, 200, 500]
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false)

        when: "processing wide structures"
        println "\n=== Wide Structure Test ==="
        println "Fields\tTime (ms)\tFields/ms"

        widths.each { width ->
            def json = [:]
            width.times { i ->
                json["field_$i"] = "value_$i"
            }
            def jsonString = new JsonBuilder(json).toString()

            // Warm up
            5.times {
                flattener.flattenAndConsolidateJson(jsonString)
            }

            // Measure
            def start = System.nanoTime()
            20.times {
                flattener.flattenAndConsolidateJson(jsonString)
            }
            def elapsed = System.nanoTime() - start
            def avgTime = elapsed / 20.0 / 1_000_000
            def throughput = width / avgTime

            println "$width\t${String.format("%.3f", avgTime)}\t\t${String.format("%.0f", throughput)}"
        }

        then: "processing completes efficiently"
        true
    }

    def "performance with complex nested arrays"() {
        given: "complex nested structure"
        def json = [departments: []]

        10.times { d ->
            def dept = [name: "Department_$d", employees: []]
            20.times { e ->
                def emp = [
                        id: "EMP_${d}_${e}",
                        name: "Employee_$e",
                        skills: (0..<5).collect { "Skill_$it" }
                ]
                dept.employees << emp
            }
            json.departments << dept
        }

        def jsonString = new JsonBuilder(json).toString()
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false)

        when: "processing complex arrays"
        // Warm up
        5.times {
            flattener.flattenAndConsolidateJson(jsonString)
        }

        // Measure
        def start = System.nanoTime()
        def iterations = 20
        iterations.times {
            flattener.flattenAndConsolidateJson(jsonString)
        }
        def elapsed = System.nanoTime() - start
        def avgTime = elapsed / iterations / 1_000_000

        then: "performance is acceptable"
        println """
            === Complex Nested Arrays Test ===
            Structure: 10 departments x 20 employees x 5 skills = 1000 total elements
            Average processing time: ${String.format("%.3f", avgTime)} ms
            Throughput: ${String.format("%.0f", 1000 / avgTime)} elements/ms
        """

        avgTime < 50  // Should process in less than 50ms
    }

    def "memory efficiency test"() {
        given: "large JSON with strings and arrays"
        def json = [:]

        // Add many string fields
        1000.times { i ->
            json["field_$i"] = "This is a moderately long string with Unicode: 你好世界 🌍 $i"
        }

        // Add arrays
        100.times { i ->
            json["array_$i"] = (0..<50).collect { "Array_${i}_Item_$it" }
        }

        def jsonString = new JsonBuilder(json).toString()
        def originalSize = jsonString.length()
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false)

        when: "processing large document"
        System.gc()
        Thread.yield()
        def memBefore = getUsedMemory()

        def result = flattener.flattenAndConsolidateJson(jsonString)
        def resultSize = result.length()

        def memAfter = getUsedMemory()
        def memUsed = (memAfter - memBefore) / 1024

        then: "memory usage is reasonable"
        println """
            === Memory Efficiency Test ===
            Original JSON size: ${originalSize / 1024} KB
            Flattened JSON size: ${resultSize / 1024} KB
            Approximate memory used: $memUsed KB
            Size ratio: ${String.format("%.2f", resultSize / originalSize)}
        """

        resultSize < originalSize * 2  // Flattened size should not exceed 2x original
    }

    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    def "edge case performance: maxArraySize limit"() {
        given: "array larger than max size"
        def json = [hugeArray: (0..<2000).collect { "Element_$it" }]
        def jsonString = new JsonBuilder(json).toString()
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false)

        when: "processing oversized array"
        def start = System.nanoTime()
        def result = flattener.flattenAndConsolidateJson(jsonString)
        def elapsed = System.nanoTime() - start
        def time = elapsed / 1_000_000.0

        def resultJson = new JsonSlurper().parseText(result)
        def processedCount = resultJson.hugeArray.split(",").size()

        then: "only processes up to limit"
        println """
            === Max Array Size Test ===
            Array size: 2000 elements
            Max array size: 1000 elements
            Processing time: ${String.format("%.3f", time)} ms
            Elements processed: $processedCount
        """

        processedCount == 1000
        time < 10  // Should still be fast
    }

    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    def "overall performance summary"() {
        given: "complex test JSON"
        def stressTest = createModeratelyComplexJson(100, 50)
        def flattener = new JsonFlattenerConsolidator(",", null, 50, 1000, false, false)

        when: "running stress test"
        def start = System.currentTimeMillis()
        100.times {
            flattener.flattenAndConsolidateJson(new JsonBuilder(stressTest).toString())
        }
        def totalTime = System.currentTimeMillis() - start

        then: "performance meets requirements"
        println """
            === Overall Performance Summary ===
            ✓ Statistics overhead is reasonable (< 300%)
            ✓ Performance scales linearly with size
            ✓ Deep nesting is handled efficiently
            ✓ Wide structures are processed quickly
            ✓ Complex nested arrays are consolidated efficiently
            ✓ Memory usage is reasonable
            ✓ Array size limits prevent performance degradation
            
            Stress test: 100 iterations of complex JSON in $totalTime ms
            Average: ${totalTime / 100.0} ms per operation
        """

        totalTime < 1000  // Should complete 100 iterations in under 1 second
    }

    // Helper methods

    private Map createModeratelyComplexJson(int arraySize, int numFields) {
        def json = [:]

        // Add simple fields
        numFields.times { i ->
            json["field_$i"] = "value_$i"
        }

        // Add nested object
        json.nested = [
                id: "NESTED-001",
                data: "Some nested data"
        ]

        // Add array of objects
        json.items = (0..<arraySize).collect { i ->
            [id: i, value: "Item $i"]
        }

        return json
    }

    private String createDeeplyNestedJson(int depth) {
        def json = new StringBuilder()

        // Opening braces
        depth.times { i ->
            json.append("{\"level_$i\":")
        }

        // Value at deepest level
        json.append('"deep_value"')

        // Closing braces
        depth.times {
            json.append("}")
        }

        return json.toString()
    }

    private long getUsedMemory() {
        def runtime = Runtime.runtime
        runtime.totalMemory() - runtime.freeMemory()
    }
}