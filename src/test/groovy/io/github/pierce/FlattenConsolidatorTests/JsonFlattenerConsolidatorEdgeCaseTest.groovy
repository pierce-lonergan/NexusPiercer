package io.github.pierce.FlattenConsolidatorTests

import io.github.pierce.JsonFlattenerConsolidator
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import spock.lang.Specification
import spock.lang.Unroll
import spock.lang.Timeout
import java.util.concurrent.TimeUnit

/**
 * Edge case and data quality tests for JsonFlattenerConsolidator in Groovy
 *
 * These tests focus on unusual scenarios, data quality issues, and edge cases
 * that have been discovered in production systems.
 */
class JsonFlattenerConsolidatorEdgeCaseTest extends Specification {

    JsonFlattenerConsolidator flattener

    def setup() {
        flattener = new JsonFlattenerConsolidator(",", "null", 50, 1000, false)
    }

    def "should prevent infinite loops from circular references"() {
        given: "deeply nested JSON simulating circular references"
        def circularJson = '''
            {
                "id": 1,
                "name": "Parent",
                "child": {
                    "id": 2,
                    "name": "Child",
                    "parent": {
                        "id": 1,
                        "name": "Parent",
                        "child": {
                            "id": 2,
                            "name": "Child"
                        }
                    }
                }
            }
        '''

        and: "a flattener with limited depth"
        def limitedDepth = new JsonFlattenerConsolidator(",", "null", 3, 100, false)

        when: "processing the circular structure"
        def result = limitedDepth.flattenAndConsolidateJson(circularJson)
        def resultJson = new JsonSlurper().parseText(result)

        then: "depth limiting prevents infinite loop"
        resultJson.id == 1
        resultJson.child_id == 2
        resultJson.child_parent_id == 1
        resultJson.child_parent_child.contains('"id":2')
    }

    @Unroll
    def "should preserve numeric precision: #description"() {
        when: "processing numeric values"
        def result = flattener.flattenAndConsolidateJson(json)
        def resultJson = new JsonSlurper().parseText(result)

        then: "numeric precision is preserved"
        resultJson[expectedKey].toString() == expectedValue

        where:
        description                    | json                                                | expectedKey | expectedValue
        "Very large number"           | '{"amount": 9007199254740993}'                     | "amount"    | "9007199254740993"
        "High precision decimal"      | '{"rate": 0.123456789012345678901234567890}'      | "rate"      | "0.12345678901234568"
        "Scientific notation"         | '{"value": 1.23e+10}'                              | "value"     | "1.23E10"
        "Negative scientific"         | '{"tiny": 1.23e-10}'                               | "tiny"      | "1.23E-10"
        "Array of mixed formats"      | '{"numbers": [123, 123.456, 1.23e5, 0.0000001]}'  | "numbers"   | "123,123.456,123000.0,1.0E-7"
    }

    def "should preserve various date formats without modification"() {
        given: "JSON with various date formats"
        def json = '''
            {
                "dates": {
                    "iso": "2024-01-15T10:30:00.000Z",
                    "epochMillis": 1705318200000,
                    "epochSeconds": 1705318200,
                    "custom": "15-JAN-2024 10:30:00",
                    "array": [
                        "2024-01-15",
                        "2024-01-16",
                        "2024-01-17"
                    ]
                }
            }
        '''

        when: "flattening date fields"
        def result = flattener.flattenAndConsolidateJson(json)
        def resultJson = new JsonSlurper().parseText(result)

        then: "all date formats are preserved as-is"
        resultJson.dates_iso == "2024-01-15T10:30:00.000Z"
        resultJson.dates_epochMillis == 1705318200000
        resultJson.dates_epochSeconds == 1705318200
        resultJson.dates_custom == "15-JAN-2024 10:30:00"
        resultJson.dates_array == "2024-01-15,2024-01-16,2024-01-17"
    }

    @Unroll
    def "should handle malformed JSON gracefully: #description"() {
        when: "processing malformed JSON"
        def result = flattener.flattenAndConsolidateJson(malformedJson)

        then: "returns empty JSON instead of crashing"
        result == "{}"

        where:
        description            | malformedJson
        "Unclosed string"     | '{"unclosed": "string'
        "Unclosed object"     | '{"unclosed": {"nested":'
        "Unclosed array"      | '[1, 2, 3'
        "JavaScript undefined"| '{"key": undefined}'
        "JavaScript NaN"      | '{"key": NaN}'
        "Not JSON at all"     | 'Some random text'
        "Just null"           | 'null'
        "Just a number"       | '12345'
    }

    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    def "should prevent memory explosion from multiple large arrays"() {
        given: "document with multiple large arrays"
        def doc = new JsonBuilder()
        def data = [:]

        // Create multiple large arrays
        5.times { arrayNum ->
            def array = []
            200.times { i ->
                array << "value_${arrayNum}_${i}"
            }
            data["array${arrayNum}"] = array
        }
        doc(data)

        when: "processing the large document"
        def startMemory = getUsedMemory()
        def result = flattener.flattenAndConsolidateJson(doc.toString())
        def endMemory = getUsedMemory()
        def resultJson = new JsonSlurper().parseText(result)

        then: "each array is consolidated independently"
        5.times { arrayNum ->
            assert resultJson["array${arrayNum}_count"] == 200
            assert resultJson["array${arrayNum}"].split(",").size() == 200
        }

        and: "memory usage is reasonable"
        def memoryUsed = endMemory - startMemory
        memoryUsed < 10 * 1024 * 1024  // Less than 10MB
    }

    def "should handle potentially malicious key names safely"() {
        given: "JSON with potentially dangerous keys"
        def json = '''
            {
                "normal_key": "value1",
                "key'; DROP TABLE users; --": "sql_injection_attempt",
                "key\\" OR \\"1\\"=\\"1": "another_attempt",
                "<script>alert('xss')</script>": "xss_attempt",
                "../../etc/passwd": "path_traversal",
                "key\\nwith\\nnewlines": "multiline",
                "key\\twith\\ttabs": "tabs",
                "key\\u0000with\\u0000nulls": "null_bytes"
            }
        '''

        when: "processing dangerous keys"
        def result = flattener.flattenAndConsolidateJson(json)
        def resultJson = new JsonSlurper().parseText(result)

        then: "all keys are preserved (security handled downstream)"
        resultJson.normal_key == "value1"
        resultJson["key'; DROP TABLE users; --"] == "sql_injection_attempt"
        resultJson["<script>alert('xss')</script>"] != null
        resultJson["__/__/etc/passwd"] == "path_traversal"
    }

    def "should handle base64 encoded binary data"() {
        given: "JSON with base64 data"
        def base64Image = Base64.encoder.encodeToString(new byte[1024])
        def json = """
            {
                "document": {
                    "title": "Test Document",
                    "thumbnail": "${base64Image}",
                    "attachments": [
                        {"name": "file1.pdf", "data": "${base64Image}"},
                        {"name": "file2.pdf", "data": "${base64Image}"}
                    ]
                }
            }
        """

        when: "processing binary data"
        def result = flattener.flattenAndConsolidateJson(json)
        def resultJson = new JsonSlurper().parseText(result)

        then: "binary data is preserved as strings"
        resultJson.document_thumbnail == base64Image
        resultJson.document_attachments_data.contains(base64Image)
        resultJson.document_attachments_data_count == 2
    }

    def "should handle arrays with different types at different positions"() {
        given: "heterogeneous array"
        def json = '''
            {
                "data": [
                    {"type": "A", "value": 123},
                    {"type": "B", "name": "test"},
                    {"type": "C", "flag": true, "extra": "field"},
                    null,
                    {"type": "D"}
                ]
            }
        '''

        when: "processing mixed array"
        def result = flattener.flattenAndConsolidateJson(json)
        def resultJson = new JsonSlurper().parseText(result)

        then: "each field is consolidated separately"
        resultJson.data_type == "A,B,C,D"
        resultJson.data_value == "123"
        resultJson.data_value_count == 1
        resultJson.data_name == "test"
        resultJson.data_name_count == 1
        resultJson.data_flag == "true"
        resultJson.data_extra == "field"
    }

    def "should efficiently handle sparse data with many nulls"() {
        given: "sparse document with 95% nulls"
        def sparseDoc = [:]
        1000.times { i ->
            sparseDoc["field_${i}"] = (i % 20 == 0) ? "value_${i}" : null
        }

        when: "processing sparse data"
        def result = flattener.flattenAndConsolidateJson(new JsonBuilder(sparseDoc).toString())
        def resultJson = new JsonSlurper().parseText(result)

        then: "non-null count is correct"
        def nonNullCount = resultJson.findAll { k, v -> v != "null" }.size()
        nonNullCount == 50  // 5% of 1000
        resultJson.size() == 1000  // All fields present
    }

    def "should handle documents from different schema versions"() {
        given: "documents with evolving schemas"
        def v1Doc = '''
            {
                "version": 1,
                "customerId": "123",
                "amount": 100.50
            }
        '''

        def v2Doc = '''
            {
                "version": 2,
                "customerId": "456",
                "amount": 200.75,
                "currency": "USD",
                "metadata": {
                    "source": "API"
                }
            }
        '''

        def v3Doc = '''
            {
                "version": 3,
                "customerId": 789,
                "amount": "300.00",
                "currency": "EUR",
                "metadata": {
                    "source": "API",
                    "tags": ["important", "rush"]
                }
            }
        '''

        when: "processing different versions"
        def v1Result = new JsonSlurper().parseText(flattener.flattenAndConsolidateJson(v1Doc))
        def v2Result = new JsonSlurper().parseText(flattener.flattenAndConsolidateJson(v2Doc))
        def v3Result = new JsonSlurper().parseText(flattener.flattenAndConsolidateJson(v3Doc))

        then: "V1 has basic fields"
        v1Result.version == 1
        v1Result.customerId == "123"
        v1Result.amount == 100.50
        !v1Result.containsKey("currency")

        and: "V2 has additional fields"
        v2Result.version == 2
        v2Result.containsKey("currency")
        v2Result.metadata_source == "API"

        and: "V3 handles type changes"
        v3Result.version == 3
        v3Result.customerId.toString() == "789"
        v3Result.amount == "300.00"
        v3Result.metadata_tags == "important,rush"
    }

    private long getUsedMemory() {
        def runtime = Runtime.runtime
        runtime.totalMemory() - runtime.freeMemory()
    }
}