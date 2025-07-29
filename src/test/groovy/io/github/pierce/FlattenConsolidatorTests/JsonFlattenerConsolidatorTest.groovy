package io.github.pierce.FlattenConsolidatorTests

import io.github.pierce.JsonFlattenerConsolidator
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Groovy/Spock test suite for JsonFlattenerConsolidator
 *
 * Improvements over Java version:
 * - Uses Spock framework for more expressive tests
 * - Data-driven testing with @Unroll
 * - More concise assertions with Groovy truth
 * - Better test organization with given/when/then blocks
 */
class JsonFlattenerConsolidatorTest extends Specification {

    JsonFlattenerConsolidator flattenerConsolidator

    def setup() {
        flattenerConsolidator = new JsonFlattenerConsolidator(",", "null", 50, 1000, false)
    }

    def "test flatten simple JSON"() {
        given: "a simple JSON with name and age"
        def input = '{"name": "John", "age": 30}'

        when: "flattening the JSON"
        def result = flattenerConsolidator.flattenAndConsolidateJson(input)
        def resultJson = new JsonSlurper().parseText(result)

        then: "fields are preserved at top level"
        resultJson.name == "John"
        resultJson.age == 30
    }

    def "test flatten nested JSON"() {
        given: "a nested JSON structure"
        def input = '{"person": {"name": "John", "address": {"city": "NYC"}}}'

        when: "flattening the JSON"
        def result = flattenerConsolidator.flattenAndConsolidateJson(input)
        def resultJson = new JsonSlurper().parseText(result)

        then: "nested fields are flattened with underscore separator"
        resultJson.person_name == "John"
        resultJson.person_address_city == "NYC"
    }

    def "test flatten array of primitives"() {
        given: "JSON with numeric array"
        def input = '{"numbers": [1, 2, 3, 4, 5]}'

        when: "flattening the JSON"
        def result = flattenerConsolidator.flattenAndConsolidateJson(input)
        def resultJson = new JsonSlurper().parseText(result)

        then: "array is consolidated to string with statistics"
        resultJson.numbers == "1,2,3,4,5"
        resultJson.numbers_count == 5
        resultJson.numbers_distinct_count == 5
    }

    def "test flatten array of objects"() {
        given: "JSON with array of user objects"
        def input = '{"users": [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]}'

        when: "flattening the JSON"
        def result = flattenerConsolidator.flattenAndConsolidateJson(input)
        def resultJson = new JsonSlurper().parseText(result)

        then: "object arrays are consolidated by field"
        resultJson.users_name == "John,Jane"
        resultJson.users_age == "30,25"
        resultJson.users_name_count == 2
        resultJson.users_age_count == 2
    }

    def "test handle null values"() {
        given: "JSON with null values and custom placeholder"
        def consolidator = new JsonFlattenerConsolidator(",", "NULL", 50, 1000, false)
        def input = '{"value": null, "array": [1, null, 3]}'

        when: "flattening the JSON"
        def result = consolidator.flattenAndConsolidateJson(input)
        def resultJson = new JsonSlurper().parseText(result)

        then: "nulls are replaced with placeholder"
        resultJson.value == "NULL"
        resultJson.array == "1,NULL,3"
    }

    def "test array size limit"() {
        given: "consolidator with small array limit"
        def consolidator = new JsonFlattenerConsolidator(",", null, 50, 3, false)
        def input = '{"numbers": [1, 2, 3, 4, 5]}'

        when: "flattening the JSON"
        def result = consolidator.flattenAndConsolidateJson(input)
        def resultJson = new JsonSlurper().parseText(result)

        then: "array is truncated to limit"
        resultJson.numbers == "1,2,3"
    }

    def "test consolidate with matrix denotors"() {
        given: "consolidator with matrix denotors enabled"
        def consolidator = new JsonFlattenerConsolidator(",", null, 50, 1000, true)
        def input = '{"matrix": [[1, 2], [3, 4]]}'

        when: "flattening the JSON"
        def result = consolidator.flattenAndConsolidateJson(input)
        def resultJson = new JsonSlurper().parseText(result)

        then: "matrix indices are included"
        resultJson.matrix.contains("[")
    }

    @Unroll
    def "test edge cases: #scenario"() {
        when: "processing edge case input"
        def result = flattenerConsolidator.flattenAndConsolidateJson(input)

        then: "result matches expected"
        result == expected

        where:
        scenario          | input                      | expected
        "empty JSON"      | "{}"                      | "{}"
        "null input"      | null                      | "{}"
        "invalid JSON"    | "{invalid json}"          | "{}"
    }

    def "test statistics generation"() {
        given: "JSON with string array"
        def input = '{"words": ["hello", "world", "hello", "test"]}'

        when: "flattening the JSON"
        def result = flattenerConsolidator.flattenAndConsolidateJson(input)
        def resultJson = new JsonSlurper().parseText(result)

        then: "statistics are correctly calculated"
        resultJson.words_count == 4
        resultJson.words_distinct_count == 3
        resultJson.words_min_length == 4  // "test"
        resultJson.words_max_length == 5  // "hello" or "world"
        Math.abs(resultJson.words_avg_length - 4.75) < 0.01
        resultJson.words_type == "string_list_consolidated"
    }

    @Unroll
    def "test mixed types: #description"() {
        when: "processing mixed type input"
        def result = flattenerConsolidator.flattenAndConsolidateJson(input)
        def resultJson = new JsonSlurper().parseText(result)

        then: "result is valid JSON with content"
        result != null
        resultJson.size() > 0

        where:
        description              | input
        "mixed primitive array"  | '{"mixed": [1, "two", true]}'
        "nested object array"    | '{"nested": [{"a": 1}, {"b": 2}]}'
    }
}