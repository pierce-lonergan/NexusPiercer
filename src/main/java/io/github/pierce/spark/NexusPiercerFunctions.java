package io.github.pierce.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pierce.JsonFlattenerConsolidator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * NexusPiercerFunctions - Spark SQL functions for JSON flattening and processing.
 * ...
 */
public class NexusPiercerFunctions {



    public static final String DEFAULT_DELIMITER = ",";
    public static final int DEFAULT_MAX_NESTING = 50;
    public static final int DEFAULT_MAX_ARRAY_SIZE = 1000;
    private static final ObjectMapper STRICT_JSON_MAPPER = new ObjectMapper();




    public static UserDefinedFunction flattenJson = udf(
            (String json) -> {
                if (json == null || json.trim().isEmpty()) return null;
                try {
                    JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                            DEFAULT_DELIMITER, null, DEFAULT_MAX_NESTING, DEFAULT_MAX_ARRAY_SIZE, false
                    );
                    return flattener.flattenAndConsolidateJson(json);
                } catch (Exception e) {
                    return null;
                }
            },
            DataTypes.StringType
    );

    public static UserDefinedFunction flattenJsonWithDelimiter = udf(
            (String json, String delimiter) -> {
                if (json == null || json.trim().isEmpty()) return null;
                try {
                    JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                            delimiter != null ? delimiter : DEFAULT_DELIMITER,
                            null, DEFAULT_MAX_NESTING, DEFAULT_MAX_ARRAY_SIZE, false
                    );
                    return flattener.flattenAndConsolidateJson(json);
                } catch (Exception e) {
                    return null;
                }
            },
            DataTypes.StringType
    );

    public static UserDefinedFunction flattenJsonWithStats = udf(
            (String json) -> {
                if (json == null || json.trim().isEmpty()) return null;
                try {
                    JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                            DEFAULT_DELIMITER, null, DEFAULT_MAX_NESTING, DEFAULT_MAX_ARRAY_SIZE, false, true
                    );
                    return flattener.flattenAndConsolidateJson(json);
                } catch (Exception e) {
                    return null;
                }
            },
            DataTypes.StringType
    );



    public static UserDefinedFunction extractJsonArray = udf(
            (String json, String arrayPath) -> {
                if (json == null || arrayPath == null) return null;
                try {
                    JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                            DEFAULT_DELIMITER, null, DEFAULT_MAX_NESTING, DEFAULT_MAX_ARRAY_SIZE, false
                    );
                    String flattened = flattener.flattenAndConsolidateJson(json);


                    JsonNode obj = STRICT_JSON_MAPPER.readTree(flattened);
                    String key = arrayPath.replace(".", "_");
                    JsonNode valueNode = obj.get(key);
                    return (valueNode != null && !valueNode.isNull()) ? valueNode.asText() : null;
                } catch (Exception e) {
                    return null;
                }
            },
            DataTypes.StringType
    );

    public static UserDefinedFunction jsonArrayCount = udf(
            (String json, String arrayPath) -> {
                if (json == null || arrayPath == null) return null;
                try {
                    JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                            DEFAULT_DELIMITER, null, DEFAULT_MAX_NESTING, DEFAULT_MAX_ARRAY_SIZE, false, true
                    );
                    String flattened = flattener.flattenAndConsolidateJson(json);

                    JsonNode obj = STRICT_JSON_MAPPER.readTree(flattened);
                    String countKey = arrayPath.replace(".", "_") + "_count";
                    JsonNode countNode = obj.get(countKey);
                    return (countNode != null && !countNode.isNull()) ? countNode.asLong() : null;
                } catch (Exception e) {
                    return null;
                }
            },
            DataTypes.LongType
    );

    public static UserDefinedFunction jsonArrayDistinctCount = udf(
            (String json, String arrayPath) -> {
                if (json == null || arrayPath == null) return null;
                try {
                    JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                            DEFAULT_DELIMITER, null, DEFAULT_MAX_NESTING, DEFAULT_MAX_ARRAY_SIZE, false, true
                    );
                    String flattened = flattener.flattenAndConsolidateJson(json);

                    JsonNode obj = STRICT_JSON_MAPPER.readTree(flattened);
                    String distinctKey = arrayPath.replace(".", "_") + "_distinct_count";
                    JsonNode distinctNode = obj.get(distinctKey);
                    return (distinctNode != null && !distinctNode.isNull()) ? distinctNode.asLong() : null;
                } catch (Exception e) {
                    return null;
                }
            },
            DataTypes.LongType
    );



    public static UserDefinedFunction explodeJsonArray = udf(
            (String json, String explosionPath) -> {
                if (json == null || explosionPath == null) return null;
                try {
                    JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                            DEFAULT_DELIMITER, null, DEFAULT_MAX_NESTING, DEFAULT_MAX_ARRAY_SIZE,
                            false, true, explosionPath
                    );
                    List<String> exploded = flattener.flattenAndExplodeJson(json);
                    return exploded.toArray(new String[0]);
                } catch (Exception e) {
                    return new String[]{null};
                }
            },
            DataTypes.createArrayType(DataTypes.StringType)
    );


    /**
     * Validate if JSON is well-formed.
     */
    public static UserDefinedFunction isValidJson = udf(
            (String json) -> {
                if (json == null) {
                    return false;
                }
                String trimmedJson = json.trim();
                if (trimmedJson.isEmpty() || (!trimmedJson.startsWith("{") && !trimmedJson.startsWith("["))) {
                    return false;
                }
                try {

                    STRICT_JSON_MAPPER.readTree(trimmedJson);
                    return true;
                } catch (Exception e) {
                    return false;
                }
            },
            DataTypes.BooleanType
    );

    /**
     * Get JSON parsing error message.
     */
    public static UserDefinedFunction getJsonError = udf(
            (String json) -> {
                if (json == null) {
                    return "JSON is null";
                }
                String trimmedJson = json.trim();
                if (trimmedJson.isEmpty()) {
                    return "JSON is empty";
                }
                if (!trimmedJson.startsWith("{") && !trimmedJson.startsWith("[")) {
                    return "JSON text must start with '{' or '['";
                }

                try {

                    STRICT_JSON_MAPPER.readTree(trimmedJson);
                    return "";
                } catch (Exception e) {

                    String message = e.getMessage();
                    if (message != null) {

                        int end = message.indexOf("\n at [");
                        return end != -1 ? message.substring(0, end) : message;
                    }
                    return "Unknown JSON parsing error";
                }
            },
            DataTypes.StringType
    );



    public static UserDefinedFunction extractNestedField = udf(
            (String json, String fieldPath) -> {
                if (json == null || fieldPath == null) return null;
                try {
                    JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                            DEFAULT_DELIMITER, null, DEFAULT_MAX_NESTING, DEFAULT_MAX_ARRAY_SIZE, false
                    );
                    String flattened = flattener.flattenAndConsolidateJson(json);

                    JsonNode obj = STRICT_JSON_MAPPER.readTree(flattened);
                    String key = fieldPath.replace(".", "_");
                    JsonNode fieldNode = obj.get(key);
                    return (fieldNode != null && !fieldNode.isNull()) ? fieldNode.asText() : null;
                } catch (Exception e) {
                    return null;
                }
            },
            DataTypes.StringType
    );



    public static Column flattenJson(Column jsonColumn) {
        return flattenJson.apply(jsonColumn);
    }
    public static Column flattenJson(Column jsonColumn, String delimiter) {
        return flattenJsonWithDelimiter.apply(jsonColumn, lit(delimiter));
    }
    public static Column flattenJsonWithStatistics(Column jsonColumn) {
        return flattenJsonWithStats.apply(jsonColumn);
    }
    public static Column extractArray(Column jsonColumn, String arrayPath) {
        return extractJsonArray.apply(jsonColumn, lit(arrayPath));
    }
    public static Column arrayCount(Column jsonColumn, String arrayPath) {
        return jsonArrayCount.apply(jsonColumn, lit(arrayPath));
    }
    public static Column arrayDistinctCount(Column jsonColumn, String arrayPath) {
        return jsonArrayDistinctCount.apply(jsonColumn, lit(arrayPath));
    }
    public static Column isValid(Column jsonColumn) {
        return isValidJson.apply(jsonColumn);
    }
    public static Column jsonError(Column jsonColumn) {
        return getJsonError.apply(jsonColumn);
    }
    public static Column extractField(Column jsonColumn, String fieldPath) {
        return extractNestedField.apply(jsonColumn, lit(fieldPath));
    }



    public static void registerAll(org.apache.spark.sql.SparkSession spark) {
        spark.udf().register("flatten_json", flattenJson);
        spark.udf().register("flatten_json_with_delimiter", flattenJsonWithDelimiter);
        spark.udf().register("flatten_json_with_stats", flattenJsonWithStats);
        spark.udf().register("extract_json_array", extractJsonArray);
        spark.udf().register("json_array_count", jsonArrayCount);
        spark.udf().register("json_array_distinct_count", jsonArrayDistinctCount);
        spark.udf().register("explode_json_array", explodeJsonArray);
        spark.udf().register("is_valid_json", isValidJson);
        spark.udf().register("get_json_error", getJsonError);
        spark.udf().register("extract_nested_field", extractNestedField);
    }
    public static void register(org.apache.spark.sql.SparkSession spark, String... functionNames) {
        for (String name : functionNames) {
            switch (name) {
                case "flatten_json":
                    spark.udf().register(name, flattenJson);
                    break;
                case "flatten_json_with_delimiter":
                    spark.udf().register(name, flattenJsonWithDelimiter);
                    break;
                case "flatten_json_with_stats":
                    spark.udf().register(name, flattenJsonWithStats);
                    break;
                case "extract_json_array":
                    spark.udf().register(name, extractJsonArray);
                    break;
                case "json_array_count":
                    spark.udf().register(name, jsonArrayCount);
                    break;
                case "json_array_distinct_count":
                    spark.udf().register(name, jsonArrayDistinctCount);
                    break;
                case "explode_json_array":
                    spark.udf().register(name, explodeJsonArray);
                    break;
                case "is_valid_json":
                    spark.udf().register(name, isValidJson);
                    break;
                case "get_json_error":
                    spark.udf().register(name, getJsonError);
                    break;
                case "extract_nested_field":
                    spark.udf().register(name, extractNestedField);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown function: " + name);
            }
        }
    }
}