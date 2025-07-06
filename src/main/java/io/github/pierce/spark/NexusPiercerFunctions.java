package io.github.pierce.spark;

import io.github.pierce.JsonFlattenerConsolidator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * NexusPiercerFunctions - Spark SQL functions for JSON flattening and processing.
 *
 * These functions can be used directly in Spark SQL queries or DataFrame operations
 * without setting up a full pipeline.
 *
 * Example usage:
 * <pre>
 * import static io.github.pierce.spark.NexusPiercerFunctions.*;
 *
 * // Register functions for SQL
 * NexusPiercerFunctions.registerAll(spark);
 *
 * // Use in DataFrame API
 * df.withColumn("flattened", flattenJson(col("json_column")))
 *   .withColumn("array_count", jsonArrayCount(col("json_column"), "items"))
 *   .withColumn("exploded", explodeJsonArray(col("json_column"), "items"));
 *
 * // Use in SQL
 * spark.sql("""
 *   SELECT
 *     flatten_json(json_data) as flattened,
 *     json_array_count(json_data, 'items') as item_count,
 *     json_extract_array(json_data, 'items') as items_array
 *   FROM json_table
 * """);
 * </pre>
 */
public class NexusPiercerFunctions {

    // ===== CONFIGURATION CONSTANTS =====

    public static final String DEFAULT_DELIMITER = ",";
    public static final int DEFAULT_MAX_NESTING = 50;
    public static final int DEFAULT_MAX_ARRAY_SIZE = 1000;

    // ===== JSON FLATTENING FUNCTIONS =====

    /**
     * Flatten and consolidate JSON with default settings
     */
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

    /**
     * Flatten and consolidate JSON with custom delimiter
     */
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

    /**
     * Flatten JSON with array statistics
     */
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

    // ===== ARRAY EXTRACTION FUNCTIONS =====

    /**
     * Extract a specific array from JSON as a delimited string
     */
    public static UserDefinedFunction extractJsonArray = udf(
            (String json, String arrayPath) -> {
                if (json == null || arrayPath == null) return null;
                try {
                    JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                            DEFAULT_DELIMITER, null, DEFAULT_MAX_NESTING, DEFAULT_MAX_ARRAY_SIZE, false
                    );
                    String flattened = flattener.flattenAndConsolidateJson(json);

                    // Parse flattened JSON and extract the array field
                    org.json.JSONObject obj = new org.json.JSONObject(flattened);
                    String key = arrayPath.replace(".", "_");
                    return obj.optString(key, null);
                } catch (Exception e) {
                    return null;
                }
            },
            DataTypes.StringType
    );

    /**
     * Count elements in a JSON array
     */
    public static UserDefinedFunction jsonArrayCount = udf(
            (String json, String arrayPath) -> {
                if (json == null || arrayPath == null) return null;
                try {
                    JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                            DEFAULT_DELIMITER, null, DEFAULT_MAX_NESTING, DEFAULT_MAX_ARRAY_SIZE, false, true
                    );
                    String flattened = flattener.flattenAndConsolidateJson(json);

                    org.json.JSONObject obj = new org.json.JSONObject(flattened);
                    String countKey = arrayPath.replace(".", "_") + "_count";
                    return obj.has(countKey) ? obj.getLong(countKey) : null;
                } catch (Exception e) {
                    return null;
                }
            },
            DataTypes.LongType
    );

    /**
     * Get distinct count of elements in a JSON array
     */
    public static UserDefinedFunction jsonArrayDistinctCount = udf(
            (String json, String arrayPath) -> {
                if (json == null || arrayPath == null) return null;
                try {
                    JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                            DEFAULT_DELIMITER, null, DEFAULT_MAX_NESTING, DEFAULT_MAX_ARRAY_SIZE, false, true
                    );
                    String flattened = flattener.flattenAndConsolidateJson(json);

                    org.json.JSONObject obj = new org.json.JSONObject(flattened);
                    String distinctKey = arrayPath.replace(".", "_") + "_distinct_count";
                    return obj.has(distinctKey) ? obj.getLong(distinctKey) : null;
                } catch (Exception e) {
                    return null;
                }
            },
            DataTypes.LongType
    );

    // ===== EXPLOSION FUNCTIONS =====

    /**
     * Explode JSON array into multiple JSON strings
     */
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

    // ===== VALIDATION FUNCTIONS =====

    /**
     * Validate if JSON is well-formed
     */
    public static UserDefinedFunction isValidJson = udf(
            (String json) -> {
                if (json == null || json.trim().isEmpty()) return false;
                try {
                    new org.json.JSONObject(json);
                    return true;
                } catch (Exception e) {
                    try {
                        new org.json.JSONArray(json);
                        return true;
                    } catch (Exception e2) {
                        return false;
                    }
                }
            },
            DataTypes.BooleanType
    );

    /**
     * Get JSON parsing error message
     */
    public static UserDefinedFunction getJsonError = udf(
            (String json) -> {
                if (json == null || json.trim().isEmpty()) return "JSON is null or empty";
                try {
                    new org.json.JSONObject(json);
                    return null;
                } catch (Exception e) {
                    try {
                        new org.json.JSONArray(json);
                        return null;
                    } catch (Exception e2) {
                        return e.getMessage();
                    }
                }
            },
            DataTypes.StringType
    );

    // ===== NESTED FIELD EXTRACTION =====

    /**
     * Extract a nested field value from JSON
     */
    public static UserDefinedFunction extractNestedField = udf(
            (String json, String fieldPath) -> {
                if (json == null || fieldPath == null) return null;
                try {
                    JsonFlattenerConsolidator flattener = new JsonFlattenerConsolidator(
                            DEFAULT_DELIMITER, null, DEFAULT_MAX_NESTING, DEFAULT_MAX_ARRAY_SIZE, false
                    );
                    String flattened = flattener.flattenAndConsolidateJson(json);

                    org.json.JSONObject obj = new org.json.JSONObject(flattened);
                    String key = fieldPath.replace(".", "_");
                    return obj.has(key) ? obj.get(key).toString() : null;
                } catch (Exception e) {
                    return null;
                }
            },
            DataTypes.StringType
    );

    // ===== COLUMN-BASED FUNCTIONS =====

    /**
     * Flatten JSON column with default settings
     */
    public static Column flattenJson(Column jsonColumn) {
        return flattenJson.apply(jsonColumn);
    }

    /**
     * Flatten JSON column with custom delimiter
     */
    public static Column flattenJson(Column jsonColumn, String delimiter) {
        return flattenJsonWithDelimiter.apply(jsonColumn, lit(delimiter));
    }

    /**
     * Flatten JSON column with statistics
     */
    public static Column flattenJsonWithStatistics(Column jsonColumn) {
        return flattenJsonWithStats.apply(jsonColumn);
    }

    /**
     * Extract array from JSON column
     */
    public static Column extractArray(Column jsonColumn, String arrayPath) {
        return extractJsonArray.apply(jsonColumn, lit(arrayPath));
    }

    /**
     * Count array elements in JSON column
     */
    public static Column arrayCount(Column jsonColumn, String arrayPath) {
        return jsonArrayCount.apply(jsonColumn, lit(arrayPath));
    }

    /**
     * Get distinct count of array elements
     */
    public static Column arrayDistinctCount(Column jsonColumn, String arrayPath) {
        return jsonArrayDistinctCount.apply(jsonColumn, lit(arrayPath));
    }

    /**
     * Validate JSON column
     */
    public static Column isValid(Column jsonColumn) {
        return isValidJson.apply(jsonColumn);
    }

    /**
     * Get JSON error for invalid records
     */
    public static Column jsonError(Column jsonColumn) {
        return getJsonError.apply(jsonColumn);
    }

    /**
     * Extract nested field from JSON
     */
    public static Column extractField(Column jsonColumn, String fieldPath) {
        return extractNestedField.apply(jsonColumn, lit(fieldPath));
    }

    // ===== REGISTRATION FOR SPARK SQL =====

    /**
     * Register all functions with Spark SQL
     */
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

    /**
     * Register specific functions
     */
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