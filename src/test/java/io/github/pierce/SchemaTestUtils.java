package io.github.pierce;
import org.apache.avro.Schema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * Utilities for testing schema operations
 */
public class SchemaTestUtils {

    /**
     * Compare two Avro schemas for structural equality
     */
    public static boolean schemasAreEquivalent(Schema schema1, Schema schema2) {
        if (!schema1.getType().equals(schema2.getType())) {
            return false;
        }

        if (schema1.getType() == Schema.Type.RECORD) {
            if (schema1.getFields().size() != schema2.getFields().size()) {
                return false;
            }

            Map<String, Schema> fields1 = new HashMap<>();
            for (Schema.Field field : schema1.getFields()) {
                fields1.put(field.name(), field.schema());
            }

            for (Schema.Field field : schema2.getFields()) {
                if (!fields1.containsKey(field.name())) {
                    return false;
                }
                if (!schemasAreEquivalent(fields1.get(field.name()), field.schema())) {
                    return false;
                }
            }
            return true;
        }

        return schema1.equals(schema2);
    }

    /**
     * Extract all field names from a flattened schema
     */
    public static Set<String> extractFieldNames(Schema schema) {
        Set<String> fieldNames = new HashSet<>();
        if (schema.getType() == Schema.Type.RECORD) {
            for (Schema.Field field : schema.getFields()) {
                fieldNames.add(field.name());
            }
        }
        return fieldNames;
    }

    /**
     * Count statistics fields in a schema
     */
    public static int countStatisticsFields(Schema schema) {
        int count = 0;
        for (Schema.Field field : schema.getFields()) {
            String name = field.name();
            if (name.endsWith("_count") ||
                    name.endsWith("_distinct_count") ||
                    name.endsWith("_min_length") ||
                    name.endsWith("_max_length") ||
                    name.endsWith("_avg_length") ||
                    name.endsWith("_type")) {
                count++;
            }
        }
        return count;
    }

    /**
     * Verify Spark schema contains expected fields
     */
    public static void assertSparkSchemaContainsFields(StructType schema, String... expectedFields) {
        Set<String> actualFields = new HashSet<>(Arrays.asList(schema.fieldNames()));
        for (String expected : expectedFields) {
            if (!actualFields.contains(expected)) {
                throw new AssertionError("Expected field '" + expected + "' not found in schema");
            }
        }
    }

    /**
     * Get field type from Spark schema
     */
    public static DataType getFieldType(StructType schema, String fieldName) {
        for (StructField field : schema.fields()) {
            if (field.name().equals(fieldName)) {
                return field.dataType();
            }
        }
        return null;
    }
}