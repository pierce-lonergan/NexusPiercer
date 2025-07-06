package io.github.pierce;

// CreateSparkStructFromAvroSchema.java - Optimized and Corrected version

import org.apache.avro.Schema;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Optimized converter from Avro Schema to Spark StructType.
 * Provides both cached and non-cached conversion methods.
 */
public class CreateSparkStructFromAvroSchema {

    private static final Logger LOG = LoggerFactory.getLogger(CreateSparkStructFromAvroSchema.class);

    // Cache for converted schemas for performance in standard operations
    private static final Map<String, StructType> structTypeCache = new ConcurrentHashMap<>();

    /**
     * Convert Avro schema to Spark StructType WITH caching.
     * This is the default, high-performance method.
     *
     * @param avroSchema The Avro schema to convert.
     * @return A cached or newly created Spark StructType.
     */
    public static StructType convertNestedAvroSchemaToSparkSchema(Schema avroSchema) {
        String cacheKey = avroSchema.getFullName() + ":" + avroSchema.hashCode(); // More robust key
        return structTypeCache.computeIfAbsent(cacheKey, k -> doConvert(avroSchema));
    }

    /**
     * Convert Avro schema to Spark StructType WITHOUT using the cache.
     * This ensures a new StructType object is created every time.
     * Useful when the calling context (like AvroSchemaLoader) manages its own caching.
     *
     * @param avroSchema The Avro schema to convert.
     * @return A new Spark StructType instance.
     */
    public static StructType convertNestedAvroSchemaToSparkSchemaNoCache(Schema avroSchema) {
        // Directly call the private conversion logic, bypassing the cache
        return doConvert(avroSchema);
    }

    /**
     * Private core conversion logic.
     * This implementation assumes a "flattened" context where all fields should be nullable
     * for robustness against missing nested data.
     *
     * @param avroSchema The Avro schema to convert.
     * @return A new Spark StructType.
     */
    private static StructType doConvert(Schema avroSchema) {
        List<StructField> fields = new ArrayList<>();

        for (Schema.Field field : avroSchema.getFields()) {
            DataType sparkType = convertAvroTypeToSparkType(field.schema());

            // Forcing all fields to be nullable is a robust choice for flattened schemas
            boolean isNullable = true;

            StructField structField = DataTypes.createStructField(
                    field.name(),
                    sparkType,
                    isNullable,
                    createMetadata(field)
            );
            fields.add(structField);
        }

        StructType result = DataTypes.createStructType(fields);
        LOG.debug("Converted Avro schema {} to Spark StructType with {} fields",
                avroSchema.getName(), fields.size());

        return result;
    }


    /**
     * Convert Avro type to Spark DataType
     */
    private static DataType convertAvroTypeToSparkType(Schema avroType) {
        // First, handle the UNION case which is common for nullable fields
        if (avroType.getType() == Schema.Type.UNION) {
            Schema nonNullType = getNonNullType(avroType);
            if (nonNullType != null) {
                // Recursively convert the actual type within the union
                return convertAvroTypeToSparkType(nonNullType);
            }
            LOG.warn("Encountered a union containing only NULL. Defaulting to StringType.");
            return DataTypes.StringType;
        }

        switch (avroType.getType()) {
            case STRING:
            case ENUM:
                return DataTypes.StringType;
            case INT:
                return DataTypes.IntegerType;
            case LONG:
                return DataTypes.LongType;
            case FLOAT:
                return DataTypes.FloatType;
            case DOUBLE:
                return DataTypes.DoubleType;
            case BOOLEAN:
                return DataTypes.BooleanType;
            case BYTES:
            case FIXED:
                return DataTypes.BinaryType;
            case NULL:
                // A standalone NULL type (not in a union) should be NullType.
                // However, this is rare in Avro schemas.
                return DataTypes.NullType;
            case ARRAY:
                DataType elementType = convertAvroTypeToSparkType(avroType.getElementType());
                // Array elements are considered nullable by default for flexibility.
                return DataTypes.createArrayType(elementType, true);
            case MAP:
                DataType valueType = convertAvroTypeToSparkType(avroType.getValueType());
                // Map values are considered nullable by default for flexibility.
                return DataTypes.createMapType(DataTypes.StringType, valueType, true);
            case RECORD:
                // This method is designed for already-flattened schemas, so nested records are not expected.
                // If one is encountered, log a warning and default to StringType.
                LOG.warn("Encountered nested RECORD type in what should be a flattened schema: {}. Defaulting to StringType.", avroType.getName());
                return DataTypes.StringType;
            default:
                LOG.warn("Unknown Avro type: {}, defaulting to StringType", avroType.getType());
                return DataTypes.StringType;
        }
    }

    /**
     * Create metadata for Spark field from Avro field
     */
    private static Metadata createMetadata(Schema.Field field) {
        MetadataBuilder builder = new MetadataBuilder();

        if (field.doc() != null && !field.doc().isEmpty()) {
            builder.putString("comment", field.doc());
        }

        // Add Avro-specific metadata for traceability
        builder.putString("avro.field.name", field.name());

        if (field.defaultVal() != null) {
            builder.putString("avro.field.default", field.defaultVal().toString());
        }

        return builder.build();
    }

    /**
     * Get the first non-null type from a union schema.
     */
    private static Schema getNonNullType(Schema union) {
        for (Schema branch : union.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
                return branch;
            }
        }
        return null; // All types in the union were NULL
    }

    /**
     * Clear cache (useful for testing)
     */
    public static void clearCache() {
        structTypeCache.clear();
        LOG.debug("CreateSparkStructFromAvroSchema cache cleared.");
    }
}