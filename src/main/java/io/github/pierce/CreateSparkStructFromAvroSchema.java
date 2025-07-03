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
 * Optimized converter from Avro Schema to Spark StructType
 */
public class CreateSparkStructFromAvroSchema {

    private static final Logger LOG = LoggerFactory.getLogger(CreateSparkStructFromAvroSchema.class);

    // Cache for converted schemas
    private static final Map<String, StructType> structTypeCache = new ConcurrentHashMap<>();

    /**
     * Convert Avro schema to Spark StructType with caching.
     * This implementation assumes a "flattened" context where all fields should be nullable
     * for robustness against missing nested data.
     */
    public static StructType convertNestedAvroSchemaToSparkSchema(Schema avroSchema) {
        String cacheKey = avroSchema.getFullName();

        return structTypeCache.computeIfAbsent(cacheKey, k -> {
            List<StructField> fields = new ArrayList<>();

            for (Schema.Field field : avroSchema.getFields()) {
                DataType sparkType = convertAvroTypeToSparkType(field.schema());

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
        });
    }

    /**
     * Convert Avro type to Spark DataType
     */
    private static DataType convertAvroTypeToSparkType(Schema avroType) {
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
                return DataTypes.NullType;

            case UNION:
                // For unions, use the first non-null type found.
                Schema nonNullType = getNonNullType(avroType);
                if (nonNullType != null) {
                    return convertAvroTypeToSparkType(nonNullType);
                }
                // If the union only contains null, default to StringType as a fallback.
                return DataTypes.StringType;

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

        if (field.doc() != null) {
            builder.putString("comment", field.doc());
        }

        // Add Avro-specific metadata
        builder.putString("avro.field.name", field.name());

        if (field.defaultVal() != null) {
            builder.putString("avro.field.default", field.defaultVal().toString());
        }

        return builder.build();
    }

    /**
     * Get the first non-null type from a union schema.
     * Avro convention often places null as the first type in a nullable union.
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
    }
}