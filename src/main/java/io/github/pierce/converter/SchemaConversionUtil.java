package io.github.pierce.converter;


import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility class for converting between Avro and Iceberg schemas.
 *
 * <p>Provides bidirectional conversion and schema analysis utilities.</p>
 */
public final class SchemaConversionUtil {

    private SchemaConversionUtil() {
        // Utility class
    }

    /**
     * Converts an Avro schema to an Iceberg schema.
     *
     * @param avroSchema the Avro schema
     * @return the equivalent Iceberg schema
     */
    public static Schema avroToIceberg(org.apache.avro.Schema avroSchema) {
        return AvroSchemaUtil.toIceberg(avroSchema);
    }

    /**
     * Converts an Iceberg schema to an Avro schema.
     *
     * @param icebergSchema the Iceberg schema
     * @param recordName the name for the Avro record
     * @return the equivalent Avro schema
     */
    public static org.apache.avro.Schema icebergToAvro(Schema icebergSchema, String recordName) {
        return AvroSchemaUtil.convert(icebergSchema, recordName);
    }

    /**
     * Creates an Iceberg schema from field definitions.
     * Automatically assigns field IDs.
     */
    public static Schema createIcebergSchema(List<FieldDefinition> fields) {
        AtomicInteger idCounter = new AtomicInteger(1);
        List<Types.NestedField> icebergFields = new ArrayList<>();

        for (FieldDefinition field : fields) {
            Type type = createIcebergType(field.type(), field.typeParams(), idCounter);
            Types.NestedField nestedField = field.required()
                    ? Types.NestedField.required(idCounter.getAndIncrement(), field.name(), type)
                    : Types.NestedField.optional(idCounter.getAndIncrement(), field.name(), type);
            icebergFields.add(nestedField);
        }

        return new Schema(icebergFields);
    }

    private static Type createIcebergType(FieldType type, Object[] params, AtomicInteger idCounter) {
        return switch (type) {
            case BOOLEAN -> Types.BooleanType.get();
            case INTEGER -> Types.IntegerType.get();
            case LONG -> Types.LongType.get();
            case FLOAT -> Types.FloatType.get();
            case DOUBLE -> Types.DoubleType.get();
            case STRING -> Types.StringType.get();
            case DATE -> Types.DateType.get();
            case TIME -> Types.TimeType.get();
            case TIMESTAMP -> Types.TimestampType.withoutZone();
            case TIMESTAMPTZ -> Types.TimestampType.withZone();
            case TIMESTAMP_NANO -> Types.TimestampNanoType.withoutZone();
            case TIMESTAMP_NANOTZ -> Types.TimestampNanoType.withZone();
            case UUID -> Types.UUIDType.get();
            case BINARY -> Types.BinaryType.get();
            case DECIMAL -> {
                int precision = params != null && params.length > 0 ? (int) params[0] : 38;
                int scale = params != null && params.length > 1 ? (int) params[1] : 0;
                yield Types.DecimalType.of(precision, scale);
            }
            case FIXED -> {
                int length = params != null && params.length > 0 ? (int) params[0] : 16;
                yield Types.FixedType.ofLength(length);
            }
            case LIST -> {
                @SuppressWarnings("unchecked")
                FieldDefinition elementDef = params != null && params.length > 0
                        ? (FieldDefinition) params[0]
                        : new FieldDefinition("element", FieldType.STRING, false);
                Type elementType = createIcebergType(elementDef.type(), elementDef.typeParams(), idCounter);
                yield Types.ListType.ofOptional(idCounter.getAndIncrement(), elementType);
            }
            case MAP -> {
                @SuppressWarnings("unchecked")
                FieldDefinition keyDef = params != null && params.length > 0
                        ? (FieldDefinition) params[0]
                        : new FieldDefinition("key", FieldType.STRING, true);
                @SuppressWarnings("unchecked")
                FieldDefinition valueDef = params != null && params.length > 1
                        ? (FieldDefinition) params[1]
                        : new FieldDefinition("value", FieldType.STRING, false);
                Type keyType = createIcebergType(keyDef.type(), keyDef.typeParams(), idCounter);
                Type valueType = createIcebergType(valueDef.type(), valueDef.typeParams(), idCounter);
                yield Types.MapType.ofOptional(
                        idCounter.getAndIncrement(),
                        idCounter.getAndIncrement(),
                        keyType,
                        valueType);
            }
            case STRUCT -> {
                @SuppressWarnings("unchecked")
                List<FieldDefinition> structFields = params != null && params.length > 0
                        ? (List<FieldDefinition>) params[0]
                        : List.of();
                List<Types.NestedField> nestedFields = new ArrayList<>();
                for (FieldDefinition sf : structFields) {
                    Type fieldType = createIcebergType(sf.type(), sf.typeParams(), idCounter);
                    nestedFields.add(sf.required()
                            ? Types.NestedField.required(idCounter.getAndIncrement(), sf.name(), fieldType)
                            : Types.NestedField.optional(idCounter.getAndIncrement(), sf.name(), fieldType));
                }
                yield Types.StructType.of(nestedFields);
            }
        };
    }

    /**
     * Analyzes an Avro schema and returns information about its fields.
     */
    public static List<FieldInfo> analyzeAvroSchema(org.apache.avro.Schema schema) {
        if (schema.getType() != org.apache.avro.Schema.Type.RECORD) {
            throw new IllegalArgumentException("Expected RECORD type, got: " + schema.getType());
        }

        List<FieldInfo> fields = new ArrayList<>();

        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            org.apache.avro.Schema fieldSchema = field.schema();
            boolean nullable = isNullable(fieldSchema);
            org.apache.avro.Schema effectiveSchema = unwrapNullable(fieldSchema);
            LogicalType logicalType = effectiveSchema.getLogicalType();

            fields.add(new FieldInfo(
                    field.name(),
                    effectiveSchema.getType().name(),
                    logicalType != null ? logicalType.getName() : null,
                    nullable,
                    field.hasDefaultValue()
            ));
        }

        return fields;
    }

    /**
     * Analyzes an Iceberg schema and returns information about its fields.
     */
    public static List<FieldInfo> analyzeIcebergSchema(Schema schema) {
        List<FieldInfo> fields = new ArrayList<>();

        for (Types.NestedField field : schema.columns()) {
            Type type = field.type();
            String logicalType = null;

            if (type instanceof Types.DecimalType dt) {
                logicalType = String.format("decimal(%d,%d)", dt.precision(), dt.scale());
            } else if (type instanceof Types.TimestampType tt) {
                logicalType = tt.shouldAdjustToUTC() ? "timestamptz" : "timestamp";
            } else if (type instanceof Types.TimestampNanoType ttNano) {
                logicalType = ttNano.shouldAdjustToUTC() ? "timestamp_ns_tz" : "timestamp_ns";
            }

            fields.add(new FieldInfo(
                    field.name(),
                    type.typeId().name(),
                    logicalType,
                    field.isOptional(),
                    false // Iceberg doesn't have defaults in the same way
            ));
        }

        return fields;
    }

    private static boolean isNullable(org.apache.avro.Schema schema) {
        if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .anyMatch(s -> s.getType() == org.apache.avro.Schema.Type.NULL);
        }
        return false;
    }

    private static org.apache.avro.Schema unwrapNullable(org.apache.avro.Schema schema) {
        if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .filter(s -> s.getType() != org.apache.avro.Schema.Type.NULL)
                    .findFirst()
                    .orElse(schema);
        }
        return schema;
    }

    /**
     * Supported field types for schema creation.
     */
    public enum FieldType {
        BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, STRING,
        DATE, TIME, TIMESTAMP, TIMESTAMPTZ,
        TIMESTAMP_NANO, TIMESTAMP_NANOTZ,
        UUID, BINARY, DECIMAL, FIXED,
        LIST, MAP, STRUCT
    }

    /**
     * Field definition for schema creation.
     */
    public record FieldDefinition(
            String name,
            FieldType type,
            boolean required,
            Object... typeParams
    ) {
        public FieldDefinition(String name, FieldType type, boolean required) {
            this(name, type, required, new Object[0]);
        }
    }

    /**
     * Information about a schema field.
     */
    public record FieldInfo(
            String name,
            String type,
            String logicalType,
            boolean nullable,
            boolean hasDefault
    ) {}
}