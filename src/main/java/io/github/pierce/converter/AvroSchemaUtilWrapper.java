package io.github.pierce.converter;

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
 * <p>This is a facade around Iceberg's built-in AvroSchemaUtil with some
 * additional convenience methods.</p>
 */
public final class AvroSchemaUtilWrapper {

    private AvroSchemaUtilWrapper() {
        // Utility class
    }

    /**
     * Converts an Avro schema to an Iceberg schema.
     *
     * @param avroSchema the Avro schema
     * @return the equivalent Iceberg schema
     */
    public static Schema toIceberg(org.apache.avro.Schema avroSchema) {
        // Use Iceberg's built-in conversion
        return AvroSchemaUtil.toIceberg(avroSchema);
    }

    /**
     * Converts an Iceberg schema to an Avro schema.
     *
     * @param icebergSchema the Iceberg schema
     * @param recordName the name for the Avro record
     * @return the equivalent Avro schema
     */
    public static org.apache.avro.Schema convert(Schema icebergSchema, String recordName) {
        // Use Iceberg's built-in conversion
        return AvroSchemaUtil.convert(icebergSchema, recordName);
    }

    /**
     * Converts an Iceberg type to an Avro schema.
     *
     * @param type the Iceberg type
     * @param name the field name (for record types)
     * @return the equivalent Avro schema
     */
    public static org.apache.avro.Schema typeToAvro(Type type, String name) {
        return switch (type.typeId()) {
            case BOOLEAN -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN);
            case INTEGER -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT);
            case LONG -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);
            case FLOAT -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT);
            case DOUBLE -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE);
            case STRING -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
            case BINARY -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES);
            case UUID -> {
                org.apache.avro.Schema uuidSchema = org.apache.avro.Schema.create(
                        org.apache.avro.Schema.Type.STRING);
                LogicalTypes.uuid().addToSchema(uuidSchema);
                yield uuidSchema;
            }
            case DATE -> {
                org.apache.avro.Schema dateSchema = org.apache.avro.Schema.create(
                        org.apache.avro.Schema.Type.INT);
                LogicalTypes.date().addToSchema(dateSchema);
                yield dateSchema;
            }
            case TIME -> {
                org.apache.avro.Schema timeSchema = org.apache.avro.Schema.create(
                        org.apache.avro.Schema.Type.LONG);
                LogicalTypes.timeMicros().addToSchema(timeSchema);
                yield timeSchema;
            }
            case TIMESTAMP -> {
                Types.TimestampType ts = (Types.TimestampType) type;
                org.apache.avro.Schema tsSchema = org.apache.avro.Schema.create(
                        org.apache.avro.Schema.Type.LONG);
                if (ts.shouldAdjustToUTC()) {
                    LogicalTypes.timestampMicros().addToSchema(tsSchema);
                } else {
                    LogicalTypes.localTimestampMicros().addToSchema(tsSchema);
                }
                yield tsSchema;
            }
            case TIMESTAMP_NANO -> {
                Types.TimestampNanoType tsNano = (Types.TimestampNanoType) type;
                org.apache.avro.Schema tsNanoSchema = org.apache.avro.Schema.create(
                        org.apache.avro.Schema.Type.LONG);
                if (tsNano.shouldAdjustToUTC()) {
                    LogicalTypes.timestampNanos().addToSchema(tsNanoSchema);
                } else {
                    LogicalTypes.localTimestampNanos().addToSchema(tsNanoSchema);
                }
                yield tsNanoSchema;
            }
            case DECIMAL -> {
                Types.DecimalType decimal = (Types.DecimalType) type;
                org.apache.avro.Schema decSchema = org.apache.avro.Schema.create(
                        org.apache.avro.Schema.Type.BYTES);
                LogicalTypes.decimal(decimal.precision(), decimal.scale()).addToSchema(decSchema);
                yield decSchema;
            }
            case FIXED -> {
                Types.FixedType fixed = (Types.FixedType) type;
                yield org.apache.avro.Schema.createFixed(name + "_fixed",
                        null, null, fixed.length());
            }
            case LIST -> {
                Types.ListType listType = (Types.ListType) type;
                org.apache.avro.Schema elementSchema = typeToAvro(listType.elementType(), name + "_element");
                if (listType.isElementOptional()) {
                    elementSchema = nullable(elementSchema);
                }
                yield org.apache.avro.Schema.createArray(elementSchema);
            }
            case MAP -> {
                Types.MapType mapType = (Types.MapType) type;
                org.apache.avro.Schema valueSchema = typeToAvro(mapType.valueType(), name + "_value");
                if (mapType.isValueOptional()) {
                    valueSchema = nullable(valueSchema);
                }
                yield org.apache.avro.Schema.createMap(valueSchema);
            }
            case STRUCT -> {
                Types.StructType structType = (Types.StructType) type;
                List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
                for (Types.NestedField field : structType.fields()) {
                    org.apache.avro.Schema fieldSchema = typeToAvro(field.type(), field.name());
                    if (field.isOptional()) {
                        fieldSchema = nullable(fieldSchema);
                    }
                    fields.add(new org.apache.avro.Schema.Field(field.name(), fieldSchema));
                }
                yield org.apache.avro.Schema.createRecord(name, null, null, false, fields);
            }
        };
    }

    /**
     * Makes an Avro schema nullable by wrapping it in a union with null.
     */
    public static org.apache.avro.Schema nullable(org.apache.avro.Schema schema) {
        if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
            return schema; // Already a union
        }
        return org.apache.avro.Schema.createUnion(
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                schema
        );
    }

    /**
     * Checks if an Avro schema is nullable (a union containing null).
     */
    public static boolean isNullable(org.apache.avro.Schema schema) {
        if (schema.getType() != org.apache.avro.Schema.Type.UNION) {
            return false;
        }
        return schema.getTypes().stream()
                .anyMatch(s -> s.getType() == org.apache.avro.Schema.Type.NULL);
    }

    /**
     * Unwraps a nullable union to get the non-null type.
     */
    public static org.apache.avro.Schema unwrapNullable(org.apache.avro.Schema schema) {
        if (schema.getType() != org.apache.avro.Schema.Type.UNION) {
            return schema;
        }
        return schema.getTypes().stream()
                .filter(s -> s.getType() != org.apache.avro.Schema.Type.NULL)
                .findFirst()
                .orElse(schema);
    }
}