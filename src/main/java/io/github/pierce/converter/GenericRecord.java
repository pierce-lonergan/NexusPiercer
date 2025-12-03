package io.github.pierce.converter;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A mutable implementation of Iceberg's record structure.
 *
 * <p>This is a simple implementation for use with the converter.
 * For production use with Iceberg writers, you may want to use
 * Iceberg's built-in GenericRecord from iceberg-data module.</p>
 */
public class GenericRecord {

    private final Schema schema;
    private final Map<String, Object> values;

    private GenericRecord(Schema schema) {
        this.schema = Objects.requireNonNull(schema, "Schema cannot be null");
        this.values = new LinkedHashMap<>();
    }

    /**
     * Creates a new GenericRecord for the given schema.
     */
    public static GenericRecord create(Schema schema) {
        return new GenericRecord(schema);
    }

    /**
     * Sets a field value by name.
     */
    public GenericRecord setField(String name, Object value) {
        Types.NestedField field = schema.findField(name);
        if (field == null) {
            throw new IllegalArgumentException("Unknown field: " + name);
        }
        values.put(name, value);
        return this;
    }

    /**
     * Gets a field value by name.
     */
    public Object getField(String name) {
        return values.get(name);
    }

    /**
     * Gets a field value by position.
     */
    public Object get(int pos) {
        Types.NestedField field = schema.columns().get(pos);
        return values.get(field.name());
    }

    /**
     * Sets a field value by position.
     */
    public void set(int pos, Object value) {
        Types.NestedField field = schema.columns().get(pos);
        values.put(field.name(), value);
    }

    /**
     * Returns the schema.
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Returns a copy of the values map.
     */
    public Map<String, Object> toMap() {
        return new LinkedHashMap<>(values);
    }

    /**
     * Returns the number of fields.
     */
    public int size() {
        return schema.columns().size();
    }

    /**
     * Copies values from a map.
     */
    public GenericRecord copy(Map<String, Object> data) {
        for (Types.NestedField field : schema.columns()) {
            String name = field.name();
            if (data.containsKey(name)) {
                values.put(name, data.get(name));
            }
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GenericRecord{");
        boolean first = true;
        for (Types.NestedField field : schema.columns()) {
            if (!first) sb.append(", ");
            sb.append(field.name()).append("=").append(values.get(field.name()));
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GenericRecord that = (GenericRecord) o;
        return Objects.equals(schema, that.schema) && Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, values);
    }
}
