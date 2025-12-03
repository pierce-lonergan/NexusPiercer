package io.github.pierce.converter;




import java.util.*;

/**
 * Converter for struct/record values.
 *
 * <p>Converts nested record structures (Map or GenericRecord) to a Map
 * with fields converted according to the struct schema.</p>
 */
public class StructConverter extends AbstractTypeConverter<Map<String, Object>> {

    private final Map<String, FieldInfo> fields;
    private final Set<String> requiredFields;

    public StructConverter(ConversionConfig config, List<FieldInfo> fieldInfos) {
        super(config, buildTypeName(fieldInfos));

        this.fields = new LinkedHashMap<>();
        this.requiredFields = new HashSet<>();

        for (FieldInfo field : fieldInfos) {
            fields.put(field.name(), field);
            if (field.isRequired()) {
                requiredFields.add(field.name());
            }
        }
    }

    private static String buildTypeName(List<FieldInfo> fieldInfos) {
        StringBuilder sb = new StringBuilder("struct<");
        boolean first = true;
        for (FieldInfo field : fieldInfos) {
            if (!first) sb.append(", ");
            sb.append(field.name()).append(": ").append(field.converter().getTargetTypeName());
            first = false;
        }
        sb.append(">");
        return sb.toString();
    }

    public Map<String, FieldInfo> getFields() {
        return Collections.unmodifiableMap(fields);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Object> doConvert(Object value) throws TypeConversionException {
        Map<String, Object> sourceMap;

        // Handle Map input
        if (value instanceof Map<?, ?> map) {
            sourceMap = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                // Convert keys to String (handles Avro Utf8)
                String key = entry.getKey() != null ? entry.getKey().toString() : null;
                sourceMap.put(key, entry.getValue());
            }
        } else {
            throw unsupportedType(value);
        }

        // Build result map
        Map<String, Object> result = new LinkedHashMap<>();

        for (Map.Entry<String, FieldInfo> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            FieldInfo fieldInfo = entry.getValue();

            Object fieldValue = sourceMap.get(fieldName);

            // Check for missing required fields
            if (fieldValue == null && !sourceMap.containsKey(fieldName)) {
                if (fieldInfo.isRequired()) {
                    if (fieldInfo.hasDefault() && config.isUseSchemaDefaults()) {
                        fieldValue = fieldInfo.defaultValue();
                    } else {
                        throw new NullValueException(fieldName,
                                "Required field is missing from input");
                    }
                } else if (fieldInfo.hasDefault() && config.isUseSchemaDefaults()) {
                    fieldValue = fieldInfo.defaultValue();
                } else {
                    result.put(fieldName, null);
                    continue;
                }
            }

            // Handle null for required fields
            if (fieldValue == null) {
                if (fieldInfo.isRequired()) {
                    throw new NullValueException(fieldName);
                }
                result.put(fieldName, null);
                continue;
            }

            // Convert the field value
            try {
                Object converted = fieldInfo.converter().convert(fieldValue);
                result.put(fieldName, converted);
            } catch (TypeConversionException e) {
                throw new TypeConversionException(
                        fieldName, fieldValue, fieldInfo.converter().getTargetTypeName(),
                        e.getMessage(), e);
            }
        }

        // Handle extra fields
        if (!config.isAllowExtraFields()) {
            Set<String> extraFields = new HashSet<>(sourceMap.keySet());
            extraFields.removeAll(fields.keySet());
            if (!extraFields.isEmpty()) {
                throw new TypeConversionException(
                        "Extra fields not allowed: " + extraFields);
            }
        }

        return result;
    }

    @Override
    protected Map<String, Object> handleNull() {
        return null;
    }

    /**
     * Information about a struct field.
     */
    public record FieldInfo(
            String name,
            TypeConverter<Object, Object> converter,
            boolean isRequired,
            Object defaultValue
    ) {
        /**
         * Creates a FieldInfo without a default value.
         */
        @SuppressWarnings("unchecked")
        public static FieldInfo of(String name, TypeConverter<?, ?> converter, boolean isRequired) {
            return new FieldInfo(name, (TypeConverter<Object, Object>) converter, isRequired, null);
        }

        /**
         * Creates a FieldInfo with a default value.
         */
        @SuppressWarnings("unchecked")
        public static FieldInfo of(String name, TypeConverter<?, ?> converter, boolean isRequired, Object defaultValue) {
            return new FieldInfo(name, (TypeConverter<Object, Object>) converter, isRequired, defaultValue);
        }

        public boolean hasDefault() {
            return defaultValue != null;
        }
    }

    /**
     * Builder for StructConverter.
     */
    public static class Builder {
        private final ConversionConfig config;
        private final List<FieldInfo> fields = new ArrayList<>();

        public Builder(ConversionConfig config) {
            this.config = config;
        }

        public Builder addField(String name, TypeConverter<?, ?> converter, boolean required) {
            fields.add(FieldInfo.of(name, converter, required));
            return this;
        }

        public Builder addField(String name, TypeConverter<?, ?> converter,
                                boolean required, Object defaultValue) {
            fields.add(FieldInfo.of(name, converter, required, defaultValue));
            return this;
        }

        public Builder addField(FieldInfo fieldInfo) {
            fields.add(fieldInfo);
            return this;
        }

        public StructConverter build() {
            return new StructConverter(config, fields);
        }
    }
}