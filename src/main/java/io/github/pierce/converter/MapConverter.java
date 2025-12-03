package io.github.pierce.converter;

import java.util.*;

/**
 * Converter for map values.
 *
 * <p>Converts maps with keys and values converted according to their schemas.
 * Handles Avro Utf8 map keys.</p>
 */
public class MapConverter extends AbstractTypeConverter<Map<Object, Object>> {

    private final TypeConverter<Object, ?> keyConverter;
    private final TypeConverter<Object, ?> valueConverter;

    @SuppressWarnings("unchecked")
    public MapConverter(ConversionConfig config,
                        TypeConverter<?, ?> keyConverter,
                        TypeConverter<?, ?> valueConverter) {
        super(config, "map<" + keyConverter.getTargetTypeName() + ", " +
                valueConverter.getTargetTypeName() + ">");
        this.keyConverter = (TypeConverter<Object, ?>) Objects.requireNonNull(keyConverter, "Key converter cannot be null");
        this.valueConverter = (TypeConverter<Object, ?>) Objects.requireNonNull(valueConverter, "Value converter cannot be null");
    }

    public TypeConverter<Object, ?> getKeyConverter() {
        return keyConverter;
    }

    public TypeConverter<Object, ?> getValueConverter() {
        return valueConverter;
    }

    @Override
    protected Map<Object, Object> doConvert(Object value) throws TypeConversionException {
        if (!(value instanceof Map<?, ?> source)) {
            throw unsupportedType(value);
        }

        // Use LinkedHashMap to preserve order
        Map<Object, Object> result = new LinkedHashMap<>(
                (int) (source.size() / 0.75f) + 1);

        for (Map.Entry<?, ?> entry : source.entrySet()) {
            Object rawKey = entry.getKey();
            Object rawValue = entry.getValue();

            try {
                Object convertedKey = keyConverter.convert(rawKey);
                Object convertedValue = valueConverter.convert(rawValue);
                result.put(convertedKey, convertedValue);
            } catch (TypeConversionException e) {
                String keyStr = rawKey != null ? rawKey.toString() : "null";
                String path = "[" + keyStr + "]";
                if (e.getFieldPath() != null && !e.getFieldPath().isEmpty()) {
                    path = path + "." + e.getFieldPath();
                }
                throw new TypeConversionException(path, rawValue,
                        valueConverter.getTargetTypeName(), e.getMessage(), e);
            }
        }

        return result;
    }

    @Override
    protected Map<Object, Object> handleNull() {
        return null;
    }
}