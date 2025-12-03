package io.github.pierce.converter;

/**
 * Abstract base class for type converters providing common functionality.
 *
 * @param <T> the target type
 */
public abstract class AbstractTypeConverter<T> implements TypeConverter<Object, T> {

    protected final ConversionConfig config;
    private final String targetTypeName;

    protected AbstractTypeConverter(ConversionConfig config, String targetTypeName) {
        this.config = config != null ? config : ConversionConfig.defaults();
        this.targetTypeName = targetTypeName;
    }

    @Override
    public T convert(Object value) {
        if (value == null) {
            return handleNull();
        }

        // Handle empty string as null if configured
        if (config.isCoerceEmptyStringsToNull() && value instanceof CharSequence cs) {
            if (cs.length() == 0) {
                return handleNull();
            }
        }

        try {
            return doConvert(value);
        } catch (TypeConversionException e) {
            throw e;
        } catch (Exception e) {
            throw conversionError(value, e.getMessage(), e);
        }
    }

    /**
     * Performs the actual conversion. Subclasses must implement this.
     */
    protected abstract T doConvert(Object value) throws TypeConversionException;

    /**
     * Handles null values. Default returns null.
     */
    protected T handleNull() {
        return null;
    }

    @Override
    public String getTargetTypeName() {
        return targetTypeName;
    }

    /**
     * Creates a TypeConversionException for an unsupported type.
     */
    protected TypeConversionException unsupportedType(Object value) {
        return new TypeConversionException(value, targetTypeName,
                "Unsupported input type: " + value.getClass().getName());
    }

    /**
     * Creates a TypeConversionException with a custom message.
     */
    protected TypeConversionException conversionError(Object value, String message) {
        return new TypeConversionException(value, targetTypeName, message);
    }

    /**
     * Creates a TypeConversionException with a custom message and cause.
     */
    protected TypeConversionException conversionError(Object value, String message, Throwable cause) {
        return new TypeConversionException(value, targetTypeName, message, cause);
    }

    /**
     * Converts a CharSequence to String, handling Avro Utf8.
     */
    protected String charSequenceToString(Object value) {
        if (value instanceof String s) {
            return config.isTrimStrings() ? s.trim() : s;
        }
        if (value instanceof CharSequence cs) {
            String s = cs.toString();
            return config.isTrimStrings() ? s.trim() : s;
        }
        return null;
    }

    /**
     * Parses a number from various input types.
     */
    protected Number parseNumber(Object value) {
        if (value instanceof Number n) {
            return n;
        }
        if (value instanceof CharSequence) {
            String str = charSequenceToString(value);
            if (str != null && !str.isEmpty()) {
                try {
                    // Try to parse as the most specific type possible
                    if (str.contains(".") || str.toLowerCase().contains("e")) {
                        return Double.parseDouble(str);
                    }
                    long l = Long.parseLong(str);
                    if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                        return (int) l;
                    }
                    return l;
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }
        return null;
    }
}
