package io.github.pierce.converter;

/**
 * Exception thrown when type conversion fails.
 */
public class TypeConversionException extends SchemaConversionException {

    private final Object value;
    private final String targetType;

    public TypeConversionException(String message) {
        super(message);
        this.value = null;
        this.targetType = null;
    }

    public TypeConversionException(String message, Throwable cause) {
        super(message, cause);
        this.value = null;
        this.targetType = null;
    }

    public TypeConversionException(String fieldPath, Object value, String targetType, String message) {
        super(fieldPath, formatMessage(value, targetType, message));
        this.value = value;
        this.targetType = targetType;
    }

    public TypeConversionException(String fieldPath, Object value, String targetType, String message, Throwable cause) {
        super(fieldPath, formatMessage(value, targetType, message), cause);
        this.value = value;
        this.targetType = targetType;
    }

    public TypeConversionException(Object value, String targetType, String message) {
        super(formatMessage(value, targetType, message));
        this.value = value;
        this.targetType = targetType;
    }

    public TypeConversionException(Object value, String targetType, String message, Throwable cause) {
        super(formatMessage(value, targetType, message), cause);
        this.value = value;
        this.targetType = targetType;
    }

    private static String formatMessage(Object value, String targetType, String message) {
        String valueStr = value == null ? "null" : truncateValue(value);
        String sourceType = value == null ? "null" : value.getClass().getSimpleName();
        return String.format("Cannot convert %s (%s) to %s: %s",
                valueStr, sourceType, targetType, message);
    }

    private static String truncateValue(Object value) {
        String str = value.toString();
        if (str.length() > 50) {
            return str.substring(0, 47) + "...";
        }
        return str;
    }

    /**
     * Returns the value that failed conversion.
     */
    public Object getValue() {
        return value;
    }

    /**
     * Returns the target type for the conversion.
     */
    public String getTargetType() {
        return targetType;
    }
}
