package io.github.pierce.converter;

/**
 * Base exception for all schema conversion errors.
 */
public class SchemaConversionException extends RuntimeException {

    private final String fieldPath;

    public SchemaConversionException(String message) {
        super(message);
        this.fieldPath = null;
    }

    public SchemaConversionException(String message, Throwable cause) {
        super(message, cause);
        this.fieldPath = null;
    }

    public SchemaConversionException(String fieldPath, String message) {
        super(message);
        this.fieldPath = fieldPath;
    }

    public SchemaConversionException(String fieldPath, String message, Throwable cause) {
        super(message, cause);
        this.fieldPath = fieldPath;
    }

    /**
     * Returns the field path where the error occurred.
     */
    public String getFieldPath() {
        return fieldPath;
    }

    @Override
    public String getMessage() {
        if (fieldPath != null && !fieldPath.isEmpty()) {
            return "Field '" + fieldPath + "': " + super.getMessage();
        }
        return super.getMessage();
    }
}
