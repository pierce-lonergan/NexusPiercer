package io.github.pierce.converter;

/**
 * Exception thrown when a null value is encountered for a required field.
 */
public class NullValueException extends SchemaConversionException {

    public NullValueException(String fieldPath) {
        super(fieldPath, "Required field cannot be null");
    }

    public NullValueException(String fieldPath, String message) {
        super(fieldPath, message);
    }
}
