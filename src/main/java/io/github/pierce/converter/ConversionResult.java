package io.github.pierce.converter;

import java.util.List;

/**
 * Result of a conversion operation that may contain either a value or errors.
 *
 * @param <T> the result type
 */
public final class ConversionResult<T> {

    private final T value;
    private final List<ConversionError> errors;
    private final boolean success;

    private ConversionResult(T value, List<ConversionError> errors, boolean success) {
        this.value = value;
        this.errors = errors != null ? List.copyOf(errors) : List.of();
        this.success = success;
    }

    /**
     * Creates a successful result with a value.
     */
    public static <T> ConversionResult<T> success(T value) {
        return new ConversionResult<>(value, null, true);
    }

    /**
     * Creates a failed result with errors.
     */
    public static <T> ConversionResult<T> failure(List<ConversionError> errors) {
        return new ConversionResult<>(null, errors, false);
    }

    /**
     * Creates a failed result with a single error.
     */
    public static <T> ConversionResult<T> failure(ConversionError error) {
        return new ConversionResult<>(null, List.of(error), false);
    }

    /**
     * Creates a partial result (has value but also has warnings/errors).
     */
    public static <T> ConversionResult<T> partial(T value, List<ConversionError> errors) {
        return new ConversionResult<>(value, errors, errors == null || errors.isEmpty());
    }

    /**
     * Returns true if the conversion was successful with no errors.
     */
    public boolean isSuccess() {
        return success && errors.isEmpty();
    }

    /**
     * Returns true if there are any errors.
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    /**
     * Returns the converted value, or null if conversion failed.
     */
    public T getValue() {
        return value;
    }

    /**
     * Returns the converted value or throws if there are errors.
     */
    public T getValueOrThrow() {
        if (hasErrors()) {
            throw new SchemaConversionException(formatErrors());
        }
        return value;
    }

    /**
     * Returns the list of conversion errors.
     */
    public List<ConversionError> getErrors() {
        return errors;
    }

    /**
     * Formats all errors into a single string.
     */
    public String formatErrors() {
        if (errors.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < errors.size(); i++) {
            if (i > 0) sb.append("; ");
            ConversionError err = errors.get(i);
            if (err.fieldPath() != null) {
                sb.append(err.fieldPath()).append(": ");
            }
            sb.append(err.message());
        }
        return sb.toString();
    }

    /**
     * Represents a single conversion error.
     */
    public record ConversionError(String fieldPath, String message, Throwable cause) {
        public ConversionError(String fieldPath, String message) {
            this(fieldPath, message, null);
        }
    }
}
