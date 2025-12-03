package io.github.pierce.converter;

/**
 * Interface for type conversion.
 *
 * @param <I> the input type
 * @param <O> the output type
 */
public interface TypeConverter<I, O> {

    /**
     * Converts a value from input type to output type.
     *
     * @param value the value to convert
     * @return the converted value
     * @throws TypeConversionException if conversion fails
     */
    O convert(I value);

    /**
     * Converts a value with field context for error reporting.
     *
     * @param value the value to convert
     * @param fieldName the name of the field being converted
     * @return the converted value
     * @throws TypeConversionException if conversion fails
     */
    default O convert(I value, String fieldName) {
        try {
            return convert(value);
        } catch (TypeConversionException e) {
            if (e.getFieldPath() == null || e.getFieldPath().isEmpty()) {
                throw new TypeConversionException(fieldName, value, getTargetTypeName(), e.getMessage(), e);
            }
            throw e;
        }
    }

    /**
     * Returns the target type name for error messages.
     */
    String getTargetTypeName();
}
