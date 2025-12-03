package io.github.pierce.converter;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

/**
 * Converter for string values.
 *
 * <p>Handles conversion from various types to String representation.</p>
 */
public class StringConverter extends AbstractTypeConverter<String> {

    private final Integer maxLength;

    public StringConverter() {
        this(ConversionConfig.defaults());
    }

    public StringConverter(ConversionConfig config) {
        this(config, null);
    }

    /**
     * Creates a converter with a maximum string length.
     */
    public StringConverter(ConversionConfig config, Integer maxLength) {
        super(config, maxLength != null ? "string(" + maxLength + ")" : "string");
        this.maxLength = maxLength;
    }

    @Override
    protected String doConvert(Object value) throws TypeConversionException {
        String result;

        // Handle CharSequence (including String and Avro Utf8)
        if (value instanceof CharSequence cs) {
            result = cs.toString();
        }
        // Handle byte arrays - encode as Base64
        else if (value instanceof byte[] bytes) {
            result = Base64.getEncoder().encodeToString(bytes);
        }
        // Handle ByteBuffer - encode as Base64
        else if (value instanceof ByteBuffer bb) {
            ByteBuffer duplicate = bb.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            result = Base64.getEncoder().encodeToString(bytes);
        }
        // Handle UUID
        else if (value instanceof UUID uuid) {
            result = uuid.toString();
        }
        // Handle all other types via toString()
        else {
            result = value.toString();
        }

        // Apply trimming if configured
        if (config.isTrimStrings()) {
            result = result.trim();
        }

        // Handle empty strings
        if (result.isEmpty() && config.isCoerceEmptyStringsToNull()) {
            return null;
        }

        // Apply length constraints
        if (maxLength != null && result.length() > maxLength) {
            result = applyTruncation(result);
        }

        return result;
    }

    private String applyTruncation(String str) {
        switch (config.getStringTruncationMode()) {
            case ERROR -> throw conversionError(str,
                    String.format("String length %d exceeds maximum %d", str.length(), maxLength));
            case TRUNCATE -> {
                return str.substring(0, maxLength);
            }
            case TRUNCATE_WITH_WARNING -> {
                // In a real implementation, you might log a warning here
                return str.substring(0, maxLength);
            }
        }
        return str;
    }

    /**
     * Creates a converter with a maximum length.
     */
    public static StringConverter withMaxLength(ConversionConfig config, int maxLength) {
        if (maxLength <= 0) {
            throw new IllegalArgumentException("Max length must be positive: " + maxLength);
        }
        return new StringConverter(config, maxLength);
    }
}
