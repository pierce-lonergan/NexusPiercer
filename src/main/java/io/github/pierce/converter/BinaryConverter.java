package io.github.pierce.converter;


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Converter for binary values to ByteBuffer.
 *
 * <p>Iceberg represents binary data as ByteBuffer. This converter handles:
 * <ul>
 *   <li>ByteBuffer - duplicated to prevent mutation</li>
 *   <li>byte[] - wrapped in ByteBuffer</li>
 *   <li>String - decoded from Base64 or converted from UTF-8</li>
 * </ul>
 */
public class BinaryConverter extends AbstractTypeConverter<ByteBuffer> {

    private final Integer fixedLength;
    private final boolean isFixed;

    public BinaryConverter() {
        this(ConversionConfig.defaults(), null);
    }

    public BinaryConverter(ConversionConfig config) {
        this(config, null);
    }

    /**
     * Creates a converter for fixed-length binary (Iceberg FIXED type).
     */
    public BinaryConverter(ConversionConfig config, Integer fixedLength) {
        super(config, fixedLength != null ? "fixed[" + fixedLength + "]" : "binary");
        this.fixedLength = fixedLength;
        this.isFixed = fixedLength != null;
    }

    public Integer getFixedLength() {
        return fixedLength;
    }

    public boolean isFixed() {
        return isFixed;
    }

    @Override
    protected ByteBuffer doConvert(Object value) throws TypeConversionException {
        ByteBuffer result;

        // Handle ByteBuffer
        if (value instanceof ByteBuffer bb) {
            // Duplicate to prevent mutation of original
            result = bb.duplicate();
        }
        // Handle byte array
        else if (value instanceof byte[] bytes) {
            result = ByteBuffer.wrap(bytes.clone()); // Clone to prevent mutation
        }
        // Handle String (Base64 or UTF-8)
        else {
            String str = charSequenceToString(value);
            if (str != null) {
                result = parseString(str);
            } else {
                throw unsupportedType(value);
            }
        }

        // Validate fixed length if applicable
        if (isFixed) {
            validateFixedLength(result);
        }

        return result;
    }

    private ByteBuffer parseString(String str) {
        if (str.isEmpty()) {
            if (config.isCoerceEmptyStringsToNull()) {
                return null;
            }
            return ByteBuffer.allocate(0);
        }

        // Try Base64 decoding first
        try {
            byte[] decoded = Base64.getDecoder().decode(str);
            return ByteBuffer.wrap(decoded);
        } catch (IllegalArgumentException e) {
            // Not valid Base64, try URL-safe Base64
            try {
                byte[] decoded = Base64.getUrlDecoder().decode(str);
                return ByteBuffer.wrap(decoded);
            } catch (IllegalArgumentException e2) {
                // Not Base64, treat as UTF-8 string
                if (!config.isStrictTypeChecking()) {
                    return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
                }
                throw conversionError(str,
                        "String is not valid Base64. Use Base64-encoded data for binary fields.");
            }
        }
    }

    private void validateFixedLength(ByteBuffer buffer) {
        int length = buffer.remaining();
        if (length != fixedLength) {
            if (length < fixedLength) {
                // Pad with zeros
                byte[] padded = new byte[fixedLength];
                buffer.duplicate().get(padded, 0, length);
                buffer = ByteBuffer.wrap(padded);
            } else {
                // Truncate or error
                switch (config.getStringTruncationMode()) {
                    case ERROR -> throw conversionError(buffer,
                            String.format("Binary length %d exceeds fixed length %d",
                                    length, fixedLength));
                    case TRUNCATE, TRUNCATE_WITH_WARNING -> {
                        byte[] truncated = new byte[fixedLength];
                        buffer.duplicate().get(truncated);
                    }
                }
            }
        }
    }

    /**
     * Creates a converter for the Iceberg FIXED type.
     */
    public static BinaryConverter forFixed(ConversionConfig config, int length) {
        if (length <= 0) {
            throw new IllegalArgumentException("Fixed length must be positive: " + length);
        }
        return new BinaryConverter(config, length);
    }

    /**
     * Converts ByteBuffer to Base64 string.
     */
    public static String toBase64(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    /**
     * Converts ByteBuffer to byte array.
     */
    public static byte[] toByteArray(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}