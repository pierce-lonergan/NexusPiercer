package io.github.pierce.converter;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Converter for UUID values.
 *
 * <p>Handles conversion from:
 * <ul>
 *   <li>UUID - returned as-is</li>
 *   <li>String - parsed as UUID format</li>
 *   <li>byte[] - 16 bytes representing MSB/LSB</li>
 *   <li>ByteBuffer - 16 bytes representing MSB/LSB</li>
 * </ul>
 */
public class UUIDConverter extends AbstractTypeConverter<UUID> {

    private static final Pattern UUID_PATTERN = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    );

    // Pattern for UUID without hyphens
    private static final Pattern UUID_NO_HYPHENS_PATTERN = Pattern.compile(
            "^[0-9a-fA-F]{32}$"
    );

    public UUIDConverter() {
        this(ConversionConfig.defaults());
    }

    public UUIDConverter(ConversionConfig config) {
        super(config, "uuid");
    }

    @Override
    protected UUID doConvert(Object value) throws TypeConversionException {
        // Handle UUID
        if (value instanceof UUID uuid) {
            return uuid;
        }

        // Handle String
        if (value instanceof CharSequence) {
            String str = charSequenceToString(value);
            if (str == null || str.isEmpty()) {
                if (config.isCoerceEmptyStringsToNull()) {
                    return null;
                }
                throw conversionError(value, "Empty string cannot be converted to UUID");
            }
            return parseUuidString(str);
        }

        // Handle byte[]
        if (value instanceof byte[] bytes) {
            return bytesToUuid(bytes);
        }

        // Handle ByteBuffer
        if (value instanceof ByteBuffer bb) {
            ByteBuffer duplicate = bb.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return bytesToUuid(bytes);
        }

        throw unsupportedType(value);
    }

    private UUID parseUuidString(String str) {
        // Standard format with hyphens
        if (UUID_PATTERN.matcher(str).matches()) {
            try {
                return UUID.fromString(str);
            } catch (IllegalArgumentException e) {
                throw conversionError(str, "Invalid UUID format", e);
            }
        }

        // Format without hyphens
        if (UUID_NO_HYPHENS_PATTERN.matcher(str).matches()) {
            String formatted = str.substring(0, 8) + "-" +
                    str.substring(8, 12) + "-" +
                    str.substring(12, 16) + "-" +
                    str.substring(16, 20) + "-" +
                    str.substring(20);
            try {
                return UUID.fromString(formatted);
            } catch (IllegalArgumentException e) {
                throw conversionError(str, "Invalid UUID format", e);
            }
        }

        throw conversionError(str, "Invalid UUID format: '" + str + "'");
    }

    private UUID bytesToUuid(byte[] bytes) {
        if (bytes.length != 16) {
            throw conversionError(bytes,
                    String.format("UUID requires exactly 16 bytes, got %d", bytes.length));
        }

        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long msb = bb.getLong();
        long lsb = bb.getLong();
        return new UUID(msb, lsb);
    }

    /**
     * Converts a UUID to its 16-byte representation.
     */
    public static byte[] toBytes(UUID uuid) {
        if (uuid == null) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

    /**
     * Converts 16 bytes to UUID.
     */
    public static UUID fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length != 16) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return new UUID(bb.getLong(), bb.getLong());
    }
}
