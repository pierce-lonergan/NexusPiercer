package io.github.pierce.converter;

/**
 * Converter for integer (32-bit signed) values.
 *
 * <p>Handles type promotion from smaller types and validates
 * range for larger types.</p>
 */
public class IntegerConverter extends AbstractTypeConverter<Integer> {

    public IntegerConverter() {
        this(ConversionConfig.defaults());
    }

    public IntegerConverter(ConversionConfig config) {
        super(config, "int");
    }

    @Override
    protected Integer doConvert(Object value) throws TypeConversionException {
        // Handle Integer
        if (value instanceof Integer i) {
            return i;
        }

        // Handle other Number types
        if (value instanceof Number n) {
            return convertNumber(n);
        }

        // Handle Boolean
        if (value instanceof Boolean b) {
            return b ? 1 : 0;
        }

        // Handle String
        String str = charSequenceToString(value);
        if (str != null) {
            if (str.isEmpty()) {
                if (config.isCoerceEmptyStringsToNull()) {
                    return null;
                }
                throw conversionError(value, "Empty string cannot be converted to integer");
            }
            try {
                // Handle decimal strings
                if (str.contains(".")) {
                    double d = Double.parseDouble(str);
                    return convertDouble(d, value);
                }
                // Parse as long first to check for overflow
                long l = Long.parseLong(str);
                return convertLong(l, value);
            } catch (NumberFormatException e) {
                throw conversionError(value, "Invalid integer string: '" + str + "'", e);
            }
        }

        throw unsupportedType(value);
    }

    private Integer convertNumber(Number n) {
        if (n instanceof Long l) {
            return convertLong(l, n);
        }
        if (n instanceof Double d) {
            return convertDouble(d, n);
        }
        if (n instanceof Float f) {
            return convertDouble(f.doubleValue(), n);
        }
        // Short, Byte, etc. - safe to convert
        return n.intValue();
    }

    private Integer convertLong(long l, Object original) {
        if (l > Integer.MAX_VALUE || l < Integer.MIN_VALUE) {
            if (config.isAllowNumericOverflow()) {
                return (int) l;
            }
            throw conversionError(original,
                    String.format("Value %d is outside integer range [%d, %d]",
                            l, Integer.MIN_VALUE, Integer.MAX_VALUE));
        }
        return (int) l;
    }

    private Integer convertDouble(double d, Object original) {
        if (!Double.isFinite(d)) {
            throw conversionError(original, "Cannot convert non-finite value to integer");
        }
        if (d > Integer.MAX_VALUE || d < Integer.MIN_VALUE) {
            if (config.isAllowNumericOverflow()) {
                return (int) d;
            }
            throw conversionError(original,
                    String.format("Value %f is outside integer range [%d, %d]",
                            d, Integer.MIN_VALUE, Integer.MAX_VALUE));
        }
        if (!config.isAllowPrecisionLoss() && d != Math.floor(d)) {
            throw conversionError(original,
                    "Value " + d + " has fractional part that would be lost");
        }
        return (int) d;
    }
}
