package io.github.pierce.converter;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Converter for long (64-bit signed) values.
 */
public class LongConverter extends AbstractTypeConverter<Long> {

    public LongConverter() {
        this(ConversionConfig.defaults());
    }

    public LongConverter(ConversionConfig config) {
        super(config, "long");
    }

    @Override
    protected Long doConvert(Object value) throws TypeConversionException {
        // Handle Long
        if (value instanceof Long l) {
            return l;
        }

        // Handle Integer (promotion)
        if (value instanceof Integer i) {
            return i.longValue();
        }

        // Handle other Number types
        if (value instanceof Number n) {
            return convertNumber(n);
        }

        // Handle Boolean
        if (value instanceof Boolean b) {
            return b ? 1L : 0L;
        }

        // Handle String
        String str = charSequenceToString(value);
        if (str != null) {
            if (str.isEmpty()) {
                if (config.isCoerceEmptyStringsToNull()) {
                    return null;
                }
                throw conversionError(value, "Empty string cannot be converted to long");
            }
            try {
                // Handle decimal strings
                if (str.contains(".") || str.toLowerCase().contains("e")) {
                    double d = Double.parseDouble(str);
                    return convertDouble(d, value);
                }
                return Long.parseLong(str);
            } catch (NumberFormatException e) {
                // Try BigInteger for very large numbers
                try {
                    BigInteger bi = new BigInteger(str);
                    return convertBigInteger(bi, value);
                } catch (NumberFormatException e2) {
                    throw conversionError(value, "Invalid long string: '" + str + "'", e);
                }
            }
        }

        throw unsupportedType(value);
    }

    private Long convertNumber(Number n) {
        if (n instanceof Double d) {
            return convertDouble(d, n);
        }
        if (n instanceof Float f) {
            return convertDouble(f.doubleValue(), n);
        }
        if (n instanceof BigDecimal bd) {
            return convertBigDecimal(bd, n);
        }
        if (n instanceof BigInteger bi) {
            return convertBigInteger(bi, n);
        }
        // Smaller types - safe to convert
        return n.longValue();
    }

    private Long convertDouble(double d, Object original) {
        if (!Double.isFinite(d)) {
            throw conversionError(original, "Cannot convert non-finite value to long");
        }
        if (d > Long.MAX_VALUE || d < Long.MIN_VALUE) {
            if (config.isAllowNumericOverflow()) {
                return (long) d;
            }
            throw conversionError(original,
                    String.format("Value %f is outside long range", d));
        }
        if (!config.isAllowPrecisionLoss() && d != Math.floor(d)) {
            throw conversionError(original,
                    "Value " + d + " has fractional part that would be lost");
        }
        return (long) d;
    }

    private Long convertBigDecimal(BigDecimal bd, Object original) {
        try {
            if (!config.isAllowPrecisionLoss() && bd.scale() > 0) {
                BigDecimal stripped = bd.stripTrailingZeros();
                if (stripped.scale() > 0) {
                    throw conversionError(original,
                            "Value " + bd + " has fractional part that would be lost");
                }
            }
            return bd.longValueExact();
        } catch (ArithmeticException e) {
            if (config.isAllowNumericOverflow()) {
                return bd.longValue();
            }
            throw conversionError(original, "Value " + bd + " is outside long range", e);
        }
    }

    private Long convertBigInteger(BigInteger bi, Object original) {
        if (bi.bitLength() > 63) {
            if (config.isAllowNumericOverflow()) {
                return bi.longValue();
            }
            throw conversionError(original, "Value " + bi + " is outside long range");
        }
        return bi.longValue();
    }
}
