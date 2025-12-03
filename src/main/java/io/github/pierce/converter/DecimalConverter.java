package io.github.pierce.converter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

/**
 * Converter for decimal values with specific precision and scale.
 *
 * <p>Handles conversion to BigDecimal with precision/scale validation.</p>
 */
public class DecimalConverter extends AbstractTypeConverter<BigDecimal> {

    private final int precision;
    private final int scale;
    private final BigDecimal maxValue;
    private final BigDecimal minValue;

    public DecimalConverter(ConversionConfig config, int precision, int scale) {
        super(config, String.format("decimal(%d,%d)", precision, scale));
        
        if (precision <= 0) {
            throw new IllegalArgumentException("Precision must be positive: " + precision);
        }
        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException(
                    String.format("Scale must be between 0 and precision (%d): %d", precision, scale));
        }
        
        this.precision = precision;
        this.scale = scale;
        
        // Calculate max/min based on precision
        // For decimal(10,2), max is 99999999.99 (8 digits before decimal, 2 after)
        int integerDigits = precision - scale;
        BigInteger maxUnscaled = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
        this.maxValue = new BigDecimal(maxUnscaled, scale);
        this.minValue = maxValue.negate();
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    @Override
    protected BigDecimal doConvert(Object value) throws TypeConversionException {
        BigDecimal result;

        // Handle BigDecimal
        if (value instanceof BigDecimal bd) {
            result = bd;
        }
        // Handle BigInteger
        else if (value instanceof BigInteger bi) {
            result = new BigDecimal(bi);
        }
        // Handle Long/Integer
        else if (value instanceof Long l) {
            result = BigDecimal.valueOf(l);
        }
        else if (value instanceof Integer i) {
            result = BigDecimal.valueOf(i);
        }
        // Handle Double/Float
        else if (value instanceof Double d) {
            if (!Double.isFinite(d)) {
                throw conversionError(value, "Cannot convert non-finite value to decimal");
            }
            result = BigDecimal.valueOf(d);
        }
        else if (value instanceof Float f) {
            if (!Float.isFinite(f)) {
                throw conversionError(value, "Cannot convert non-finite value to decimal");
            }
            result = BigDecimal.valueOf(f);
        }
        // Handle String
        else if (value instanceof CharSequence) {
            String str = charSequenceToString(value);
            if (str == null || str.isEmpty()) {
                if (config.isCoerceEmptyStringsToNull()) {
                    return null;
                }
                throw conversionError(value, "Empty string cannot be converted to decimal");
            }
            try {
                result = new BigDecimal(str);
            } catch (NumberFormatException e) {
                throw conversionError(value, "Invalid decimal string: '" + str + "'", e);
            }
        }
        // Handle byte[] (Avro decimal representation)
        else if (value instanceof byte[] bytes) {
            result = new BigDecimal(new BigInteger(bytes), scale);
        }
        // Handle ByteBuffer (Avro decimal representation)
        else if (value instanceof ByteBuffer bb) {
            ByteBuffer duplicate = bb.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            result = new BigDecimal(new BigInteger(bytes), scale);
        }
        else {
            throw unsupportedType(value);
        }

        // Apply scale
        result = adjustScale(result);

        // Validate precision
        validatePrecision(result, value);

        return result;
    }

    private BigDecimal adjustScale(BigDecimal value) {
        if (value.scale() != scale) {
            RoundingMode mode = config.getDecimalRoundingMode();
            if (!config.isAllowPrecisionLoss() && value.scale() > scale) {
                // Check if we would lose precision
                BigDecimal scaled = value.setScale(scale, mode);
                if (scaled.compareTo(value) != 0) {
                    throw conversionError(value,
                            String.format("Value requires scale %d but target scale is %d",
                                    value.scale(), scale));
                }
            }
            return value.setScale(scale, mode);
        }
        return value;
    }

    private void validatePrecision(BigDecimal value, Object original) {
        // Check if value fits within precision
        if (value.compareTo(maxValue) > 0 || value.compareTo(minValue) < 0) {
            if (config.isAllowNumericOverflow()) {
                return; // Allow it
            }
            throw conversionError(original,
                    String.format("Value %s exceeds precision(%d,%d) bounds [%s, %s]",
                            value, precision, scale, minValue, maxValue));
        }

        // Also check actual precision (number of significant digits)
        int actualPrecision = value.precision();
        if (actualPrecision > precision) {
            throw conversionError(original,
                    String.format("Value %s has precision %d but maximum is %d",
                            value, actualPrecision, precision));
        }
    }

    /**
     * Converts a BigDecimal to Avro byte representation.
     */
    public static byte[] toBytes(BigDecimal value) {
        if (value == null) {
            return null;
        }
        return value.unscaledValue().toByteArray();
    }

    /**
     * Converts bytes to BigDecimal with the given scale.
     */
    public static BigDecimal fromBytes(byte[] bytes, int scale) {
        if (bytes == null) {
            return null;
        }
        return new BigDecimal(new BigInteger(bytes), scale);
    }
}
