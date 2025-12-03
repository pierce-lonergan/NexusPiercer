package io.github.pierce.converter;

import java.math.BigDecimal;

/**
 * Converter for float (32-bit floating point) values.
 */
public class FloatConverter extends AbstractTypeConverter<Float> {

    public FloatConverter() {
        this(ConversionConfig.defaults());
    }

    public FloatConverter(ConversionConfig config) {
        super(config, "float");
    }

    @Override
    protected Float doConvert(Object value) throws TypeConversionException {
        // Handle Float
        if (value instanceof Float f) {
            validateFinite(f, value);
            return f;
        }

        // Handle other Number types
        if (value instanceof Number n) {
            return convertNumber(n);
        }

        // Handle Boolean
        if (value instanceof Boolean b) {
            return b ? 1.0f : 0.0f;
        }

        // Handle String
        String str = charSequenceToString(value);
        if (str != null) {
            if (str.isEmpty()) {
                if (config.isCoerceEmptyStringsToNull()) {
                    return null;
                }
                throw conversionError(value, "Empty string cannot be converted to float");
            }
            try {
                float f = Float.parseFloat(str);
                validateFinite(f, value);
                return f;
            } catch (NumberFormatException e) {
                throw conversionError(value, "Invalid float string: '" + str + "'", e);
            }
        }

        throw unsupportedType(value);
    }

    private Float convertNumber(Number n) {
        if (n instanceof Double d) {
            validateFinite(d.floatValue(), n);
            if (!config.isAllowPrecisionLoss()) {
                // Check if precision loss occurs
                float f = d.floatValue();
                if (Double.isFinite(d) && d != (double) f) {
                    throw conversionError(n,
                            "Precision loss converting double to float: " + d + " -> " + f);
                }
            }
            return d.floatValue();
        }
        if (n instanceof BigDecimal bd) {
            float f = bd.floatValue();
            validateFinite(f, n);
            return f;
        }
        // Integer, Long, etc.
        return n.floatValue();
    }

    private void validateFinite(float f, Object original) {
        if (Float.isNaN(f)) {
            throw conversionError(original, "NaN values are not supported");
        }
        if (Float.isInfinite(f)) {
            throw conversionError(original, "Infinite values are not supported");
        }
    }
}
