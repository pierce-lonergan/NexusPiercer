package io.github.pierce.converter;

import java.math.BigDecimal;

/**
 * Converter for double (64-bit floating point) values.
 */
public class DoubleConverter extends AbstractTypeConverter<Double> {

    public DoubleConverter() {
        this(ConversionConfig.defaults());
    }

    public DoubleConverter(ConversionConfig config) {
        super(config, "double");
    }

    @Override
    protected Double doConvert(Object value) throws TypeConversionException {
        // Handle Double
        if (value instanceof Double d) {
            validateFinite(d, value);
            return d;
        }

        // Handle Float (promotion)
        if (value instanceof Float f) {
            validateFinite(f.doubleValue(), value);
            return f.doubleValue();
        }

        // Handle other Number types
        if (value instanceof Number n) {
            return convertNumber(n);
        }

        // Handle Boolean
        if (value instanceof Boolean b) {
            return b ? 1.0 : 0.0;
        }

        // Handle String
        String str = charSequenceToString(value);
        if (str != null) {
            if (str.isEmpty()) {
                if (config.isCoerceEmptyStringsToNull()) {
                    return null;
                }
                throw conversionError(value, "Empty string cannot be converted to double");
            }
            try {
                double d = Double.parseDouble(str);
                validateFinite(d, value);
                return d;
            } catch (NumberFormatException e) {
                throw conversionError(value, "Invalid double string: '" + str + "'", e);
            }
        }

        throw unsupportedType(value);
    }

    private Double convertNumber(Number n) {
        if (n instanceof BigDecimal bd) {
            double d = bd.doubleValue();
            validateFinite(d, n);
            return d;
        }
        // Integer, Long, etc.
        return n.doubleValue();
    }

    private void validateFinite(double d, Object original) {
        if (Double.isNaN(d)) {
            throw conversionError(original, "NaN values are not supported");
        }
        if (Double.isInfinite(d)) {
            throw conversionError(original, "Infinite values are not supported");
        }
    }
}
