package io.github.pierce.converter;

import java.util.Set;

/**
 * Converter for boolean values.
 *
 * <p>Handles various representations of boolean values:
 * <ul>
 *   <li>Boolean - returned as-is</li>
 *   <li>Number - 0 is false, non-zero is true</li>
 *   <li>String - "true", "false", "1", "0", "yes", "no", etc.</li>
 * </ul>
 */
public class BooleanConverter extends AbstractTypeConverter<Boolean> {

    private static final Set<String> TRUE_VALUES = Set.of(
            "true", "t", "yes", "y", "1", "on"
    );

    private static final Set<String> FALSE_VALUES = Set.of(
            "false", "f", "no", "n", "0", "off"
    );

    public BooleanConverter() {
        this(ConversionConfig.defaults());
    }

    public BooleanConverter(ConversionConfig config) {
        super(config, "boolean");
    }

    @Override
    protected Boolean doConvert(Object value) throws TypeConversionException {
        // Handle Boolean
        if (value instanceof Boolean b) {
            return b;
        }

        // Handle Number
        if (value instanceof Number n) {
            return n.doubleValue() != 0.0;
        }

        // Handle String
        String str = charSequenceToString(value);
        if (str != null) {
            String lower = str.toLowerCase();
            if (TRUE_VALUES.contains(lower)) {
                return true;
            }
            if (FALSE_VALUES.contains(lower)) {
                return false;
            }
            if (config.isCoerceEmptyStringsToNull() && str.isEmpty()) {
                return null;
            }
            if (config.isStrictTypeChecking()) {
                throw conversionError(value, "Invalid boolean string: '" + str + "'");
            }
            // Non-strict: non-empty string is truthy
            return !str.isEmpty();
        }

        throw unsupportedType(value);
    }
}
