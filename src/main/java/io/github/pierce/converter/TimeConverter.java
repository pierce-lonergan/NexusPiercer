package io.github.pierce.converter;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Converter for time values.
 *
 * <p>Iceberg stores time as microseconds since midnight (long).
 * This converter handles various time representations.</p>
 */
public class TimeConverter extends AbstractTypeConverter<Long> {

    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long MICROS_PER_MILLI = 1_000L;
    private static final long MICROS_PER_MINUTE = 60 * MICROS_PER_SECOND;
    private static final long MICROS_PER_HOUR = 60 * MICROS_PER_MINUTE;
    private static final long MICROS_PER_DAY = 24 * MICROS_PER_HOUR;

    public TimeConverter() {
        this(ConversionConfig.defaults());
    }

    public TimeConverter(ConversionConfig config) {
        super(config, "time");
    }

    @Override
    protected Long doConvert(Object value) throws TypeConversionException {
        // Handle LocalTime
        if (value instanceof LocalTime lt) {
            return localTimeToMicros(lt);
        }

        // Handle Long (already microseconds or needs conversion)
        if (value instanceof Long l) {
            return convertLong(l, value);
        }

        // Handle Integer (milliseconds or seconds)
        if (value instanceof Integer i) {
            return convertLong(i.longValue(), value);
        }

        // Handle String
        String str = charSequenceToString(value);
        if (str != null) {
            if (str.isEmpty()) {
                if (config.isCoerceEmptyStringsToNull()) {
                    return null;
                }
                throw conversionError(value, "Empty string cannot be converted to time");
            }
            return parseTimeString(str, value);
        }

        throw unsupportedType(value);
    }

    private Long convertLong(long l, Object original) {
        // Interpret based on configured precision or auto-detect
        return switch (config.getInputTimestampPrecision()) {
            case SECONDS -> l * MICROS_PER_SECOND;
            case MILLISECONDS -> l * MICROS_PER_MILLI;
            case MICROSECONDS -> l;
            case NANOSECONDS -> l / 1000;
            case AUTO_DETECT -> autoDetectPrecision(l);
        };
    }

    private Long autoDetectPrecision(long l) {
        // Heuristic based on value magnitude
        // Microseconds per day: 86,400,000,000
        // Milliseconds per day: 86,400,000
        // Seconds per day: 86,400

        if (l >= MICROS_PER_DAY) {
            // Likely already microseconds or larger (nanoseconds)
            if (l >= MICROS_PER_DAY * 1000) {
                // Nanoseconds
                return l / 1000;
            }
            return l;
        } else if (l >= 86_400_000) {
            // Between millis/day and micros/day - ambiguous, assume millis
            return l * MICROS_PER_MILLI;
        } else if (l >= 86_400) {
            // Between seconds/day and millis/day - assume milliseconds
            return l * MICROS_PER_MILLI;
        } else {
            // Small number - assume seconds
            return l * MICROS_PER_SECOND;
        }
    }

    private Long parseTimeString(String str, Object original) {
        // Try ISO time format (HH:mm:ss or HH:mm:ss.SSS)
        try {
            LocalTime lt = LocalTime.parse(str);
            return localTimeToMicros(lt);
        } catch (DateTimeParseException e) {
            // Continue trying other formats
        }

        // Try HH:mm format
        try {
            LocalTime lt = LocalTime.parse(str, DateTimeFormatter.ofPattern("H:mm"));
            return localTimeToMicros(lt);
        } catch (DateTimeParseException e) {
            // Continue
        }

        // Try parsing as numeric microseconds
        try {
            long l = Long.parseLong(str);
            return convertLong(l, original);
        } catch (NumberFormatException e) {
            // Not a number
        }

        throw conversionError(original, "Cannot parse time from string: '" + str + "'");
    }

    /**
     * Converts LocalTime to microseconds since midnight.
     */
    public static long localTimeToMicros(LocalTime time) {
        return time.toNanoOfDay() / 1000;
    }

    /**
     * Converts microseconds since midnight to LocalTime.
     */
    public static LocalTime microsToLocalTime(long micros) {
        return LocalTime.ofNanoOfDay(micros * 1000);
    }
}
