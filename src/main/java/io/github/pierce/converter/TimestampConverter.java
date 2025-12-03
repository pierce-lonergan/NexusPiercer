package io.github.pierce.converter;

import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.Date;

/**
 * Converter for timestamp values.
 *
 * <p>Iceberg stores timestamps as microseconds since Unix epoch (long).
 * Handles both timezone-aware (timestamptz) and local timestamps.</p>
 */
public class TimestampConverter extends AbstractTypeConverter<Long> {

    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long MICROS_PER_MILLI = 1_000L;

    private final boolean adjustToUtc;

    public TimestampConverter(ConversionConfig config, boolean adjustToUtc) {
        super(config, adjustToUtc ? "timestamptz" : "timestamp");
        this.adjustToUtc = adjustToUtc;
    }

    public boolean isAdjustToUtc() {
        return adjustToUtc;
    }

    @Override
    protected Long doConvert(Object value) throws TypeConversionException {
        // Handle Instant
        if (value instanceof Instant instant) {
            return instantToMicros(instant);
        }

        // Handle Long (epoch time in some precision)
        if (value instanceof Long l) {
            return convertLong(l, value);
        }

        // Handle Integer
        if (value instanceof Integer i) {
            return convertLong(i.longValue(), value);
        }

        // Handle LocalDateTime
        if (value instanceof LocalDateTime ldt) {
            return localDateTimeToMicros(ldt);
        }

        // Handle ZonedDateTime
        if (value instanceof ZonedDateTime zdt) {
            return instantToMicros(zdt.toInstant());
        }

        // Handle OffsetDateTime
        if (value instanceof OffsetDateTime odt) {
            return instantToMicros(odt.toInstant());
        }

        // Handle java.util.Date
        if (value instanceof Date date) {
            return date.getTime() * MICROS_PER_MILLI;
        }

        // Handle java.sql.Timestamp
        if (value instanceof java.sql.Timestamp ts) {
            long seconds = ts.getTime() / 1000;
            int nanos = ts.getNanos();
            return seconds * MICROS_PER_SECOND + nanos / 1000;
        }

        // Handle String
        String str = charSequenceToString(value);
        if (str != null) {
            if (str.isEmpty()) {
                if (config.isCoerceEmptyStringsToNull()) {
                    return null;
                }
                throw conversionError(value, "Empty string cannot be converted to timestamp");
            }
            return parseTimestampString(str, value);
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
        // Reasonable timestamp ranges:
        // - Seconds: ~1970-2100 = 0 to ~4,100,000,000
        // - Milliseconds: ~1970-2100 = 0 to ~4,100,000,000,000
        // - Microseconds: ~1970-2100 = 0 to ~4,100,000,000,000,000

        long absValue = Math.abs(l);

        // If the value is very large, it's likely nanoseconds or microseconds
        if (absValue > 1_000_000_000_000_000L) {
            // Likely nanoseconds
            return l / 1000;
        } else if (absValue > 100_000_000_000_000L) {
            // Likely microseconds
            return l;
        } else if (absValue > 100_000_000_000L) {
            // Likely milliseconds
            return l * MICROS_PER_MILLI;
        } else {
            // Could be seconds (reasonable epoch timestamp range)
            // 100B seconds is year ~5100
            return l * MICROS_PER_SECOND;
        }
    }

    private Long localDateTimeToMicros(LocalDateTime ldt) {
        ZoneOffset offset;
        if (adjustToUtc || config.isAssumeUtcForNaiveTimestamps()) {
            offset = ZoneOffset.UTC;
        } else {
            offset = config.getDefaultTimezone().getRules().getOffset(ldt);
        }
        Instant instant = ldt.toInstant(offset);
        return instantToMicros(instant);
    }

    private Long parseTimestampString(String str, Object original) {
        // Try ISO-8601 instant (with Z or offset)
        try {
            Instant instant = Instant.parse(str);
            return instantToMicros(instant);
        } catch (DateTimeParseException e) {
            // Continue
        }

        // Try ISO local datetime
        try {
            LocalDateTime ldt = LocalDateTime.parse(str);
            return localDateTimeToMicros(ldt);
        } catch (DateTimeParseException e) {
            // Continue
        }

        // Try with space separator (common in databases)
        try {
            LocalDateTime ldt = LocalDateTime.parse(str.replace(" ", "T"));
            return localDateTimeToMicros(ldt);
        } catch (DateTimeParseException e) {
            // Continue
        }

        // Try ZonedDateTime
        try {
            ZonedDateTime zdt = ZonedDateTime.parse(str);
            return instantToMicros(zdt.toInstant());
        } catch (DateTimeParseException e) {
            // Continue
        }

        // Try parsing as numeric timestamp
        try {
            long l = Long.parseLong(str);
            return convertLong(l, original);
        } catch (NumberFormatException e) {
            // Not a number
        }

        throw conversionError(original, "Cannot parse timestamp from string: '" + str + "'");
    }

    /**
     * Converts an Instant to microseconds since epoch.
     */
    public static long instantToMicros(Instant instant) {
        return instant.getEpochSecond() * MICROS_PER_SECOND + instant.getNano() / 1000;
    }

    /**
     * Converts microseconds since epoch to Instant.
     */
    public static Instant microsToInstant(long micros) {
        long seconds = micros / MICROS_PER_SECOND;
        int nanoAdjustment = (int) ((micros % MICROS_PER_SECOND) * 1000);
        if (nanoAdjustment < 0) {
            seconds--;
            nanoAdjustment += 1_000_000_000;
        }
        return Instant.ofEpochSecond(seconds, nanoAdjustment);
    }

    /**
     * Converts microseconds to LocalDateTime in the given timezone.
     */
    public static LocalDateTime microsToLocalDateTime(long micros, ZoneId zone) {
        return microsToInstant(micros).atZone(zone).toLocalDateTime();
    }
}
