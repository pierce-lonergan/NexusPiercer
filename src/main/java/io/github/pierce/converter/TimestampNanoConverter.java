package io.github.pierce.converter;

import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.Date;

/**
 * Converter for timestamp values with nanosecond precision.
 *
 * <p>Iceberg TIMESTAMP_NANO stores timestamps as nanoseconds since Unix epoch (long).
 * Handles both timezone-aware (timestamptz) and local timestamps.</p>
 */
public class TimestampNanoConverter extends AbstractTypeConverter<Long> {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final long NANOS_PER_MICRO = 1_000L;

    private final boolean adjustToUtc;

    public TimestampNanoConverter(ConversionConfig config, boolean adjustToUtc) {
        super(config, adjustToUtc ? "timestamp_ns_tz" : "timestamp_ns");
        this.adjustToUtc = adjustToUtc;
    }

    public boolean isAdjustToUtc() {
        return adjustToUtc;
    }

    @Override
    protected Long doConvert(Object value) throws TypeConversionException {
        // Handle Instant
        if (value instanceof Instant instant) {
            return instantToNanos(instant);
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
            return localDateTimeToNanos(ldt);
        }

        // Handle ZonedDateTime
        if (value instanceof ZonedDateTime zdt) {
            return instantToNanos(zdt.toInstant());
        }

        // Handle OffsetDateTime
        if (value instanceof OffsetDateTime odt) {
            return instantToNanos(odt.toInstant());
        }

        // Handle java.util.Date
        if (value instanceof Date date) {
            return date.getTime() * NANOS_PER_MILLI;
        }

        // Handle java.sql.Timestamp
        if (value instanceof java.sql.Timestamp ts) {
            long seconds = ts.getTime() / 1000;
            int nanos = ts.getNanos();
            return seconds * NANOS_PER_SECOND + nanos;
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
            case SECONDS -> l * NANOS_PER_SECOND;
            case MILLISECONDS -> l * NANOS_PER_MILLI;
            case MICROSECONDS -> l * NANOS_PER_MICRO;
            case NANOSECONDS -> l;
            case AUTO_DETECT -> autoDetectPrecision(l);
        };
    }

    private Long autoDetectPrecision(long l) {
        // Heuristic based on value magnitude
        // Reasonable timestamp ranges:
        // - Seconds: ~1970-2100 = 0 to ~4,100,000,000
        // - Milliseconds: ~1970-2100 = 0 to ~4,100,000,000,000
        // - Microseconds: ~1970-2100 = 0 to ~4,100,000,000,000,000
        // - Nanoseconds: ~1970-2100 = 0 to ~4,100,000,000,000,000,000

        long absValue = Math.abs(l);

        if (absValue > 1_000_000_000_000_000_000L) {
            // Already nanoseconds (or overflow, but we'll assume nanos)
            return l;
        } else if (absValue > 1_000_000_000_000_000L) {
            // Likely nanoseconds
            return l;
        } else if (absValue > 100_000_000_000_000L) {
            // Likely microseconds
            return l * NANOS_PER_MICRO;
        } else if (absValue > 100_000_000_000L) {
            // Likely milliseconds
            return l * NANOS_PER_MILLI;
        } else {
            // Could be seconds
            return l * NANOS_PER_SECOND;
        }
    }

    private Long localDateTimeToNanos(LocalDateTime ldt) {
        ZoneOffset offset;
        if (adjustToUtc || config.isAssumeUtcForNaiveTimestamps()) {
            offset = ZoneOffset.UTC;
        } else {
            offset = config.getDefaultTimezone().getRules().getOffset(ldt);
        }
        Instant instant = ldt.toInstant(offset);
        return instantToNanos(instant);
    }

    private Long parseTimestampString(String str, Object original) {
        // Try ISO-8601 instant (with Z or offset)
        try {
            Instant instant = Instant.parse(str);
            return instantToNanos(instant);
        } catch (DateTimeParseException e) {
            // Continue
        }

        // Try ISO local datetime
        try {
            LocalDateTime ldt = LocalDateTime.parse(str);
            return localDateTimeToNanos(ldt);
        } catch (DateTimeParseException e) {
            // Continue
        }

        // Try with space separator (common in databases)
        try {
            LocalDateTime ldt = LocalDateTime.parse(str.replace(" ", "T"));
            return localDateTimeToNanos(ldt);
        } catch (DateTimeParseException e) {
            // Continue
        }

        // Try ZonedDateTime
        try {
            ZonedDateTime zdt = ZonedDateTime.parse(str);
            return instantToNanos(zdt.toInstant());
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
     * Converts an Instant to nanoseconds since epoch.
     */
    public static long instantToNanos(Instant instant) {
        return instant.getEpochSecond() * NANOS_PER_SECOND + instant.getNano();
    }

    /**
     * Converts nanoseconds since epoch to Instant.
     */
    public static Instant nanosToInstant(long nanos) {
        long seconds = nanos / NANOS_PER_SECOND;
        int nanoAdjustment = (int) (nanos % NANOS_PER_SECOND);
        if (nanoAdjustment < 0) {
            seconds--;
            nanoAdjustment += NANOS_PER_SECOND;
        }
        return Instant.ofEpochSecond(seconds, nanoAdjustment);
    }

    /**
     * Converts nanoseconds to LocalDateTime in the given timezone.
     */
    public static LocalDateTime nanosToLocalDateTime(long nanos, ZoneId zone) {
        return nanosToInstant(nanos).atZone(zone).toLocalDateTime();
    }
}