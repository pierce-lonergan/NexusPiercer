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

    /**
     * The formats parseTimestampString tries, in order, named so the failure can say what it attempted.
     *
     * <p>THE CASCADE STAYS A CASCADE. The branches are not homogeneous - they produce
     * different intermediate types - so folding them into a formatter array plus a loop would
     * need a lambda per branch and would risk silently reordering which format wins for
     * ambiguous input. Each catch body records the failure into {@code firstFailure} instead
     * of discarding it: that is real work rather than a comment, it costs nothing on the
     * success path, and it finally hands the discarded exception to the terminal.</p>
     *
     * <p>THIS CLASS: FIVE branches, and config is reached INDIRECTLY rather than in any branch
     * body, exactly as in {@link TimestampConverter} - the shared local-datetime helper reads
     * the configured default timezone. No branch body names config.</p>
     *
     * <p>PMD's EmptyCatchBlock does NOT accept a commented catch as configured -
     * allowCommentedBlocks defaults to false and src/main/pmd/pmd-ruleset.xml sets no
     * properties - so all of these counted while carrying comments like "// Continue trying
     * other formats". Flipping that property would have cleared twenty-five findings without
     * a thought applied to any of them.</p>
     */
    private static final String TRIED_FORMATS = "ISO-8601 instant, ISO local datetime, space-separated datetime, ZonedDateTime, numeric epoch";

    private Long parseTimestampString(String str, Object original) {
        // See ConversionFailure for why this is a method call and not two obvious lines, and
        // for why it is the FIRST failure rather than the last.
        RuntimeException firstFailure = null;
        // Try ISO-8601 instant (with Z or offset)
        try {
            Instant instant = Instant.parse(str);
            return instantToNanos(instant);
        } catch (DateTimeParseException e) {
            // Continue
            firstFailure = ConversionFailure.first(firstFailure, e);
        }

        // Try ISO local datetime
        try {
            LocalDateTime ldt = LocalDateTime.parse(str);
            return localDateTimeToNanos(ldt);
        } catch (DateTimeParseException e) {
            // Continue
            firstFailure = ConversionFailure.first(firstFailure, e);
        }

        // Try with space separator (common in databases)
        try {
            LocalDateTime ldt = LocalDateTime.parse(str.replace(" ", "T"));
            return localDateTimeToNanos(ldt);
        } catch (DateTimeParseException e) {
            // Continue
            firstFailure = ConversionFailure.first(firstFailure, e);
        }

        // Try ZonedDateTime
        try {
            ZonedDateTime zdt = ZonedDateTime.parse(str);
            return instantToNanos(zdt.toInstant());
        } catch (DateTimeParseException e) {
            // Continue
            firstFailure = ConversionFailure.first(firstFailure, e);
        }

        // Try parsing as numeric timestamp
        try {
            long l = Long.parseLong(str);
            return convertLong(l, original);
        } catch (NumberFormatException e) {
            // Not a number
            firstFailure = ConversionFailure.first(firstFailure, e);
        }

        throw conversionError(original, "Cannot parse timestamp from string: '" + str + "'"
                + ". Tried: " + TRIED_FORMATS, firstFailure);
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