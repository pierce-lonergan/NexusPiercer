package io.github.pierce.converter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Date;

/**
 * Converter for date values.
 *
 * <p>Iceberg stores dates as integer days since Unix epoch (1970-01-01).
 * This converter handles various date representations.</p>
 */
public class DateConverter extends AbstractTypeConverter<Integer> {

    private static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);

    public DateConverter() {
        this(ConversionConfig.defaults());
    }

    public DateConverter(ConversionConfig config) {
        super(config, "date");
    }

    @Override
    protected Integer doConvert(Object value) throws TypeConversionException {
        // Handle LocalDate
        if (value instanceof LocalDate ld) {
            return localDateToDays(ld);
        }

        // Handle Integer (already days since epoch)
        if (value instanceof Integer days) {
            return days;
        }

        // Handle Long
        if (value instanceof Long l) {
            // Could be epoch days or epoch millis - need to determine
            return convertLong(l, value);
        }

        // Handle LocalDateTime (extract date part)
        if (value instanceof LocalDateTime ldt) {
            return localDateToDays(ldt.toLocalDate());
        }

        // Handle Instant
        if (value instanceof Instant instant) {
            LocalDate ld = instant.atZone(config.getDefaultTimezone()).toLocalDate();
            return localDateToDays(ld);
        }

        // Handle java.util.Date
        if (value instanceof Date date) {
            LocalDate ld = date.toInstant().atZone(config.getDefaultTimezone()).toLocalDate();
            return localDateToDays(ld);
        }

        // Handle java.sql.Date
        if (value instanceof java.sql.Date sqlDate) {
            return localDateToDays(sqlDate.toLocalDate());
        }

        // Handle String
        String str = charSequenceToString(value);
        if (str != null) {
            if (str.isEmpty()) {
                if (config.isCoerceEmptyStringsToNull()) {
                    return null;
                }
                throw conversionError(value, "Empty string cannot be converted to date");
            }
            return parseDateString(str, value);
        }

        throw unsupportedType(value);
    }

    private Integer convertLong(long l, Object original) {
        // Heuristic: if the number is small, treat as days; if large, as millis
        // Days since epoch for year 3000 would be about 376,000
        // Millis since epoch for year 1970 would be 0
        if (Math.abs(l) < 1_000_000) {
            // Likely days
            return (int) l;
        } else {
            // Likely epoch millis - convert to days
            LocalDate ld = Instant.ofEpochMilli(l)
                    .atZone(config.getDefaultTimezone())
                    .toLocalDate();
            return localDateToDays(ld);
        }
    }

    private Integer parseDateString(String str, Object original) {
        // Try ISO date format first (yyyy-MM-dd)
        try {
            LocalDate ld = LocalDate.parse(str);
            return localDateToDays(ld);
        } catch (DateTimeParseException e) {
            // Continue trying other formats
        }

        // Try ISO datetime format
        try {
            LocalDateTime ldt = LocalDateTime.parse(str);
            return localDateToDays(ldt.toLocalDate());
        } catch (DateTimeParseException e) {
            // Continue trying other formats
        }

        // Try parsing as instant
        try {
            Instant instant = Instant.parse(str);
            LocalDate ld = instant.atZone(config.getDefaultTimezone()).toLocalDate();
            return localDateToDays(ld);
        } catch (DateTimeParseException e) {
            // Continue trying other formats
        }

        // Try common US format (MM/dd/yyyy)
        try {
            LocalDate ld = LocalDate.parse(str, DateTimeFormatter.ofPattern("M/d/yyyy"));
            return localDateToDays(ld);
        } catch (DateTimeParseException e) {
            // Continue
        }

        // Try epoch millis as string
        try {
            long millis = Long.parseLong(str);
            return convertLong(millis, original);
        } catch (NumberFormatException e) {
            // Not a number
        }

        throw conversionError(original, "Cannot parse date from string: '" + str + "'");
    }

    /**
     * Converts LocalDate to days since epoch.
     */
    public static int localDateToDays(LocalDate date) {
        return (int) ChronoUnit.DAYS.between(EPOCH, date);
    }

    /**
     * Converts days since epoch to LocalDate.
     */
    public static LocalDate daysToLocalDate(int days) {
        return EPOCH.plusDays(days);
    }
}
