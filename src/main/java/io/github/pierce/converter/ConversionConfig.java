package io.github.pierce.converter;

import java.math.RoundingMode;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Configuration options for schema-based map conversion.
 *
 * <p>This class uses the builder pattern for configuration and is immutable
 * once constructed.</p>
 */
public final class ConversionConfig {

    // Error handling
    private final ErrorHandlingMode errorHandlingMode;
    private final boolean strictTypeChecking;

    // Null handling
    private final NullHandlingMode nullHandlingMode;
    private final boolean coerceEmptyStringsToNull;

    // Numeric handling
    private final boolean allowNumericOverflow;
    private final boolean allowPrecisionLoss;
    private final RoundingMode decimalRoundingMode;

    // Timestamp handling
    private final ZoneId defaultTimezone;
    private final TimestampPrecision inputTimestampPrecision;
    private final boolean assumeUtcForNaiveTimestamps;

    // String handling
    private final boolean trimStrings;
    private final StringTruncationMode stringTruncationMode;

    // Schema evolution
    private final boolean allowExtraFields;
    private final boolean allowMissingOptionalFields;
    private final boolean enableTypePromotion;
    private final boolean useSchemaDefaults;

    // Performance
    private final boolean cacheConverters;
    private final int initialCacheCapacity;

    // Output format
    private final OutputFormat outputFormat;

    private ConversionConfig(Builder builder) {
        this.errorHandlingMode = builder.errorHandlingMode;
        this.strictTypeChecking = builder.strictTypeChecking;
        this.nullHandlingMode = builder.nullHandlingMode;
        this.coerceEmptyStringsToNull = builder.coerceEmptyStringsToNull;
        this.allowNumericOverflow = builder.allowNumericOverflow;
        this.allowPrecisionLoss = builder.allowPrecisionLoss;
        this.decimalRoundingMode = builder.decimalRoundingMode;
        this.defaultTimezone = builder.defaultTimezone;
        this.inputTimestampPrecision = builder.inputTimestampPrecision;
        this.assumeUtcForNaiveTimestamps = builder.assumeUtcForNaiveTimestamps;
        this.trimStrings = builder.trimStrings;
        this.stringTruncationMode = builder.stringTruncationMode;
        this.allowExtraFields = builder.allowExtraFields;
        this.allowMissingOptionalFields = builder.allowMissingOptionalFields;
        this.enableTypePromotion = builder.enableTypePromotion;
        this.useSchemaDefaults = builder.useSchemaDefaults;
        this.cacheConverters = builder.cacheConverters;
        this.initialCacheCapacity = builder.initialCacheCapacity;
        this.outputFormat = builder.outputFormat;
    }

    /**
     * Returns a builder with default settings.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the default configuration.
     */
    public static ConversionConfig defaults() {
        return builder().build();
    }

    /**
     * Returns a strict configuration that fails fast on any issues.
     */
    public static ConversionConfig strict() {
        return builder()
                .errorHandlingMode(ErrorHandlingMode.FAIL_FAST)
                .strictTypeChecking(true)
                .allowNumericOverflow(false)
                .allowPrecisionLoss(false)
                .allowExtraFields(false)
                .build();
    }

    /**
     * Returns a lenient configuration that attempts to convert whenever possible.
     */
    public static ConversionConfig lenient() {
        return builder()
                .errorHandlingMode(ErrorHandlingMode.COLLECT_ERRORS)
                .strictTypeChecking(false)
                .allowNumericOverflow(false) // Still don't silently overflow
                .allowPrecisionLoss(true)
                .allowExtraFields(true)
                .allowMissingOptionalFields(true)
                .enableTypePromotion(true)
                .coerceEmptyStringsToNull(true)
                .trimStrings(true)
                .build();
    }

    // Getters

    public ErrorHandlingMode getErrorHandlingMode() {
        return errorHandlingMode;
    }

    public boolean isStrictTypeChecking() {
        return strictTypeChecking;
    }

    public NullHandlingMode getNullHandlingMode() {
        return nullHandlingMode;
    }

    public boolean isCoerceEmptyStringsToNull() {
        return coerceEmptyStringsToNull;
    }

    public boolean isAllowNumericOverflow() {
        return allowNumericOverflow;
    }

    public boolean isAllowPrecisionLoss() {
        return allowPrecisionLoss;
    }

    public RoundingMode getDecimalRoundingMode() {
        return decimalRoundingMode;
    }

    public ZoneId getDefaultTimezone() {
        return defaultTimezone;
    }

    public TimestampPrecision getInputTimestampPrecision() {
        return inputTimestampPrecision;
    }

    public boolean isAssumeUtcForNaiveTimestamps() {
        return assumeUtcForNaiveTimestamps;
    }

    public boolean isTrimStrings() {
        return trimStrings;
    }

    public StringTruncationMode getStringTruncationMode() {
        return stringTruncationMode;
    }

    public boolean isAllowExtraFields() {
        return allowExtraFields;
    }

    public boolean isAllowMissingOptionalFields() {
        return allowMissingOptionalFields;
    }

    public boolean isEnableTypePromotion() {
        return enableTypePromotion;
    }

    public boolean isUseSchemaDefaults() {
        return useSchemaDefaults;
    }

    public boolean isCacheConverters() {
        return cacheConverters;
    }

    public int getInitialCacheCapacity() {
        return initialCacheCapacity;
    }

    public OutputFormat getOutputFormat() {
        return outputFormat;
    }

    // Enums

    public enum ErrorHandlingMode {
        /** Throw an exception on the first error encountered */
        FAIL_FAST,
        /** Collect all errors and return them together */
        COLLECT_ERRORS,
        /** Skip fields with errors and continue */
        SKIP_ON_ERROR
    }

    public enum NullHandlingMode {
        /** Throw an exception for null values in required fields */
        STRICT,
        /** Use schema defaults for null required fields if available */
        USE_DEFAULTS,
        /** Pass null through even for required fields */
        PASS_THROUGH
    }

    public enum TimestampPrecision {
        /** Input timestamps are in seconds */
        SECONDS,
        /** Input timestamps are in milliseconds */
        MILLISECONDS,
        /** Input timestamps are in microseconds */
        MICROSECONDS,
        /** Input timestamps are in nanoseconds */
        NANOSECONDS,
        /** Attempt to detect precision automatically */
        AUTO_DETECT
    }

    public enum StringTruncationMode {
        /** Throw an error if string exceeds fixed length */
        ERROR,
        /** Truncate strings silently to fit */
        TRUNCATE,
        /** Truncate and add warning */
        TRUNCATE_WITH_WARNING
    }

    public enum OutputFormat {
        /** Return GenericRecord (Iceberg or Avro depending on converter) */
        GENERIC_RECORD,
        /** Return {@code Map<String, Object>} with converted values */
        MAP
    }

    // Builder

    public static final class Builder {
        private ErrorHandlingMode errorHandlingMode = ErrorHandlingMode.FAIL_FAST;
        private boolean strictTypeChecking = false;
        private NullHandlingMode nullHandlingMode = NullHandlingMode.STRICT;
        private boolean coerceEmptyStringsToNull = false;
        private boolean allowNumericOverflow = false;
        private boolean allowPrecisionLoss = false;
        private RoundingMode decimalRoundingMode = RoundingMode.HALF_UP;
        private ZoneId defaultTimezone = ZoneOffset.UTC;
        private TimestampPrecision inputTimestampPrecision = TimestampPrecision.AUTO_DETECT;
        private boolean assumeUtcForNaiveTimestamps = true;
        private boolean trimStrings = false;
        private StringTruncationMode stringTruncationMode = StringTruncationMode.ERROR;
        private boolean allowExtraFields = true;
        private boolean allowMissingOptionalFields = true;
        private boolean enableTypePromotion = true;
        private boolean useSchemaDefaults = true;
        private boolean cacheConverters = true;
        private int initialCacheCapacity = 64;
        private OutputFormat outputFormat = OutputFormat.GENERIC_RECORD;

        public Builder errorHandlingMode(ErrorHandlingMode mode) {
            this.errorHandlingMode = mode;
            return this;
        }

        public Builder strictTypeChecking(boolean strict) {
            this.strictTypeChecking = strict;
            return this;
        }

        public Builder nullHandlingMode(NullHandlingMode mode) {
            this.nullHandlingMode = mode;
            return this;
        }

        public Builder coerceEmptyStringsToNull(boolean coerce) {
            this.coerceEmptyStringsToNull = coerce;
            return this;
        }

        public Builder allowNumericOverflow(boolean allow) {
            this.allowNumericOverflow = allow;
            return this;
        }

        public Builder allowPrecisionLoss(boolean allow) {
            this.allowPrecisionLoss = allow;
            return this;
        }

        public Builder decimalRoundingMode(RoundingMode mode) {
            this.decimalRoundingMode = mode;
            return this;
        }

        public Builder defaultTimezone(ZoneId timezone) {
            this.defaultTimezone = timezone;
            return this;
        }

        public Builder inputTimestampPrecision(TimestampPrecision precision) {
            this.inputTimestampPrecision = precision;
            return this;
        }

        public Builder assumeUtcForNaiveTimestamps(boolean assume) {
            this.assumeUtcForNaiveTimestamps = assume;
            return this;
        }

        public Builder trimStrings(boolean trim) {
            this.trimStrings = trim;
            return this;
        }

        public Builder stringTruncationMode(StringTruncationMode mode) {
            this.stringTruncationMode = mode;
            return this;
        }

        public Builder allowExtraFields(boolean allow) {
            this.allowExtraFields = allow;
            return this;
        }

        public Builder allowMissingOptionalFields(boolean allow) {
            this.allowMissingOptionalFields = allow;
            return this;
        }

        public Builder enableTypePromotion(boolean enable) {
            this.enableTypePromotion = enable;
            return this;
        }

        public Builder useSchemaDefaults(boolean use) {
            this.useSchemaDefaults = use;
            return this;
        }

        public Builder cacheConverters(boolean cache) {
            this.cacheConverters = cache;
            return this;
        }

        public Builder initialCacheCapacity(int capacity) {
            this.initialCacheCapacity = capacity;
            return this;
        }

        public Builder outputFormat(OutputFormat format) {
            this.outputFormat = format;
            return this;
        }

        public ConversionConfig build() {
            return new ConversionConfig(this);
        }
    }
}