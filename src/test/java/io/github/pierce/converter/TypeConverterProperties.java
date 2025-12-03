package io.github.pierce.converter;



import net.jqwik.api.*;
import net.jqwik.api.constraints.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

/**
 * Property-based tests for type converters using jqwik.
 *
 * These tests verify converter behavior across a wide range of inputs,
 * catching edge cases that might be missed by example-based tests.
 */
class TypeConverterProperties {

    private final ConversionConfig config = ConversionConfig.defaults();

    // ==================== Integer Converter Properties ====================

    @Property
    @Label("IntegerConverter: round-trip for valid integers")
    void integerRoundTrip(@ForAll @IntRange(min = Integer.MIN_VALUE, max = Integer.MAX_VALUE) int value) {
        IntegerConverter converter = new IntegerConverter(config);

        Integer converted = converter.convert(value);

        assertThat(converted).isEqualTo(value);
    }

    @Property
    @Label("IntegerConverter: string parsing preserves value")
    void integerStringParsingPreservesValue(@ForAll @IntRange(min = -1_000_000, max = 1_000_000) int value) {
        IntegerConverter converter = new IntegerConverter(config);

        Integer converted = converter.convert(String.valueOf(value));

        assertThat(converted).isEqualTo(value);
    }

    @Property
    @Label("IntegerConverter: Long within range converts correctly")
    void integerFromLongWithinRange(@ForAll @LongRange(min = Integer.MIN_VALUE, max = Integer.MAX_VALUE) long value) {
        IntegerConverter converter = new IntegerConverter(config);

        Integer converted = converter.convert(value);

        assertThat(converted).isEqualTo((int) value);
    }

    // ==================== Long Converter Properties ====================

    @Property
    @Label("LongConverter: round-trip for valid longs")
    void longRoundTrip(@ForAll long value) {
        LongConverter converter = new LongConverter(config);

        Long converted = converter.convert(value);

        assertThat(converted).isEqualTo(value);
    }

    @Property
    @Label("LongConverter: Integer promotion preserves value")
    void longFromIntegerPreservesValue(@ForAll int value) {
        LongConverter converter = new LongConverter(config);

        Long converted = converter.convert(value);

        assertThat(converted).isEqualTo((long) value);
    }

    // ==================== Double Converter Properties ====================

    @Property
    @Label("DoubleConverter: round-trip for finite doubles")
    void doubleRoundTrip(@ForAll @DoubleRange(min = -1e15, max = 1e15) double value) {
        // Skip NaN and Infinity for this test
        Assume.that(Double.isFinite(value));

        DoubleConverter converter = new DoubleConverter(config);

        Double converted = converter.convert(value);

        assertThat(converted).isEqualTo(value);
    }

    @Property
    @Label("DoubleConverter: Float promotion preserves precision")
    void doubleFromFloatPreservesPrecision(@ForAll float value) {
        Assume.that(Float.isFinite(value));

        DoubleConverter converter = new DoubleConverter(config);

        Double converted = converter.convert(value);

        assertThat(converted).isEqualTo((double) value);
    }

    // ==================== String Converter Properties ====================

    @Property
    @Label("StringConverter: round-trip for strings")
    void stringRoundTrip(@ForAll @StringLength(max = 1000) String value) {
        StringConverter converter = new StringConverter(config);

        String converted = converter.convert(value);

        assertThat(converted).isEqualTo(value);
    }

    @Property
    @Label("StringConverter: number toString is reversible")
    void stringFromNumberToStringIsReversible(@ForAll @IntRange(min = -1_000_000, max = 1_000_000) int value) {
        StringConverter converter = new StringConverter(config);
        IntegerConverter intConverter = new IntegerConverter(config);

        String stringValue = converter.convert(value);
        Integer backToInt = intConverter.convert(stringValue);

        assertThat(backToInt).isEqualTo(value);
    }

    // ==================== Boolean Converter Properties ====================

    @Property
    @Label("BooleanConverter: round-trip for booleans")
    void booleanRoundTrip(@ForAll boolean value) {
        BooleanConverter converter = new BooleanConverter(config);

        Boolean converted = converter.convert(value);

        assertThat(converted).isEqualTo(value);
    }

    @Property
    @Label("BooleanConverter: non-zero numbers are true")
    void nonZeroNumbersAreTrue(@ForAll @IntRange(min = 1, max = Integer.MAX_VALUE) int value) {
        BooleanConverter converter = new BooleanConverter(config);

        Boolean converted = converter.convert(value);

        assertThat(converted).isTrue();
    }

    // ==================== Decimal Converter Properties ====================

    @Property(tries = 100)
    @Label("DecimalConverter: scale is correctly set")
    void decimalScaleIsCorrectlySet(
            @ForAll @LongRange(min = -99999999L, max = 99999999L) long unscaledValue) {
        DecimalConverter converter = new DecimalConverter(config, 10, 2);

        BigDecimal input = BigDecimal.valueOf(unscaledValue, 2);
        BigDecimal converted = converter.convert(input);

        assertThat(converted.scale()).isEqualTo(2);
    }

    @Property(tries = 100)
    @Label("DecimalConverter: values within precision are preserved")
    void decimalValuesWithinPrecisionArePreserved(
            @ForAll @LongRange(min = -9999999L, max = 9999999L) long unscaledValue) {
        DecimalConverter converter = new DecimalConverter(config, 10, 2);

        BigDecimal input = BigDecimal.valueOf(unscaledValue, 2);
        BigDecimal converted = converter.convert(input);

        assertThat(converted).isEqualByComparingTo(input);
    }

    @Property(tries = 50)
    @Label("DecimalConverter: string parsing produces same result as BigDecimal constructor")
    void decimalStringParsingMatchesBigDecimalConstructor(
            @ForAll @LongRange(min = -9999999L, max = 9999999L) long value) {
        DecimalConverter converter = new DecimalConverter(config, 10, 2);

        String stringValue = String.format("%d.%02d", value / 100, Math.abs(value % 100));
        BigDecimal expected = new BigDecimal(stringValue).setScale(2, RoundingMode.HALF_UP);
        BigDecimal converted = converter.convert(stringValue);

        assertThat(converted).isEqualByComparingTo(expected);
    }

    // ==================== UUID Converter Properties ====================

    @Property
    @Label("UUIDConverter: round-trip for UUIDs")
    void uuidRoundTrip(@ForAll("uuids") UUID value) {
        UUIDConverter converter = new UUIDConverter(config);

        UUID converted = converter.convert(value);

        assertThat(converted).isEqualTo(value);
    }

    @Property
    @Label("UUIDConverter: string parsing round-trip")
    void uuidStringRoundTrip(@ForAll("uuids") UUID value) {
        UUIDConverter converter = new UUIDConverter(config);

        UUID converted = converter.convert(value.toString());

        assertThat(converted).isEqualTo(value);
    }

    @Property
    @Label("UUIDConverter: byte array round-trip")
    void uuidByteArrayRoundTrip(@ForAll("uuids") UUID value) {
        UUIDConverter converter = new UUIDConverter(config);

        byte[] bytes = UUIDConverter.toBytes(value);
        UUID converted = converter.convert(bytes);

        assertThat(converted).isEqualTo(value);
    }

    @Provide
    Arbitrary<UUID> uuids() {
        return Arbitraries.create(UUID::randomUUID);
    }

    // ==================== Date Converter Properties ====================

    @Property
    @Label("DateConverter: round-trip for dates")
    void dateRoundTrip(@ForAll("localDates") LocalDate value) {
        DateConverter converter = new DateConverter(config);

        Integer days = converter.convert(value);
        LocalDate converted = DateConverter.daysToLocalDate(days);

        assertThat(converted).isEqualTo(value);
    }

    @Property
    @Label("DateConverter: epoch is day 0")
    void epochIsDayZero() {
        DateConverter converter = new DateConverter(config);

        Integer days = converter.convert(LocalDate.of(1970, 1, 1));

        assertThat(days).isEqualTo(0);
    }

    @Provide
    Arbitrary<LocalDate> localDates() {
        return Arbitraries.integers()
                .between(1970, 2100)
                .flatMap(year -> Arbitraries.integers().between(1, 12)
                        .flatMap(month -> Arbitraries.integers()
                                .between(1, LocalDate.of(year, month, 1).lengthOfMonth())
                                .map(day -> LocalDate.of(year, month, day))));
    }

    // ==================== Time Converter Properties ====================

    @Property
    @Label("TimeConverter: round-trip for times (microsecond precision)")
    void timeRoundTrip(@ForAll("localTimes") LocalTime value) {
        TimeConverter converter = new TimeConverter(config);

        // Truncate to microsecond precision for comparison
        LocalTime truncated = value.truncatedTo(java.time.temporal.ChronoUnit.MICROS);

        Long micros = converter.convert(truncated);
        LocalTime converted = TimeConverter.microsToLocalTime(micros);

        assertThat(converted).isEqualTo(truncated);
    }

    @Property
    @Label("TimeConverter: midnight is 0 microseconds")
    void midnightIsZero() {
        TimeConverter converter = new TimeConverter(config);

        Long micros = converter.convert(LocalTime.MIDNIGHT);

        assertThat(micros).isEqualTo(0L);
    }

    @Provide
    Arbitrary<LocalTime> localTimes() {
        return Arbitraries.integers().between(0, 23)
                .flatMap(hour -> Arbitraries.integers().between(0, 59)
                        .flatMap(minute -> Arbitraries.integers().between(0, 59)
                                .flatMap(second -> Arbitraries.integers().between(0, 999999)
                                        .map(micro -> LocalTime.of(hour, minute, second, micro * 1000)))));
    }

    // ==================== Timestamp Converter Properties ====================

    @Property
    @Label("TimestampConverter: round-trip for instants (microsecond precision)")
    void timestampRoundTrip(@ForAll("instants") Instant value) {
        TimestampConverter converter = new TimestampConverter(config, true);

        // Truncate to microsecond precision
        Instant truncated = Instant.ofEpochSecond(
                value.getEpochSecond(),
                (value.getNano() / 1000) * 1000);

        Long micros = converter.convert(truncated);
        Instant converted = TimestampConverter.microsToInstant(micros);

        assertThat(converted).isEqualTo(truncated);
    }

    @Property
    @Label("TimestampConverter: epoch is 0 microseconds")
    void epochIsZeroMicros() {
        TimestampConverter converter = new TimestampConverter(config, true);

        Long micros = converter.convert(Instant.EPOCH);

        assertThat(micros).isEqualTo(0L);
    }

    @Provide
    Arbitrary<Instant> instants() {
        // Generate instants between 2000 and 2100
        long min = Instant.parse("2000-01-01T00:00:00Z").getEpochSecond();
        long max = Instant.parse("2100-01-01T00:00:00Z").getEpochSecond();

        return Arbitraries.longs().between(min, max)
                .flatMap(seconds -> Arbitraries.integers().between(0, 999_999_999)
                        .map(nanos -> Instant.ofEpochSecond(seconds, nanos)));
    }

    // ==================== Cross-Converter Properties ====================

    @Property
    @Label("Integer to Long promotion is lossless")
    void integerToLongPromotionIsLossless(@ForAll int value) {
        IntegerConverter intConverter = new IntegerConverter(config);
        LongConverter longConverter = new LongConverter(config);

        Integer intResult = intConverter.convert(value);
        Long longResult = longConverter.convert(value);

        assertThat(longResult.intValue()).isEqualTo(intResult);
    }

    @Property
    @Label("Float to Double promotion is lossless")
    void floatToDoublePromotionIsLossless(@ForAll float value) {
        Assume.that(Float.isFinite(value));

        FloatConverter floatConverter = new FloatConverter(config);
        DoubleConverter doubleConverter = new DoubleConverter(config);

        Float floatResult = floatConverter.convert(value);
        Double doubleResult = doubleConverter.convert(value);

        assertThat(doubleResult.floatValue()).isEqualTo(floatResult);
    }

    // ==================== Null Handling Properties ====================

    @Property
    @Label("All converters return null for null input")
    void allConvertersReturnNullForNullInput(@ForAll("converterTypes") String converterType) {
        Object result = switch (converterType) {
            case "boolean" -> new BooleanConverter(config).convert(null);
            case "integer" -> new IntegerConverter(config).convert(null);
            case "long" -> new LongConverter(config).convert(null);
            case "float" -> new FloatConverter(config).convert(null);
            case "double" -> new DoubleConverter(config).convert(null);
            case "string" -> new StringConverter(config).convert(null);
            case "uuid" -> new UUIDConverter(config).convert(null);
            case "date" -> new DateConverter(config).convert(null);
            case "time" -> new TimeConverter(config).convert(null);
            case "timestamp" -> new TimestampConverter(config, true).convert(null);
            default -> throw new IllegalArgumentException("Unknown converter: " + converterType);
        };

        assertThat(result).isNull();
    }

    @Provide
    Arbitrary<String> converterTypes() {
        return Arbitraries.of(
                "boolean", "integer", "long", "float", "double",
                "string", "uuid", "date", "time", "timestamp"
        );
    }
}