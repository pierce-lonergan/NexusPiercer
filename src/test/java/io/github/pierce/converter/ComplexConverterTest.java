package io.github.pierce.converter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.Base64;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for complex type converters: Decimal, UUID, Date, Time, Timestamp, Binary.
 */
class ComplexConverterTest {

    private ConversionConfig defaultConfig;

    @BeforeEach
    void setUp() {
        defaultConfig = ConversionConfig.defaults();
    }

    // ==================== Decimal Converter Tests ====================

    @Nested
    @DisplayName("DecimalConverter")
    class DecimalConverterTests {

        @Test
        @DisplayName("converts BigDecimal directly")
        void convertsBigDecimalDirectly() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 10, 2);
            BigDecimal result = converter.convert(new BigDecimal("123.45"));
            assertThat(result).isEqualByComparingTo("123.45");
            assertThat(result.scale()).isEqualTo(2);
        }

        @Test
        @DisplayName("adjusts scale automatically")
        void adjustsScale() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 10, 2);
            BigDecimal result = converter.convert(new BigDecimal("123.4"));
            assertThat(result.scale()).isEqualTo(2);
            assertThat(result).isEqualByComparingTo("123.40");
        }

        @Test
        @DisplayName("converts Long to BigDecimal")
        void convertsLong() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 10, 2);
            BigDecimal result = converter.convert(123L);
            assertThat(result).isEqualByComparingTo("123.00");
        }

        @Test
        @DisplayName("converts Integer to BigDecimal")
        void convertsInteger() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 10, 2);
            BigDecimal result = converter.convert(42);
            assertThat(result).isEqualByComparingTo("42.00");
        }

        @Test
        @DisplayName("converts Double to BigDecimal")
        void convertsDouble() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 10, 2);
            BigDecimal result = converter.convert(99.99);
            assertThat(result).isEqualByComparingTo("99.99");
        }

        @Test
        @DisplayName("converts String to BigDecimal")
        void convertsString() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 10, 2);
            BigDecimal result = converter.convert("123.45");
            assertThat(result).isEqualByComparingTo("123.45");
        }

        @Test
        @DisplayName("converts BigInteger to BigDecimal")
        void convertsBigInteger() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 10, 2);
            BigDecimal result = converter.convert(BigInteger.valueOf(12345));
            assertThat(result).isEqualByComparingTo("12345.00");
        }

        @Test
        @DisplayName("converts byte array (Avro format)")
        void convertsByteArray() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 10, 2);
            // 12345 as byte array with scale 2 = 123.45
            byte[] bytes = BigInteger.valueOf(12345).toByteArray();
            BigDecimal result = converter.convert(bytes);
            assertThat(result).isEqualByComparingTo("123.45");
        }

        @Test
        @DisplayName("converts ByteBuffer (Avro format)")
        void convertsByteBuffer() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 10, 2);
            byte[] bytes = BigInteger.valueOf(12345).toByteArray();
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            BigDecimal result = converter.convert(buffer);
            assertThat(result).isEqualByComparingTo("123.45");
        }

        @Test
        @DisplayName("throws on precision overflow")
        void throwsOnPrecisionOverflow() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 5, 2);
            assertThatThrownBy(() -> converter.convert(new BigDecimal("9999.99")))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("precision");
        }

        @Test
        @DisplayName("throws on value exceeding bounds")
        void throwsOnValueExceedingBounds() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 5, 2);
            assertThatThrownBy(() -> converter.convert(new BigDecimal("10000.00")))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("exceeds");
        }

        @Test
        @DisplayName("throws on non-finite double")
        void throwsOnNonFiniteDouble() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 10, 2);
            assertThatThrownBy(() -> converter.convert(Double.NaN))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("non-finite");
        }

        @Test
        @DisplayName("throws on invalid string")
        void throwsOnInvalidString() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 10, 2);
            assertThatThrownBy(() -> converter.convert("not a number"))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("Invalid decimal string");
        }

        @Test
        @DisplayName("toBytes and fromBytes round-trip")
        void toBytesRoundTrip() {
            BigDecimal original = new BigDecimal("123.45");
            byte[] bytes = DecimalConverter.toBytes(original);
            BigDecimal restored = DecimalConverter.fromBytes(bytes, 2);
            assertThat(restored).isEqualByComparingTo(original);
        }
    }

    // ==================== UUID Converter Tests ====================

    @Nested
    @DisplayName("UUIDConverter")
    class UUIDConverterTests {

        @Test
        @DisplayName("converts UUID directly")
        void convertsUUIDDirectly() {
            UUIDConverter converter = new UUIDConverter(defaultConfig);
            UUID uuid = UUID.randomUUID();
            assertThat(converter.convert(uuid)).isEqualTo(uuid);
        }

        @Test
        @DisplayName("returns null for null input")
        void returnsNullForNull() {
            UUIDConverter converter = new UUIDConverter(defaultConfig);
            assertThat(converter.convert(null)).isNull();
        }

        @Test
        @DisplayName("parses standard UUID string")
        void parsesStandardUUIDString() {
            UUIDConverter converter = new UUIDConverter(defaultConfig);
            String uuidStr = "550e8400-e29b-41d4-a716-446655440000";
            UUID result = converter.convert(uuidStr);
            assertThat(result.toString()).isEqualTo(uuidStr);
        }

        @Test
        @DisplayName("parses uppercase UUID string")
        void parsesUppercaseUUIDString() {
            UUIDConverter converter = new UUIDConverter(defaultConfig);
            String uuidStr = "550E8400-E29B-41D4-A716-446655440000";
            UUID result = converter.convert(uuidStr);
            assertThat(result.toString()).isEqualToIgnoringCase(uuidStr);
        }

        @Test
        @DisplayName("parses UUID string without hyphens")
        void parsesUUIDWithoutHyphens() {
            UUIDConverter converter = new UUIDConverter(defaultConfig);
            String uuidStr = "550e8400e29b41d4a716446655440000";
            UUID result = converter.convert(uuidStr);
            assertThat(result.toString()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        }

        @Test
        @DisplayName("converts byte array")
        void convertsByteArray() {
            UUIDConverter converter = new UUIDConverter(defaultConfig);
            UUID original = UUID.randomUUID();
            byte[] bytes = UUIDConverter.toBytes(original);
            UUID result = converter.convert(bytes);
            assertThat(result).isEqualTo(original);
        }

        @Test
        @DisplayName("converts ByteBuffer")
        void convertsByteBuffer() {
            UUIDConverter converter = new UUIDConverter(defaultConfig);
            UUID original = UUID.randomUUID();
            ByteBuffer buffer = ByteBuffer.wrap(UUIDConverter.toBytes(original));
            UUID result = converter.convert(buffer);
            assertThat(result).isEqualTo(original);
        }

        @Test
        @DisplayName("throws on invalid UUID string")
        void throwsOnInvalidString() {
            UUIDConverter converter = new UUIDConverter(defaultConfig);
            assertThatThrownBy(() -> converter.convert("not-a-uuid"))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("Invalid UUID format");
        }

        @Test
        @DisplayName("throws on wrong byte array length")
        void throwsOnWrongByteLength() {
            UUIDConverter converter = new UUIDConverter(defaultConfig);
            assertThatThrownBy(() -> converter.convert(new byte[10]))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("16 bytes");
        }

        @Test
        @DisplayName("toBytes and fromBytes round-trip")
        void toBytesRoundTrip() {
            UUID original = UUID.randomUUID();
            byte[] bytes = UUIDConverter.toBytes(original);
            assertThat(bytes).hasSize(16);
            UUID restored = UUIDConverter.fromBytes(bytes);
            assertThat(restored).isEqualTo(original);
        }
    }

    // ==================== Date Converter Tests ====================

    @Nested
    @DisplayName("DateConverter")
    class DateConverterTests {

        @Test
        @DisplayName("converts LocalDate to days since epoch")
        void convertsLocalDate() {
            DateConverter converter = new DateConverter(defaultConfig);
            LocalDate date = LocalDate.of(2024, 1, 15);
            Integer days = converter.convert(date);
            assertThat(DateConverter.daysToLocalDate(days)).isEqualTo(date);
        }

        @Test
        @DisplayName("epoch is day 0")
        void epochIsDayZero() {
            DateConverter converter = new DateConverter(defaultConfig);
            Integer days = converter.convert(LocalDate.of(1970, 1, 1));
            assertThat(days).isEqualTo(0);
        }

        @Test
        @DisplayName("pre-epoch dates are negative")
        void preEpochDatesAreNegative() {
            DateConverter converter = new DateConverter(defaultConfig);
            Integer days = converter.convert(LocalDate.of(1969, 12, 31));
            assertThat(days).isEqualTo(-1);
        }

        @Test
        @DisplayName("converts Integer days directly")
        void convertsIntegerDays() {
            DateConverter converter = new DateConverter(defaultConfig);
            assertThat(converter.convert(0)).isEqualTo(0);
            assertThat(converter.convert(100)).isEqualTo(100);
        }

        @Test
        @DisplayName("converts LocalDateTime (extracts date)")
        void convertsLocalDateTime() {
            DateConverter converter = new DateConverter(defaultConfig);
            LocalDateTime ldt = LocalDateTime.of(2024, 1, 15, 12, 30);
            Integer days = converter.convert(ldt);
            assertThat(DateConverter.daysToLocalDate(days)).isEqualTo(LocalDate.of(2024, 1, 15));
        }

        @Test
        @DisplayName("converts Instant")
        void convertsInstant() {
            DateConverter converter = new DateConverter(defaultConfig);
            Instant instant = Instant.parse("2024-01-15T12:00:00Z");
            Integer days = converter.convert(instant);
            assertThat(days).isGreaterThan(0);
        }

        @Test
        @DisplayName("parses ISO date string")
        void parsesISODateString() {
            DateConverter converter = new DateConverter(defaultConfig);
            Integer days = converter.convert("2024-01-15");
            assertThat(DateConverter.daysToLocalDate(days)).isEqualTo(LocalDate.of(2024, 1, 15));
        }

        @Test
        @DisplayName("parses ISO datetime string")
        void parsesISODateTimeString() {
            DateConverter converter = new DateConverter(defaultConfig);
            Integer days = converter.convert("2024-01-15T12:30:00");
            assertThat(DateConverter.daysToLocalDate(days)).isEqualTo(LocalDate.of(2024, 1, 15));
        }

        @Test
        @DisplayName("throws on unparseable string")
        void throwsOnUnparseableString() {
            DateConverter converter = new DateConverter(defaultConfig);
            assertThatThrownBy(() -> converter.convert("not a date"))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("Cannot parse date");
        }
    }

    // ==================== Time Converter Tests ====================

    @Nested
    @DisplayName("TimeConverter")
    class TimeConverterTests {

        @Test
        @DisplayName("converts LocalTime to microseconds")
        void convertsLocalTime() {
            TimeConverter converter = new TimeConverter(defaultConfig);
            LocalTime time = LocalTime.of(12, 30, 45, 123_456_000);
            Long micros = converter.convert(time);
            LocalTime restored = TimeConverter.microsToLocalTime(micros);
            assertThat(restored).isEqualTo(time.truncatedTo(java.time.temporal.ChronoUnit.MICROS));
        }

        @Test
        @DisplayName("midnight is 0 microseconds")
        void midnightIsZero() {
            TimeConverter converter = new TimeConverter(defaultConfig);
            Long micros = converter.convert(LocalTime.MIDNIGHT);
            assertThat(micros).isEqualTo(0L);
        }

        @Test
        @DisplayName("end of day")
        void endOfDay() {
            TimeConverter converter = new TimeConverter(defaultConfig);
            LocalTime endOfDay = LocalTime.of(23, 59, 59, 999_999_000);
            Long micros = converter.convert(endOfDay);
            assertThat(micros).isLessThan(24L * 60 * 60 * 1_000_000);
        }

        @Test
        @DisplayName("parses ISO time string")
        void parsesISOTimeString() {
            TimeConverter converter = new TimeConverter(defaultConfig);
            Long micros = converter.convert("12:30:45");
            LocalTime restored = TimeConverter.microsToLocalTime(micros);
            assertThat(restored).isEqualTo(LocalTime.of(12, 30, 45));
        }

        @Test
        @DisplayName("parses short time format")
        void parsesShortTimeFormat() {
            TimeConverter converter = new TimeConverter(defaultConfig);
            Long micros = converter.convert("9:30");
            LocalTime restored = TimeConverter.microsToLocalTime(micros);
            assertThat(restored).isEqualTo(LocalTime.of(9, 30, 0));
        }
    }

    // ==================== Timestamp Converter Tests ====================

    @Nested
    @DisplayName("TimestampConverter")
    class TimestampConverterTests {

        @Test
        @DisplayName("converts Instant to microseconds")
        void convertsInstant() {
            TimestampConverter converter = new TimestampConverter(defaultConfig, true);
            Instant instant = Instant.parse("2024-01-15T12:30:45.123456Z");
            Long micros = converter.convert(instant);
            Instant restored = TimestampConverter.microsToInstant(micros);
            assertThat(restored).isEqualTo(instant.truncatedTo(java.time.temporal.ChronoUnit.MICROS));
        }

        @Test
        @DisplayName("epoch is 0 microseconds")
        void epochIsZero() {
            TimestampConverter converter = new TimestampConverter(defaultConfig, true);
            Long micros = converter.convert(Instant.EPOCH);
            assertThat(micros).isEqualTo(0L);
        }

        @Test
        @DisplayName("converts LocalDateTime with UTC")
        void convertsLocalDateTimeWithUTC() {
            TimestampConverter converter = new TimestampConverter(defaultConfig, true);
            LocalDateTime ldt = LocalDateTime.of(2024, 1, 15, 12, 30, 45);
            Long micros = converter.convert(ldt);
            assertThat(micros).isGreaterThan(0L);
        }

        @Test
        @DisplayName("converts ZonedDateTime")
        void convertsZonedDateTime() {
            TimestampConverter converter = new TimestampConverter(defaultConfig, true);
            ZonedDateTime zdt = ZonedDateTime.parse("2024-01-15T12:30:45+05:00");
            Long micros = converter.convert(zdt);
            Instant restored = TimestampConverter.microsToInstant(micros);
            assertThat(restored).isEqualTo(zdt.toInstant());
        }

        @Test
        @DisplayName("converts OffsetDateTime")
        void convertsOffsetDateTime() {
            TimestampConverter converter = new TimestampConverter(defaultConfig, true);
            OffsetDateTime odt = OffsetDateTime.parse("2024-01-15T12:30:45-08:00");
            Long micros = converter.convert(odt);
            Instant restored = TimestampConverter.microsToInstant(micros);
            assertThat(restored).isEqualTo(odt.toInstant());
        }

        @Test
        @DisplayName("parses ISO instant string")
        void parsesISOInstantString() {
            TimestampConverter converter = new TimestampConverter(defaultConfig, true);
            Long micros = converter.convert("2024-01-15T12:30:45Z");
            assertThat(micros).isGreaterThan(0L);
        }

        @Test
        @DisplayName("parses ISO local datetime string")
        void parsesISOLocalDateTimeString() {
            TimestampConverter converter = new TimestampConverter(defaultConfig, true);
            Long micros = converter.convert("2024-01-15T12:30:45");
            assertThat(micros).isGreaterThan(0L);
        }

        @Test
        @DisplayName("converts epoch milliseconds")
        void convertsEpochMillis() {
            ConversionConfig millisConfig = ConversionConfig.builder()
                    .inputTimestampPrecision(ConversionConfig.TimestampPrecision.MILLISECONDS)
                    .build();
            TimestampConverter converter = new TimestampConverter(millisConfig, true);
            Long micros = converter.convert(1705322445000L); // epoch millis
            assertThat(micros).isEqualTo(1705322445000L * 1000); // converted to micros
        }

        @Test
        @DisplayName("auto-detects timestamp precision")
        void autoDetectsPrecision() {
            TimestampConverter converter = new TimestampConverter(defaultConfig, true);
            // Small number = likely seconds
            Long fromSeconds = converter.convert(1705322445L);
            // Large number = likely already microseconds
            Long fromMicros = converter.convert(1705322445000000L);
            // Both should represent valid timestamps
            assertThat(fromSeconds).isGreaterThan(0L);
            assertThat(fromMicros).isGreaterThan(0L);
        }
    }

    // ==================== Binary Converter Tests ====================

    @Nested
    @DisplayName("BinaryConverter")
    class BinaryConverterTests {

        @Test
        @DisplayName("converts byte array")
        void convertsByteArray() {
            BinaryConverter converter = new BinaryConverter(defaultConfig);
            byte[] bytes = {1, 2, 3, 4, 5};
            ByteBuffer result = converter.convert(bytes);
            assertThat(result.remaining()).isEqualTo(5);
        }

        @Test
        @DisplayName("converts ByteBuffer")
        void convertsByteBuffer() {
            BinaryConverter converter = new BinaryConverter(defaultConfig);
            ByteBuffer input = ByteBuffer.wrap(new byte[]{1, 2, 3});
            ByteBuffer result = converter.convert(input);
            assertThat(result.remaining()).isEqualTo(3);
        }

        @Test
        @DisplayName("converts Base64 string")
        void convertsBase64String() {
            BinaryConverter converter = new BinaryConverter(defaultConfig);
            byte[] original = {1, 2, 3, 4, 5};
            String base64 = Base64.getEncoder().encodeToString(original);
            ByteBuffer result = converter.convert(base64);
            byte[] decoded = new byte[result.remaining()];
            result.get(decoded);
            assertThat(decoded).isEqualTo(original);
        }

        @Test
        @DisplayName("converts URL-safe Base64")
        void convertsUrlSafeBase64() {
            BinaryConverter converter = new BinaryConverter(defaultConfig);
            byte[] original = {-1, -2, -3};
            String base64 = Base64.getUrlEncoder().encodeToString(original);
            ByteBuffer result = converter.convert(base64);
            byte[] decoded = new byte[result.remaining()];
            result.get(decoded);
            assertThat(decoded).isEqualTo(original);
        }

        @Test
        @DisplayName("fixed length converter validates length")
        void fixedLengthValidation() {
            BinaryConverter converter = BinaryConverter.forFixed(defaultConfig, 4);
            assertThat(converter.isFixed()).isTrue();
            assertThat(converter.getFixedLength()).isEqualTo(4);
        }

        @Test
        @DisplayName("toBase64 and back round-trip")
        void toBase64RoundTrip() {
            byte[] original = {10, 20, 30, 40, 50};
            ByteBuffer buffer = ByteBuffer.wrap(original);
            String base64 = BinaryConverter.toBase64(buffer);
            byte[] decoded = Base64.getDecoder().decode(base64);
            assertThat(decoded).isEqualTo(original);
        }

        @Test
        @DisplayName("toByteArray extracts bytes")
        void toByteArrayExtractsBytes() {
            byte[] original = {1, 2, 3};
            ByteBuffer buffer = ByteBuffer.wrap(original);
            byte[] extracted = BinaryConverter.toByteArray(buffer);
            assertThat(extracted).isEqualTo(original);
        }
    }
}
