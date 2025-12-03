package io.github.pierce.converter;

import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Edge case and boundary tests for converters.
 */
class EdgeCaseTest {

    private ConversionConfig defaultConfig;

    @BeforeEach
    void setUp() {
        defaultConfig = ConversionConfig.defaults();
    }

    // ==================== Numeric Boundary Tests ====================

    @Nested
    @DisplayName("Numeric Boundaries")
    class NumericBoundaryTests {

        @Test
        @DisplayName("Integer at MAX_VALUE boundary")
        void integerAtMaxBoundary() {
            IntegerConverter converter = new IntegerConverter(defaultConfig);
            assertThat(converter.convert(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
            assertThat(converter.convert((long) Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
        }

        @Test
        @DisplayName("Integer at MIN_VALUE boundary")
        void integerAtMinBoundary() {
            IntegerConverter converter = new IntegerConverter(defaultConfig);
            assertThat(converter.convert(Integer.MIN_VALUE)).isEqualTo(Integer.MIN_VALUE);
            assertThat(converter.convert((long) Integer.MIN_VALUE)).isEqualTo(Integer.MIN_VALUE);
        }

        @Test
        @DisplayName("Integer overflow by one")
        void integerOverflowByOne() {
            IntegerConverter converter = new IntegerConverter(defaultConfig);
            assertThatThrownBy(() -> converter.convert((long) Integer.MAX_VALUE + 1))
                    .isInstanceOf(TypeConversionException.class);
        }

        @Test
        @DisplayName("Integer underflow by one")
        void integerUnderflowByOne() {
            IntegerConverter converter = new IntegerConverter(defaultConfig);
            assertThatThrownBy(() -> converter.convert((long) Integer.MIN_VALUE - 1))
                    .isInstanceOf(TypeConversionException.class);
        }

        @Test
        @DisplayName("Long at MAX_VALUE boundary")
        void longAtMaxBoundary() {
            LongConverter converter = new LongConverter(defaultConfig);
            assertThat(converter.convert(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
        }

        @Test
        @DisplayName("Long from BigInteger at boundary")
        void longFromBigIntegerAtBoundary() {
            LongConverter converter = new LongConverter(defaultConfig);
            assertThat(converter.convert(BigInteger.valueOf(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
            assertThat(converter.convert(BigInteger.valueOf(Long.MIN_VALUE))).isEqualTo(Long.MIN_VALUE);
        }

        @Test
        @DisplayName("Long overflow from BigInteger")
        void longOverflowFromBigInteger() {
            LongConverter converter = new LongConverter(defaultConfig);
            BigInteger tooLarge = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
            assertThatThrownBy(() -> converter.convert(tooLarge))
                    .isInstanceOf(TypeConversionException.class);
        }

        @Test
        @DisplayName("Float subnormal values")
        void floatSubnormalValues() {
            FloatConverter converter = new FloatConverter(defaultConfig);
            assertThat(converter.convert(Float.MIN_VALUE)).isEqualTo(Float.MIN_VALUE);
            assertThat(converter.convert(Float.MIN_NORMAL)).isEqualTo(Float.MIN_NORMAL);
        }

        @Test
        @DisplayName("Double subnormal values")
        void doubleSubnormalValues() {
            DoubleConverter converter = new DoubleConverter(defaultConfig);
            assertThat(converter.convert(Double.MIN_VALUE)).isEqualTo(Double.MIN_VALUE);
            assertThat(converter.convert(Double.MIN_NORMAL)).isEqualTo(Double.MIN_NORMAL);
        }

        @Test
        @DisplayName("Decimal at precision boundary")
        void decimalAtPrecisionBoundary() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 5, 2);
            // Max value for precision 5, scale 2 is 999.99
            BigDecimal maxValue = new BigDecimal("999.99");
            assertThat(converter.convert(maxValue)).isEqualByComparingTo(maxValue);

            // Just over the limit
            assertThatThrownBy(() -> converter.convert(new BigDecimal("1000.00")))
                    .isInstanceOf(TypeConversionException.class);
        }

        @Test
        @DisplayName("Decimal with negative values")
        void decimalWithNegativeValues() {
            DecimalConverter converter = new DecimalConverter(defaultConfig, 5, 2);
            BigDecimal minValue = new BigDecimal("-999.99");
            assertThat(converter.convert(minValue)).isEqualByComparingTo(minValue);
        }
    }

    // ==================== String Edge Cases ====================

    @Nested
    @DisplayName("String Edge Cases")
    class StringEdgeCaseTests {

        @Test
        @DisplayName("handles empty string")
        void handlesEmptyString() {
            StringConverter converter = new StringConverter(defaultConfig);
            assertThat(converter.convert("")).isEqualTo("");
        }

        @Test
        @DisplayName("handles whitespace-only string")
        void handlesWhitespaceOnlyString() {
            StringConverter converter = new StringConverter(defaultConfig);
            assertThat(converter.convert("   ")).isEqualTo("   ");
        }

        @Test
        @DisplayName("handles whitespace-only with trim")
        void handlesWhitespaceWithTrim() {
            ConversionConfig trimConfig = ConversionConfig.builder()
                    .trimStrings(true)
                    .coerceEmptyStringsToNull(true)
                    .build();
            StringConverter converter = new StringConverter(trimConfig);
            assertThat(converter.convert("   ")).isNull();
        }

        @Test
        @DisplayName("handles unicode strings")
        void handlesUnicodeStrings() {
            StringConverter converter = new StringConverter(defaultConfig);
            assertThat(converter.convert("Hello 世界 🌍")).isEqualTo("Hello 世界 🌍");
        }

        @Test
        @DisplayName("handles Avro Utf8 type")
        void handlesAvroUtf8() {
            StringConverter converter = new StringConverter(defaultConfig);
            Utf8 utf8 = new Utf8("Avro Utf8 String");
            assertThat(converter.convert(utf8)).isEqualTo("Avro Utf8 String");
        }

        @Test
        @DisplayName("handles StringBuilder")
        void handlesStringBuilder() {
            StringConverter converter = new StringConverter(defaultConfig);
            StringBuilder sb = new StringBuilder("StringBuilder content");
            assertThat(converter.convert(sb)).isEqualTo("StringBuilder content");
        }

        @Test
        @DisplayName("handles very long strings")
        void handlesVeryLongStrings() {
            StringConverter converter = new StringConverter(defaultConfig);
            String longString = "x".repeat(100_000);
            assertThat(converter.convert(longString)).hasSize(100_000);
        }

        @Test
        @DisplayName("handles string with null characters")
        void handlesStringWithNullChars() {
            StringConverter converter = new StringConverter(defaultConfig);
            String withNulls = "hello\0world";
            assertThat(converter.convert(withNulls)).isEqualTo("hello\0world");
        }
    }

    // ==================== Temporal Edge Cases ====================

    @Nested
    @DisplayName("Temporal Edge Cases")
    class TemporalEdgeCaseTests {

        @Test
        @DisplayName("Date at Unix epoch")
        void dateAtEpoch() {
            DateConverter converter = new DateConverter(defaultConfig);
            assertThat(converter.convert(LocalDate.of(1970, 1, 1))).isEqualTo(0);
        }

        @Test
        @DisplayName("Date before Unix epoch")
        void dateBeforeEpoch() {
            DateConverter converter = new DateConverter(defaultConfig);
            Integer days = converter.convert(LocalDate.of(1969, 12, 31));
            assertThat(days).isEqualTo(-1);
        }

        @Test
        @DisplayName("Date far in future")
        void dateFarInFuture() {
            DateConverter converter = new DateConverter(defaultConfig);
            LocalDate futureDate = LocalDate.of(3000, 1, 1);
            Integer days = converter.convert(futureDate);
            assertThat(DateConverter.daysToLocalDate(days)).isEqualTo(futureDate);
        }

        @Test
        @DisplayName("Date far in past")
        void dateFarInPast() {
            DateConverter converter = new DateConverter(defaultConfig);
            LocalDate pastDate = LocalDate.of(1800, 1, 1);
            Integer days = converter.convert(pastDate);
            assertThat(DateConverter.daysToLocalDate(days)).isEqualTo(pastDate);
        }

        @Test
        @DisplayName("Time at midnight")
        void timeAtMidnight() {
            TimeConverter converter = new TimeConverter(defaultConfig);
            assertThat(converter.convert(LocalTime.MIDNIGHT)).isEqualTo(0L);
        }

        @Test
        @DisplayName("Time at end of day")
        void timeAtEndOfDay() {
            TimeConverter converter = new TimeConverter(defaultConfig);
            LocalTime almostMidnight = LocalTime.of(23, 59, 59, 999_999_000);
            Long micros = converter.convert(almostMidnight);
            assertThat(micros).isLessThan(24L * 60 * 60 * 1_000_000);
        }

        @Test
        @DisplayName("Time with nanoseconds truncated to microseconds")
        void timeNanosTruncated() {
            TimeConverter converter = new TimeConverter(defaultConfig);
            LocalTime withNanos = LocalTime.of(12, 30, 45, 123_456_789);
            Long micros = converter.convert(withNanos);
            LocalTime restored = TimeConverter.microsToLocalTime(micros);
            // Nanoseconds should be truncated
            assertThat(restored.getNano()).isEqualTo(123_456_000);
        }

        @Test
        @DisplayName("Timestamp at Unix epoch")
        void timestampAtEpoch() {
            TimestampConverter converter = new TimestampConverter(defaultConfig, true);
            assertThat(converter.convert(Instant.EPOCH)).isEqualTo(0L);
        }

        @Test
        @DisplayName("Timestamp before Unix epoch")
        void timestampBeforeEpoch() {
            TimestampConverter converter = new TimestampConverter(defaultConfig, true);
            Instant beforeEpoch = Instant.parse("1969-12-31T23:59:59Z");
            Long micros = converter.convert(beforeEpoch);
            assertThat(micros).isNegative();
        }

        @Test
        @DisplayName("Timestamp with different timezone handling")
        void timestampTimezoneHandling() {
            TimestampConverter withUtc = new TimestampConverter(defaultConfig, true);
            TimestampConverter withoutUtc = new TimestampConverter(defaultConfig, false);

            LocalDateTime ldt = LocalDateTime.of(2024, 6, 15, 12, 0, 0);
            Long microsWithUtc = withUtc.convert(ldt);
            Long microsWithoutUtc = withoutUtc.convert(ldt);

            // Both should return valid microseconds
            assertThat(microsWithUtc).isNotNull();
            assertThat(microsWithoutUtc).isNotNull();
        }
    }

    // ==================== UUID Edge Cases ====================

    @Nested
    @DisplayName("UUID Edge Cases")
    class UUIDEdgeCaseTests {

        @Test
        @DisplayName("handles nil UUID")
        void handlesNilUUID() {
            UUIDConverter converter = new UUIDConverter(defaultConfig);
            UUID nil = new UUID(0L, 0L);
            assertThat(converter.convert(nil)).isEqualTo(nil);
            assertThat(converter.convert("00000000-0000-0000-0000-000000000000")).isEqualTo(nil);
        }

        @Test
        @DisplayName("handles max UUID")
        void handlesMaxUUID() {
            UUIDConverter converter = new UUIDConverter(defaultConfig);
            UUID max = new UUID(-1L, -1L);
            assertThat(converter.convert(max)).isEqualTo(max);
        }

        @Test
        @DisplayName("rejects invalid UUID strings")
        void rejectsInvalidUUIDStrings() {
            UUIDConverter converter = new UUIDConverter(defaultConfig);
            assertThatThrownBy(() -> converter.convert("not-a-uuid"))
                    .isInstanceOf(TypeConversionException.class);
            assertThatThrownBy(() -> converter.convert("550e8400-e29b-41d4-a716"))
                    .isInstanceOf(TypeConversionException.class);
            assertThatThrownBy(() -> converter.convert("550e8400-e29b-41d4-a716-4466554400000")) // extra char
                    .isInstanceOf(TypeConversionException.class);
        }
    }

    // ==================== Binary Edge Cases ====================

    @Nested
    @DisplayName("Binary Edge Cases")
    class BinaryEdgeCaseTests {

        @Test
        @DisplayName("handles empty byte array")
        void handlesEmptyByteArray() {
            BinaryConverter converter = new BinaryConverter(defaultConfig);
            ByteBuffer result = converter.convert(new byte[0]);
            assertThat(result.remaining()).isEqualTo(0);
        }

        @Test
        @DisplayName("handles empty ByteBuffer")
        void handlesEmptyByteBuffer() {
            BinaryConverter converter = new BinaryConverter(defaultConfig);
            ByteBuffer result = converter.convert(ByteBuffer.allocate(0));
            assertThat(result.remaining()).isEqualTo(0);
        }

        @Test
        @DisplayName("handles large binary data")
        void handlesLargeBinaryData() {
            BinaryConverter converter = new BinaryConverter(defaultConfig);
            byte[] large = new byte[1_000_000];
            new Random().nextBytes(large);
            ByteBuffer result = converter.convert(large);
            assertThat(result.remaining()).isEqualTo(1_000_000);
        }

        @Test
        @DisplayName("fixed length converter validates length")
        void fixedLengthValidatesLength() {
            BinaryConverter converter = BinaryConverter.forFixed(defaultConfig, 8);
            assertThat(converter.isFixed()).isTrue();
            assertThat(converter.getFixedLength()).isEqualTo(8);

            // Converting exact length works
            byte[] exactLength = new byte[8];
            ByteBuffer result = converter.convert(exactLength);
            assertThat(result.remaining()).isEqualTo(8);
        }
    }

    // ==================== Collection Edge Cases ====================

    @Nested
    @DisplayName("Collection Edge Cases")
    class CollectionEdgeCaseTests {

        @Test
        @DisplayName("handles empty list")
        void handlesEmptyList() {
            ListConverter converter = new ListConverter(defaultConfig, new IntegerConverter(defaultConfig));
            List<Object> result = converter.convert(List.of());
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("handles single element list")
        void handlesSingleElementList() {
            ListConverter converter = new ListConverter(defaultConfig, new IntegerConverter(defaultConfig));
            List<Object> result = converter.convert(List.of(42));
            assertThat(result).containsExactly(42);
        }

        @Test
        @DisplayName("handles large list")
        void handlesLargeList() {
            ListConverter converter = new ListConverter(defaultConfig, new IntegerConverter(defaultConfig));
            List<Integer> large = new ArrayList<>();
            for (int i = 0; i < 100_000; i++) {
                large.add(i);
            }
            List<Object> result = converter.convert(large);
            assertThat(result).hasSize(100_000);
        }

        @Test
        @DisplayName("handles empty map")
        void handlesEmptyMap() {
            MapConverter converter = new MapConverter(
                    defaultConfig,
                    new StringConverter(defaultConfig),
                    new IntegerConverter(defaultConfig)
            );
            Map<Object, Object> result = converter.convert(Map.of());
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("handles map with complex keys")
        void handlesMapWithComplexKeys() {
            // Keys coerced to String
            MapConverter converter = new MapConverter(
                    defaultConfig,
                    new StringConverter(defaultConfig),
                    new IntegerConverter(defaultConfig)
            );
            Map<Object, Object> input = new HashMap<>();
            input.put(123, 1);
            input.put(456, 2);
            Map<Object, Object> result = converter.convert(input);
            assertThat(result).containsEntry("123", 1).containsEntry("456", 2);
        }
    }

    // ==================== Null Handling Edge Cases ====================

    @Nested
    @DisplayName("Null Handling Edge Cases")
    class NullHandlingEdgeCaseTests {

        @ParameterizedTest
        @ValueSource(classes = {
                BooleanConverter.class, IntegerConverter.class, LongConverter.class,
                FloatConverter.class, DoubleConverter.class, StringConverter.class,
                UUIDConverter.class, DateConverter.class, TimeConverter.class
        })
        @DisplayName("all converters handle null input")
        void allConvertersHandleNull(Class<?> converterClass) throws Exception {
            var constructor = converterClass.getConstructor(ConversionConfig.class);
            var converter = (TypeConverter<?, ?>) constructor.newInstance(defaultConfig);
            assertThat(converter.convert(null)).isNull();
        }

        @Test
        @DisplayName("list with all null elements")
        void listWithAllNullElements() {
            ListConverter converter = new ListConverter(defaultConfig, new StringConverter(defaultConfig));
            List<String> input = new ArrayList<>();
            input.add(null);
            input.add(null);
            input.add(null);
            List<Object> result = converter.convert(input);
            assertThat(result).containsExactly(null, null, null);
        }

        @Test
        @DisplayName("map with null values")
        void mapWithNullValues() {
            MapConverter converter = new MapConverter(
                    defaultConfig,
                    new StringConverter(defaultConfig),
                    new StringConverter(defaultConfig)
            );
            Map<String, String> input = new HashMap<>();
            input.put("key1", null);
            input.put("key2", "value2");
            Map<Object, Object> result = converter.convert(input);
            assertThat(result).containsEntry("key1", null).containsEntry("key2", "value2");
        }
    }

    // ==================== Avro Interoperability ====================

    @Nested
    @DisplayName("Avro Interoperability")
    class AvroInteroperabilityTests {

        @Test
        @DisplayName("handles Avro Utf8")
        void handlesAvroUtf8() {
            StringConverter converter = new StringConverter(defaultConfig);
            Utf8 utf8 = new Utf8("test");
            assertThat(converter.convert(utf8)).isEqualTo("test");
        }

        @Test
        @DisplayName("handles ByteBuffer from Avro")
        void handlesByteBufferFromAvro() {
            BinaryConverter converter = new BinaryConverter(defaultConfig);
            ByteBuffer avroBuffer = ByteBuffer.wrap(new byte[]{1, 2, 3});
            ByteBuffer result = converter.convert(avroBuffer);
            assertThat(result.remaining()).isEqualTo(3);
        }

        @Test
        @DisplayName("handles CharSequence implementations")
        void handlesCharSequenceImplementations() {
            StringConverter converter = new StringConverter(defaultConfig);

            // StringBuilder
            assertThat(converter.convert(new StringBuilder("sb"))).isEqualTo("sb");

            // StringBuffer
            assertThat(converter.convert(new StringBuffer("sbuf"))).isEqualTo("sbuf");

            // Utf8
            assertThat(converter.convert(new Utf8("utf8"))).isEqualTo("utf8");
        }
    }

    // ==================== Thread Safety ====================

    @Nested
    @DisplayName("Thread Safety")
    class ThreadSafetyTests {

        @Test
        @DisplayName("converters are thread-safe")
        void convertersAreThreadSafe() throws InterruptedException {
            IntegerConverter converter = new IntegerConverter(defaultConfig);
            int threadCount = 10;
            int iterationsPerThread = 1000;

            Thread[] threads = new Thread[threadCount];
            boolean[] errors = new boolean[]{false};

            for (int i = 0; i < threadCount; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        for (int j = 0; j < iterationsPerThread; j++) {
                            Integer result = converter.convert(j);
                            if (!result.equals(j)) {
                                errors[0] = true;
                            }
                        }
                    } catch (Exception e) {
                        errors[0] = true;
                    }
                });
            }

            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }

            assertThat(errors[0]).isFalse();
        }

        @Test
        @DisplayName("TypeConverterRegistry is thread-safe")
        void registryIsThreadSafe() throws InterruptedException {
            TypeConverterRegistry registry = new TypeConverterRegistry(defaultConfig);
            int threadCount = 10;
            Set<IntegerConverter> converters = Collections.synchronizedSet(new HashSet<>());

            Thread[] threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++) {
                threads[i] = new Thread(() -> {
                    converters.add(registry.getInteger());
                });
            }

            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }

            // All threads should get the same cached instance
            assertThat(converters).hasSize(1);
        }
    }
}