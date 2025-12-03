package io.github.pierce.converter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive unit tests for primitive type converters.
 */
class PrimitiveConverterTest {

    private ConversionConfig defaultConfig;
    private ConversionConfig strictConfig;
    private ConversionConfig lenientConfig;

    @BeforeEach
    void setUp() {
        defaultConfig = ConversionConfig.defaults();
        strictConfig = ConversionConfig.strict();
        lenientConfig = ConversionConfig.lenient();
    }

    // ==================== Boolean Converter Tests ====================

    @Nested
    @DisplayName("BooleanConverter")
    class BooleanConverterTests {

        @Test
        @DisplayName("converts Boolean values directly")
        void convertsBooleanDirectly() {
            BooleanConverter converter = new BooleanConverter(defaultConfig);
            assertThat(converter.convert(true)).isTrue();
            assertThat(converter.convert(false)).isFalse();
        }

        @Test
        @DisplayName("returns null for null input")
        void returnsNullForNull() {
            BooleanConverter converter = new BooleanConverter(defaultConfig);
            assertThat(converter.convert(null)).isNull();
        }

        @ParameterizedTest
        @ValueSource(strings = {"true", "TRUE", "True", "t", "T", "yes", "YES", "y", "Y", "1", "on", "ON"})
        @DisplayName("converts truthy strings to true")
        void convertsTruthyStrings(String value) {
            BooleanConverter converter = new BooleanConverter(defaultConfig);
            assertThat(converter.convert(value)).isTrue();
        }

        @ParameterizedTest
        @ValueSource(strings = {"false", "FALSE", "False", "f", "F", "no", "NO", "n", "N", "0", "off", "OFF"})
        @DisplayName("converts falsy strings to false")
        void convertsFalsyStrings(String value) {
            BooleanConverter converter = new BooleanConverter(defaultConfig);
            assertThat(converter.convert(value)).isFalse();
        }

        @ParameterizedTest
        @ValueSource(ints = {1, 2, -1, 100, Integer.MAX_VALUE})
        @DisplayName("converts non-zero numbers to true")
        void convertsNonZeroToTrue(int value) {
            BooleanConverter converter = new BooleanConverter(defaultConfig);
            assertThat(converter.convert(value)).isTrue();
        }

        @Test
        @DisplayName("converts zero to false")
        void convertsZeroToFalse() {
            BooleanConverter converter = new BooleanConverter(defaultConfig);
            assertThat(converter.convert(0)).isFalse();
            assertThat(converter.convert(0L)).isFalse();
            assertThat(converter.convert(0.0)).isFalse();
            assertThat(converter.convert(0.0f)).isFalse();
        }

        @Test
        @DisplayName("strict mode throws on invalid string")
        void strictModeThrowsOnInvalidString() {
            BooleanConverter converter = new BooleanConverter(strictConfig);
            assertThatThrownBy(() -> converter.convert("invalid"))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("Invalid boolean string");
        }
    }

    // ==================== Integer Converter Tests ====================

    @Nested
    @DisplayName("IntegerConverter")
    class IntegerConverterTests {

        @Test
        @DisplayName("converts Integer values directly")
        void convertsIntegerDirectly() {
            IntegerConverter converter = new IntegerConverter(defaultConfig);
            assertThat(converter.convert(42)).isEqualTo(42);
            assertThat(converter.convert(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
            assertThat(converter.convert(Integer.MIN_VALUE)).isEqualTo(Integer.MIN_VALUE);
        }

        @Test
        @DisplayName("returns null for null input")
        void returnsNullForNull() {
            IntegerConverter converter = new IntegerConverter(defaultConfig);
            assertThat(converter.convert(null)).isNull();
        }

        @Test
        @DisplayName("converts Long within range")
        void convertsLongWithinRange() {
            IntegerConverter converter = new IntegerConverter(defaultConfig);
            assertThat(converter.convert(42L)).isEqualTo(42);
            assertThat(converter.convert((long) Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
        }

        @Test
        @DisplayName("throws on Long overflow")
        void throwsOnLongOverflow() {
            IntegerConverter converter = new IntegerConverter(defaultConfig);
            assertThatThrownBy(() -> converter.convert(Long.MAX_VALUE))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("outside integer range");
        }

        @Test
        @DisplayName("converts valid string")
        void convertsValidString() {
            IntegerConverter converter = new IntegerConverter(defaultConfig);
            assertThat(converter.convert("42")).isEqualTo(42);
            assertThat(converter.convert("-123")).isEqualTo(-123);
        }

        @Test
        @DisplayName("throws on invalid string")
        void throwsOnInvalidString() {
            IntegerConverter converter = new IntegerConverter(defaultConfig);
            assertThatThrownBy(() -> converter.convert("not a number"))
                    .isInstanceOf(TypeConversionException.class);
        }

        @Test
        @DisplayName("converts boolean to 0 or 1")
        void convertsBooleanToInt() {
            IntegerConverter converter = new IntegerConverter(defaultConfig);
            assertThat(converter.convert(true)).isEqualTo(1);
            assertThat(converter.convert(false)).isEqualTo(0);
        }

        @Test
        @DisplayName("handles decimal strings with precision loss check")
        void handlesDecimalStrings() {
            ConversionConfig allowPrecisionLoss = ConversionConfig.builder()
                    .allowPrecisionLoss(true)
                    .build();
            IntegerConverter converter = new IntegerConverter(allowPrecisionLoss);
            assertThat(converter.convert("42.9")).isEqualTo(42);
        }

        @Test
        @DisplayName("throws on precision loss when not allowed")
        void throwsOnPrecisionLoss() {
            ConversionConfig noPrecisionLoss = ConversionConfig.builder()
                    .allowPrecisionLoss(false)
                    .build();
            IntegerConverter converter = new IntegerConverter(noPrecisionLoss);
            assertThatThrownBy(() -> converter.convert(42.5))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("fractional part");
        }
    }

    // ==================== Long Converter Tests ====================

    @Nested
    @DisplayName("LongConverter")
    class LongConverterTests {

        @Test
        @DisplayName("converts Long values directly")
        void convertsLongDirectly() {
            LongConverter converter = new LongConverter(defaultConfig);
            assertThat(converter.convert(42L)).isEqualTo(42L);
            assertThat(converter.convert(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
            assertThat(converter.convert(Long.MIN_VALUE)).isEqualTo(Long.MIN_VALUE);
        }

        @Test
        @DisplayName("promotes Integer to Long")
        void promotesIntegerToLong() {
            LongConverter converter = new LongConverter(defaultConfig);
            assertThat(converter.convert(42)).isEqualTo(42L);
            assertThat(converter.convert(Integer.MAX_VALUE)).isEqualTo((long) Integer.MAX_VALUE);
        }

        @Test
        @DisplayName("converts BigInteger within range")
        void convertsBigIntegerWithinRange() {
            LongConverter converter = new LongConverter(defaultConfig);
            assertThat(converter.convert(BigInteger.valueOf(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
        }

        @Test
        @DisplayName("throws on BigInteger overflow")
        void throwsOnBigIntegerOverflow() {
            LongConverter converter = new LongConverter(defaultConfig);
            BigInteger huge = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TEN);
            assertThatThrownBy(() -> converter.convert(huge))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("outside long range");
        }

        @Test
        @DisplayName("converts BigDecimal")
        void convertsBigDecimal() {
            LongConverter converter = new LongConverter(defaultConfig);
            assertThat(converter.convert(new BigDecimal("123"))).isEqualTo(123L);
        }
    }

    // ==================== Float Converter Tests ====================

    @Nested
    @DisplayName("FloatConverter")
    class FloatConverterTests {

        @Test
        @DisplayName("converts Float values directly")
        void convertsFloatDirectly() {
            FloatConverter converter = new FloatConverter(defaultConfig);
            assertThat(converter.convert(3.14f)).isEqualTo(3.14f);
        }

        @Test
        @DisplayName("promotes Integer to Float")
        void promotesIntegerToFloat() {
            FloatConverter converter = new FloatConverter(defaultConfig);
            assertThat(converter.convert(42)).isEqualTo(42.0f);
        }

        @Test
        @DisplayName("throws on NaN")
        void throwsOnNaN() {
            FloatConverter converter = new FloatConverter(defaultConfig);
            assertThatThrownBy(() -> converter.convert(Float.NaN))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("NaN");
        }

        @Test
        @DisplayName("throws on Infinity")
        void throwsOnInfinity() {
            FloatConverter converter = new FloatConverter(defaultConfig);
            assertThatThrownBy(() -> converter.convert(Float.POSITIVE_INFINITY))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("Infinite");
        }

        @Test
        @DisplayName("converts valid string")
        void convertsValidString() {
            FloatConverter converter = new FloatConverter(defaultConfig);
            assertThat(converter.convert("3.14")).isEqualTo(3.14f);
            assertThat(converter.convert("-1.5e10")).isEqualTo(-1.5e10f);
        }
    }

    // ==================== Double Converter Tests ====================

    @Nested
    @DisplayName("DoubleConverter")
    class DoubleConverterTests {

        @Test
        @DisplayName("converts Double values directly")
        void convertsDoubleDirectly() {
            DoubleConverter converter = new DoubleConverter(defaultConfig);
            assertThat(converter.convert(3.14159)).isEqualTo(3.14159);
        }

        @Test
        @DisplayName("promotes Float to Double")
        void promotesFloatToDouble() {
            DoubleConverter converter = new DoubleConverter(defaultConfig);
            assertThat(converter.convert(3.14f)).isEqualTo((double) 3.14f);
        }

        @Test
        @DisplayName("promotes Long to Double")
        void promotesLongToDouble() {
            DoubleConverter converter = new DoubleConverter(defaultConfig);
            assertThat(converter.convert(Long.MAX_VALUE)).isEqualTo((double) Long.MAX_VALUE);
        }

        @Test
        @DisplayName("throws on NaN")
        void throwsOnNaN() {
            DoubleConverter converter = new DoubleConverter(defaultConfig);
            assertThatThrownBy(() -> converter.convert(Double.NaN))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("NaN");
        }

        @Test
        @DisplayName("converts BigDecimal")
        void convertsBigDecimal() {
            DoubleConverter converter = new DoubleConverter(defaultConfig);
            assertThat(converter.convert(new BigDecimal("123.456"))).isEqualTo(123.456);
        }
    }

    // ==================== String Converter Tests ====================

    @Nested
    @DisplayName("StringConverter")
    class StringConverterTests {

        @Test
        @DisplayName("converts String values directly")
        void convertsStringDirectly() {
            StringConverter converter = new StringConverter(defaultConfig);
            assertThat(converter.convert("hello")).isEqualTo("hello");
            assertThat(converter.convert("")).isEqualTo("");
        }

        @Test
        @DisplayName("returns null for null input")
        void returnsNullForNull() {
            StringConverter converter = new StringConverter(defaultConfig);
            assertThat(converter.convert(null)).isNull();
        }

        @Test
        @DisplayName("converts numbers via toString")
        void convertsNumbers() {
            StringConverter converter = new StringConverter(defaultConfig);
            assertThat(converter.convert(42)).isEqualTo("42");
            assertThat(converter.convert(3.14)).isEqualTo("3.14");
        }

        @Test
        @DisplayName("trims strings when configured")
        void trimsStrings() {
            ConversionConfig trimConfig = ConversionConfig.builder()
                    .trimStrings(true)
                    .build();
            StringConverter converter = new StringConverter(trimConfig);
            assertThat(converter.convert("  hello  ")).isEqualTo("hello");
        }

        @Test
        @DisplayName("coerces empty string to null when configured")
        void coercesEmptyToNull() {
            ConversionConfig coerceConfig = ConversionConfig.builder()
                    .coerceEmptyStringsToNull(true)
                    .build();
            StringConverter converter = new StringConverter(coerceConfig);
            assertThat(converter.convert("")).isNull();
        }

        @Test
        @DisplayName("enforces max length")
        void enforcesMaxLength() {
            StringConverter converter = StringConverter.withMaxLength(defaultConfig, 5);
            assertThatThrownBy(() -> converter.convert("toolongstring"))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("exceeds maximum");
        }

        @Test
        @DisplayName("truncates when configured")
        void truncatesWhenConfigured() {
            ConversionConfig truncateConfig = ConversionConfig.builder()
                    .stringTruncationMode(ConversionConfig.StringTruncationMode.TRUNCATE)
                    .build();
            StringConverter converter = new StringConverter(truncateConfig, 5);
            assertThat(converter.convert("toolongstring")).isEqualTo("toolo");
        }
    }

    // ==================== Cross-Type Promotion Tests ====================

    @Nested
    @DisplayName("Type Promotion")
    class TypePromotionTests {

        @ParameterizedTest
        @MethodSource("intToLongPromotionCases")
        @DisplayName("Integer to Long promotion preserves value")
        void intToLongPromotion(Integer input, Long expected) {
            LongConverter converter = new LongConverter(defaultConfig);
            assertThat(converter.convert(input)).isEqualTo(expected);
        }

        static Stream<Arguments> intToLongPromotionCases() {
            return Stream.of(
                    Arguments.of(0, 0L),
                    Arguments.of(1, 1L),
                    Arguments.of(-1, -1L),
                    Arguments.of(Integer.MAX_VALUE, (long) Integer.MAX_VALUE),
                    Arguments.of(Integer.MIN_VALUE, (long) Integer.MIN_VALUE)
            );
        }

        @ParameterizedTest
        @MethodSource("floatToDoublePromotionCases")
        @DisplayName("Float to Double promotion preserves value")
        void floatToDoublePromotion(Float input, Double expected) {
            DoubleConverter converter = new DoubleConverter(defaultConfig);
            assertThat(converter.convert(input)).isEqualTo(expected);
        }

        static Stream<Arguments> floatToDoublePromotionCases() {
            return Stream.of(
                    Arguments.of(0.0f, 0.0),
                    Arguments.of(1.0f, 1.0),
                    Arguments.of(-1.0f, -1.0),
                    Arguments.of(Float.MAX_VALUE, (double) Float.MAX_VALUE)
            );
        }
    }
}
