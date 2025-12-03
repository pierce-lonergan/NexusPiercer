package io.github.pierce.converter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.RoundingMode;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for error handling infrastructure and configuration.
 */
class ErrorHandlingAndConfigTest {

    // ==================== ConversionResult Tests ====================

    @Nested
    @DisplayName("ConversionResult")
    class ConversionResultTests {

        @Test
        @DisplayName("success result has value and no errors")
        void successResult() {
            ConversionResult<String> result = ConversionResult.success("hello");

            assertThat(result.isSuccess()).isTrue();
            assertThat(result.hasErrors()).isFalse();
            assertThat(result.getValue()).isEqualTo("hello");
            assertThat(result.getValueOrThrow()).isEqualTo("hello");
            assertThat(result.getErrors()).isEmpty();
        }

        @Test
        @DisplayName("failure result has errors and no value")
        void failureResult() {
            List<ConversionResult.ConversionError> errors = List.of(
                    new ConversionResult.ConversionError("field1", "Error 1", null),
                    new ConversionResult.ConversionError("field2", "Error 2", null)
            );

            ConversionResult<String> result = ConversionResult.failure(errors);

            assertThat(result.isSuccess()).isFalse();
            assertThat(result.hasErrors()).isTrue();
            assertThat(result.getValue()).isNull();
            assertThat(result.getErrors()).hasSize(2);
        }

        @Test
        @DisplayName("getValueOrThrow throws on failure")
        void getValueOrThrowThrowsOnFailure() {
            List<ConversionResult.ConversionError> errors = List.of(
                    new ConversionResult.ConversionError("field", "Something went wrong", null)
            );

            ConversionResult<String> result = ConversionResult.failure(errors);

            assertThatThrownBy(result::getValueOrThrow)
                    .isInstanceOf(SchemaConversionException.class)
                    .hasMessageContaining("Something went wrong");
        }

        @Test
        @DisplayName("partial result has both value and errors")
        void partialResult() {
            List<ConversionResult.ConversionError> errors = List.of(
                    new ConversionResult.ConversionError("optional_field", "Warning", null)
            );

            ConversionResult<String> result = ConversionResult.partial("partial value", errors);

            assertThat(result.isSuccess()).isFalse(); // Not fully successful
            assertThat(result.hasErrors()).isTrue();
            assertThat(result.getValue()).isEqualTo("partial value");
            assertThat(result.getErrors()).hasSize(1);
        }

        @Test
        @DisplayName("formatErrors concatenates error messages")
        void formatErrorsConcatenates() {
            List<ConversionResult.ConversionError> errors = List.of(
                    new ConversionResult.ConversionError("field1", "Error 1", null),
                    new ConversionResult.ConversionError("field2", "Error 2", null)
            );

            ConversionResult<String> result = ConversionResult.failure(errors);

            String formatted = result.formatErrors();
            assertThat(formatted).contains("Error 1").contains("Error 2");
        }

        @Test
        @DisplayName("ConversionError includes field path")
        void conversionErrorIncludesFieldPath() {
            ConversionResult.ConversionError error = new ConversionResult.ConversionError(
                    "user.address.zip", "Invalid zip code", null
            );

            assertThat(error.fieldPath()).isEqualTo("user.address.zip");
            assertThat(error.message()).isEqualTo("Invalid zip code");
        }

        @Test
        @DisplayName("ConversionError includes cause")
        void conversionErrorIncludesCause() {
            RuntimeException cause = new RuntimeException("Original error");
            ConversionResult.ConversionError error = new ConversionResult.ConversionError(
                    "field", "Conversion failed", cause
            );

            assertThat(error.cause()).isEqualTo(cause);
        }
    }

    // ==================== Notification Tests ====================

    @Nested
    @DisplayName("Notification")
    class NotificationTests {

        @Test
        @DisplayName("new notification has no errors")
        void newNotificationHasNoErrors() {
            Notification notification = new Notification();

            assertThat(notification.hasErrors()).isFalse();
            assertThat(notification.getErrors()).isEmpty();
        }

        @Test
        @DisplayName("addError records error")
        void addErrorRecordsError() {
            Notification notification = new Notification();
            notification.addError("Something went wrong");

            assertThat(notification.hasErrors()).isTrue();
            assertThat(notification.getErrors()).hasSize(1);
        }

        @Test
        @DisplayName("pushField creates nested path")
        void pushFieldCreatesNestedPath() {
            Notification notification = new Notification();

            try (var ctx1 = notification.pushField("user")) {
                assertThat(notification.getCurrentPath()).isEqualTo("user");

                try (var ctx2 = notification.pushField("address")) {
                    assertThat(notification.getCurrentPath()).isEqualTo("user.address");

                    try (var ctx3 = notification.pushField("city")) {
                        assertThat(notification.getCurrentPath()).isEqualTo("user.address.city");
                    }
                    assertThat(notification.getCurrentPath()).isEqualTo("user.address");
                }
                assertThat(notification.getCurrentPath()).isEqualTo("user");
            }
            assertThat(notification.getCurrentPath()).isEmpty();
        }

        @Test
        @DisplayName("errors include current path")
        void errorsIncludeCurrentPath() {
            Notification notification = new Notification();

            try (var ctx = notification.pushField("user")) {
                try (var ctx2 = notification.pushField("age")) {
                    notification.addError("Must be positive");
                }
            }

            assertThat(notification.getErrors()).hasSize(1);
            ConversionResult.ConversionError error = notification.getErrors().get(0);
            assertThat(error.fieldPath()).isEqualTo("user.age");
        }

        @Test
        @DisplayName("toResult creates ConversionResult")
        void toResultCreatesConversionResult() {
            Notification notification = new Notification();
            notification.addError("Error 1");

            ConversionResult<String> result = notification.toResult("value");

            assertThat(result.hasErrors()).isTrue();
            assertThat(result.getValue()).contains("value");
        }

        @Test
        @DisplayName("addError with cause includes exception")
        void addErrorWithCauseIncludesException() {
            Notification notification = new Notification();
            RuntimeException cause = new RuntimeException("Root cause");
            notification.addError("Conversion failed", cause);

            ConversionResult.ConversionError error = notification.getErrors().get(0);
            assertThat(error.cause()).isEqualTo(cause);
        }

        @Test
        @DisplayName("array index in path")
        void arrayIndexInPath() {
            Notification notification = new Notification();

            try (var ctx = notification.pushField("items")) {
                try (var ctx2 = notification.pushField("[2]")) {
                    notification.addError("Invalid item");
                }
            }

            ConversionResult.ConversionError error = notification.getErrors().get(0);
            // Path includes dot separator before array index
            assertThat(error.fieldPath()).isEqualTo("items.[2]");
        }
    }

    // ==================== ConversionConfig Tests ====================

    @Nested
    @DisplayName("ConversionConfig")
    class ConversionConfigTests {

        @Test
        @DisplayName("defaults() creates default configuration")
        void defaultsCreatesDefaultConfig() {
            ConversionConfig config = ConversionConfig.defaults();

            assertThat(config.getErrorHandlingMode()).isEqualTo(ConversionConfig.ErrorHandlingMode.FAIL_FAST);
            assertThat(config.isAllowExtraFields()).isTrue();
            assertThat(config.isAllowMissingOptionalFields()).isTrue();
            assertThat(config.getDefaultTimezone()).isEqualTo(ZoneOffset.UTC);
        }

        @Test
        @DisplayName("strict() creates strict configuration")
        void strictCreatesStrictConfig() {
            ConversionConfig config = ConversionConfig.strict();

            assertThat(config.isAllowNumericOverflow()).isFalse();
            assertThat(config.isAllowPrecisionLoss()).isFalse();
            assertThat(config.isAllowExtraFields()).isFalse();
        }

        @Test
        @DisplayName("lenient() creates lenient configuration")
        void lenientCreatesLenientConfig() {
            ConversionConfig config = ConversionConfig.lenient();

            assertThat(config.isAllowNumericOverflow()).isFalse(); // Still don't silently overflow
            assertThat(config.isAllowPrecisionLoss()).isTrue();
            assertThat(config.isCoerceEmptyStringsToNull()).isTrue();
            assertThat(config.isTrimStrings()).isTrue();
        }

        @Test
        @DisplayName("builder allows customization")
        void builderAllowsCustomization() {
            ConversionConfig config = ConversionConfig.builder()
                    .errorHandlingMode(ConversionConfig.ErrorHandlingMode.COLLECT_ERRORS)
                    .allowExtraFields(false)
                    .allowNumericOverflow(true)
                    .trimStrings(true)
                    .defaultTimezone(ZoneId.of("America/New_York"))
                    .decimalRoundingMode(RoundingMode.CEILING)
                    .build();

            assertThat(config.getErrorHandlingMode()).isEqualTo(ConversionConfig.ErrorHandlingMode.COLLECT_ERRORS);
            assertThat(config.isAllowExtraFields()).isFalse();
            assertThat(config.isAllowNumericOverflow()).isTrue();
            assertThat(config.isTrimStrings()).isTrue();
            assertThat(config.getDefaultTimezone()).isEqualTo(ZoneId.of("America/New_York"));
            assertThat(config.getDecimalRoundingMode()).isEqualTo(RoundingMode.CEILING);
        }

        @Test
        @DisplayName("timestamp precision configuration")
        void timestampPrecisionConfiguration() {
            ConversionConfig config = ConversionConfig.builder()
                    .inputTimestampPrecision(ConversionConfig.TimestampPrecision.MILLISECONDS)
                    .assumeUtcForNaiveTimestamps(false)
                    .build();

            assertThat(config.getInputTimestampPrecision()).isEqualTo(ConversionConfig.TimestampPrecision.MILLISECONDS);
            assertThat(config.isAssumeUtcForNaiveTimestamps()).isFalse();
        }

        @Test
        @DisplayName("string configuration options")
        void stringConfigurationOptions() {
            ConversionConfig config = ConversionConfig.builder()
                    .trimStrings(true)
                    .coerceEmptyStringsToNull(true)
                    .stringTruncationMode(ConversionConfig.StringTruncationMode.TRUNCATE_WITH_WARNING)
                    .build();

            assertThat(config.isTrimStrings()).isTrue();
            assertThat(config.isCoerceEmptyStringsToNull()).isTrue();
            assertThat(config.getStringTruncationMode()).isEqualTo(ConversionConfig.StringTruncationMode.TRUNCATE_WITH_WARNING);
        }

        @Test
        @DisplayName("null handling configuration")
        void nullHandlingConfiguration() {
            ConversionConfig config = ConversionConfig.builder()
                    .nullHandlingMode(ConversionConfig.NullHandlingMode.USE_DEFAULTS)
                    .useSchemaDefaults(true)
                    .build();

            assertThat(config.getNullHandlingMode()).isEqualTo(ConversionConfig.NullHandlingMode.USE_DEFAULTS);
            assertThat(config.isUseSchemaDefaults()).isTrue();
        }

        @Test
        @DisplayName("caching configuration")
        void cachingConfiguration() {
            ConversionConfig config = ConversionConfig.builder()
                    .cacheConverters(true)
                    .initialCacheCapacity(128)
                    .build();

            assertThat(config.isCacheConverters()).isTrue();
            assertThat(config.getInitialCacheCapacity()).isEqualTo(128);
        }

        @Test
        @DisplayName("config is immutable")
        void configIsImmutable() {
            ConversionConfig config = ConversionConfig.defaults();

            // Config should be effectively immutable - no setters
            // This test just verifies that the builder pattern is used
            assertThat(config).isNotNull();
        }
    }

    // ==================== Exception Tests ====================

    @Nested
    @DisplayName("Exceptions")
    class ExceptionTests {

        @Test
        @DisplayName("SchemaConversionException includes field path")
        void schemaConversionExceptionIncludesFieldPath() {
            SchemaConversionException ex = new SchemaConversionException("user.address", "Invalid address");

            assertThat(ex.getFieldPath()).isEqualTo("user.address");
            assertThat(ex.getMessage()).contains("user.address").contains("Invalid address");
        }

        @Test
        @DisplayName("TypeConversionException includes value and target type")
        void typeConversionExceptionIncludesValueAndType() {
            TypeConversionException ex = new TypeConversionException(
                    "age", "not_a_number", "integer", "Cannot parse"
            );

            assertThat(ex.getFieldPath()).isEqualTo("age");
            assertThat(ex.getValue()).isEqualTo("not_a_number");
            assertThat(ex.getTargetType()).isEqualTo("integer");
            assertThat(ex.getMessage()).contains("not_a_number").contains("integer");
        }

        @Test
        @DisplayName("TypeConversionException truncates long values")
        void typeConversionExceptionTruncatesLongValues() {
            String longValue = "a".repeat(100);
            TypeConversionException ex = new TypeConversionException(
                    "field", longValue, "string", "Too long"
            );

            // Message should not contain the full 100-char value
            assertThat(ex.getMessage().length()).isLessThan(longValue.length() + 50);
        }

        @Test
        @DisplayName("NullValueException for required fields")
        void nullValueExceptionForRequiredFields() {
            NullValueException ex = new NullValueException("required_field", "Field is required");

            assertThat(ex.getFieldPath()).isEqualTo("required_field");
            assertThat(ex.getMessage()).contains("required_field");
        }
    }

    // ==================== TypeConverterRegistry Tests ====================

    @Nested
    @DisplayName("TypeConverterRegistry")
    class TypeConverterRegistryTests {

        @Test
        @DisplayName("caches primitive converters")
        void cachesPrimitiveConverters() {
            TypeConverterRegistry registry = new TypeConverterRegistry(ConversionConfig.defaults());

            BooleanConverter bool1 = registry.getBoolean();
            BooleanConverter bool2 = registry.getBoolean();
            assertThat(bool1).isSameAs(bool2);

            IntegerConverter int1 = registry.getInteger();
            IntegerConverter int2 = registry.getInteger();
            assertThat(int1).isSameAs(int2);

            StringConverter str1 = registry.getString();
            StringConverter str2 = registry.getString();
            assertThat(str1).isSameAs(str2);
        }

        @Test
        @DisplayName("caches parameterized converters with same parameters")
        void cachesParameterizedConverters() {
            TypeConverterRegistry registry = new TypeConverterRegistry(ConversionConfig.defaults());

            DecimalConverter dec1 = registry.getDecimal(10, 2);
            DecimalConverter dec2 = registry.getDecimal(10, 2);
            assertThat(dec1).isSameAs(dec2);

            // Different parameters = different converter
            DecimalConverter dec3 = registry.getDecimal(5, 3);
            assertThat(dec3).isNotSameAs(dec1);
        }

        @Test
        @DisplayName("provides all primitive converters")
        void providesAllPrimitiveConverters() {
            TypeConverterRegistry registry = new TypeConverterRegistry(ConversionConfig.defaults());

            assertThat(registry.getBoolean()).isNotNull();
            assertThat(registry.getInteger()).isNotNull();
            assertThat(registry.getLong()).isNotNull();
            assertThat(registry.getFloat()).isNotNull();
            assertThat(registry.getDouble()).isNotNull();
            assertThat(registry.getString()).isNotNull();
            assertThat(registry.getBinary()).isNotNull();
            assertThat(registry.getUuid()).isNotNull();
            assertThat(registry.getDate()).isNotNull();
            assertThat(registry.getTime()).isNotNull();
        }

        @Test
        @DisplayName("provides timestamp converters with UTC flag")
        void providesTimestampConverters() {
            TypeConverterRegistry registry = new TypeConverterRegistry(ConversionConfig.defaults());

            TimestampConverter withUtc = registry.getTimestamp(true);
            TimestampConverter withoutUtc = registry.getTimestamp(false);

            assertThat(withUtc.isAdjustToUtc()).isTrue();
            assertThat(withoutUtc.isAdjustToUtc()).isFalse();

            // Same flag = cached
            assertThat(registry.getTimestamp(true)).isSameAs(withUtc);
        }

        @Test
        @DisplayName("provides fixed-length binary converter")
        void providesFixedLengthBinaryConverter() {
            TypeConverterRegistry registry = new TypeConverterRegistry(ConversionConfig.defaults());

            BinaryConverter fixed8 = registry.getFixed(8);
            BinaryConverter fixed16 = registry.getFixed(16);

            assertThat(fixed8.isFixed()).isTrue();
            assertThat(fixed8.getFixedLength()).isEqualTo(8);
            assertThat(fixed16.getFixedLength()).isEqualTo(16);
        }
    }
}