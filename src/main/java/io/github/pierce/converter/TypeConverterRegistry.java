package io.github.pierce.converter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for type converters with caching support.
 *
 * <p>Provides cached converter instances for common types.</p>
 */
public class TypeConverterRegistry {

    private final ConversionConfig config;
    private final Map<String, TypeConverter<?, ?>> converterCache;

    public TypeConverterRegistry(ConversionConfig config) {
        this.config = config != null ? config : ConversionConfig.defaults();
        this.converterCache = config.isCacheConverters()
                ? new ConcurrentHashMap<>(config.getInitialCacheCapacity())
                : new ConcurrentHashMap<>();
    }

    /**
     * Gets or creates a Boolean converter.
     */
    public BooleanConverter getBoolean() {
        return (BooleanConverter) converterCache.computeIfAbsent("boolean",
                k -> new BooleanConverter(config));
    }

    /**
     * Gets or creates an Integer converter.
     */
    public IntegerConverter getInteger() {
        return (IntegerConverter) converterCache.computeIfAbsent("int",
                k -> new IntegerConverter(config));
    }

    /**
     * Gets or creates a Long converter.
     */
    public LongConverter getLong() {
        return (LongConverter) converterCache.computeIfAbsent("long",
                k -> new LongConverter(config));
    }

    /**
     * Gets or creates a Float converter.
     */
    public FloatConverter getFloat() {
        return (FloatConverter) converterCache.computeIfAbsent("float",
                k -> new FloatConverter(config));
    }

    /**
     * Gets or creates a Double converter.
     */
    public DoubleConverter getDouble() {
        return (DoubleConverter) converterCache.computeIfAbsent("double",
                k -> new DoubleConverter(config));
    }

    /**
     * Gets or creates a String converter.
     */
    public StringConverter getString() {
        return (StringConverter) converterCache.computeIfAbsent("string",
                k -> new StringConverter(config));
    }

    /**
     * Gets or creates a Binary converter.
     */
    public BinaryConverter getBinary() {
        return (BinaryConverter) converterCache.computeIfAbsent("binary",
                k -> new BinaryConverter(config));
    }

    /**
     * Gets or creates a UUID converter.
     */
    public UUIDConverter getUuid() {
        return (UUIDConverter) converterCache.computeIfAbsent("uuid",
                k -> new UUIDConverter(config));
    }

    /**
     * Gets or creates a Date converter.
     */
    public DateConverter getDate() {
        return (DateConverter) converterCache.computeIfAbsent("date",
                k -> new DateConverter(config));
    }

    /**
     * Gets or creates a Time converter.
     */
    public TimeConverter getTime() {
        return (TimeConverter) converterCache.computeIfAbsent("time",
                k -> new TimeConverter(config));
    }

    /**
     * Gets or creates a Timestamp converter.
     */
    public TimestampConverter getTimestamp(boolean adjustToUtc) {
        String key = adjustToUtc ? "timestamptz" : "timestamp";
        return (TimestampConverter) converterCache.computeIfAbsent(key,
                k -> new TimestampConverter(config, adjustToUtc));
    }

    /**
     * Gets or creates a TimestampNano converter.
     */
    public TimestampNanoConverter getTimestampNano(boolean adjustToUtc) {
        String key = adjustToUtc ? "timestamp_ns_tz" : "timestamp_ns";
        return (TimestampNanoConverter) converterCache.computeIfAbsent(key,
                k -> new TimestampNanoConverter(config, adjustToUtc));
    }

    /**
     * Gets or creates a Decimal converter.
     */
    public DecimalConverter getDecimal(int precision, int scale) {
        String key = String.format("decimal(%d,%d)", precision, scale);
        return (DecimalConverter) converterCache.computeIfAbsent(key,
                k -> new DecimalConverter(config, precision, scale));
    }

    /**
     * Gets or creates a Fixed binary converter.
     */
    public BinaryConverter getFixed(int length) {
        String key = "fixed[" + length + "]";
        return (BinaryConverter) converterCache.computeIfAbsent(key,
                k -> BinaryConverter.forFixed(config, length));
    }

    /**
     * Gets the configuration used by this registry.
     */
    public ConversionConfig getConfig() {
        return config;
    }

    /**
     * Clears the converter cache.
     */
    public void clearCache() {
        converterCache.clear();
    }

    /**
     * Returns the current cache size.
     */
    public int cacheSize() {
        return converterCache.size();
    }
}