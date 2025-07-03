package io.github.pierce.files;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Simplified FileFinder facade that maintains backward compatibility
 * while using the secure implementation underneath.
 */
public class FileFinder {

    private static final SecureFileFinder DEFAULT_INSTANCE;
    private static final LoadingCache<FileFinderProfile, SecureFileFinder> PROFILE_INSTANCES;

    static {
        // Initialize default instance with standard configuration
        DEFAULT_INSTANCE = new SecureFileFinder(
                new SecureFileFinder.FileFinderConfig.Builder()
                        .maxCacheSize(500)
                        .cacheExpireMinutes(30)
                        .enableContentCache(true)
                        .validateChecksum(false)
                        .build()
        );

        // Initialize profile-based instances cache
        PROFILE_INSTANCES = CacheBuilder.newBuilder()
                .maximumSize(10)
                .expireAfterAccess(1, TimeUnit.HOURS)
                .build(new CacheLoader<FileFinderProfile, SecureFileFinder>() {
                    @Override
                    public SecureFileFinder load(FileFinderProfile profile) {
                        return createFinderForProfile(profile);
                    }
                });
    }

    /**
     * Find file using default instance (backward compatible)
     */
    public static InputStream findFile(String fileName) throws IOException {
        return DEFAULT_INSTANCE.findFile(fileName);
    }

    /**
     * Get file content as bytes
     */
    public static byte[] getFileContent(String fileName) throws IOException {
        return DEFAULT_INSTANCE.getFileContent(fileName);
    }

    /**
     * Check if file exists
     */
    public static boolean fileExists(String fileName) {
        return DEFAULT_INSTANCE.fileExists(fileName);
    }

    /**
     * Get file metadata
     */
    public static SecureFileFinder.FileMetadata getFileMetadata(String fileName) throws IOException {
        return DEFAULT_INSTANCE.getFileMetadata(fileName);
    }

    /**
     * Clear caches
     */
    public static void clearCaches() {
        DEFAULT_INSTANCE.clearCaches();
    }

    /**
     * Get a file finder instance for a specific profile
     */
    public static SecureFileFinder getInstance(FileFinderProfile profile) {
        try {
            return PROFILE_INSTANCES.get(profile);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to create FileFinder for profile: " + profile, e);
        }
    }

    /**
     * Create a custom file finder with specific configuration
     */
    public static SecureFileFinder createCustom(SecureFileFinder.FileFinderConfig config) {
        return new SecureFileFinder(config);
    }

    /**
     * Predefined file finder profiles
     */
    public enum FileFinderProfile {
        /**
         * High performance profile with large caches and no validation
         */
        HIGH_PERFORMANCE,

        /**
         * High security profile with strict validation and checksums
         */
        HIGH_SECURITY,

        /**
         * Minimal memory profile with small caches
         */
        LOW_MEMORY,

        /**
         * Development profile with file watching enabled
         */
        DEVELOPMENT,

        /**
         * HDFS optimized profile
         */
        HDFS,

        /**
         * S3 optimized profile
         */
        S3
    }

    /**
     * Create finder for specific profile
     */
    private static SecureFileFinder createFinderForProfile(FileFinderProfile profile) {
        SecureFileFinder.FileFinderConfig.Builder builder =
                new SecureFileFinder.FileFinderConfig.Builder();

        switch (profile) {
            case HIGH_PERFORMANCE:
                return new SecureFileFinder(builder
                        .maxCacheSize(5000)
                        .cacheExpireMinutes(120)
                        .enableContentCache(true)
                        .validateChecksum(false)
                        .maxRetries(1)
                        .build());

            case HIGH_SECURITY:
                return new SecureFileFinder(builder
                        .maxCacheSize(100)
                        .cacheExpireMinutes(10)
                        .enableContentCache(false)
                        .validateChecksum(true)
                        .maxRetries(3)
                        .addBlockedPath("/etc")
                        .addBlockedPath("/var")
                        .addBlockedPath("C:\\Windows")
                        .addBlockedPath("C:\\Program Files")
                        .build());

            case LOW_MEMORY:
                return new SecureFileFinder(builder
                        .maxCacheSize(50)
                        .cacheExpireMinutes(5)
                        .enableContentCache(false)
                        .validateChecksum(false)
                        .build());

            case DEVELOPMENT:
                return new SecureFileFinder(builder
                        .maxCacheSize(200)
                        .cacheExpireMinutes(5)
                        .enableContentCache(true)
                        .enableFileWatching(true)
                        .validateChecksum(false)
                        .build());

            case HDFS:
                return new SecureFileFinder(builder
                        .maxCacheSize(1000)
                        .cacheExpireMinutes(60)
                        .enableContentCache(false)
                        .validateChecksum(false)
                        .maxRetries(5)
                        .retryDelayMs(2000)
                        .build());

            case S3:
                return new SecureFileFinder(builder
                        .maxCacheSize(2000)
                        .cacheExpireMinutes(120)
                        .enableContentCache(true)
                        .validateChecksum(true)
                        .maxRetries(5)
                        .retryDelayMs(3000)
                        .build());

            default:
                throw new IllegalArgumentException("Unknown profile: " + profile);
        }
    }

    /**
     * Shutdown all file finder instances
     */
    public static void shutdownAll() {
        DEFAULT_INSTANCE.shutdown();
        PROFILE_INSTANCES.asMap().values().forEach(SecureFileFinder::shutdown);
        PROFILE_INSTANCES.invalidateAll();
    }
}
