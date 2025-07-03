package io.github.pierce.files;

// FileFinderFactory.java - Factory for creating configured instances

import java.util.Arrays;
import java.util.HashSet;

/**
 * Factory for creating configured FileFinder instances
 */
public class FileFinderFactory {

    /**
     * Create a file finder for schema files
     */
    public static SecureFileFinder createSchemaFileFinder() {
        return new SecureFileFinder(
                new SecureFileFinder.FileFinderConfig.Builder()
                        .allowedExtensions(new HashSet<>(Arrays.asList(".avsc", ".json", ".schema")))
                        .maxFileSize(10L * 1024 * 1024) // 10MB
                        .maxCacheSize(200)
                        .cacheExpireMinutes(60)
                        .enableContentCache(true)
                        .validateChecksum(true)
                        .build()
        );
    }

    /**
     * Create a file finder for configuration files
     */
    public static SecureFileFinder createConfigFileFinder() {
        return new SecureFileFinder(
                new SecureFileFinder.FileFinderConfig.Builder()
                        .allowedExtensions(new HashSet<>(Arrays.asList(
                                ".properties", ".conf", ".yaml", ".yml", ".xml", ".json")))
                        .maxFileSize(1L * 1024 * 1024) // 1MB
                        .maxCacheSize(100)
                        .cacheExpireMinutes(30)
                        .enableContentCache(true)
                        .enableFileWatching(true)
                        .build()
        );
    }

    /**
     * Create a file finder for data files
     */
    public static SecureFileFinder createDataFileFinder() {
        return new SecureFileFinder(
                new SecureFileFinder.FileFinderConfig.Builder()
                        .allowedExtensions(new HashSet<>(Arrays.asList(
                                ".parquet", ".avro", ".csv", ".json", ".txt", ".gz")))
                        .maxFileSize(1024L * 1024 * 1024) // 1GB
                        .maxCacheSize(50)
                        .cacheExpireMinutes(120)
                        .enableContentCache(false) // Too large for content cache
                        .validateChecksum(false) // Too expensive for large files
                        .build()
        );
    }

    /**
     * Create a file finder for test environments
     */
    public static SecureFileFinder createTestFileFinder() {
        return new SecureFileFinder(
                new SecureFileFinder.FileFinderConfig.Builder()
                        .maxFileSize(100L * 1024 * 1024) // 100MB
                        .maxCacheSize(10)
                        .cacheExpireMinutes(1)
                        .enableContentCache(false)
                        .validateChecksum(false)
                        .maxRetries(1)
                        .build()
        );
    }

    /**
     * Create a restricted file finder for untrusted sources
     */
    public static SecureFileFinder createRestrictedFileFinder(String... allowedPaths) {
        SecureFileFinder.FileFinderConfig.Builder builder =
                new SecureFileFinder.FileFinderConfig.Builder()
                        .maxFileSize(10L * 1024 * 1024) // 10MB
                        .maxCacheSize(50)
                        .cacheExpireMinutes(10)
                        .enableContentCache(false)
                        .validateChecksum(true)
                        .maxRetries(1);

        // Add allowed paths
        for (String path : allowedPaths) {
            builder.addAllowedPath(path);
        }

        // Block common sensitive paths
        builder.addBlockedPath("/etc");
        builder.addBlockedPath("/var/log");
        builder.addBlockedPath("/root");
        builder.addBlockedPath("/home");
        builder.addBlockedPath("C:\\Windows");
        builder.addBlockedPath("C:\\Users");
        builder.addBlockedPath("C:\\Program Files");

        return new SecureFileFinder(builder.build());
    }
}