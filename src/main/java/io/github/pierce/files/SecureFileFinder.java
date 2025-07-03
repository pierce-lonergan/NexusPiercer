package io.github.pierce.files;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * Secure, robust and efficient file finder with support for multiple file systems,
 * caching, validation, and comprehensive error handling.
 */
public class SecureFileFinder {

    private static final Logger LOG = LoggerFactory.getLogger(SecureFileFinder.class);

    // Security configurations
    private static final Set<String> ALLOWED_EXTENSIONS = new HashSet<>(Arrays.asList(
            ".avsc", ".json", ".xml", ".properties", ".conf", ".yaml", ".yml",
            ".txt", ".csv", ".parquet", ".avro", ".gz"
    ));

    private static final Set<String> BLOCKED_PATTERNS = new HashSet<>(Arrays.asList(
            "../", "..\\", "%2e%2e", "%252e%252e", "..%2f", "..%5c"
    ));

    private static final long MAX_FILE_SIZE = 100L * 1024 * 1024; // 100MB default
    private static final int MAX_PATH_LENGTH = 4096;

    // Cache configurations
    private final LoadingCache<String, FileHandle> fileCache;
    private final LoadingCache<String, byte[]> contentCache;
    private final ConcurrentHashMap<String, FileWatcher> fileWatchers;

    // Thread pool for async operations
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutor;

    // Configuration
    private final FileFinderConfig config;
    private final Configuration hadoopConfig;

    // Metrics
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final AtomicLong securityBlocks = new AtomicLong();

    /**
     * File finder configuration
     */
    public static class FileFinderConfig {
        private final Set<String> allowedExtensions;
        private final Set<String> allowedPaths;
        private final Set<String> blockedPaths;
        private long maxFileSize = MAX_FILE_SIZE;
        private int maxCacheSize = 1000;
        private long cacheExpireMinutes = 60;
        private boolean enableFileWatching = false;
        private boolean enableContentCache = true;
        private boolean validateChecksum = false;
        private int maxRetries = 3;
        private long retryDelayMs = 1000;

        private FileFinderConfig() {
            this.allowedExtensions = new HashSet<>(ALLOWED_EXTENSIONS);
            this.allowedPaths = new HashSet<>();
            this.blockedPaths = new HashSet<>();
        }

        // Builder pattern for configuration
        public static class Builder {
            private final FileFinderConfig config = new FileFinderConfig();

            public Builder allowedExtensions(Set<String> extensions) {
                config.allowedExtensions.clear();
                config.allowedExtensions.addAll(extensions);
                return this;
            }

            public Builder addAllowedPath(String path) {
                config.allowedPaths.add(normalizePath(path));
                return this;
            }

            public Builder addBlockedPath(String path) {
                config.blockedPaths.add(normalizePath(path));
                return this;
            }

            public Builder maxFileSize(long size) {
                config.maxFileSize = size;
                return this;
            }

            public Builder maxCacheSize(int size) {
                config.maxCacheSize = size;
                return this;
            }

            public Builder cacheExpireMinutes(long minutes) {
                config.cacheExpireMinutes = minutes;
                return this;
            }

            public Builder enableFileWatching(boolean enable) {
                config.enableFileWatching = enable;
                return this;
            }

            public Builder enableContentCache(boolean enable) {
                config.enableContentCache = enable;
                return this;
            }

            public Builder validateChecksum(boolean validate) {
                config.validateChecksum = validate;
                return this;
            }

            public Builder maxRetries(int retries) {
                config.maxRetries = retries;
                return this;
            }

            public Builder retryDelayMs(long delayMs) {
                config.retryDelayMs = delayMs;
                return this;
            }

            public FileFinderConfig build() {
                return config;
            }
        }
    }

    /**
     * File handle with metadata
     */
    private static class FileHandle {
        final String path;
        final FileSystem fileSystem;
        final long lastModified;
        final long size;
        final String checksum;
        final boolean isCompressed;

        FileHandle(String path, FileSystem fs, long lastModified, long size,
                   String checksum, boolean isCompressed) {
            this.path = path;
            this.fileSystem = fs;
            this.lastModified = lastModified;
            this.size = size;
            this.checksum = checksum;
            this.isCompressed = isCompressed;
        }
    }

    /**
     * Constructor with configuration
     */
    public SecureFileFinder(FileFinderConfig config) {
        this.config = config;
        this.hadoopConfig = new Configuration();
        this.fileWatchers = new ConcurrentHashMap<>();

        // Initialize thread pools
        this.executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                r -> {
                    Thread t = new Thread(r, "FileFinder-" + Thread.currentThread().getId());
                    t.setDaemon(true);
                    return t;
                }
        );

        this.scheduledExecutor = Executors.newScheduledThreadPool(2);

        // Initialize file cache
        this.fileCache = CacheBuilder.newBuilder()
                .maximumSize(config.maxCacheSize)
                .expireAfterWrite(config.cacheExpireMinutes, TimeUnit.MINUTES)
                .recordStats()
                .build(new CacheLoader<>() {
                    @Override
                    public FileHandle load(String path) throws Exception {
                        return loadFileHandle(path);
                    }
                });

        // Initialize content cache if enabled
        if (config.enableContentCache) {
            this.contentCache = CacheBuilder.newBuilder()
                    .maximumSize(config.maxCacheSize / 2)
                    .maximumWeight(50L * 1024 * 1024) // 50MB max content cache
                    .weigher((String key, byte[] value) -> value.length)
                    .expireAfterWrite(config.cacheExpireMinutes, TimeUnit.MINUTES)
                    .recordStats()
                    .build(new CacheLoader<>() {
                        @Override
                        public byte[] load(String path) throws Exception {
                            return loadFileContent(path);
                        }
                    });
        } else {
            this.contentCache = null;
        }

        // Schedule metrics logging
        scheduledExecutor.scheduleAtFixedRate(this::logMetrics, 5, 5, TimeUnit.MINUTES);

        LOG.info("SecureFileFinder initialized with config: maxFileSize={}, maxCacheSize={}, " +
                        "cacheExpireMinutes={}, enableFileWatching={}",
                config.maxFileSize, config.maxCacheSize,
                config.cacheExpireMinutes, config.enableFileWatching);
    }

    /**
     * Default constructor with standard configuration
     */
    public SecureFileFinder() {
        this(new FileFinderConfig());
    }

    /**
     * Find and return an InputStream for the specified file
     */
    public InputStream findFile(String fileName) throws IOException {
        validateFileName(fileName);

        try {
            FileHandle handle = fileCache.get(fileName);
            cacheHits.incrementAndGet();

            if (config.enableFileWatching && !fileWatchers.containsKey(fileName)) {
                setupFileWatcher(fileName, handle);
            }

            return openInputStream(handle);

        } catch (ExecutionException e) {
            cacheMisses.incrementAndGet();
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Failed to find file: " + fileName, cause);
        }
    }

    /**
     * Find file asynchronously
     */
    public CompletableFuture<InputStream> findFileAsync(String fileName) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return findFile(fileName);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        }, executorService);
    }

    /**
     * Get file content as byte array (uses content cache if enabled)
     */
    public byte[] getFileContent(String fileName) throws IOException {
        if (!config.enableContentCache) {
            try (InputStream is = findFile(fileName)) {
                return is.readAllBytes();
            }
        }

        try {
            return contentCache.get(fileName);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Failed to load file content: " + fileName, cause);
        }
    }

    /**
     * Check if file exists
     */
    public boolean fileExists(String fileName) {
        try {
            validateFileName(fileName);
            FileHandle handle = fileCache.get(fileName);
            return handle != null;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Get file metadata
     */
    public FileMetadata getFileMetadata(String fileName) throws IOException {
        validateFileName(fileName);

        try {
            FileHandle handle = fileCache.get(fileName);
            return new FileMetadata(
                    handle.path,
                    handle.size,
                    handle.lastModified,
                    handle.checksum,
                    handle.isCompressed
            );
        } catch (ExecutionException e) {
            throw new IOException("Failed to get file metadata: " + fileName, e.getCause());
        }
    }

    /**
     * Validate file name for security
     */
    private void validateFileName(String fileName) throws IOException {
        if (fileName == null || fileName.isEmpty()) {
            throw new IOException("File name cannot be null or empty");
        }

        if (fileName.length() > MAX_PATH_LENGTH) {
            throw new IOException("File path too long: " + fileName.length());
        }

        // Check for path traversal attempts
        String normalized = normalizePath(fileName);
        for (String blocked : BLOCKED_PATTERNS) {
            if (normalized.contains(blocked)) {
                securityBlocks.incrementAndGet();
                throw new SecurityException("Potential path traversal detected: " + fileName);
            }
        }

        // Check allowed extensions
        String extension = getFileExtension(fileName);
        if (!config.allowedExtensions.isEmpty() &&
                !config.allowedExtensions.contains(extension.toLowerCase())) {
            securityBlocks.incrementAndGet();
            throw new SecurityException("File extension not allowed: " + extension);
        }

        // Check allowed/blocked paths
        if (!isPathAllowed(normalized)) {
            securityBlocks.incrementAndGet();
            throw new SecurityException("Path not allowed: " + normalized);
        }
    }

    /**
     * Load file handle with retry logic
     */
    private FileHandle loadFileHandle(String fileName) throws IOException {
        IOException lastException = null;

        for (int attempt = 0; attempt < config.maxRetries; attempt++) {
            try {
                if (attempt > 0) {
                    // Exponential backoff
                    long sleepTime = config.retryDelayMs * attempt;
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while waiting for retry", e);
                    }
                }

                return doLoadFileHandle(fileName);

            } catch (IOException e) {
                lastException = e;
                LOG.warn("Failed to load file {} on attempt {}: {}",
                        fileName, attempt + 1, e.getMessage());
            }
        }

        throw new IOException("Failed to load file after " + config.maxRetries + " attempts",
                lastException);
    }

    /**
     * Actually load the file handle
     */
    private FileHandle doLoadFileHandle(String fileName) throws IOException {
        // Try multiple strategies in order

        // 1. Try classpath
        URL resource = getClass().getClassLoader().getResource(fileName);
        if (resource != null) {
            return createFileHandleFromURL(resource, fileName);
        }

        // 2. Try local file system
        Path localPath = Paths.get(fileName);
        if (Files.exists(localPath)) {
            return createFileHandleFromLocalPath(localPath);
        }

        // 3. Try Hadoop file systems (HDFS, S3, etc.)
        try {
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(fileName);
            FileSystem fs = hadoopPath.getFileSystem(hadoopConfig);
            if (fs.exists(hadoopPath)) {
                return createFileHandleFromHadoop(fs, hadoopPath);
            }
        } catch (Exception e) {
            LOG.debug("Not found in Hadoop filesystem: {}", e.getMessage());
        }

        // 4. Try as URI
        try {
            URI uri = new URI(fileName);
            if (uri.getScheme() != null) {
                return createFileHandleFromURI(uri);
            }
        } catch (URISyntaxException e) {
            LOG.debug("Not a valid URI: {}", fileName);
        }

        // 5. Search in configured paths
        for (String basePath : config.allowedPaths) {
            Path searchPath = Paths.get(basePath, fileName);
            if (Files.exists(searchPath)) {
                return createFileHandleFromLocalPath(searchPath);
            }
        }

        throw new FileNotFoundException("File not found: " + fileName);
    }

    /**
     * Create file handle from URL
     */
    private FileHandle createFileHandleFromURL(URL url, String fileName) throws IOException {
        URLConnection connection = url.openConnection();
        long lastModified = connection.getLastModified();
        long size = connection.getContentLengthLong();

        validateFileSize(size);

        String checksum = null;
        if (config.validateChecksum) {
            try (InputStream is = url.openStream()) {
                checksum = calculateChecksum(is);
            }
        }

        boolean isCompressed = fileName.endsWith(".gz") || fileName.endsWith(".zip");

        return new FileHandle(url.toString(), null, lastModified, size, checksum, isCompressed);
    }

    /**
     * Create file handle from local path
     */
    private FileHandle createFileHandleFromLocalPath(Path path) throws IOException {
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);

        validateFileSize(attrs.size());

        String checksum = null;
        if (config.validateChecksum) {
            try (InputStream is = Files.newInputStream(path)) {
                checksum = calculateChecksum(is);
            }
        }

        boolean isCompressed = path.toString().endsWith(".gz") ||
                path.toString().endsWith(".zip");

        return new FileHandle(path.toString(), null,
                attrs.lastModifiedTime().toMillis(),
                attrs.size(), checksum, isCompressed);
    }

    /**
     * Create file handle from Hadoop filesystem
     */
    private FileHandle createFileHandleFromHadoop(FileSystem fs, org.apache.hadoop.fs.Path path)
            throws IOException {
        FileStatus status = fs.getFileStatus(path);

        validateFileSize(status.getLen());

        String checksum = null;
        if (config.validateChecksum) {
            try (InputStream is = fs.open(path)) {
                checksum = calculateChecksum(is);
            }
        }

        boolean isCompressed = path.toString().endsWith(".gz") ||
                path.toString().endsWith(".zip");

        return new FileHandle(path.toString(), fs, status.getModificationTime(),
                status.getLen(), checksum, isCompressed);
    }

    /**
     * Create file handle from URI
     */
    private FileHandle createFileHandleFromURI(URI uri) throws IOException {
        if ("file".equals(uri.getScheme())) {
            return createFileHandleFromLocalPath(Paths.get(uri));
        }

        // For other schemes, use Hadoop filesystem
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(uri);
        FileSystem fs = hadoopPath.getFileSystem(hadoopConfig);
        return createFileHandleFromHadoop(fs, hadoopPath);
    }

    /**
     * Open input stream from file handle
     */
    private InputStream openInputStream(FileHandle handle) throws IOException {
        InputStream is;

        if (handle.fileSystem != null) {
            // Hadoop filesystem
            is = handle.fileSystem.open(new org.apache.hadoop.fs.Path(handle.path));
        } else if (handle.path.startsWith("http://") || handle.path.startsWith("https://")) {
            // URL
            is = new URL(handle.path).openStream();
        } else if (handle.path.contains("://")) {
            // URI with scheme
            try {
                is = new URI(handle.path).toURL().openStream();
            } catch (URISyntaxException e) {
                throw new IOException("Invalid URI: " + handle.path, e);
            }
        } else {
            // Local file
            is = Files.newInputStream(Paths.get(handle.path));
        }

        // Wrap with decompression if needed
        if (handle.isCompressed && handle.path.endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }

        // Wrap with buffering
        return new BufferedInputStream(is, 64 * 1024);
    }

    /**
     * Load file content
     */
    private byte[] loadFileContent(String fileName) throws IOException {
        try (InputStream is = findFile(fileName)) {
            return is.readAllBytes();
        }
    }

    /**
     * Calculate file checksum
     */
    private String calculateChecksum(InputStream is) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            int read;

            while ((read = is.read(buffer)) != -1) {
                md.update(buffer, 0, read);
            }

            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();

        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 not available", e);
        }
    }

    /**
     * Setup file watcher for cache invalidation
     */
    private void setupFileWatcher(String fileName, FileHandle handle) {
        if (handle.path.contains("://") || handle.fileSystem != null) {
            return; // Only watch local files
        }

        try {
            Path path = Paths.get(handle.path);
            Path parent = path.getParent();
            if (parent == null) return;

            WatchService watchService = FileSystems.getDefault().newWatchService();
            parent.register(watchService,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE);

            FileWatcher watcher = new FileWatcher(watchService, path, fileName);
            fileWatchers.put(fileName, watcher);

            scheduledExecutor.submit(watcher);

        } catch (IOException e) {
            LOG.warn("Failed to setup file watcher for {}: {}", fileName, e.getMessage());
        }
    }

    /**
     * File watcher for cache invalidation
     */
    private class FileWatcher implements Runnable {
        private final WatchService watchService;
        private final Path watchedFile;
        private final String cacheKey;
        private volatile boolean running = true;

        FileWatcher(WatchService watchService, Path watchedFile, String cacheKey) {
            this.watchService = watchService;
            this.watchedFile = watchedFile;
            this.cacheKey = cacheKey;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    WatchKey key = watchService.take();

                    for (WatchEvent<?> event : key.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();

                        if (kind == StandardWatchEventKinds.OVERFLOW) {
                            continue;
                        }

                        Path changed = (Path) event.context();
                        if (watchedFile.getFileName().equals(changed)) {
                            LOG.debug("File changed, invalidating cache: {}", cacheKey);
                            fileCache.invalidate(cacheKey);
                            if (contentCache != null) {
                                contentCache.invalidate(cacheKey);
                            }
                        }
                    }

                    key.reset();

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    running = false;
                } catch (Exception e) {
                    LOG.error("Error in file watcher", e);
                }
            }
        }

        void stop() {
            running = false;
            try {
                watchService.close();
            } catch (IOException e) {
                LOG.warn("Error closing watch service", e);
            }
        }
    }

    /**
     * Validate file size
     */
    private void validateFileSize(long size) throws IOException {
        if (size > config.maxFileSize) {
            throw new IOException(String.format(
                    "File size %d exceeds maximum allowed size %d",
                    size, config.maxFileSize));
        }
    }

    /**
     * Check if path is allowed
     */
    private boolean isPathAllowed(String path) {
        // Check blocked paths first
        for (String blocked : config.blockedPaths) {
            if (path.startsWith(blocked)) {
                return false;
            }
        }

        // If no allowed paths configured, allow all (except blocked)
        if (config.allowedPaths.isEmpty()) {
            return true;
        }

        // Check if path starts with any allowed path
        for (String allowed : config.allowedPaths) {
            if (path.startsWith(allowed)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Normalize path for comparison
     */
    private static String normalizePath(String path) {
        return path.replace('\\', '/').toLowerCase();
    }

    /**
     * Get file extension
     */
    private static String getFileExtension(String fileName) {
        int lastDot = fileName.lastIndexOf('.');
        return lastDot > 0 ? fileName.substring(lastDot) : "";
    }

    /**
     * Log metrics
     */
    private void logMetrics() {
        LOG.info("FileFinder metrics - Cache hits: {}, misses: {}, security blocks: {}, " +
                        "cache size: {}, content cache size: {}",
                cacheHits.get(), cacheMisses.get(), securityBlocks.get(),
                fileCache.size(),
                contentCache != null ? contentCache.size() : 0);

        if (LOG.isDebugEnabled()) {
            LOG.debug("File cache stats: {}", fileCache.stats());
            if (contentCache != null) {
                LOG.debug("Content cache stats: {}", contentCache.stats());
            }
        }
    }

    /**
     * Clear all caches
     */
    public void clearCaches() {
        fileCache.invalidateAll();
        if (contentCache != null) {
            contentCache.invalidateAll();
        }
        LOG.info("All caches cleared");
    }

    /**
     * Shutdown the file finder
     */
    public void shutdown() {
        LOG.info("Shutting down SecureFileFinder");

        // Stop file watchers
        fileWatchers.values().forEach(FileWatcher::stop);
        fileWatchers.clear();

        // Shutdown executors
        executorService.shutdown();
        scheduledExecutor.shutdown();

        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
            if (!scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            scheduledExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Clear caches
        clearCaches();

        LOG.info("SecureFileFinder shutdown complete");
    }

    /**
     * File metadata class
     */
    public static class FileMetadata {
        public final String path;
        public final long size;
        public final long lastModified;
        public final String checksum;
        public final boolean isCompressed;

        FileMetadata(String path, long size, long lastModified,
                     String checksum, boolean isCompressed) {
            this.path = path;
            this.size = size;
            this.lastModified = lastModified;
            this.checksum = checksum;
            this.isCompressed = isCompressed;
        }
    }
}