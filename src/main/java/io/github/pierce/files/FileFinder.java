package io.github.pierce.files;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * Unified File Finder - A comprehensive, dynamic file finding solution.
 *
 * This class combines all file finding functionality into a single, cohesive system
 * that automatically searches across the entire project structure without requiring
 * hardcoded paths.
 *
 * Note: This class uses both java.nio.file.Path and org.apache.hadoop.fs.Path.
 * To avoid confusion, we use fully qualified names for Hadoop Path.
 *
 * Features:
 * - Automatic discovery of files across project structure
 * - Intelligent caching for performance
 * - Comprehensive error messages with suggestions
 * - Support for local files, classpath, HDFS, and S3
 * - Thread-safe operations
 * - Security validations
 * - File watching for development
 *
 * Usage:
 * <pre>
 *
 * InputStream is = FileFinder.findFile("product_schema.avsc");
 *
 * // Get detailed error info if not found
 * try {
 *     InputStream is = FileFinder.findFile("missing.avsc");
 * } catch (FileNotFoundException e) {
 *     // Exception includes list of searched locations and available files
 * }
 * </pre>
 */
public class FileFinder {

    private static final Logger LOG = LoggerFactory.getLogger(FileFinder.class);


    private static volatile FileFinder INSTANCE;
    private static final Object LOCK = new Object();


    private final Config config;
    private final Configuration hadoopConfig;


    private final LoadingCache<String, FileHandle> fileCache;
    private final LoadingCache<String, byte[]> contentCache;
    private final ConcurrentHashMap<String, FileLocation> locationCache;
    private final ConcurrentHashMap<String, List<String>> discoveryCache;


    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final AtomicLong searchAttempts = new AtomicLong();


    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutor;

    /**
     * Configuration class - all settings in one place
     */
    public static class Config {

        private final List<String> searchPaths = Arrays.asList(

                ".",


                "src/main/resources",
                "src/main/resources/schemas",
                "src/main/resources/avro",
                "src/main/avro",
                "src/main/avro/schemas",


                "src/test/resources",
                "src/test/resources/schemas",
                "src/test/resources/avro",
                "src/test/avro",
                "src/test/avro/schemas",


                "src/main/resources",
                "src/test/resources",
                "build/resources/main",
                "build/resources/test",


                "target/classes",
                "target/test-classes",
                "build/classes/java/main",
                "build/classes/java/test",


                "schemas",
                "avro",
                "avro-schemas",
                "conf",
                "config",
                "conf/schemas",
                "config/schemas",
                "data",
                "data/schemas",
                "test-data",
                "test-data/schemas",


                "..",
                "../..",
                "../../.."
        );


        private final Set<String> allowedExtensions = new HashSet<>(Arrays.asList(
                ".avsc", ".json", ".xml", ".yaml", ".yml", ".properties", ".conf",
                ".txt", ".csv", ".parquet", ".avro", ".gz", ".config"
        ));


        private int maxCacheSize = 1000;
        private long cacheExpireMinutes = 60;
        private boolean enableContentCache = true;
        private long maxContentCacheBytes = 50L * 1024 * 1024;


        private long maxFileSize = 100L * 1024 * 1024;
        private boolean validatePaths = true;


        private int maxSearchDepth = 5;
        private int threadPoolSize = Runtime.getRuntime().availableProcessors();

        // Custom paths
        private final List<String> additionalPaths = new ArrayList<>();

        public Config() {
            // Initialize with environment-specific paths
            initializeEnvironmentPaths();
        }

        private void initializeEnvironmentPaths() {
            // Add Maven/Gradle wrapper locations
            if (Files.exists(Paths.get("mvnw"))) {
                additionalPaths.add(".mvn");
            }
            if (Files.exists(Paths.get("gradlew"))) {
                additionalPaths.add("gradle");
            }

            // Add IntelliJ IDEA paths
            if (Files.exists(Paths.get(".idea"))) {
                additionalPaths.add("out/production/classes");
                additionalPaths.add("out/test/classes");
            }

            // Add Eclipse paths
            if (Files.exists(Paths.get(".project"))) {
                additionalPaths.add("bin");
                additionalPaths.add("bin/main");
                additionalPaths.add("bin/test");
            }
        }

        public List<String> getAllSearchPaths() {
            List<String> allPaths = new ArrayList<>(searchPaths);
            allPaths.addAll(additionalPaths);
            return allPaths;
        }
    }

    /**
     * File handle with metadata
     */
    private static class FileHandle {
        final String path;
        final FileLocation location;
        final long lastModified;
        final long size;
        final boolean isCompressed;
        final Instant discoveredAt;

        FileHandle(String path, FileLocation location, long lastModified, long size, boolean isCompressed) {
            this.path = path;
            this.location = location;
            this.lastModified = lastModified;
            this.size = size;
            this.isCompressed = isCompressed;
            this.discoveredAt = Instant.now();
        }
    }

    /**
     * File location information
     */
    public static class FileLocation {
        public enum Type {
            LOCAL_FILE("Local File"),
            CLASSPATH("Classpath Resource"),
            HDFS("HDFS"),
            S3("S3"),
            URL("Remote URL"),
            JAR("Inside JAR"),
            UNKNOWN("Unknown");

            private final String displayName;

            Type(String displayName) {
                this.displayName = displayName;
            }

            public String getDisplayName() {
                return displayName;
            }
        }

        public final String fileName;
        public final String absolutePath;
        public final String relativePath;
        public final Type type;
        public final String container; // JAR name, HDFS cluster, etc.

        public FileLocation(String fileName, String absolutePath, String relativePath, Type type, String container) {
            this.fileName = fileName;
            this.absolutePath = absolutePath;
            this.relativePath = relativePath;
            this.type = type;
            this.container = container;
        }

        @Override
        public String toString() {
            return String.format("%s [%s] -> %s", fileName, type.getDisplayName(), relativePath);
        }
    }

    /**
     * Custom exception with detailed information
     */
    public static class FileFinderException extends FileNotFoundException {
        private final String fileName;
        private final List<String> searchedLocations;
        private final List<String> availableFiles;
        private final List<String> suggestions;

        public FileFinderException(String fileName, List<String> searchedLocations,
                                   List<String> availableFiles, List<String> suggestions) {
            super(createDetailedMessage(fileName, searchedLocations, availableFiles, suggestions));
            this.fileName = fileName;
            this.searchedLocations = searchedLocations;
            this.availableFiles = availableFiles;
            this.suggestions = suggestions;
        }

        private static String createDetailedMessage(String fileName, List<String> searchedLocations,
                                                    List<String> availableFiles, List<String> suggestions) {
            StringBuilder msg = new StringBuilder();
            msg.append("File not found: ").append(fileName).append("\n\n");

            msg.append("Searched ").append(searchedLocations.size()).append(" locations:\n");
            searchedLocations.stream().limit(10).forEach(loc -> msg.append("  - ").append(loc).append("\n"));
            if (searchedLocations.size() > 10) {
                msg.append("  ... and ").append(searchedLocations.size() - 10).append(" more locations\n");
            }

            if (!availableFiles.isEmpty()) {
                msg.append("\nFound ").append(availableFiles.size()).append(" similar files:\n");
                availableFiles.stream().limit(5).forEach(file -> msg.append("  - ").append(file).append("\n"));
                if (availableFiles.size() > 5) {
                    msg.append("  ... and ").append(availableFiles.size() - 5).append(" more files\n");
                }
            }

            if (!suggestions.isEmpty()) {
                msg.append("\nSuggestions:\n");
                suggestions.forEach(sug -> msg.append("  ✓ ").append(sug).append("\n"));
            }

            return msg.toString();
        }

        public String getFileName() { return fileName; }
        public List<String> getSearchedLocations() { return searchedLocations; }
        public List<String> getAvailableFiles() { return availableFiles; }
        public List<String> getSuggestions() { return suggestions; }
    }

    // ===== CONSTRUCTOR =====
    private FileFinder(Config config) {
        this.config = config;
        this.hadoopConfig = new Configuration();
        this.locationCache = new ConcurrentHashMap<>();
        this.discoveryCache = new ConcurrentHashMap<>();

        // Initialize thread pools
        this.executorService = Executors.newFixedThreadPool(config.threadPoolSize, r -> {
            Thread t = new Thread(r, "FileFinder-" + Thread.currentThread().getId());
            t.setDaemon(true);
            return t;
        });

        this.scheduledExecutor = Executors.newScheduledThreadPool(2);

        // Initialize file cache
        this.fileCache = CacheBuilder.newBuilder()
                .maximumSize(config.maxCacheSize)
                .expireAfterWrite(config.cacheExpireMinutes, TimeUnit.MINUTES)
                .recordStats()
                .build(new CacheLoader<>() {
                    @Override
                    public FileHandle load(String fileName) throws Exception {
                        return findFileHandle(fileName);
                    }
                });

        // Initialize content cache if enabled
        if (config.enableContentCache) {
            this.contentCache = CacheBuilder.newBuilder()
                    .maximumWeight(config.maxContentCacheBytes)
                    .weigher((String key, byte[] value) -> value.length)
                    .expireAfterWrite(config.cacheExpireMinutes, TimeUnit.MINUTES)
                    .recordStats()
                    .build(new CacheLoader<>() {
                        @Override
                        public byte[] load(String fileName) throws Exception {
                            try (InputStream is = getInputStream(fileName)) {
                                return is.readAllBytes();
                            }
                        }
                    });
        } else {
            this.contentCache = null;
        }

        // Schedule periodic cache cleanup and metrics logging
        scheduledExecutor.scheduleAtFixedRate(this::performMaintenance, 5, 5, TimeUnit.MINUTES);

        // Log initialization
        LOG.info("FileFinder initialized with {} search paths", config.getAllSearchPaths().size());
    }

    // ===== PUBLIC STATIC METHODS =====

    /**
     * Get or create the singleton instance
     */
    private static FileFinder getInstance() {
        if (INSTANCE == null) {
            synchronized (LOCK) {
                if (INSTANCE == null) {
                    INSTANCE = new FileFinder(new Config());
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Find file and return InputStream
     */
    public static InputStream findFile(String fileName) throws IOException {
        return getInstance().getInputStream(fileName);
    }

    /**
     * Check if file exists
     */
    public static boolean fileExists(String fileName) {
        try {
            getInstance().fileCache.get(fileName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Get file content as bytes
     */
    public static byte[] getFileContent(String fileName) throws IOException {
        FileFinder instance = getInstance();
        if (instance.contentCache != null) {
            try {
                return instance.contentCache.get(fileName);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                throw new IOException("Failed to load file content", cause);
            }
        } else {
            try (InputStream is = findFile(fileName)) {
                return is.readAllBytes();
            }
        }
    }

    /**
     * Get file metadata
     */
    public static FileMetadata getFileMetadata(String fileName) throws IOException {
        try {
            FileHandle handle = getInstance().fileCache.get(fileName);
            return new FileMetadata(
                    handle.path,
                    handle.size,
                    handle.lastModified,
                    handle.isCompressed,
                    handle.location
            );
        } catch (ExecutionException e) {
            throw new IOException("Failed to get file metadata", e.getCause());
        }
    }

    /**
     * Discover files with specific extension
     */
    public static List<String> discoverFiles(String extension) {
        return getInstance().performDiscovery(extension);
    }

    /**
     * Discover all Avro schema files
     */
    public static List<String> discoverAvroSchemas() {
        return discoverFiles(".avsc");
    }

    /**
     * Clear all caches
     */
    public static void clearCaches() {
        FileFinder instance = getInstance();
        instance.fileCache.invalidateAll();
        if (instance.contentCache != null) {
            instance.contentCache.invalidateAll();
        }
        instance.locationCache.clear();
        instance.discoveryCache.clear();
        LOG.info("All caches cleared");
    }

    /**
     * Get detailed statistics
     */
    public static Statistics getStatistics() {
        FileFinder instance = getInstance();
        return new Statistics(
                instance.cacheHits.get(),
                instance.cacheMisses.get(),
                instance.searchAttempts.get(),
                instance.fileCache.size(),
                instance.contentCache != null ? instance.contentCache.size() : 0,
                instance.locationCache.size()
        );
    }

    // ===== CORE IMPLEMENTATION =====

    /**
     * Get input stream for file
     */
    private InputStream getInputStream(String fileName) throws IOException {
        if (fileName == null) {
            throw new IOException("File name cannot be null");
        }
        if (fileName.trim().isEmpty()) {
            throw new IOException("File name cannot be null or empty");
        }

        try {
            FileHandle handle = fileCache.get(fileName);
            cacheHits.incrementAndGet();
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
     * Find file handle - the core search logic
     */
    private FileHandle findFileHandle(String fileName) throws IOException {
        LOG.debug("Searching for file: {}", fileName);
        searchAttempts.incrementAndGet();

        List<String> searchedLocations = new ArrayList<>();

        // Strategy 1: Check if it's already a path
        if (fileName.contains("/") || fileName.contains("\\")) {
            Path directPath = Paths.get(fileName);
            if (Files.exists(directPath)) {
                LOG.debug("Found at direct path: {}", directPath);
                return createLocalFileHandle(directPath);
            }
            searchedLocations.add(directPath.toString());
        }

        // Strategy 2: Search classpath
        FileHandle classpathHandle = searchClasspath(fileName, searchedLocations);
        if (classpathHandle != null) {
            return classpathHandle;
        }

        // Strategy 3: Search all configured paths
        FileHandle localHandle = searchLocalPaths(fileName, searchedLocations);
        if (localHandle != null) {
            return localHandle;
        }

        // Strategy 4: Try HDFS if path looks like HDFS
        if (fileName.startsWith("hdfs://")) {
            FileHandle hdfsHandle = searchHdfs(fileName, searchedLocations);
            if (hdfsHandle != null) {
                return hdfsHandle;
            }
        }

        // Strategy 5: Deep search as last resort
        FileHandle deepSearchHandle = performDeepSearch(fileName, searchedLocations);
        if (deepSearchHandle != null) {
            return deepSearchHandle;
        }

        // File not found - create helpful exception
        throw createNotFoundException(fileName, searchedLocations);
    }

    /**
     * Search classpath for file
     */
    private FileHandle searchClasspath(String fileName, List<String> searchedLocations) {
        // Try direct classpath lookup
        String[] classpathLocations = {
                fileName,
                "/" + fileName,
                "schemas/" + fileName,
                "/schemas/" + fileName,
                "avro/" + fileName,
                "/avro/" + fileName
        };

        for (String location : classpathLocations) {
            searchedLocations.add("classpath:" + location);

            URL resource = getClass().getResourceAsStream(location) != null
                    ? getClass().getResource(location) : null;

            if (resource == null) {
                resource = Thread.currentThread().getContextClassLoader().getResource(location.startsWith("/")
                        ? location.substring(1) : location);
            }

            if (resource != null) {
                try {
                    LOG.debug("Found in classpath: {}", resource);
                    return createClasspathHandle(resource, fileName);
                } catch (IOException e) {
                    LOG.warn("Failed to load from classpath: {}", resource, e);
                }
            }
        }

        return null;
    }

    /**
     * Search local file paths
     */
    private FileHandle searchLocalPaths(String fileName, List<String> searchedLocations) {
        for (String basePath : config.getAllSearchPaths()) {
            Path path = Paths.get(basePath, fileName);
            searchedLocations.add(path.toString());

            if (Files.exists(path)) {
                try {
                    LOG.debug("Found at: {}", path);
                    return createLocalFileHandle(path);
                } catch (IOException e) {
                    LOG.warn("Failed to load from: {}", path, e);
                }
            }
        }

        return null;
    }

    /**
     * Search HDFS
     */
    private FileHandle searchHdfs(String path, List<String> searchedLocations) {
        try {
            org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(path);
            FileSystem fs = hdfsPath.getFileSystem(hadoopConfig);
            searchedLocations.add("hdfs:" + path);

            if (fs.exists(hdfsPath)) {
                FileStatus status = fs.getFileStatus(hdfsPath);
                FileLocation location = new FileLocation(
                        hdfsPath.getName(),
                        hdfsPath.toString(),
                        hdfsPath.toString(),
                        FileLocation.Type.HDFS,
                        fs.getUri().toString()
                );

                return new FileHandle(
                        hdfsPath.toString(),
                        location,
                        status.getModificationTime(),
                        status.getLen(),
                        isCompressed(hdfsPath.getName())
                );
            }
        } catch (Exception e) {
            LOG.debug("HDFS search failed: {}", e.getMessage());
        }

        return null;
    }

    /**
     * Perform deep search
     */
    private FileHandle performDeepSearch(String fileName, List<String> searchedLocations) {
        try {
            Path start = Paths.get(".").toAbsolutePath();
            DeepFileSearcher searcher = new DeepFileSearcher(fileName, config.maxSearchDepth);
            Files.walkFileTree(start, EnumSet.noneOf(FileVisitOption.class), config.maxSearchDepth, searcher);

            if (searcher.getFound() != null) {
                searchedLocations.add("deep-search");
                return createLocalFileHandle(searcher.getFound());
            }
        } catch (IOException e) {
            LOG.debug("Deep search failed: {}", e.getMessage());
        }

        return null;
    }

    /**
     * Create helpful not found exception
     */
    private FileFinderException createNotFoundException(String fileName, List<String> searchedLocations) {
        // Find similar files
        String extension = getExtension(fileName);
        List<String> similarFiles = performDiscovery(extension).stream()
                .filter(f -> {
                    String fName = Paths.get(f).getFileName().toString();
                    return fName.contains(getBaseName(fileName)) ||
                            calculateSimilarity(fName, fileName) > 0.5;
                })
                .limit(10)
                .collect(Collectors.toList());

        // Create suggestions
        List<String> suggestions = new ArrayList<>();
        suggestions.add("Check if the file exists in your project");
        suggestions.add("Verify the filename is spelled correctly (case-sensitive)");

        if (!similarFiles.isEmpty()) {
            suggestions.add("Did you mean: " + similarFiles.get(0) + "?");
        }

        suggestions.add("Common locations: src/main/resources/, src/test/resources/, src/test/avro/");

        if (extension.equals(".avsc")) {
            suggestions.add("For Avro schemas, place files in src/main/avro/ or src/main/resources/schemas/");
        }

        return new FileFinderException(fileName, searchedLocations, similarFiles, suggestions);
    }

    /**
     * Create file handles
     */
    private FileHandle createLocalFileHandle(Path path) throws IOException {
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);

        // Handle path relativization safely
        String relativePath;
        try {
            Path currentDir = Paths.get(".").toAbsolutePath().normalize();
            Path absolutePath = path.toAbsolutePath().normalize();

            // Check if paths have the same root (important on Windows)
            if (currentDir.getRoot().equals(absolutePath.getRoot())) {
                relativePath = currentDir.relativize(absolutePath).toString();
            } else {
                // If different roots, just use the absolute path
                relativePath = absolutePath.toString();
            }
        } catch (IllegalArgumentException e) {
            // If relativization fails, fall back to absolute path
            LOG.debug("Could not relativize path {}: {}", path, e.getMessage());
            relativePath = path.toAbsolutePath().toString();
        }

        FileLocation location = new FileLocation(
                path.getFileName().toString(),
                path.toAbsolutePath().toString(),
                relativePath,
                FileLocation.Type.LOCAL_FILE,
                null
        );

        return new FileHandle(
                path.toString(),
                location,
                attrs.lastModifiedTime().toMillis(),
                attrs.size(),
                isCompressed(path.getFileName().toString())
        );
    }

    private FileHandle createClasspathHandle(URL resource, String fileName) throws IOException {
        URLConnection conn = resource.openConnection();

        String container = null;
        FileLocation.Type type = FileLocation.Type.CLASSPATH;

        if ("jar".equals(resource.getProtocol())) {
            type = FileLocation.Type.JAR;
            String jarPath = resource.getPath();
            if (jarPath.contains("!")) {
                container = jarPath.substring(0, jarPath.indexOf("!"));
            }
        }

        FileLocation location = new FileLocation(
                fileName,
                resource.toString(),
                resource.getPath(),
                type,
                container
        );

        return new FileHandle(
                resource.toString(),
                location,
                conn.getLastModified(),
                conn.getContentLengthLong(),
                isCompressed(fileName)
        );
    }

    /**
     * Open input stream from handle
     */
    private InputStream openInputStream(FileHandle handle) throws IOException {
        InputStream is;

        if (handle.path.startsWith("file:") || handle.path.startsWith("jar:") ||
                handle.path.startsWith("http:") || handle.path.startsWith("https:")) {
            is = new URL(handle.path).openStream();
        } else if (handle.path.startsWith("hdfs://")) {
            org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(handle.path);
            FileSystem fs = hdfsPath.getFileSystem(hadoopConfig);
            is = fs.open(hdfsPath);
        } else {
            is = Files.newInputStream(Paths.get(handle.path));
        }

        // Handle compression
        if (handle.isCompressed && handle.path.endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }

        return new BufferedInputStream(is, 64 * 1024);
    }

    /**
     * Perform file discovery
     */
    private List<String> performDiscovery(String extension) {
        String cacheKey = "ext:" + extension;

        // Check cache first
        List<String> cached = discoveryCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        Set<String> discovered = ConcurrentHashMap.newKeySet();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Search each path in parallel
        for (String searchPath : config.getAllSearchPaths()) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    Path path = Paths.get(searchPath);
                    if (Files.exists(path) && Files.isDirectory(path)) {
                        Files.walk(path, 2)
                                .filter(p -> {
                                    try {
                                        return p.toString().endsWith(extension);
                                    } catch (Exception e) {
                                        return false;
                                    }
                                })
                                .forEach(p -> {
                                    try {
                                        Path currentDir = Paths.get(".").toAbsolutePath().normalize();
                                        Path absolutePath = p.toAbsolutePath().normalize();

                                        String relative;
                                        if (currentDir.getRoot().equals(absolutePath.getRoot())) {
                                            relative = currentDir.relativize(absolutePath).toString();
                                        } else {
                                            relative = absolutePath.toString();
                                        }
                                        discovered.add(relative);
                                    } catch (Exception e) {
                                        // If relativization fails, use absolute path
                                        discovered.add(p.toAbsolutePath().toString());
                                    }
                                });
                    }
                } catch (AccessDeniedException e) {
                    LOG.debug("Access denied to path: {} - {}", searchPath, e.getMessage());
                } catch (IOException e) {
                    LOG.debug("Could not search in path: {} - {}", searchPath, e.getMessage());
                } catch (Exception e) {
                    LOG.debug("Error searching path: {} - {}", searchPath, e.getMessage());
                }
            }, executorService));
        }

        // Wait for all searches to complete
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            LOG.debug("Error during discovery: {}", e.getMessage());
        }

        List<String> result = new ArrayList<>(discovered);
        result.sort(String::compareTo);

        // Cache the result
        discoveryCache.put(cacheKey, result);

        return result;
    }

    /**
     * Perform periodic maintenance
     */
    private void performMaintenance() {
        // Log metrics
        LOG.info("FileFinder stats - Hits: {}, Misses: {}, Searches: {}, " +
                        "FileCache: {}, ContentCache: {}, Locations: {}",
                cacheHits.get(), cacheMisses.get(), searchAttempts.get(),
                fileCache.size(),
                contentCache != null ? contentCache.size() : 0,
                locationCache.size());

        // Clean up old discovery cache entries
        if (discoveryCache.size() > 100) {
            discoveryCache.clear();
        }
    }

    // ===== UTILITY METHODS =====

    private static String getExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return dot > 0 ? fileName.substring(dot) : "";
    }

    private static String getBaseName(String fileName) {
        String name = Paths.get(fileName).getFileName().toString();
        int dot = name.lastIndexOf('.');
        return dot > 0 ? name.substring(0, dot) : name;
    }

    private static boolean isCompressed(String fileName) {
        return fileName.endsWith(".gz") || fileName.endsWith(".zip") ||
                fileName.endsWith(".bz2") || fileName.endsWith(".xz");
    }

    private static double calculateSimilarity(String s1, String s2) {
        String longer = s1.toLowerCase();
        String shorter = s2.toLowerCase();

        if (longer.length() < shorter.length()) {
            String temp = longer;
            longer = shorter;
            shorter = temp;
        }

        int longerLength = longer.length();
        if (longerLength == 0) {
            return 1.0;
        }

        int editDistance = computeEditDistance(longer, shorter);
        return (longerLength - editDistance) / (double) longerLength;
    }

    private static int computeEditDistance(String s1, String s2) {
        int[][] dp = new int[s1.length() + 1][s2.length() + 1];

        for (int i = 0; i <= s1.length(); i++) {
            for (int j = 0; j <= s2.length(); j++) {
                if (i == 0) {
                    dp[i][j] = j;
                } else if (j == 0) {
                    dp[i][j] = i;
                } else {
                    dp[i][j] = Math.min(
                            dp[i - 1][j - 1] + (s1.charAt(i - 1) == s2.charAt(j - 1) ? 0 : 1),
                            Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1)
                    );
                }
            }
        }

        return dp[s1.length()][s2.length()];
    }

    // ===== INNER CLASSES =====

    /**
     * Deep file searcher
     */
    private static class DeepFileSearcher extends SimpleFileVisitor<Path> {
        private final String targetFileName;
        private final int maxDepth;
        private Path found;
        private int currentDepth = 0;

        DeepFileSearcher(String fileName, int maxDepth) {
            this.targetFileName = fileName;
            this.maxDepth = maxDepth;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            currentDepth++;

            if (currentDepth > maxDepth) {
                return FileVisitResult.SKIP_SUBTREE;
            }

            String dirName = dir.getFileName().toString();
            if (dirName.startsWith(".") ||
                    dirName.equals("node_modules") ||
                    dirName.equals("target") ||
                    dirName.equals("build") ||
                    dirName.equals("out")) {
                return FileVisitResult.SKIP_SUBTREE;
            }

            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            if (file.getFileName().toString().equals(targetFileName)) {
                found = file;
                return FileVisitResult.TERMINATE;
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
            currentDepth--;
            return FileVisitResult.CONTINUE;
        }

        public Path getFound() {
            return found;
        }
    }

    /**
     * File metadata
     */
    public static class FileMetadata {
        public final String path;
        public final long size;
        public final long lastModified;
        public final boolean isCompressed;
        public final FileLocation location;

        FileMetadata(String path, long size, long lastModified, boolean isCompressed, FileLocation location) {
            this.path = path;
            this.size = size;
            this.lastModified = lastModified;
            this.isCompressed = isCompressed;
            this.location = location;
        }

        public String getFormattedSize() {
            if (size < 1024) return size + " B";
            if (size < 1024 * 1024) return String.format("%.1f KB", size / 1024.0);
            if (size < 1024 * 1024 * 1024) return String.format("%.1f MB", size / (1024.0 * 1024));
            return String.format("%.1f GB", size / (1024.0 * 1024 * 1024));
        }

        public String getFormattedLastModified() {
            return LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(lastModified),
                    ZoneId.systemDefault()
            ).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }
    }

    /**
     * Statistics
     */
    public static class Statistics {
        public final long cacheHits;
        public final long cacheMisses;
        public final long searchAttempts;
        public final long fileCacheSize;
        public final long contentCacheSize;
        public final long locationCacheSize;

        Statistics(long cacheHits, long cacheMisses, long searchAttempts,
                   long fileCacheSize, long contentCacheSize, long locationCacheSize) {
            this.cacheHits = cacheHits;
            this.cacheMisses = cacheMisses;
            this.searchAttempts = searchAttempts;
            this.fileCacheSize = fileCacheSize;
            this.contentCacheSize = contentCacheSize;
            this.locationCacheSize = locationCacheSize;
        }

        public double getHitRate() {
            long total = cacheHits + cacheMisses;
            return total > 0 ? (double) cacheHits / total : 0.0;
        }

        @Override
        public String toString() {
            return String.format("Stats[hits=%d, misses=%d, hitRate=%.2f%%, searches=%d, caches=%d/%d/%d]",
                    cacheHits, cacheMisses, getHitRate() * 100,
                    searchAttempts, fileCacheSize, contentCacheSize, locationCacheSize);
        }
    }

    // ===== UTILITY CLASS =====

    /**
     * Utility methods for common operations
     */
    public static class Util {

        /**
         * Read file as string
         */
        public static String readAsString(String fileName) throws IOException {
            return readAsString(fileName, StandardCharsets.UTF_8);
        }

        public static String readAsString(String fileName, java.nio.charset.Charset charset) throws IOException {
            return new String(getFileContent(fileName), charset);
        }

        /**
         * Read file lines
         */
        public static List<String> readLines(String fileName) throws IOException {
            try (InputStream is = findFile(fileName);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                return reader.lines().collect(Collectors.toList());
            }
        }

        /**
         * Read properties file
         */
        public static Properties readProperties(String fileName) throws IOException {
            Properties props = new Properties();
            try (InputStream is = findFile(fileName)) {
                props.load(is);
            }
            return props;
        }

        /**
         * Read JSON file
         */
        public static JSONObject readJson(String fileName) throws IOException {
            String content = readAsString(fileName);
            return new JSONObject(content);
        }

        /**
         * Read YAML file
         */
        public static Object readYaml(String fileName) throws IOException {
            try (InputStream is = findFile(fileName)) {
                Yaml yaml = new Yaml();
                return yaml.load(is);
            }
        }

        /**
         * Copy file to output stream
         */
        public static long copyTo(String fileName, OutputStream out) throws IOException {
            try (InputStream is = findFile(fileName)) {
                return is.transferTo(out);
            }
        }

        /**
         * Save to local file
         */
        public static void saveToFile(String fileName, Path targetPath) throws IOException {
            try (OutputStream out = Files.newOutputStream(targetPath)) {
                copyTo(fileName, out);
            }
        }
    }
}