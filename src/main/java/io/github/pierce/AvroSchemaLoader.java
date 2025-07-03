package io.github.pierce;

import io.github.pierce.files.FileFinder;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enhanced Avro Schema Loader that supports multiple environments and search strategies.
 *
 * Search order:
 * 1. Target directory (if specified)
 * 2. Current working directory
 * 3. Classpath resources
 * 4. Project standard locations
 * 5. HDFS paths (in production)
 *
 * Features:
 * - Multi-environment support (local, test, production)
 * - Caching for performance
 * - Batch loading with partial failure handling
 * - Integration with existing FileFinder
 * - Comprehensive error reporting
 */
public class AvroSchemaLoader {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaLoader.class);

    // Cache for loaded schemas
    private static final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
    private static final Map<String, StructType> structTypeCache = new ConcurrentHashMap<>();

    // Configuration
    private final Configuration hadoopConf;
    private final boolean includeArrayStatistics;
    private final List<String> searchPaths;
    private final boolean enableCaching;
    private final String targetDirectory;

    // Standard schema locations
    private static final String[] STANDARD_SCHEMA_PATHS = {
            "schemas",
            "src/main/resources/schemas",
            "src/test/resources/schemas",
            "conf/schemas",
            "config/schemas",
            "avro/schemas"
    };

    /**
     * Builder for configuring AvroSchemaLoader
     */
    public static class Builder {
        private Configuration hadoopConf = new Configuration();
        private boolean includeArrayStatistics = false;
        private List<String> additionalSearchPaths = new ArrayList<>();
        private boolean enableCaching = true;
        private String targetDirectory = null;

        public Builder withHadoopConfiguration(Configuration conf) {
            this.hadoopConf = conf;
            return this;
        }

        public Builder withArrayStatistics(boolean include) {
            this.includeArrayStatistics = include;
            return this;
        }

        public Builder addSearchPath(String path) {
            this.additionalSearchPaths.add(path);
            return this;
        }

        public Builder addSearchPaths(List<String> paths) {
            this.additionalSearchPaths.addAll(paths);
            return this;
        }

        public Builder withCaching(boolean enable) {
            this.enableCaching = enable;
            return this;
        }

        public Builder withTargetDirectory(String directory) {
            this.targetDirectory = directory;
            return this;
        }

        public AvroSchemaLoader build() {
            return new AvroSchemaLoader(this);
        }
    }

    // Private constructor - use Builder
    private AvroSchemaLoader(Builder builder) {
        this.hadoopConf = builder.hadoopConf;
        this.includeArrayStatistics = builder.includeArrayStatistics;
        this.enableCaching = builder.enableCaching;
        this.targetDirectory = builder.targetDirectory;

        // Build search paths
        this.searchPaths = new ArrayList<>();
        if (targetDirectory != null) {
            searchPaths.add(targetDirectory);
        }
        searchPaths.add("."); // Current directory
        searchPaths.addAll(Arrays.asList(STANDARD_SCHEMA_PATHS));
        searchPaths.addAll(builder.additionalSearchPaths);
    }

    /**
     * Create a default loader for backward compatibility
     */
    public static AvroSchemaLoader createDefault() {
        return new Builder().build();
    }

    /**
     * Load a single Avro schema, searching multiple locations
     */
    public Schema loadAvroSchema(String schemaName) throws IOException {
        String normalizedName = normalizeSchemaName(schemaName);

        // Check cache first
        if (enableCaching && schemaCache.containsKey(normalizedName)) {
            LOG.debug("Schema {} found in cache", normalizedName);
            return schemaCache.get(normalizedName);
        }

        // Try to find and load the schema
        Schema schema = findAndLoadSchema(normalizedName);

        if (schema == null) {
            throw new IOException("Schema not found: " + schemaName +
                    " (searched " + searchPaths.size() + " locations)");
        }

        // Cache the result
        if (enableCaching) {
            schemaCache.put(normalizedName, schema);
        }

        return schema;
    }

    /**
     * Load a single flattened schema and convert to Spark StructType
     */
    public StructType loadFlattenedSchema(String schemaName) throws IOException {
        String cacheKey = schemaName + ":" + includeArrayStatistics;

        // Check cache first
        if (enableCaching && structTypeCache.containsKey(cacheKey)) {
            LOG.debug("Flattened schema {} found in cache", schemaName);
            return structTypeCache.get(cacheKey);
        }

        // Load and flatten the schema
        Schema avroSchema = loadAvroSchema(schemaName);
        Schema flattenedSchema = new AvroSchemaFlattener(includeArrayStatistics)
                .getFlattenedSchema(avroSchema);

        // Convert to Spark StructType
        StructType sparkSchema = CreateSparkStructFromAvroSchema
                .convertNestedAvroSchemaToSparkSchema(flattenedSchema);

        // Cache the result
        if (enableCaching) {
            structTypeCache.put(cacheKey, sparkSchema);
        }

        return sparkSchema;
    }

    /**
     * Load multiple schemas with partial failure handling
     */
    public SchemaLoadResult loadFlattenedSchemas(String... schemaNames) {
        return loadFlattenedSchemas(Arrays.asList(schemaNames));
    }

    /**
     * Load multiple schemas with partial failure handling
     */
    public SchemaLoadResult loadFlattenedSchemas(List<String> schemaNames) {
        Map<String, StructType> successfulSchemas = new HashMap<>();
        Map<String, Exception> failedSchemas = new HashMap<>();

        for (String schemaName : schemaNames) {
            try {
                StructType schema = loadFlattenedSchema(schemaName);
                successfulSchemas.put(schemaName, schema);
                LOG.info("Successfully loaded schema: {}", schemaName);
            } catch (Exception e) {
                failedSchemas.put(schemaName, e);
                LOG.error("Failed to load schema: {}", schemaName, e);
            }
        }

        return new SchemaLoadResult(successfulSchemas, failedSchemas);
    }

    /**
     * Discover all available schemas in the search paths
     */
    public List<String> discoverAvailableSchemas() {
        Set<String> discoveredSchemas = new HashSet<>();

        for (String searchPath : searchPaths) {
            try {
                // Check if it's an HDFS path
                if (searchPath.startsWith("hdfs://") || searchPath.startsWith("s3://")) {
                    discoverSchemasInHdfs(searchPath, discoveredSchemas);
                } else {
                    // Local file system
                    discoverSchemasInLocalPath(searchPath, discoveredSchemas);
                }
            } catch (Exception e) {
                LOG.debug("Could not search in path: {} - {}", searchPath, e.getMessage());
            }
        }

        // Also check classpath
        discoverSchemasInClasspath(discoveredSchemas);

        return new ArrayList<>(discoveredSchemas);
    }

    /**
     * Get detailed information about where schemas are found
     */
    public Map<String, SchemaLocation> getSchemaLocations(List<String> schemaNames) throws IOException {
        Map<String, SchemaLocation> locations = new HashMap<>();

        for (String schemaName : schemaNames) {
            SchemaLocation location = findSchemaLocation(normalizeSchemaName(schemaName));
            if (location != null) {
                locations.put(schemaName, location);
            }
        }

        return locations;
    }

    /**
     * Clear all caches
     */
    public static void clearCaches() {
        schemaCache.clear();
        structTypeCache.clear();
        AvroSchemaFlattener.clearCache();
    }

    // Private helper methods

    private Schema findAndLoadSchema(String schemaName) throws IOException {
        // 1. First try using FileFinder if available
        try {
            InputStream is = FileFinder.findFile(schemaName);
            if (is != null) {
                LOG.info("Schema {} found via FileFinder", schemaName);
                return new Schema.Parser().parse(is);
            }
        } catch (Exception e) {
            LOG.debug("FileFinder could not locate schema: {}", schemaName);
        }

        // 2. Try each search path
        for (String basePath : searchPaths) {
            try {
                Schema schema = tryLoadFromPath(basePath, schemaName);
                if (schema != null) {
                    LOG.info("Schema {} found in: {}", schemaName, basePath);
                    return schema;
                }
            } catch (Exception e) {
                LOG.trace("Schema not found in {}: {}", basePath, e.getMessage());
            }
        }

        // 3. Try classpath as last resort
        try {
            Schema schema = loadFromClasspath(schemaName);
            if (schema != null) {
                LOG.info("Schema {} found in classpath", schemaName);
                return schema;
            }
        } catch (Exception e) {
            LOG.debug("Schema not found in classpath: {}", e.getMessage());
        }

        return null;
    }

    private Schema tryLoadFromPath(String basePath, String schemaName) throws IOException {
        // Handle different path types
        if (basePath.startsWith("hdfs://") || basePath.startsWith("s3://")) {
            return loadFromHdfs(basePath, schemaName);
        } else {
            return loadFromLocalFileSystem(basePath, schemaName);
        }
    }

    private Schema loadFromLocalFileSystem(String basePath, String schemaName) throws IOException {
        java.nio.file.Path schemaPath = Paths.get(basePath, schemaName);

        if (Files.exists(schemaPath)) {
            String content = new String(Files.readAllBytes(schemaPath));
            return new Schema.Parser().parse(content);
        }

        return null;
    }

    private Schema loadFromHdfs(String basePath, String schemaName) throws IOException {
        Path hdfsPath = new Path(basePath, schemaName);
        FileSystem fs = hdfsPath.getFileSystem(hadoopConf);

        if (fs.exists(hdfsPath)) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fs.open(hdfsPath)))) {
                StringBuilder content = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line).append("\n");
                }
                return new Schema.Parser().parse(content.toString());
            }
        }

        return null;
    }

    private Schema loadFromClasspath(String schemaName) throws IOException {
        // Try multiple classpath locations
        String[] classpathLocations = {
                schemaName,
                "schemas/" + schemaName,
                "/" + schemaName,
                "/schemas/" + schemaName
        };

        for (String location : classpathLocations) {
            InputStream is = getClass().getResourceAsStream(location);
            if (is == null) {
                is = Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream(location);
            }

            if (is != null) {
                try  {
                    return new Schema.Parser().parse(is);
                } catch (Exception e){
                    continue;
                }
            }
        }

        return null;
    }

    private String normalizeSchemaName(String schemaName) {
        // Ensure .avsc extension
        if (!schemaName.endsWith(".avsc")) {
            return schemaName + ".avsc";
        }
        return schemaName;
    }

    private void discoverSchemasInLocalPath(String path, Set<String> schemas) {
        File dir = new File(path);
        if (dir.exists() && dir.isDirectory()) {
            File[] avscFiles = dir.listFiles((d, name) -> name.endsWith(".avsc"));
            if (avscFiles != null) {
                for (File file : avscFiles) {
                    schemas.add(file.getName());
                }
            }
        }
    }

    private void discoverSchemasInHdfs(String path, Set<String> schemas) throws IOException {
        Path hdfsPath = new Path(path);
        FileSystem fs = hdfsPath.getFileSystem(hadoopConf);

        if (fs.exists(hdfsPath) && fs.isDirectory(hdfsPath)) {
            FileStatus[] statuses = fs.listStatus(hdfsPath);
            for (FileStatus status : statuses) {
                if (status.getPath().getName().endsWith(".avsc")) {
                    schemas.add(status.getPath().getName());
                }
            }
        }
    }

    private void discoverSchemasInClasspath(Set<String> schemas) {
        try {
            // This is a bit tricky - we'll check common locations
            for (String location : new String[]{"schemas", "/schemas"}) {
                InputStream is = getClass().getResourceAsStream(location);
                if (is != null) {
                    // If we can access the directory, try to list files
                    // This might not work in all environments (e.g., JARs)
                    LOG.debug("Found schema directory in classpath: {}", location);
                }
            }
        } catch (Exception e) {
            LOG.debug("Could not discover schemas in classpath: {}", e.getMessage());
        }
    }

    private SchemaLocation findSchemaLocation(String schemaName) throws IOException {
        // Check each location type
        for (String basePath : searchPaths) {
            try {
                if (basePath.startsWith("hdfs://") || basePath.startsWith("s3://")) {
                    Path hdfsPath = new Path(basePath, schemaName);
                    FileSystem fs = hdfsPath.getFileSystem(hadoopConf);
                    if (fs.exists(hdfsPath)) {
                        return new SchemaLocation(schemaName, basePath,
                                SchemaLocation.LocationType.HDFS, hdfsPath.toString());
                    }
                } else {
                    java.nio.file.Path localPath = Paths.get(basePath, schemaName);
                    if (Files.exists(localPath)) {
                        return new SchemaLocation(schemaName, basePath,
                                SchemaLocation.LocationType.LOCAL_FILE,
                                localPath.toAbsolutePath().toString());
                    }
                }
            } catch (Exception e) {
                LOG.trace("Error checking location {}: {}", basePath, e.getMessage());
            }
        }

        // Check classpath
        if (loadFromClasspath(schemaName) != null) {
            return new SchemaLocation(schemaName, "classpath",
                    SchemaLocation.LocationType.CLASSPATH, "classpath:/" + schemaName);
        }

        return null;
    }

    /**
     * Result of batch schema loading operation
     */
    public static class SchemaLoadResult {
        private final Map<String, StructType> successfulSchemas;
        private final Map<String, Exception> failedSchemas;

        public SchemaLoadResult(Map<String, StructType> successfulSchemas,
                                Map<String, Exception> failedSchemas) {
            this.successfulSchemas = Collections.unmodifiableMap(successfulSchemas);
            this.failedSchemas = Collections.unmodifiableMap(failedSchemas);
        }

        public Map<String, StructType> getSuccessfulSchemas() {
            return successfulSchemas;
        }

        public Map<String, Exception> getFailedSchemas() {
            return failedSchemas;
        }

        public boolean hasFailures() {
            return !failedSchemas.isEmpty();
        }

        public boolean isCompleteSuccess() {
            return failedSchemas.isEmpty();
        }

        public int getSuccessCount() {
            return successfulSchemas.size();
        }

        public int getFailureCount() {
            return failedSchemas.size();
        }

        public void logSummary() {
            LOG.info("Schema loading summary: {} successful, {} failed",
                    getSuccessCount(), getFailureCount());

            if (hasFailures()) {
                LOG.error("Failed schemas:");
                failedSchemas.forEach((name, exception) ->
                        LOG.error("  - {}: {}", name, exception.getMessage()));
            }
        }
    }

    /**
     * Information about where a schema was found
     */
    public static class SchemaLocation {
        public enum LocationType {
            LOCAL_FILE,
            HDFS,
            CLASSPATH,
            FILEFINDER
        }

        private final String schemaName;
        private final String basePath;
        private final LocationType locationType;
        private final String fullPath;

        public SchemaLocation(String schemaName, String basePath,
                              LocationType locationType, String fullPath) {
            this.schemaName = schemaName;
            this.basePath = basePath;
            this.locationType = locationType;
            this.fullPath = fullPath;
        }

        // Getters
        public String getSchemaName() { return schemaName; }
        public String getBasePath() { return basePath; }
        public LocationType getLocationType() { return locationType; }
        public String getFullPath() { return fullPath; }

        @Override
        public String toString() {
            return String.format("%s [%s] -> %s", schemaName, locationType, fullPath);
        }
    }

//    /**
//     * Static convenience methods for backward compatibility
//     */
//    public static StructType loadFlattenedSchema(String schemaPath) throws IOException {
//        return createDefault().loadFlattenedSchema(schemaPath);
//    }
//
//    public static Map<String, StructType> loadFlattenedSchemas(String basePath, String[] schemaNames)
//            throws IOException {
//        AvroSchemaLoader loader = new Builder()
//                .addSearchPath(basePath)
//                .build();
//
//        SchemaLoadResult result = loader.loadFlattenedSchemas(schemaNames);
//
//        if (result.hasFailures()) {
//            // For backward compatibility, throw exception on any failure
//            String failedSchemas = result.getFailedSchemas().keySet().stream()
//                    .collect(Collectors.joining(", "));
//            throw new IOException("Failed to load schemas: " + failedSchemas);
//        }
//
//        return result.getSuccessfulSchemas();
//    }
}