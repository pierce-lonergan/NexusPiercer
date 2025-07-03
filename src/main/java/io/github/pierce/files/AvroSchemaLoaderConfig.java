package io.github.pierce.files;


import io.github.pierce.AvroSchemaLoader;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Configuration helper for AvroSchemaLoader to support environment-specific settings
 */
public class AvroSchemaLoaderConfig {

    private static final String DEFAULT_CONFIG_FILE = "schema-loader.properties";

    // Property keys
    private static final String TARGET_DIRECTORY = "schema.loader.target.directory";
    private static final String SEARCH_PATHS = "schema.loader.search.paths";
    private static final String ARRAY_STATISTICS = "schema.loader.array.statistics";
    private static final String CACHING_ENABLED = "schema.loader.caching.enabled";
    private static final String HDFS_NAMENODE = "schema.loader.hdfs.namenode";
    private static final String S3_BUCKET = "schema.loader.s3.bucket";
    private static final String ENVIRONMENT = "schema.loader.environment";

    /**
     * Create loader from properties file
     */
    public static AvroSchemaLoader fromProperties(String propertiesFile) throws IOException {
        Properties props = loadProperties(propertiesFile);
        return createLoaderFromProperties(props);
    }

    /**
     * Create loader from default properties file
     */
    public static AvroSchemaLoader fromDefaultProperties() throws IOException {
        return fromProperties(DEFAULT_CONFIG_FILE);
    }

    /**
     * Create loader from environment variables
     */
    public static AvroSchemaLoader fromEnvironment() {
        AvroSchemaLoader.Builder builder = new AvroSchemaLoader.Builder();

        // Target directory
        String targetDir = System.getenv("SCHEMA_TARGET_DIR");
        if (targetDir != null) {
            builder.withTargetDirectory(targetDir);
        }

        // Search paths
        String searchPaths = System.getenv("SCHEMA_SEARCH_PATHS");
        if (searchPaths != null) {
            Arrays.stream(searchPaths.split(","))
                    .map(String::trim)
                    .forEach(builder::addSearchPath);
        }

        // Array statistics
        String arrayStats = System.getenv("SCHEMA_ARRAY_STATS");
        if (arrayStats != null) {
            builder.withArrayStatistics(Boolean.parseBoolean(arrayStats));
        }

        // Caching
        String caching = System.getenv("SCHEMA_CACHING");
        if (caching != null) {
            builder.withCaching(Boolean.parseBoolean(caching));
        }

        // Hadoop configuration
        String hdfsNamenode = System.getenv("HDFS_NAMENODE");
        if (hdfsNamenode != null) {
            Configuration hadoopConf = new Configuration();
            hadoopConf.set("fs.defaultFS", hdfsNamenode);
            builder.withHadoopConfiguration(hadoopConf);
        }

        return builder.build();
    }

    /**
     * Create environment-specific loader
     */
    public static AvroSchemaLoader forEnvironment(Environment env) {
        switch (env) {
            case LOCAL:
                return createLocalLoader();
            case DEVELOPMENT:
                return createDevelopmentLoader();
            case TEST:
                return createTestLoader();
            case STAGING:
                return createStagingLoader();
            case PRODUCTION:
                return createProductionLoader();
            default:
                throw new IllegalArgumentException("Unknown environment: " + env);
        }
    }

    private static Properties loadProperties(String filename) throws IOException {
        Properties props = new Properties();

        // Try to load from classpath
        try (InputStream is = AvroSchemaLoaderConfig.class
                .getClassLoader().getResourceAsStream(filename)) {
            if (is != null) {
                props.load(is);
            } else {
                throw new IOException("Properties file not found: " + filename);
            }
        }

        return props;
    }

    private static AvroSchemaLoader createLoaderFromProperties(Properties props) {
        AvroSchemaLoader.Builder builder = new AvroSchemaLoader.Builder();

        // Target directory
        String targetDir = props.getProperty(TARGET_DIRECTORY);
        if (targetDir != null && !targetDir.isEmpty()) {
            builder.withTargetDirectory(targetDir);
        }

        // Search paths
        String searchPaths = props.getProperty(SEARCH_PATHS);
        if (searchPaths != null && !searchPaths.isEmpty()) {
            Arrays.stream(searchPaths.split(","))
                    .map(String::trim)
                    .forEach(builder::addSearchPath);
        }

        // Array statistics
        builder.withArrayStatistics(
                Boolean.parseBoolean(props.getProperty(ARRAY_STATISTICS, "false"))
        );

        // Caching
        builder.withCaching(
                Boolean.parseBoolean(props.getProperty(CACHING_ENABLED, "true"))
        );

        // Hadoop configuration
        String hdfsNamenode = props.getProperty(HDFS_NAMENODE);
        if (hdfsNamenode != null && !hdfsNamenode.isEmpty()) {
            Configuration hadoopConf = new Configuration();
            hadoopConf.set("fs.defaultFS", hdfsNamenode);

            // Add S3 configuration if specified
            String s3Bucket = props.getProperty(S3_BUCKET);
            if (s3Bucket != null && !s3Bucket.isEmpty()) {
                // Configure S3 access (assuming credentials are in environment)
                hadoopConf.set("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"));
                hadoopConf.set("fs.s3a.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"));
                builder.addSearchPath("s3a://" + s3Bucket + "/schemas");
            }

            builder.withHadoopConfiguration(hadoopConf);
        }

        return builder.build();
    }

    private static AvroSchemaLoader createLocalLoader() {
        return new AvroSchemaLoader.Builder()
                .addSearchPath("schemas")
                .addSearchPath("src/main/resources/schemas")
                .addSearchPath("src/test/resources/schemas")
                .withArrayStatistics(true)
                .withCaching(false)
                .build();
    }

    private static AvroSchemaLoader createDevelopmentLoader() {
        return new AvroSchemaLoader.Builder()
                .withTargetDirectory("target/schemas")
                .addSearchPath("src/main/resources/schemas")
                .addSearchPath("schemas")
                .withArrayStatistics(true)
                .withCaching(false)
                .build();
    }

    private static AvroSchemaLoader createTestLoader() {
        return new AvroSchemaLoader.Builder()
                .withTargetDirectory("src/test/resources/test-schemas")
                .addSearchPath("src/test/resources/schemas")
                .addSearchPath("src/main/resources/schemas")
                .withArrayStatistics(true)
                .withCaching(true)
                .build();
    }

    private static AvroSchemaLoader createStagingLoader() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "hdfs://staging-namenode:9000");

        return new AvroSchemaLoader.Builder()
                .withTargetDirectory("/app/schemas/staging")
                .addSearchPath("hdfs://staging-namenode:9000/schemas")
                .addSearchPath("/app/schemas/backup")
                .withHadoopConfiguration(hadoopConf)
                .withArrayStatistics(false)
                .withCaching(true)
                .build();
    }

    private static AvroSchemaLoader createProductionLoader() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "hdfs://prod-namenode:9000");

        // Production S3 configuration
        hadoopConf.set("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"));
        hadoopConf.set("fs.s3a.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"));
        hadoopConf.set("fs.s3a.endpoint", "s3.amazonaws.com");

        return new AvroSchemaLoader.Builder()
                .withTargetDirectory("/app/schemas/current")
                .addSearchPath("hdfs://prod-namenode:9000/schemas/v2")
                .addSearchPath("hdfs://prod-namenode:9000/schemas/v1")
                .addSearchPath("s3a://production-schemas/current")
                .addSearchPath("/app/schemas/fallback")
                .withHadoopConfiguration(hadoopConf)
                .withArrayStatistics(false)
                .withCaching(true)
                .build();
    }

    /**
     * Environment enumeration
     */
    public enum Environment {
        LOCAL,
        DEVELOPMENT,
        TEST,
        STAGING,
        PRODUCTION;

        public static Environment fromString(String env) {
            if (env == null) {
                return LOCAL;
            }
            try {
                return valueOf(env.toUpperCase());
            } catch (IllegalArgumentException e) {
                return LOCAL;
            }
        }
    }

    /**
     * Example properties file content:
     *
     * # Schema Loader Configuration
     * schema.loader.environment=development
     * schema.loader.target.directory=/app/schemas/current
     * schema.loader.search.paths=/app/schemas/v1,/app/schemas/v2,hdfs://namenode:9000/schemas
     * schema.loader.array.statistics=false
     * schema.loader.caching.enabled=true
     * schema.loader.hdfs.namenode=hdfs://namenode:9000
     * schema.loader.s3.bucket=my-schema-bucket
     */

    /**
     * Usage examples
     */
    public static void main(String[] args) throws IOException {
        // Example 1: Load from environment
        AvroSchemaLoader envLoader = AvroSchemaLoaderConfig.fromEnvironment();

        // Example 2: Load from properties file
        AvroSchemaLoader propsLoader = AvroSchemaLoaderConfig.fromDefaultProperties();

        // Example 3: Load for specific environment
        String envName = System.getenv("APP_ENV");
        Environment env = Environment.fromString(envName);
        AvroSchemaLoader envSpecificLoader = AvroSchemaLoaderConfig.forEnvironment(env);

        // Example 4: Create custom configuration
        AvroSchemaLoader customLoader = new AvroSchemaLoader.Builder()
                .withTargetDirectory(System.getProperty("schema.dir", "schemas"))
                .addSearchPaths(getSearchPathsFromConfig())
                .withArrayStatistics(isDevelopmentMode())
                .withCaching(!isDevelopmentMode())
                .build();
    }

    private static List<String> getSearchPathsFromConfig() {
        String paths = System.getProperty("schema.search.paths", "schemas");
        return Arrays.stream(paths.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    private static boolean isDevelopmentMode() {
        String env = System.getProperty("app.env", "development");
        return "development".equalsIgnoreCase(env) || "local".equalsIgnoreCase(env);
    }
}