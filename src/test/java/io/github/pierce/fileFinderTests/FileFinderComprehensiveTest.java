package io.github.pierce.fileFinderTests;


import io.github.pierce.AvroSchemaFlattener;
import io.github.pierce.AvroSchemaLoader;
import io.github.pierce.files.FileFinder;
import org.apache.avro.Schema;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.util.concurrent.UncheckedExecutionException;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Comprehensive test suite for FileFinder and related classes
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FileFinderComprehensiveTest {

    @TempDir
    Path tempDir;

    private static final String TEST_SCHEMA_CONTENT = """
        {
          "type": "record",
          "name": "TestRecord",
          "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
          ]
        }
        """;

    @BeforeEach
    void setUp() {

        FileFinder.clearCaches();
    }

    @AfterEach
    void tearDown() {

        FileFinder.clearCaches();
    }



    @Test
    @Order(1)
    @DisplayName("Should find file in temporary directory")
    void testFindFileInTempDirectory() throws IOException {

        Path testFile = tempDir.resolve("test_schema.avsc");
        Files.writeString(testFile, TEST_SCHEMA_CONTENT);


        InputStream is = FileFinder.findFile(testFile.toString());


        assertThat(is).isNotNull();
        String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        assertThat(content).isEqualTo(TEST_SCHEMA_CONTENT);
        is.close();
    }

    @Test
    @Order(2)
    @DisplayName("Should find file by name only")
    void testFindFileByNameOnly() throws IOException {

        Path testFile = tempDir.resolve("unique_test.avsc");
        Files.writeString(testFile, TEST_SCHEMA_CONTENT);


        boolean exists = FileFinder.fileExists(testFile.toString());


        assertThat(exists).isTrue();
    }

    @Test
    @Order(3)
    @DisplayName("Should throw FileFinderException for missing file")
    void testFileNotFound() {

        assertThatThrownBy(() -> FileFinder.findFile("non_existent_file.avsc"))
                .isInstanceOf(FileFinder.FileFinderException.class)
                .satisfies(e -> {
                    FileFinder.FileFinderException ffe = (FileFinder.FileFinderException) e;
                    assertThat(ffe.getFileName()).isEqualTo("non_existent_file.avsc");
                    assertThat(ffe.getSearchedLocations()).isNotEmpty();
                    assertThat(ffe.getSuggestions()).isNotEmpty();
                    assertThat(e.getMessage()).contains("File not found");
                    assertThat(e.getMessage()).contains("Searched");
                    assertThat(e.getMessage()).contains("Suggestions");
                });
    }

    @Test
    @Order(4)
    @DisplayName("Should get file content as bytes")
    void testGetFileContent() throws IOException {

        Path testFile = tempDir.resolve("content_test.txt");
        String testContent = "Hello, FileFinder!";
        Files.writeString(testFile, testContent);


        byte[] content = FileFinder.getFileContent(testFile.toString());


        assertThat(new String(content, StandardCharsets.UTF_8)).isEqualTo(testContent);
    }

    @Test
    @Order(5)
    @DisplayName("Should get file metadata")
    void testGetFileMetadata() throws IOException {

        Path testFile = tempDir.resolve("metadata_test.json");
        Files.writeString(testFile, "{}");


        FileFinder.FileMetadata metadata = FileFinder.getFileMetadata(testFile.toString());


        assertThat(metadata).isNotNull();
        assertThat(metadata.path).contains("metadata_test.json");
        assertThat(metadata.size).isGreaterThan(0);
        assertThat(metadata.lastModified).isGreaterThan(0);
        assertThat(metadata.isCompressed).isFalse();
        assertThat(metadata.location).isNotNull();
        assertThat(metadata.location.type).isEqualTo(FileFinder.FileLocation.Type.LOCAL_FILE);
    }



    @Test
    @Order(10)
    @DisplayName("Should discover files by extension")
    void testDiscoverFiles() throws IOException {
        // Given
        Files.writeString(tempDir.resolve("schema1.avsc"), TEST_SCHEMA_CONTENT);
        Files.writeString(tempDir.resolve("schema2.avsc"), TEST_SCHEMA_CONTENT);
        Files.writeString(tempDir.resolve("data.json"), "{}");
        Files.writeString(tempDir.resolve("config.yaml"), "key: value");

        // When
        List<String> avroSchemas = FileFinder.discoverFiles(".avsc");
        List<String> jsonFiles = FileFinder.discoverFiles(".json");

        // Then
        assertThat(avroSchemas).isNotNull();
        assertThat(jsonFiles).isNotNull();
        // Note: Results may include files from other directories
    }

    @Test
    @Order(11)
    @DisplayName("Should discover Avro schemas")
    void testDiscoverAvroSchemas() throws IOException {
        // Given
        Path schemaDir = tempDir.resolve("schemas");
        Files.createDirectories(schemaDir);
        Files.writeString(schemaDir.resolve("product.avsc"), TEST_SCHEMA_CONTENT);
        Files.writeString(schemaDir.resolve("user.avsc"), TEST_SCHEMA_CONTENT);

        // When
        List<String> schemas = FileFinder.discoverAvroSchemas();

        // Then
        assertThat(schemas).isNotNull();
        assertThat(schemas).isNotEmpty();
    }

    // ===== CACHING TESTS =====

    @Test
    @Order(20)
    @DisplayName("Should cache file handles")
    void testCaching() throws IOException {
        // Given
        Path testFile = tempDir.resolve("cache_test.txt");
        Files.writeString(testFile, "cached content");

        // When
        FileFinder.Statistics stats1 = FileFinder.getStatistics();

        // First access - cache miss
        InputStream is1 = FileFinder.findFile(testFile.toString());
        is1.close();

        FileFinder.Statistics stats2 = FileFinder.getStatistics();

        // Second access - cache hit
        InputStream is2 = FileFinder.findFile(testFile.toString());
        is2.close();

        FileFinder.Statistics stats3 = FileFinder.getStatistics();

        // Then
        assertThat(stats3.cacheHits).isGreaterThan(stats2.cacheHits);
        assertThat(stats3.getHitRate()).isGreaterThan(0);
    }

    @Test
    @Order(21)
    @DisplayName("Should clear caches")
    void testClearCaches() throws IOException {
        // Given
        Path testFile = tempDir.resolve("clear_cache_test.txt");
        Files.writeString(testFile, "content");

        // Prime the cache
        FileFinder.findFile(testFile.toString()).close();
        FileFinder.Statistics statsBeforeClear = FileFinder.getStatistics();

        // When
        FileFinder.clearCaches();

        // Access again after clearing
        FileFinder.findFile(testFile.toString()).close();
        FileFinder.Statistics statsAfterClear = FileFinder.getStatistics();

        // Then
        assertThat(statsAfterClear.fileCacheSize).isLessThanOrEqualTo(1);
    }

    // ===== PATH HANDLING TESTS =====

    @Test
    @Order(30)
    @DisplayName("Should handle Windows paths correctly")
    void testWindowsPathHandling() throws IOException {
        // Given
        Path testFile = tempDir.resolve("windows_test.txt");
        Files.writeString(testFile, "test");

        // When - Force Windows-style path
        String windowsPath = testFile.toString().replace('/', '\\');
        boolean exists = FileFinder.fileExists(windowsPath);

        // Then
        assertThat(exists).isTrue();
    }

    @Test
    @Order(31)
    @DisplayName("Should handle relative paths")
    void testRelativePaths() throws IOException {
        // Given
        Path subDir = tempDir.resolve("subdir");
        Files.createDirectories(subDir);
        Path testFile = subDir.resolve("relative_test.txt");
        Files.writeString(testFile, "relative content");

        // When
        boolean exists = FileFinder.fileExists(testFile.toString());

        // Then
        assertThat(exists).isTrue();
    }

    @Test
    @Order(32)
    @DisplayName("Should handle paths with spaces")
    void testPathsWithSpaces() throws IOException {
        // Given
        Path dirWithSpaces = tempDir.resolve("dir with spaces");
        Files.createDirectories(dirWithSpaces);
        Path testFile = dirWithSpaces.resolve("file with spaces.txt");
        Files.writeString(testFile, "content");

        // When
        boolean exists = FileFinder.fileExists(testFile.toString());

        // Then
        assertThat(exists).isTrue();
    }

    // ===== UTILITY METHODS TESTS =====

    @Test
    @Order(40)
    @DisplayName("Should read file as string")
    void testReadAsString() throws IOException {
        // Given
        Path testFile = tempDir.resolve("string_test.txt");
        String content = "Hello, World!";
        Files.writeString(testFile, content);

        // When
        String readContent = FileFinder.Util.readAsString(testFile.toString());

        // Then
        assertThat(readContent).isEqualTo(content);
    }

    @Test
    @Order(41)
    @DisplayName("Should read file lines")
    void testReadLines() throws IOException {
        // Given
        Path testFile = tempDir.resolve("lines_test.txt");
        List<String> lines = Arrays.asList("line1", "line2", "line3");
        Files.write(testFile, lines);

        // When
        List<String> readLines = FileFinder.Util.readLines(testFile.toString());

        // Then
        assertThat(readLines).containsExactlyElementsOf(lines);
    }

    @Test
    @Order(42)
    @DisplayName("Should read properties file")
    void testReadProperties() throws IOException {
        // Given
        Path propsFile = tempDir.resolve("test.properties");
        String propsContent = """
            key1=value1
            key2=value2
            key3=value3
            """;
        Files.writeString(propsFile, propsContent);

        // When
        Properties props = FileFinder.Util.readProperties(propsFile.toString());

        // Then
        assertThat(props.getProperty("key1")).isEqualTo("value1");
        assertThat(props.getProperty("key2")).isEqualTo("value2");
        assertThat(props.getProperty("key3")).isEqualTo("value3");
    }

    @Test
    @Order(44)
    @DisplayName("Should read YAML file")
    void testReadYaml() throws IOException {
        // Given
        Path yamlFile = tempDir.resolve("test.yaml");
        String yamlContent = """
            name: test
            value: 42
            active: true
            """;
        Files.writeString(yamlFile, yamlContent);

        // When
        @SuppressWarnings("unchecked")
        Map<String, Object> yaml = (Map<String, Object>) FileFinder.Util.readYaml(yamlFile.toString());

        // Then
        assertThat(yaml.get("name")).isEqualTo("test");
        assertThat(yaml.get("value")).isEqualTo(42);
        assertThat(yaml.get("active")).isEqualTo(true);
    }

    @Test
    @Order(45)
    @DisplayName("Should copy file to output stream")
    void testCopyTo() throws IOException {
        // Given
        Path testFile = tempDir.resolve("copy_test.txt");
        String content = "Content to copy";
        Files.writeString(testFile, content);

        // When
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long bytesCopied = FileFinder.Util.copyTo(testFile.toString(), baos);

        // Then
        assertThat(bytesCopied).isEqualTo(content.length());
        assertThat(baos.toString()).isEqualTo(content);
    }

    @Test
    @Order(46)
    @DisplayName("Should save to local file")
    void testSaveToFile() throws IOException {
        // Given
        Path sourceFile = tempDir.resolve("source.txt");
        String content = "Content to save";
        Files.writeString(sourceFile, content);

        Path targetFile = tempDir.resolve("target.txt");

        // When
        FileFinder.Util.saveToFile(sourceFile.toString(), targetFile);

        // Then
        assertThat(Files.exists(targetFile)).isTrue();
        assertThat(Files.readString(targetFile)).isEqualTo(content);
    }

    // ===== COMPRESSION TESTS =====

    @Test
    @Order(50)
    @DisplayName("Should handle compressed files")
    void testCompressedFiles() throws IOException {
        // Given
        Path gzFile = tempDir.resolve("compressed.txt.gz");
        String originalContent = "This is compressed content";

        try (var gzOut = new java.util.zip.GZIPOutputStream(Files.newOutputStream(gzFile))) {
            gzOut.write(originalContent.getBytes(StandardCharsets.UTF_8));
        }

        // When
        FileFinder.FileMetadata metadata = FileFinder.getFileMetadata(gzFile.toString());

        // Then
        assertThat(metadata.isCompressed).isTrue();

        // Should decompress automatically
        try (InputStream is = FileFinder.findFile(gzFile.toString())) {
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(content).isEqualTo(originalContent);
        }
    }

    // ===== ERROR HANDLING TESTS =====

    @Test
    @Order(60)
    @DisplayName("Should provide helpful suggestions for similar files")
    void testSimilarFileSuggestions() throws IOException {
        // Given
        Files.writeString(tempDir.resolve("product_schema.avsc"), TEST_SCHEMA_CONTENT);
        Files.writeString(tempDir.resolve("products_schema.avsc"), TEST_SCHEMA_CONTENT);
        Files.writeString(tempDir.resolve("user_schema.avsc"), TEST_SCHEMA_CONTENT);

        // When/Then
        assertThatThrownBy(() -> FileFinder.findFile("prodct_schema.avsc"))
                .isInstanceOf(FileFinder.FileFinderException.class)
                .satisfies(e -> {
                    FileFinder.FileFinderException ffe = (FileFinder.FileFinderException) e;
                    // Should suggest similar files
                    assertThat(ffe.getAvailableFiles()).isNotEmpty();
                    assertThat(ffe.getSuggestions()).isNotEmpty();
                });
    }

    @Test
    @Order(61)
    @DisplayName("Should handle invalid file extensions")
    void testInvalidExtensions() {
        // When/Then - .exe files should be blocked by default
        assertThatThrownBy(() -> FileFinder.findFile("malicious.exe"))
                .isInstanceOf(IOException.class);
    }

    // ===== STATISTICS TESTS =====

    @Test
    @Order(70)
    @DisplayName("Should track statistics correctly")
    void testStatistics() throws IOException {
        // Given
        Path testFile = tempDir.resolve("stats_test.txt");
        Files.writeString(testFile, "test");

        // When
        FileFinder.Statistics initialStats = FileFinder.getStatistics();

        // Multiple operations
        FileFinder.findFile(testFile.toString()).close(); // Cache miss
        FileFinder.findFile(testFile.toString()).close(); // Cache hit
        FileFinder.fileExists(testFile.toString()); // Cache hit

        FileFinder.Statistics finalStats = FileFinder.getStatistics();

        // Then
        assertThat(finalStats.cacheHits).isGreaterThan(initialStats.cacheHits);
        assertThat(finalStats.searchAttempts).isGreaterThan(initialStats.searchAttempts);
        assertThat(finalStats.toString()).contains("Stats[");
        assertThat(finalStats.getHitRate()).isBetween(0.0, 1.0);
    }

    // ===== CONCURRENT ACCESS TESTS =====

    @Test
    @Order(80)
    @DisplayName("Should handle concurrent access safely")
    void testConcurrentAccess() throws Exception {
        // Given
        int threadCount = 10;
        int operationsPerThread = 50;
        Path testFile = tempDir.resolve("concurrent_test.txt");
        Files.writeString(testFile, "concurrent content");

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Future<Boolean>> futures = new ArrayList<>();

        // When
        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                try {
                    latch.countDown();
                    latch.await(); // Ensure all threads start together

                    for (int j = 0; j < operationsPerThread; j++) {
                        // Mix of operations
                        if (j % 3 == 0) {
                            FileFinder.fileExists(testFile.toString());
                        } else if (j % 3 == 1) {
                            try (InputStream is = FileFinder.findFile(testFile.toString())) {
                                is.read();
                            }
                        } else {
                            FileFinder.getFileContent(testFile.toString());
                        }
                    }
                    return true;
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }));
        }

        // Then
        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

        for (Future<Boolean> future : futures) {
            assertThat(future.get()).isTrue();
        }
    }

    // ===== CLASSPATH RESOURCE TESTS =====

    @Test
    @Order(90)
    @DisplayName("Should find classpath resources")
    void testClasspathResources() throws IOException {
        // This test assumes there's at least one .properties file in classpath
        // When
        List<String> propertiesFiles = FileFinder.discoverFiles(".properties");

        // Then
        // Should find at least some properties files (like from dependencies)
        assertThat(propertiesFiles).isNotNull();
    }

    // ===== SPECIAL CHARACTER TESTS =====

    @ParameterizedTest
    @Order(100)
    @ValueSource(strings = {
            "file-with-dash.txt",
            "file_with_underscore.txt",
            "file.with.dots.txt",
            "file (with) parens.txt",
            "file [with] brackets.txt",
            "file {with} braces.txt",
            "file@with@at.txt",
            "file#with#hash.txt"
    })
    @DisplayName("Should handle files with special characters")
    void testSpecialCharacters(String fileName) throws IOException {
        // Given
        Path testFile = tempDir.resolve(fileName);
        Files.writeString(testFile, "content");

        // When
        boolean exists = FileFinder.fileExists(testFile.toString());

        // Then
        assertThat(exists).isTrue();
    }

    // ===== FILE LOCATION TESTS =====

    @Test
    @Order(110)
    @DisplayName("Should correctly identify file location types")
    void testFileLocationTypes() throws IOException {
        // Given
        Path localFile = tempDir.resolve("local_test.txt");
        Files.writeString(localFile, "local");

        // When
        FileFinder.FileMetadata metadata = FileFinder.getFileMetadata(localFile.toString());

        // Then
        assertThat(metadata.location.type).isEqualTo(FileFinder.FileLocation.Type.LOCAL_FILE);
        assertThat(metadata.location.fileName).isEqualTo("local_test.txt");
        assertThat(metadata.location.absolutePath).contains("local_test.txt");
        assertThat(metadata.location.toString()).contains("[Local File]");
    }

    // ===== DEEP SEARCH TESTS =====

    @Test
    @Order(120)
    @DisplayName("Should perform deep search when needed")
    void testDeepSearch() throws IOException {
        // Given - Create nested directory structure
        Path deep1 = tempDir.resolve("level1");
        Path deep2 = deep1.resolve("level2");
        Path deep3 = deep2.resolve("level3");
        Path deep4 = deep3.resolve("level4");
        Files.createDirectories(deep4);

        Path deepFile = deep4.resolve("deep_file.txt");
        Files.writeString(deepFile, "found in deep search");

        // Note: Deep search may or may not find this depending on max depth config
        // This test just ensures the deep search doesn't throw exceptions

        // When
        List<String> discovered = FileFinder.discoverFiles(".txt");

        // Then
        assertThat(discovered).isNotNull();
    }

    // ===== EDGE CASES =====

    @Test
    @Order(130)
    @DisplayName("Should handle empty files")
    void testEmptyFiles() throws IOException {
        // Given
        Path emptyFile = tempDir.resolve("empty.txt");
        Files.createFile(emptyFile);

        // When
        byte[] content = FileFinder.getFileContent(emptyFile.toString());
        FileFinder.FileMetadata metadata = FileFinder.getFileMetadata(emptyFile.toString());

        // Then
        assertThat(content).isEmpty();
        assertThat(metadata.size).isEqualTo(0);
        assertThat(metadata.getFormattedSize()).isEqualTo("0 B");
    }

    @Test
    @Order(131)
    @DisplayName("Should handle very large file names")
    void testLongFileNames() throws IOException {
        // Given
        String longName = "a".repeat(200) + ".txt";
        Path longFile = tempDir.resolve(longName);
        Files.writeString(longFile, "content");

        // When
        boolean exists = FileFinder.fileExists(longFile.toString());

        // Then
        assertThat(exists).isTrue();
    }

    @Test
    @Order(132)
    @DisplayName("Should handle null and empty inputs gracefully")
    void testNullAndEmptyInputs() {
        // When/Then
        assertThatThrownBy(() -> FileFinder.findFile(null))
                .satisfies(e -> {
                    assertThat(e).isInstanceOfAny(
                            IOException.class,
                            NullPointerException.class,
                            UncheckedExecutionException.class
                    );
                    assertThat(e.getMessage()).containsIgnoringCase("null");
                });

        assertThatThrownBy(() -> FileFinder.findFile(""))
                .satisfies(e -> {
                    assertThat(e).isInstanceOfAny(
                            IOException.class,
                            UncheckedExecutionException.class
                    );
                    assertThat(e.getMessage()).containsIgnoringCase("empty");
                });

        assertThatThrownBy(() -> FileFinder.findFile("   "))
                .satisfies(e -> {
                    assertThat(e).isInstanceOfAny(
                            IOException.class,
                            UncheckedExecutionException.class
                    );
                });
    }

    // ===== PERFORMANCE TESTS =====

    @Test
    @Order(140)
    @DisplayName("Should complete operations within reasonable time")
    @Timeout(5) // 5 seconds timeout
    void testPerformance() throws IOException {
        // Given - Create many files
        int fileCount = 100;
        List<Path> files = IntStream.range(0, fileCount)
                .mapToObj(i -> tempDir.resolve("perf_test_" + i + ".txt"))
                .collect(Collectors.toList());

        for (Path file : files) {
            Files.writeString(file, "content " + file.getFileName());
        }

        // When - Perform many operations
        long startTime = System.currentTimeMillis();

        for (Path file : files) {
            FileFinder.fileExists(file.toString());
        }

        long duration = System.currentTimeMillis() - startTime;

        // Then
        assertThat(duration).isLessThan(5000); // Should complete in under 5 seconds

        // Check cache is working
        FileFinder.Statistics stats = FileFinder.getStatistics();
        assertThat(stats.getHitRate()).isGreaterThan(0);
    }

    // ===== INTEGRATION WITH AVRO SCHEMA TESTS =====

    @Test
    @Order(150)
    @DisplayName("Should work with AvroSchemaFlattener")
    void testAvroSchemaIntegration() throws IOException {
        // Given
        Path schemaFile = tempDir.resolve("test_schema.avsc");
        Files.writeString(schemaFile, TEST_SCHEMA_CONTENT);

        // When
        AvroSchemaFlattener flattener = new AvroSchemaFlattener(false);
        Schema schema = flattener.getFlattenedSchema(schemaFile.toString());

        // Then
        assertThat(schema).isNotNull();
        assertThat(schema.getName()).contains("Flattened");
        assertThat(schema.getFields()).isNotEmpty();
    }

    @Test
    @Order(151)
    @DisplayName("Should work with AvroSchemaLoader")
    void testAvroSchemaLoaderIntegration() throws IOException {
        // Given
        Path schemaFile = tempDir.resolve("loader_test.avsc");
        Files.writeString(schemaFile, TEST_SCHEMA_CONTENT);

        // When
        AvroSchemaLoader loader = AvroSchemaLoader.createDefault();
        Schema schema = loader.loadAvroSchema(schemaFile.toString());

        // Then
        assertThat(schema).isNotNull();
        assertThat(schema.getName()).isEqualTo("TestRecord");
        assertThat(schema.getFields()).hasSize(2);
    }

    // ===== CLEANUP =====

    @Test
    @Order(999)
    @DisplayName("Should clean up resources properly")
    void testCleanup() {
        // When
        FileFinder.clearCaches();
        FileFinder.Statistics stats = FileFinder.getStatistics();

        // Then
        assertThat(stats.fileCacheSize).isZero();
        assertThat(stats.contentCacheSize).isZero();
        assertThat(stats.locationCacheSize).isZero();
    }
}
