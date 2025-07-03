package io.github.pierce.files;

// FileFinderUtil.java - Utility methods

import org.json.JSONObject;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Utility methods for working with FileFinder
 */
public class FileFinderUtil {

    /**
     * Read file as string
     */
    public static String readFileAsString(String fileName) throws IOException {
        return readFileAsString(fileName, StandardCharsets.UTF_8.name());
    }

    /**
     * Read file as string with specific encoding
     */
    public static String readFileAsString(String fileName, String encoding) throws IOException {
        byte[] content = FileFinder.getFileContent(fileName);
        return new String(content, encoding);
    }

    /**
     * Read properties file
     */
    public static Properties readProperties(String fileName) throws IOException {
        Properties props = new Properties();
        try (InputStream is = FileFinder.findFile(fileName)) {
            props.load(is);
        }
        return props;
    }

    /**
     * Read JSON file
     */
    public static JSONObject readJson(String fileName) throws IOException {
        String content = readFileAsString(fileName);
        return new JSONObject(content);
    }

    /**
     * Read YAML file
     */
    public static Object readYaml(String fileName) throws IOException {
        try (InputStream is = FileFinder.findFile(fileName)) {
            Yaml yaml = new Yaml();
            return yaml.load(is);
        }
    }

    /**
     * Read file lines
     */
    public static List<String> readLines(String fileName) throws IOException {
        List<String> lines = new ArrayList<>();
        try (InputStream is = FileFinder.findFile(fileName);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }

    /**
     * Copy file to output stream
     */
    public static long copyTo(String fileName, OutputStream out) throws IOException {
        try (InputStream is = FileFinder.findFile(fileName)) {
            return is.transferTo(out);
        }
    }

    /**
     * Copy file to local path
     */
    public static void copyToFile(String fileName, String targetPath) throws IOException {
        try (OutputStream out = Files.newOutputStream(Paths.get(targetPath))) {
            copyTo(fileName, out);
        }
    }

    /**
     * Get file size
     */
    public static long getFileSize(String fileName) throws IOException {
        return FileFinder.getFileMetadata(fileName).size;
    }

    /**
     * Get file last modified time
     */
    public static long getLastModified(String fileName) throws IOException {
        return FileFinder.getFileMetadata(fileName).lastModified;
    }

    /**
     * Check if file is compressed
     */
    public static boolean isCompressed(String fileName) throws IOException {
        return FileFinder.getFileMetadata(fileName).isCompressed;
    }
}
