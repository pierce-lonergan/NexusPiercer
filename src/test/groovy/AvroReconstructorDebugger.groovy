package io.github.pierce;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Advanced debugging utility for AvroReconstructor
 *
 * Add this to your test to get detailed reconstruction traces:
 *
 * <pre>
 * AvroReconstructorDebugger debugger = new AvroReconstructorDebugger();
 * debugger.enableDetailedLogging(true);
 *
 * // During reconstruction, call:
 * debugger.logFieldExtraction("lineItems[0].product.attributes[0].name",
 *                             rawValue, extractedValue, schema);
 * </pre>
 */
public class AvroReconstructorDebugger {
    private static final Logger log = LoggerFactory.getLogger(AvroReconstructorDebugger.class);

    private boolean detailedLogging = false;
    private Map<String, List<FieldTrace>> traces = new LinkedHashMap<>();
    private Set<String> problemPaths = new LinkedHashSet<>();

    public static class FieldTrace {
        String path;
        String fieldName;
        Schema.Type expectedType;
        Object rawValue;
        Object extractedValue;
        Object convertedValue;
        boolean isNull;
        boolean shouldBeNull;
        String errorMessage;
        long timestamp;

        public FieldTrace(String path, String fieldName) {
            this.path = path;
            this.fieldName = fieldName;
            this.timestamp = System.nanoTime();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Path: %s\n", path));
            sb.append(String.format("  Field: %s (type: %s)\n", fieldName, expectedType));
            sb.append(String.format("  Raw value: %s (class: %s)\n",
                    rawValue, rawValue != null ? rawValue.getClass().getSimpleName() : "null"));
            sb.append(String.format("  Extracted: %s (class: %s)\n",
                    extractedValue, extractedValue != null ? extractedValue.getClass().getSimpleName() : "null"));
            sb.append(String.format("  Converted: %s (class: %s)\n",
                    convertedValue, convertedValue != null ? convertedValue.getClass().getSimpleName() : "null"));
            sb.append(String.format("  Is null: %s, Should be null: %s\n", isNull, shouldBeNull));
            if (errorMessage != null) {
                sb.append(String.format("  ❌ ERROR: %s\n", errorMessage));
            }
            return sb.toString();
        }
    }

    public void enableDetailedLogging(boolean enable) {
        this.detailedLogging = enable;
    }

    /**
     * Log a field extraction for debugging
     */
    public void logFieldExtraction(String path, String fieldName, Schema.Type expectedType,
                                   Object rawValue, Object extractedValue, Object convertedValue) {
        FieldTrace trace = new FieldTrace(path, fieldName);
        trace.expectedType = expectedType;
        trace.rawValue = rawValue;
        trace.extractedValue = extractedValue;
        trace.convertedValue = convertedValue;
        trace.isNull = convertedValue == null;

        traces.computeIfAbsent(path, k -> new ArrayList<>()).add(trace);

        if (detailedLogging) {
            log.info("Field Extraction: {}", trace);
        }

        // Flag potential issues
        if (convertedValue == null && expectedType != Schema.Type.NULL) {
            trace.errorMessage = "Unexpected null for non-nullable type";
            problemPaths.add(path);
        }
    }

    /**
     * Log when a field is about to be set on a record builder
     */
    public void logFieldSetting(String path, String fieldName, Object value,
                                boolean isRequired, boolean isNullable) {
        if (detailedLogging) {
            log.info("Setting field: {}.{} = {} (required: {}, nullable: {})",
                    path, fieldName, value, isRequired, isNullable);
        }

        if (value == null && isRequired && !isNullable) {
            String errorPath = path + "." + fieldName;
            problemPaths.add(errorPath);
            log.error("❌ PROBLEM: Attempting to set null on required non-nullable field: {}", errorPath);
        }
    }

    /**
     * Log array parsing details
     */
    public void logArrayParsing(String path, String arrayString, List<Object> parsedArray,
                                String format, boolean jsonFailed) {
        if (detailedLogging) {
            log.info("Array Parsing at {}", path);
            log.info("  Input: {}", arrayString);
            log.info("  Format: {}", format);
            log.info("  JSON failed: {}", jsonFailed);
            log.info("  Parsed size: {}", parsedArray != null ? parsedArray.size() : "null");
            if (parsedArray != null && parsedArray.size() <= 10) {
                log.info("  Parsed values: {}", parsedArray);
            }
        }
    }

    /**
     * Log index extraction from doubly nested arrays
     */
    public void logIndexExtraction(String path, Object rawValue, int outerIndex,
                                   int innerIndex, Object extracted) {
        if (detailedLogging) {
            log.info("Index Extraction at {}", path);
            log.info("  Raw value: {}", rawValue);
            log.info("  Outer index: {}, Inner index: {}", outerIndex, innerIndex);
            log.info("  Extracted: {}", extracted);
        }

        if (extracted == null && rawValue != null) {
            problemPaths.add(path);
            log.warn("⚠️ Extracted null from non-null raw value at {}", path);
        }
    }

    /**
     * Generate a comprehensive report of all traces
     */
    public String generateReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("═══════════════════════════════════════════════════════════════\n");
        sb.append("          AVRO RECONSTRUCTION DEBUGGING REPORT\n");
        sb.append("═══════════════════════════════════════════════════════════════\n\n");

        sb.append(String.format("Total paths traced: %d\n", traces.size()));
        sb.append(String.format("Problem paths found: %d\n\n", problemPaths.size()));

        if (!problemPaths.isEmpty()) {
            sb.append("❌ PROBLEM PATHS:\n");
            for (String path : problemPaths) {
                sb.append(String.format("  - %s\n", path));
            }
            sb.append("\n");
        }

        sb.append("DETAILED TRACES:\n");
        sb.append("─────────────────────────────────────────────────────────────\n");

        for (Map.Entry<String, List<FieldTrace>> entry : traces.entrySet()) {
            String path = entry.getKey();
            List<FieldTrace> pathTraces = entry.getValue();

            sb.append(String.format("\nPath: %s (%d traces)\n", path, pathTraces.size()));

            for (FieldTrace trace : pathTraces) {
                sb.append("  ").append(trace.toString().replace("\n", "\n  ")).append("\n");
            }
        }

        return sb.toString();
    }

    /**
     * Print summary of issues found
     */
    public void printSummary() {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("RECONSTRUCTION DEBUG SUMMARY");
        System.out.println("=".repeat(70));

        if (problemPaths.isEmpty()) {
            System.out.println("✅ No problems detected!");
        } else {
            System.out.println("❌ Found " + problemPaths.size() + " problem path(s):");
            for (String path : problemPaths) {
                System.out.println("  - " + path);
            }
        }

        System.out.println("=".repeat(70) + "\n");
    }

    /**
     * Create a visual tree of the reconstruction process
     */
    public String generateTreeView() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("RECONSTRUCTION TREE VIEW\n");
        sb.append("═══════════════════════════════════════════════════════════════\n\n");

        // Sort paths to create a tree structure
        List<String> sortedPaths = new ArrayList<>(traces.keySet());
        Collections.sort(sortedPaths);

        Map<Integer, String> depthPrefix = new HashMap<>();

        for (String path : sortedPaths) {
            int depth = countChar(path, '.') + countChar(path, '[');
            String prefix = "  ".repeat(depth);

            boolean hasProblems = problemPaths.contains(path);
            String marker = hasProblems ? "❌" : "✓";

            List<FieldTrace> pathTraces = traces.get(path);
            int nullCount = (int) pathTraces.stream().filter(t -> t.isNull).count();

            sb.append(String.format("%s%s %s (fields: %d, nulls: %d)\n",
                    prefix, marker, simplifyPath(path), pathTraces.size(), nullCount));
        }

        return sb.toString();
    }

    private int countChar(String str, char c) {
        return (int) str.chars().filter(ch -> ch == c).count();
    }

    private String simplifyPath(String path) {
        // Simplify long paths for readability
        if (path.length() > 60) {
            int lastDot = path.lastIndexOf('.');
            if (lastDot > 0) {
                return "..." + path.substring(lastDot);
            }
        }
        return path;
    }
}