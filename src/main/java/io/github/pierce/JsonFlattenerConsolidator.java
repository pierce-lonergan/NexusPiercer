package io.github.pierce;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonFlattenerConsolidator implements Serializable {
    private static final long serialVersionUID = 1L;


    // Configuration
    private final String arrayDelimiter;
    private final String nullPlaceholder;
    private final int maxNestingDepth;
    private final int maxArraySize;
    private final boolean consolidateWithMatrixDenotorsInValue;
    private final boolean gatherStatistics;

    // Array explosion configuration
    private final Set<String> explosionPaths;
    private final boolean explosionEnabled;

    // Track which fields were originally arrays
    private final Set<String> arrayFields = new HashSet<>();

    // Patterns for consolidation
    private static final Pattern ARRAY_INDEX_STRIP_PATTERN = Pattern.compile("\\[\\d+\\]");
    private static final Pattern ALL_INDICES_PATTERN = Pattern.compile("\\[(\\d+)\\]");
    private static final Pattern MALFORMED_JSON_PATTERN = Pattern.compile("[:,\\[]\\s*(undefined|NaN)\\s*[,\\}\\]]");
    private static final Pattern ARRAY_INDEX_PATTERN = Pattern.compile("(.+?)\\[(\\d+)\\](.*)");

    // New constructor with explosion paths
    public JsonFlattenerConsolidator(String arrayDelimiter, String nullPlaceholder,
                                     int maxNestingDepth, int maxArraySize,
                                     boolean consolidateWithMatrixDenotorsInValue,
                                     boolean gatherStatistics,
                                     String... explosionPaths) {
        this.arrayDelimiter = arrayDelimiter != null ? arrayDelimiter : ",";
        this.nullPlaceholder = nullPlaceholder;
        this.maxNestingDepth = maxNestingDepth > 0 ? maxNestingDepth : 50;
        this.maxArraySize = maxArraySize > 0 ? maxArraySize : 1000;
        this.consolidateWithMatrixDenotorsInValue = consolidateWithMatrixDenotorsInValue;
        this.gatherStatistics = gatherStatistics;
        this.explosionPaths = new HashSet<>(Arrays.asList(explosionPaths));
        this.explosionEnabled = explosionPaths.length > 0;
    }

    // Backward compatibility constructor (statistics enabled by default)
    public JsonFlattenerConsolidator(String arrayDelimiter, String nullPlaceholder,
                                     int maxNestingDepth, int maxArraySize,
                                     boolean consolidateWithMatrixDenotorsInValue) {
        this(arrayDelimiter, nullPlaceholder, maxNestingDepth, maxArraySize,
                consolidateWithMatrixDenotorsInValue, true);
    }

    // Backward compatibility constructor (no explosion)
    public JsonFlattenerConsolidator(String arrayDelimiter, String nullPlaceholder,
                                     int maxNestingDepth, int maxArraySize,
                                     boolean consolidateWithMatrixDenotorsInValue,
                                     boolean gatherStatistics) {
        this(arrayDelimiter, nullPlaceholder, maxNestingDepth, maxArraySize,
                consolidateWithMatrixDenotorsInValue, gatherStatistics, new String[0]);
    }

    /**
     * Main method to flatten and consolidate JSON (unchanged behavior)
     * Returns a JSON string that can be parsed by Spark's from_json
     */
    public String flattenAndConsolidateJson(String jsonString) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            return "{}";
        }

        try {
            // More aggressive trimming to handle text blocks and whitespace
            String trimmed = jsonString.trim();

            // Remove potential BOM (Byte Order Mark) characters
            if (trimmed.startsWith("\uFEFF")) {
                trimmed = trimmed.substring(1);
            }

            // Ensure we have a JSON object
            if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
                System.err.println("JSON validation failed: doesn't start/end with braces");
                System.err.println("First char: '" + (trimmed.isEmpty() ? "" : trimmed.charAt(0)) + "'");
                System.err.println("Last char: '" + (trimmed.isEmpty() ? "" : trimmed.charAt(trimmed.length()-1)) + "'");
                return "{}";
            }

            // Make the malformed pattern check more specific
            if (trimmed.contains(": undefined") || trimmed.contains(": NaN")) {
                System.err.println("JSON validation failed: contains undefined or NaN");
                return "{}";
            }

            JSONObject jsonObject = new JSONObject(trimmed);

            // Clear array fields tracking for this run
            arrayFields.clear();

            // Step 1: Flatten the JSON
            Map<String, Object> flattened = flattenJson(jsonObject);

            // Step 2: Consolidate the flattened data
            Map<String, Object> consolidated = consolidateFlattened(flattened);

            // Step 3: Convert to JSON with underscored keys
            JSONObject result = new JSONObject();
            for (Map.Entry<String, Object> entry : consolidated.entrySet()) {
                result.put(entry.getKey(), entry.getValue());
            }

            return result.toString();

        } catch (Exception e) {
            // More detailed error logging
            System.err.println("Error processing JSON: " + e.getMessage());
            e.printStackTrace();
            return "{}";
        }
    }

    /**
     * New method to flatten and explode JSON based on specified paths
     * Returns a list of JSON strings, one for each exploded record
     */
    public List<String> flattenAndExplodeJson(String jsonString) {
        if (!explosionEnabled) {
            // If no explosion paths specified, return single consolidated record
            return Collections.singletonList(flattenAndConsolidateJson(jsonString));
        }

        if (jsonString == null || jsonString.trim().isEmpty()) {
            return Collections.singletonList("{}");
        }

        try {
            String trimmed = preprocessJson(jsonString);
            if (trimmed.equals("{}")) {
                return Collections.singletonList("{}");
            }

            JSONObject jsonObject = new JSONObject(trimmed);
            arrayFields.clear();

            // Step 1: Use SPECIAL flattening that doesn't consolidate arrays in explosion paths
            Map<String, Object> flattened = flattenJsonForExplosion(jsonObject);

            // Step 2: Perform explosion on the flattened data
            List<Map<String, Object>> explodedRecords = performExplosionOnFlattened(flattened);

            // Step 3: Consolidate each exploded record separately
            List<String> results = new ArrayList<>();
            for (Map<String, Object> record : explodedRecords) {
                Map<String, Object> consolidated = consolidateFlattened(record);
                JSONObject result = new JSONObject();
                for (Map.Entry<String, Object> entry : consolidated.entrySet()) {
                    result.put(entry.getKey(), entry.getValue());
                }
                results.add(result.toString());
            }

            return results;

        } catch (Exception e) {
            System.err.println("Error in explosion processing: " + e.getMessage());
            e.printStackTrace();
            return Collections.singletonList("{}");
        }
    }

    /**
     * Updated performExplosionOnFlattened with debugging
     */
    private List<Map<String, Object>> performExplosionOnFlattened(Map<String, Object> flattened) {
        if (explosionPaths.isEmpty()) {
            return Collections.singletonList(flattened);
        }

        // Debug: Print what we have
        System.err.println("=== performExplosionOnFlattened ===");
        System.err.println("Explosion paths: " + explosionPaths);
        System.err.println("Flattened data sample (first 10 keys):");
        int count = 0;
        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            System.err.println("  " + entry.getKey() + " = " + entry.getValue());
            if (++count >= 10) break;
        }

        List<Map<String, Object>> currentRecords = Collections.singletonList(new LinkedHashMap<>(flattened));

        // Process each explosion path
        for (String explosionPath : explosionPaths) {
            List<Map<String, Object>> nextRecords = new ArrayList<>();

            for (Map<String, Object> record : currentRecords) {
                List<Map<String, Object>> exploded = explodeFlattened(record, explosionPath);
                nextRecords.addAll(exploded);
            }

            currentRecords = nextRecords;
        }

        return currentRecords;
    }

    /**
     * The real fix - handle the fact that dots might already be underscores
     */
    private List<Map<String, Object>> explodeFlattened(Map<String, Object> flattened, String explosionPath) {
        // IMPORTANT: At this point, the flattened data still has dots, not underscores!
        // Underscores only appear after consolidation.

        String[] pathParts = explosionPath.split("\\.");

        // Group records by their indices at the explosion level
        Map<String, Map<String, Object>> recordGroups = new LinkedHashMap<>();
        Map<String, Object> nonArrayFields = new LinkedHashMap<>();

        // Pattern to match the explosion path with indices
        StringBuilder pattern = new StringBuilder("^");
        for (int i = 0; i < pathParts.length; i++) {
            if (i > 0) pattern.append("\\.");
            pattern.append(Pattern.quote(pathParts[i]));

            // After each path part, there might be array indices
            if (i < pathParts.length - 1) {
                // Before explosion level - optional indices
                pattern.append("(?:\\[(\\d+)\\])?");
            } else {
                // At explosion level - required index
                pattern.append("\\[(\\d+)\\]");
            }
        }
        pattern.append("(.*)$"); // Rest of the path

        Pattern explosionPattern = Pattern.compile(pattern.toString());

        // First pass: find explosion fields and group them
        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            String key = entry.getKey();
            Matcher m = explosionPattern.matcher(key);

            if (m.matches()) {
                // This belongs to the explosion path
                StringBuilder groupKey = new StringBuilder();

                // Collect all captured indices
                for (int i = 1; i <= pathParts.length; i++) {
                    String idx = m.group(i);
                    if (idx != null) {
                        if (groupKey.length() > 0) groupKey.append("_");
                        groupKey.append(idx);
                    }
                }

                recordGroups.computeIfAbsent(groupKey.toString(), k -> new LinkedHashMap<>())
                        .put(key, entry.getValue());
            }
        }

        // Second pass: categorize all fields
        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            String key = entry.getKey();

            // Skip if already in a record group
            boolean inGroup = false;
            for (Map<String, Object> group : recordGroups.values()) {
                if (group.containsKey(key)) {
                    inGroup = true;
                    break;
                }
            }
            if (inGroup) continue;

            // Check if this is a parent field that should be included in specific groups
            boolean isParentField = false;

            // Try to match parent patterns at different levels
            for (int parentLevel = 0; parentLevel < pathParts.length; parentLevel++) {
                StringBuilder parentPattern = new StringBuilder("^");

                // Build pattern up to parent level
                for (int i = 0; i <= parentLevel; i++) {
                    if (i > 0) parentPattern.append("\\.");
                    parentPattern.append(Pattern.quote(pathParts[i]));

                    // Capture optional indices
                    parentPattern.append("(?:\\[(\\d+)\\])?");
                }

                // Should have a field name after this that's NOT the next path part
                parentPattern.append("\\.([^\\[.]+)");

                // Might have more structure after the field
                parentPattern.append(".*$");

                Pattern pp = Pattern.compile(parentPattern.toString());
                Matcher pm = pp.matcher(key);

                if (pm.matches()) {
                    // Get the field name to check it's not continuing the explosion path
                    String fieldName = pm.group(parentLevel + 2);

                    // If this field name is the next part of the explosion path, skip it
                    if (parentLevel + 1 < pathParts.length && fieldName.equals(pathParts[parentLevel + 1])) {
                        continue;
                    }

                    // This is a valid parent field
                    isParentField = true;

                    // Build parent key from indices
                    StringBuilder parentKey = new StringBuilder();
                    for (int i = 1; i <= parentLevel + 1; i++) {
                        String idx = pm.group(i);
                        if (idx != null) {
                            if (parentKey.length() > 0) parentKey.append("_");
                            parentKey.append(idx);
                        }
                    }

                    // Add to all groups that match this parent key
                    String parentKeyStr = parentKey.toString();
                    for (Map.Entry<String, Map<String, Object>> group : recordGroups.entrySet()) {
                        String groupKey = group.getKey();

                        // Check if group key starts with parent key
                        if (parentKeyStr.isEmpty() || groupKey.startsWith(parentKeyStr)) {
                            group.getValue().put(key, entry.getValue());
                        }
                    }
                    break;
                }
            }

            if (!isParentField) {
                // Non-array field or unrelated field
                nonArrayFields.put(key, entry.getValue());
            }
        }

        // If no groups found, return original
        if (recordGroups.isEmpty()) {
            return Collections.singletonList(flattened);
        }

        // Create exploded records
        List<Map<String, Object>> results = new ArrayList<>();
        int explosionIndex = 0;

        // Sort groups for consistent ordering
        List<String> sortedGroupKeys = new ArrayList<>(recordGroups.keySet());
        Collections.sort(sortedGroupKeys);

        for (String groupKey : sortedGroupKeys) {
            Map<String, Object> groupFields = recordGroups.get(groupKey);
            String[] indices = groupKey.split("_");

            Map<String, Object> newRecord = new LinkedHashMap<>(nonArrayFields);

            // Add all fields from this group, transforming keys
            for (Map.Entry<String, Object> field : groupFields.entrySet()) {
                String oldKey = field.getKey();
                String newKey = oldKey;

                // Remove indices for the explosion path
                for (int i = 0; i < pathParts.length; i++) {
                    // Only remove the index at the explosion level
                    if (i == pathParts.length - 1 && i < indices.length) {
                        String searchPattern = pathParts[i] + "[" + indices[i] + "]";
                        String replacement = pathParts[i];
                        newKey = newKey.replace(searchPattern, replacement);
                    }
                }

                newRecord.put(newKey, field.getValue());
            }

            // Add explosion index
            newRecord.put(explosionPath + "_explosion_index", (long) explosionIndex++);

            results.add(newRecord);
        }

        return results;
    }

    /**
     * Special flattening that preserves arrays in explosion paths
     */
    private Map<String, Object> flattenJsonForExplosion(JSONObject jsonObject) {
        Map<String, Object> flattenedOutput = new LinkedHashMap<>();
        ArrayDeque<FlattenTask> taskQueue = new ArrayDeque<>();

        // Initialize queue with top-level entries
        Iterator<String> keys = jsonObject.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object value = jsonObject.get(key);
            taskQueue.add(new FlattenTask(key, value, 1));
        }

        // Process queue
        while (!taskQueue.isEmpty()) {
            FlattenTask currentTask = taskQueue.pollFirst();

            if (currentTask.depth > maxNestingDepth) {
                flattenedOutput.put(currentTask.prefix, safeToString(currentTask.value));
                continue;
            }

            if (currentTask.value == null || currentTask.value == JSONObject.NULL) {
                flattenedOutput.put(currentTask.prefix, nullPlaceholder);
            } else if (currentTask.value instanceof JSONObject) {
                JSONObject obj = (JSONObject) currentTask.value;
                if (obj.length() == 0) {
                    flattenedOutput.put(currentTask.prefix, nullPlaceholder);
                } else if (currentTask.depth == maxNestingDepth) {
                    flattenedOutput.put(currentTask.prefix, obj.toString());
                } else {
                    Iterator<String> objKeys = obj.keys();
                    while (objKeys.hasNext()) {
                        String key = objKeys.next();
                        Object val = obj.get(key);
                        String newPrefix = currentTask.prefix + "." + key;
                        taskQueue.add(new FlattenTask(newPrefix, val, currentTask.depth + 1));
                    }
                }
            } else if (currentTask.value instanceof JSONArray) {
                JSONArray array = (JSONArray) currentTask.value;
                if (array.length() == 0) {
                    flattenedOutput.put(currentTask.prefix, nullPlaceholder);
                    arrayFields.add(currentTask.prefix);
                } else {
                    // Check if we should keep this array as individual elements
                    boolean shouldKeepElements = shouldKeepAsArrayElements(currentTask.prefix);

                    // Check if all elements are primitives
                    boolean allPrimitives = true;
                    for (int i = 0; i < array.length(); i++) {
                        Object item = array.get(i);
                        if (item instanceof JSONObject || item instanceof JSONArray) {
                            allPrimitives = false;
                            break;
                        }
                    }

                    if (allPrimitives && !shouldKeepElements) {
                        // Primitive array NOT in explosion path - consolidate
                        StringBuilder sb = new StringBuilder();
                        int limit = Math.min(array.length(), maxArraySize);
                        for (int i = 0; i < limit; i++) {
                            if (i > 0) sb.append(arrayDelimiter);
                            Object item = array.get(i);
                            if (item == null || item == JSONObject.NULL) {
                                sb.append(nullPlaceholder != null ? nullPlaceholder : "");
                            } else {
                                if (item instanceof BigDecimal) {
                                    item = ((BigDecimal) item).doubleValue();
                                }
                                sb.append(item.toString());
                            }
                        }
                        flattenedOutput.put(currentTask.prefix, sb.toString());
                        arrayFields.add(currentTask.prefix);
                    } else {
                        // Keep as individual elements (either complex array or in explosion path)
                        int limit = Math.min(array.length(), maxArraySize);
                        for (int i = 0; i < limit; i++) {
                            Object item = array.get(i);
                            String newPrefix = currentTask.prefix + "[" + i + "]";
                            taskQueue.add(new FlattenTask(newPrefix, item, currentTask.depth + 1));
                        }
                        arrayFields.add(currentTask.prefix);
                    }
                }
            } else {
                // Primitive value
                Object val = currentTask.value;
                if (val instanceof BigDecimal) {
                    val = ((BigDecimal) val).doubleValue();
                }
                flattenedOutput.put(currentTask.prefix, val);
            }
        }

        return flattenedOutput;
    }


    /**
     * Check if we should use the special flattening for explosion
     * This is the key - we need to ensure arrays in explosion paths aren't consolidated
     */
    private boolean shouldKeepAsArrayElements(String currentPath) {
        // Debug
        System.err.println("shouldKeepAsArrayElements: checking '" + currentPath + "'");

        // Normalize the current path by removing array indices
        String normalizedPath = currentPath.replaceAll("\\[\\d+\\]", "");

        for (String explosionPath : explosionPaths) {
            // Direct match
            if (normalizedPath.equals(explosionPath)) {
                System.err.println("  -> YES (direct match with " + explosionPath + ")");
                return true;
            }

            // Check if this path is part of a larger explosion path
            String[] explosionParts = explosionPath.split("\\.");
            String[] currentParts = normalizedPath.split("\\.");

            // If current path is a prefix of explosion path
            if (currentParts.length <= explosionParts.length) {
                boolean isPrefix = true;
                for (int i = 0; i < currentParts.length; i++) {
                    if (!currentParts[i].equals(explosionParts[i])) {
                        isPrefix = false;
                        break;
                    }
                }
                if (isPrefix) {
                    System.err.println("  -> YES (prefix of " + explosionPath + ")");
                    return true;
                }
            }
        }

        System.err.println("  -> NO");
        return false;
    }


    /**
     * Preprocess JSON string for validation
     */
    private String preprocessJson(String jsonString) {
        String trimmed = jsonString.trim();

        // Remove potential BOM characters
        if (trimmed.startsWith("\uFEFF")) {
            trimmed = trimmed.substring(1);
        }

        // Validate JSON structure
        if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
            return "{}";
        }

        if (trimmed.contains(": undefined") || trimmed.contains(": NaN")) {
            return "{}";
        }

        return trimmed;
    }

    /**
     * Flatten JSON following the same logic as the Groovy implementation
     */
    private Map<String, Object> flattenJson(JSONObject jsonObject) {
        Map<String, Object> flattenedOutput = new LinkedHashMap<>();
        ArrayDeque<FlattenTask> taskQueue = new ArrayDeque<>();

        // Initialize queue with top-level entries
        Iterator<String> keys = jsonObject.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object value = jsonObject.get(key);
            taskQueue.add(new FlattenTask(key, value, 1)); // Start depth at 1
        }

        // Process queue
        while (!taskQueue.isEmpty()) {
            FlattenTask currentTask = taskQueue.pollFirst();

            if (currentTask.depth > maxNestingDepth) {
                // Too deep - convert to string
                flattenedOutput.put(currentTask.prefix, safeToString(currentTask.value));
                continue;
            }

            if (currentTask.value == null || currentTask.value == JSONObject.NULL) {
                flattenedOutput.put(currentTask.prefix, nullPlaceholder);
            } else if (currentTask.value instanceof JSONObject) {
                JSONObject obj = (JSONObject) currentTask.value;
                if (obj.length() == 0) {
                    flattenedOutput.put(currentTask.prefix, nullPlaceholder);
                } else if (currentTask.depth == maxNestingDepth) {
                    // At max depth - convert object to string
                    flattenedOutput.put(currentTask.prefix, obj.toString());
                } else {
                    Iterator<String> objKeys = obj.keys();
                    while (objKeys.hasNext()) {
                        String key = objKeys.next();
                        Object val = obj.get(key);
                        String newPrefix = currentTask.prefix + "." + key;
                        taskQueue.add(new FlattenTask(newPrefix, val, currentTask.depth + 1));
                    }
                }
            } else if (currentTask.value instanceof JSONArray) {
                JSONArray array = (JSONArray) currentTask.value;
                if (array.length() == 0) {
                    flattenedOutput.put(currentTask.prefix, nullPlaceholder);
                    arrayFields.add(currentTask.prefix);
                } else {
                    // Check if all elements are primitives
                    boolean allPrimitives = true;
                    for (int i = 0; i < array.length(); i++) {
                        Object item = array.get(i);
                        if (item instanceof JSONObject || item instanceof JSONArray) {
                            allPrimitives = false;
                            break;
                        }
                    }

                    if (allPrimitives) {
                        // Simple array - concatenate values
                        StringBuilder sb = new StringBuilder();
                        int limit = Math.min(array.length(), maxArraySize);
                        for (int i = 0; i < limit; i++) {
                            if (i > 0) sb.append(arrayDelimiter);
                            Object item = array.get(i);
                            if (item == null || item == JSONObject.NULL) {
                                sb.append(nullPlaceholder != null ? nullPlaceholder : "");
                            } else {
                                // Convert BigDecimal to double for consistent string representation
                                if (item instanceof BigDecimal) {
                                    item = ((BigDecimal) item).doubleValue();
                                }
                                sb.append(item.toString());
                            }
                        }
                        flattenedOutput.put(currentTask.prefix, sb.toString());
                        // Track that this was an array field
                        arrayFields.add(currentTask.prefix);
                    } else {
                        // Complex array - process each element
                        int limit = Math.min(array.length(), maxArraySize);
                        for (int i = 0; i < limit; i++) {
                            Object item = array.get(i);
                            String newPrefix = currentTask.prefix + "[" + i + "]";
                            taskQueue.add(new FlattenTask(newPrefix, item, currentTask.depth + 1));
                        }
                        // Track that this prefix represents an array
                        arrayFields.add(currentTask.prefix);
                    }
                }
            } else {
                // Primitive value - preserve original type
                Object val = currentTask.value;
                // Convert BigDecimal to double for consistent representation
                if (val instanceof BigDecimal) {
                    val = ((BigDecimal) val).doubleValue();
                }
                // Don't convert to string here - preserve the original type
                flattenedOutput.put(currentTask.prefix, val);
            }
        }

        return flattenedOutput;
    }

    /**
     * Consolidate flattened data following the Groovy logic
     */
    private Map<String, Object> consolidateFlattened(Map<String, Object> flattened) {
        // Group by base key (strip array indices and replace dots with underscores)
        Map<String, List<KeyedValue>> groupedByBaseKey = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            String flattenedKey = entry.getKey();
            Object value = entry.getValue();

            // Calculate base key
            String baseKey = ARRAY_INDEX_STRIP_PATTERN.matcher(flattenedKey).replaceAll("");
            baseKey = baseKey.replace('.', '_');

            // Check if it has array index
            boolean hasArrayIndex = flattenedKey.contains("[");

            // Add to grouped data
            List<KeyedValue> list = groupedByBaseKey.computeIfAbsent(baseKey, k -> new ArrayList<>());
            list.add(new KeyedValue(flattenedKey, value, hasArrayIndex));
        }

        // Process each group
        Map<String, Object> consolidatedOutput = new LinkedHashMap<>();
        for (Map.Entry<String, List<KeyedValue>> entry : groupedByBaseKey.entrySet()) {
            processKeyGroup(entry.getKey(), entry.getValue(), consolidatedOutput);
        }

        return consolidatedOutput;
    }

    /**
     * Process a group of values for the same base key
     */
    private void processKeyGroup(String consolidatedKey, List<KeyedValue> keyedValues,
                                 Map<String, Object> consolidatedOutput) {
        // Check if any value had array indices
        boolean wasOriginallyArray = keyedValues.stream().anyMatch(kv -> kv.hasArrayIndex);

        // Also check if the original field was tracked as an array field
        boolean wasTrackedAsArray = false;
        for (KeyedValue kv : keyedValues) {
            // Need to convert the flattened key to base key for checking
            String baseKey = ARRAY_INDEX_STRIP_PATTERN.matcher(kv.originalFlattenedKey).replaceAll("");
            if (arrayFields.contains(baseKey) || arrayFields.contains(kv.originalFlattenedKey)) {
                wasTrackedAsArray = true;
                break;
            }
        }

        // Filter valid values (non-null and not null placeholder)
        List<KeyedValue> validKeyedValues = new ArrayList<>();
        for (KeyedValue kv : keyedValues) {
            if (kv.value != null && !kv.value.equals(nullPlaceholder)) {
                validKeyedValues.add(kv);
            }
        }

        // Determine if we should generate array statistics
        boolean shouldGenerateArrayStats = gatherStatistics && (wasOriginallyArray || wasTrackedAsArray);

        if (validKeyedValues.isEmpty()) {
            // All values were null
            consolidatedOutput.put(consolidatedKey, nullPlaceholder);
            if (shouldGenerateArrayStats) {
                consolidatedOutput.put(consolidatedKey + "_count", 0L);
                consolidatedOutput.put(consolidatedKey + "_distinct_count", 0L);
            }
        } else if (validKeyedValues.size() == 1 && !wasOriginallyArray) {
            // Single value case
            KeyedValue single = validKeyedValues.get(0);
            String value = single.value.toString();

            // Check if this single value is actually a consolidated array
            if (wasTrackedAsArray && value.contains(arrayDelimiter)) {
                // This is a flattened array - process as array
                processArrayValues(consolidatedKey, value.split(Pattern.quote(arrayDelimiter), -1),
                        consolidatedOutput);
            } else {
                // True single value
                Object finalValue = single.value;

                if (consolidateWithMatrixDenotorsInValue && single.hasArrayIndex) {
                    String indices = extractIndicesPrefix(single.originalFlattenedKey);
                    finalValue = indices + single.value.toString();
                }

                consolidatedOutput.put(consolidatedKey, finalValue);

                if (wasTrackedAsArray && gatherStatistics) {
                    // Single value from array - only add statistics if toggle is on
                    consolidatedOutput.put(consolidatedKey + "_count", 1L);
                    consolidatedOutput.put(consolidatedKey + "_distinct_count", 1L);
                    int length = finalValue.toString().length();
                    consolidatedOutput.put(consolidatedKey + "_min_length", (long) length);
                    consolidatedOutput.put(consolidatedKey + "_max_length", (long) length);
                    consolidatedOutput.put(consolidatedKey + "_avg_length", (double) length);
                    consolidatedOutput.put(consolidatedKey + "_type", determineType(single.value));
                }
            }
        } else {
            // Multiple values - process as array
            List<String> arrayValues = new ArrayList<>();

            for (KeyedValue kv : validKeyedValues) {
                String stringValue;
                if (consolidateWithMatrixDenotorsInValue && kv.hasArrayIndex) {
                    String indices = extractIndicesPrefix(kv.originalFlattenedKey);
                    stringValue = indices + kv.value.toString();
                } else {
                    stringValue = kv.value.toString();
                }
                arrayValues.add(stringValue);
            }

            processArrayValues(consolidatedKey, arrayValues.toArray(new String[0]),
                    consolidatedOutput);
        }
    }

    /**
     * Process array values and generate statistics
     */
    private void processArrayValues(String consolidatedKey, String[] values,
                                    Map<String, Object> consolidatedOutput) {
        String joined = String.join(arrayDelimiter, values);
        consolidatedOutput.put(consolidatedKey, joined);

        // Only gather statistics if the toggle is on
        if (gatherStatistics) {
            Set<String> uniqueValues = new HashSet<>(Arrays.asList(values));

            int minLength = Integer.MAX_VALUE;
            int maxLength = 0;
            int totalLength = 0;

            for (String val : values) {
                int length = val.length();
                totalLength += length;
                minLength = Math.min(minLength, length);
                maxLength = Math.max(maxLength, length);
            }

            consolidatedOutput.put(consolidatedKey + "_count", (long) values.length);
            consolidatedOutput.put(consolidatedKey + "_distinct_count", (long) uniqueValues.size());
            consolidatedOutput.put(consolidatedKey + "_min_length", (long) minLength);
            consolidatedOutput.put(consolidatedKey + "_max_length", (long) maxLength);
            consolidatedOutput.put(consolidatedKey + "_avg_length", totalLength / (double) values.length);
            consolidatedOutput.put(consolidatedKey + "_type", determineArrayType(Arrays.asList(values)));
        }
    }

    private String extractIndicesPrefix(String flattenedKey) {
        StringBuilder prefix = new StringBuilder();
        Matcher matcher = ALL_INDICES_PATTERN.matcher(flattenedKey);
        while (matcher.find()) {
            prefix.append("[").append(matcher.group(1)).append("]");
        }
        return prefix.toString();
    }

    private String determineType(Object value) {
        if (value instanceof Number) return "numeric_single_value";
        if (value instanceof Boolean) return "boolean_single_value";
        if (value instanceof String) return "string_single_value";
        return "object_single_value";
    }

    private String determineListType(List<KeyedValue> values) {
        boolean allNumbers = true;
        boolean allBooleans = true;
        boolean allStrings = true;

        for (KeyedValue kv : values) {
            if (!(kv.value instanceof Number)) allNumbers = false;
            if (!(kv.value instanceof Boolean)) allBooleans = false;
            if (!(kv.value instanceof String)) allStrings = false;
        }

        if (allNumbers) return "numeric_list_consolidated";
        if (allBooleans) return "boolean_list_consolidated";
        if (allStrings) return "string_list_consolidated";
        return "mixed_or_string_after_consolidation";
    }

    private String determineArrayType(List<String> values) {
        boolean allNumbers = true;
        boolean allBooleans = true;

        for (String val : values) {
            try {
                // A simple check for numeric values
                if (val.equalsIgnoreCase("NaN")) { // Double.parseDouble("NaN") works but we treat it as non-numeric string
                    allNumbers = false;
                    continue;
                }
                Double.parseDouble(val);
            } catch (NumberFormatException e) {
                allNumbers = false;
            }

            if (!val.equalsIgnoreCase("true") && !val.equalsIgnoreCase("false")) {
                allBooleans = false;
            }
        }

        if (allNumbers) return "numeric_list_consolidated";
        if (allBooleans) return "boolean_list_consolidated";
        return "string_list_consolidated";
    }

    private String safeToString(Object value) {
        if (value == null) return nullPlaceholder != null ? nullPlaceholder : "";
        try {
            if (value instanceof BigDecimal) {
                value = ((BigDecimal) value).doubleValue();
            }
            if (value instanceof JSONObject || value instanceof JSONArray) {
                return value.toString();
            }
            return value.toString();
        } catch (Exception e) {
            return "Error_Serializing_Value";
        }
    }

    // Helper classes
    private static class FlattenTask implements Serializable {
        private static final long serialVersionUID = 1L;
        String prefix;
        Object value;
        int depth;

        FlattenTask(String prefix, Object value, int depth) {
            this.prefix = prefix;
            this.value = value;
            this.depth = depth;
        }
    }

    private static class KeyedValue implements Serializable {
        private static final long serialVersionUID = 1L;
        String originalFlattenedKey;
        Object value;
        boolean hasArrayIndex;

        KeyedValue(String key, Object val, boolean hasIdx) {
            this.originalFlattenedKey = key;
            this.value = val;
            this.hasArrayIndex = hasIdx;
        }
    }
}