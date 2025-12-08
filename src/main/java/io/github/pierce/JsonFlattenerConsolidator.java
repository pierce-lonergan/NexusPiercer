package io.github.pierce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JsonFlattenerConsolidator - Flattens nested JSON into flat key-value pairs with array consolidation.
 *
 * This class uses Jackson for JSON processing (Apache 2.0 License).
 */
public class JsonFlattenerConsolidator implements Serializable {
    private static final long serialVersionUID = 1L;

    // Thread-safe ObjectMapper for JSON processing
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String arrayDelimiter;
    private final String nullPlaceholder;
    private final int maxNestingDepth;
    private final int maxArraySize;
    private final boolean consolidateWithMatrixDenotorsInValue;
    private final boolean gatherStatistics;

    private final Set<String> explosionPaths;
    private final boolean explosionEnabled;

    // Thread-local storage for array fields tracking - ensures thread safety
    private static final ThreadLocal<Set<String>> arrayFieldsThreadLocal =
            ThreadLocal.withInitial(HashSet::new);

    private static final Pattern ARRAY_INDEX_STRIP_PATTERN = Pattern.compile("\\[\\d+\\]");
    private static final Pattern ALL_INDICES_PATTERN = Pattern.compile("\\[(\\d+)\\]");
    private static final Pattern MALFORMED_JSON_PATTERN = Pattern.compile("[:,\\[]\\s*(undefined|NaN)\\s*[,\\}\\]]");
    private static final Pattern ARRAY_INDEX_PATTERN = Pattern.compile("(.+?)\\[(\\d+)\\](.*)");

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

    public JsonFlattenerConsolidator(String arrayDelimiter, String nullPlaceholder,
                                     int maxNestingDepth, int maxArraySize,
                                     boolean consolidateWithMatrixDenotorsInValue) {
        this(arrayDelimiter, nullPlaceholder, maxNestingDepth, maxArraySize,
                consolidateWithMatrixDenotorsInValue, true);
    }

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
            String trimmed = jsonString.trim();

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

            JsonNode jsonNode = OBJECT_MAPPER.readTree(trimmed);
            if (!jsonNode.isObject()) {
                return "{}";
            }

            // Clear array fields tracking for this run (thread-local)
            arrayFieldsThreadLocal.get().clear();

            // Step 1: Flatten the JSON
            Map<String, Object> flattened = flattenJson(jsonNode);

            // Step 2: Consolidate the flattened data
            Map<String, Object> consolidated = consolidateFlattened(flattened);

            // Step 3: Convert to JSON with underscored keys
            ObjectNode result = OBJECT_MAPPER.createObjectNode();
            for (Map.Entry<String, Object> entry : consolidated.entrySet()) {
                putValue(result, entry.getKey(), entry.getValue());
            }

            return OBJECT_MAPPER.writeValueAsString(result);

        } catch (Exception e) {
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

            JsonNode jsonNode = OBJECT_MAPPER.readTree(trimmed);
            if (!jsonNode.isObject()) {
                return Collections.singletonList("{}");
            }

            arrayFieldsThreadLocal.get().clear();

            // Step 1: Use SPECIAL flattening that doesn't consolidate arrays in explosion paths
            Map<String, Object> flattened = flattenJsonForExplosion(jsonNode);

            // Step 2: Perform explosion on the flattened data
            List<Map<String, Object>> explodedRecords = performExplosionOnFlattened(flattened);

            // Step 3: Consolidate each exploded record separately
            List<String> results = new ArrayList<>();
            for (Map<String, Object> record : explodedRecords) {
                Map<String, Object> consolidated = consolidateFlattened(record);
                ObjectNode result = OBJECT_MAPPER.createObjectNode();
                for (Map.Entry<String, Object> entry : consolidated.entrySet()) {
                    putValue(result, entry.getKey(), entry.getValue());
                }
                results.add(OBJECT_MAPPER.writeValueAsString(result));
            }

            return results;

        } catch (Exception e) {
            System.err.println("Error in explosion processing: " + e.getMessage());
            e.printStackTrace();
            return Collections.singletonList("{}");
        }
    }

    /**
     * Helper method to put a value into an ObjectNode with proper type handling
     */
    private void putValue(ObjectNode node, String key, Object value) {
        if (value == null) {
            node.putNull(key);
        } else if (value instanceof String) {
            node.put(key, (String) value);
        } else if (value instanceof Integer) {
            node.put(key, (Integer) value);
        } else if (value instanceof Long) {
            node.put(key, (Long) value);
        } else if (value instanceof Double) {
            node.put(key, (Double) value);
        } else if (value instanceof Float) {
            node.put(key, (Float) value);
        } else if (value instanceof Boolean) {
            node.put(key, (Boolean) value);
        } else if (value instanceof BigDecimal) {
            node.put(key, (BigDecimal) value);
        } else {
            node.put(key, value.toString());
        }
    }

    /**
     * Updated performExplosionOnFlattened with debugging
     */
    private List<Map<String, Object>> performExplosionOnFlattened(Map<String, Object> flattened) {
        if (explosionPaths.isEmpty()) {
            return Collections.singletonList(flattened);
        }

        System.err.println("=== performExplosionOnFlattened ===");
        System.err.println("Explosion paths: " + explosionPaths);
        System.err.println("Flattened data sample (first 10 keys):");
        int count = 0;
        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            System.err.println("  " + entry.getKey() + " = " + entry.getValue());
            if (++count >= 10) break;
        }

        List<Map<String, Object>> currentRecords = Collections.singletonList(new LinkedHashMap<>(flattened));

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

    private List<Map<String, Object>> explodeFlattened(Map<String, Object> flattened, String explosionPath) {
        String[] pathParts = explosionPath.split("\\.");

        Map<String, Map<String, Object>> recordGroups = new LinkedHashMap<>();
        Map<String, Object> nonArrayFields = new LinkedHashMap<>();

        StringBuilder pattern = new StringBuilder("^");
        for (int i = 0; i < pathParts.length; i++) {
            if (i > 0) pattern.append("\\.");
            pattern.append(Pattern.quote(pathParts[i]));

            if (i < pathParts.length - 1) {
                pattern.append("(?:\\[(\\d+)\\])?");
            } else {
                pattern.append("\\[(\\d+)\\]");
            }
        }
        pattern.append("(.*)$");

        Pattern explosionPattern = Pattern.compile(pattern.toString());

        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            String key = entry.getKey();
            Matcher m = explosionPattern.matcher(key);

            if (m.matches()) {
                StringBuilder groupKey = new StringBuilder();

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

        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            String key = entry.getKey();

            boolean inGroup = false;
            for (Map<String, Object> group : recordGroups.values()) {
                if (group.containsKey(key)) {
                    inGroup = true;
                    break;
                }
            }
            if (inGroup) continue;

            boolean isParentField = false;

            for (int parentLevel = 0; parentLevel < pathParts.length; parentLevel++) {
                StringBuilder parentPattern = new StringBuilder("^");

                for (int i = 0; i <= parentLevel; i++) {
                    if (i > 0) parentPattern.append("\\.");
                    parentPattern.append(Pattern.quote(pathParts[i]));
                    parentPattern.append("(?:\\[(\\d+)\\])?");
                }

                parentPattern.append("\\.([^\\[.]+)");
                parentPattern.append(".*$");

                Pattern pp = Pattern.compile(parentPattern.toString());
                Matcher pm = pp.matcher(key);

                if (pm.matches()) {
                    String fieldName = pm.group(parentLevel + 2);

                    if (parentLevel + 1 < pathParts.length && fieldName.equals(pathParts[parentLevel + 1])) {
                        continue;
                    }

                    isParentField = true;

                    StringBuilder parentKey = new StringBuilder();
                    for (int i = 1; i <= parentLevel + 1; i++) {
                        String idx = pm.group(i);
                        if (idx != null) {
                            if (parentKey.length() > 0) parentKey.append("_");
                            parentKey.append(idx);
                        }
                    }

                    String parentKeyStr = parentKey.toString();
                    for (Map.Entry<String, Map<String, Object>> group : recordGroups.entrySet()) {
                        String groupKeyStr = group.getKey();

                        if (parentKeyStr.isEmpty() || groupKeyStr.startsWith(parentKeyStr)) {
                            group.getValue().put(key, entry.getValue());
                        }
                    }
                    break;
                }
            }

            if (!isParentField) {
                nonArrayFields.put(key, entry.getValue());
            }
        }

        if (recordGroups.isEmpty()) {
            return Collections.singletonList(flattened);
        }

        List<Map<String, Object>> results = new ArrayList<>();
        int explosionIndex = 0;

        List<String> sortedGroupKeys = new ArrayList<>(recordGroups.keySet());
        Collections.sort(sortedGroupKeys);

        for (String groupKey : sortedGroupKeys) {
            Map<String, Object> groupFields = recordGroups.get(groupKey);
            String[] indices = groupKey.split("_");

            Map<String, Object> newRecord = new LinkedHashMap<>(nonArrayFields);

            for (Map.Entry<String, Object> field : groupFields.entrySet()) {
                String oldKey = field.getKey();
                String newKey = oldKey;

                for (int i = 0; i < pathParts.length; i++) {
                    if (i == pathParts.length - 1 && i < indices.length) {
                        String searchPattern = pathParts[i] + "[" + indices[i] + "]";
                        String replacement = pathParts[i];
                        newKey = newKey.replace(searchPattern, replacement);
                    }
                }

                newRecord.put(newKey, field.getValue());
            }

            newRecord.put(explosionPath + "_explosion_index", (long) explosionIndex++);

            results.add(newRecord);
        }

        return results;
    }

    private Map<String, Object> flattenJsonForExplosion(JsonNode jsonNode) {
        Map<String, Object> flattenedOutput = new LinkedHashMap<>();
        ArrayDeque<FlattenTask> taskQueue = new ArrayDeque<>();

        Iterator<String> fieldNames = jsonNode.fieldNames();
        while (fieldNames.hasNext()) {
            String key = fieldNames.next();
            JsonNode value = jsonNode.get(key);
            taskQueue.add(new FlattenTask(key, value, 1));
        }

        while (!taskQueue.isEmpty()) {
            FlattenTask currentTask = taskQueue.pollFirst();

            if (currentTask.depth > maxNestingDepth) {
                flattenedOutput.put(currentTask.prefix, safeToString(currentTask.value));
                continue;
            }

            JsonNode currentValue = (JsonNode) currentTask.value;

            if (currentValue == null || currentValue.isNull()) {
                flattenedOutput.put(currentTask.prefix, nullPlaceholder);
            } else if (currentValue.isObject()) {
                if (currentValue.isEmpty()) {
                    flattenedOutput.put(currentTask.prefix, nullPlaceholder);
                } else if (currentTask.depth == maxNestingDepth) {
                    flattenedOutput.put(currentTask.prefix, currentValue.toString());
                } else {
                    Iterator<String> objKeys = currentValue.fieldNames();
                    while (objKeys.hasNext()) {
                        String key = objKeys.next();
                        JsonNode val = currentValue.get(key);
                        String newPrefix = currentTask.prefix + "." + key;
                        taskQueue.add(new FlattenTask(newPrefix, val, currentTask.depth + 1));
                    }
                }
            } else if (currentValue.isArray()) {
                ArrayNode array = (ArrayNode) currentValue;
                if (array.isEmpty()) {
                    flattenedOutput.put(currentTask.prefix, nullPlaceholder);
                    arrayFieldsThreadLocal.get().add(currentTask.prefix);
                } else {
                    boolean shouldKeepElements = shouldKeepAsArrayElements(currentTask.prefix);

                    boolean allPrimitives = true;
                    for (int i = 0; i < array.size(); i++) {
                        JsonNode item = array.get(i);
                        if (item.isObject() || item.isArray()) {
                            allPrimitives = false;
                            break;
                        }
                    }

                    if (allPrimitives && !shouldKeepElements) {
                        StringBuilder sb = new StringBuilder();
                        int limit = Math.min(array.size(), maxArraySize);
                        for (int i = 0; i < limit; i++) {
                            if (i > 0) sb.append(arrayDelimiter);
                            JsonNode item = array.get(i);
                            if (item == null || item.isNull()) {
                                sb.append(nullPlaceholder != null ? nullPlaceholder : "");
                            } else {
                                sb.append(getNodeValue(item).toString());
                            }
                        }
                        flattenedOutput.put(currentTask.prefix, sb.toString());
                        arrayFieldsThreadLocal.get().add(currentTask.prefix);
                    } else {
                        int limit = Math.min(array.size(), maxArraySize);
                        for (int i = 0; i < limit; i++) {
                            JsonNode item = array.get(i);
                            String newPrefix = currentTask.prefix + "[" + i + "]";
                            taskQueue.add(new FlattenTask(newPrefix, item, currentTask.depth + 1));
                        }
                        arrayFieldsThreadLocal.get().add(currentTask.prefix);
                    }
                }
            } else {
                Object val = getNodeValue(currentValue);
                flattenedOutput.put(currentTask.prefix, val);
            }
        }

        return flattenedOutput;
    }

    private boolean shouldKeepAsArrayElements(String currentPath) {
        System.err.println("shouldKeepAsArrayElements: checking '" + currentPath + "'");

        String normalizedPath = currentPath.replaceAll("\\[\\d+\\]", "");

        for (String explosionPath : explosionPaths) {
            if (normalizedPath.equals(explosionPath)) {
                System.err.println("  -> YES (direct match with " + explosionPath + ")");
                return true;
            }

            String[] explosionParts = explosionPath.split("\\.");
            String[] currentParts = normalizedPath.split("\\.");

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

    private String preprocessJson(String jsonString) {
        String trimmed = jsonString.trim();

        if (trimmed.startsWith("\uFEFF")) {
            trimmed = trimmed.substring(1);
        }

        if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
            return "{}";
        }

        if (trimmed.contains(": undefined") || trimmed.contains(": NaN")) {
            return "{}";
        }

        return trimmed;
    }

    private Map<String, Object> flattenJson(JsonNode jsonNode) {
        Map<String, Object> flattenedOutput = new LinkedHashMap<>();
        ArrayDeque<FlattenTask> taskQueue = new ArrayDeque<>();

        Iterator<String> fieldNames = jsonNode.fieldNames();
        while (fieldNames.hasNext()) {
            String key = fieldNames.next();
            JsonNode value = jsonNode.get(key);
            taskQueue.add(new FlattenTask(key, value, 1));
        }

        while (!taskQueue.isEmpty()) {
            FlattenTask currentTask = taskQueue.pollFirst();

            if (currentTask.depth > maxNestingDepth) {
                flattenedOutput.put(currentTask.prefix, safeToString(currentTask.value));
                continue;
            }

            JsonNode currentValue = (JsonNode) currentTask.value;

            if (currentValue == null || currentValue.isNull()) {
                flattenedOutput.put(currentTask.prefix, nullPlaceholder);
            } else if (currentValue.isObject()) {
                if (currentValue.isEmpty()) {
                    flattenedOutput.put(currentTask.prefix, nullPlaceholder);
                } else if (currentTask.depth == maxNestingDepth) {
                    flattenedOutput.put(currentTask.prefix, currentValue.toString());
                } else {
                    Iterator<String> objKeys = currentValue.fieldNames();
                    while (objKeys.hasNext()) {
                        String key = objKeys.next();
                        JsonNode val = currentValue.get(key);
                        String newPrefix = currentTask.prefix + "." + key;
                        taskQueue.add(new FlattenTask(newPrefix, val, currentTask.depth + 1));
                    }
                }
            } else if (currentValue.isArray()) {
                ArrayNode array = (ArrayNode) currentValue;
                if (array.isEmpty()) {
                    flattenedOutput.put(currentTask.prefix, nullPlaceholder);
                    arrayFieldsThreadLocal.get().add(currentTask.prefix);
                } else {
                    int limit = Math.min(array.size(), maxArraySize);
                    for (int i = 0; i < limit; i++) {
                        JsonNode item = array.get(i);
                        String newPrefix = currentTask.prefix + "[" + i + "]";
                        taskQueue.add(new FlattenTask(newPrefix, item, currentTask.depth + 1));
                    }
                    arrayFieldsThreadLocal.get().add(currentTask.prefix);
                }
            } else {
                Object val = getNodeValue(currentValue);
                flattenedOutput.put(currentTask.prefix, val);
            }
        }

        return flattenedOutput;
    }

    private Object getNodeValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        } else if (node.isTextual()) {
            return node.asText();
        } else if (node.isInt()) {
            return node.asInt();
        } else if (node.isLong()) {
            return node.asLong();
        } else if (node.isDouble() || node.isFloat()) {
            return node.asDouble();
        } else if (node.isBoolean()) {
            return node.asBoolean();
        } else if (node.isBigDecimal()) {
            return node.decimalValue().doubleValue();
        } else if (node.isBigInteger()) {
            return node.bigIntegerValue().longValue();
        } else {
            return node.toString();
        }
    }

    private Map<String, Object> consolidateFlattened(Map<String, Object> flattened) {
        Map<String, Object> consolidatedOutput = new LinkedHashMap<>();
        Map<String, List<KeyedValue>> groupedByBase = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : flattened.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            String consolidatedKey = key.replace(".", "_");
            boolean hasArrayIndex = ARRAY_INDEX_PATTERN.matcher(key).find();

            if (hasArrayIndex) {
                String baseKey = ARRAY_INDEX_STRIP_PATTERN.matcher(consolidatedKey).replaceAll("");
                groupedByBase.computeIfAbsent(baseKey, k -> new ArrayList<>())
                        .add(new KeyedValue(key, value, true));
            } else {
                consolidatedOutput.put(consolidatedKey, value);
            }
        }

        for (Map.Entry<String, List<KeyedValue>> group : groupedByBase.entrySet()) {
            String consolidatedKey = group.getKey();
            List<KeyedValue> keyedValues = group.getValue();

            processGroupedValues(consolidatedKey, keyedValues, consolidatedOutput);
        }

        return consolidatedOutput;
    }

    private void processGroupedValues(String consolidatedKey, List<KeyedValue> keyedValues,
                                      Map<String, Object> consolidatedOutput) {
        String originalBaseKey = consolidatedKey.replace("_", ".");
        boolean wasTrackedAsArray = false;
        for (String arrayField : arrayFieldsThreadLocal.get()) {
            String normalizedArrayField = arrayField.replaceAll("\\[\\d+\\]", "");
            if (originalBaseKey.startsWith(normalizedArrayField)) {
                wasTrackedAsArray = true;
                break;
            }
        }

        List<KeyedValue> validKeyedValues = new ArrayList<>();
        boolean wasOriginallyArray = keyedValues.size() > 1;

        for (KeyedValue kv : keyedValues) {
            if (kv.value != null) {
                validKeyedValues.add(kv);
            }
        }

        boolean shouldGenerateArrayStats = wasTrackedAsArray && gatherStatistics;

        if (validKeyedValues.isEmpty()) {
            consolidatedOutput.put(consolidatedKey, nullPlaceholder);
            if (shouldGenerateArrayStats) {
                consolidatedOutput.put(consolidatedKey + "_count", 0L);
                consolidatedOutput.put(consolidatedKey + "_distinct_count", 0L);
            }
        } else if (validKeyedValues.size() == 1 && !wasOriginallyArray) {
            KeyedValue single = validKeyedValues.get(0);
            String value = single.value.toString();

            if (wasTrackedAsArray && value.contains(arrayDelimiter)) {
                processArrayValues(consolidatedKey, value.split(Pattern.quote(arrayDelimiter), -1),
                        consolidatedOutput);
            } else {
                Object finalValue = single.value;

                if (consolidateWithMatrixDenotorsInValue && single.hasArrayIndex) {
                    String indices = extractIndicesPrefix(single.originalFlattenedKey);
                    finalValue = indices + single.value.toString();
                }

                consolidatedOutput.put(consolidatedKey, finalValue);

                if (wasTrackedAsArray && gatherStatistics) {
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

    private void processArrayValues(String consolidatedKey, String[] values,
                                    Map<String, Object> consolidatedOutput) {
        String joined = String.join(arrayDelimiter, values);
        consolidatedOutput.put(consolidatedKey, joined);

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

    private String determineArrayType(List<String> values) {
        boolean allNumbers = true;
        boolean allBooleans = true;

        for (String val : values) {
            try {
                if (val.equalsIgnoreCase("NaN")) {
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
            if (value instanceof JsonNode) {
                return value.toString();
            }
            return value.toString();
        } catch (Exception e) {
            return "Error_Serializing_Value";
        }
    }

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