package io.github.pierce

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.transform.TypeChecked

import java.util.regex.Matcher
import java.util.regex.Pattern

/**
 * Groovy implementation of JsonFlattenerConsolidator
 *
 * This class provides efficient JSON flattening and consolidation functionality
 * optimized for Apache Spark data processing pipelines.
 *
 * Key improvements in Groovy version:
 * - Leverages Groovy's native JSON handling
 * - More concise syntax with optional typing
 * - Enhanced null safety with Elvis operator
 * - Simplified collection operations
 * - Better string interpolation with GStrings
 */
@CompileStatic
@TypeChecked
class JsonFlattenerConsolidator implements Serializable {
    private static final long serialVersionUID = 1L

    // Configuration properties
    final String arrayDelimiter
    final String nullPlaceholder
    final int maxNestingDepth
    final int maxArraySize
    final boolean consolidateWithMatrixDenotorsInValue
    final boolean gatherStatistics

    // Explosion configuration
    final Set<String> explosionPaths
    final boolean explosionEnabled

    // Track array fields during processing
    private final Set<String> arrayFields = new HashSet<>()

    // Regex patterns for processing
    private static final Pattern ARRAY_INDEX_STRIP_PATTERN = ~/\[\d+\]/
    private static final Pattern ALL_INDICES_PATTERN = ~/\[(\d+)\]/
    private static final Pattern ARRAY_INDEX_PATTERN = ~/(.+?)\[(\d+)\](.*)/

    /**
     * Full constructor with explosion paths
     */
    JsonFlattenerConsolidator(String arrayDelimiter = ",",
                              String nullPlaceholder = null,
                              int maxNestingDepth = 50,
                              int maxArraySize = 1000,
                              boolean consolidateWithMatrixDenotorsInValue = false,
                              boolean gatherStatistics = true,
                              String... explosionPaths) {
        this.arrayDelimiter = arrayDelimiter ?: ","
        this.nullPlaceholder = nullPlaceholder
        this.maxNestingDepth = maxNestingDepth > 0 ? maxNestingDepth : 50
        this.maxArraySize = maxArraySize > 0 ? maxArraySize : 1000
        this.consolidateWithMatrixDenotorsInValue = consolidateWithMatrixDenotorsInValue
        this.gatherStatistics = gatherStatistics
        this.explosionPaths = explosionPaths as Set
        this.explosionEnabled = explosionPaths.length > 0
    }

    /**
     * Main method to flatten and consolidate JSON
     * Returns a JSON string that can be parsed by Spark's from_json
     */
    String flattenAndConsolidateJson(String jsonString) {
        if (!jsonString?.trim()) {
            return "{}"
        }

        try {
            def trimmed = preprocessJson(jsonString)
            if (trimmed == "{}") {
                return "{}"
            }

            def jsonSlurper = new JsonSlurper()
            def jsonObject = jsonSlurper.parseText(trimmed)

            // Clear array fields tracking for this run
            arrayFields.clear()

            // Step 1: Flatten the JSON
            Map<String, Object> flattened = flattenJson(jsonObject)

            // Step 2: Consolidate the flattened data
            Map<String, Object> consolidated = consolidateFlattened(flattened)

            // Step 3: Convert to JSON string
            def jsonBuilder = new JsonBuilder(consolidated)
            return jsonBuilder.toString()

        } catch (Exception e) {
            System.err.println("Error processing JSON: ${e.message}")
            e.printStackTrace()
            return "{}"
        }
    }

    /**
     * New method to flatten and explode JSON based on specified paths
     * Returns a list of JSON strings, one for each exploded record
     */
    List<String> flattenAndExplodeJson(String jsonString) {
        if (!explosionEnabled) {
            return [flattenAndConsolidateJson(jsonString)]
        }

        if (!jsonString?.trim()) {
            return ["{}}"]
        }

        try {
            def trimmed = preprocessJson(jsonString)
            if (trimmed == "{}") {
                return ["{}}"]
            }

            def jsonSlurper = new JsonSlurper()
            def jsonObject = jsonSlurper.parseText(trimmed)
            arrayFields.clear()

            // Step 1: Use special flattening for explosion
            Map<String, Object> flattened = flattenJsonForExplosion(jsonObject)

            // Step 2: Perform explosion on the flattened data
            List<Map<String, Object>> explodedRecords = performExplosionOnFlattened(flattened)

            // Step 3: Consolidate each exploded record
            List<String> results = []
            explodedRecords.each { record ->
                Map<String, Object> consolidated = consolidateFlattened(record)
                def jsonBuilder = new JsonBuilder(consolidated)
                results << jsonBuilder.toString()
            }

            return results

        } catch (Exception e) {
            System.err.println("Error in explosion processing: ${e.message}")
            e.printStackTrace()
            return ["{}}"]
        }
    }

    /**
     * Perform explosion on flattened data
     */
    private List<Map<String, Object>> performExplosionOnFlattened(Map<String, Object> flattened) {
        if (!explosionPaths) {
            return [new LinkedHashMap<>(flattened)] as List<Map<String, Object>>
        }

        // Debug logging
        System.err.println("=== performExplosionOnFlattened ===")
        System.err.println("Explosion paths: $explosionPaths")
        System.err.println("Flattened data sample (first 10 keys):")

        flattened.take(10).each { key, value ->
            System.err.println("  $key = $value")
        }

        List<Map<String, Object>> currentRecords = [new LinkedHashMap<>(flattened)] as List<Map<String, Object>>

        // Process each explosion path
        explosionPaths.each { explosionPath ->
            List<Map<String, Object>> nextRecords = []

            currentRecords.each { record ->
                List<Map<String, Object>> exploded = explodeFlattened(record, explosionPath)
                nextRecords.addAll(exploded)
            }

            currentRecords = nextRecords
        }

        return currentRecords
    }

    /**
     * Explode flattened data based on explosion path
     */
    private static List<Map<String, Object>> explodeFlattened(Map<String, Object> flattened, String explosionPath) {
        def pathParts = explosionPath.split(/\./)

        // Group records by their indices at the explosion level
        Map<String, Map<String, Object>> recordGroups = [:]
        Map<String, Object> nonArrayFields = [:]

        // Build regex pattern for explosion path
        def patternBuilder = new StringBuilder("^")
        pathParts.eachWithIndex { part, i ->
            if (i > 0) patternBuilder.append(/\./)
            patternBuilder.append(Pattern.quote(part))

            if (i < pathParts.size() - 1) {
                // Before explosion level - optional indices
                patternBuilder.append(/(?:\[(\d+)\])?/)
            } else {
                // At explosion level - required index
                patternBuilder.append(/\[(\d+)\]/)
            }
        }
        patternBuilder.append(/(.*)$/) // Rest of the path

        def explosionPattern = Pattern.compile(patternBuilder.toString())

        // First pass: find explosion fields and group them
        flattened.each { key, value ->
            Matcher m = explosionPattern.matcher(key)

            if (m.matches()) {
                // Build group key from indices
                def groupKeyParts = []
                (1..pathParts.size()).each { i ->
                    def idx = m.group(i)
                    if (idx) {
                        groupKeyParts << idx
                    }
                }
                def groupKey = groupKeyParts.join("_")

                if (!recordGroups.containsKey(groupKey)) {
                    recordGroups.put(groupKey, new LinkedHashMap<String, Object>())
                }
                recordGroups.get(groupKey).put(key, value)
            }
        }

        // Second pass: categorize all fields
        flattened.each { key, value ->
            // Skip if already in a record group
            def inGroup = recordGroups.values().any { it.containsKey(key) }
            if (inGroup) return

            // Check if this is a parent field
            def isParentField = false

            // Try to match parent patterns at different levels
            for (int parentLevel = 0; parentLevel < pathParts.size(); parentLevel++) {
                def parentPatternBuilder = new StringBuilder("^")

                // Build pattern up to parent level
                (0..parentLevel).each { i ->
                    if (i > 0) parentPatternBuilder.append(/\./)
                    parentPatternBuilder.append(Pattern.quote(pathParts[i]))
                    parentPatternBuilder.append(/(?:\[(\d+)\])?/)
                }

                // Should have a field name after this
                parentPatternBuilder.append(/\.([^\[.]+)/)
                parentPatternBuilder.append(/.*$/)

                def pp = Pattern.compile(parentPatternBuilder.toString())
                Matcher pm = pp.matcher(key)

                if (pm.matches()) {
                    def fieldName = pm.group(parentLevel + 2)

                    // Skip if field name continues explosion path
                    if (parentLevel + 1 < pathParts.size() && fieldName == pathParts[parentLevel + 1]) {
                        continue
                    }

                    isParentField = true

                    // Build parent key from indices
                    def parentKeyParts = []
                    (1..parentLevel + 1).each { i ->
                        def idx = pm.group(i)
                        if (idx) {
                            parentKeyParts << idx
                        }
                    }
                    def parentKeyStr = parentKeyParts.join("_")

                    // Add to matching groups
                    recordGroups.each { groupKey, group ->
                        if (!parentKeyStr || groupKey.startsWith(parentKeyStr)) {
                            group[key] = value
                        }
                    }
                    break
                }
            }

            if (!isParentField) {
                nonArrayFields[key] = value
            }
        }

        // If no groups found, return original
        if (!recordGroups) {
            return [flattened]
        }

        // Create exploded records
        List<Map<String, Object>> results = []
        int explosionIndex = 0

        // Sort groups for consistent ordering
        def sortedGroupKeys = recordGroups.keySet().sort()

        sortedGroupKeys.each { groupKey ->
            def groupFields = recordGroups[groupKey]
            def indices = groupKey.split("_")

            Map<String, Object> newRecord = new LinkedHashMap<>(nonArrayFields)

            // Add all fields from this group, transforming keys
            groupFields.each { oldKey, fieldValue ->
                def newKey = oldKey

                // Remove indices for the explosion path
                pathParts.eachWithIndex { part, i ->
                    if (i == pathParts.size() - 1 && i < indices.size()) {
                        def searchPattern = "$part[${indices[i]}]"
                        def replacement = part
                        newKey = newKey.replace(searchPattern, replacement)
                    }
                }

                newRecord[newKey] = fieldValue
            }

            // Add explosion index
            newRecord["${explosionPath}_explosion_index" as String] = explosionIndex++ as Long

            results << newRecord
        }

        return results
    }

    /**
     * Special flattening that preserves arrays in explosion paths
     */
    private Map<String, Object> flattenJsonForExplosion(Object jsonObject) {
        Map<String, Object> flattenedOutput = [:]
        ArrayDeque<FlattenTask> taskQueue = new ArrayDeque<>()

        // Initialize queue with top-level entries
        if (jsonObject instanceof Map) {
            jsonObject.each { key, value ->
                taskQueue.add(new FlattenTask(key.toString(), value, 1))
            }
        }

        // Process queue
        while (!taskQueue.isEmpty()) {
            def currentTask = taskQueue.pollFirst()

            if (currentTask.depth > maxNestingDepth) {
                flattenedOutput[currentTask.prefix] = safeToString(currentTask.value)
                continue
            }

            if (currentTask.value == null) {
                flattenedOutput[currentTask.prefix] = nullPlaceholder
            } else if (currentTask.value instanceof Map) {
                def obj = currentTask.value as Map
                if (obj.isEmpty()) {
                    flattenedOutput[currentTask.prefix] = nullPlaceholder
                } else if (currentTask.depth == maxNestingDepth) {
                    flattenedOutput[currentTask.prefix] = new JsonBuilder(obj).toString()
                } else {
                    obj.each { key, val ->
                        def newPrefix = "${currentTask.prefix}.${key}"
                        taskQueue.add(new FlattenTask(newPrefix, val, currentTask.depth + 1))
                    }
                }
            } else if (currentTask.value instanceof List) {
                def array = currentTask.value as List
                if (array.isEmpty()) {
                    flattenedOutput[currentTask.prefix] = nullPlaceholder
                    arrayFields.add(currentTask.prefix)
                } else {
                    // Check if we should keep this array as individual elements
                    def shouldKeepElements = shouldKeepAsArrayElements(currentTask.prefix)

                    // Check if all elements are primitives
                    def allPrimitives = array.every { item ->
                        !(item instanceof Map || item instanceof List)
                    }

                    if (allPrimitives && !shouldKeepElements) {
                        // Primitive array NOT in explosion path - consolidate
                        def values = []
                        def limit = Math.min(array.size(), maxArraySize)
                        (0..<limit).each { i ->
                            def item = array[i]
                            if (item == null) {
                                values << (nullPlaceholder ?: "")
                            } else {
                                if (item instanceof BigDecimal) {
                                    item = item.doubleValue()
                                }
                                values << item.toString()
                            }
                        }
                        flattenedOutput[currentTask.prefix] = values.join(arrayDelimiter)
                        arrayFields.add(currentTask.prefix)
                    } else {
                        // Keep as individual elements
                        def limit = Math.min(array.size(), maxArraySize)
                        (0..<limit).each { i ->
                            def item = array[i]
                            def newPrefix = "${currentTask.prefix}[${i}]"
                            taskQueue.add(new FlattenTask(newPrefix, item, currentTask.depth + 1))
                        }
                        arrayFields.add(currentTask.prefix)
                    }
                }
            } else {
                // Primitive value
                def val = currentTask.value
                if (val instanceof BigDecimal) {
                    val = val.doubleValue()
                }
                flattenedOutput[currentTask.prefix] = val
            }
        }

        return flattenedOutput
    }

    /**
     * Check if array should be kept as individual elements for explosion
     */
    private boolean shouldKeepAsArrayElements(String currentPath) {
        System.err.println("shouldKeepAsArrayElements: checking '$currentPath'")

        // Normalize path by removing array indices
        def normalizedPath = currentPath.replaceAll(/\[\d+\]/, "")

        for (explosionPath in explosionPaths) {
            // Direct match
            if (normalizedPath == explosionPath) {
                System.err.println("  -> YES (direct match with $explosionPath)")
                return true
            }

            // Check if this path is part of a larger explosion path
            def explosionParts = explosionPath.split(/\./)
            def currentParts = normalizedPath.split(/\./)

            // If current path is a prefix of explosion path
            if (currentParts.size() <= explosionParts.size()) {
                def isPrefix = true
                currentParts.eachWithIndex { part, i ->
                    if (part != explosionParts[i]) {
                        isPrefix = false
                    }
                }
                if (isPrefix) {
                    System.err.println("  -> YES (prefix of $explosionPath)")
                    return true
                }
            }
        }

        System.err.println("  -> NO")
        return false
    }

    /**
     * Preprocess JSON string for validation
     */
    private String preprocessJson(String jsonString) {
        def trimmed = jsonString.trim()

        // Remove potential BOM characters
        if (trimmed.startsWith("\uFEFF")) {
            trimmed = trimmed.substring(1)
        }

        // Validate JSON structure
        if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
            System.err.println("JSON validation failed: doesn't start/end with braces")
            return "{}"
        }

        if (trimmed.contains(": undefined") || trimmed.contains(": NaN")) {
            System.err.println("JSON validation failed: contains undefined or NaN")
            return "{}"
        }

        return trimmed
    }

    /**
     * Flatten JSON following the same logic as the original implementation
     */
    private Map<String, Object> flattenJson(Object jsonObject) {
        Map<String, Object> flattenedOutput = [:]
        ArrayDeque<FlattenTask> taskQueue = new ArrayDeque<>()

        // Initialize queue with top-level entries
        if (jsonObject instanceof Map) {
            jsonObject.each { key, value ->
                taskQueue.add(new FlattenTask(key.toString(), value, 1))
            }
        }

        // Process queue
        while (!taskQueue.isEmpty()) {
            def currentTask = taskQueue.pollFirst()

            if (currentTask.depth > maxNestingDepth) {
                flattenedOutput[currentTask.prefix] = safeToString(currentTask.value)
                continue
            }

            if (currentTask.value == null) {
                flattenedOutput[currentTask.prefix] = nullPlaceholder
            } else if (currentTask.value instanceof Map) {
                def obj = currentTask.value as Map
                if (obj.isEmpty()) {
                    flattenedOutput[currentTask.prefix] = nullPlaceholder
                } else if (currentTask.depth == maxNestingDepth) {
                    flattenedOutput[currentTask.prefix] = new JsonBuilder(obj).toString()
                } else {
                    obj.each { key, val ->
                        def newPrefix = "${currentTask.prefix}.${key}"
                        taskQueue.add(new FlattenTask(newPrefix, val, currentTask.depth + 1))
                    }
                }
            } else if (currentTask.value instanceof List) {
                def array = currentTask.value as List
                if (array.isEmpty()) {
                    flattenedOutput[currentTask.prefix] = nullPlaceholder
                    arrayFields.add(currentTask.prefix)
                } else {
                    // Check if all elements are primitives
                    def allPrimitives = array.every { item ->
                        !(item instanceof Map || item instanceof List)
                    }

                    if (allPrimitives) {
                        // Simple array - concatenate values
                        def values = []
                        def limit = Math.min(array.size(), maxArraySize)
                        (0..<limit).each { i ->
                            def item = array[i]
                            if (item == null) {
                                values << (nullPlaceholder ?: "")
                            } else {
                                if (item instanceof BigDecimal) {
                                    item = item.doubleValue()
                                }
                                values << item.toString()
                            }
                        }
                        flattenedOutput[currentTask.prefix] = values.join(arrayDelimiter)
                        arrayFields.add(currentTask.prefix)
                    } else {
                        // Complex array - process each element
                        def limit = Math.min(array.size(), maxArraySize)
                        (0..<limit).each { i ->
                            def item = array[i]
                            def newPrefix = "${currentTask.prefix}[${i}]"
                            taskQueue.add(new FlattenTask(newPrefix, item, currentTask.depth + 1))
                        }
                        arrayFields.add(currentTask.prefix)
                    }
                }
            } else {
                // Primitive value
                def val = currentTask.value
                if (val instanceof BigDecimal) {
                    val = val.doubleValue()
                }
                flattenedOutput[currentTask.prefix] = val
            }
        }

        return flattenedOutput
    }

    /**
     * Consolidate flattened data
     */
    private Map<String, Object> consolidateFlattened(Map<String, Object> flattened) {
        // Group by base key
        Map<String, List<KeyedValue>> groupedByBaseKey = [:]

        flattened.each { flattenedKey, value ->
            // Calculate base key
            def baseKey = flattenedKey.replaceAll(ARRAY_INDEX_STRIP_PATTERN, "")
            baseKey = baseKey.replace('.', '_')

            // Check if it has array index
            def hasArrayIndex = flattenedKey.contains("[")

            // Add to grouped data
            def list = groupedByBaseKey.computeIfAbsent(baseKey) { [] }
            list << new KeyedValue(flattenedKey, value, hasArrayIndex)
        }

        // Process each group
        Map<String, Object> consolidatedOutput = [:]
        groupedByBaseKey.each { consolidatedKey, keyedValues ->
            processKeyGroup(consolidatedKey, keyedValues, consolidatedOutput)
        }

        return consolidatedOutput
    }

    /**
     * Process a group of values for the same base key
     */
    private void processKeyGroup(String consolidatedKey, List<KeyedValue> keyedValues,
                                 Map<String, Object> consolidatedOutput) {
        // Check if any value had array indices
        def wasOriginallyArray = keyedValues.any { it.hasArrayIndex }

        // Check if the field was tracked as an array
        def wasTrackedAsArray = keyedValues.any { kv ->
            def baseKey = kv.originalFlattenedKey.replaceAll(ARRAY_INDEX_STRIP_PATTERN, "")
            arrayFields.contains(baseKey) || arrayFields.contains(kv.originalFlattenedKey)
        }

        // Filter valid values
        def validKeyedValues = keyedValues.findAll { kv ->
            kv.value != null && kv.value != nullPlaceholder
        }

        // Determine if we should generate array statistics
        def shouldGenerateArrayStats = gatherStatistics && (wasOriginallyArray || wasTrackedAsArray)

        if (validKeyedValues.isEmpty()) {
            // All values were null
            consolidatedOutput[consolidatedKey] = nullPlaceholder
            if (shouldGenerateArrayStats) {
                consolidatedOutput["${consolidatedKey}_count" as String] = 0L
                consolidatedOutput["${consolidatedKey}_distinct_count" as String] = 0L
            }
        } else if (validKeyedValues.size() == 1 && !wasOriginallyArray) {
            // Single value case
            def single = validKeyedValues[0]
            def value = single.value.toString()

            // Check if this single value is actually a consolidated array
            if (wasTrackedAsArray && value.contains(arrayDelimiter)) {
                // This is a flattened array
                processArrayValues(consolidatedKey, value.split(Pattern.quote(arrayDelimiter), -1),
                        consolidatedOutput)
            } else {
                // True single value
                def finalValue = single.value

                if (consolidateWithMatrixDenotorsInValue && single.hasArrayIndex) {
                    def indices = extractIndicesPrefix(single.originalFlattenedKey)
                    finalValue = indices + single.value.toString()
                }

                consolidatedOutput[consolidatedKey] = finalValue

                if (wasTrackedAsArray && gatherStatistics) {
                    // Single value from array
                    consolidatedOutput["${consolidatedKey}_count" as String] = 1L
                    consolidatedOutput["${consolidatedKey}_distinct_count" as String] = 1L
                    def length = finalValue.toString().length()
                    consolidatedOutput["${consolidatedKey}_min_length" as String] = length as Long
                    consolidatedOutput["${consolidatedKey}_max_length" as String] = length as Long
                    consolidatedOutput["${consolidatedKey}_avg_length" as String] = length as Double
                    consolidatedOutput["${consolidatedKey}_type" as String] = determineType(single.value)
                }
            }
        } else {
            // Multiple values - process as array
            def arrayValues = validKeyedValues.collect { kv ->
                if (consolidateWithMatrixDenotorsInValue && kv.hasArrayIndex) {
                    def indices = extractIndicesPrefix(kv.originalFlattenedKey)
                    indices + kv.value.toString()
                } else {
                    kv.value.toString()
                }
            }

            processArrayValues(consolidatedKey, arrayValues as String[], consolidatedOutput)
        }
    }

    /**
     * Process array values and generate statistics
     */
    private void processArrayValues(String consolidatedKey, String[] values,
                                    Map<String, Object> consolidatedOutput) {
        def joined = values.join(arrayDelimiter)
        consolidatedOutput[consolidatedKey] = joined

        // Only gather statistics if enabled
        if (gatherStatistics) {
            def uniqueValues = values as Set

            def lengths = values.collect { it.length() }
            def minLength = lengths.min() ?: 0
            def maxLength = lengths.max() ?: 0
            def totalLength = lengths.sum() ?: 0

            consolidatedOutput["${consolidatedKey}_count" as String] = values.size() as Long
            consolidatedOutput["${consolidatedKey}_distinct_count" as String] = uniqueValues.size() as Long
            consolidatedOutput["${consolidatedKey}_min_length" as String] = minLength as Long
            consolidatedOutput["${consolidatedKey}_max_length" as String] = maxLength as Long
            consolidatedOutput["${consolidatedKey}_avg_length" as String] = totalLength / (double) values.size()
            consolidatedOutput["${consolidatedKey}_type" as String] = determineArrayType(values as List)
        }
    }

    /**
     * Extract array indices from flattened key
     */
    private String extractIndicesPrefix(String flattenedKey) {
        def prefix = new StringBuilder()
        def matcher = ALL_INDICES_PATTERN.matcher(flattenedKey)
        while (matcher.find()) {
            prefix.append("[").append(matcher.group(1)).append("]")
        }
        return prefix.toString()
    }

    /**
     * Determine type of a single value
     */
    private String determineType(Object value) {
        if (value instanceof Number) return "numeric_single_value"
        if (value instanceof Boolean) return "boolean_single_value"
        if (value instanceof String) return "string_single_value"
        return "object_single_value"
    }

    /**
     * Determine type of an array
     */
    private String determineArrayType(List<String> values) {
        def allNumbers = values.every { val ->
            if (val.equalsIgnoreCase("NaN")) {
                return false
            }
            try {
                Double.parseDouble(val)
                return true
            } catch (NumberFormatException e) {
                return false
            }
        }

        def allBooleans = values.every { val ->
            val.equalsIgnoreCase("true") || val.equalsIgnoreCase("false")
        }

        if (allNumbers) return "numeric_list_consolidated"
        if (allBooleans) return "boolean_list_consolidated"
        return "string_list_consolidated"
    }

    /**
     * Safe string conversion
     */
    private String safeToString(Object value) {
        if (value == null) return nullPlaceholder ?: ""
        try {
            if (value instanceof BigDecimal) {
                value = value.doubleValue()
            }
            if (value instanceof Map || value instanceof List) {
                return new JsonBuilder(value).toString()
            }
            return value.toString()
        } catch (Exception e) {
            return "Error_Serializing_Value"
        }
    }

    /**
     * Helper class for flattening tasks
     */
    @CompileStatic
    static class FlattenTask implements Serializable {
        private static final long serialVersionUID = 1L
        String prefix
        Object value
        int depth

        FlattenTask(String prefix, Object value, int depth) {
            this.prefix = prefix
            this.value = value
            this.depth = depth
        }
    }

    /**
     * Helper class for keyed values
     */
    @CompileStatic
    static class KeyedValue implements Serializable {
        private static final long serialVersionUID = 1L
        String originalFlattenedKey
        Object value
        boolean hasArrayIndex

        KeyedValue(String key, Object val, boolean hasIdx) {
            this.originalFlattenedKey = key
            this.value = val
            this.hasArrayIndex = hasIdx
        }
    }
}