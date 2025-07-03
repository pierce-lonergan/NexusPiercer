package io.github.pierce;

import io.github.pierce.files.FileFinder;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Optimized Avro Schema Flattener that produces schemas compatible with JsonFlattenerConsolidator output.
 * This version is thread-safe, uses caching for better performance, maintains field order, and
 * provides comprehensive metadata collection and Excel export capabilities.
 *
 * Key feature: Automatically converts primitive types to STRING when they are descendants of arrays,
 * since JsonFlattenerConsolidator consolidates array values into comma-separated strings.
 */
public class AvroSchemaFlattener {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaFlattener.class);

    private static final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();

    private final boolean includeArrayStatistics;
    private final Set<String> arrayFieldNames;

    // Enhanced metadata tracking
    private final List<FieldMetadata> fieldMetadataList;
    private final Map<String, TypeTransformation> typeTransformations;
    private final Set<String> fieldsWithinArrays;
    private final SchemaStatistics schemaStats;

    private static final String COUNT_SUFFIX = "_count";
    private static final String DISTINCT_COUNT_SUFFIX = "_distinct_count";
    private static final String MIN_LENGTH_SUFFIX = "_min_length";
    private static final String MAX_LENGTH_SUFFIX = "_max_length";
    private static final String AVG_LENGTH_SUFFIX = "_avg_length";
    private static final String TYPE_SUFFIX = "_type";

    /**
     * No-argument constructor that defaults to not including array statistics
     */
    public AvroSchemaFlattener() {
        this(false);
    }

    public AvroSchemaFlattener(boolean includeArrayStatistics) {
        this.includeArrayStatistics = includeArrayStatistics;
        this.arrayFieldNames = new HashSet<>();
        this.fieldMetadataList = new ArrayList<>();
        this.typeTransformations = new HashMap<>();
        this.fieldsWithinArrays = new HashSet<>();
        this.schemaStats = new SchemaStatistics();
    }

    public Schema getFlattenedSchema(String schemaPath) throws IOException {
        String cacheKey = schemaPath + ":" + this.includeArrayStatistics;
        return schemaCache.computeIfAbsent(cacheKey, path -> {
            try {
                InputStream is = FileFinder.findFile(schemaPath);
                Schema schema = new Schema.Parser().parse(is);
                return flattenSchema(schema);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load schema: " + schemaPath, e);
            }
        });
    }

    public Schema getFlattenedSchema(Schema schema) {
        String cacheKey = schema.getFullName() + ":" + this.includeArrayStatistics;
        return schemaCache.computeIfAbsent(cacheKey, k -> flattenSchema(schema));
    }

    private Schema flattenSchema(Schema schema) {
        // Clear all tracking collections
        arrayFieldNames.clear();
        fieldMetadataList.clear();
        typeTransformations.clear();
        fieldsWithinArrays.clear();
        schemaStats.reset();

        // Set basic schema statistics
        schemaStats.originalSchemaName = schema.getFullName();
        schemaStats.originalFieldCount = schema.getFields().size();

        List<Field> flattenedFields = new ArrayList<>();

        // Process fields in order to maintain field ordering
        for (Field field : schema.getFields()) {
            processFieldRecursively("", field, field.schema(), flattenedFields, false, 0, "");
        }

        String flatNamespace = schema.getNamespace() + ".flattened";
        String flatName = "Flattened" + schema.getName();
        Schema flattenedSchema = Schema.createRecord(flatName, "Flattened version of " + schema.getName(), flatNamespace, false);
        flattenedSchema.setFields(flattenedFields);

        // Update final statistics
        schemaStats.flattenedFieldCount = flattenedFields.size();
        schemaStats.arrayFieldCount = arrayFieldNames.size();
        schemaStats.fieldsWithinArraysCount = fieldsWithinArrays.size();

        LOG.debug("Flattened schema {} with {} fields", schema.getName(), flattenedFields.size());
        return flattenedSchema;
    }

    private void processFieldRecursively(String prefix, Field field, Schema fieldSchema,
                                         List<Field> flattenedFields, boolean isWithinArray,
                                         int depth, String path) {
        String fieldName = prefix.isEmpty() ? field.name() : prefix + "_" + field.name();
        String currentPath = path.isEmpty() ? field.name() : path + "." + field.name();

        // Update max depth
        schemaStats.maxNestingDepth = Math.max(schemaStats.maxNestingDepth, depth);

        switch (fieldSchema.getType()) {
            case RECORD:
                schemaStats.recordFieldCount++;
                // For records, immediately process their sub-fields to maintain order
                for (Field subField : fieldSchema.getFields()) {
                    processFieldRecursively(fieldName, subField, subField.schema(),
                            flattenedFields, isWithinArray, depth + 1, currentPath);
                }
                break;

            case UNION:
                Schema actualType = getNonNullType(fieldSchema);
                if (actualType != null) {
                    processFieldRecursively(prefix, field, actualType, flattenedFields, isWithinArray, depth, path);
                }
                break;

            case ARRAY:
                arrayFieldNames.add(fieldName);

                // Create metadata for the array field
                FieldMetadata arrayMetadata = new FieldMetadata(
                        fieldName, currentPath, depth, Type.ARRAY, Type.STRING,
                        field.doc(), isNullable(fieldSchema), true, isWithinArray
                );
                fieldMetadataList.add(arrayMetadata);

                // Add the array field itself first
                addField(flattenedFields, fieldName, Schema.create(Type.STRING), field.doc(), false);

                // Add statistics fields immediately after the array field if needed
                if (includeArrayStatistics) {
                    addArrayStatisticsFields(flattenedFields, fieldName);
                }

                // Then process the array's element type - NOW WITHIN ARRAY CONTEXT
                Schema elementType = getNonNullType(fieldSchema.getElementType());
                if (elementType != null && elementType.getType() == Type.RECORD) {
                    // For arrays of records, process each field of the record
                    for (Field subField : elementType.getFields()) {
                        processFieldRecursively(fieldName, subField, subField.schema(),
                                flattenedFields, true, depth + 1, currentPath); // NOW WITHIN ARRAY
                    }
                }
                break;

            case MAP:
                schemaStats.mapFieldCount++;
                FieldMetadata mapMetadata = new FieldMetadata(
                        fieldName, currentPath, depth, Type.MAP, Type.STRING,
                        field.doc(), isNullable(fieldSchema), false, isWithinArray
                );
                fieldMetadataList.add(mapMetadata);
                addField(flattenedFields, fieldName, Schema.create(Type.STRING), field.doc(), isWithinArray);
                break;

            case ENUM:
                schemaStats.enumFieldCount++;
                FieldMetadata enumMetadata = new FieldMetadata(
                        fieldName, currentPath, depth, Type.ENUM, Type.STRING,
                        field.doc(), isNullable(fieldSchema), false, isWithinArray
                );
                fieldMetadataList.add(enumMetadata);
                addField(flattenedFields, fieldName, Schema.create(Type.STRING), field.doc(), isWithinArray);
                break;

            case FIXED:
                FieldMetadata fixedMetadata = new FieldMetadata(
                        fieldName, currentPath, depth, Type.FIXED, Type.BYTES,
                        field.doc(), isNullable(fieldSchema), false, isWithinArray
                );
                fieldMetadataList.add(fixedMetadata);
                addField(flattenedFields, fieldName, Schema.create(Type.BYTES), field.doc(), isWithinArray);
                break;

            default: // Primitives
                Type originalType = fieldSchema.getType();
                Type flattenedType = isWithinArray ? Type.STRING : originalType;

                FieldMetadata primitiveMetadata = new FieldMetadata(
                        fieldName, currentPath, depth, originalType, flattenedType,
                        field.doc(), isNullable(fieldSchema), false, isWithinArray
                );
                fieldMetadataList.add(primitiveMetadata);

                if (isWithinArray && originalType != flattenedType) {
                    typeTransformations.put(fieldName, new TypeTransformation(
                            fieldName, originalType, flattenedType, "Array descendant - converted to STRING"
                    ));
                }

                addField(flattenedFields, fieldName, fieldSchema, field.doc(), isWithinArray);
                break;
        }
    }

    private void addArrayStatisticsFields(List<Field> flattenedFields, String baseFieldName) {
        // Statistics fields always keep their original types
        addField(flattenedFields, baseFieldName + COUNT_SUFFIX, Schema.create(Type.LONG),
                "Count of array elements", false);
        addField(flattenedFields, baseFieldName + DISTINCT_COUNT_SUFFIX, Schema.create(Type.LONG),
                "Count of distinct array elements", false);
        addField(flattenedFields, baseFieldName + MIN_LENGTH_SUFFIX, Schema.create(Type.LONG),
                "Minimum length of string representations of array elements", false);
        addField(flattenedFields, baseFieldName + MAX_LENGTH_SUFFIX, Schema.create(Type.LONG),
                "Maximum length of string representations of array elements", false);
        addField(flattenedFields, baseFieldName + AVG_LENGTH_SUFFIX, Schema.create(Type.DOUBLE),
                "Average length of string representations of array elements", false);
        addField(flattenedFields, baseFieldName + TYPE_SUFFIX, Schema.create(Type.STRING),
                "Type classification of array elements", false);
    }

    private void addField(List<Field> fields, String name, Schema type, String doc, boolean isWithinArray) {
        // Check for duplicates
        for (Field existing : fields) {
            if (existing.name().equals(name)) {
                LOG.trace("Field {} already exists, skipping", name);
                return;
            }
        }

        // Track fields within arrays
        if (isWithinArray) {
            fieldsWithinArrays.add(name);
        }

        // If this field is within an array and it's a primitive type, convert to STRING
        Schema actualType = type;
        if (isWithinArray && isPrimitiveType(type)) {
            LOG.debug("Converting field {} from {} to STRING because it's within an array",
                    name, type.getType());
            actualType = Schema.create(Type.STRING);
        }

        Schema nullableType = wrapNullable(actualType);
        Field newField = new Field(name, nullableType, doc, (Object) null);
        fields.add(newField);
    }

    private boolean isPrimitiveType(Schema schema) {
        Type type = schema.getType();
        return type == Type.INT || type == Type.LONG || type == Type.FLOAT ||
                type == Type.DOUBLE || type == Type.BOOLEAN || type == Type.BYTES ||
                type == Type.FIXED || type == Type.STRING;
    }

    private Schema wrapNullable(Schema schema) {
        if (schema.isNullable()) {
            return schema;
        }
        return Schema.createUnion(Schema.create(Type.NULL), schema);
    }

    private Schema getNonNullType(Schema schema) {
        if (schema.getType() != Type.UNION) {
            return schema;
        }
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() != Type.NULL) {
                return branch;
            }
        }
        return null;
    }

    private boolean isNullable(Schema schema) {
        return schema.getType() == Type.UNION &&
                schema.getTypes().stream().anyMatch(s -> s.getType() == Type.NULL);
    }

    // Public accessors for metadata
    public Set<String> getArrayFieldNames() {
        return new HashSet<>(arrayFieldNames);
    }

    public Set<String> getFieldsWithinArrays() {
        return new HashSet<>(fieldsWithinArrays);
    }

    public List<FieldMetadata> getFieldMetadata() {
        return new ArrayList<>(fieldMetadataList);
    }

    public Map<String, TypeTransformation> getTypeTransformations() {
        return new HashMap<>(typeTransformations);
    }

    public SchemaStatistics getSchemaStatistics() {
        return schemaStats.copy();
    }

    /**
     * Export comprehensive schema analysis to Excel
     */
    public void exportToExcel(String filePath) throws IOException {
        try (Workbook workbook = new XSSFWorkbook();
             FileOutputStream fileOut = new FileOutputStream(filePath)) {

            // Create styles
            CellStyle headerStyle = createHeaderStyle(workbook);
            CellStyle titleStyle = createTitleStyle(workbook);
            CellStyle highlightStyle = createHighlightStyle(workbook);

            // Sheet 1: Schema Overview
            createOverviewSheet(workbook, titleStyle, headerStyle);

            // Sheet 2: Field Catalog
            createFieldCatalogSheet(workbook, titleStyle, headerStyle, highlightStyle);

            // Sheet 3: Array Analysis
            createArrayAnalysisSheet(workbook, titleStyle, headerStyle);

            // Sheet 4: Type Transformations
            createTypeTransformationsSheet(workbook, titleStyle, headerStyle);

            // Sheet 5: Nesting Analysis
            createNestingAnalysisSheet(workbook, titleStyle, headerStyle);

            workbook.write(fileOut);
            LOG.info("Schema analysis exported to: {}", filePath);
        }
    }

    private void createOverviewSheet(Workbook workbook, CellStyle titleStyle, CellStyle headerStyle) {
        Sheet sheet = workbook.createSheet("Schema Overview");
        int rowNum = 0;

        // Title
        Row titleRow = sheet.createRow(rowNum++);
        Cell titleCell = titleRow.createCell(0);
        titleCell.setCellValue("Avro Schema Flattening Analysis");
        titleCell.setCellStyle(titleStyle);
        sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, 3));
        rowNum++;

        // Timestamp
        Row timestampRow = sheet.createRow(rowNum++);
        timestampRow.createCell(0).setCellValue("Generated:");
        timestampRow.createCell(1).setCellValue(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        rowNum++;

        // Basic Statistics
        createStatisticsSection(sheet, rowNum, headerStyle);

        // Auto-size columns
        for (int i = 0; i < 4; i++) {
            sheet.autoSizeColumn(i);
        }
    }

    private void createStatisticsSection(Sheet sheet, int startRow, CellStyle headerStyle) {
        int rowNum = startRow;

        Row headerRow = sheet.createRow(rowNum++);
        Cell headerCell = headerRow.createCell(0);
        headerCell.setCellValue("Schema Statistics");
        headerCell.setCellStyle(headerStyle);
        sheet.addMergedRegion(new CellRangeAddress(headerRow.getRowNum(), headerRow.getRowNum(), 0, 1));

        createStatRow(sheet, rowNum++, "Original Schema Name", schemaStats.originalSchemaName);
        createStatRow(sheet, rowNum++, "Original Field Count", String.valueOf(schemaStats.originalFieldCount));
        createStatRow(sheet, rowNum++, "Flattened Field Count", String.valueOf(schemaStats.flattenedFieldCount));
        createStatRow(sheet, rowNum++, "Field Expansion Ratio",
                String.format("%.2f", (double) schemaStats.flattenedFieldCount / schemaStats.originalFieldCount));
        createStatRow(sheet, rowNum++, "Maximum Nesting Depth", String.valueOf(schemaStats.maxNestingDepth));
        createStatRow(sheet, rowNum++, "Array Fields", String.valueOf(schemaStats.arrayFieldCount));
        createStatRow(sheet, rowNum++, "Fields Within Arrays", String.valueOf(schemaStats.fieldsWithinArraysCount));
        createStatRow(sheet, rowNum++, "Record Fields", String.valueOf(schemaStats.recordFieldCount));
        createStatRow(sheet, rowNum++, "Map Fields", String.valueOf(schemaStats.mapFieldCount));
        createStatRow(sheet, rowNum++, "Enum Fields", String.valueOf(schemaStats.enumFieldCount));
        createStatRow(sheet, rowNum++, "Array Statistics Included", includeArrayStatistics ? "Yes" : "No");
    }

    private void createStatRow(Sheet sheet, int rowNum, String label, String value) {
        Row row = sheet.createRow(rowNum);
        row.createCell(0).setCellValue(label);
        row.createCell(1).setCellValue(value);
    }

    private void createFieldCatalogSheet(Workbook workbook, CellStyle titleStyle, CellStyle headerStyle, CellStyle highlightStyle) {
        Sheet sheet = workbook.createSheet("Field Catalog");
        int rowNum = 0;

        // Title
        Row titleRow = sheet.createRow(rowNum++);
        Cell titleCell = titleRow.createCell(0);
        titleCell.setCellValue("Complete Field Catalog");
        titleCell.setCellStyle(titleStyle);
        sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, 7));
        rowNum++;

        // Headers
        Row headerRow = sheet.createRow(rowNum++);
        String[] headers = {"Flattened Name", "Original Path", "Depth", "Original Type",
                "Flattened Type", "Nullable", "Is Array", "Within Array", "Documentation"};
        for (int i = 0; i < headers.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers[i]);
            cell.setCellStyle(headerStyle);
        }

        // Data rows
        for (FieldMetadata metadata : fieldMetadataList) {
            Row row = sheet.createRow(rowNum++);
            row.createCell(0).setCellValue(metadata.flattenedName);
            row.createCell(1).setCellValue(metadata.originalPath);
            row.createCell(2).setCellValue(metadata.nestingDepth);
            row.createCell(3).setCellValue(metadata.originalType.toString());
            row.createCell(4).setCellValue(metadata.flattenedType.toString());
            row.createCell(5).setCellValue(metadata.isNullable ? "Yes" : "No");
            row.createCell(6).setCellValue(metadata.isArray ? "Yes" : "No");
            Cell withinArrayCell = row.createCell(7);
            withinArrayCell.setCellValue(metadata.isWithinArray ? "Yes" : "No");
            if (metadata.isWithinArray) {
                withinArrayCell.setCellStyle(highlightStyle);
            }
            row.createCell(8).setCellValue(metadata.documentation != null ? metadata.documentation : "");
        }

        // Auto-size columns
        for (int i = 0; i < headers.length; i++) {
            sheet.autoSizeColumn(i);
        }
    }

    private void createArrayAnalysisSheet(Workbook workbook, CellStyle titleStyle, CellStyle headerStyle) {
        Sheet sheet = workbook.createSheet("Array Analysis");
        int rowNum = 0;

        // Title
        Row titleRow = sheet.createRow(rowNum++);
        Cell titleCell = titleRow.createCell(0);
        titleCell.setCellValue("Array Fields Analysis");
        titleCell.setCellStyle(titleStyle);
        sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, 3));
        rowNum++;

        // Array fields section
        Row sectionHeader = sheet.createRow(rowNum++);
        Cell sectionCell = sectionHeader.createCell(0);
        sectionCell.setCellValue("Array Fields");
        sectionCell.setCellStyle(headerStyle);

        for (String arrayField : arrayFieldNames) {
            Row row = sheet.createRow(rowNum++);
            row.createCell(0).setCellValue(arrayField);

            // Find descendants
            List<String> descendants = fieldsWithinArrays.stream()
                    .filter(f -> f.startsWith(arrayField + "_"))
                    .collect(Collectors.toList());
            row.createCell(1).setCellValue("Descendants: " + descendants.size());
            row.createCell(2).setCellValue(String.join(", ", descendants));
        }

        rowNum++;

        // Fields within arrays section
        Row withinHeader = sheet.createRow(rowNum++);
        Cell withinCell = withinHeader.createCell(0);
        withinCell.setCellValue("All Fields Within Arrays");
        withinCell.setCellStyle(headerStyle);

        for (String field : fieldsWithinArrays) {
            Row row = sheet.createRow(rowNum++);
            row.createCell(0).setCellValue(field);

            // Find which array it belongs to
            String parentArray = arrayFieldNames.stream()
                    .filter(a -> field.startsWith(a + "_"))
                    .findFirst()
                    .orElse("Unknown");
            row.createCell(1).setCellValue("Parent Array: " + parentArray);
        }

        // Auto-size columns
        for (int i = 0; i < 3; i++) {
            sheet.autoSizeColumn(i);
        }
    }

    private void createTypeTransformationsSheet(Workbook workbook, CellStyle titleStyle, CellStyle headerStyle) {
        Sheet sheet = workbook.createSheet("Type Transformations");
        int rowNum = 0;

        // Title
        Row titleRow = sheet.createRow(rowNum++);
        Cell titleCell = titleRow.createCell(0);
        titleCell.setCellValue("Type Transformations During Flattening");
        titleCell.setCellStyle(titleStyle);
        sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, 3));
        rowNum++;

        // Headers
        Row headerRow = sheet.createRow(rowNum++);
        String[] headers = {"Field Name", "Original Type", "Transformed Type", "Reason"};
        for (int i = 0; i < headers.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers[i]);
            cell.setCellStyle(headerStyle);
        }

        // Data rows
        for (TypeTransformation transformation : typeTransformations.values()) {
            Row row = sheet.createRow(rowNum++);
            row.createCell(0).setCellValue(transformation.fieldName);
            row.createCell(1).setCellValue(transformation.originalType.toString());
            row.createCell(2).setCellValue(transformation.transformedType.toString());
            row.createCell(3).setCellValue(transformation.reason);
        }

        // Auto-size columns
        for (int i = 0; i < headers.length; i++) {
            sheet.autoSizeColumn(i);
        }
    }

    private void createNestingAnalysisSheet(Workbook workbook, CellStyle titleStyle, CellStyle headerStyle) {
        Sheet sheet = workbook.createSheet("Nesting Analysis");
        int rowNum = 0;

        // Title
        Row titleRow = sheet.createRow(rowNum++);
        Cell titleCell = titleRow.createCell(0);
        titleCell.setCellValue("Schema Nesting Analysis");
        titleCell.setCellStyle(titleStyle);
        sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, 2));
        rowNum++;

        // Group by depth
        Map<Integer, List<FieldMetadata>> fieldsByDepth = fieldMetadataList.stream()
                .collect(Collectors.groupingBy(f -> f.nestingDepth));

        for (int depth = 0; depth <= schemaStats.maxNestingDepth; depth++) {
            List<FieldMetadata> fieldsAtDepth = fieldsByDepth.getOrDefault(depth, new ArrayList<>());

            Row depthHeader = sheet.createRow(rowNum++);
            Cell depthCell = depthHeader.createCell(0);
            depthCell.setCellValue("Depth " + depth + " (" + fieldsAtDepth.size() + " fields)");
            depthCell.setCellStyle(headerStyle);

            for (FieldMetadata field : fieldsAtDepth) {
                Row row = sheet.createRow(rowNum++);
                row.createCell(0).setCellValue("  " + field.flattenedName);
                row.createCell(1).setCellValue(field.originalPath);
                row.createCell(2).setCellValue(field.originalType.toString());
            }
            rowNum++; // Empty row between depths
        }

        // Auto-size columns
        for (int i = 0; i < 3; i++) {
            sheet.autoSizeColumn(i);
        }
    }

    // Style creation methods
    private CellStyle createHeaderStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setBold(true);
        style.setFont(font);
        style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        return style;
    }

    private CellStyle createTitleStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setBold(true);
        font.setFontHeightInPoints((short) 14);
        style.setFont(font);
        return style;
    }

    private CellStyle createHighlightStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        style.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        return style;
    }

    public static void clearCache() {
        schemaCache.clear();
    }

    // Inner classes for metadata tracking
    public static class FieldMetadata {
        public final String flattenedName;
        public final String originalPath;
        public final int nestingDepth;
        public final Type originalType;
        public final Type flattenedType;
        public final String documentation;
        public final boolean isNullable;
        public final boolean isArray;
        public final boolean isWithinArray;

        public FieldMetadata(String flattenedName, String originalPath, int nestingDepth,
                             Type originalType, Type flattenedType, String documentation,
                             boolean isNullable, boolean isArray, boolean isWithinArray) {
            this.flattenedName = flattenedName;
            this.originalPath = originalPath;
            this.nestingDepth = nestingDepth;
            this.originalType = originalType;
            this.flattenedType = flattenedType;
            this.documentation = documentation;
            this.isNullable = isNullable;
            this.isArray = isArray;
            this.isWithinArray = isWithinArray;
        }
    }

    public static class TypeTransformation {
        public final String fieldName;
        public final Type originalType;
        public final Type transformedType;
        public final String reason;

        public TypeTransformation(String fieldName, Type originalType, Type transformedType, String reason) {
            this.fieldName = fieldName;
            this.originalType = originalType;
            this.transformedType = transformedType;
            this.reason = reason;
        }
    }

    public static class SchemaStatistics {
        public String originalSchemaName = "";
        public int originalFieldCount = 0;
        public int flattenedFieldCount = 0;
        public int maxNestingDepth = 0;
        public int arrayFieldCount = 0;
        public int fieldsWithinArraysCount = 0;
        public int recordFieldCount = 0;
        public int mapFieldCount = 0;
        public int enumFieldCount = 0;

        public void reset() {
            originalSchemaName = "";
            originalFieldCount = 0;
            flattenedFieldCount = 0;
            maxNestingDepth = 0;
            arrayFieldCount = 0;
            fieldsWithinArraysCount = 0;
            recordFieldCount = 0;
            mapFieldCount = 0;
            enumFieldCount = 0;
        }

        public SchemaStatistics copy() {
            SchemaStatistics copy = new SchemaStatistics();
            copy.originalSchemaName = this.originalSchemaName;
            copy.originalFieldCount = this.originalFieldCount;
            copy.flattenedFieldCount = this.flattenedFieldCount;
            copy.maxNestingDepth = this.maxNestingDepth;
            copy.arrayFieldCount = this.arrayFieldCount;
            copy.fieldsWithinArraysCount = this.fieldsWithinArraysCount;
            copy.recordFieldCount = this.recordFieldCount;
            copy.mapFieldCount = this.mapFieldCount;
            copy.enumFieldCount = this.enumFieldCount;
            return copy;
        }
    }
}