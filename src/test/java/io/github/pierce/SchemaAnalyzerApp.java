package io.github.pierce;

import org.apache.avro.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A command-line application to analyze and flatten an Avro schema.
 *
 * <p>This application takes an Avro schema file (.avsc) as input and produces
 * a flattened version of the schema and a detailed Excel analysis report.
 * It uses the {@link AvroSchemaFlattener} class to perform the core logic.
 * </p>
 *
 * <p><b>Usage:</b></p>
 * <pre>
 * java -cp your-jar-with-dependencies.jar io.github.pierce.SchemaAnalyzerApp <path-to-input-schema.avsc> <path-to-output-directory>
 * </pre>
 */
public class SchemaAnalyzerApp {

    public static void main(String[] args) {


        String inputSchemaLocation = "src/test/avro/product_schema.avsc";
        String outputDirLocation = "src/test/avro";

        Path inputPath = Paths.get(inputSchemaLocation);
        Path outputPath = Paths.get(outputDirLocation);

        if (!Files.exists(inputPath) || !Files.isRegularFile(inputPath)) {
            System.err.println("ERROR: Input schema file not found or is not a regular file: " + inputPath);
            System.exit(1);
        }

        try {
            // --- 2. Prepare Output Directory ---
            Files.createDirectories(outputPath);
            System.out.println("Output directory prepared: " + outputPath.toAbsolutePath());
        } catch (IOException e) {
            System.err.println("ERROR: Could not create output directory: " + outputPath);
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("Starting schema analysis for: " + inputPath.getFileName());

        try {
            // --- 3. Instantiate and Run the Flattener ---
            // We use true to include array statistics, which is more informative for a standalone tool.
            AvroSchemaFlattener flattener = new AvroSchemaFlattener(false);

            // Process the schema. This populates all the metadata inside the flattener instance.
            Schema flattenedSchema = flattener.getFlattenedSchema(inputSchemaLocation);
            System.out.println("Schema successfully flattened.");

            // --- 4. Generate Output Files ---
            String baseName = getBaseFileName(inputPath);

            // a) Save the flattened schema to a file
            Path flattenedSchemaPath = outputPath.resolve("Flattened_" + baseName + ".avsc");
            // The 'true' argument pretty-prints the JSON schema
            Files.writeString(flattenedSchemaPath, flattenedSchema.toString(true));
            System.out.println("-> Flattened schema saved to: " + flattenedSchemaPath);

            // b) Export the detailed analysis to Excel
            Path excelReportPath = outputPath.resolve(baseName + "_Analysis.xlsx");
            flattener.exportToExcel(excelReportPath.toString());
            System.out.println("-> Excel analysis report saved to: " + excelReportPath);

            System.out.println("\nAnalysis complete!");

        } catch (Exception e) {
            System.err.println("\nAn error occurred during schema processing:");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Helper method to get the file name without its extension.
     * e.g., "my_schema.avsc" -> "my_schema"
     */
    private static String getBaseFileName(Path path) {
        String fileName = path.getFileName().toString();
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex > 0) {
            return fileName.substring(0, dotIndex);
        }
        return fileName;
    }
}