package io.github.pierce.fidelity;

import io.github.pierce.AvroReconstructor;
import io.github.pierce.AvroSchemaFlattener;
import io.github.pierce.JsonFlattener;
import io.github.pierce.JsonReconstructor;
import io.github.pierce.MapFlattener;
import org.apache.avro.Schema;

import java.util.Map;

/**
 * The single source of truth for every code snippet {@code manifest.stacks} publishes.
 *
 * <h2>Why the snippets live here and not in the manifest</h2>
 *
 * <p>They used to live only in {@code manifest.stacks[*].code}, as free text that nothing
 * compiled, nothing executed and nothing compared to anything. One of them did not compile:
 * {@code JsonFlattener}'s only constructor is private and every factory returns a
 * {@code FluentOperation}, so {@code JsonFlattener jf = JsonFlattener.builder().build();} could
 * never have been pasted into a consumer's project. Another compiled and was wrong: it named the
 * statically CACHED {@code getFlattenedSchema(Schema)}, which no corpus row had ever executed.</p>
 *
 * <p>Held as a method body, a snippet cannot survive {@code test-compile} while being wrong about
 * types, and {@code PublishedSnippetsCompileTest} additionally runs each one and compares the
 * result to the answer the contract records. Each method RETURNS its result so nothing here is
 * dead code that javac would let rot.</p>
 *
 * <p>The {@code SNIPPET-BEGIN}/{@code SNIPPET-END} markers delimit exactly the lines the manifest
 * publishes. {@code FidelitySnippetSource} extracts between them, from this file on disk.</p>
 *
 * <p><b>The sentence above is only true because of two invariants that were missing.</b> "A snippet
 * cannot survive test-compile while being wrong" is a claim about compiled code, and marker text is
 * not compiled code: javac accepts comments too. Adversarial review pasted a byte-identical copy of
 * the MAP region into a block comment above {@link #stackMap} and changed the real body at the same
 * time; every group of the gate stayed green while a different recipe executed. The extractor now
 * requires each marker line to be UNIQUE and each region to lie strictly inside the source range of
 * the method the execution tests call. Do not add a second copy of a marker anywhere in this file,
 * commented or not - the gate will refuse to run rather than guess which one is the recipe.</p>
 */
final class PublishedStackRecipes {

    private PublishedStackRecipes() {
    }

    /** Stack A - Map level. */
    static Map<String, Object> stackMap(Map<String, Object> sourceMap) {
        // SNIPPET-BEGIN MAP
        MapFlattener f = MapFlattener.builder().build();
        Map<String, Object> flat = f.flatten(sourceMap);
        Map<String, Object> back = JsonReconstructor.quickReconstruct(flat);
        // lossless means back.equals(sourceMap)
        // SNIPPET-END MAP
        return back;
    }

    /** Stack B - JSON level. */
    static String stackJson(String jsonString) {
        // SNIPPET-BEGIN JSON
        Map<String, Object> flat = JsonFlattener.create().from(jsonString).toMap();
        String back = JsonReconstructor.builder().build().reconstructToJson(flat);
        // lossless means back is semantically equal to jsonString
        // SNIPPET-END JSON
        return back;
    }

    /** Stack C - Avro, DATA path. */
    static Map<String, Object> stackAvroData(Schema schema, Map<String, Object> datum) {
        // SNIPPET-BEGIN AVRO
        AvroSchemaFlattener.clearCache();
        AvroSchemaFlattener sf = new AvroSchemaFlattener(false, true);
        // getFlattenedSchemaNoCache, NOT getFlattenedSchema: the cached factory keys on the
        // schema's full name and returns another schema's columns when the name repeats.
        // flatSchema is the Spark-facing COLUMN LAYOUT - build your StructType from it.
        // Reconstruction takes the ORIGINAL schema, not this one.
        Schema flatSchema = sf.getFlattenedSchemaNoCache(schema);
        Map<String, Object> flat = MapFlattener.builder().build().flatten(datum);
        Map<String, Object> back = AvroReconstructor.builder().build().reconstructToMap(flat, schema);
        // SNIPPET-END AVRO
        return columns(flatSchema, back);
    }

    /**
     * Keeps {@code flatSchema} genuinely consumed outside the published region, so the snippet's
     * claim that the column layout is a real output is not quietly a dead assignment.
     */
    private static Map<String, Object> columns(Schema flatSchema, Map<String, Object> back) {
        if (flatSchema.getFields().isEmpty()) {
            throw new IllegalStateException("the flattened schema declares no columns");
        }
        return back;
    }

    /** Stack C - Avro, SCHEMA inverse path. Six corpus rows are measured here and it had no recipe. */
    static Schema stackAvroSchema(Schema schema) {
        // SNIPPET-BEGIN AVRO_SCHEMA
        AvroSchemaFlattener.clearCache();
        AvroSchemaFlattener sf = new AvroSchemaFlattener(false, true);
        Schema flatSchema = sf.getFlattenedSchemaNoCache(schema);
        Schema original = sf.reconstructOriginalSchema(flatSchema);
        // SNIPPET-END AVRO_SCHEMA
        return original;
    }
}
