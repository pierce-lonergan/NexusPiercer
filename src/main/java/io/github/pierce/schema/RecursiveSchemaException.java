package io.github.pierce.schema;

/**
 * A named type contains itself, directly or through a cycle.
 *
 * <p>Avro explicitly permits this — {@code record Node { Node next; }} is legal — but a flattened
 * form of it does not exist, because the path set is infinite. Detected on the way down by
 * tracking which named types are currently open, so it fails at the point of recursion rather
 * than after exhausting the stack.</p>
 *
 * <p>The message names the cycle and the path, so an operator can fix the schema without reading
 * a stack trace.</p>
 */
public class RecursiveSchemaException extends SchemaFlattenException {

    private static final long serialVersionUID = 1L;

    private final String recursiveType;

    public RecursiveSchemaException(String recursiveType, String schemaName, String path) {
        super(String.format(
                "Schema '%s' is recursive: type '%s' contains itself at path '%s'. A recursive "
                        + "schema has no finite flattened form. Break the cycle, or exclude the "
                        + "recursive branch before flattening.",
                schemaName, recursiveType, path),
                schemaName, path);
        this.recursiveType = recursiveType;
    }

    /** Full name of the type that recurred. */
    public String getRecursiveType() {
        return recursiveType;
    }
}
