package io.github.pierce.schema;

/**
 * Base type for a flatten that could not complete.
 *
 * <p>Typed rather than a bare {@code RuntimeException} because these are conditions a caller
 * processing untrusted producer schemas in bulk must be able to catch, classify and report per
 * schema — quarantining one bad {@code .avsc} instead of failing a batch. Previously the same
 * conditions surfaced as {@code StackOverflowError} or an unbounded heap climb, neither of which
 * is catchable in any useful sense: the first unwinds through arbitrary frames, the second takes
 * the JVM.</p>
 */
public class SchemaFlattenException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String schemaName;
    private final String path;

    public SchemaFlattenException(String message, String schemaName, String path) {
        super(message);
        this.schemaName = schemaName;
        this.path = path;
    }

    /** Full name of the schema being flattened, for per-schema quarantine. */
    public String getSchemaName() {
        return schemaName;
    }

    /** The path at which the problem was detected. */
    public String getPath() {
        return path;
    }
}
