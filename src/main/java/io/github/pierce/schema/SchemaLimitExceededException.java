package io.github.pierce.schema;

/**
 * A schema exceeded a configured traversal limit.
 *
 * <p>Depth and field-count limits defend against schemas that are not recursive but are still
 * pathological — deeply nested generated types, or a wide cross-product expanding to hundreds of
 * thousands of columns. A bounded, typed failure beats an {@code OutOfMemoryError}, which takes
 * the whole JVM rather than one schema.</p>
 */
public class SchemaLimitExceededException extends SchemaFlattenException {

    private static final long serialVersionUID = 1L;

    private final String limitName;
    private final long limit;
    private final long actual;

    public SchemaLimitExceededException(String limitName, long limit, long actual,
                                        String schemaName, String path) {
        super(String.format(
                "Schema '%s' exceeded %s: limit %d, reached %d at path '%s'. Raise it via "
                        + "FlattenOptions if this schema is legitimate.",
                schemaName, limitName, limit, actual, path),
                schemaName, path);
        this.limitName = limitName;
        this.limit = limit;
        this.actual = actual;
    }

    public String getLimitName() {
        return limitName;
    }

    public long getLimit() {
        return limit;
    }

    public long getActual() {
        return actual;
    }
}
