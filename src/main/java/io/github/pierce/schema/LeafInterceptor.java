package io.github.pierce.schema;

/**
 * Called once per emitted column, in emission order, while the schema is being flattened.
 *
 * <p>"Emission order" means the order the caller's sink receives columns, and the
 * {@link FlattenedField#position()} it reads is the final one. Injected columns are included:
 * a governance rule that could not see an operator column would have a blind spot exactly where
 * audit columns live. This was not true until it was fixed — interception ran inside the
 * traversal, on the wrong side of the injection merge, so injected columns were never offered and
 * positions were pre-renumber source ordinals.</p>
 *
 * <p>One asymmetry worth knowing: {@link TypeMapper} runs on source leaves only. An injected
 * column arrives carrying whatever {@code mappedType} its builder set, because the caller
 * constructed it and mapping it again would overwrite a deliberate choice.</p>
 *
 * <p>Exists so enrichment happens during traversal rather than in a second sweep over the
 * results. That is not only cheaper — it is the difference between annotating a field while its
 * provenance is in hand and re-deriving that provenance from a rendered name afterwards, which is
 * precisely the ambiguity the injective encoding exists to eliminate.</p>
 *
 * <p>The typical use is short-circuiting: recognise an audit column and stamp its canonical
 * identity before any downstream matching runs.</p>
 *
 * <pre>{@code
 * FlattenOptions.builder()
 *     .leafInterceptor(field -> auditRegistry.lookup(field.name())
 *             .ifPresent(a -> {
 *                 field.properties().put("logicalName", a.logicalName());
 *                 field.properties().put("deid", a.deid());
 *             }))
 *     .build();
 * }</pre>
 *
 * <p>The field is fully populated when the interceptor sees it, including its mapped type.
 * {@link FlattenedField#properties()} is mutable; nothing else is. Throwing aborts the flatten —
 * intentional, so a governance rule can veto a schema outright rather than let it through
 * half-annotated.</p>
 */
/*
 * Serializable for the same reason as TypeMapper: FlattenOptions travels to Spark executors, and
 * an interceptor that cannot travel with it would fail only in distributed execution.
 */
@FunctionalInterface
public interface LeafInterceptor extends java.io.Serializable {

    /** @param field the leaf just produced; its property map may be mutated */
    void onLeaf(FlattenedField field);

    /**
     * The shared do-nothing interceptor.
     *
     * <p>A single instance, not a fresh lambda per call, so {@code == NOOP} is a reliable test for
     * "no interceptor configured". {@code noop()} used to return a new lambda each time, which
     * made every such identity check silently false — including one written in
     * {@code EnrichedSchemaFlattener} to skip wrapping the sink. The check compiled, read
     * correctly, and could never fire.</p>
     */
    LeafInterceptor NOOP = field -> { };

    /** An interceptor that does nothing. Always the same instance; see {@link #NOOP}. */
    static LeafInterceptor noop() {
        return NOOP;
    }

    /** Runs this interceptor, then {@code next}. */
    default LeafInterceptor andThen(LeafInterceptor next) {
        return field -> {
            this.onLeaf(field);
            next.onLeaf(field);
        };
    }
}
