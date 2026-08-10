package io.github.pierce.schema;

/**
 * Called once per leaf, in emission order, while the schema is being flattened.
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

    /** An interceptor that does nothing. */
    static LeafInterceptor noop() {
        return field -> { };
    }

    /** Runs this interceptor, then {@code next}. */
    default LeafInterceptor andThen(LeafInterceptor next) {
        return field -> {
            this.onLeaf(field);
            next.onLeaf(field);
        };
    }
}
