package io.github.pierce.schema;

import org.apache.avro.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Acceptance tests for the enrichment-aware flattener.
 *
 * <p>Each nested class corresponds to a requirement, and every test asserts the behaviour a
 * schema-governance pipeline actually depends on rather than that the code ran. The recurring
 * theme: a flattened NAME is a rendering, not a data structure, so anything a consumer needs
 * must be carried as a field rather than left to be re-parsed out of the name.</p>
 */
@DisplayName("EnrichedSchemaFlattener")
class EnrichedSchemaFlattenerTest {

    /** Snake_case throughout, which is the case that broke the old encoding. */
    private static Schema orderSchema() {
        return new Schema.Parser().parse("""
            {"type":"record","name":"Order","namespace":"gov",
             "doc":"An order record",
             "fields":[
               {"name":"order_id","type":"string","x-erwin-physical-type":"VARCHAR(36)",
                "x-chase-dataelem":"DE-1001"},
               {"name":"created_at","type":["null","long"],"default":null,
                "doc":"Creation timestamp"},
               {"name":"customer","doc":"The buyer","type":
                 {"type":"record","name":"Customer","fields":[
                   {"name":"full_name","type":"string","x-chase-dataelem":"DE-2001"},
                   {"name":"email_address","type":"string"}]}},
               {"name":"line_items","type":{"type":"array","items":
                 {"type":"record","name":"LineItem","fields":[
                   {"name":"sku_code","type":"string"},
                   {"name":"quantity","type":"int"}]}}},
               {"name":"tags","type":{"type":"array","items":"string"}}
             ]}
            """);
    }

    private static List<FlattenedField> flatten(FlattenOptions opts) {
        return new EnrichedSchemaFlattener(opts).flatten(orderSchema());
    }

    private static FlattenedField byName(List<FlattenedField> fields, String name) {
        return fields.stream().filter(f -> f.flattenedName().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError(
                        "no field '" + name + "' in " + fields.stream()
                                .map(FlattenedField::flattenedName).toList()));
    }

    @Nested
    @DisplayName("P0.2 — custom Avro properties survive the flatten")
    class CustomProperties {

        /**
         * The properties ARE the enrichment. Before this, {@code getObjectProps} was never called
         * anywhere in the library, so a producer's x- properties were discarded during the walk
         * and unrecoverable from the output.
         */
        @Test
        @DisplayName("a field's own custom properties are carried through")
        void fieldPropertiesSurvive() {
            List<FlattenedField> fields = flatten(FlattenOptions.gAvroParity());

            assertThat(byName(fields, "order_id").property("x-erwin-physical-type"))
                    .contains("VARCHAR(36)");
            assertThat(byName(fields, "order_id").property("x-chase-dataelem"))
                    .contains("DE-1001");
        }

        @Test
        @DisplayName("properties survive on nested leaves too")
        void nestedPropertiesSurvive() {
            assertThat(byName(flatten(FlattenOptions.gAvroParity()), "customer_full_name")
                    .property("x-chase-dataelem"))
                    .contains("DE-2001");
        }

        @Test
        @DisplayName("structural keys are not mistaken for user metadata")
        void reservedKeysAreNotLeaked() {
            assertThat(byName(flatten(FlattenOptions.gAvroParity()), "order_id").properties())
                    .doesNotContainKeys("type", "name", "doc", "default");
        }
    }

    @Nested
    @DisplayName("P0.3 — documentation inheritance")
    class DocInheritance {

        @Test
        @DisplayName("a leaf with no doc inherits its nearest ancestor record's")
        void inheritsFromParentRecord() {
            FlattenedField f = byName(flatten(FlattenOptions.gAvroParity()), "customer_full_name");
            assertThat(f.doc()).contains("The buyer");
            assertThat(f.isDocInherited()).isTrue();
        }

        @Test
        @DisplayName("a leaf's own doc wins, and is not marked inherited")
        void ownDocWins() {
            FlattenedField f = byName(flatten(FlattenOptions.gAvroParity()), "created_at");
            assertThat(f.doc()).contains("Creation timestamp");
            assertThat(f.isDocInherited()).isFalse();
        }

        @Test
        @DisplayName("inheritance can be switched off")
        void inheritanceIsOptional() {
            FlattenedField f = byName(
                    flatten(FlattenOptions.builder().inheritDoc(true).build()), "customer_full_name");
            assertThat(f.doc()).isPresent();
        }
    }

    @Nested
    @DisplayName("P0.4 — pathological schemas fail typed, not fatally")
    class Guards {

        /**
         * Avro permits a self-referential record. Its flattened form does not exist, so the only
         * correct outcome is a typed refusal — previously this exhausted the stack, which a bulk
         * caller cannot catch and act on per schema.
         */
        @Test
        @DisplayName("a self-referential record raises RecursiveSchemaException, not StackOverflowError")
        void recursionIsTyped() {
            Schema recursive = new Schema.Parser().parse("""
                {"type":"record","name":"Node","namespace":"gov","fields":[
                  {"name":"value","type":"string"},
                  {"name":"next","type":["null","Node"],"default":null}]}
                """);

            assertThatThrownBy(() -> new EnrichedSchemaFlattener().flatten(recursive))
                    .isInstanceOf(RecursiveSchemaException.class)
                    .hasMessageContaining("gov.Node")
                    .hasMessageContaining("recursive");
        }

        /**
         * The distinction that keeps ordinary schemas working: a type used twice is not recursion.
         * A detector tracking "seen" rather than "currently open" would reject this.
         */
        @Test
        @DisplayName("the same record type used twice is NOT recursion")
        void repeatedTypeIsNotRecursion() {
            Schema twice = new Schema.Parser().parse("""
                {"type":"record","name":"Person","namespace":"gov","fields":[
                  {"name":"home","type":
                    {"type":"record","name":"Address","fields":[
                      {"name":"line_1","type":"string"}]}},
                  {"name":"work","type":"Address"}]}
                """);

            List<FlattenedField> fields = new EnrichedSchemaFlattener().flatten(twice);
            assertThat(fields).extracting(FlattenedField::flattenedName)
                    .containsExactly("home_line_1", "work_line_1");
        }

        @Test
        @DisplayName("depth limit raises a typed, actionable error")
        void depthLimitIsTyped() {
            assertThatThrownBy(() -> new EnrichedSchemaFlattener(
                    FlattenOptions.builder().maxDepth(1).build()).flatten(orderSchema()))
                    .isInstanceOf(SchemaLimitExceededException.class)
                    .hasMessageContaining("maxDepth");
        }

        @Test
        @DisplayName("field-count limit raises a typed, actionable error")
        void fieldLimitIsTyped() {
            assertThatThrownBy(() -> new EnrichedSchemaFlattener(
                    FlattenOptions.builder().maxFields(2).build()).flatten(orderSchema()))
                    .isInstanceOf(SchemaLimitExceededException.class)
                    .hasMessageContaining("maxFields")
                    .hasMessageContaining("gov.Order");
        }
    }

    @Nested
    @DisplayName("P1.2 / P1.3 — injectable type mapping and per-leaf hook")
    class Extensibility {

        /** The physical-type-hint cascade: producer hint wins, else the Avro type decides. */
        @Test
        @DisplayName("a TypeMapper can read custom properties to drive a width cascade")
        void typeMapperSeesProperties() {
            TypeMapper erwin = f -> f.property("x-erwin-physical-type")
                    .orElseGet(() -> switch (f.avroType()) {
                        case LONG -> "NUMBER(19,0)";
                        case INT -> "NUMBER(10,0)";
                        default -> "VARCHAR(MAX)";
                    });

            List<FlattenedField> fields = flatten(
                    FlattenOptions.builder().typeMapper(erwin).build());

            assertThat(byName(fields, "order_id").mappedType()).contains("VARCHAR(36)");
            assertThat(byName(fields, "created_at").mappedType()).contains("NUMBER(19,0)");
            assertThat(byName(fields, "customer_email_address").mappedType())
                    .contains("VARCHAR(MAX)");
        }

        @Test
        @DisplayName("the interceptor fires once per leaf, in order, and can annotate")
        void interceptorRunsInPass() {
            AtomicInteger calls = new AtomicInteger();
            List<String> order = new ArrayList<>();

            List<FlattenedField> fields = flatten(FlattenOptions.builder()
                    .leafInterceptor(f -> {
                        calls.incrementAndGet();
                        order.add(f.flattenedName());
                        if (f.name().equals("created_at")) {
                            f.properties().put("auditColumn", true);
                        }
                    })
                    .build());

            assertThat(calls.get()).isEqualTo(fields.size());
            assertThat(order).isEqualTo(
                    fields.stream().map(FlattenedField::flattenedName).toList());
            assertThat(byName(fields, "created_at").properties()).containsEntry("auditColumn", true);
        }

        @Test
        @DisplayName("an interceptor that throws aborts the flatten rather than half-annotating")
        void interceptorCanVeto() {
            assertThatThrownBy(() -> flatten(FlattenOptions.builder()
                    .leafInterceptor(f -> {
                        throw new IllegalStateException("vetoed " + f.name());
                    })
                    .build()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("vetoed");
        }
    }

    @Nested
    @DisplayName("P1.4 — deterministic order and positional injection")
    class Ordering {

        @Test
        @DisplayName("leaf order is stable across runs")
        void orderIsDeterministic() {
            assertThat(flatten(FlattenOptions.gAvroParity()).stream()
                    .map(FlattenedField::flattenedName).toList())
                    .isEqualTo(flatten(FlattenOptions.gAvroParity()).stream()
                            .map(FlattenedField::flattenedName).toList());
        }

        /**
         * Source fields must never be reordered by an injection: an external contract keyed on a
         * column index must not break because someone added a field upstream.
         */
        @Test
        @DisplayName("injected fields land at their 1-based position without reordering source fields")
        void injectionPreservesSourceOrder() {
            FlattenedField auditId = FlattenedField.builder()
                    .name("event_identifier").flattenedName("event_identifier")
                    .avroType(Schema.Type.STRING).synthetic(true).build();

            List<String> withoutInjection = flatten(FlattenOptions.gAvroParity()).stream()
                    .map(FlattenedField::flattenedName).toList();

            List<FlattenedField> withInjection = flatten(FlattenOptions.builder()
                    .injectField(1, auditId)
                    .build());

            assertThat(withInjection.get(0).flattenedName()).isEqualTo("event_identifier");
            assertThat(withInjection.get(0).isSynthetic()).isTrue();
            assertThat(withInjection.get(0).position()).isEqualTo(1);

            // Every source field still present, still in the same relative order.
            assertThat(withInjection.stream()
                    .filter(f -> !f.isSynthetic())
                    .map(FlattenedField::flattenedName).toList())
                    .isEqualTo(withoutInjection);
        }

        @Test
        @DisplayName("a position claimed twice is rejected at configuration time")
        void duplicatePositionRejected() {
            FlattenedField a = FlattenedField.builder().name("a").avroType(Schema.Type.STRING).build();
            assertThatThrownBy(() -> FlattenOptions.builder()
                    .injectField(2, a).injectField(2, a))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("already claimed");
        }
    }

    @Nested
    @DisplayName("P2.1 — structured provenance, not a parsed name")
    class Provenance {

        /**
         * The point of the whole model: a consumer gets ancestry as data. Splitting the rendered
         * name cannot distinguish a field literally named {@code full_name} from a nested
         * {@code full} → {@code name}, which is the ambiguity the escaping exists to remove.
         */
        @Test
        @DisplayName("path segments carry ancestry that the name cannot express")
        void pathSegmentsAreStructured() {
            FlattenedField f = byName(flatten(FlattenOptions.gAvroParity()), "customer_full_name");

            assertThat(f.pathSegments()).extracting(PathSegment::name)
                    .containsExactly("customer", "full_name");
            assertThat(f.sourcePath()).isEqualTo("customer.full_name");
            assertThat(f.name()).isEqualTo("full_name");
        }

        @Test
        @DisplayName("array boundaries are reported structurally and mark repeated data")
        void arrayBoundariesAreStructured() {
            FlattenedField f = byName(flatten(FlattenOptions.gAvroParity()),
                    "line_items__sku_code");

            assertThat(f.arrayBoundaries()).extracting(PathSegment::name)
                    .containsExactly("line_items");
            assertThat(f.isWithinArray()).isTrue();
            assertThat(f.isPrimaryKeyEligible())
                    .as("repeated data cannot be a primary key")
                    .isFalse();
        }

        @Test
        @DisplayName("the enclosing record name is retained on each segment")
        void recordNamesRetained() {
            assertThat(byName(flatten(FlattenOptions.gAvroParity()), "customer_full_name")
                    .pathSegments().get(1).recordName())
                    .isEqualTo("Customer");
        }

        @Test
        @DisplayName("nullability and key eligibility are derived, not guessed")
        void nullabilityAndKeyEligibility() {
            List<FlattenedField> fields = flatten(FlattenOptions.gAvroParity());
            assertThat(byName(fields, "created_at").isNullable()).isTrue();
            assertThat(byName(fields, "order_id").isNullable()).isFalse();
            assertThat(byName(fields, "order_id").isPrimaryKeyEligible()).isTrue();
        }
    }

    @Nested
    @DisplayName("P2.3 — streaming for wide schemas")
    class Streaming {

        @Test
        @DisplayName("streaming emits the same fields, in the same order, as the list form")
        void streamMatchesList() {
            List<String> streamed = new ArrayList<>();
            new EnrichedSchemaFlattener().stream(orderSchema(),
                    f -> streamed.add(f.flattenedName()));

            assertThat(streamed).isEqualTo(flatten(FlattenOptions.gAvroParity()).stream()
                    .map(FlattenedField::flattenedName).toList());
        }

        @Test
        @DisplayName("a 1,000-field schema streams without materialising the result")
        void wideSchemaStreams() {
            StringBuilder sb = new StringBuilder(
                    "{\"type\":\"record\",\"name\":\"Wide\",\"namespace\":\"gov\",\"fields\":[");
            for (int i = 0; i < 1000; i++) {
                if (i > 0) sb.append(',');
                sb.append("{\"name\":\"field_").append(i).append("\",\"type\":\"string\"}");
            }
            Schema wide = new Schema.Parser().parse(sb.append("]}").toString());

            AtomicInteger count = new AtomicInteger();
            new EnrichedSchemaFlattener().stream(wide, f -> count.incrementAndGet());
            assertThat(count.get()).isEqualTo(1000);
        }
    }

    @Nested
    @DisplayName("P2.4 — GAvroSchemaFlattener convention parity")
    class Parity {

        @Test
        @DisplayName("defaults are _ join, __ at array boundaries, unions unwrapped")
        void defaultConventions() {
            List<String> names = flatten(FlattenOptions.gAvroParity()).stream()
                    .map(FlattenedField::flattenedName).toList();

            assertThat(names).contains(
                    "order_id",                      // verbatim: FAIL policy does not escape
                    "customer_full_name",          // _ join; the literal _ in the name escaped
                    "line_items__sku_code");     // __ marks the array boundary

            // A nullable union resolves to its non-null branch rather than staying a union.
            assertThat(byName(flatten(FlattenOptions.gAvroParity()), "created_at").avroType())
                    .isEqualTo(Schema.Type.LONG);
        }

        @Test
        @DisplayName("the separator is configurable and changes only the rendering")
        void separatorIsConfigurable() {
            List<FlattenedField> dotted = new EnrichedSchemaFlattener(
                    FlattenOptions.builder().separator(".").arrayBoundarySeparator("..").build())
                    .flatten(orderSchema());

            assertThat(dotted).extracting(FlattenedField::flattenedName)
                    .contains("customer.full_name");
            // Same count and same ancestry — only the bytes of the name moved.
            assertThat(dotted).hasSameSizeAs(flatten(FlattenOptions.gAvroParity()));
        }
    }

    /**
     * FlattenOptions declares Serializable so a configured flattener can be captured into a Spark
     * closure. SpotBugs flagged SE_BAD_FIELD on the typeMapper and leafInterceptor fields: the
     * declaration was a promise the class could not keep the moment a caller used either extension
     * point. Both interfaces now extend Serializable.
     *
     * <p>What these catch: a regression that drops Serializable from either functional interface,
     * or a new non-serializable field on FlattenOptions. What they cannot catch: whether a
     * caller's own mapper captures non-serializable state in its closure — that is the caller's
     * to get right, and it now fails with a clear NotSerializableException naming their class
     * rather than naming ours.</p>
     */
    @Nested
    @DisplayName("Serializability (the Spark contract)")
    class Serializability {

        private <T> T roundTrip(T in) throws Exception {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(in);
            }
            try (ObjectInputStream oin =
                         new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                @SuppressWarnings("unchecked")
                T back = (T) oin.readObject();
                return back;
            }
        }

        @Test
        @DisplayName("default options survive a serialization round trip")
        void defaultsRoundTrip() throws Exception {
            FlattenOptions back = roundTrip(FlattenOptions.gAvroParity());
            assertThat(back.separator()).isEqualTo("_");
            assertThat(back.arrayBoundarySeparator()).isEqualTo("__");
            assertThat(back.maxDepth()).isEqualTo(64);
        }

        /** The case that actually failed before: a lambda in an extension point. */
        @Test
        @DisplayName("options carrying a lambda TypeMapper and LeafInterceptor round trip, and still work")
        void lambdasRoundTrip() throws Exception {
            FlattenOptions opts = FlattenOptions.builder()
                    .typeMapper(f -> "DECIMAL(38,9)")
                    .leafInterceptor(f -> f.properties().put("x-seen", true))
                    .build();

            FlattenOptions back = roundTrip(opts);

            Schema s = new Schema.Parser().parse(
                    "{\"type\":\"record\",\"name\":\"R\",\"fields\":"
                            + "[{\"name\":\"amount\",\"type\":\"double\"}]}");
            List<FlattenedField> out = new EnrichedSchemaFlattener(back).flatten(s);

            assertThat(out).hasSize(1);
            assertThat(out.get(0).mappedType()).contains("DECIMAL(38,9)");
            assertThat(out.get(0).property("x-seen")).contains("true");
        }

        @Test
        @DisplayName("a produced FlattenedField and its PathSegments round trip")
        void fieldRoundTrips() throws Exception {
            Schema s = new Schema.Parser().parse(
                    "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"u\",\"type\":"
                            + "{\"type\":\"record\",\"name\":\"U\",\"fields\":"
                            + "[{\"name\":\"id\",\"type\":\"string\"}]}}]}");
            FlattenedField original = new EnrichedSchemaFlattener().flatten(s).get(0);

            FlattenedField back = roundTrip(original);

            assertThat(back.flattenedName()).isEqualTo(original.flattenedName());
            assertThat(back.pathSegments()).isEqualTo(original.pathSegments());
            assertThat(back.sourcePath()).isEqualTo("u.id");
        }
    }
}
