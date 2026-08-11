package io.github.pierce.schema;

import io.github.pierce.GAvroSchemaFlattener;
import io.github.pierce.path.FlattenedPath;
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
            List<FlattenedField> fields = flatten(FlattenOptions.defaults());

            assertThat(byName(fields, "order_id").property("x-erwin-physical-type"))
                    .contains("VARCHAR(36)");
            assertThat(byName(fields, "order_id").property("x-chase-dataelem"))
                    .contains("DE-1001");
        }

        @Test
        @DisplayName("properties survive on nested leaves too")
        void nestedPropertiesSurvive() {
            assertThat(byName(flatten(FlattenOptions.defaults()), "customer_full_name")
                    .property("x-chase-dataelem"))
                    .contains("DE-2001");
        }

        /**
         * The seven names Avro hands over on a record FIELD, measured against avro 1.12.0:
         * {@code namespace, fields, items, values, symbols, size, logicalType}. None is in
         * {@code Schema.Field.FIELD_RESERVED}, so all seven are ordinary producer metadata at
         * that position — and all seven were in the library's private blocklist.
         */
        @Test
        @DisplayName("a field-level property named \"size\" is producer metadata and survives")
        void aFieldLevelPropertyNamedSizeSurvives() {
            Schema s = new Schema.Parser().parse("""
                {"type":"record","name":"Sku","namespace":"gov","fields":[
                  {"name":"garment","type":"string","size":"XL","x-chase-dataelem":"DE-9001"}]}
                """);
            FlattenedField f = new EnrichedSchemaFlattener().flatten(s).get(0);

            assertThat(f.property("size"))
                    .as("Avro 1.12 hands 'size' over on a record field - it is structural only on "
                            + "a FIXED schema - so dropping it destroys producer metadata")
                    .contains("XL");
            assertThat(f.property("x-chase-dataelem")).contains("DE-9001");
        }

        @Test
        @DisplayName("every field-level name Avro hands over is preserved, not filtered by name")
        void everyFieldLevelNameAvroHandsOverIsPreserved() {
            Schema s = new Schema.Parser().parse("""
                {"type":"record","name":"Meta","namespace":"gov","fields":[
                  {"name":"payload","type":"string",
                   "namespace":"NS","fields":"F","items":"I","values":"V","symbols":"Y",
                   "size":"Z","logicalType":"L","x-owner":"payments"}]}
                """);
            FlattenedField f = new EnrichedSchemaFlattener().flatten(s).get(0);

            assertThat(f.properties())
                    .as("all seven names Avro exposes at field level, plus the x- control")
                    .containsEntry("namespace", "NS")
                    .containsEntry("fields", "F")
                    .containsEntry("items", "I")
                    .containsEntry("values", "V")
                    .containsEntry("symbols", "Y")
                    .containsEntry("size", "Z")
                    .containsEntry("logicalType", "L")
                    .containsEntry("x-owner", "payments");
        }

        /**
         * The arm the original defect report missed. On a RECORD schema Avro's SCHEMA_RESERVED
         * omits {@code default}, {@code order} and {@code logicalType}, so all three are ordinary
         * record-level metadata that inheritRecordProperties is supposed to push onto every leaf.
         */
        @Test
        @DisplayName("record-level default, order and logicalType reach the leaf by inheritance")
        void recordLevelDefaultOrderAndLogicalTypeReachTheLeaf() {
            Schema s = new Schema.Parser().parse("""
                {"type":"record","name":"Ann","namespace":"gov",
                 "default":"D","order":"O","logicalType":"L","x-owner":"payments",
                 "fields":[{"name":"amount","type":"double"}]}
                """);

            FlattenedField inherited = new EnrichedSchemaFlattener().flatten(s).get(0);
            assertThat(inherited.properties())
                    .containsEntry("default", "D")
                    .containsEntry("order", "O")
                    .containsEntry("logicalType", "L")
                    .containsEntry("x-owner", "payments");

            FlattenedField isolated = new EnrichedSchemaFlattener(
                    FlattenOptions.builder().inheritRecordProperties(false).build())
                    .flatten(s).get(0);
            assertThat(isolated.properties())
                    .as("with inheritance off none of them may appear, so the test above cannot "
                            + "pass by accident if inheritance silently turned itself on or off")
                    .doesNotContainKeys("default", "order", "logicalType", "x-owner");
        }

        /**
         * THE BLOCKING ARM, and the rebuild of a test that could only ever pass. Its predecessor
         * asserted that {@code order_id}'s properties omitted {@code type/name/doc/default} — four
         * names Avro's own FIELD_RESERVED already strips, on a field that declared none of them.
         * It held with the blocklist present, absent or empty.
         *
         * <p>This version declares all four, asserts they are absent from the property map AND
         * still reachable through their typed accessors, and pins the map with
         * {@code containsOnlyKeys} so a repair that scraped raw JSON would fail here.
         */
        @Test
        @DisplayName("Avro's structural attributes are still not mistaken for user metadata")
        void avroStructuralAttributesStillDoNotAppearAsProperties() {
            Schema s = new Schema.Parser().parse("""
                {"type":"record","name":"Acct","namespace":"gov","doc":"the record",
                 "aliases":["gov.Legacy"],"x-owner":"payments","fields":[
                   {"name":"status","type":"string","doc":"the doc","default":"d",
                    "order":"descending","aliases":["legacy_status"],"x-pii":"none"}]}
                """);
            FlattenedField f = new EnrichedSchemaFlattener().flatten(s).get(0);

            assertThat(f.properties())
                    .as("the parser consumed every structural attribute into a typed accessor "
                            + "before getObjectProps was populated, so the map is exactly the "
                            + "producer's own metadata and nothing else")
                    .containsOnlyKeys("x-owner", "x-pii");

            Schema.Field declared = s.getField("status");
            assertThat(f.doc()).as("still reachable, typed").contains("the doc");
            assertThat(declared.defaultVal()).isEqualTo("d");
            assertThat(declared.order()).isEqualTo(Schema.Field.Order.DESCENDING);
            assertThat(declared.aliases()).containsExactly("legacy_status");
            assertThat(s.getAliases()).containsExactly("gov.Legacy");
        }

        /**
         * THE MISSING/EMPTY ARM, and the guard against the plausible over-correction. On a FIXED
         * schema {@code size} IS structural: Avro keeps it out of getObjectProps and exposes it as
         * getFixedSize(). A "fix" that reached past getObjectProps into the raw JSON would start
         * emitting {@code size=8} here.
         */
        @Test
        @DisplayName("a schema with no custom properties yields empty property maps")
        void aSchemaWithNoCustomPropertiesYieldsAnEmptyPropertyMap() {
            Schema s = new Schema.Parser().parse("""
                {"type":"record","name":"Plain","namespace":"gov","fields":[
                  {"name":"id","type":"string"},
                  {"name":"hash","type":{"type":"fixed","name":"H","size":8}}]}
                """);
            List<FlattenedField> fields = new EnrichedSchemaFlattener().flatten(s);

            assertThat(fields).allSatisfy(f -> assertThat(f.properties()).isEmpty());
            assertThat(byName(fields, "hash").properties()).doesNotContainKey("size");
            assertThat(byName(fields, "hash").schema().getFixedSize())
                    .as("the same name, the opposite verdict, decided by position - and the "
                            + "structural answer still reachable")
                    .isEqualTo(8);
        }
    }

    @Nested
    @DisplayName("P0.3 — documentation inheritance")
    class DocInheritance {

        @Test
        @DisplayName("a leaf with no doc inherits its nearest ancestor record's")
        void inheritsFromParentRecord() {
            FlattenedField f = byName(flatten(FlattenOptions.defaults()), "customer_full_name");
            assertThat(f.doc()).contains("The buyer");
            assertThat(f.isDocInherited()).isTrue();
        }

        @Test
        @DisplayName("a leaf's own doc wins, and is not marked inherited")
        void ownDocWins() {
            FlattenedField f = byName(flatten(FlattenOptions.defaults()), "created_at");
            assertThat(f.doc()).contains("Creation timestamp");
            assertThat(f.isDocInherited()).isFalse();
        }

        /**
         * The control, wired up. Its predecessor was called "inheritance can be switched off",
         * passed {@code inheritDoc(TRUE)} - the default - and asserted the doc was present, so it
         * could only ever pass; it was then rewritten to assert the dead-control behaviour, and is
         * now rewritten again to assert the correct one. Pinned at the corpus level by
         * {@code schema/enriched-inheritdoc-false-reports-declared-only-docs}.
         */
        @Test
        @DisplayName("inheritDoc(false) reports declared-only documentation")
        void inheritanceCanBeSwitchedOff() {
            List<FlattenedField> fields = flatten(
                    FlattenOptions.builder().inheritDoc(false).build());

            FlattenedField nested = byName(fields, "customer_full_name");
            assertThat(nested.doc())
                    .as("inheritDoc(false) asks for declared-only documentation and this leaf "
                            + "declares none, so nothing may be attributed to it")
                    .isEmpty();
            assertThat(nested.isDocInherited()).isFalse();

            FlattenedField root = byName(fields, "order_id");
            assertThat(root.doc())
                    .as("whose only available doc is the ROOT record's 'An order record'")
                    .isEmpty();
            assertThat(root.isDocInherited()).isFalse();
        }

        /**
         * The unit-level mirror of the corpus probe, and the drill that makes a future revert to a
         * dead control fail loudly instead of silently. The two renderings were byte-identical
         * before the flag was honoured; that identity WAS the finding.
         */
        @Test
        @DisplayName("switching inheritance off changes the output")
        void switchingInheritanceOffChangesTheOutput() {
            assertThat(renderDocs(flatten(FlattenOptions.builder().inheritDoc(false).build())))
                    .as("a flag that cannot change any leaf's documentation is a dead control")
                    .isNotEqualTo(renderDocs(flatten(
                            FlattenOptions.builder().inheritDoc(true).build())));
        }

        /**
         * THE OVER-REACH GUARD. Stated honestly: this PASSES before the fix as well as after. It
         * fails only against the naive repair that nulls the doc unconditionally, or one that
         * wires inheritDoc to the property branch. The flag turns inheritance off, not
         * documentation off, and inheritRecordProperties is a separate control.
         */
        @Test
        @DisplayName("a leaf's own doc survives with inheritance off, and properties are untouched")
        void aLeafsOwnDocSurvivesWithInheritanceOff() {
            List<FlattenedField> fields =
                    flatten(FlattenOptions.builder().inheritDoc(false).build());

            FlattenedField own = byName(fields, "created_at");
            assertThat(own.doc()).contains("Creation timestamp");
            assertThat(own.isDocInherited()).isFalse();

            assertThat(byName(fields, "order_id").property("x-chase-dataelem"))
                    .as("inheritRecordProperties is a separate control and must not move")
                    .contains("DE-1001");
        }

        /**
         * THE MISSING/EMPTY ARM. Also passes before the fix, and declared as such: it proves the
         * DIFFERENT result above comes from inheritance being switched off rather than from the
         * flag blanket-clearing documentation.
         */
        @Test
        @DisplayName("declared-only mode is stable when nothing declares documentation")
        void declaredOnlyModeIsStableWhenNothingDeclaresDocumentation() {
            Schema undocumented = new Schema.Parser().parse("""
                {"type":"record","name":"Bare","namespace":"gov","fields":[
                  {"name":"a","type":"string"},
                  {"name":"b","type":{"type":"record","name":"Inner","fields":[
                    {"name":"c","type":"string"}]}}]}
                """);

            for (boolean inherit : new boolean[] {true, false}) {
                List<FlattenedField> fields = new EnrichedSchemaFlattener(
                        FlattenOptions.builder().inheritDoc(inherit).build()).flatten(undocumented);
                assertThat(fields).hasSize(2);
                assertThat(fields).allSatisfy(f -> {
                    assertThat(f.doc()).isEmpty();
                    assertThat(f.isDocInherited()).isFalse();
                });
            }
        }

        /** doc + inheritance flag for every leaf, which is what the corpus probe compares. */
        private String renderDocs(List<FlattenedField> fields) {
            return fields.stream()
                    .map(f -> f.flattenedName() + "|doc=" + f.doc().orElse(null)
                            + "|inherited=" + f.isDocInherited())
                    .toList().toString();
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
            assertThat(flatten(FlattenOptions.defaults()).stream()
                    .map(FlattenedField::flattenedName).toList())
                    .isEqualTo(flatten(FlattenOptions.defaults()).stream()
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

            List<String> withoutInjection = flatten(FlattenOptions.defaults()).stream()
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
            FlattenedField f = byName(flatten(FlattenOptions.defaults()), "customer_full_name");

            assertThat(f.pathSegments()).extracting(PathSegment::name)
                    .containsExactly("customer", "full_name");
            assertThat(f.sourcePath()).isEqualTo("customer.full_name");
            assertThat(f.name()).isEqualTo("full_name");
        }

        @Test
        @DisplayName("array boundaries are reported structurally and mark repeated data")
        void arrayBoundariesAreStructured() {
            FlattenedField f = byName(flatten(FlattenOptions.defaults()),
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
            assertThat(byName(flatten(FlattenOptions.defaults()), "customer_full_name")
                    .pathSegments().get(1).recordName())
                    .isEqualTo("Customer");
        }

        @Test
        @DisplayName("nullability and key eligibility are derived, not guessed")
        void nullabilityAndKeyEligibility() {
            List<FlattenedField> fields = flatten(FlattenOptions.defaults());
            assertThat(byName(fields, "created_at").isNullable()).isTrue();
            assertThat(byName(fields, "order_id").isNullable()).isFalse();
            assertThat(byName(fields, "order_id").isPrimaryKeyEligible()).isTrue();
        }
    }

    @Nested
    @DisplayName("P2.3 — streaming for wide schemas")
    class Streaming {

        /** A three-leaf record, small enough that a five-element expectation is readable. */
        private static Schema shippingSchema() {
            return new Schema.Parser().parse("""
                {"type":"record","name":"Order","namespace":"gov","fields":[
                  {"name":"order_id","type":"string"},
                  {"name":"ship","type":{"type":"record","name":"Ship","fields":[
                    {"name":"city","type":"string"},
                    {"name":"post_code","type":"string"}]}}]}
                """);
        }

        private static FlattenedField synthetic(String name) {
            return FlattenedField.builder().name(name).flattenedName(name)
                    .avroType(Schema.Type.STRING).synthetic(true).build();
        }

        /**
         * One canonical string per field. Name alone is not enough: the existing
         * {@code streamMatchesList} compared names only and stayed green through a defect that
         * dropped whole columns AND renumbered every surviving one, so position and synthetic are
         * both in the rendering. {@link FlattenedField} declares no {@code equals}, so comparing
         * the objects would be identity comparison and would silently pass.
         */
        private static List<String> render(List<FlattenedField> fields) {
            return fields.stream()
                    .map(f -> f.position() + "|" + f.flattenedName()
                            + "|synthetic=" + f.isSynthetic()
                            + "|" + f.avroType()
                            + "|nullable=" + f.isNullable()
                            + "|withinArray=" + f.isWithinArray()
                            + "|path=" + f.sourcePath()
                            + "|mapped=" + f.mappedType().orElse(null))
                    .toList();
        }

        private static List<String> renderStream(FlattenOptions options, Schema schema) {
            List<FlattenedField> out = new ArrayList<>();
            new EnrichedSchemaFlattener(options).stream(schema, out::add);
            return render(out);
        }

        @Test
        @DisplayName("streaming emits the same fields, in the same order, as the list form")
        void streamMatchesList() {
            List<FlattenedField> streamed = new ArrayList<>();
            new EnrichedSchemaFlattener().stream(orderSchema(), streamed::add);

            assertThat(render(streamed))
                    .as("compared on the FULL rendering, not names: a name-only comparison is what "
                            + "let stream() diverge on position for the life of this branch")
                    .isEqualTo(render(flatten(FlattenOptions.defaults())));
        }

        /** The permanent two-entry-point equivalence assertion. */
        @Test
        @DisplayName("stream() honours injections exactly as flatten() does")
        void streamHonoursInjectionsExactlyAsFlattenDoes() {
            FlattenOptions opts = FlattenOptions.builder()
                    .injectField(1, synthetic("event_identifier"))
                    .injectField(3, synthetic("tenant_id"))
                    .build();

            assertThat(renderStream(opts, shippingSchema()))
                    .isEqualTo(render(new EnrichedSchemaFlattener(opts).flatten(shippingSchema())));
        }

        @Test
        @DisplayName("an injection past the final column is appended, not dropped and not at 99")
        void streamHonoursAnInjectionPastTheFinalColumn() {
            FlattenOptions opts = FlattenOptions.builder()
                    .injectField(99, synthetic("audit_tag")).build();

            List<FlattenedField> streamed = new ArrayList<>();
            new EnrichedSchemaFlattener(opts).stream(shippingSchema(), streamed::add);

            assertThat(render(streamed))
                    .isEqualTo(render(new EnrichedSchemaFlattener(opts).flatten(shippingSchema())));
            assertThat(streamed).hasSize(4);
            assertThat(streamed.get(3).flattenedName()).isEqualTo("audit_tag");
            assertThat(streamed.get(3).position()).isEqualTo(4);
        }

        /**
         * Pins the flush LOOP. An implementation using {@code if} instead of {@code while} would
         * emit only the position-1 injection before the first source field and silently defer the
         * position-2 one, reordering it against flatten().
         */
        @Test
        @DisplayName("consecutive head injections keep their relative order")
        void consecutiveHeadInjectionsKeepTheirRelativeOrder() {
            FlattenOptions opts = FlattenOptions.builder()
                    .injectField(1, synthetic("first_tag"))
                    .injectField(2, synthetic("second_tag"))
                    .build();

            List<FlattenedField> streamed = new ArrayList<>();
            new EnrichedSchemaFlattener(opts).stream(shippingSchema(), streamed::add);

            assertThat(streamed).extracting(FlattenedField::flattenedName)
                    .containsExactly("first_tag", "second_tag",
                            "order_id", "ship_city", "ship_post_code");
            assertThat(render(streamed))
                    .isEqualTo(render(new EnrichedSchemaFlattener(opts).flatten(shippingSchema())));
        }

        /**
         * THE GUARD AGAINST THE FIX ACQUIRING THE PATHOLOGY IT REPAIRS. The lazy way to make the
         * three tests above pass is to collect into a list inside stream() and splice at the end,
         * which destroys the only reason stream() exists while every sequence assertion still
         * passes. Only an interleaving assertion can see that.
         */
        @Test
        @DisplayName("stream() does not buffer the field list when injecting")
        void streamDoesNotBufferTheFieldListWhenInjecting() {
            List<String> log = new ArrayList<>();
            FlattenOptions opts = FlattenOptions.builder()
                    .injectField(1, synthetic("event_identifier"))
                    .leafInterceptor(f -> log.add("I:" + f.flattenedName()))
                    .build();

            new EnrichedSchemaFlattener(opts)
                    .stream(shippingSchema(), f -> log.add("S:" + f.flattenedName()));

            assertThat(log).containsExactly(
                    "I:order_id", "S:event_identifier", "S:order_id",
                    "I:ship_city", "S:ship_city",
                    "I:ship_post_code", "S:ship_post_code");
        }

        /**
         * TESTS THE ORACLE, NOT THE FLATTENER, and passes before the fix as well as after. Stated
         * plainly rather than dressed up: the four tests above are only as strong as
         * {@link #render}, and a rendering that omitted position or synthetic would let all four
         * pass against a stream() that emitted injected columns at the wrong indices. That is
         * exactly how the old name-only {@code streamMatchesList} stayed green.
         */
        @Test
        @DisplayName("the parity rendering can actually report a difference, and null input blocks")
        void theParityRenderingCanActuallyReportADifference() {
            List<FlattenedField> base = new EnrichedSchemaFlattener().flatten(shippingSchema());

            List<FlattenedField> shifted = new ArrayList<>(base);
            shifted.set(0, FlattenedField.builder().name(base.get(0).name())
                    .flattenedName(base.get(0).flattenedName())
                    .avroType(base.get(0).avroType()).position(base.get(0).position() + 1).build());
            assertThat(render(shifted)).as("a one-slot position shift must be visible")
                    .isNotEqualTo(render(base));

            List<FlattenedField> flagged = new ArrayList<>(base);
            flagged.set(0, FlattenedField.builder().name(base.get(0).name())
                    .flattenedName(base.get(0).flattenedName())
                    .avroType(base.get(0).avroType()).position(base.get(0).position())
                    .synthetic(true).build());
            assertThat(render(flagged)).as("a flipped synthetic flag must be visible")
                    .isNotEqualTo(render(base));

            EnrichedSchemaFlattener f = new EnrichedSchemaFlattener();
            assertThatThrownBy(() -> f.stream(null, x -> { }))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("schema");
            assertThatThrownBy(() -> f.stream(shippingSchema(), null))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("sink");
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
    @DisplayName("P2.4 — the defaults, and genuine GAvroSchemaFlattener parity")
    class Parity {

        /** Underscore-bearing field AND an array of records: the two axes parity changes. */
        private static Schema bothAxesSchema() {
            return new Schema.Parser().parse("""
                {"type":"record","name":"Order","namespace":"gov","fields":[
                  {"name":"order_id","type":"string"},
                  {"name":"items","type":{"type":"array","items":
                    {"type":"record","name":"Item","fields":[
                      {"name":"sku","type":"string"}]}}}]}
                """);
        }

        private static Schema underscoredSchema() {
            return new Schema.Parser().parse("""
                {"type":"record","name":"Order","namespace":"gov","fields":[
                  {"name":"order_id","type":"string"},
                  {"name":"ship","type":{"type":"record","name":"Ship","fields":[
                    {"name":"city","type":"string"},
                    {"name":"post_code","type":"string"}]}}]}
                """);
        }

        private static Schema cartSchema() {
            return new Schema.Parser().parse("""
                {"type":"record","name":"Cart","namespace":"gov","fields":[
                  {"name":"items","type":{"type":"array","items":
                    {"type":"record","name":"Item","fields":[
                      {"name":"sku","type":"string"}]}}}]}
                """);
        }

        private static List<String> sortedNames(FlattenOptions options, Schema schema) {
            return new EnrichedSchemaFlattener(options).flatten(schema).stream()
                    .map(FlattenedField::flattenedName).sorted().toList();
        }

        private static List<String> sortedGAvro(Schema schema) {
            return new GAvroSchemaFlattener().flattenSchema(schema).keySet()
                    .stream().sorted().toList();
        }

        @Test
        @DisplayName("the DEFAULTS are _ join, __ at array boundaries, unions unwrapped, no escaping")
        void defaultConventions() {
            List<String> names = flatten(FlattenOptions.defaults()).stream()
                    .map(FlattenedField::flattenedName).toList();

            assertThat(names).contains(
                    "order_id",                  // verbatim: the FAIL policy escapes nothing
                    "customer_full_name",        // _ join, and the literal _ in the name is NOT
                                                 // escaped either - only ESCAPE does that
                    "line_items__sku_code");     // __ marks the array boundary

            // A nullable union resolves to its non-null branch rather than staying a union.
            assertThat(byName(flatten(FlattenOptions.defaults()), "created_at").avroType())
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
            assertThat(dotted).hasSameSizeAs(flatten(FlattenOptions.defaults()));
        }

        @Test
        @DisplayName("gAvroParity() reproduces GAvroSchemaFlattener's names for underscored fields")
        void gAvroParityReproducesGAvroNamesForUnderscoredFields() {
            assertThat(sortedNames(FlattenOptions.gAvroParity(), underscoredSchema()))
                    .as("GAvroSchemaFlattener.buildPath escapes EVERY segment, so parity must too")
                    .containsExactlyElementsOf(sortedGAvro(underscoredSchema()))
                    .containsExactly("order\\_id", "ship_city", "ship_post\\_code");
        }

        @Test
        @DisplayName("gAvroParity() reproduces GAvroSchemaFlattener's names across an array boundary")
        void gAvroParityReproducesGAvroNamesAcrossAnArrayBoundary() {
            assertThat(sortedNames(FlattenOptions.gAvroParity(), cartSchema()))
                    .as("GAvro has no array-boundary marker at all - useArrayBoundarySeparator is "
                            + "a global rename - so its default joins arrays with a single _")
                    .containsExactlyElementsOf(sortedGAvro(cartSchema()))
                    .containsExactly("items_sku");
        }

        /**
         * THE ANTI-ALIAS DRILL, and the test that would have caught this defect originally. It
         * fails the day someone "simplifies" gAvroParity() back into builder().build().
         */
        @Test
        @DisplayName("the defaults are NOT the parity preset, and the no-arg flattener uses the defaults")
        void theDefaultsAreNotTheParityPreset() {
            assertThat(sortedNames(FlattenOptions.defaults(), bothAxesSchema()))
                    .as("an alias wearing a promise is what this whole repair is about")
                    .isNotEqualTo(sortedNames(FlattenOptions.gAvroParity(), bothAxesSchema()));

            assertThat(new EnrichedSchemaFlattener().flatten(bothAxesSchema()).stream()
                    .map(FlattenedField::flattenedName).sorted().toList())
                    .as("the zero-config entry point must render the DEFAULTS, not parity - "
                            + "without this the parity repair silently becomes a default change")
                    .isEqualTo(sortedNames(FlattenOptions.defaults(), bothAxesSchema()))
                    .containsExactly("items__sku", "order_id");
        }

        /**
         * THE BOUNDING ARM. Passes before and after, and is declared as such: it proves the
         * difference above is confined to the two axes parity actually changes, so gAvroParity()
         * is not simply a different renderer.
         */
        @Test
        @DisplayName("parity and the defaults agree when nothing needs escaping and no array is crossed")
        void parityAndDefaultsAgreeWhenNothingNeedsEscapingAndNoArrayIsCrossed() {
            Schema person = new Schema.Parser().parse("""
                {"type":"record","name":"Person","namespace":"gov","fields":[
                  {"name":"name","type":"string"},
                  {"name":"home","type":{"type":"record","name":"Address","fields":[
                    {"name":"city","type":"string"}]}}]}
                """);

            List<String> expected = List.of("home_city", "name");
            assertThat(sortedNames(FlattenOptions.defaults(), person)).isEqualTo(expected);
            assertThat(sortedNames(FlattenOptions.gAvroParity(), person)).isEqualTo(expected);
            assertThat(sortedGAvro(person)).isEqualTo(expected);
        }

        /**
         * The javadoc's recipe for a caller who ran GAvro with {@code useArrayBoundarySeparator(true)},
         * executed rather than merely published. An untested documented recipe is the same
         * pathology in prose.
         */
        @Test
        @DisplayName("the documented doubled-separator recipe matches GAvro with useArrayBoundarySeparator")
        void theDocumentedDoubledSeparatorRecipeMatchesGAvroWithUseArrayBoundarySeparator() {
            FlattenOptions doubled = FlattenOptions.builder()
                    .separator("__").arrayBoundarySeparator("__")
                    .collisionPolicy(NameCollisionPolicy.ESCAPE)
                    .build();

            List<String> gavro = new GAvroSchemaFlattener(
                    GAvroSchemaFlattener.AvroFlatteningConfig.builder()
                            .useArrayBoundarySeparator(true).build())
                    .flattenSchema(bothAxesSchema()).keySet().stream().sorted().toList();

            assertThat(sortedNames(doubled, bothAxesSchema()))
                    .containsExactlyElementsOf(gavro);

            // Tied to the factory on purpose. Without this the recipe is self-contained and would
            // keep passing if gAvroParity() drifted away from it, which is the drift the javadoc
            // recipe exists to survive: it is documented as "parity, plus the two separators".
            assertThat(doubled.collisionPolicy())
                    .as("the recipe is gAvroParity()'s policy with the separators doubled")
                    .isEqualTo(FlattenOptions.gAvroParity().collisionPolicy());
        }
    }

    /**
     * NameCollisionPolicy end to end. Before this class existed the file carried 27 tests and not
     * one of them mentioned {@code collisionPolicy}, {@code NameCollisionPolicy} or {@code ESCAPE};
     * the enum was referenced nowhere under {@code src/test/java} outside the fidelity harness.
     * That absence is why an ESCAPE collision guard that returns before it checks anything reached
     * an open pull request.
     */
    @Nested
    @DisplayName("NameCollisionPolicy — injectivity, checked rather than asserted")
    class Collisions {

        /**
         * {@code a} is an array of records with one field {@code b}, so the array-boundary marker
         * sits between them. A sibling scalar named {@code a + marker + b} therefore renders to
         * the same string whenever the marker is spelled from characters segment escaping does not
         * escape. Both names are legal Avro, and they live in the same record.
         */
        private static Schema forging(String marker) {
            return new Schema.Parser().parse("""
                {"type":"record","name":"Cart","namespace":"gov","fields":[
                  {"name":"a","type":{"type":"array","items":
                    {"type":"record","name":"Item","fields":[
                      {"name":"b","type":"string"}]}}},
                  {"name":"%s","type":"string"}]}
                """.formatted("a" + marker + "b"));
        }

        private static Schema acctSchema() {
            return new Schema.Parser().parse("""
                {"type":"record","name":"Acct","namespace":"gov","fields":[
                  {"name":"user_name","type":"string"},
                  {"name":"user","type":{"type":"record","name":"User","fields":[
                    {"name":"name","type":"string"}]}}]}
                """);
        }

        private static Schema cart() {
            return new Schema.Parser().parse("""
                {"type":"record","name":"Cart","namespace":"gov","fields":[
                  {"name":"items","type":{"type":"array","items":
                    {"type":"record","name":"Item","fields":[
                      {"name":"sku","type":"string"}]}}}]}
                """);
        }

        private static FlattenOptions escaping(String boundary) {
            return FlattenOptions.builder()
                    .collisionPolicy(NameCollisionPolicy.ESCAPE)
                    .arrayBoundarySeparator(boundary)
                    .build();
        }

        /** The whole defect, made executable in one schema. */
        @Test
        @DisplayName("ESCAPE refuses a forged array boundary instead of emitting the name twice")
        void escapePolicyRefusesAForgedArrayBoundary() {
            assertThatThrownBy(() ->
                    new EnrichedSchemaFlattener(escaping("x")).flatten(forging("x")))
                    .isInstanceOf(SchemaFlattenException.class)
                    .hasMessageContaining("a.b")
                    .hasMessageContaining("axb");
        }

        @Test
        @DisplayName("the ESCAPE diagnostic names the boundary marker, not the policy already in use")
        void escapeCollisionMessageDoesNotRecommendEscapeToACallerAlreadyUsingIt() {
            assertThatThrownBy(() ->
                    new EnrichedSchemaFlattener(escaping("x")).flatten(forging("x")))
                    .isInstanceOf(SchemaFlattenException.class)
                    .hasMessageContaining("arrayBoundarySeparator")
                    .hasMessageNotContaining("NameCollisionPolicy.ESCAPE");
        }

        /**
         * The generalised guard: whatever the boundary marker, the flattener either refuses or
         * returns distinct names. Never two leaves with one name, silently.
         */
        @Test
        @DisplayName("ESCAPE never returns two leaves with the same name, under any boundary marker")
        void escapeNeverReturnsTwoLeavesWithTheSameName() {
            for (String marker : new String[] {"__", "___", "_arr_", "x", "arr", "_"}) {
                for (Schema schema : new Schema[] {forging(marker), cart(), acctSchema()}) {
                    List<String> names;
                    try {
                        names = new EnrichedSchemaFlattener(escaping(marker)).flatten(schema)
                                .stream().map(FlattenedField::flattenedName).toList();
                    } catch (SchemaFlattenException refused) {
                        continue;
                    }
                    assertThat(names)
                            .as("boundary '%s' on %s returned duplicate flattened names with no "
                                    + "diagnostic", marker, schema.getFullName())
                            .doesNotHaveDuplicates();
                }
            }
        }

        /**
         * A DOCUMENTATION PIN, not a verifying test: it passes before the fix as well as after.
         * It puts the narrowed javadoc claim into executable form, so a future attempt to make the
         * rendered name decodable reddens it and forces the guarantee back onto the table.
         */
        @Test
        @DisplayName("across an array boundary the structure is in arrayBoundaries(), not in the name")
        void arrayBoundaryIsRecoverableOnlyFromArrayBoundaries() {
            FlattenedField f = new EnrichedSchemaFlattener(
                    FlattenOptions.builder().collisionPolicy(NameCollisionPolicy.ESCAPE).build())
                    .flatten(cart()).get(0);

            assertThat(f.flattenedName()).isEqualTo("items__sku");
            assertThat(FlattenedPath.decodeSegments("items__sku", "_"))
                    .as("the marker is emitted OUTSIDE the escaped alphabet, so splitting the name "
                            + "interposes a phantom empty segment")
                    .containsExactly("items", "", "sku")
                    .isNotEqualTo(List.of("items", "sku"));
            assertThat(f.arrayBoundaries()).extracting(PathSegment::name).containsExactly("items");
            assertThat(f.pathSegments()).extracting(PathSegment::name)
                    .containsExactly("items", "sku");
        }

        /**
         * A CHANGE DETECTOR, passing before and after. The FAIL diagnostic is recorded verbatim by
         * {@code enriched-fail-policy-refuses-a-colliding-schema}, truncated mid-word at 240
         * characters, so any refactor that merges the two policies' messages into one shared
         * template reddens both this test and that corpus row.
         */
        @Test
        @DisplayName("the FAIL diagnostic is byte-unchanged by this repair")
        void failPolicyMessageIsUnchangedByThisRepair() {
            assertThatThrownBy(() -> new EnrichedSchemaFlattener().flatten(acctSchema()))
                    .isInstanceOf(SchemaFlattenException.class)
                    .hasMessageStartingWith(
                            "Flattened name collision in schema 'gov.Acct': source paths "
                            + "'user_name' and 'user.name' both render to 'user_name'. Joining "
                            + "segments with '_' is ambiguous when a field name contains it. "
                            + "Rename a field, choose a separator absent from your na");
        }

        /**
         * The third leg of the drill, and the guard against an over-strict repair such as
         * validating arrayBoundarySeparator at build() time, which would reject the default
         * configuration if written carelessly. Passes before and after.
         */
        @Test
        @DisplayName("ESCAPE with the defaults still marks the boundary and still resolves collisions")
        void escapeWithDefaultsStillMarksTheArrayBoundaryAndDoesNotThrow() {
            FlattenOptions escape = FlattenOptions.builder()
                    .collisionPolicy(NameCollisionPolicy.ESCAPE).build();

            assertThat(new EnrichedSchemaFlattener(escape).flatten(cart()))
                    .extracting(FlattenedField::flattenedName).containsExactly("items__sku");
            assertThat(new EnrichedSchemaFlattener(escape).flatten(acctSchema()))
                    .extracting(FlattenedField::flattenedName)
                    .containsExactly("user\\_name", "user_name");
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
            FlattenOptions back = roundTrip(FlattenOptions.defaults());
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
