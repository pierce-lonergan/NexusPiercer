package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * One fixture file, loaded from {@code src/test/resources/fidelity/<family>/<id>.json}.
 *
 * <p>A fixture is deliberately self-describing: input document, machine-readable configuration,
 * the designer's PREDICTED classification, and the MEASURED renderings the harness compares
 * against. A reviewer should be able to open a single file and understand what it claims without
 * reading any Java.</p>
 *
 * @param id                 stable fixture identifier, also the file name
 * @param family             corpus family, also the directory name
 * @param title              one-line statement of what the fixture demonstrates
 * @param stack              MAP, JSON, BOTH or AVRO
 * @param referenceIssue     issue id this fixture pins, or empty
 * @param rationale          why this fixture exists and what it is worth
 * @param catchesBugClass    the class of regression it will detect
 * @param cannotCatch        the honest limits of what it proves
 * @param configDescription  human-readable statement of the configuration
 * @param predicted          the designer's prediction, kept even where measurement disagreed
 * @param config             machine-readable configuration applied by the runner
 * @param probe              optional extra measurement (config comparison, typed twin)
 * @param input              the source document as JSON text, or null when {@code javaInput} is used
 * @param javaInput          a typed-constructor spec for a source document JSON cannot express
 *                           (java.util.Date, UUID, Instant, enum, byte[], Object[], Set,
 *                           non-String map keys, or a cycle), or null when {@code input} is used
 * @param expected           the recorded renderings the guarantee is checked against
 */
record FidelityFixture(
        String id,
        String family,
        String title,
        String stack,
        String referenceIssue,
        String rationale,
        String catchesBugClass,
        String cannotCatch,
        String configDescription,
        JsonNode predicted,
        JsonNode config,
        JsonNode probe,
        String input,
        JsonNode javaInput,
        JsonNode expected) {

    static FidelityFixture from(JsonNode node) {
        JsonNode probe = node.path("probe");
        JsonNode java = node.path("javaInput");
        boolean hasJava = java.isObject() && java.size() > 0;
        String id = text(node, "id");
        // SOURCE XOR, deliberately an error rather than a precedence rule. If javaInput merely
        // "won" when present, a fixture could carry a vestigial input string that reads like the
        // source and is not - a field that appears present and does nothing, in the file a
        // reviewer opens first.
        JsonNode rawInput = node.get("input");
        boolean hasInput = rawInput != null && !rawInput.isNull() && !rawInput.asText().isEmpty();
        if (hasInput && hasJava) {
            throw new IllegalStateException("fixture " + id + " declares BOTH input and javaInput - "
                    + "one of them would be silently ignored");
        }
        if (!hasInput && !hasJava) {
            throw new IllegalStateException("fixture " + id + " declares no source document");
        }
        return new FidelityFixture(
                id,
                text(node, "family"),
                text(node, "title"),
                text(node, "stack"),
                node.path("referenceIssue").asText(""),
                text(node, "rationale"),
                text(node, "catchesBugClass"),
                text(node, "cannotCatch"),
                node.path("configDescription").asText("defaults"),
                node.path("predicted"),
                node.path("config"),
                probe.isObject() ? probe : null,
                hasInput ? rawInput.asText() : null,
                hasJava ? java : null,
                node.path("expected"));
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull() || v.asText().isEmpty()) {
            throw new IllegalStateException(
                    "fixture is missing required field '" + field + "': " + node.path("id").asText("<no id>"));
        }
        return v.asText();
    }
}
