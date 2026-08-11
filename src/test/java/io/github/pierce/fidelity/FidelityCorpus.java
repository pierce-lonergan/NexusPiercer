package io.github.pierce.fidelity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * The one loader for the corpus: manifest, entries and fixture files.
 *
 * <p>Extracted so the published-snippet gate reads the same corpus the guarantee test asserts.
 * Two loaders would be two ideas of what the corpus IS, and they could disagree about which
 * fixtures exist - the failure mode where a gate runs over a smaller set than it appears to.</p>
 *
 * <p>Every failure path fails loudly and re-throws rather than caching, so a broken manifest can
 * never be cached into silence and none of these can present as "0 fixtures executed, all good".</p>
 */
final class FidelityCorpus {

    static final String MANIFEST_RESOURCE = "/fidelity/manifest.json";

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Map<String, JsonNode> MANIFEST_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, JsonNode> ENTRY_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, FidelityFixture> FIXTURES = new ConcurrentHashMap<>();
    private static final Map<String, FidelityRunner.Measurement> MEASUREMENTS = new ConcurrentHashMap<>();

    private FidelityCorpus() {
    }

    static Path corpusRoot() {
        URL url = FidelityCorpus.class.getResource(MANIFEST_RESOURCE);
        if (url == null) {
            throw new AssertionError("FIDELITY CORPUS DID NOT RUN: " + MANIFEST_RESOURCE
                    + " is not on the test classpath. The guarantee is unverified - this is a "
                    + "failure, not an empty pass.");
        }
        try {
            return Path.of(url.toURI()).getParent();
        } catch (URISyntaxException e) {
            throw new AssertionError("FIDELITY CORPUS DID NOT RUN: manifest URL is not a file path: " + url, e);
        }
    }

    /** The module root, located from the classpath rather than from the working directory. */
    static Path moduleRoot() {
        Path p = corpusRoot();
        while (p != null && !Files.isRegularFile(p.resolve("pom.xml"))) {
            p = p.getParent();
        }
        if (p == null) {
            throw new AssertionError("could not find the module root (no pom.xml above "
                    + corpusRoot() + ")");
        }
        return p;
    }

    /** Parses once and caches, but ONLY on success. */
    static JsonNode manifest() {
        JsonNode cached = MANIFEST_CACHE.get("m");
        if (cached != null) {
            return cached;
        }
        JsonNode parsed = readManifest();
        MANIFEST_CACHE.put("m", parsed);
        return parsed;
    }

    private static JsonNode readManifest() {
        Path file = corpusRoot().resolve("manifest.json");
        String text;
        try {
            text = Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AssertionError("FIDELITY CORPUS DID NOT RUN: manifest is unreadable at " + file, e);
        }
        if (text.isBlank()) {
            throw new AssertionError("FIDELITY CORPUS DID NOT RUN: manifest at " + file
                    + " is empty. An empty contract is not a satisfied contract.");
        }
        JsonNode node;
        try {
            node = JSON.readTree(text);
        } catch (IOException e) {
            throw new AssertionError("FIDELITY CORPUS DID NOT RUN: manifest at " + file
                    + " is not parseable JSON", e);
        }
        JsonNode fixtures = node.get("fixtures");
        if (fixtures == null || !fixtures.isArray() || fixtures.isEmpty()) {
            throw new AssertionError("FIDELITY CORPUS DID NOT RUN: manifest at " + file
                    + " declares no fixtures. Zero fixtures is a failure, never a pass.");
        }
        return node;
    }

    static List<JsonNode> manifestEntries() {
        List<JsonNode> out = new ArrayList<>();
        manifest().get("fixtures").forEach(out::add);
        return out;
    }

    static Stream<String> manifestFixtureIds() {
        return manifestEntries().stream().map(e -> e.get("id").asText());
    }

    static JsonNode entry(String id) {
        return ENTRY_CACHE.computeIfAbsent(id, k -> {
            for (JsonNode e : manifestEntries()) {
                if (k.equals(e.get("id").asText())) {
                    return e;
                }
            }
            throw new AssertionError("no manifest entry for fixture id " + k);
        });
    }

    static FidelityFixture fixture(String id) {
        return FIXTURES.computeIfAbsent(id, k -> {
            JsonNode e = entry(k);
            Path file = corpusRoot().resolve(e.get("file").asText());
            if (!Files.isRegularFile(file)) {
                throw new AssertionError("MANIFEST REFERENCES A MISSING FIXTURE: " + e.get("file").asText()
                        + " is declared in the manifest but no such file exists under "
                        + corpusRoot() + ". The guarantee for '" + k + "' is unverifiable.");
            }
            try {
                return FidelityFixture.from(JSON.readTree(Files.readString(file, StandardCharsets.UTF_8)));
            } catch (IOException ex) {
                throw new AssertionError("fixture file is unreadable or unparseable: " + file, ex);
            }
        });
    }

    static FidelityRunner.Measurement measure(String id) {
        return MEASUREMENTS.computeIfAbsent(id, k -> FidelityRunner.run(fixture(k)));
    }
}
