package io.github.pierce.gates;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Renders the library's externally visible API surface as a sorted set of stable signature lines.
 *
 * <p>WHY A HAND-ROLLED EXTRACTOR RATHER THAN japicmp OR revapi. Both are better tools and both were
 * considered. Both also resolve their comparison baseline from a remote repository, and this
 * build's verification path runs with {@code -o}: adding a plugin that must reach the network turns
 * an offline build from "passes" into "fails to resolve", which is a worse regression than the gap
 * being closed. This extractor reads only {@code target/classes} and a checked-in text file, so it
 * works offline, in an air-gapped clone, and in review as a visible diff. If the project later
 * gains a network-permitted verification job, japicmp is the upgrade and this can go.
 *
 * <p>WHAT COUNTS AS THE SURFACE. A member is included when it is reachable by a consumer compiling
 * against the artifact: the declaring type must be public (and every enclosing type public), and
 * the member must be {@code public} or {@code protected} — {@code protected} counts because a
 * consumer can subclass a non-final public type and bind to it. Synthetic and bridge members are
 * excluded: the compiler regenerates those, so they are an implementation detail that would make
 * the snapshot churn without a real change. Enum constants are emitted as fields, so removing one
 * is caught.
 *
 * <p>WHAT THE SIGNATURE CAPTURES. Erased parameter types and erased return type, because those are
 * what binary compatibility is defined over. Generic signatures are deliberately NOT captured: a
 * change from {@code List<String>} to {@code List<T>} is source-visible but binary-compatible, and
 * including it would make the snapshot fail on changes that break nobody. That is a known and
 * accepted blind spot, recorded here rather than discovered later.
 */
final class PublicApiSurface {

    private PublicApiSurface() {
    }

    /** Only these package roots are the library's own surface. */
    private static final String PACKAGE_ROOT = "io.github.pierce";

    /**
     * Avro's Maven plugin writes generated record classes into the same artifact. They are public
     * and they are not ours to keep stable — a schema edit legitimately rewrites them.
     */
    private static boolean isGenerated(Class<?> c) {
        return c.getName().contains(".generated.")
                || c.isAnnotationPresent(org.apache.avro.specific.AvroGenerated.class);
    }

    /** Every enclosing type must be public, or a consumer cannot name the member at all. */
    private static boolean isExternallyVisible(Class<?> c) {
        for (Class<?> k = c; k != null; k = k.getEnclosingClass()) {
            if (!Modifier.isPublic(k.getModifiers())) {
                return false;
            }
        }
        return true;
    }

    private static String typeName(Class<?> c) {
        return c.getCanonicalName() != null ? c.getCanonicalName() : c.getName();
    }

    private static String params(Class<?>[] ps) {
        return Stream.of(ps).map(PublicApiSurface::typeName).collect(Collectors.joining(","));
    }

    /** Modifiers that a consumer can actually observe. Ordered so the text is stable. */
    private static String mods(int m) {
        List<String> out = new ArrayList<>();
        if (Modifier.isPublic(m)) {
            out.add("public");
        }
        if (Modifier.isProtected(m)) {
            out.add("protected");
        }
        if (Modifier.isStatic(m)) {
            out.add("static");
        }
        if (Modifier.isFinal(m)) {
            out.add("final");
        }
        if (Modifier.isAbstract(m)) {
            out.add("abstract");
        }
        return String.join(" ", out);
    }

    /** Collect the surface of one already-loaded class. */
    static void collect(Class<?> c, SortedSet<String> into) {
        if (!isExternallyVisible(c) || isGenerated(c)) {
            return;
        }
        String kind = c.isInterface() ? "interface" : c.isEnum() ? "enum" : "class";
        into.add(String.format(Locale.ROOT, "TYPE %s %s %s", mods(c.getModifiers()), kind,
                typeName(c)));

        for (Constructor<?> k : c.getDeclaredConstructors()) {
            int m = k.getModifiers();
            if ((Modifier.isPublic(m) || Modifier.isProtected(m)) && !k.isSynthetic()) {
                into.add(String.format(Locale.ROOT, "CTOR %s %s(%s)", mods(m), typeName(c),
                        params(k.getParameterTypes())));
            }
        }
        for (Method k : c.getDeclaredMethods()) {
            int m = k.getModifiers();
            if ((Modifier.isPublic(m) || Modifier.isProtected(m))
                    && !k.isSynthetic() && !k.isBridge()) {
                into.add(String.format(Locale.ROOT, "METH %s %s %s.%s(%s)", mods(m),
                        typeName(k.getReturnType()), typeName(c), k.getName(),
                        params(k.getParameterTypes())));
            }
        }
        for (Field k : c.getDeclaredFields()) {
            int m = k.getModifiers();
            if ((Modifier.isPublic(m) || Modifier.isProtected(m)) && !k.isSynthetic()) {
                into.add(String.format(Locale.ROOT, "FIELD %s %s %s.%s", mods(m),
                        typeName(k.getType()), typeName(c), k.getName()));
            }
        }
    }

    /**
     * Walk a compiled-classes directory and render the whole surface.
     *
     * <p>A class that fails to load is reported rather than skipped. Silently skipping is how a
     * surface gate ends up passing because it measured nothing — the failure mode this repository
     * keeps finding.
     */
    static SortedSet<String> fromClassesDirectory(Path classesDir, ClassLoader loader) {
        SortedSet<String> out = new TreeSet<>();
        List<String> unloadable = new ArrayList<>();
        int seen = 0;
        try (Stream<Path> walk = Files.walk(classesDir)) {
            List<Path> classFiles = walk
                    .filter(p -> p.toString().endsWith(".class"))
                    .sorted()
                    .collect(Collectors.toList());
            for (Path p : classFiles) {
                String rel = classesDir.relativize(p).toString()
                        .replace('\\', '/').replaceAll("\\.class$", "");
                String binary = rel.replace('/', '.');
                if (!binary.startsWith(PACKAGE_ROOT) || binary.endsWith("module-info")
                        || binary.endsWith("package-info")) {
                    continue;
                }
                seen++;
                try {
                    collect(Class.forName(binary, false, loader), out);
                } catch (Throwable t) {
                    unloadable.add(binary + " (" + t.getClass().getSimpleName() + ")");
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (seen == 0) {
            throw new AssertionError("THE SURFACE SCAN FOUND NO CLASSES under " + classesDir
                    + ". An empty scan would make every containment check below trivially true.");
        }
        // Loading needs optional/provided deps (Spark) that are absent from some profiles; a few
        // NoClassDefFoundError misses are tolerable, a landslide means the scan is not measuring.
        if (unloadable.size() > seen / 4) {
            throw new AssertionError("THE SURFACE SCAN COULD NOT LOAD " + unloadable.size()
                    + " of " + seen + " classes, so it is not measuring the real surface. First few: "
                    + unloadable.subList(0, Math.min(8, unloadable.size())));
        }
        return out;
    }

    static SortedSet<String> readBaseline(Path file) {
        try {
            SortedSet<String> out = new TreeSet<>();
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                String t = line.strip();
                if (!t.isEmpty() && !t.startsWith("#")) {
                    out.add(t);
                }
            }
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
