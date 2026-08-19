package io.github.pierce.docs;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compiles one published snippet in-process, against the real test classpath.
 *
 * <h2>Why the classpath comes from a file and not from the JVM</h2>
 *
 * <p>Surefire runs {@code forkCount=1, reuseForks=false} with a manifest-only booter jar, so
 * {@code System.getProperty("java.class.path")} inside a test is ONE jar. A gate that silently
 * compiles against a one-jar classpath has two outcomes and both are bad: it fails everything
 * (loud and survivable) or somebody "fixes" it into warnings-only (silent and fatal). The
 * classpath is therefore read from {@code target/test-cp.txt}, written by a
 * {@code maven-dependency-plugin:build-classpath} execution bound to
 * {@code generate-test-resources}, and its existence is asserted rather than assumed.</p>
 */
final class DocSnippetCompiler {

    /** Matches the project's {@code maven.compiler.release}. Asserted against the pom by the gate. */
    static final String RELEASE = "17";

    private static final Pattern TOP_LEVEL_TYPE =
            Pattern.compile("(?m)^\\s*(?:public\\s+|final\\s+|abstract\\s+)*"
                    + "(?:class|interface|enum|record)\\s+(\\w+)");

    private DocSnippetCompiler() {
    }

    /** The real test classpath, from the file the build writes. Never silently empty. */
    static String classpath() {
        Path root = DocSnippetSource.moduleRoot();
        Path cpFile = root.resolve("target/test-cp.txt");
        if (!Files.isRegularFile(cpFile)) {
            throw new AssertionError("DOC SNIPPET GATE DID NOT RUN: " + cpFile + " is missing.\n"
                    + "It is written by the maven-dependency-plugin 'build-test-classpath' "
                    + "execution at generate-test-resources. Without it this gate would have to "
                    + "guess its own classpath from java.class.path, which under surefire's "
                    + "manifest-only booter jar is a single entry - and a gate compiling against "
                    + "one jar is a gate that only ever fails, until somebody makes it a gate "
                    + "that only ever passes.");
        }
        String text;
        try {
            text = Files.readString(cpFile, StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            throw new AssertionError("cannot read " + cpFile, e);
        }
        if (text.isEmpty()) {
            throw new AssertionError("DOC SNIPPET GATE DID NOT RUN: " + cpFile + " is blank");
        }
        String sep = java.io.File.pathSeparator;
        return root.resolve("target/classes") + sep + root.resolve("target/test-classes") + sep + text;
    }

    /**
     * Splits a fragment's own leading {@code import} lines off the front of its body.
     *
     * <p>A published example is usually more useful WITH its imports, and a fragment cannot
     * declare an import inside a method. Hoisting them into the generated unit keeps the document
     * honest without weakening anything: an import cannot stub a method or invent a type, so
     * nothing a snippet imports can make a phantom API compile. Contrast the templates, which
     * COULD - which is why they are restricted to variables and drilled for it.</p>
     *
     * @return {@code [hoisted imports, remaining body]}
     */
    static String[] hoistImports(String code) {
        StringBuilder imports = new StringBuilder();
        StringBuilder body = new StringBuilder();
        boolean stillLeading = true;
        for (String line : code.split("\n", -1)) {
            String t = line.strip();
            if (stillLeading && (t.startsWith("import ") || t.isEmpty()
                    || (t.startsWith("//") && imports.length() == 0))) {
                if (t.startsWith("import ")) {
                    imports.append(t).append('\n');
                    continue;
                }
                if (t.isEmpty() && imports.length() > 0) {
                    continue;
                }
                if (t.startsWith("//")) {
                    body.append(line).append('\n');
                    continue;
                }
            }
            if (!t.isEmpty()) {
                stillLeading = false;
            }
            body.append(line).append('\n');
        }
        return new String[] {imports.toString(), body.toString()};
    }

    /** Wraps a snippet into a whole compilation unit according to its directive. */
    static String wrap(DocSnippetSource.Snippet s, int ordinal) {
        String name = "DocSnippet_" + ordinal;
        if (s.kind() == DocSnippetSource.Kind.UNIT) {
            return s.code();
        }
        String[] split = hoistImports(s.code());
        String head = SnippetEnvironments.imports(s.env()) + "\n" + split[0] + "\n"
                + "@SuppressWarnings(\"all\")\n"
                + "final class " + name + " {\n";
        switch (s.kind()) {
            case BODY:
                return head
                        + "    static void run() throws Exception {\n"
                        + SnippetEnvironments.locals(s.env()) + "\n"
                        + split[1]
                        + "    }\n"
                        + "}\n";
            case MEMBERS:
                return head + split[1] + "}\n";
            default:
                throw new AssertionError("pseudo snippets are not compiled: " + s.where());
        }
    }

    /** The compilation-unit name javac will insist on for a wrapped source. */
    static String unitName(DocSnippetSource.Snippet s, int ordinal, String source) {
        if (s.kind() == DocSnippetSource.Kind.UNIT) {
            Matcher m = TOP_LEVEL_TYPE.matcher(source);
            if (!m.find()) {
                throw new AssertionError(s.where() + " is marked 'unit' but declares no top-level "
                        + "class, interface, enum or record. Use 'members' or 'body' for a "
                        + "fragment; 'unit' means a whole compilation unit.");
            }
            return m.group(1);
        }
        return "DocSnippet_" + ordinal;
    }

    static JavaCompiler javac() {
        JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
        if (javac == null) {
            throw new AssertionError("DOC SNIPPET GATE DID NOT RUN: "
                    + "ToolProvider.getSystemJavaCompiler() returned null, which means this build "
                    + "is running under a JRE, not a JDK. The gate cannot compile anything and "
                    + "must not be treated as having passed.");
        }
        return javac;
    }

    /**
     * One shared file manager for every snippet.
     *
     * <p>Building a {@code StandardJavaFileManager} scans the whole classpath, and this project's
     * test classpath carries Spark, Hadoop, Iceberg and Avro. Sixty fresh managers is minutes of
     * wall time, and a gate slow enough to be annoying is a gate somebody moves behind a profile -
     * which is a way for it to stop running. Compilation itself stays PER SNIPPET so one bad
     * block cannot cascade into, mask or duplicate another's diagnostics.</p>
     */
    private static StandardJavaFileManager sharedFiles;

    static synchronized StandardJavaFileManager files() {
        if (sharedFiles == null) {
            sharedFiles = javac().getStandardFileManager(null, null, StandardCharsets.UTF_8);
        }
        return sharedFiles;
    }

    static synchronized void closeFiles() {
        if (sharedFiles != null) {
            try {
                sharedFiles.close();
            } catch (IOException ignored) {
                // Nothing actionable at teardown; the JVM is about to exit.
            }
            sharedFiles = null;
        }
    }

    /** Compiles one source and returns every ERROR diagnostic, rendered. */
    static List<String> errors(String unitName, String source, Path outputDir) {
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        try {
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            throw new AssertionError("cannot create " + outputDir, e);
        }
        List<String> options = new ArrayList<>(List.of(
                "-classpath", classpath(),
                "-d", outputDir.toString(),
                "-proc:none",
                "-nowarn",
                "-Xlint:none",
                "--release", RELEASE));
        JavaFileObject unit = new SimpleJavaFileObject(
                URI.create("string:///" + unitName + ".java"), JavaFileObject.Kind.SOURCE) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                return source;
            }
        };
        javac().getTask(null, files(), diagnostics, options, null, List.of(unit)).call();
        List<String> out = new ArrayList<>();
        for (Diagnostic<? extends JavaFileObject> d : diagnostics.getDiagnostics()) {
            if (d.getKind() == Diagnostic.Kind.ERROR) {
                out.add("line " + d.getLineNumber() + ": " + d.getMessage(Locale.ROOT));
            }
        }
        return out;
    }
}
