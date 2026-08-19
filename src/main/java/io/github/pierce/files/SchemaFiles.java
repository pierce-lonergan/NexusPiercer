package io.github.pierce.files;

import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Reads a schema file the caller already knows the path of.
 *
 * <p>This is the replacement for {@link FileFinder} at every call site inside this library, and
 * the difference is the whole point: a schema path is a PATH, not a search query. {@code
 * SchemaFiles} resolves the literal name it is given and nothing else. No search paths, no
 * classpath probing, no parent-directory walk, no depth-five tree walk, no Levenshtein "did you
 * mean", no singleton, no thread pools, no caches.</p>
 *
 * <h2>What it does enforce</h2>
 * <ul>
 *   <li>a null byte in the name is refused, before anything else, because it corrupts every check
 *       that would follow;</li>
 *   <li>a relative escape ({@code ..}) is refused;</li>
 *   <li>the file is stat'd and refused if it exceeds {@link #maxBytes()};</li>
 *   <li>the returned stream is capped at the same limit, so a file that grew between the stat and
 *       the read, or one whose size could not be determined, still cannot exceed it.</li>
 * </ul>
 *
 * <p>The cap is applied twice deliberately. A stat-only check races the read, and a stream-only
 * check has already handed the caller some bytes by the time it fires.</p>
 *
 * <p>NOT A GENERAL FILE UTILITY. It is deliberately narrower than {@code FileFinder.Util}: it will
 * not fetch over HTTP, will not read from HDFS and will not transparently decompress. If you need
 * those, you want the Hadoop or HTTP client directly, where the timeouts and credentials are
 * yours to set.</p>
 *
 * @since 2.1.0
 */
public final class SchemaFiles {

    /**
     * 100 MB, matching {@code FileFinder}'s historical {@code maxFileSize} so repointing a call
     * site from one to the other does not change which files are accepted.
     */
    private static final long MAX_BYTES = 100L * 1024 * 1024;

    private SchemaFiles() {
    }

    /** The size ceiling, in bytes, applied to every read through this class. */
    public static long maxBytes() {
        return MAX_BYTES;
    }

    /**
     * Opens the file at {@code path}, size-capped.
     *
     * @param path the literal path to read; no searching is performed
     * @return a buffered, size-capped stream the caller must close
     * @throws SecurityException if the name carries a null byte or escapes its base
     * @throws IOException       if the file is absent, unreadable, or over the cap
     */
    public static InputStream open(String path) throws IOException {
        Path resolved = validate(path);

        long size = Files.size(resolved);
        if (size > MAX_BYTES) {
            throw new IOException(String.format(
                    "File '%s' is %d bytes, exceeding the %d byte limit", path, size, MAX_BYTES));
        }

        InputStream raw = Files.newInputStream(resolved);
        return new BufferedInputStream(new CappedStream(raw, MAX_BYTES, path), 64 * 1024);
    }

    /**
     * Reads the file at {@code path} as UTF-8 text, size-capped.
     *
     * @param path the literal path to read; no searching is performed
     * @return the file's content
     * @throws SecurityException if the name carries a null byte or escapes its base
     * @throws IOException       if the file is absent, unreadable, or over the cap
     */
    public static String readString(String path) throws IOException {
        try (InputStream in = open(path)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static Path validate(String path) throws IOException {
        if (path == null || path.trim().isEmpty()) {
            throw new IOException("Schema path cannot be null or empty");
        }
        if (path.indexOf((char) 0) >= 0) {
            // First, always. A NUL truncates the name in some native layers, so a check performed
            // after it would be testing a different string from the one that gets opened.
            throw new SecurityException("Null byte in schema path: '" + path + "'");
        }

        String normalised = path.replace('\\', '/');
        if (normalised.contains("../") || normalised.startsWith("..")
                || normalised.contains("/..")) {
            throw new SecurityException(
                    "Path traversal rejected: '" + path + "'. A relative escape would read files "
                            + "outside the working tree. Pass a path that stays inside it, or an "
                            + "absolute path to the file you mean.");
        }

        Path resolved;
        try {
            resolved = Paths.get(path);
        } catch (InvalidPathException notAPath) {
            throw new IOException("Not a valid path on this platform: '" + path + "'", notAPath);
        }
        if (!Files.isRegularFile(resolved)) {
            throw new java.io.FileNotFoundException("Schema file not found: '" + path + "'");
        }
        return resolved;
    }

    /**
     * Hard byte cap. Covers the window between the stat and the read, which a stat-only check
     * cannot.
     */
    private static final class CappedStream extends FilterInputStream {
        private final long limit;
        private final String name;
        private long read;

        CappedStream(InputStream in, long limit, String name) {
            super(in);
            this.limit = limit;
            this.name = name;
        }

        private void count(long n) throws IOException {
            if (n <= 0) {
                return;
            }
            read += n;
            if (read > limit) {
                throw new IOException(String.format(
                        "Limit of %d bytes exceeded while reading '%s': stopped after %d bytes",
                        limit, name, read));
            }
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b >= 0) {
                count(1);
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            count(n);
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            long skipped = super.skip(n);
            count(skipped);
            return skipped;
        }
    }
}
