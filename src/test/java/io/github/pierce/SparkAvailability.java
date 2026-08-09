package io.github.pierce;

import java.nio.channels.Selector;

/**
 * Probes whether this machine can actually host an embedded Spark session.
 *
 * <p><b>Why this exists.</b> Spark's RPC layer is built on Netty, and Netty's
 * {@code NioEventLoop} opens a {@link Selector} during construction. On Windows, the JDK's
 * default {@code WEPollSelectorProvider} implements that selector over a Unix-domain-socket
 * loopback pipe. Some Windows environments — endpoint security products, hardened network
 * policies, restricted sandboxes — reject that connection with
 * {@code SocketException: Invalid argument: connect}. Netty then reports the far less
 * illuminating {@code IllegalStateException: failed to create a child event loop}, and every
 * Spark test in the run errors out during {@code @BeforeAll}.</p>
 *
 * <p>That failure says nothing about this library. Treating it as a test failure would mean
 * either a permanently red local build or — the outcome this repository already reached once —
 * commenting the Spark tests out entirely, which is how {@code io.github.pierce.spark} came to
 * sit at 0% coverage across 2,995 instructions of shipped public API.</p>
 *
 * <p>So the tests self-skip where Spark genuinely cannot run, and execute everywhere it can,
 * including CI. A skip is visible in the Surefire report; a commented-out test is not.</p>
 *
 * <p>Set {@code -Dnexuspiercer.spark.tests=require} to turn an unavailable environment into a
 * hard failure. CI uses this so that a Linux runner losing the ability to start Spark is treated
 * as a broken build rather than silently skipped coverage.</p>
 */
public final class SparkAvailability {

    private static final String MODE_PROPERTY = "nexuspiercer.spark.tests";

    /** Probed once — the answer cannot change within a JVM, and the probe is not free. */
    private static final boolean AVAILABLE = probe();

    private static volatile String unavailableReason = "";

    private SparkAvailability() {
    }

    /**
     * @return {@code true} when an embedded Spark session can be started in this JVM
     * @throws IllegalStateException if Spark is unavailable and {@code -Dnexuspiercer.spark.tests=require}
     */
    public static boolean isAvailable() {
        if (!AVAILABLE && "require".equalsIgnoreCase(System.getProperty(MODE_PROPERTY))) {
            throw new IllegalStateException(
                    "Spark tests were required (-D" + MODE_PROPERTY + "=require) but this "
                            + "environment cannot open an NIO selector: " + unavailableReason);
        }
        return AVAILABLE;
    }

    /** Human-readable reason, for the skip message. */
    public static String reason() {
        return AVAILABLE
                ? "Spark is available"
                : "Spark cannot start in this environment: " + unavailableReason;
    }

    /**
     * Opens and immediately closes a selector. This is precisely the operation Netty performs
     * when constructing an event loop, so it succeeds exactly when Spark's RPC layer would.
     */
    private static boolean probe() {
        try (Selector selector = Selector.open()) {
            return selector.isOpen();
        } catch (Throwable t) {
            // Throwable, not Exception: a missing or misconfigured SelectorProvider surfaces as
            // an Error, and an unhandled one here would abort the whole run.
            Throwable root = t;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            unavailableReason = t.getClass().getSimpleName() + ": " + t.getMessage()
                    + " (root cause: " + root.getClass().getSimpleName() + ": " + root.getMessage() + ")";
            return false;
        }
    }
}
