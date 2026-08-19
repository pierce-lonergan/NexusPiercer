package io.github.pierce.docs;

// SNIPPET-BEGIN IMPORTS spark
import io.github.pierce.spark.NexusPiercerFunctions;
import io.github.pierce.spark.NexusPiercerPatterns;
import io.github.pierce.spark.NexusPiercerSparkPipeline;
import io.github.pierce.spark.NexusPiercerSparkPipeline.ErrorHandling;
import io.github.pierce.spark.NexusPiercerSparkPipeline.ProcessingResult;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.util.HashMap;
import java.util.Map;

import static io.github.pierce.spark.NexusPiercerFunctions.*;
import static org.apache.spark.sql.functions.*;
// SNIPPET-END IMPORTS spark

/**
 * Template for {@code env=spark}: a session and an already-loaded {@code Dataset}.
 *
 * <p>VARIABLES AND IMPORTS ONLY. See {@link SnippetEnvironments}.</p>
 *
 * <p>{@code spark} and {@code df} are declared, never built - {@link #locals()} is never
 * executed. It exists so javac type-checks the declarations on every build.</p>
 */
@SuppressWarnings({"unused", "PMD"})
final class SnippetEnvSpark {

    private SnippetEnvSpark() {
    }

    static void locals() throws Exception {
        // SNIPPET-BEGIN LOCALS spark
        SparkSession spark = SparkSession.builder().appName("doc").master("local[1]").getOrCreate();
        Dataset<Row> df = spark.emptyDataFrame();
        Dataset<Row> dimensionTable = spark.emptyDataFrame();
        // SNIPPET-END LOCALS spark
        touch(spark, df, dimensionTable);
    }

    private static void touch(Object... ignored) {
        // See SnippetEnvCore.touch.
    }
}
