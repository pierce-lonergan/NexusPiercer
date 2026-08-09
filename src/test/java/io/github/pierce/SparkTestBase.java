package io.github.pierce;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class SparkTestBase {

    protected static SparkSession spark;

    @BeforeAll
    static void setUpSpark() {
        // Must be the first statement here, not in a subclass @BeforeEach: @BeforeAll runs
        // first, so a guard placed downstream never gets the chance to skip.
        org.junit.jupiter.api.Assumptions.assumeTrue(
                SparkAvailability.isAvailable(), SparkAvailability::reason);

        spark = SparkSession.builder()
                .appName("Test")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.warehouse.dir", "target/spark-warehouse")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }
}