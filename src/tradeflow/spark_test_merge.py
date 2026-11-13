
from pathlib import Path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    sha2,
    to_json,
    struct
)

from delta import (
    configure_spark_with_delta_pip,
    DeltaTable
)


def main():
    root = Path(__file__).resolve().parents[2]

    source_path = (
        root / "data" / "processed" / "delta"
        / "national_trade_2023"
    )

    test_path = (
        root / "data" / "processed" / "delta"
        / "_tests" / "merge_test"
    )

    builder = (
        SparkSession.builder
        .appName("TradeFlow-Delta-Merge-Test")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = (
        configure_spark_with_delta_pip(builder)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\nStarting Delta MERGE Test")

        # Read actual curated data
        source_df = (
            spark.read
            .format("delta")
            .load(str(source_path))
        )

        original_columns = source_df.columns

        # Cache two actual records for consistent testing
        sample_df = source_df.limit(2)
        sample_count = sample_df.count()

        if sample_count != 2:
            raise ValueError("Expected at least two source records")

        print("\nActual source records selected:", sample_count)

        # Create a stable key from the original record values
        record_key = sha2(
            to_json(struct(*[col(c) for c in original_columns])),
            256
        )

        # Clean only the isolated test directory
        if test_path.exists():
            shutil.rmtree(test_path)

        test_path.parent.mkdir(parents=True, exist_ok=True)

        # Initial target: one real record with PENDING status
        initial_target = (
            sample_df.limit(1)
            .withColumn("test_record_key", record_key)
            .withColumn("validation_status", lit("PENDING"))
        )

        initial_target.write \
            .format("delta") \
            .mode("overwrite") \
            .save(str(test_path))

        print("\n1. INITIAL DELTA TABLE CREATED")
        print("Initial records:", 1)

        # Merge source: two real records with UPDATED status
        merge_source = (
            sample_df
            .withColumn("test_record_key", record_key)
            .withColumn("validation_status", lit("UPDATED"))
        )

        # Perform MERGE
        delta_table = DeltaTable.forPath(
            spark,
            str(test_path)
        )

        (
            delta_table.alias("target")
            .merge(
                merge_source.alias("source"),
                "target.test_record_key = source.test_record_key"
            )
            .whenMatchedUpdate(
                set={
                    "validation_status": "source.validation_status"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("\n2. DELTA MERGE EXECUTED")

        # Verify results
        result_df = (
            spark.read
            .format("delta")
            .load(str(test_path))
        )

        final_count = result_df.count()

        print("\n3. MERGE VALIDATION")
        print("Final records:", final_count)

        result_df.groupBy("validation_status").count().show()

        assert final_count == 2

        updated_count = (
            result_df
            .filter(col("validation_status") == "UPDATED")
            .count()
        )

        assert updated_count == 2

        print("Matched update: PASSED")
        print("Unmatched insert: PASSED")

        # Verify Delta transaction history
        print("\n4. DELTA HISTORY")

        history = delta_table.history()

        history.select(
            "version",
            "operation"
        ).show(truncate=False)

        latest_operation = (
            history
            .orderBy(col("version").desc())
            .first()["operation"]
        )

        assert latest_operation == "MERGE"

        # Verify original table snapshot
        version_zero = (
            spark.read
            .format("delta")
            .option("versionAsOf", 0)
            .load(str(test_path))
        )

        assert version_zero.count() == 1

        print("\n5. TIME TRAVEL VALIDATION")
        print("Version 0 records:", version_zero.count())

        print("\nDELTA MERGE TEST PASSED!")

    finally:
        spark.stop()



        # Remove only the isolated test table
        if test_path.exists():
            shutil.rmtree(test_path)


if __name__ == "__main__":
    main()
