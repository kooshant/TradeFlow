
from pathlib import Path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from delta import configure_spark_with_delta_pip


def main():
    root = Path(__file__).resolve().parents[2]

    source_path = (
        root / "data" / "processed" / "delta"
        / "national_trade_2023"
    )

    test_path = (
        root / "data" / "processed" / "delta"
        / "_tests" / "schema_enforcement"
    )

    builder = (
        SparkSession.builder
        .appName("TradeFlow-Schema-Enforcement-Test")
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
        print("\nStarting Delta Schema Enforcement Test")

        # Read actual curated Delta data
        source_df = (
            spark.read
            .format("delta")
            .load(str(source_path))
        )

        original_columns = source_df.columns

        print("\nSource records:", source_df.count())
        print("Source columns:", len(original_columns))

        # Clean only the isolated test directory
        if test_path.exists():
            shutil.rmtree(test_path)

        test_path.parent.mkdir(parents=True, exist_ok=True)

        # Create an isolated test table using one real record
        source_df.limit(1).write \
            .format("delta") \
            .mode("overwrite") \
            .save(str(test_path))

        print("\n1. TEST TABLE CREATED")
        print("Initial test records:", 1)

        # Introduce an unexpected column
        invalid_df = (
            source_df.limit(1)
            .withColumn(
                "unexpected_test_column",
                lit("schema_violation")
            )
        )

        print("\n2. TESTING INVALID SCHEMA APPEND")

        try:
            invalid_df.write \
                .format("delta") \
                .mode("append") \
                .save(str(test_path))

        except Exception as error:
            print("SCHEMA ENFORCEMENT: PASSED")
            print("Invalid schema rejected:", type(error).__name__)

        else:
            raise AssertionError(
                "Schema enforcement failed: "
                "unexpected column was accepted!"
            )

        # Verify original test table remains unchanged
        result_df = (
            spark.read
            .format("delta")
            .load(str(test_path))
        )

        final_count = result_df.count()
        final_columns = result_df.columns

        print("\n3. VERIFYING TEST TABLE")

        print("Final records:", final_count)
        print("Final columns:", len(final_columns))

        assert final_count == 1
        assert final_columns == original_columns
        assert "unexpected_test_column" not in final_columns

        print("\nTABLE INTEGRITY: PASSED")
        print("\nDELTA SCHEMA ENFORCEMENT TEST PASSED!")

    finally:
        spark.stop()

        # Remove only the temporary test table
        if test_path.exists():
            shutil.rmtree(test_path)


if __name__ == "__main__":
    main()
