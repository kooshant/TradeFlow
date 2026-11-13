
from pathlib import Path

from pyspark.sql import SparkSession
from delta import DeltaTable


def main():
    root = Path(__file__).resolve().parents[2]

    delta_dir = root / "data" / "processed" / "delta"

    tables = {
        "national_trade_2023": 10264,
        "import_partner_breakdown_2023": 99159,
    }

    builder = (
        SparkSession.builder
        .appName("TradeFlow-Delta-Validation")
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
        __import__(
            "delta",
            fromlist=["configure_spark_with_delta_pip"]
        )
        .configure_spark_with_delta_pip(builder)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\nStarting Delta Lake Feature Validation")

        for name, expected_count in tables.items():

            table_path = delta_dir / name
            table_location = str(table_path)

            print(f"\n{'=' * 50}")
            print("TABLE:", name)
            print("=" * 50)

            # 1. Verify Delta transaction log
            log_path = table_path / "_delta_log"

            assert log_path.exists(), (
                f"Transaction log missing: {name}"
            )

            assert DeltaTable.isDeltaTable(
                spark, table_location
            ), f"Invalid Delta table: {name}"

            print("\n1. TRANSACTION LOG: PASSED")

            # 2. Read current Delta table
            delta_table = DeltaTable.forPath(
                spark, table_location
            )

            current_df = delta_table.toDF()

            current_count = current_df.count()

            print("\n2. CURRENT TABLE VALIDATION")
            print("Current records:", current_count)

            assert current_count == expected_count

            # 3. Inspect transaction history
            history = delta_table.history()

            print("\n3. DELTA TABLE HISTORY")
            history.select(
                "version",
                "timestamp",
                "operation"
            ).show(truncate=False)

            latest_version = (
                history.agg({"version": "max"})
                .first()[0]
            )

            print("Latest table version:", latest_version)

            # 4. Time travel to initial version
            version_zero = (
                spark.read
                .format("delta")
                .option("versionAsOf", 0)
                .load(table_location)
            )

            historical_count = version_zero.count()

            print("\n4. TIME TRAVEL VALIDATION")
            print("Version 0 records:", historical_count)

            assert historical_count == expected_count

            print(f"\n{name}: ALL CHECKS PASSED")

        print("\nALL DELTA FEATURES VALIDATED SUCCESSFULLY!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
