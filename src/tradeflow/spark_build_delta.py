
from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


def main():
    root = Path(__file__).resolve().parents[2]

    spark_dir = root / "data" / "processed" / "spark"
    delta_dir = root / "data" / "processed" / "delta"

    sources = {
        "national_trade_2023": (
            spark_dir / "national_trade_2023"
        ),
        "import_partner_breakdown_2023": (
            spark_dir / "import_partner_breakdown_2023"
        ),
    }

    targets = {
        name: delta_dir / name
        for name in sources
    }

    builder = (
        SparkSession.builder
        .appName("TradeFlow-Delta-Conversion")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "4")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\nStarting TradeFlow Delta Lake Conversion")

        expected_counts = {
            "national_trade_2023": 10264,
            "import_partner_breakdown_2023": 99159,
        }

        for name, source_path in sources.items():

            target_path = targets[name]

            print(f"\nProcessing: {name}")

            if not source_path.exists():
                raise FileNotFoundError(
                    f"Parquet source not found: {source_path}"
                )

            # Read existing validated Parquet data
            df = spark.read.parquet(str(source_path))

            source_count = df.count()

            print("Source Parquet records:", source_count)

            assert source_count == expected_counts[name], (
                f"Unexpected source count for {name}"
            )

            # Write as Delta Lake
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .save(str(target_path))
            )

            # Confirm Delta table was created
            assert DeltaTable.isDeltaTable(
                spark, str(target_path)
            ), f"Delta table creation failed: {name}"

            # Read the saved Delta table independently
            delta_df = (
                spark.read
                .format("delta")
                .load(str(target_path))
            )

            delta_count = delta_df.count()

            print("Delta records:", delta_count)
            print("Delta table:", target_path)

            assert delta_count == source_count, (
                f"Record count mismatch for {name}"
            )

            print(f"{name}: CONVERSION PASSED")

        print("\nALL DELTA TABLES CREATED SUCCESSFULLY!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
