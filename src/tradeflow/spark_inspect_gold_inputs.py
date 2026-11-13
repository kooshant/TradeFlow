
from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def main():
    root = Path(__file__).resolve().parents[2]
    delta_dir = root / "data" / "processed" / "delta"

    tables = {
        "national_trade_2023": delta_dir / "national_trade_2023",
        "import_partner_breakdown_2023": (
            delta_dir / "import_partner_breakdown_2023"
        ),
    }

    builder = (
        SparkSession.builder
        .appName("TradeFlow-Gold-Input-Inspection")
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

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\nTRADEFLOW GOLD INPUT INSPECTION")

        for name, path in tables.items():
            print("\n" + "=" * 65)
            print("TABLE:", name)
            print("=" * 65)

            df = (
                spark.read
                .format("delta")
                .load(str(path))
            )

            print("\nRecord count:", df.count())
            print("Column count:", len(df.columns))

            print("\nSCHEMA:")
            df.printSchema()

            print("\nCOLUMN NAMES:")
            for index, column in enumerate(df.columns, start=1):
                print(f"{index}. {column}")

        print("\nGOLD INPUT INSPECTION COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
