
from pathlib import Path

from pyspark.sql import SparkSession


def main():
    project_root = Path(__file__).resolve().parents[2]

    silver_path = (
        project_root
        / "data"
        / "processed"
        / "silver"
        / "un_comtrade"
        / "india_trade_2023_silver.csv"
    )

    if not silver_path.exists():
        raise FileNotFoundError(f"Silver dataset not found: {silver_path}")

    spark = (
        SparkSession.builder
        .appName("TradeFlow-Silver-Reader")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\nReading TradeFlow Silver dataset...")

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(str(silver_path))
        )

        print("\nDataset Schema:")
        df.printSchema()

        print("\nTotal Records:", df.count())
        print("Total Columns:", len(df.columns))

        print("\nFirst 5 Records:")
        df.show(5, truncate=False)

        print("\nTrade Flow Distribution:")
        df.groupBy("trade_flow").count().show()

        print("\nSpark Silver Reading Completed Successfully!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
