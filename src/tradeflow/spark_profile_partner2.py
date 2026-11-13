
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    root = Path(__file__).resolve().parents[2]

    path = (
        root / "data" / "processed" / "silver"
        / "un_comtrade" / "india_trade_2023_silver.csv"
    )

    spark = (
        SparkSession.builder
        .appName("TradeFlow-Partner2-Profile")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(str(path))
        )

        detail = df.filter(F.col("hs_code_length") == 6)

        print("\nSECONDARY PARTNER PROFILE")

        print("\n1. Distinct secondary partner codes")

        print(
            "Unique partner2 codes:",
            detail.select("partner2Code").distinct().count()
        )

        print("\n2. Records by secondary partner code")

        (
            detail.groupBy("partner2Code")
            .agg(
                F.count("*").alias("record_count"),
                F.sum("trade_value_usd").alias("trade_value_usd")
            )
            .orderBy(F.desc("record_count"))
            .show(30, truncate=False)
        )

        print("\n3. Secondary partner code distribution by flow")

        (
            detail.groupBy("trade_flow", "partner2Code")
            .count()
            .orderBy("trade_flow", F.desc("count"))
            .show(30, truncate=False)
        )

        print("\n4. Null and special code counts")

        (
            detail.select(
                F.sum(F.col("partner2Code").isNull().cast("int"))
                .alias("null_codes"),

                F.sum((F.col("partner2Code") == 0).cast("int"))
                .alias("zero_codes"),

                F.sum((F.col("partner2Code") < 0).cast("int"))
                .alias("negative_codes")
            )
            .show()
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
