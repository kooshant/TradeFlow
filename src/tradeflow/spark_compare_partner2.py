
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
        .appName("TradeFlow-Partner2-Comparison")
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

        print("\n1. VALUE AND RECORD COUNTS BY FLOW AND PARTNER2")

        (
            detail.groupBy("trade_flow", "partner2Code")
            .agg(
                F.count("*").alias("records"),
                F.countDistinct("cmdCode").alias("unique_hs_codes"),
                F.sum("trade_value_usd").alias("total_value_usd")
            )
            .orderBy("trade_flow", "partner2Code")
            .show(300, truncate=False)
        )

        print("\n2. EXPORT SAMPLE: WORLD VS AREAS NES")

        exports = detail.filter(F.col("trade_flow") == "Export")

        (
            exports.filter(F.col("cmdCode").isin(
                "010121", "020110", "030111", "720421"
            ))
            .select(
                "cmdCode",
                "partner2Code",
                "trade_value_usd",
                "primaryValue"
            )
            .orderBy("cmdCode", "partner2Code")
            .show(50, truncate=False)
        )

        print("\n3. IMPORT SAMPLE: WORLD VS SECONDARY PARTNERS")

        imports = detail.filter(
            (F.col("trade_flow") == "Import")
            & (F.col("cmdCode") == "720421")
        )

        (
            imports.select(
                "cmdCode",
                "partner2Code",
                "trade_value_usd",
                "primaryValue"
            )
            .orderBy("partner2Code")
            .show(100, truncate=False)
        )

        print("\nPartner2 comparison completed!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
