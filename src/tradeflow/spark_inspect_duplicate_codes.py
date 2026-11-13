
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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

    spark = (
        SparkSession.builder
        .appName("TradeFlow-Duplicate-Code-Investigation")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(str(silver_path))
        )

        detail_df = df.filter(F.col("hs_code_length") == 6)

        # 1. Count repeated HS codes
        code_counts = (
            detail_df
            .groupBy("trade_flow", "cmdCode")
            .agg(F.count("*").alias("record_count"))
        )

        repeated_codes = code_counts.filter(
            F.col("record_count") > 1
        )

        print("\n1. REPEATED HS CODE STATISTICS")

        print("Total detailed records:", detail_df.count())
        print("Unique flow-code combinations:", code_counts.count())
        print("Repeated code groups:", repeated_codes.count())
        print(
            "Records belonging to repeated groups:",
            repeated_codes.agg(
                F.sum("record_count")
            ).first()[0]
        )

        # 2. Inspect repeated records
        print("\n2. INSPECTING REPEATED HS CODE RECORDS")

        top_codes = (
            repeated_codes
            .orderBy(F.desc("record_count"))
            .limit(5)
        )

        sample_records = (
            detail_df
            .join(
                top_codes.select("trade_flow", "cmdCode"),
                on=["trade_flow", "cmdCode"],
                how="inner"
            )
            .select(
                "trade_flow",
                "cmdCode",
                "partnerCode",
                "partner2Code",
                "classificationCode",
                "customsCode",
                "mosCode",
                "motCode",
                "qtyUnitCode",
                "altQtyUnitCode",
                "legacyEstimationFlag",
                "primaryValue",
                "qty",
                "netWgt",
                "isReported",
                "isAggregate"
            )
            .orderBy("trade_flow", "cmdCode")
        )

        sample_records.show(30, truncate=False)

        # 3. Check exact duplicate rows
        print("\n3. EXACT DUPLICATE ROW CHECK")

        original_count = df.count()
        distinct_count = df.dropDuplicates().count()

        print("Original Silver records:", original_count)
        print("Distinct Silver records:", distinct_count)
        print("Exact duplicate rows:", original_count - distinct_count)

        print("\nDuplicate-code investigation completed!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
