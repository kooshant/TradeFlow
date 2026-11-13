
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
        .appName("TradeFlow-Silver-Transform")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\nStarting TradeFlow PySpark Transformation")

        # Step 1: Read Silver dataset
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(str(silver_path))
        )

        # Step 2: Select detailed six-digit HS records
        detail_df = (
            df.filter(
                (F.col("hs_level") == "subheading")
                & (F.col("hs_code_length") == 6)
            )
            .withColumn(
                "hs_chapter",
                F.substring(F.col("cmdCode"), 1, 2)
            )
        )

        print("\nDetailed Dataset Statistics")
        print("Total detailed records:", detail_df.count())
        print("Unique HS codes:", detail_df.select("cmdCode").distinct().count())

        # Step 3: Calculate exports and imports
        print("\nTrade Flow Summary")

        flow_summary = (
            detail_df
            .groupBy("trade_flow")
            .agg(
                F.count("*").alias("record_count"),
                F.sum("trade_value_usd").alias("total_trade_value_usd")
            )
            .orderBy("trade_flow")
        )

        flow_summary.show(truncate=False)

        # Step 4: Aggregate trade values by HS chapter
        print("\nTrade Value by HS Chapter")

        chapter_summary = (
            detail_df
            .groupBy("hs_chapter", "trade_flow")
            .agg(
                F.sum("trade_value_usd").alias("total_trade_value_usd"),
                F.count("*").alias("record_count")
            )
            .orderBy("hs_chapter", "trade_flow")
        )

        chapter_summary.show(20, truncate=False)

        # Step 5: Calculate trade balance
        print("\nIndia Trade Balance")

        trade_balance = (
            detail_df
            .groupBy()
            .agg(
                F.sum(
                    F.when(
                        F.col("trade_flow") == "Export",
                        F.col("trade_value_usd")
                    ).otherwise(0)
                ).alias("total_exports_usd"),

                F.sum(
                    F.when(
                        F.col("trade_flow") == "Import",
                        F.col("trade_value_usd")
                    ).otherwise(0)
                ).alias("total_imports_usd")
            )
            .withColumn(
                "trade_balance_usd",
                F.col("total_exports_usd") - F.col("total_imports_usd")
            )
        )

        trade_balance.show(truncate=False)

        print("\nPySpark Transformation Completed Successfully!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
