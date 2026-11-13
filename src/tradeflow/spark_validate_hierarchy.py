
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
        .appName("TradeFlow-Hierarchy-Validation")
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

        print("\nTRADEFLOW HS HIERARCHY VALIDATION")

        # 1. Record counts and values at each HS level
        print("\n1. Records and trade values by HS level")

        hierarchy_summary = (
            df.groupBy("hs_level", "trade_flow")
            .agg(
                F.count("*").alias("record_count"),
                F.sum("trade_value_usd").alias("total_value_usd")
            )
            .orderBy("hs_level", "trade_flow")
        )

        hierarchy_summary.show(50, truncate=False)

        # 2. Compare chapter totals with six-digit totals
        print("\n2. Chapter versus six-digit reconciliation")

        comparison = (
            df.filter(
                F.col("hs_level").isin("chapter", "subheading")
                & (
                    (F.col("hs_code_length") == 2)
                    | (F.col("hs_code_length") == 6)
                )
            )
            .groupBy("trade_flow")
            .agg(
                F.sum(
                    F.when(
                        F.col("hs_code_length") == 2,
                        F.col("trade_value_usd")
                    ).otherwise(0)
                ).alias("chapter_total_usd"),

                F.sum(
                    F.when(
                        F.col("hs_code_length") == 6,
                        F.col("trade_value_usd")
                    ).otherwise(0)
                ).alias("six_digit_total_usd")
            )
            .withColumn(
                "difference_usd",
                F.col("chapter_total_usd") - F.col("six_digit_total_usd")
            )
        )

        comparison.show(truncate=False)

        # 3. Check whether six-digit records have duplicate codes
        print("\n3. Duplicate six-digit codes by trade flow")

        duplicates = (
            df.filter(F.col("hs_code_length") == 6)
            .groupBy("trade_flow", "cmdCode")
            .count()
            .filter(F.col("count") > 1)
        )

        print("Duplicate code groups:", duplicates.count())
        duplicates.show(10, truncate=False)

        print("\nHierarchy validation analysis completed!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
