
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    when
)

from delta import configure_spark_with_delta_pip


def profile_table(spark, table_name, table_path):
    print("\n" + "=" * 65)
    print("TABLE:", table_name)
    print("=" * 65)

    df = (
        spark.read
        .format("delta")
        .load(str(table_path))
    )

    print("\nTOTAL RECORDS:", df.count())

    print("\n1. TRADE FLOW DISTRIBUTION")

    if "trade_flow" in df.columns:
        df.groupBy("trade_flow").count().show(truncate=False)

    print("\n2. TRADE VALUE BASIS")

    if "trade_value_basis" in df.columns:
        df.groupBy("trade_value_basis").count().show(truncate=False)

    print("\n3. TRADE VALUE SUMMARY")

    if "trade_value_usd" in df.columns:
        df.select(
            spark_sum("trade_value_usd").alias("total_trade_value"),
            count(when(col("trade_value_usd").isNull(), 1)).alias(
                "null_trade_values"
            ),
            count(when(col("trade_value_usd") < 0, 1)).alias(
                "negative_trade_values"
            )
        ).show(truncate=False)

    print("\n4. PRODUCT DIMENSION")

    if "cmdCode" in df.columns:
        df.select(
            countDistinct("cmdCode").alias("unique_commodity_codes"),
            count(when(col("cmdCode").isNull(), 1)).alias(
                "null_commodity_codes"
            )
        ).show()

    if "cmdDesc" in df.columns:
        df.select(
            count(when(col("cmdDesc").isNull(), 1)).alias(
                "null_commodity_descriptions"
            ),
            countDistinct("cmdDesc").alias(
                "unique_commodity_descriptions"
            )
        ).show()

    print("\n5. HS CHAPTER DISTRIBUTION")

    if "hs_chapter" in df.columns:
        df.groupBy("hs_chapter").agg(
            count("*").alias("records"),
            spark_sum("trade_value_usd").alias("trade_value_usd")
        ).orderBy(
            col("trade_value_usd").desc()
        ).show(15, truncate=False)

    print("\n6. PARTNER DIMENSION")

    if "partner2Code" in df.columns:
        df.select(
            countDistinct("partner2Code").alias("unique_partner_codes"),
            count(when(col("partner2Code").isNull(), 1)).alias(
                "null_partner_codes"
            )
        ).show()

    if "partner2Desc" in df.columns:
        df.select(
            count(when(col("partner2Desc").isNull(), 1)).alias(
                "null_partner_descriptions"
            ),
            countDistinct("partner2Desc").alias(
                "unique_partner_descriptions"
            )
        ).show()

    print("\n7. QUANTITY AND ESTIMATION")

    quantity_columns = [
        c for c in [
            "qty",
            "netWgt",
            "grossWgt",
            "isQtyEstimated",
            "isNetWgtEstimated",
            "isReported",
            "isAggregate"
        ] if c in df.columns
    ]

    if quantity_columns:
        df.select(*quantity_columns).summary().show(truncate=False)

    print("\n8. TOP 10 PRODUCTS")

    if {"cmdCode", "cmdDesc", "trade_value_usd"}.issubset(df.columns):
        df.groupBy("cmdCode", "cmdDesc").agg(
            spark_sum("trade_value_usd").alias("trade_value_usd")
        ).orderBy(
            col("trade_value_usd").desc()
        ).show(10, truncate=False)


def main():
    root = Path(__file__).resolve().parents[2]

    delta_root = root / "data" / "processed" / "delta"

    builder = (
        SparkSession.builder
        .appName("TradeFlow-Gold-Input-Profiling")
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
        print("\nTRADEFLOW GOLD INPUT PROFILING")

        tables = {
            "national_trade_2023": (
                delta_root / "national_trade_2023"
            ),
            "import_partner_breakdown_2023": (
                delta_root / "import_partner_breakdown_2023"
            )
        }

        for table_name, table_path in tables.items():
            profile_table(
                spark,
                table_name,
                table_path
            )

        print("\nGOLD INPUT PROFILING COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
