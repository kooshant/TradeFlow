
from pathlib import Path

from pyspark.sql import SparkSession, Window

from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    round as spark_round,
    row_number,
    desc,
    asc,
    lit
)

from delta import configure_spark_with_delta_pip


def main():

    # --------------------------------------------------
    # 1. PROJECT PATHS
    # --------------------------------------------------

    root = Path(__file__).resolve().parents[2]

    source_path = (
        root / "data" / "processed" / "delta"
        / "enriched" / "national_trade_2023"
    )

    output_path = (
        root / "data" / "processed" / "delta"
        / "gold" / "product_trade_analytics_2023"
    )

    # --------------------------------------------------
    # 2. SPARK SESSION
    # --------------------------------------------------

    builder = (
        SparkSession.builder
        .appName("TradeFlow-Gold-Product-Analytics")
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

        print("\nTRADEFLOW GOLD: PRODUCT TRADE ANALYTICS")
        print("=" * 70)

        # --------------------------------------------------
        # 3. READ ENRICHED NATIONAL DATA
        # --------------------------------------------------

        source = (
            spark.read
            .format("delta")
            .load(str(source_path))
        )

        source_count = source.count()

        print("\nSOURCE VALIDATION")
        print("Source records:", source_count)

        if source_count == 0:
            raise ValueError("Source table is empty")

        # --------------------------------------------------
        # 4. AGGREGATE AT PRODUCT LEVEL
        # --------------------------------------------------

        product_gold = (
            source
            .groupBy(
                "refYear",
                "classificationCode",
                "trade_flow",
                "trade_value_basis",
                "cmdCode",
                "commodity_description",
                "hs_chapter"
            )
            .agg(
                spark_sum("trade_value_usd").alias(
                    "product_trade_value_usd"
                ),
                count("*").alias(
                    "source_record_count"
                )
            )
        )

        print("\nPRODUCT AGGREGATION")
        print(
            "Aggregated product records:",
            product_gold.count()
        )

        # --------------------------------------------------
        # 5. ADD REPORTER METADATA
        # --------------------------------------------------

        product_gold = (
            product_gold
            .withColumn("reporterISO", lit("IND"))
            .withColumn("reporterDesc", lit("India"))
        )

        # --------------------------------------------------
        # 6. CALCULATE FLOW TOTALS AND PRODUCT SHARE
        # --------------------------------------------------

        flow_window = Window.partitionBy("trade_flow")

        product_gold = (
            product_gold
            .withColumn(
                "total_flow_trade_value_usd",
                spark_sum("product_trade_value_usd").over(
                    flow_window
                )
            )
            .withColumn(
                "trade_value_share_pct",
                spark_round(
                    (
                        col("product_trade_value_usd")
                        / col("total_flow_trade_value_usd")
                    ) * 100,
                    6
                )
            )
        )

        # --------------------------------------------------
        # 7. RANK PRODUCTS WITHIN EACH FLOW
        # --------------------------------------------------

        ranking_window = (
            Window
            .partitionBy("trade_flow")
            .orderBy(
                desc("product_trade_value_usd"),
                asc("cmdCode")
            )
        )

        product_gold = (
            product_gold
            .withColumn(
                "product_rank",
                row_number().over(ranking_window)
            )
        )

        # --------------------------------------------------
        # 8. VALIDATE GOLD DATA
        # --------------------------------------------------

        print("\nPRODUCT GOLD VALIDATION")

        gold_count = product_gold.count()

        print("Gold product records:", gold_count)

        if gold_count != source_count:
            raise ValueError(
                "Product aggregation record count mismatch"
            )

        duplicate_keys = (
            product_gold
            .groupBy("trade_flow", "cmdCode")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        print("Duplicate product keys:", duplicate_keys)

        if duplicate_keys > 0:
            raise ValueError(
                "Duplicate product keys detected"
            )

        required_columns = [
            "reporterISO",
            "reporterDesc",
            "refYear",
            "trade_flow",
            "trade_value_basis",
            "cmdCode",
            "commodity_description",
            "product_trade_value_usd",
            "trade_value_share_pct",
            "product_rank"
        ]

        for column_name in required_columns:

            null_count = (
                product_gold
                .filter(col(column_name).isNull())
                .count()
            )

            if null_count > 0:
                raise ValueError(
                    f"Null values in {column_name}: {null_count}"
                )

        invalid_values = product_gold.filter(
            (col("product_trade_value_usd") < 0)
            | (col("trade_value_share_pct") < 0)
            | (col("product_rank") < 1)
            | (col("total_flow_trade_value_usd") <= 0)
        ).count()

        if invalid_values > 0:
            raise ValueError(
                "Invalid product analytics values detected"
            )

        print("Required columns validation passed!")

        # --------------------------------------------------
        # 9. RECONCILE SOURCE AND GOLD TOTALS
        # --------------------------------------------------

        print("\nTRADE VALUE RECONCILIATION")

        source_totals = (
            source
            .groupBy("trade_flow")
            .agg(
                spark_sum("trade_value_usd").alias(
                    "source_total"
                )
            )
        )

        gold_totals = (
            product_gold
            .groupBy("trade_flow")
            .agg(
                spark_sum("product_trade_value_usd").alias(
                    "gold_total"
                )
            )
        )

        reconciliation = (
            source_totals
            .join(
                gold_totals,
                on="trade_flow",
                how="full_outer"
            )
            .withColumn(
                "difference",
                col("source_total") - col("gold_total")
            )
        )

        reconciliation.show(truncate=False)

        failed = reconciliation.filter(
            col("source_total").isNull()
            | col("gold_total").isNull()
            | (col("difference") > 0.01)
            | (col("difference") < -0.01)
        ).count()

        if failed > 0:
            raise ValueError(
                "Product trade value reconciliation failed"
            )

        print("Product trade value reconciliation passed!")

        # --------------------------------------------------
        # 10. VALIDATE PRODUCT SHARES
        # --------------------------------------------------

        print("\nPRODUCT SHARE VALIDATION")

        share_totals = (
            product_gold
            .groupBy("trade_flow")
            .agg(
                spark_sum("trade_value_share_pct").alias(
                    "total_share_pct"
                )
            )
        )

        share_totals.show(truncate=False)

        invalid_shares = share_totals.filter(
            (col("total_share_pct") < 99.99)
            | (col("total_share_pct") > 100.01)
        ).count()

        if invalid_shares > 0:
            raise ValueError(
                "Product share totals do not reconcile to 100%"
            )

        # --------------------------------------------------
        # 11. DISPLAY TOP 10 EXPORT PRODUCTS
        # --------------------------------------------------

        print("\nTOP 10 EXPORT PRODUCTS")

        (
            product_gold
            .filter(col("trade_flow") == "Export")
            .orderBy("product_rank")
            .select(
                "product_rank",
                "cmdCode",
                "commodity_description",
                "product_trade_value_usd",
                "trade_value_share_pct"
            )
            .show(10, truncate=False)
        )

        # --------------------------------------------------
        # 12. DISPLAY TOP 10 IMPORT PRODUCTS
        # --------------------------------------------------

        print("\nTOP 10 IMPORT PRODUCTS")

        (
            product_gold
            .filter(col("trade_flow") == "Import")
            .orderBy("product_rank")
            .select(
                "product_rank",
                "cmdCode",
                "commodity_description",
                "product_trade_value_usd",
                "trade_value_share_pct"
            )
            .show(10, truncate=False)
        )

        # --------------------------------------------------
        # 13. WRITE GOLD DELTA TABLE
        # --------------------------------------------------

        print("\nWRITING PRODUCT GOLD DELTA TABLE")

        (
            product_gold
            .write
            .format("delta")
            .mode("errorifexists")
            .save(str(output_path))
        )

        print("\nGOLD TABLE CREATED:")
        print(output_path)

        # --------------------------------------------------
        # 14. READ BACK AND VALIDATE
        # --------------------------------------------------

        saved = (
            spark.read
            .format("delta")
            .load(str(output_path))
        )

        saved_count = saved.count()

        print("\nSAVED GOLD TABLE VALIDATION")
        print("Saved records:", saved_count)

        if saved_count != gold_count:
            raise ValueError(
                "Saved Gold record count mismatch"
            )

        metadata_errors = saved.filter(
            (col("reporterISO") != "IND")
            | (col("reporterDesc") != "India")
        ).count()

        if metadata_errors > 0:
            raise ValueError(
                "Reporter metadata validation failed"
            )

        saved.groupBy("trade_flow").count().show()

        print("\n" + "=" * 70)
        print("PRODUCT TRADE ANALYTICS CREATED SUCCESSFULLY!")
        print("ALL PRODUCT GOLD VALIDATIONS PASSED!")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
