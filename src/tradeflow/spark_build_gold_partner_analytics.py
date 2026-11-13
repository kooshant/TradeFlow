
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    max as spark_max,
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
        / "enriched" / "import_partner_breakdown_2023"
    )

    output_path = (
        root / "data" / "processed" / "delta"
        / "gold" / "import_partner_analytics_2023"
    )

    # --------------------------------------------------
    # 2. SPARK SESSION
    # --------------------------------------------------

    builder = (
        SparkSession.builder
        .appName("TradeFlow-Gold-Import-Partner-Analytics")
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

        print("\nTRADEFLOW GOLD: IMPORT PARTNER ANALYTICS")
        print("=" * 70)

        # --------------------------------------------------
        # 3. READ ENRICHED IMPORT PARTNER DATA
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
            raise ValueError("Import partner source is empty")

        # This dataset represents secondary partner-level
        # import breakdown, not country of origin.

        invalid_flows = (
            source
            .filter(col("trade_flow") != "Import")
            .count()
        )

        if invalid_flows > 0:
            raise ValueError(
                "Unexpected non-import records found"
            )

        # --------------------------------------------------
        # 4. AGGREGATE IMPORTS BY PARTNER AND PRODUCT
        # --------------------------------------------------

        partner_product = (
            source
            .groupBy(
                "refYear",
                "classificationCode",
                "trade_flow",
                "trade_value_basis",
                "partner2Code",
                "cmdCode",
                "commodity_description",
                "hs_chapter"
            )
            .agg(
                spark_sum("trade_value_usd").alias(
                    "partner_product_import_value_usd"
                ),
                count("*").alias(
                    "source_record_count"
                ),
                spark_max("partner_description").alias(
                    "partner2Desc"
                )
            )
            .withColumn("reporterISO", lit("IND"))
            .withColumn("reporterDesc", lit("India"))
        )

        print("\nPARTNER PRODUCT AGGREGATION")

        aggregated_count = partner_product.count()

        print(
            "Aggregated partner-product records:",
            aggregated_count
        )

        # --------------------------------------------------
        # 5. PARTNER-LEVEL IMPORT TOTALS
        # --------------------------------------------------

        partner_window = Window.partitionBy("partner2Code")

        partner_product = (
            partner_product
            .withColumn(
                "partner_total_import_value_usd",
                spark_sum(
                    "partner_product_import_value_usd"
                ).over(partner_window)
            )
        )

        # --------------------------------------------------
        # 6. PARTNER SHARE OF INDIA'S TOTAL IMPORTS
        # --------------------------------------------------

        total_import_value = (
            source
            .agg(
                spark_sum("trade_value_usd").alias("total")
            )
            .first()["total"]
        )

        if total_import_value is None or total_import_value <= 0:
            raise ValueError(
                "Invalid total import value in source"
            )

        partner_product = (
            partner_product
            .withColumn(
                "partner_import_share_pct",
                spark_round(
                    (
                        col("partner_total_import_value_usd")
                        / lit(total_import_value)
                    ) * 100,
                    6
                )
            )
        )

        # --------------------------------------------------
        # 7. PRODUCT SHARE WITHIN EACH PARTNER
        # --------------------------------------------------

        partner_product = (
            partner_product
            .withColumn(
                "product_share_within_partner_pct",
                spark_round(
                    (
                        col("partner_product_import_value_usd")
                        / col("partner_total_import_value_usd")
                    ) * 100,
                    6
                )
            )
        )

        # --------------------------------------------------
        # 8. RANK PARTNERS BY TOTAL IMPORT VALUE
        # --------------------------------------------------

        partner_summary = (
            partner_product
            .select(
                "partner2Code",
                "partner_total_import_value_usd"
            )
            .dropDuplicates(["partner2Code"])
        )

        partner_rank_window = Window.orderBy(
            desc("partner_total_import_value_usd"),
            asc("partner2Code")
        )

        partner_summary = (
            partner_summary
            .withColumn(
                "partner_rank",
                row_number().over(partner_rank_window)
            )
        )

        partner_product = partner_product.join(
            partner_summary.select(
                "partner2Code",
                "partner_rank"
            ),
            on="partner2Code",
            how="inner"
        )

        # --------------------------------------------------
        # 9. RANK PRODUCTS WITHIN EACH PARTNER
        # --------------------------------------------------

        product_rank_window = (
            Window
            .partitionBy("partner2Code")
            .orderBy(
                desc("partner_product_import_value_usd"),
                asc("cmdCode")
            )
        )

        partner_product = (
            partner_product
            .withColumn(
                "product_rank_within_partner",
                row_number().over(product_rank_window)
            )
        )

        # --------------------------------------------------
        # 10. VALIDATE GOLD DATA
        # --------------------------------------------------

        print("\nPARTNER GOLD VALIDATION")

        gold_count = partner_product.count()

        print("Gold records:", gold_count)

        if gold_count != aggregated_count:
            raise ValueError(
                "Gold record count changed after ranking"
            )

        duplicate_keys = (
            partner_product
            .groupBy("partner2Code", "cmdCode")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        print("Duplicate partner-product keys:", duplicate_keys)

        if duplicate_keys > 0:
            raise ValueError(
                "Duplicate partner-product keys detected"
            )

        required_columns = [
            "reporterISO",
            "reporterDesc",
            "partner2Code",
            "partner2Desc",
            "cmdCode",
            "commodity_description",
            "partner_product_import_value_usd",
            "partner_total_import_value_usd",
            "partner_import_share_pct",
            "product_share_within_partner_pct",
            "partner_rank",
            "product_rank_within_partner"
        ]

        for column_name in required_columns:

            null_count = (
                partner_product
                .filter(col(column_name).isNull())
                .count()
            )

            if null_count > 0:
                raise ValueError(
                    f"Null values in {column_name}: {null_count}"
                )

        print("Required columns validation passed!")

        invalid_values = (
            partner_product
            .filter(
                (col("partner_product_import_value_usd") < 0)
                | (col("partner_total_import_value_usd") <= 0)
                | (col("partner_import_share_pct") < 0)
                | (col("product_share_within_partner_pct") < 0)
                | (col("partner_rank") < 1)
                | (col("product_rank_within_partner") < 1)
            )
            .count()
        )

        if invalid_values > 0:
            raise ValueError(
                "Invalid partner analytics values detected"
            )

        print("Numeric validation passed!")

        # --------------------------------------------------
        # 11. RECONCILE SOURCE AND GOLD TOTALS
        # --------------------------------------------------

        print("\nIMPORT VALUE RECONCILIATION")

        source_total = total_import_value

        gold_total = (
            partner_product
            .agg(
                spark_sum(
                    "partner_product_import_value_usd"
                ).alias("total")
            )
            .first()["total"]
        )

        difference = source_total - gold_total

        print("Source import total:", source_total)
        print("Gold import total:", gold_total)
        print("Difference:", difference)

        if abs(difference) > 0.01:
            raise ValueError(
                "Import value reconciliation failed"
            )

        print("Import value reconciliation passed!")

        # --------------------------------------------------
        # 12. VALIDATE PARTNER SHARES
        # --------------------------------------------------

        print("\nPARTNER SHARE VALIDATION")

        partner_shares = (
            partner_product
            .select(
                "partner2Code",
                "partner_total_import_value_usd",
                "partner_import_share_pct"
            )
            .dropDuplicates(["partner2Code"])
        )

        total_partner_share = (
            partner_shares
            .agg(
                spark_sum(
                    "partner_import_share_pct"
                ).alias("total_share")
            )
            .first()["total_share"]
        )

        print("Total partner share:", total_partner_share)

        if not (99.99 <= total_partner_share <= 100.01):
            raise ValueError(
                "Partner shares do not reconcile to 100%"
            )

        print("Partner share validation passed!")

        # --------------------------------------------------
        # 13. VALIDATE UNIQUE PARTNER COUNT
        # --------------------------------------------------

        partner_count = (
            partner_product
            .select("partner2Code")
            .distinct()
            .count()
        )

        print("\nUnique import partners:", partner_count)

        if partner_count != 214:
            raise ValueError(
                f"Expected 214 import partners, found {partner_count}"
            )

        # --------------------------------------------------
        # 14. DISPLAY TOP 10 IMPORT PARTNERS
        # --------------------------------------------------

        print("\nTOP 10 IMPORT PARTNERS")

        (
            partner_summary
            .join(
                partner_shares.select(
                    "partner2Code",
                    "partner_import_share_pct"
                ),
                on="partner2Code",
                how="inner"
            )
            .orderBy("partner_rank")
            .show(10, truncate=False)
        )

        # --------------------------------------------------
        # 15. DISPLAY TOP PRODUCTS BY PARTNER
        # --------------------------------------------------

        print("\nTOP PRODUCTS BY TOP 5 IMPORT PARTNERS")

        (
            partner_product
            .filter(col("partner_rank") <= 5)
            .filter(col("product_rank_within_partner") <= 3)
            .select(
                "partner_rank",
                "partner2Desc",
                "product_rank_within_partner",
                "cmdCode",
                "commodity_description",
                "partner_product_import_value_usd",
                "product_share_within_partner_pct"
            )
            .orderBy(
                "partner_rank",
                "product_rank_within_partner"
            )
            .show(15, truncate=False)
        )

        # --------------------------------------------------
        # 16. WRITE GOLD DELTA TABLE
        # --------------------------------------------------

        print("\nWRITING PARTNER GOLD DELTA TABLE")

        if output_path.exists():
            raise FileExistsError(
                f"Gold output already exists: {output_path}. "
                "Inspect it before deciding whether to overwrite."
            )

        (
            partner_product
            .write
            .format("delta")
            .mode("errorifexists")
            .save(str(output_path))
        )

        print("\nGOLD TABLE CREATED:")
        print(output_path)

        # --------------------------------------------------
        # 17. READ BACK AND VALIDATE
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

        saved_partner_count = (
            saved
            .select("partner2Code")
            .distinct()
            .count()
        )

        print("Unique import partners:", saved_partner_count)

        if saved_partner_count != 214:
            raise ValueError(
                "Unexpected import partner count in saved Gold table"
            )

        print("\n" + "=" * 70)
        print("IMPORT PARTNER ANALYTICS CREATED SUCCESSFULLY!")
        print("ALL PARTNER GOLD VALIDATIONS PASSED!")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
