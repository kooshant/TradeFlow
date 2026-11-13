
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    countDistinct,
    min as spark_min,
    max as spark_max
)

from delta import configure_spark_with_delta_pip


def check(condition, message):
    if not condition:
        raise ValueError(message)

    print(f"PASSED: {message}")


def check_required_columns(df, required_columns, table_name):
    missing = sorted(set(required_columns) - set(df.columns))

    check(
        len(missing) == 0,
        f"{table_name}: required columns exist"
    )

    if missing:
        raise ValueError(
            f"{table_name}: missing columns: {missing}"
        )


def check_nulls(df, columns, table_name):
    for column_name in columns:
        null_count = (
            df.filter(col(column_name).isNull()).count()
        )

        check(
            null_count == 0,
            f"{table_name}: {column_name} has no nulls"
        )


def check_no_duplicates(df, keys, table_name):
    duplicate_count = (
        df.groupBy(*keys)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    check(
        duplicate_count == 0,
        f"{table_name}: no duplicate business keys"
    )


def check_reconciliation(
    source_df,
    gold_df,
    source_value_column,
    gold_value_column,
    table_name,
    tolerance=0.01
):
    source_totals = {
        row["trade_flow"]: row["total"]
        for row in (
            source_df
            .groupBy("trade_flow")
            .agg(
                spark_sum(source_value_column).alias("total")
            )
            .collect()
        )
    }

    gold_totals = {
        row["trade_flow"]: row["total"]
        for row in (
            gold_df
            .groupBy("trade_flow")
            .agg(
                spark_sum(gold_value_column).alias("total")
            )
            .collect()
        )
    }

    check(
        set(source_totals) == set(gold_totals),
        f"{table_name}: source and Gold trade flows match"
    )

    for flow in sorted(source_totals):

        difference = (
            source_totals[flow] - gold_totals[flow]
        )

        print(f"\n{table_name} - {flow}")
        print("Source total:", source_totals[flow])
        print("Gold total:", gold_totals[flow])
        print("Difference:", difference)

        check(
            abs(difference) <= tolerance,
            f"{table_name}: {flow} trade value reconciles"
        )


def check_non_negative(df, columns, table_name):
    for column_name in columns:

        invalid_count = (
            df.filter(col(column_name) < 0).count()
        )

        check(
            invalid_count == 0,
            f"{table_name}: {column_name} is non-negative"
        )


def main():

    # --------------------------------------------------
    # 1. PROJECT PATHS
    # --------------------------------------------------

    root = Path(__file__).resolve().parents[2]

    enriched_path = (
        root / "data" / "processed" / "delta" / "enriched"
    )

    gold_path = (
        root / "data" / "processed" / "delta" / "gold"
    )

    national_source_path = (
        enriched_path / "national_trade_2023"
    )

    partner_source_path = (
        enriched_path / "import_partner_breakdown_2023"
    )

    national_gold_path = (
        gold_path / "national_trade_flow_summary_2023"
    )

    product_gold_path = (
        gold_path / "product_trade_analytics_2023"
    )

    partner_gold_path = (
        gold_path / "import_partner_analytics_2023"
    )

    # --------------------------------------------------
    # 2. SPARK SESSION
    # --------------------------------------------------

    builder = (
        SparkSession.builder
        .appName("TradeFlow-Gold-Layer-Validation")
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

        print("\n" + "=" * 75)
        print("TRADEFLOW: INDEPENDENT GOLD LAYER VALIDATION")
        print("=" * 75)

        # --------------------------------------------------
        # 3. CHECK ALL TABLE PATHS
        # --------------------------------------------------

        required_paths = [
            national_source_path,
            partner_source_path,
            national_gold_path,
            product_gold_path,
            partner_gold_path
        ]

        for path in required_paths:
            check(
                path.exists(),
                f"Table path exists: {path.name}"
            )

        # --------------------------------------------------
        # 4. READ DELTA TABLES
        # --------------------------------------------------

        national_source = (
            spark.read.format("delta")
            .load(str(national_source_path))
        )

        partner_source = (
            spark.read.format("delta")
            .load(str(partner_source_path))
        )

        national_gold = (
            spark.read.format("delta")
            .load(str(national_gold_path))
        )

        product_gold = (
            spark.read.format("delta")
            .load(str(product_gold_path))
        )

        partner_gold = (
            spark.read.format("delta")
            .load(str(partner_gold_path))
        )

        # --------------------------------------------------
        # 5. RECORD COUNT VALIDATION
        # --------------------------------------------------

        print("\n" + "-" * 75)
        print("RECORD COUNT VALIDATION")
        print("-" * 75)

        counts = {
            "National source": national_source.count(),
            "Partner source": partner_source.count(),
            "National Gold": national_gold.count(),
            "Product Gold": product_gold.count(),
            "Partner Gold": partner_gold.count()
        }

        for name, value in counts.items():
            print(f"{name}: {value}")

            check(
                value > 0,
                f"{name} is not empty"
            )

        check(
            counts["National Gold"] == 2,
            "National Gold contains two trade-flow records"
        )

        check(
            counts["Product Gold"] == 10264,
            "Product Gold record count matches expected count"
        )

        check(
            counts["Partner Gold"] == 99159,
            "Partner Gold record count matches expected count"
        )

        # --------------------------------------------------
        # 6. SCHEMA VALIDATION
        # --------------------------------------------------

        print("\n" + "-" * 75)
        print("SCHEMA VALIDATION")
        print("-" * 75)

        check_required_columns(
            national_gold,
            [
                "refYear",
                "classificationCode",
                "trade_flow",
                "trade_value_basis",
                "total_trade_value_usd",
                "record_count",
                "unique_commodity_count",
                "unique_hs_chapter_count",
                "reporterISO",
                "reporterDesc"
            ],
            "National Gold"
        )

        check_required_columns(
            product_gold,
            [
                "refYear",
                "classificationCode",
                "trade_flow",
                "trade_value_basis",
                "cmdCode",
                "commodity_description",
                "hs_chapter",
                "product_trade_value_usd",
                "source_record_count",
                "reporterISO",
                "reporterDesc",
                "total_flow_trade_value_usd",
                "trade_value_share_pct",
                "product_rank"
            ],
            "Product Gold"
        )

        check_required_columns(
            partner_gold,
            [
                "partner2Code",
                "refYear",
                "classificationCode",
                "trade_flow",
                "trade_value_basis",
                "cmdCode",
                "commodity_description",
                "hs_chapter",
                "partner_product_import_value_usd",
                "source_record_count",
                "partner2Desc",
                "reporterISO",
                "reporterDesc",
                "partner_total_import_value_usd",
                "partner_import_share_pct",
                "product_share_within_partner_pct",
                "partner_rank",
                "product_rank_within_partner"
            ],
            "Partner Gold"
        )

        # --------------------------------------------------
        # 7. NULL VALIDATION
        # --------------------------------------------------

        print("\n" + "-" * 75)
        print("NULL VALIDATION")
        print("-" * 75)

        check_nulls(
            national_gold,
            [
                "refYear",
                "classificationCode",
                "trade_flow",
                "trade_value_basis",
                "total_trade_value_usd",
                "record_count",
                "unique_commodity_count",
                "unique_hs_chapter_count",
                "reporterISO",
                "reporterDesc"
            ],
            "National Gold"
        )

        check_nulls(
            product_gold,
            [
                "refYear",
                "classificationCode",
                "trade_flow",
                "trade_value_basis",
                "cmdCode",
                "commodity_description",
                "hs_chapter",
                "product_trade_value_usd",
                "source_record_count",
                "reporterISO",
                "reporterDesc",
                "total_flow_trade_value_usd",
                "trade_value_share_pct",
                "product_rank"
            ],
            "Product Gold"
        )

        check_nulls(
            partner_gold,
            [
                "partner2Code",
                "refYear",
                "classificationCode",
                "trade_flow",
                "trade_value_basis",
                "cmdCode",
                "commodity_description",
                "hs_chapter",
                "partner_product_import_value_usd",
                "source_record_count",
                "partner2Desc",
                "reporterISO",
                "reporterDesc",
                "partner_total_import_value_usd",
                "partner_import_share_pct",
                "product_share_within_partner_pct",
                "partner_rank",
                "product_rank_within_partner"
            ],
            "Partner Gold"
        )

        # --------------------------------------------------
        # 8. BUSINESS KEY VALIDATION
        # --------------------------------------------------

        print("\n" + "-" * 75)
        print("DUPLICATE BUSINESS KEY VALIDATION")
        print("-" * 75)

        check_no_duplicates(
            national_gold,
            ["refYear", "trade_flow"],
            "National Gold"
        )

        check_no_duplicates(
            product_gold,
            ["refYear", "trade_flow", "cmdCode"],
            "Product Gold"
        )

        check_no_duplicates(
            partner_gold,
            ["refYear", "partner2Code", "cmdCode"],
            "Partner Gold"
        )

        # --------------------------------------------------
        # 9. REPORTER AND TRADE FLOW VALIDATION
        # --------------------------------------------------

        print("\n" + "-" * 75)
        print("REPORTER AND TRADE FLOW VALIDATION")
        print("-" * 75)

        for name, df in [
            ("National Gold", national_gold),
            ("Product Gold", product_gold),
            ("Partner Gold", partner_gold)
        ]:

            check(
                df.filter(
                    (col("reporterISO") != "IND")
                    | (col("reporterDesc") != "India")
                ).count() == 0,
                f"{name}: reporter is India"
            )

            check(
                df.filter(col("refYear") != 2023).count() == 0,
                f"{name}: reference year is 2023"
            )

        check(
            set(
                row["trade_flow"]
                for row in national_gold.select("trade_flow").distinct().collect()
            ) == {"Export", "Import"},
            "National Gold contains Export and Import"
        )

        check(
            product_gold.filter(
                ~col("trade_flow").isin("Export", "Import")
            ).count() == 0,
            "Product Gold contains only valid trade flows"
        )

        check(
            partner_gold.filter(
                col("trade_flow") != "Import"
            ).count() == 0,
            "Partner Gold contains only Import records"
        )

        # --------------------------------------------------
        # 10. NUMERIC VALIDATION
        # --------------------------------------------------

        print("\n" + "-" * 75)
        print("NUMERIC VALIDATION")
        print("-" * 75)

        check_non_negative(
            national_gold,
            [
                "total_trade_value_usd",
                "record_count",
                "unique_commodity_count",
                "unique_hs_chapter_count"
            ],
            "National Gold"
        )

        check_non_negative(
            product_gold,
            [
                "product_trade_value_usd",
                "source_record_count",
                "total_flow_trade_value_usd",
                "trade_value_share_pct"
            ],
            "Product Gold"
        )

        check_non_negative(
            partner_gold,
            [
                "partner_product_import_value_usd",
                "source_record_count",
                "partner_total_import_value_usd",
                "partner_import_share_pct",
                "product_share_within_partner_pct"
            ],
            "Partner Gold"
        )

        # --------------------------------------------------
        # 11. SOURCE-TO-GOLD RECONCILIATION
        # --------------------------------------------------

        print("\n" + "-" * 75)
        print("SOURCE-TO-GOLD RECONCILIATION")
        print("-" * 75)

        check_reconciliation(
            national_source,
            national_gold,
            "trade_value_usd",
            "total_trade_value_usd",
            "National Gold"
        )

        check_reconciliation(
            national_source,
            product_gold,
            "trade_value_usd",
            "product_trade_value_usd",
            "Product Gold"
        )

        check_reconciliation(
            partner_source,
            partner_gold,
            "trade_value_usd",
            "partner_product_import_value_usd",
            "Partner Gold"
        )

        # --------------------------------------------------
        # 12. NATIONAL SUMMARY METADATA VALIDATION
        # --------------------------------------------------

        print("\n" + "-" * 75)
        print("NATIONAL SUMMARY VALIDATION")
        print("-" * 75)

        national_metrics = (
            national_source
            .groupBy("trade_flow")
            .agg(
                count("*").alias("source_records"),
                countDistinct("cmdCode").alias("commodities"),
                countDistinct("hs_chapter").alias("chapters")
            )
        )

        national_comparison = (
            national_gold
            .join(
                national_metrics,
                on="trade_flow",
                how="inner"
            )
        )

        check(
            national_comparison.count() == 2,
            "National summary matches both source trade flows"
        )

        check(
            national_comparison.filter(
                (col("record_count") != col("source_records"))
                | (col("unique_commodity_count") != col("commodities"))
                | (col("unique_hs_chapter_count") != col("chapters"))
            ).count() == 0,
            "National summary source counts and distinct counts match"
        )

        # --------------------------------------------------
        # 13. PRODUCT SHARE VALIDATION
        # --------------------------------------------------

        print("\n" + "-" * 75)
        print("PRODUCT SHARE VALIDATION")
        print("-" * 75)

        product_shares = (
            product_gold
            .groupBy("trade_flow")
            .agg(
                spark_sum("trade_value_share_pct").alias("total_share")
            )
        )

        product_shares.show(truncate=False)

        check(
            product_shares.filter(
                (col("total_share") < 99.99)
                | (col("total_share") > 100.01)
            ).count() == 0,
            "Product shares total approximately 100% per trade flow"
        )

        check(
            product_gold.filter(
                col("product_rank") < 1
            ).count() == 0,
            "Product ranks are positive"
        )

        # --------------------------------------------------
        # 14. PARTNER SHARE VALIDATION
        # --------------------------------------------------

        print("\n" + "-" * 75)
        print("PARTNER SHARE VALIDATION")
        print("-" * 75)

        partner_totals = (
            partner_gold
            .select(
                "partner2Code",
                "partner_total_import_value_usd",
                "partner_import_share_pct",
                "partner_rank"
            )
            .dropDuplicates(["partner2Code"])
        )

        check(
            partner_totals.count() == 214,
            "Partner Gold contains 214 unique partners"
        )

        partner_share_total = (
            partner_totals
            .agg(
                spark_sum("partner_import_share_pct").alias("total")
            )
            .first()["total"]
        )

        print("Total partner share:", partner_share_total)

        check(
            99.99 <= partner_share_total <= 100.01,
            "Partner shares total approximately 100%"
        )

        partner_product_shares = (
            partner_gold
            .groupBy("partner2Code")
            .agg(
                spark_sum(
                    "product_share_within_partner_pct"
                ).alias("total_share")
            )
        )

        check(
            partner_product_shares.filter(
                (col("total_share") < 99.99)
                | (col("total_share") > 100.01)
            ).count() == 0,
            "Product shares total approximately 100% within each partner"
        )

        # Validate partner totals against their product values.

        calculated_partner_totals = (
            partner_gold
            .groupBy("partner2Code")
            .agg(
                spark_sum(
                    "partner_product_import_value_usd"
                ).alias("calculated_total"),
                spark_max(
                    "partner_total_import_value_usd"
                ).alias("stored_total")
            )
        )

        check(
            calculated_partner_totals.filter(
                col("calculated_total") - col("stored_total") > 0.01
            ).count() == 0,
            "Partner totals are not below calculated product totals"
        )

        check(
            calculated_partner_totals.filter(
                col("stored_total") - col("calculated_total") > 0.01
            ).count() == 0,
            "Partner totals match calculated product totals"
        )

        # --------------------------------------------------
        # 15. PARTNER RANK VALIDATION
        # --------------------------------------------------

        print("\n" + "-" * 75)
        print("PARTNER RANK VALIDATION")
        print("-" * 75)

        rank_stats = (
            partner_totals
            .agg(
                countDistinct("partner_rank").alias("unique_ranks"),
                spark_min("partner_rank").alias("minimum_rank"),
                spark_max("partner_rank").alias("maximum_rank")
            )
            .first()
        )

        check(
            rank_stats["unique_ranks"] == 214
            and rank_stats["minimum_rank"] == 1
            and rank_stats["maximum_rank"] == 214,
            "Partner ranks are unique and continuous from 1 to 214"
        )

        check(
            partner_gold.filter(
                col("product_rank_within_partner") < 1
            ).count() == 0,
            "Product ranks within partners are positive"
        )

        # --------------------------------------------------
        # 16. FINAL SUMMARY
        # --------------------------------------------------

        print("\n" + "=" * 75)
        print("TRADEFLOW GOLD LAYER VALIDATION COMPLETED")
        print("=" * 75)

        print("\nValidated tables:")
        print("1. National Trade Flow Summary")
        print("2. Product Trade Analytics")
        print("3. Import Partner Analytics")

        print("\nValidated records:")
        print("National Gold:", counts["National Gold"])
        print("Product Gold:", counts["Product Gold"])
        print("Partner Gold:", counts["Partner Gold"])

        print("\nALL GOLD LAYER VALIDATIONS PASSED!")

        print("=" * 75)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
