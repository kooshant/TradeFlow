
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    lit
)

from delta import configure_spark_with_delta_pip


def main():
    # --------------------------------------------------
    # 1. PROJECT PATHS
    # --------------------------------------------------

    root = Path(__file__).resolve().parents[2]

    source_path = (
        root
        / "data"
        / "processed"
        / "delta"
        / "enriched"
        / "national_trade_2023"
    )

    output_path = (
        root
        / "data"
        / "processed"
        / "delta"
        / "gold"
        / "national_trade_flow_summary_2023"
    )

    # --------------------------------------------------
    # 2. SPARK SESSION
    # --------------------------------------------------

    builder = (
        SparkSession.builder
        .appName("TradeFlow-Gold-National-Summary")
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
        print("\nTRADEFLOW GOLD: NATIONAL TRADE SUMMARY")
        print("=" * 65)

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
            raise ValueError("National source table is empty")

        # --------------------------------------------------
        # 4. BUILD NATIONAL TRADE SUMMARY
        # --------------------------------------------------

        gold = (
            source
            .groupBy(
                "refYear",
                "classificationCode",
                "trade_flow",
                "trade_value_basis"
            )
            .agg(
                spark_sum("trade_value_usd").alias(
                    "total_trade_value_usd"
                ),
                count("*").alias(
                    "record_count"
                ),
                countDistinct("cmdCode").alias(
                    "unique_commodity_count"
                ),
                countDistinct("hs_chapter").alias(
                    "unique_hs_chapter_count"
                )
            )
        )

        # --------------------------------------------------
        # 5. ADD REPORTER METADATA
        # --------------------------------------------------

        gold = (
            gold
            .withColumn(
                "reporterISO",
                lit("IND")
            )
            .withColumn(
                "reporterDesc",
                lit("India")
            )
        )

        # --------------------------------------------------
        # 6. GOLD DATA VALIDATION
        # --------------------------------------------------

        print("\nGOLD SUMMARY PREVIEW")

        gold.orderBy("trade_flow").show(
            truncate=False
        )

        gold_count = gold.count()

        print("Gold summary rows:", gold_count)

        if gold_count != 2:
            raise ValueError(
                f"Expected 2 summary rows, got {gold_count}"
            )

        required_columns = [
            "reporterISO",
            "reporterDesc",
            "refYear",
            "classificationCode",
            "trade_flow",
            "trade_value_basis",
            "total_trade_value_usd"
        ]

        for column_name in required_columns:
            null_count = (
                gold
                .filter(col(column_name).isNull())
                .count()
            )

            if null_count > 0:
                raise ValueError(
                    f"Null values found in {column_name}"
                )

        if gold.filter(
            col("total_trade_value_usd") < 0
        ).count() > 0:
            raise ValueError(
                "Negative trade values found in Gold summary"
            )

        # --------------------------------------------------
        # 7. SOURCE-TO-GOLD RECONCILIATION
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
            gold
            .select(
                "trade_flow",
                col("total_trade_value_usd").alias(
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

        reconciliation.show(
            truncate=False
        )

        failed = reconciliation.filter(
            col("source_total").isNull()
            | col("gold_total").isNull()
            | (col("difference") > 0.01)
            | (col("difference") < -0.01)
        ).count()

        if failed > 0:
            raise ValueError(
                "Gold trade value reconciliation failed"
            )

        print("Trade value reconciliation passed!")

        # --------------------------------------------------
        # 8. WRITE GOLD DELTA TABLE
        # --------------------------------------------------

        print("\nWRITING GOLD DELTA TABLE")

        (
            gold.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(str(output_path))
        )

        print("\nGOLD TABLE CREATED:")
        print(output_path)

        # --------------------------------------------------
        # 9. READ BACK AND VALIDATE SAVED TABLE
        # --------------------------------------------------

        saved = (
            spark.read
            .format("delta")
            .load(str(output_path))
        )

        saved_count = saved.count()

        print("\nSAVED GOLD TABLE VALIDATION")
        print("Saved records:", saved_count)

        saved.orderBy("trade_flow").show(
            truncate=False
        )

        if saved_count != 2:
            raise ValueError(
                "Saved Gold table record count mismatch"
            )

        metadata_errors = saved.filter(
            (col("reporterISO") != "IND")
            | (col("reporterDesc") != "India")
        ).count()

        if metadata_errors > 0:
            raise ValueError(
                "Reporter metadata validation failed"
            )

        print("\n" + "=" * 65)
        print("NATIONAL TRADE SUMMARY CREATED SUCCESSFULLY!")
        print("ALL GOLD VALIDATIONS PASSED!")
        print("=" * 65)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
