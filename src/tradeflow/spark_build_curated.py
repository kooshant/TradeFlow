
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    root = Path(__file__).resolve().parents[2]

    input_path = (
        root / "data" / "processed" / "silver"
        / "un_comtrade" / "india_trade_2023_silver.csv"
    )

    output_dir = root / "data" / "processed" / "spark"

    spark = (
        SparkSession.builder
        .appName("TradeFlow-Curated-Datasets")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\nStarting TradeFlow Spark Curated Processing")

        # 1. Read validated Silver data
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(str(input_path))
        )

        # 2. Keep six-digit HS subheading records
        detail = (
            df.filter(
                (F.col("hs_code_length") == 6)
                & (F.col("hs_level") == "subheading")
            )
            .withColumn(
                "partner2Code",
                F.col("partner2Code").cast("int")
            )
            .withColumn(
                "hs_chapter",
                F.substring(F.col("cmdCode"), 1, 2)
            )
        )

        print("Detailed Silver records:", detail.count())

        # 3. National-level trade dataset
        national_trade = detail.filter(
            F.col("partner2Code") == 0
        )

        # 4. Import secondary-partner dataset
        import_partner_breakdown = detail.filter(
            (F.col("trade_flow") == "Import")
            & (F.col("partner2Code") != 0)
        )

        # 5. Validate expected record counts
        national_count = national_trade.count()
        partner_count = import_partner_breakdown.count()

        print("\nRecord counts:")
        print("National trade:", national_count)
        print("Import partner breakdown:", partner_count)

        assert national_count == 10264, (
            f"Unexpected national record count: {national_count}"
        )

        assert partner_count == 99159, (
            f"Unexpected partner record count: {partner_count}"
        )

        # 6. Check analytical totals
        national_import_value = (
            national_trade
            .filter(F.col("trade_flow") == "Import")
            .agg(F.sum("trade_value_usd"))
            .first()[0]
        )

        partner_import_value = (
            import_partner_breakdown
            .agg(F.sum("trade_value_usd"))
            .first()[0]
        )

        difference = abs(
            national_import_value - partner_import_value
        )

        print("\nImport reconciliation:")
        print("National import value:", national_import_value)
        print("Partner import value:", partner_import_value)
        print("Difference:", difference)

        # Allow a maximum USD 0.10 floating-point
        # reconciliation tolerance.
        RECONCILIATION_TOLERANCE = 0.10

        assert difference <= RECONCILIATION_TOLERANCE, (
            f"Import values differ by more than "
            f"Difference: ${difference:.6f}"
        )

        # 7. Check duplicate analytical keys
        key_columns = [
            "period",
            "reporterCode",
            "trade_flow",
            "cmdCode",
            "partnerCode",
            "partner2Code"
        ]

        for name, dataset in [
            ("National trade", national_trade),
            ("Import partner breakdown", import_partner_breakdown)
        ]:
            duplicate_count = (
                dataset.groupBy(*key_columns)
                .count()
                .filter(F.col("count") > 1)
                .count()
            )

            print(f"{name} duplicate keys:", duplicate_count)

            assert duplicate_count == 0, (
                f"Duplicate keys found in {name}"
            )

        # 8. Save curated Parquet datasets
        national_path = output_dir / "national_trade_2023"

        partner_path = (
            output_dir / "import_partner_breakdown_2023"
        )

        national_trade.write.mode("overwrite").parquet(
            str(national_path)
        )

        import_partner_breakdown.write.mode(
            "overwrite"
        ).parquet(str(partner_path))

        print("\nCURATED PROCESSING COMPLETED")
        print("National dataset:", national_path)
        print("Partner dataset:", partner_path)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
