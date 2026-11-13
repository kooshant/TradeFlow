
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    root = Path(__file__).resolve().parents[2]

    output_dir = root / "data" / "processed" / "spark"

    national_path = output_dir / "national_trade_2023"
    partner_path = output_dir / "import_partner_breakdown_2023"

    spark = (
        SparkSession.builder
        .appName("TradeFlow-Curated-Validation")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\nStarting Independent Parquet Validation")

        # 1. Read saved Parquet datasets
        national = spark.read.parquet(str(national_path))
        partners = spark.read.parquet(str(partner_path))

        # 2. Validate record counts
        national_count = national.count()
        partner_count = partners.count()

        print("\n1. RECORD COUNT VALIDATION")
        print("National records:", national_count)
        print("Partner records:", partner_count)

        assert national_count == 10264
        assert partner_count == 99159

        # 3. Validate partner2Code rules
        print("\n2. PARTNER DIMENSION VALIDATION")

        invalid_national = national.filter(
            F.col("partner2Code") != 0
        ).count()

        invalid_partners = partners.filter(
            (F.col("partner2Code") == 0)
            | (F.col("trade_flow") != "Import")
        ).count()

        print("Invalid national records:", invalid_national)
        print("Invalid partner records:", invalid_partners)

        assert invalid_national == 0
        assert invalid_partners == 0

        # 4. Validate required columns
        required_columns = [
            "period",
            "reporterCode",
            "trade_flow",
            "cmdCode",
            "partnerCode",
            "partner2Code",
            "trade_value_usd",
            "hs_chapter"
        ]

        print("\n3. SCHEMA VALIDATION")

        for column in required_columns:
            assert column in national.columns, (
                f"Missing national column: {column}"
            )

            assert column in partners.columns, (
                f"Missing partner column: {column}"
            )

        print("Required columns: PASSED")

        # 5. Validate duplicate keys
        key_columns = [
            "period",
            "reporterCode",
            "trade_flow",
            "cmdCode",
            "partnerCode",
            "partner2Code"
        ]

        print("\n4. DUPLICATE KEY VALIDATION")

        for name, dataset in [
            ("National", national),
            ("Partners", partners)
        ]:
            duplicates = (
                dataset.groupBy(*key_columns)
                .count()
                .filter(F.col("count") > 1)
                .count()
            )

            print(f"{name} duplicate keys:", duplicates)
            assert duplicates == 0

        # 6. Validate trade flow distribution
        print("\n5. NATIONAL TRADE FLOW DISTRIBUTION")

        national.groupBy("trade_flow").count().show()

        print("\n6. PARTNER TRADE FLOW DISTRIBUTION")

        partners.groupBy("trade_flow").count().show()

        # 7. Validate trade values
        print("\n7. TRADE VALUE VALIDATION")

        for name, dataset in [
            ("National", national),
            ("Partners", partners)
        ]:
            invalid_values = dataset.filter(
                F.col("trade_value_usd").isNull()
                | F.isnan("trade_value_usd")
                | (F.col("trade_value_usd") < 0)
            ).count()

            print(f"{name} invalid trade values:", invalid_values)
            assert invalid_values == 0

        # 8. Display national trade totals
        print("\n8. NATIONAL TRADE VALUE SUMMARY")

        (
            national.groupBy("trade_flow")
            .agg(
                F.count("*").alias("records"),
                F.sum("trade_value_usd").alias("total_value_usd")
            )
            .orderBy("trade_flow")
            .show(truncate=False)
        )

        print("\nINDEPENDENT PARQUET VALIDATION PASSED!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
