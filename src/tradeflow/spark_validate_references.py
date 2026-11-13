
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

from delta import configure_spark_with_delta_pip


def main():
    root = Path(__file__).resolve().parents[2]

    delta_root = root / "data" / "processed" / "delta"
    reference_root = root / "data" / "reference" / "un_comtrade"

    builder = (
        SparkSession.builder
        .appName("TradeFlow-Reference-Validation")
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

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\nTRADEFLOW REFERENCE VALIDATION")
        print("=" * 60)

        # Load Delta datasets
        national = spark.read.format("delta").load(
            str(delta_root / "national_trade_2023")
        )

        partners = spark.read.format("delta").load(
            str(delta_root / "import_partner_breakdown_2023")
        )

        # Load reference CSVs as strings to preserve leading zeroes.
        commodity_ref = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(reference_root / "hs2022_commodity_reference.csv"))
        )

        partner_ref = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(reference_root / "partner_area_reference.csv"))
        )

        # -------------------------------------------------
        # 1. COMMODITY REFERENCE VALIDATION
        # -------------------------------------------------
        print("\n1. COMMODITY REFERENCE VALIDATION")

        commodity_ref = commodity_ref.withColumn(
            "hs_code",
            col("hs_code").cast("string")
        )

        trade_codes = (
            national
            .select(col("cmdCode").cast("string").alias("hs_code"))
            .distinct()
        )

        reference_codes = commodity_ref.select("hs_code").distinct()

        missing_commodity_codes = trade_codes.join(
            reference_codes,
            on="hs_code",
            how="left_anti"
        )

        matched_commodity_codes = trade_codes.join(
            reference_codes,
            on="hs_code",
            how="inner"
        )

        print("Unique trade HS codes:", trade_codes.count())
        print("Unique reference HS codes:", reference_codes.count())
        print("Matched HS codes:", matched_commodity_codes.count())
        print("Unmatched HS codes:", missing_commodity_codes.count())

        print("\nUnmatched commodity samples:")
        missing_commodity_codes.show(20, truncate=False)

        print("\nMatched commodity samples:")
        national.select("cmdCode").distinct().join(
            commodity_ref.select("hs_code", "cmd_desc"),
            col("cmdCode") == col("hs_code"),
            "inner"
        ).select(
            "cmdCode", "cmd_desc"
        ).show(10, truncate=False)

        # -------------------------------------------------
        # 2. PARTNER REFERENCE VALIDATION
        # -------------------------------------------------
        print("\n2. PARTNER REFERENCE VALIDATION")

        partner_ref = partner_ref.withColumn(
            "partner_code",
            col("partner_code").cast("int")
        )

        trade_partner_codes = (
            partners
            .select(col("partner2Code").alias("partner_code"))
            .distinct()
        )

        reference_partner_codes = (
            partner_ref
            .select("partner_code")
            .distinct()
        )

        matched_partners = trade_partner_codes.join(
            reference_partner_codes,
            on="partner_code",
            how="inner"
        )

        missing_partners = trade_partner_codes.join(
            reference_partner_codes,
            on="partner_code",
            how="left_anti"
        )

        print("Unique trade partner codes:", trade_partner_codes.count())
        print("Unique reference partner codes:", reference_partner_codes.count())
        print("Matched partner codes:", matched_partners.count())
        print("Unmatched partner codes:", missing_partners.count())

        print("\nUnmatched partner samples:")
        missing_partners.show(20, truncate=False)

        print("\nMatched partner samples:")
        partners.select("partner2Code").distinct().join(
            partner_ref.select(
                "partner_code",
                "partner_desc",
                "partner_iso_alpha3"
            ),
            col("partner2Code") == col("partner_code"),
            "inner"
        ).select(
            "partner2Code",
            "partner_desc",
            "partner_iso_alpha3"
        ).show(20, truncate=False)

        # -------------------------------------------------
        # 3. DUPLICATE REFERENCE CHECK
        # -------------------------------------------------
        print("\n3. DUPLICATE REFERENCE CHECK")

        commodity_duplicates = (
            commodity_ref.groupBy("hs_code")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        partner_duplicates = (
            partner_ref.groupBy("partner_code")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        print("Duplicate HS reference keys:", commodity_duplicates)
        print("Duplicate partner reference keys:", partner_duplicates)

        # -------------------------------------------------
        # 4. REFERENCE QUALITY CHECK
        # -------------------------------------------------
        print("\n4. REFERENCE DESCRIPTION QUALITY")

        print(
            "Missing commodity descriptions:",
            commodity_ref.filter(
                col("cmd_desc").isNull() |
                (col("cmd_desc") == "")
            ).count()
        )

        print(
            "Missing partner descriptions:",
            partner_ref.filter(
                col("partner_desc").isNull() |
                (col("partner_desc") == "")
            ).count()
        )

        # -------------------------------------------------
        # 5. FINAL RESULT
        # -------------------------------------------------
        print("\n5. FINAL VALIDATION RESULT")

        if (
            missing_commodity_codes.count() == 0
            and missing_partners.count() == 0
            and commodity_duplicates == 0
            and partner_duplicates == 0
        ):
            print("REFERENCE VALIDATION PASSED")
        else:
            print("REFERENCE VALIDATION NEEDS INVESTIGATION")

        print("\nREFERENCE VALIDATION COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
