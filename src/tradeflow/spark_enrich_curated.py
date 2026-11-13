
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

from delta import configure_spark_with_delta_pip


def main():
    root = Path(__file__).resolve().parents[2]

    delta_root = root / "data" / "processed" / "delta"
    reference_root = root / "data" / "reference" / "un_comtrade"

    output_root = delta_root / "enriched"

    output_paths = {
        "commodity_dimension": output_root / "dim_commodity_hs2022",
        "partner_dimension": output_root / "dim_partner_area",
        "national_trade": output_root / "national_trade_2023",
        "import_partner": output_root / "import_partner_breakdown_2023"
    }

    # Protect existing output data from accidental overwrite.
    existing = [
        name for name, path in output_paths.items()
        if path.exists()
    ]

    if existing:
        raise FileExistsError(
            "Enrichment output already exists: "
            + ", ".join(existing)
            + ". Inspect existing data before deciding whether to rebuild."
        )

    builder = (
        SparkSession.builder
        .appName("TradeFlow-Curated-Enrichment")
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
        print("\nTRADEFLOW CURATED DATA ENRICHMENT")
        print("=" * 65)

        # --------------------------------------------------
        # 1. READ EXISTING DELTA TABLES
        # --------------------------------------------------

        national = spark.read.format("delta").load(
            str(delta_root / "national_trade_2023")
        )

        partners = spark.read.format("delta").load(
            str(delta_root / "import_partner_breakdown_2023")
        )

        print("\nSOURCE RECORD COUNTS")
        print("National:", national.count())
        print("Import partners:", partners.count())

        # --------------------------------------------------
        # 2. LOAD OFFICIAL REFERENCE FILES
        # --------------------------------------------------

        commodity_ref = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(
                str(reference_root / "hs2022_commodity_reference.csv")
            )
        )

        partner_ref = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(
                str(reference_root / "partner_area_reference.csv")
            )
        )

        # Prepare commodity dimension.
        commodity_dim = commodity_ref.select(
            col("hs_code").alias("commodity_code"),
            col("cmd_desc").alias("commodity_description"),
            col("parent_hs_code"),
            col("is_leaf").alias("commodity_is_leaf"),
            col("aggregation_level").alias(
                "commodity_aggregation_level"
            ),
            col("standard_unit").alias("commodity_standard_unit")
        )

        # Prepare partner dimension.
        partner_dim = partner_ref.select(
            col("partner_code").cast("int").alias("partner_code"),
            col("partner_desc").alias("partner_description"),
            col("partner_iso_alpha2").alias("partner_iso_alpha2"),
            col("partner_iso_alpha3").alias("partner_iso_alpha3"),
            col("partner_note"),
            col("is_group").alias("partner_is_group")
        )

        print("\nREFERENCE COUNTS")
        print("Commodity dimension:", commodity_dim.count())
        print("Partner dimension:", partner_dim.count())

        # --------------------------------------------------
        # 3. ENRICH NATIONAL TRADE
        # --------------------------------------------------

        print("\nENRICHING NATIONAL TRADE")

        national_enriched = (
            national.alias("trade")
            .join(
                commodity_dim.alias("commodity"),
                col("trade.cmdCode") == col("commodity.commodity_code"),
                "left"
            )
            .join(
                partner_dim.alias("partner"),
                col("trade.partner2Code") == col("partner.partner_code"),
                "left"
            )
            .select(
                "trade.*",
                col("commodity.commodity_description"),
                col("commodity.parent_hs_code"),
                col("commodity.commodity_is_leaf"),
                col("commodity.commodity_aggregation_level"),
                col("commodity.commodity_standard_unit"),
                col("partner.partner_description"),
                col("partner.partner_iso_alpha2"),
                col("partner.partner_iso_alpha3"),
                col("partner.partner_note"),
                col("partner.partner_is_group")
            )
        )

        # --------------------------------------------------
        # 4. ENRICH IMPORT PARTNER BREAKDOWN
        # --------------------------------------------------

        print("\nENRICHING IMPORT PARTNER BREAKDOWN")

        import_partner_enriched = (
            partners.alias("trade")
            .join(
                commodity_dim.alias("commodity"),
                col("trade.cmdCode") == col("commodity.commodity_code"),
                "left"
            )
            .join(
                partner_dim.alias("partner"),
                col("trade.partner2Code") == col("partner.partner_code"),
                "left"
            )
            .select(
                "trade.*",
                col("commodity.commodity_description"),
                col("commodity.parent_hs_code"),
                col("commodity.commodity_is_leaf"),
                col("commodity.commodity_aggregation_level"),
                col("commodity.commodity_standard_unit"),
                col("partner.partner_description"),
                col("partner.partner_iso_alpha2"),
                col("partner.partner_iso_alpha3"),
                col("partner.partner_note"),
                col("partner.partner_is_group")
            )
        )

        # --------------------------------------------------
        # 5. VALIDATE ENRICHED DATA BEFORE WRITING
        # --------------------------------------------------

        print("\nENRICHMENT VALIDATION")

        national_count = national.count()
        national_enriched_count = national_enriched.count()

        partner_count = partners.count()
        partner_enriched_count = import_partner_enriched.count()

        print("National source:", national_count)
        print("National enriched:", national_enriched_count)

        print("Partner source:", partner_count)
        print("Partner enriched:", partner_enriched_count)

        national_missing_commodity = (
            national_enriched
            .filter(col("commodity_description").isNull())
            .count()
        )

        national_missing_partner = (
            national_enriched
            .filter(col("partner_description").isNull())
            .count()
        )

        partner_missing_commodity = (
            import_partner_enriched
            .filter(col("commodity_description").isNull())
            .count()
        )

        partner_missing_partner = (
            import_partner_enriched
            .filter(col("partner_description").isNull())
            .count()
        )

        print("\nMISSING ENRICHED DESCRIPTIONS")
        print("National commodities:", national_missing_commodity)
        print("National partners:", national_missing_partner)
        print("Partner commodities:", partner_missing_commodity)
        print("Partner names:", partner_missing_partner)

        # Verify trade values are unchanged.
        national_source_total = national.agg(
            spark_sum("trade_value_usd")
        ).first()[0]

        national_enriched_total = national_enriched.agg(
            spark_sum("trade_value_usd")
        ).first()[0]

        partner_source_total = partners.agg(
            spark_sum("trade_value_usd")
        ).first()[0]

        partner_enriched_total = import_partner_enriched.agg(
            spark_sum("trade_value_usd")
        ).first()[0]

        print("\nTRADE VALUE RECONCILIATION")
        print("National source:", national_source_total)
        print("National enriched:", national_enriched_total)
        print("Partner source:", partner_source_total)
        print("Partner enriched:", partner_enriched_total)

        validation_passed = (
            national_count == national_enriched_count
            and partner_count == partner_enriched_count
            and national_missing_commodity == 0
            and national_missing_partner == 0
            and partner_missing_commodity == 0
            and partner_missing_partner == 0
            and abs(
                national_source_total - national_enriched_total
            ) < 0.01
            and abs(
                partner_source_total - partner_enriched_total
            ) < 0.01
        )

        if not validation_passed:
            raise ValueError(
                "Enrichment validation failed. No output tables written."
            )

        print("\nENRICHMENT VALIDATION PASSED")

        # --------------------------------------------------
        # 6. WRITE FOUR NEW DELTA TABLES
        # --------------------------------------------------

        print("\nWRITING NEW DELTA TABLES")

        commodity_dim.write.format("delta").mode("errorifexists").save(
            str(output_paths["commodity_dimension"])
        )

        partner_dim.write.format("delta").mode("errorifexists").save(
            str(output_paths["partner_dimension"])
        )

        national_enriched.write.format("delta").mode(
            "errorifexists"
        ).save(
            str(output_paths["national_trade"])
        )

        import_partner_enriched.write.format("delta").mode(
            "errorifexists"
        ).save(
            str(output_paths["import_partner"])
        )

        print("\nCREATED DELTA TABLES")

        for name, path in output_paths.items():
            print(f"{name}: {path}")

        print("\nALL ENRICHED DELTA TABLES CREATED SUCCESSFULLY!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
