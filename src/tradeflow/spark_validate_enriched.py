
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


def main():
    root = Path(__file__).resolve().parents[2]
    delta_root = root / "data" / "processed" / "delta"
    enriched_root = delta_root / "enriched"

    builder = (
        SparkSession.builder
        .appName("TradeFlow-Enriched-Independent-Validation")
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
        print("\nTRADEFLOW ENRICHED DATA VALIDATION")
        print("=" * 65)

        paths = {
            "source_national": delta_root / "national_trade_2023",
            "source_partners": delta_root / "import_partner_breakdown_2023",
            "commodity_dimension": enriched_root / "dim_commodity_hs2022",
            "partner_dimension": enriched_root / "dim_partner_area",
            "national_enriched": enriched_root / "national_trade_2023",
            "partners_enriched": enriched_root / "import_partner_breakdown_2023"
        }

        # Check that every path is a valid Delta table.
        for name, path in paths.items():
            if not DeltaTable.isDeltaTable(spark, str(path)):
                raise ValueError(f"Not a valid Delta table: {name}")

        tables = {
            name: spark.read.format("delta").load(str(path))
            for name, path in paths.items()
        }

        source_national = tables["source_national"]
        source_partners = tables["source_partners"]

        commodity_dim = tables["commodity_dimension"]
        partner_dim = tables["partner_dimension"]

        national = tables["national_enriched"]
        partners = tables["partners_enriched"]

        # --------------------------------------------------
        # 1. RECORD COUNT VALIDATION
        # --------------------------------------------------

        print("\n1. RECORD COUNTS")

        expected_counts = {
            "commodity_dimension": 6939,
            "partner_dimension": 310,
            "national_enriched": source_national.count(),
            "partners_enriched": source_partners.count()
        }

        for name, expected in expected_counts.items():
            actual = tables[name].count()
            print(f"{name}: {actual} (Expected: {expected})")

            if actual != expected:
                raise ValueError(f"Record count mismatch: {name}")

        # --------------------------------------------------
        # 2. DIMENSION KEY VALIDATION
        # --------------------------------------------------

        print("\n2. DIMENSION KEY VALIDATION")

        commodity_duplicate_keys = (
            commodity_dim.groupBy("commodity_code")
            .agg(count("*").alias("records"))
            .filter(col("records") > 1)
            .count()
        )

        partner_duplicate_keys = (
            partner_dim.groupBy("partner_code")
            .agg(count("*").alias("records"))
            .filter(col("records") > 1)
            .count()
        )

        print("Duplicate commodity keys:", commodity_duplicate_keys)
        print("Duplicate partner keys:", partner_duplicate_keys)

        if commodity_duplicate_keys != 0 or partner_duplicate_keys != 0:
            raise ValueError("Dimension keys are not unique")

        # --------------------------------------------------
        # 3. DESCRIPTION VALIDATION
        # --------------------------------------------------

        print("\n3. DESCRIPTION VALIDATION")

        checks = {
            "National missing commodity descriptions": (
                national.filter(
                    col("commodity_description").isNull()
                ).count()
            ),
            "National missing partner descriptions": (
                national.filter(
                    col("partner_description").isNull()
                ).count()
            ),
            "Partner missing commodity descriptions": (
                partners.filter(
                    col("commodity_description").isNull()
                ).count()
            ),
            "Partner missing partner descriptions": (
                partners.filter(
                    col("partner_description").isNull()
                ).count()
            )
        }

        for name, value in checks.items():
            print(f"{name}: {value}")

        if any(value != 0 for value in checks.values()):
            raise ValueError("Missing enriched descriptions found")

        # --------------------------------------------------
        # 4. ROW GRAIN VALIDATION
        # --------------------------------------------------

        print("\n4. ROW GRAIN VALIDATION")

        key_columns = ["trade_flow", "cmdCode", "partner2Code"]

        for name, df in [
            ("National", national),
            ("Import partners", partners)
        ]:
            duplicate_count = (
                df.groupBy(*key_columns)
                .agg(count("*").alias("records"))
                .filter(col("records") > 1)
                .count()
            )

            print(f"{name} duplicate business keys: {duplicate_count}")

            if duplicate_count != 0:
                raise ValueError(f"Duplicate business keys: {name}")

        # --------------------------------------------------
        # 5. TRADE VALUE RECONCILIATION
        # --------------------------------------------------

        print("\n5. TRADE VALUE RECONCILIATION")

        value_checks = [
            (
                "National",
                source_national,
                national
            ),
            (
                "Import partners",
                source_partners,
                partners
            )
        ]

        for name, source, enriched in value_checks:
            source_total = source.agg(
                {"trade_value_usd": "sum"}
            ).first()[0]

            enriched_total = enriched.agg(
                {"trade_value_usd": "sum"}
            ).first()[0]

            difference = abs(source_total - enriched_total)

            print(f"\n{name}")
            print("Source total:", source_total)
            print("Enriched total:", enriched_total)
            print("Difference:", difference)

            if difference > 0.01:
                raise ValueError(f"Trade value changed: {name}")

        # --------------------------------------------------
        # 6. BUSINESS DATA PREVIEW
        # --------------------------------------------------

        print("\n6. ENRICHED DATA PREVIEW")

        national.select(
            "trade_flow",
            "cmdCode",
            "commodity_description",
            "trade_value_usd"
        ).show(10, truncate=False)

        partners.select(
            "partner2Code",
            "partner_description",
            "partner_iso_alpha3",
            "cmdCode",
            "commodity_description",
            "trade_value_usd"
        ).show(10, truncate=False)

        print("\n" + "=" * 65)
        print("INDEPENDENT ENRICHED DATA VALIDATION PASSED!")
        print("=" * 65)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
