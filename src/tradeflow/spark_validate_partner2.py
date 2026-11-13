
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    root = Path(__file__).resolve().parents[2]

    path = (
        root
        / "data"
        / "processed"
        / "silver"
        / "un_comtrade"
        / "india_trade_2023_silver.csv"
    )

    spark = (
        SparkSession.builder
        .appName("TradeFlow-Partner2-Validation")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(str(path))
        )

        detail = (
            df.filter(
                (F.col("hs_code_length") == 6)
                & F.col("partner2Code").isNotNull()
            )
            .withColumn(
                "partner2Code",
                F.col("partner2Code").cast("int")
            )
        )

        print("\n1. EXPORT WORLD VS AREAS NES VALIDATION")

        exports = detail.filter(
            F.col("trade_flow") == "Export"
        )

        world_exports = (
            exports.filter(F.col("partner2Code") == 0)
            .groupBy("cmdCode")
            .agg(F.sum("trade_value_usd").alias("world_value"))
        )

        areas_exports = (
            exports.filter(F.col("partner2Code") == 899)
            .groupBy("cmdCode")
            .agg(F.sum("trade_value_usd").alias("areas_value"))
        )

        export_comparison = world_exports.join(
            areas_exports,
            "cmdCode",
            "full"
        )

        export_mismatches = export_comparison.filter(
            F.abs(
                F.coalesce(F.col("world_value"), F.lit(0.0))
                - F.coalesce(F.col("areas_value"), F.lit(0.0))
            ) > 0.01
        )

        print(
            "Export HS codes checked:",
            export_comparison.count()
        )

        print(
            "Export mismatches:",
            export_mismatches.count()
        )

        print("\n2. IMPORT WORLD VS SECONDARY PARTNERS")

        imports = detail.filter(
            F.col("trade_flow") == "Import"
        )

        world_imports = (
            imports.filter(F.col("partner2Code") == 0)
            .groupBy("cmdCode")
            .agg(F.sum("trade_value_usd").alias("world_value"))
        )

        partner_imports = (
            imports.filter(F.col("partner2Code") != 0)
            .groupBy("cmdCode")
            .agg(F.sum("trade_value_usd").alias("partner_value"))
        )

        import_comparison = world_imports.join(
            partner_imports,
            "cmdCode",
            "full"
        )

        import_mismatches = import_comparison.filter(
            F.abs(
                F.coalesce(F.col("world_value"), F.lit(0.0))
                - F.coalesce(F.col("partner_value"), F.lit(0.0))
            ) > 0.01
        )

        print(
            "Import HS codes checked:",
            import_comparison.count()
        )

        print(
            "Import mismatches:",
            import_mismatches.count()
        )

        print("\nImport mismatch samples:")

        import_mismatches.show(20, truncate=False)

        print("\nPartner2 validation completed!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
