
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "delta"
    / "gold"
)

NATIONAL_PATH = GOLD_PATH / "national_trade_flow_summary_2023"
PRODUCT_PATH = GOLD_PATH / "product_trade_analytics_2023"
PARTNER_PATH = GOLD_PATH / "import_partner_analytics_2023"


# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------

builder = (
    SparkSession.builder
    .appName("TradeFlow-Gold-Business-Analytics")
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


# --------------------------------------------------
# LOAD GOLD TABLES
# --------------------------------------------------

print("\n" + "=" * 75)
print("TRADEFLOW: GOLD BUSINESS ANALYTICS")
print("=" * 75)

national = spark.read.format("delta").load(str(NATIONAL_PATH))
products = spark.read.format("delta").load(str(PRODUCT_PATH))
partners = spark.read.format("delta").load(str(PARTNER_PATH))

print("\nGold tables loaded successfully.")

print(f"National records: {national.count():,}")
print(f"Product records: {products.count():,}")
print(f"Partner records: {partners.count():,}")


# --------------------------------------------------
# ANALYSIS 1: NATIONAL TRADE OVERVIEW
# --------------------------------------------------

print("\n" + "-" * 75)
print("1. INDIA NATIONAL TRADE OVERVIEW")
print("-" * 75)

national.select(
    "refYear",
    "trade_flow",
    "trade_value_basis",
    F.round("total_trade_value_usd", 2).alias(
        "total_trade_value_usd"
    ),
    "record_count",
    "unique_commodity_count"
).orderBy("trade_flow").show(truncate=False)


# --------------------------------------------------
# ANALYSIS 2: TOP 10 EXPORT PRODUCTS
# --------------------------------------------------

print("\n" + "-" * 75)
print("2. TOP 10 INDIA EXPORT PRODUCTS")
print("-" * 75)

top_exports = (
    products
    .filter(F.col("trade_flow") == "Export")
    .orderBy("product_rank")
    .select(
        "product_rank",
        "cmdCode",
        "commodity_description",
        F.round(
            "product_trade_value_usd", 2
        ).alias("export_value_usd"),
        F.round(
            "trade_value_share_pct", 4
        ).alias("export_share_pct")
    )
    .limit(10)
)

top_exports.show(10, truncate=60)


# --------------------------------------------------
# ANALYSIS 3: TOP 10 IMPORT PRODUCTS
# --------------------------------------------------

print("\n" + "-" * 75)
print("3. TOP 10 INDIA IMPORT PRODUCTS")
print("-" * 75)

top_imports = (
    products
    .filter(F.col("trade_flow") == "Import")
    .orderBy("product_rank")
    .select(
        "product_rank",
        "cmdCode",
        "commodity_description",
        F.round(
            "product_trade_value_usd", 2
        ).alias("import_value_usd"),
        F.round(
            "trade_value_share_pct", 4
        ).alias("import_share_pct")
    )
    .limit(10)
)

top_imports.show(10, truncate=60)


# --------------------------------------------------
# ANALYSIS 4: TOP 10 IMPORT PARTNERS
# --------------------------------------------------

print("\n" + "-" * 75)
print("4. TOP 10 INDIA IMPORT PARTNERS")
print("-" * 75)

partner_totals = (
    partners
    .select(
        "partner2Code",
        "partner2Desc",
        "partner_total_import_value_usd",
        "partner_import_share_pct",
        "partner_rank"
    )
    .dropDuplicates(["partner2Code"])
)

top_partners = (
    partner_totals
    .orderBy("partner_rank")
    .select(
        "partner_rank",
        "partner2Code",
        "partner2Desc",
        F.round(
            "partner_total_import_value_usd", 2
        ).alias("total_import_value_usd"),
        F.round(
            "partner_import_share_pct", 4
        ).alias("import_share_pct")
    )
    .limit(10)
)

top_partners.show(10, truncate=False)


# --------------------------------------------------
# ANALYSIS 5: TOP 5 PARTNER CONCENTRATION
# --------------------------------------------------

print("\n" + "-" * 75)
print("5. IMPORT PARTNER CONCENTRATION")
print("-" * 75)

top_5_share = (
    partner_totals
    .filter(F.col("partner_rank") <= 5)
    .agg(
        F.sum("partner_import_share_pct").alias(
            "top_5_partner_share_pct"
        )
    )
)

top_5_share.select(
    F.round(
        "top_5_partner_share_pct", 4
    ).alias("top_5_partner_import_share_pct")
).show(truncate=False)


# --------------------------------------------------
# ANALYSIS 6: TOP 5 PRODUCTS WITHIN TOP 5 PARTNERS
# --------------------------------------------------

print("\n" + "-" * 75)
print("6. TOP 5 PRODUCTS WITHIN INDIA'S TOP 5 IMPORT PARTNERS")
print("-" * 75)

top_partner_products = (
    partners
    .filter(
        (F.col("partner_rank") <= 5)
        & (F.col("product_rank_within_partner") <= 5)
    )
    .select(
        "partner_rank",
        "partner2Desc",
        "product_rank_within_partner",
        "cmdCode",
        "commodity_description",
        F.round(
            "partner_product_import_value_usd", 2
        ).alias("product_import_value_usd"),
        F.round(
            "product_share_within_partner_pct", 4
        ).alias("share_within_partner_pct")
    )
    .orderBy(
        "partner_rank",
        "product_rank_within_partner"
    )
)

top_partner_products.show(50, truncate=60)


# --------------------------------------------------
# COMPLETION
# --------------------------------------------------

print("\n" + "=" * 75)
print("TRADEFLOW BUSINESS ANALYTICS COMPLETED")
print("=" * 75)

spark.stop()
