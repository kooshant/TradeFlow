
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]

GOLD = ROOT / "data" / "processed" / "delta" / "gold"

OUTPUT = ROOT / "data" / "processed" / "analytics"

NATIONAL_PATH = GOLD / "national_trade_flow_summary_2023"
PRODUCT_PATH = GOLD / "product_trade_analytics_2023"
PARTNER_PATH = GOLD / "import_partner_analytics_2023"


# --------------------------------------------------
# SPARK
# --------------------------------------------------

builder = (
    SparkSession.builder
    .appName("TradeFlow-Analytics-Marts")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(
    builder
).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# --------------------------------------------------
# LOAD GOLD TABLES
# --------------------------------------------------

print("\nStarting TradeFlow Analytics Mart Generation")

national = spark.read.format("delta").load(str(NATIONAL_PATH))
products = spark.read.format("delta").load(str(PRODUCT_PATH))
partners = spark.read.format("delta").load(str(PARTNER_PATH))

OUTPUT.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# HELPER: SAVE PARQUET MART
# --------------------------------------------------

def save_mart(df, name):

    output_path = OUTPUT / name

    df.write \
        .mode("overwrite") \
        .parquet(str(output_path))

    saved = spark.read.parquet(str(output_path))

    print(
        f"{name}: {saved.count():,} records saved and verified"
    )


# --------------------------------------------------
# MART 1: NATIONAL TRADE KPIs
# --------------------------------------------------

national_kpis = national.select(
    "refYear",
    "reporterDesc",
    "trade_flow",
    "trade_value_basis",
    "total_trade_value_usd",
    "record_count",
    "unique_commodity_count",
    "unique_hs_chapter_count"
)

save_mart(national_kpis, "national_trade_kpis")


# --------------------------------------------------
# MART 2: TOP EXPORT PRODUCTS
# --------------------------------------------------

top_exports = (
    products
    .filter(F.col("trade_flow") == "Export")
    .filter(F.col("product_rank") <= 10)
    .select(
        "refYear",
        "cmdCode",
        "commodity_description",
        "product_trade_value_usd",
        "trade_value_share_pct",
        "product_rank"
    )
)

save_mart(top_exports, "top_export_products")


# --------------------------------------------------
# MART 3: TOP IMPORT PRODUCTS
# --------------------------------------------------

top_imports = (
    products
    .filter(F.col("trade_flow") == "Import")
    .filter(F.col("product_rank") <= 10)
    .select(
        "refYear",
        "cmdCode",
        "commodity_description",
        "product_trade_value_usd",
        "trade_value_share_pct",
        "product_rank"
    )
)

save_mart(top_imports, "top_import_products")


# --------------------------------------------------
# MART 4: IMPORT PARTNER CONCENTRATION
# --------------------------------------------------

partner_totals = (
    partners
    .select(
        "partner2Code",
        "partner2Desc",
        "refYear",
        "partner_total_import_value_usd",
        "partner_import_share_pct",
        "partner_rank"
    )
    .dropDuplicates(["partner2Code"])
)

partner_concentration = partner_totals.withColumn(
    "concentration_group",
    F.when(F.col("partner_rank") <= 5, "Top 5")
    .when(F.col("partner_rank") <= 10, "Rank 6-10")
    .otherwise("Other partners")
)

save_mart(
    partner_concentration,
    "import_partner_concentration"
)


# --------------------------------------------------
# MART 5: TOP PARTNER PRODUCT MIX
# --------------------------------------------------

top_partner_product_mix = (
    partners
    .filter(F.col("partner_rank") <= 5)
    .filter(F.col("product_rank_within_partner") <= 5)
    .select(
        "refYear",
        "partner2Code",
        "partner2Desc",
        "partner_rank",
        "cmdCode",
        "commodity_description",
        "partner_product_import_value_usd",
        "product_share_within_partner_pct",
        "product_rank_within_partner"
    )
)

save_mart(
    top_partner_product_mix,
    "top_partner_product_mix"
)


# --------------------------------------------------
# FINAL VALIDATION
# --------------------------------------------------

print("\n" + "=" * 70)
print("ANALYTICS MART VALIDATION")
print("=" * 70)

expected_counts = {
    "national_trade_kpis": 2,
    "top_export_products": 10,
    "top_import_products": 10,
    "import_partner_concentration": 214,
    "top_partner_product_mix": 25
}

for name, expected in expected_counts.items():

    path = OUTPUT / name
    df = spark.read.parquet(str(path))

    actual = df.count()

    if actual != expected:
        raise ValueError(
            f"{name}: expected {expected}, got {actual}"
        )

    print(f"PASSED: {name} = {actual} records")


print("\nALL FIVE ANALYTICS MARTS CREATED SUCCESSFULLY!")

spark.stop()
