
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

MARTS_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "analytics"
)


EXPECTED_MARTS = {
    "national_trade_kpis": 2,
    "top_export_products": 10,
    "top_import_products": 10,
    "import_partner_concentration": 214,
    "top_partner_product_mix": 25,
}


def load_mart(name):
    path = MARTS_DIR / name

    assert path.exists(), f"Missing analytics mart: {name}"

    return pd.read_parquet(path)


def test_all_analytics_marts_exist_and_have_expected_counts():

    for name, expected_count in EXPECTED_MARTS.items():

        df = load_mart(name)

        assert len(df) == expected_count, (
            f"{name}: expected {expected_count}, got {len(df)}"
        )


def test_national_kpi_values_are_valid():

    df = load_mart("national_trade_kpis")

    required_columns = {
        "trade_flow",
        "total_trade_value_usd",
        "trade_value_basis",
    }

    assert required_columns.issubset(df.columns)

    assert set(df["trade_flow"]) == {"Export", "Import"}

    assert df["total_trade_value_usd"].notna().all()

    assert (df["total_trade_value_usd"] > 0).all()


def test_product_analytics_are_valid():

    for name in [
        "top_export_products",
        "top_import_products",
    ]:

        df = load_mart(name)

        assert df["cmdCode"].notna().all()

        assert df["product_trade_value_usd"].notna().all()

        assert (df["product_trade_value_usd"] > 0).all()

        assert df["product_rank"].is_unique

        assert set(df["product_rank"]) == set(range(1, 11))


def test_import_partner_analytics_are_valid():

    df = load_mart("import_partner_concentration")

    assert df["partner2Code"].is_unique

    assert df["partner_import_share_pct"].notna().all()

    assert (df["partner_import_share_pct"] >= 0).all()

    assert (df["partner_import_share_pct"] <= 100).all()

    assert df["partner_rank"].is_unique

    assert set(df["partner_rank"]) == set(range(1, 215))


def test_partner_product_mix_is_valid():

    df = load_mart("top_partner_product_mix")

    assert df["partner2Desc"].notna().all()

    assert df["commodity_description"].notna().all()

    assert df["partner_product_import_value_usd"].notna().all()

    assert (df["partner_product_import_value_usd"] > 0).all()
