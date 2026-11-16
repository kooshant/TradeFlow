from pathlib import Path

import duckdb
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_ROOT / "sql" / "analytics"

SQL_FILES = {
    "national": "01_national_trade_overview.sql",
    "products": "02_product_trade_analytics.sql",
    "partners": "03_import_partner_concentration.sql",
    "product_mix": "04_top_partner_product_mix.sql",
}


@pytest.fixture(scope="module")
def sql_results():
    results = {}

    con = duckdb.connect()

    try:
        for name, filename in SQL_FILES.items():
            sql_path = SQL_DIR / filename

            assert sql_path.exists(), f"Missing SQL file: {sql_path}"

            query = sql_path.read_text()

            results[name] = con.execute(query).fetchdf()

    finally:
        con.close()

    return results


def test_all_sql_queries_execute(sql_results):
    assert len(sql_results) == 4

    for name, result in sql_results.items():
        assert not result.empty, f"{name} returned no rows"


def test_national_trade_overview(sql_results):
    df = sql_results["national"]

    required_columns = {
        "trade_year",
        "reporter_country",
        "reporter_name",
        "total_exports_usd",
        "total_imports_usd",
        "export_record_count",
        "import_record_count",
        "export_value_basis",
        "import_value_basis",
        "indicative_trade_balance_usd",
    }

    assert required_columns.issubset(df.columns)
    assert len(df) == 1

    row = df.iloc[0]

    assert row["trade_year"] == 2023
    assert row["reporter_country"] == "IND"
    assert row["total_exports_usd"] > 0
    assert row["total_imports_usd"] > 0

    assert row["export_value_basis"] == "FOB"
    assert row["import_value_basis"] == "CIF"

    assert row["export_record_count"] > 0
    assert row["import_record_count"] > 0

    expected_balance = (
        row["total_exports_usd"] - row["total_imports_usd"]
    )

    assert abs(
        row["indicative_trade_balance_usd"] - expected_balance
    ) < 0.01


def test_product_trade_analytics(sql_results):
    df = sql_results["products"]

    required_columns = {
        "trade_year",
        "reporter_country",
        "trade_flow",
        "hs_product_code",
        "commodity_description",
        "product_trade_value_usd",
        "trade_value_share_pct",
        "product_rank",
    }

    assert required_columns.issubset(df.columns)
    assert len(df) == 20

    assert df["reporter_country"].eq("IND").all()
    assert df["product_trade_value_usd"].notna().all()
    assert (df["product_trade_value_usd"] > 0).all()

    assert df["hs_product_code"].notna().all()
    assert df["commodity_description"].notna().all()

    for flow in ["Export", "Import"]:
        flow_df = df[
            df["trade_flow"].str.lower() == flow.lower()
        ]

        assert len(flow_df) == 10
        assert set(flow_df["product_rank"]) == set(range(1, 11))

        assert flow_df["trade_value_share_pct"].between(
            0, 100
        ).all()


def test_import_partner_concentration(sql_results):
    df = sql_results["partners"]

    required_columns = {
        "trade_year",
        "reporter_country",
        "partner_code",
        "partner_country",
        "total_import_value_usd",
        "import_share_pct",
        "partner_rank",
        "cumulative_import_share_pct",
    }

    assert required_columns.issubset(df.columns)
    assert len(df) == 214

    assert df["partner_code"].is_unique
    assert df["partner_rank"].is_unique

    assert set(df["partner_rank"]) == set(range(1, 215))

    assert df["partner_country"].notna().all()
    assert (df["total_import_value_usd"] > 0).all()

    assert df["import_share_pct"].between(0, 100).all()
    assert df["cumulative_import_share_pct"].between(
        0, 100
    ).all()

    assert df["cumulative_import_share_pct"].is_monotonic_increasing


def test_top_partner_product_mix(sql_results):
    df = sql_results["product_mix"]

    required_columns = {
        "trade_year",
        "reporter_country",
        "partner_code",
        "partner_country",
        "partner_rank",
        "hs_product_code",
        "commodity_description",
        "partner_product_import_value_usd",
        "product_share_within_partner_pct",
        "product_rank_within_partner",
    }

    assert required_columns.issubset(df.columns)
    assert len(df) == 25

    assert df["partner_country"].nunique() == 5
    assert set(df["partner_rank"]) == set(range(1, 6))

    for rank in range(1, 6):
        partner_df = df[df["partner_rank"] == rank]

        assert len(partner_df) == 5

        assert set(
            partner_df["product_rank_within_partner"]
        ) == set(range(1, 6))

    assert df["commodity_description"].notna().all()
    assert (df["partner_product_import_value_usd"] > 0).all()

    assert df["product_share_within_partner_pct"].between(
        0, 100
    ).all()


def test_top_partner_product_mix_matches_existing_mart():
    sql_path = SQL_DIR / SQL_FILES["product_mix"]
    query = sql_path.read_text().rstrip().rstrip(";")

    mart_path = (
        PROJECT_ROOT
        / "data"
        / "processed"
        / "analytics"
        / "top_partner_product_mix"
    )

    assert mart_path.exists(), "Existing analytics mart is missing"

    con = duckdb.connect()

    try:
        comparison = con.execute(
            f"""
            WITH sql_result AS (
                SELECT
                    trade_year,
                    partner_code,
                    partner_country,
                    partner_rank,
                    hs_product_code,
                    commodity_description,
                    partner_product_import_value_usd,
                    product_share_within_partner_pct,
                    product_rank_within_partner
                FROM ({query}) AS query_output
            ),

            mart_result AS (
                SELECT
                    refYear AS trade_year,
                    partner2Code AS partner_code,
                    partner2Desc AS partner_country,
                    partner_rank,
                    cmdCode AS hs_product_code,
                    commodity_description,
                    partner_product_import_value_usd,
                    product_share_within_partner_pct,
                    product_rank_within_partner
                FROM read_parquet(
                    '{mart_path.as_posix()}/*.parquet'
                )
            ),

            sql_only AS (
                SELECT * FROM sql_result
                EXCEPT ALL
                SELECT * FROM mart_result
            ),

            mart_only AS (
                SELECT * FROM mart_result
                EXCEPT ALL
                SELECT * FROM sql_result
            )

            SELECT
                (SELECT COUNT(*) FROM sql_result) AS sql_rows,
                (SELECT COUNT(*) FROM mart_result) AS mart_rows,
                (SELECT COUNT(*) FROM sql_only) AS sql_only_rows,
                (SELECT COUNT(*) FROM mart_only) AS mart_only_rows
            """
        ).fetchone()

        sql_rows, mart_rows, sql_only, mart_only = comparison

        assert sql_rows == 25
        assert mart_rows == 25
        assert sql_only == 0
        assert mart_only == 0

    finally:
        con.close()
