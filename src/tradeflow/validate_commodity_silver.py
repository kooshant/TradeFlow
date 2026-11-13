
import math
import pandas as pd

from pathlib import Path


SILVER_FILE = Path(
    "data/processed/silver/un_comtrade/"
    "india_commodity_trade_observations.csv"
)

REQUIRED_COLUMNS = [
    "source_id",
    "source_file",
    "source_record_id",
    "run_id",
    "ingested_at",
    "flow",
    "reporter_code",
    "partner_code",
    "classification",
    "hs_code",
    "period",
    "trade_value_usd",
    "is_reported",
    "is_aggregate",
]


def validate_silver():

    print("Starting Commodity Silver Validation")

    if not SILVER_FILE.exists():
        raise FileNotFoundError(
            f"Silver file not found: {SILVER_FILE}"
        )

    df = pd.read_csv(SILVER_FILE)

    errors = []

    # 1. Check empty dataset.
    if df.empty:
        errors.append("Dataset is empty")

    # 2. Check required columns.
    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        errors.append(
            f"Missing columns: {missing_columns}"
        )

        raise RuntimeError(errors)

    # 3. Check required fields for null values.
    for column in REQUIRED_COLUMNS:

        if df[column].isnull().any():
            errors.append(
                f"Null values found in {column}"
            )

    # 4. Check source.
    if not df["source_id"].eq("un_comtrade").all():
        errors.append("Unexpected source ID")

    # 5. Check trade flows.
    expected_flows = {"exports", "imports"}

    actual_flows = set(df["flow"].unique())

    if actual_flows != expected_flows:
        errors.append(
            f"Unexpected trade flows: {actual_flows}"
        )

    # 6. Check India reporter code.
    reporter_codes = pd.to_numeric(
        df["reporter_code"],
        errors="coerce"
    )

    if not reporter_codes.eq(699).all():
        errors.append("Invalid reporter code")

    # 7. Check partner code.
    partner_codes = pd.to_numeric(
        df["partner_code"],
        errors="coerce"
    )

    if not partner_codes.eq(0).all():
        errors.append("Invalid partner code")

    # 8. Check classification.
    if not df["classification"].eq("H6").all():
        errors.append("Unexpected classification")

    # 9. Check commodity codes.
    hs_codes = df["hs_code"].astype(str)

    if not hs_codes.str.fullmatch(r"\d{2,6}").all():
        errors.append("Invalid HS commodity codes")

    # 10. Check trade values.
    trade_values = pd.to_numeric(
        df["trade_value_usd"],
        errors="coerce"
    )

    if trade_values.isnull().any():
        errors.append("Invalid trade values")

    elif not trade_values.map(
        lambda value: math.isfinite(value) and value >= 0
    ).all():
        errors.append("Negative or non-finite trade values")

    # 11. Check source record duplicates.
    if df["source_record_id"].duplicated().any():
        errors.append("Duplicate source records found")

    # 12. Check duplicates within each flow and HS code.
    if df.duplicated(
        subset=["flow", "classification", "hs_code"]
    ).any():
        errors.append("Duplicate commodity codes within flow")

    # 13. Check provenance flags.
    for column in ["is_reported", "is_aggregate"]:

        if not df[column].isin([True, False]).all():
            errors.append(
                f"Invalid provenance flag: {column}"
            )

    # Final result.
    if errors:

        print("\nVALIDATION FAILED")

        for error in errors:
            print("-", error)

        raise RuntimeError("Commodity Silver validation failed")

    print("\nVALIDATION PASSED")
    print("Total records:", len(df))
    print("Total columns:", len(df.columns))
    print("Unique source records:", df["source_record_id"].nunique())
    print("Unique commodity codes:", df["hs_code"].nunique())
    print("Exports:", (df["flow"] == "exports").sum())
    print("Imports:", (df["flow"] == "imports").sum())
    print("Classification:", df["classification"].unique())
    print("Output:", SILVER_FILE)


if __name__ == "__main__":
    validate_silver()
