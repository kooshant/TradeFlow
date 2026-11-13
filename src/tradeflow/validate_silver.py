
import pandas as pd
from pathlib import Path
import math


SILVER_FILE = Path(
    "data/processed/silver/un_comtrade/india_trade_observations.csv"
)

REQUIRED_COLUMNS = [
    "source_id",
    "source_record_id",
    "flow",
    "reporter_code",
    "partner_code",
    "hs_code",
    "period",
    "trade_value_usd",
    "is_reported",
    "is_aggregate",
]


def validate_silver():

    print("Starting Silver Layer Validation")

    if not SILVER_FILE.exists():
        raise FileNotFoundError(
            f"Silver file not found: {SILVER_FILE}"
        )

    df = pd.read_csv(SILVER_FILE)

    errors = []

    # 1. Check whether dataset is empty.
    if df.empty:
        errors.append("Silver dataset is empty")

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

    if errors:
        print("\nVALIDATION FAILED")

        for error in errors:
            print("-", error)

        raise RuntimeError("Silver validation failed")

    # 3. Check required fields for null values.
    for column in REQUIRED_COLUMNS:

        if df[column].isnull().any():
            errors.append(
                f"Null values found in {column}"
            )

    # 4. Validate source.
    if not df["source_id"].eq("un_comtrade").all():
        errors.append("Unexpected source_id found")

    # 5. Validate trade flows.
    valid_flows = {"imports", "exports"}

    if not df["flow"].isin(valid_flows).all():
        errors.append("Invalid trade flow found")

    # 6. Validate India reporter code.
    if not pd.to_numeric(
        df["reporter_code"], errors="coerce"
    ).eq(699).all():
        errors.append("Unexpected reporter code")

    # 7. Validate trade values.
    trade_values = pd.to_numeric(
        df["trade_value_usd"], errors="coerce"
    )

    if trade_values.isnull().any():
        errors.append("Invalid trade value found")

    elif not trade_values.map(
        lambda value: math.isfinite(value) and value >= 0
    ).all():
        errors.append(
            "Non-finite or negative trade value found"
        )

    # 8. Validate period.
    periods = pd.to_numeric(
        df["period"], errors="coerce"
    )

    if periods.isnull().any():
        errors.append("Invalid period found")

    elif not periods.between(1900, 2100).all():
        errors.append("Period outside expected year range")

    # 9. Check source record duplicates.
    if df["source_record_id"].duplicated().any():
        errors.append("Duplicate source records found")

    # 10. Check provenance flags.
    for column in ["is_reported", "is_aggregate"]:

        if not df[column].isin([True, False]).all():
            errors.append(
                f"Invalid boolean values in {column}"
            )

    if errors:

        print("\nVALIDATION FAILED")

        for error in errors:
            print("-", error)

        raise RuntimeError("Silver validation failed")

    print("\nVALIDATION PASSED")
    print("Total records:", len(df))
    print("Total columns:", len(df.columns))
    print("Unique source records:", df["source_record_id"].nunique())
    print("Trade flows:", ", ".join(sorted(df["flow"].unique())))
    print("Silver file:", SILVER_FILE)


if __name__ == "__main__":
    validate_silver()
