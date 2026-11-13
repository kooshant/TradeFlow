
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


RAW_DATA_DIR = Path("data/raw/un_comtrade")
SILVER_DATA_DIR = Path("data/processed/silver/un_comtrade")

FLOW_CODES = {
    "exports": "X",
    "imports": "M",
}


def get_latest_files():
    """Find the latest raw JSON file for each trade flow."""

    latest_files = {}

    for file_path in RAW_DATA_DIR.glob("*.json"):

        for flow_name in FLOW_CODES:

            if flow_name in file_path.name:

                current_file = latest_files.get(flow_name)

                if (
                    current_file is None
                    or file_path.name > current_file.name
                ):
                    latest_files[flow_name] = file_path

    return latest_files


def generate_record_hash(record):
    """Generate a consistent hash for identifying source records."""

    record_json = json.dumps(
        record,
        sort_keys=True,
        separators=(",", ":"),
        default=str
    )

    return hashlib.sha256(
        record_json.encode("utf-8")
    ).hexdigest()


def transform_file(file_path, flow_name, run_id, ingested_at):
    """Read actual API records and add standardised Silver fields."""

    print(f"\nProcessing: {file_path.name}")

    with file_path.open("r", encoding="utf-8") as file:
        raw_data = json.load(file)

    records = raw_data.get("data", [])

    if not records:
        raise ValueError(
            f"No records found in {file_path.name}"
        )

    transformed_records = []

    for record in records:

        # Preserve all original UN Comtrade fields.
        silver_record = record.copy()

        # Add standardised fields.
        silver_record.update({
            "source_id": "un_comtrade",
            "source_file": file_path.name,
            "source_record_id": generate_record_hash(record),
            "run_id": run_id,
            "ingested_at": ingested_at,
            "flow": flow_name,
            "reporter_code": record.get("reporterCode"),
            "partner_code": record.get("partnerCode"),
            "hs_code": record.get("cmdCode"),
            "period": record.get("period"),
            "trade_value_usd": record.get("primaryValue"),
            "is_reported": record.get("isReported"),
            "is_aggregate": record.get("isAggregate"),
        })

        transformed_records.append(silver_record)

    print("Records transformed:", len(transformed_records))

    return transformed_records


def main():

    print("Starting TradeFlow Silver Transformation")

    if not RAW_DATA_DIR.exists():
        raise FileNotFoundError(
            f"Raw data directory not found: {RAW_DATA_DIR}"
        )

    latest_files = get_latest_files()

    for flow_name in FLOW_CODES:

        if flow_name not in latest_files:
            raise FileNotFoundError(
                f"Latest {flow_name} data not found"
            )

    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S"
    )

    ingested_at = datetime.now(timezone.utc).isoformat()

    all_records = []

    for flow_name, file_path in latest_files.items():

        records = transform_file(
            file_path,
            flow_name,
            run_id,
            ingested_at
        )

        all_records.extend(records)

    # Convert actual API records into a structured DataFrame.
    silver_df = pd.DataFrame(all_records)

    # Check for duplicate source records.
    duplicate_count = silver_df["source_record_id"].duplicated().sum()

    if duplicate_count:
        raise ValueError(
            f"Duplicate source records detected: {duplicate_count}"
        )

    # Create Silver output directory.
    SILVER_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        SILVER_DATA_DIR / "india_trade_observations.csv"
    )

    # Save the transformed dataset.
    silver_df.to_csv(
        output_file,
        index=False
    )

    print("\nSilver transformation completed!")
    print("Total records:", len(silver_df))
    print("Columns:", len(silver_df.columns))
    print("Duplicate records:", duplicate_count)
    print("Output:", output_file)


if __name__ == "__main__":
    main()
