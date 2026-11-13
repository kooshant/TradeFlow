
import csv
import json

from pathlib import Path
from datetime import datetime, timezone


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "un_comtrade"
    / "india_trade_2023_full.json"
)

SILVER_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
    / "un_comtrade"
)

OUTPUT_FILE = SILVER_DIR / "india_trade_2023_silver.csv"

METADATA_FILE = (
    SILVER_DIR
    / "india_trade_2023_silver_metadata.json"
)


def classify_hs_code(code):

    length = len(str(code))

    if length == 2:
        return "chapter"

    elif length == 4:
        return "heading"

    elif length == 6:
        return "subheading"

    return "nonstandard"


def transform_record(record):

    transformed = record.copy()

    flow_code = record["flowCode"]

    if flow_code == "X":
        trade_flow = "Export"
        value_basis = "FOB"

    elif flow_code == "M":
        trade_flow = "Import"
        value_basis = "CIF"

    else:
        raise ValueError(
            f"Unexpected flow code: {flow_code}"
        )

    hs_code = str(record["cmdCode"])

    transformed["trade_flow"] = trade_flow

    transformed["trade_value_usd"] = record["primaryValue"]

    transformed["trade_value_basis"] = value_basis

    transformed["hs_code_length"] = len(hs_code)

    transformed["hs_level"] = classify_hs_code(hs_code)

    transformed["is_annual_period"] = (
        record["freqCode"] == "A"
    )

    return transformed


def main():

    print("Starting Full Silver Transformation\n")

    if not RAW_FILE.exists():
        raise FileNotFoundError(
            f"Raw dataset not found: {RAW_FILE}"
        )

    with RAW_FILE.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    records = payload["data"]

    if len(records) != payload["count"]:
        raise ValueError("Raw record count mismatch")

    SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    derived_columns = [
        "trade_flow",
        "trade_value_usd",
        "trade_value_basis",
        "hs_code_length",
        "hs_level",
        "is_annual_period",
    ]

    fieldnames = list(records[0].keys()) + derived_columns

    total_exports = 0
    total_imports = 0

    with OUTPUT_FILE.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()

        for record in records:

            transformed = transform_record(record)

            writer.writerow(transformed)

            if transformed["trade_flow"] == "Export":
                total_exports += 1

            else:
                total_imports += 1

    metadata = {
        "dataset": "TradeFlow India Trade Silver",
        "source": "UN Comtrade",
        "reporter": "India",
        "partner": "World",
        "period": 2023,
        "classification": "H6",
        "total_records": len(records),
        "total_columns": len(fieldnames),
        "source_columns": len(records[0]),
        "derived_columns": derived_columns,
        "flow_counts": {
            "exports": total_exports,
            "imports": total_imports
        },
        "transformation_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_file": str(RAW_FILE.name),
        "output_file": OUTPUT_FILE.name
    }

    with METADATA_FILE.open(
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            metadata,
            file,
            indent=4
        )

    print("Silver transformation completed!\n")

    print("Exports:", total_exports)
    print("Imports:", total_imports)
    print("Total records:", len(records))
    print("Original columns:", len(records[0]))
    print("Derived columns:", len(derived_columns))
    print("Total Silver columns:", len(fieldnames))

    print("\nSilver output:", OUTPUT_FILE)
    print("Metadata output:", METADATA_FILE)


if __name__ == "__main__":
    main()
