
import csv
import json

from pathlib import Path
from collections import Counter


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
    / "un_comtrade"
)

CSV_FILE = SILVER_DIR / "india_trade_2023_silver.csv"

METADATA_FILE = (
    SILVER_DIR / "india_trade_2023_silver_metadata.json"
)

EXPECTED_RECORDS = 165485
EXPECTED_COLUMNS = 53

EXPECTED_FLOWS = {
    "Export": 13048,
    "Import": 152437,
}


def main():

    print("Starting Full Silver Validation\n")

    errors = []

    with METADATA_FILE.open("r", encoding="utf-8") as file:
        metadata = json.load(file)

    with CSV_FILE.open(
        "r",
        encoding="utf-8",
        newline=""
    ) as file:

        reader = csv.DictReader(file)

        columns = reader.fieldnames

        flow_counts = Counter()
        hs_levels = Counter()
        seen_records = set()

        total_records = 0
        duplicate_records = 0
        invalid_records = 0

        for row in reader:

            total_records += 1

            flow = row["trade_flow"]
            flow_counts[flow] += 1

            hs_levels[row["hs_level"]] += 1

            # Exact duplicate detection
            record_signature = json.dumps(
                row,
                sort_keys=True
            )

            if record_signature in seen_records:
                duplicate_records += 1

            seen_records.add(record_signature)

            # Derived field validation
            expected_flow = (
                "Export"
                if row["flowCode"] == "X"
                else "Import"
                if row["flowCode"] == "M"
                else None
            )

            if expected_flow != flow:
                invalid_records += 1

            if int(row["hs_code_length"]) != len(row["cmdCode"]):
                invalid_records += 1

            if row["is_annual_period"] != "True":
                invalid_records += 1

            expected_level = {
                2: "chapter",
                4: "heading",
                6: "subheading",
            }.get(len(row["cmdCode"]), "nonstandard")

            if row["hs_level"] != expected_level:
                invalid_records += 1

            if float(row["trade_value_usd"]) != float(row["primaryValue"]):
                invalid_records += 1

    print("Total records:", total_records)
    print("Total columns:", len(columns))

    print("\nFlow distribution:")

    for flow, expected in EXPECTED_FLOWS.items():

        actual = flow_counts.get(flow, 0)

        print(f"{flow}: {actual} (Expected: {expected})")

        if actual != expected:
            errors.append(f"Incorrect {flow} count")

    print("\nHS hierarchy distribution:")

    for level, count in sorted(hs_levels.items()):
        print(f"{level}: {count}")

    print("\nDuplicate records:", duplicate_records)
    print("Invalid derived records:", invalid_records)

    print("\nMetadata validation:")

    if metadata.get("total_records") != total_records:
        errors.append("Metadata record count mismatch")

    if metadata.get("total_columns") != len(columns):
        errors.append("Metadata column count mismatch")

    if metadata.get("source_columns") != 47:
        errors.append("Unexpected original column count")

    if metadata.get("period") != 2023:
        errors.append("Incorrect metadata period")

    if total_records != EXPECTED_RECORDS:
        errors.append("Unexpected total records")

    if len(columns) != EXPECTED_COLUMNS:
        errors.append("Unexpected column count")

    if duplicate_records:
        errors.append("Duplicate records detected")

    if invalid_records:
        errors.append("Invalid derived records detected")

    if errors:

        print("\nVALIDATION FAILED")

        for error in errors:
            print("-", error)

        raise SystemExit(1)

    print("\nVALIDATION PASSED")
    print("Full Silver dataset passed independent validation!")


if __name__ == "__main__":
    main()
