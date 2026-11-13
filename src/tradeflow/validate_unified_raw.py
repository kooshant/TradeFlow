
import json

from pathlib import Path
from collections import Counter


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "un_comtrade"
    / "india_trade_2023_full.json"
)

EXPECTED_TOTAL = 165485

EXPECTED_COUNTS = {
    "X": 13048,
    "M": 152437,
}


def main():

    print("Starting Unified Raw Data Validation\n")

    with INPUT_FILE.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    records = payload.get("data", [])

    metadata = payload.get("tradeflow_metadata", {})

    errors = []

    if payload.get("count") != len(records):
        errors.append("Top-level record count mismatch")

    if len(records) != EXPECTED_TOTAL:
        errors.append("Unexpected total record count")

    if metadata.get("reporter_code") != 699:
        errors.append("Invalid reporter metadata")

    if metadata.get("partner_code") != 0:
        errors.append("Invalid partner metadata")

    if metadata.get("period") != 2023:
        errors.append("Invalid period metadata")

    flow_counts = Counter(
        record.get("flowCode")
        for record in records
    )

    print("Total records:", len(records))
    print("\nFlow distribution:")

    for flow, expected in EXPECTED_COUNTS.items():

        actual = flow_counts.get(flow, 0)

        print(
            f"{flow}: {actual} "
            f"(Expected: {expected})"
        )

        if actual != expected:
            errors.append(
                f"Unexpected count for flow {flow}"
            )

    invalid_records = 0

    for index, record in enumerate(records):

        if record.get("reporterCode") != 699:
            invalid_records += 1

        elif record.get("partnerCode") != 0:
            invalid_records += 1

        elif str(record.get("period")) != "2023":
            invalid_records += 1

        elif record.get("flowCode") not in ["X", "M"]:
            invalid_records += 1

        elif record.get("classificationCode") != "H6":
            invalid_records += 1

    print("\nInvalid records:", invalid_records)

    if invalid_records:
        errors.append("Invalid records detected")

    if errors:

        print("\nVALIDATION FAILED")

        for error in errors:
            print("-", error)

        raise SystemExit(1)

    print("\nVALIDATION PASSED")
    print("Unified raw dataset is internally consistent!")


if __name__ == "__main__":
    main()
