
import json

from pathlib import Path
from datetime import datetime, timezone


# Project configuration
PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "un_comtrade"
)

PERIOD = 2023

IMPORTS_FILE = RAW_DATA_DIR / (
    "india_imports_2023_full_chapterwise.json"
)

OUTPUT_FILE = RAW_DATA_DIR / (
    "india_trade_2023_full.json"
)


def load_json(file_path):

    with file_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def main():

    print("Starting Unified Trade Consolidation")

    # Locate authenticated full exports file.
    export_files = sorted(
        RAW_DATA_DIR.glob(
            f"india_exports_{PERIOD}_full_*.json"
        )
    )

    if not export_files:
        raise FileNotFoundError(
            "Authenticated full exports file not found"
        )

    exports_file = export_files[-1]

    if not IMPORTS_FILE.exists():
        raise FileNotFoundError(
            "Consolidated imports file not found"
        )

    print("\nLoading exports:", exports_file.name)

    exports_payload = load_json(exports_file)

    print("Loading imports:", IMPORTS_FILE.name)

    imports_payload = load_json(IMPORTS_FILE)

    exports = exports_payload.get("data", [])
    imports = imports_payload.get("data", [])

    # Validate source counts.
    if exports_payload.get("count") != len(exports):
        raise ValueError("Exports API count mismatch")

    if imports_payload.get("count") != len(imports):
        raise ValueError("Imports consolidated count mismatch")

    # Validate flow identities.
    for record in exports:

        if record.get("flowCode") != "X":
            raise ValueError("Unexpected flow in exports dataset")

    for record in imports:

        if record.get("flowCode") != "M":
            raise ValueError("Unexpected flow in imports dataset")

    # Combine records without modifying source fields.
    all_records = exports + imports

    expected_total = len(exports) + len(imports)

    if len(all_records) != expected_total:
        raise ValueError("Unified record count mismatch")

    # Create unified raw dataset.
    unified_data = {
        "count": len(all_records),
        "data": all_records,
        "tradeflow_metadata": {
            "source": "UN Comtrade",
            "reporter_code": 699,
            "reporter_name": "India",
            "partner_code": 0,
            "partner_name": "World",
            "period": PERIOD,
            "classification": "H6",
            "extraction_timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            "total_records": len(all_records),
            "flow_counts": {
                "exports": len(exports),
                "imports": len(imports)
            },
            "source_files": {
                "exports": exports_file.name,
                "imports": IMPORTS_FILE.name
            },
            "extraction_methods": {
                "exports": "authenticated_full_api_request",
                "imports": "authenticated_chapter_wise_api_requests"
            }
        }
    }

    # Save safely.
    temp_file = OUTPUT_FILE.with_suffix(".json.tmp")

    with temp_file.open("w", encoding="utf-8") as file:

        json.dump(
            unified_data,
            file,
            ensure_ascii=False
        )

    temp_file.replace(OUTPUT_FILE)

    print("\nUnified trade consolidation completed!")

    print("Exports records:", len(exports))
    print("Imports records:", len(imports))
    print("Total records:", len(all_records))

    print("Output:", OUTPUT_FILE)


if __name__ == "__main__":
    main()
