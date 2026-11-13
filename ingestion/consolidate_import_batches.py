
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
EXPECTED_RECORDS = 152437

OUTPUT_FILE = RAW_DATA_DIR / (
    f"india_imports_{PERIOD}_full_chapterwise.json"
)


def main():

    print("Starting Imports Batch Consolidation")

    chapter_files = sorted(
        RAW_DATA_DIR.glob(
            f"india_imports_{PERIOD}_chapter_*.json"
        )
    )

    if len(chapter_files) != 97:
        raise ValueError(
            f"Expected 97 chapter files, found {len(chapter_files)}"
        )

    all_records = []
    processed_chapters = []

    for file_path in chapter_files:

        chapter = file_path.stem.split("_")[-1]

        print(f"Reading chapter {chapter}...")

        with file_path.open(
            "r",
            encoding="utf-8"
        ) as file:

            payload = json.load(file)

        records = payload.get("data", [])

        if payload.get("count") != len(records):
            raise ValueError(
                f"API count mismatch in {file_path.name}"
            )

        all_records.extend(records)

        processed_chapters.append({
            "chapter": chapter,
            "record_count": len(records),
            "source_file": file_path.name
        })

    # Verify consolidated record count.
    total_records = len(all_records)

    print("\nConsolidation summary")
    print("Chapter files:", len(chapter_files))
    print("Total records:", total_records)

    if total_records != EXPECTED_RECORDS:
        raise ValueError(
            f"Expected {EXPECTED_RECORDS} records, "
            f"found {total_records}"
        )

    # Build consolidated raw dataset.
    consolidated_data = {
        "count": total_records,
        "data": all_records,
        "tradeflow_extraction": {
            "source": "UN Comtrade",
            "reporter_code": 699,
            "partner_code": 0,
            "period": PERIOD,
            "flow_code": "M",
            "classification": "H6",
            "extraction_method": "chapter_wise",
            "chapter_files_processed": len(chapter_files),
            "empty_chapters": ["77", "98"],
            "created_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "chapters": processed_chapters
        }
    }

    # Save safely using a temporary file.
    temp_file = OUTPUT_FILE.with_suffix(".json.tmp")

    with temp_file.open(
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            consolidated_data,
            file,
            ensure_ascii=False
        )

    temp_file.replace(OUTPUT_FILE)

    print("\nImports consolidation completed!")
    print("Total records:", total_records)
    print("Output file:", OUTPUT_FILE)


if __name__ == "__main__":
    main()
