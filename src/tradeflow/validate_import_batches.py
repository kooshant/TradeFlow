
import json
import hashlib

from pathlib import Path


# Project configuration
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "un_comtrade"
)

PERIOD = 2023
REPORTER_CODE = 699
PARTNER_CODE = 0
EXPECTED_FLOW = "M"

EXPECTED_EMPTY_CHAPTERS = {"77", "98"}

EXPECTED_CHAPTERS = {
    f"{number:02d}"
    for number in range(1, 100)
}

EXPECTED_DATA_CHAPTERS = (
    EXPECTED_CHAPTERS - EXPECTED_EMPTY_CHAPTERS
)


def record_hash(record):

    record_json = json.dumps(
        record,
        sort_keys=True,
        ensure_ascii=False
    )

    return hashlib.sha256(
        record_json.encode("utf-8")
    ).hexdigest()


def main():

    print("Starting Imports Batch Validation\n")

    files = sorted(
        RAW_DATA_DIR.glob(
            f"india_imports_{PERIOD}_chapter_*.json"
        )
    )

    total_records = 0
    total_duplicates = 0

    found_chapters = set()
    all_record_hashes = set()

    errors = []

    for file_path in files:

        chapter = file_path.stem.split("_")[-1]

        print(f"Validating chapter {chapter}...")

        found_chapters.add(chapter)

        try:

            with file_path.open(
                "r",
                encoding="utf-8"
            ) as file:

                payload = json.load(file)

        except (json.JSONDecodeError, OSError) as error:

            errors.append(
                f"{file_path.name}: Cannot read JSON: {error}"
            )

            continue

        records = payload.get("data")

        if not isinstance(records, list):

            errors.append(
                f"{file_path.name}: Invalid data array"
            )

            continue

        api_count = payload.get("count")

        if api_count is None or int(api_count) != len(records):

            errors.append(
                f"{file_path.name}: API count mismatch"
            )

        if not records:

            errors.append(
                f"{file_path.name}: Unexpected empty chapter file"
            )

            continue

        chapter_records = 0

        for index, record in enumerate(records):

            if record.get("reporterCode") != REPORTER_CODE:
                errors.append(
                    f"{file_path.name}, record {index}: Invalid reporter"
                )

            if record.get("partnerCode") != PARTNER_CODE:
                errors.append(
                    f"{file_path.name}, record {index}: Invalid partner"
                )

            if record.get("flowCode") != EXPECTED_FLOW:
                errors.append(
                    f"{file_path.name}, record {index}: Invalid flow"
                )

            if str(record.get("period")) != str(PERIOD):
                errors.append(
                    f"{file_path.name}, record {index}: Invalid period"
                )

            code = str(record.get("cmdCode", ""))

            if not code.startswith(chapter):
                errors.append(
                    f"{file_path.name}, record {index}: "
                    f"Commodity code {code} does not match chapter"
                )

            classification = record.get("classificationCode")

            if classification != "H6":
                errors.append(
                    f"{file_path.name}, record {index}: "
                    f"Unexpected classification {classification}"
                )

            # Detect exact duplicate records across all chapters.
            fingerprint = record_hash(record)

            if fingerprint in all_record_hashes:

                total_duplicates += 1

            else:

                all_record_hashes.add(fingerprint)

            chapter_records += 1

        total_records += chapter_records

        print(
            f"Records: {chapter_records} | "
            f"API count: {api_count}"
        )

    # Check chapter coverage.
    missing_chapters = (
        EXPECTED_CHAPTERS - found_chapters
    )

    unexpected_chapters = (
        found_chapters - EXPECTED_DATA_CHAPTERS
    )

    if missing_chapters != EXPECTED_EMPTY_CHAPTERS:

        errors.append(
            f"Unexpected missing chapters: {sorted(missing_chapters)}"
        )

    if unexpected_chapters:

        errors.append(
            f"Unexpected chapter files: {sorted(unexpected_chapters)}"
        )

    # Final summary.
    print("\n========== VALIDATION SUMMARY ==========")

    print("Chapter files:", len(files))
    print("Non-empty chapters:", len(found_chapters))
    print("Expected empty chapters:", sorted(EXPECTED_EMPTY_CHAPTERS))
    print("Total records:", total_records)
    print("Unique exact records:", len(all_record_hashes))
    print("Exact duplicate records:", total_duplicates)

    if errors:

        print("\nVALIDATION FAILED")

        for error in errors[:30]:
            print("-", error)

        if len(errors) > 30:
            print(
                f"... and {len(errors) - 30} more errors"
            )

        raise SystemExit(1)

    if len(found_chapters) != 97:
        raise SystemExit(
            "VALIDATION FAILED: Expected 97 non-empty chapters"
        )

    if total_records != 152437:
        raise SystemExit(
            "VALIDATION FAILED: Record count differs from ingestion output"
        )

    if total_duplicates != 0:
        raise SystemExit(
            "VALIDATION FAILED: Exact duplicate records detected"
        )

    print("\nVALIDATION PASSED")
    print("All imports chapter files passed validation!")


if __name__ == "__main__":
    main()
