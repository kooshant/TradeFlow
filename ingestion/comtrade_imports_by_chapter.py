
import json
import os
import time
import requests

from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv


# Project configuration
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "un_comtrade"

load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("COMTRADE_API_KEY")

BASE_URL = "https://comtradeapi.un.org/data/v1/get/C/A/HS"

REPORTER_CODE = 699
PARTNER_CODE = 0
PERIOD = 2023

MAX_RECORDS = 100000
MAX_RETRIES = 5

HEADERS = {
    "Ocp-Apim-Subscription-Key": API_KEY
}


def fetch_chapter(chapter):

    params = {
        "reporterCode": REPORTER_CODE,
        "period": PERIOD,
        "partnerCode": PARTNER_CODE,
        "flowCode": "M",
        "cmdCode": f"{chapter}*",
        "maxRecords": MAX_RECORDS,
    }

    for attempt in range(MAX_RETRIES):

        try:

            response = requests.get(
                BASE_URL,
                params=params,
                headers=HEADERS,
                timeout=180
            )

            if response.status_code == 429:

                wait_time = int(
                    response.headers.get("Retry-After", 10)
                )

                if attempt == MAX_RETRIES - 1:
                    response.raise_for_status()

                print(
                    f"Rate limit reached. "
                    f"Waiting {wait_time} seconds..."
                )

                time.sleep(wait_time)
                continue

            if response.status_code in [500, 502, 503, 504]:

                if attempt == MAX_RETRIES - 1:
                    response.raise_for_status()

                wait_time = 2 ** (attempt + 1)

                print(
                    f"Server error. Retrying in "
                    f"{wait_time} seconds..."
                )

                time.sleep(wait_time)
                continue

            response.raise_for_status()

            result = response.json()
            records = result.get("data", [])

            if result.get("count") != len(records):
                raise ValueError(
                    f"Chapter {chapter}: API count mismatch"
                )

            if len(records) >= MAX_RECORDS:
                raise RuntimeError(
                    f"Chapter {chapter} reached the API limit. "
                    "Further subdivision is required."
                )

            for record in records:

                if record.get("reporterCode") != REPORTER_CODE:
                    raise ValueError("Invalid reporter code")

                if record.get("flowCode") != "M":
                    raise ValueError("Invalid trade flow")

                if str(record.get("period")) != str(PERIOD):
                    raise ValueError("Invalid period")

                if record.get("partnerCode") != PARTNER_CODE:
                    raise ValueError("Invalid partner")

                code = str(record.get("cmdCode", ""))

                if not code.startswith(chapter):
                    raise ValueError(
                        f"Unexpected commodity code: {code}"
                    )

            return result

        except requests.RequestException as error:

            if attempt == MAX_RETRIES - 1:
                raise

            wait_time = 2 ** (attempt + 1)

            print(
                f"Network error for chapter {chapter}: "
                f"{type(error).__name__}. "
                f"Retrying in {wait_time} seconds..."
            )

            time.sleep(wait_time)

    raise RuntimeError(
        f"Failed to fetch chapter {chapter}"
    )


def save_chapter(result, chapter):

    RAW_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = RAW_DATA_DIR / (
        f"india_imports_{PERIOD}_chapter_{chapter}.json"
    )

    temp_file = output_file.with_suffix(".json.tmp")

    with temp_file.open("w", encoding="utf-8") as file:

        json.dump(
            result,
            file,
            indent=2,
            ensure_ascii=False
        )

    temp_file.replace(output_file)

    print("Saved:", output_file.name)


def main():

    if not API_KEY:
        raise ValueError("COMTRADE_API_KEY missing in .env")

    print("Starting chapter-wise India imports ingestion")
    print("Reporter: India (699)")
    print("Partner: World (0)")
    print("Period:", PERIOD)

    total_records = 0
    successful_chapters = 0
    empty_chapters = 0

    for number in range(1, 100):

        chapter = f"{number:02d}"

        output_file = RAW_DATA_DIR / (
            f"india_imports_{PERIOD}_chapter_{chapter}.json"
        )

        # Resume safely if an existing batch is valid.
        if output_file.exists():

            try:
                with output_file.open(
                    "r", encoding="utf-8"
                ) as file:
                    existing = json.load(file)

                existing_records = existing.get("data", [])

                if (
                    existing.get("count") == len(existing_records)
                    and all(
                        str(r.get("cmdCode", "")).startswith(chapter)
                        and r.get("flowCode") == "M"
                        and r.get("reporterCode") == REPORTER_CODE
                        for r in existing_records
                    )
                ):
                    print(
                        f"\nChapter {chapter}: "
                        f"Already saved, skipping "
                        f"({len(existing_records)} records)"
                    )

                    total_records += len(existing_records)
                    successful_chapters += 1
                    continue

            except (json.JSONDecodeError, OSError):
                pass

            print(
                f"\nChapter {chapter}: "
                "Existing file invalid. Re-fetching."
            )

        print(f"\nFetching chapter {chapter}...")

        result = fetch_chapter(chapter)
        records = result.get("data", [])

        if records:

            save_chapter(result, chapter)

            total_records += len(records)
            successful_chapters += 1

            print("Records received:", len(records))

        else:

            empty_chapters += 1
            print("No records available for this chapter.")

        # Respect API request rate limit.
        time.sleep(1.1)

    print("\nChapter-wise imports ingestion completed!")
    print("Successful chapters:", successful_chapters)
    print("Empty chapters:", empty_chapters)
    print("Total records:", total_records)


if __name__ == "__main__":
    main()
