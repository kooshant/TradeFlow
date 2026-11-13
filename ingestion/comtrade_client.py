
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

BASE_URL = "https://comtradeapi.un.org/data/v1/get/C/A/HS"

REPORTER_CODE = 699
PARTNER_CODE = 0
PERIOD = 2023

MAX_RECORDS = 100000
MAX_RETRIES = 5

FLOW_CODES = {
    "exports": "X",
    "imports": "M",
}


# Load API key securely
load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("COMTRADE_API_KEY")


def fetch_trade_data(flow_name, flow_code):

    if not API_KEY:
        raise ValueError(
            "COMTRADE_API_KEY not found in .env"
        )

    params = {
        "reporterCode": REPORTER_CODE,
        "period": PERIOD,
        "partnerCode": PARTNER_CODE,
        "flowCode": flow_code,
        "maxRecords": MAX_RECORDS,
    }

    headers = {
        "Ocp-Apim-Subscription-Key": API_KEY
    }

    for attempt in range(MAX_RETRIES):

        try:

            response = requests.get(
                BASE_URL,
                params=params,
                headers=headers,
                timeout=120
            )

            if response.status_code == 429:

                retry_after = response.headers.get(
                    "Retry-After"
                )

                wait_time = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else min(60, 2 ** (attempt + 2))
                )

                if attempt == MAX_RETRIES - 1:
                    response.raise_for_status()

                print(
                    f"Rate limit reached. "
                    f"Waiting {wait_time} seconds..."
                )

                time.sleep(wait_time)
                continue

            if response.status_code in [
                500, 502, 503, 504
            ]:

                if attempt == MAX_RETRIES - 1:
                    response.raise_for_status()

                wait_time = 2 ** (attempt + 1)

                print(
                    f"Server error {response.status_code}. "
                    f"Retrying in {wait_time} seconds..."
                )

                time.sleep(wait_time)
                continue

            response.raise_for_status()

            data = response.json()

            if data.get("error"):
                raise RuntimeError(
                    f"API error: {data['error']}"
                )

            records = data.get("data", [])

            reported_count = data.get("count")

            if not records:
                raise ValueError(
                    f"No {flow_name} records returned"
                )

            if reported_count != len(records):
                raise ValueError(
                    "API count does not match returned records"
                )

            # Never silently accept a potentially truncated response.
            if len(records) >= MAX_RECORDS:
                raise RuntimeError(
                    f"{flow_name}: API returned the maximum "
                    f"{MAX_RECORDS} records. Completeness is "
                    "not established. Split the extraction "
                    "into smaller queries."
                )

            print(
                f"{flow_name.capitalize()} records received:",
                len(records)
            )

            return data

        except requests.RequestException as error:

            if attempt == MAX_RETRIES - 1:
                raise

            wait_time = 2 ** (attempt + 1)

            print(
                f"Network/API request failed: "
                f"{type(error).__name__}. "
                f"Retrying in {wait_time} seconds..."
            )

            time.sleep(wait_time)

    raise RuntimeError(
        f"Maximum retries exceeded for {flow_name}"
    )


def validate_records(data, flow_name):

    records = data["data"]

    expected_flow = FLOW_CODES[flow_name]

    for index, record in enumerate(records):

        if record.get("reporterCode") != REPORTER_CODE:
            raise ValueError(
                f"Record {index}: Invalid reporter"
            )

        if str(record.get("period")) != str(PERIOD):
            raise ValueError(
                f"Record {index}: Invalid period"
            )

        if record.get("partnerCode") != PARTNER_CODE:
            raise ValueError(
                f"Record {index}: Invalid partner"
            )

        if record.get("flowCode") != expected_flow:
            raise ValueError(
                f"Record {index}: Invalid trade flow"
            )


def save_raw_data(data, flow_name, run_id):

    RAW_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = RAW_DATA_DIR / (
        f"india_{flow_name}_{PERIOD}_full_{run_id}.json"
    )

    # Write to a temporary file before finalising.
    temp_file = output_file.with_suffix(".json.tmp")

    with temp_file.open("w", encoding="utf-8") as file:

        json.dump(
            data,
            file,
            indent=2,
            ensure_ascii=False
        )

    temp_file.replace(output_file)

    print("Raw data saved:", output_file)


def main():

    print("Starting authenticated UN Comtrade ingestion")

    print("Reporter: India (699)")
    print("Partner: World (0)")
    print("Period:", PERIOD)

    if not API_KEY:
        raise ValueError(
            "API key missing. Check project root .env"
        )

    run_id = datetime.now(
        timezone.utc
    ).strftime("%Y%m%d_%H%M%S_%f")

    for index, (flow_name, flow_code) in enumerate(
        FLOW_CODES.items()
    ):

        print(f"\nFetching India {flow_name}...")

        data = fetch_trade_data(
            flow_name,
            flow_code
        )

        validate_records(
            data,
            flow_name
        )

        save_raw_data(
            data,
            flow_name,
            run_id
        )

        # Respect the free API's one-request-per-second limit.
        if index < len(FLOW_CODES) - 1:
            time.sleep(1.1)

    print("\nUN Comtrade ingestion completed!")


if __name__ == "__main__":
    main()
