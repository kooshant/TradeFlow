
import os
import requests

from pathlib import Path
from dotenv import load_dotenv


# Load environment variables from project root.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("COMTRADE_API_KEY")

BASE_URL = (
    "https://comtradeapi.un.org/data/v1/get/C/A/HS"
)


def test_api_connection():

    print("Testing UN Comtrade authenticated API connection...")

    if not API_KEY:
        raise ValueError(
            "API key not found. Check your .env file."
        )

    headers = {
        "Ocp-Apim-Subscription-Key": API_KEY
    }

    params = {
        "reporterCode": 699,
        "period": 2023,
        "partnerCode": 0,
        "cmdCode": "TOTAL",
        "flowCode": "X",
        "maxRecords": 1,
    }

    response = requests.get(
        BASE_URL,
        headers=headers,
        params=params,
        timeout=60
    )

    if response.status_code in (401, 403):
        raise RuntimeError(
            "Authentication failed. Check your API key "
            "and subscription access."
        )

    response.raise_for_status()

    result = response.json()

    if result.get("error"):
        raise RuntimeError(
            f"API returned an error: {result['error']}"
        )

    print("\nAPI CONNECTION SUCCESSFUL!")
    print("HTTP Status:", response.status_code)
    print("Records received:", result.get("count", 0))
    print("API key detected: Yes")


if __name__ == "__main__":
    test_api_connection()
