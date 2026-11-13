
import os
import requests

from pathlib import Path
from dotenv import load_dotenv


# Project configuration
PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("COMTRADE_API_KEY")

if not API_KEY:
    raise ValueError("API key not found in .env")


# UN Comtrade API
url = "https://comtradeapi.un.org/data/v1/get/C/A/HS"

params = {
    "reporterCode": 699,
    "period": 2023,
    "partnerCode": 0,
    "flowCode": "M",
    "cmdCode": "01*",
    "maxRecords": 100000,
}

headers = {
    "Ocp-Apim-Subscription-Key": API_KEY
}


# Fetch imports data
print("Testing India imports for commodity wildcard 01*...")

response = requests.get(
    url,
    params=params,
    headers=headers,
    timeout=120
)

response.raise_for_status()

result = response.json()

records = result.get("data", [])


# Display API response
print("\nAPI request successful")
print("Records received:", len(records))
print("API reported count:", result.get("count"))


# Inspect commodity codes
codes = sorted({
    str(r.get("cmdCode"))
    for r in records
    if r.get("cmdCode") is not None
})

print("\nUnique commodity codes:", len(codes))
print("First 30 codes:", codes[:30])


# Inspect code lengths
print("\nCode lengths:")

for length in sorted({len(code) for code in codes}):

    count = sum(
        len(code) == length
        for code in codes
    )

    print(f"{length} digits: {count}")


# Verify returned records
print("\nClassification codes:")

classifications = sorted({
    r.get("classificationCode")
    for r in records
    if r.get("classificationCode")
})

print(classifications)

print("\nWildcard filter test completed!")
