import json
from pathlib import Path


def inspect_raw_data():

    data_dir = Path("data/raw/un_comtrade")

    files = sorted(data_dir.glob("*.json"))

    if not files:
        print("No raw data files found.")
        return

    latest_file = files[-1]

    print("Reading file:", latest_file)

    with latest_file.open("r", encoding="utf-8") as file:
        data = json.load(file)

    print("\nTop-level JSON keys:")
    print(list(data.keys()))

    records = data.get("data", [])

    print("\nTotal records:", len(records))

    if records:
        record = records[0]

        print("\nActual record fields:")
        print(list(record.keys()))

        print("\nActual record values:")

        for key, value in record.items():
            print(f"{key}: {value}")


if __name__ == "__main__":
    inspect_raw_data()