
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


def main():

    print("Starting Trade Dimension Profiling\n")

    with INPUT_FILE.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    records = payload["data"]

    # Categorical dimensions
    dimensions = [
        "flowCode",
        "freqCode",
        "refMonth",
        "classificationCode",
        "customsCode",
        "mosCode",
        "motCode",
        "qtyUnitCode",
        "altQtyUnitCode",
        "legacyEstimationFlag",
        "isReported",
        "isAggregate",
    ]

    for column in dimensions:

        counts = Counter(
            str(record.get(column))
            for record in records
        )

        print(f"\n{column}")
        print("-" * 40)

        for value, count in counts.most_common(15):
            print(f"{value}: {count}")

    # HS commodity code lengths
    code_lengths = Counter(
        len(str(record["cmdCode"]))
        for record in records
        if record.get("cmdCode")
    )

    print("\nHS Commodity Code Lengths")
    print("-" * 40)

    for length, count in sorted(code_lengths.items()):
        print(f"{length} digits: {count}")

    # Numerical ranges
    numeric_columns = [
        "primaryValue",
        "fobvalue",
        "cifvalue",
        "qty",
        "altQty",
        "netWgt",
        "grossWgt",
    ]

    print("\nNumerical Ranges")
    print("-" * 40)

    for column in numeric_columns:

        values = [
            record[column]
            for record in records
            if record.get(column) is not None
        ]

        if values:
            print(f"\n{column}")
            print("  Minimum:", min(values))
            print("  Maximum:", max(values))
            print("  Zero values:", sum(v == 0 for v in values))
            print("  Non-null values:", len(values))

    print("\nDimension profiling completed!")


if __name__ == "__main__":
    main()
