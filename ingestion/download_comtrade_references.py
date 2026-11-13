
import csv
import json
from pathlib import Path
from urllib.request import Request, urlopen


BASE_URL = "https://comtradeapi.un.org/files/v1/app/reference"

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "reference" / "un_comtrade"


def download_json(url, output_path):
    print(f"\nDownloading: {url}")

    request = Request(
        url,
        headers={"User-Agent": "TradeFlow-Data-Engineering-Project"}
    )

    with urlopen(request, timeout=60) as response:
        content = response.read()

    data = json.loads(content.decode("utf-8"))

    output_path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    print(f"Saved: {output_path}")
    return data


def build_commodity_reference(data):
    records = data.get("results", [])

    output_path = OUTPUT_DIR / "hs2022_commodity_reference.csv"

    rows = []

    for item in records:
        code = str(item.get("id", ""))

        # Exclude the TOTAL aggregate row.
        if not code.isdigit():
            continue

        description = str(item.get("text", ""))

        # Remove the code prefix from the description.
        if " - " in description:
            description = description.split(" - ", 1)[1]

        rows.append({
            "hs_code": code,
            "cmd_desc": description,
            "parent_hs_code": item.get("parent"),
            "is_leaf": item.get("isLeaf"),
            "aggregation_level": item.get("aggrlevel"),
            "standard_unit": item.get("standardUnitAbbr")
        })

    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "hs_code",
                "cmd_desc",
                "parent_hs_code",
                "is_leaf",
                "aggregation_level",
                "standard_unit"
            ]
        )
        writer.writeheader()
        writer.writerows(rows)

    print("\nCOMMODITY REFERENCE")
    print("Total records:", len(rows))
    print("Unique HS codes:", len(set(row["hs_code"] for row in rows)))
    print("Saved:", output_path)


def build_partner_reference(data):
    records = data.get("results", [])

    output_path = OUTPUT_DIR / "partner_area_reference.csv"

    rows = []

    for item in records:
        rows.append({
            "partner_code": item.get("PartnerCode", item.get("id")),
            "partner_desc": item.get("PartnerDesc", item.get("text")),
            "partner_iso_alpha2": item.get("PartnerCodeIsoAlpha2"),
            "partner_iso_alpha3": item.get("PartnerCodeIsoAlpha3"),
            "partner_note": item.get("partnerNote"),
            "is_group": item.get("isGroup")
        })

    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "partner_code",
                "partner_desc",
                "partner_iso_alpha2",
                "partner_iso_alpha3",
                "partner_note",
                "is_group"
            ]
        )
        writer.writeheader()
        writer.writerows(rows)

    codes = [row["partner_code"] for row in rows]

    print("\nPARTNER REFERENCE")
    print("Total records:", len(rows))
    print("Unique partner codes:", len(set(codes)))
    print("Duplicate partner codes:", len(codes) - len(set(codes)))
    print("Contains World code 0:", 0 in codes)
    print("Saved:", output_path)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("TRADEFLOW OFFICIAL REFERENCE DATA DOWNLOAD")
    print("=" * 60)

    commodity_data = download_json(
        f"{BASE_URL}/H6.json",
        OUTPUT_DIR / "hs2022_commodity_reference.json"
    )

    partner_data = download_json(
        f"{BASE_URL}/partnerAreas.json",
        OUTPUT_DIR / "partner_area_reference.json"
    )

    build_commodity_reference(commodity_data)
    build_partner_reference(partner_data)

    print("\nREFERENCE DOWNLOAD COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()
