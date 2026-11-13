
import json
import math

from pathlib import Path


RAW_DATA_DIR = Path("data/raw/un_comtrade")

EXPECTED_REPORTER = 699
EXPECTED_PARTNER = 0
EXPECTED_PERIOD = "2023"
EXPECTED_CLASSIFICATION = "H6"

EXPECTED_FLOWS = {
    "exports": "X",
    "imports": "M",
}


def validate_file(file_path, flow_name):

    print(f"\nValidating: {file_path.name}")

    with file_path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    records = data.get("data", [])
    errors = []

    if not records:
        errors.append("No records found")

    # Check API response count.
    if data.get("count") != len(records):
        errors.append("API response count mismatch")

    commodity_codes = []

    for index, record in enumerate(records):

        # Required fields.
        required_fields = [
            "reporterCode",
            "period",
            "flowCode",
            "partnerCode",
            "classificationCode",
            "cmdCode",
            "primaryValue",
            "isReported",
            "isAggregate",
        ]

        for field in required_fields:

            if field not in record or record[field] is None:
                errors.append(
                    f"Record {index}: Missing field {field}"
                )

        # Skip further checks if essential fields are missing.
        if any(
            record.get(field) is None
            for field in required_fields
        ):
            continue

        # Validate reporter.
        if record["reporterCode"] != EXPECTED_REPORTER:
            errors.append(
                f"Record {index}: Invalid reporter code"
            )

        # Validate period.
        if str(record["period"]) != EXPECTED_PERIOD:
            errors.append(
                f"Record {index}: Invalid period"
            )

        # Validate partner.
        if record["partnerCode"] != EXPECTED_PARTNER:
            errors.append(
                f"Record {index}: Invalid partner code"
            )

        # Validate flow.
        if record["flowCode"] != EXPECTED_FLOWS[flow_name]:
            errors.append(
                f"Record {index}: Invalid flow code"
            )

        # Validate classification.
        if record["classificationCode"] != EXPECTED_CLASSIFICATION:
            errors.append(
                f"Record {index}: Invalid classification"
            )

        # Validate commodity code.
        commodity_code = str(record["cmdCode"])

        if not commodity_code.isdigit():
            errors.append(
                f"Record {index}: Invalid commodity code"
            )

        elif not 2 <= len(commodity_code) <= 6:
            errors.append(
                f"Record {index}: Unexpected HS code length"
            )

        else:
            commodity_codes.append(commodity_code)

        # Validate trade value.
        value = record["primaryValue"]

        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value < 0
        ):
            errors.append(
                f"Record {index}: Invalid trade value"
            )

        # Validate provenance flags.
        if not isinstance(record["isReported"], bool):
            errors.append(
                f"Record {index}: Invalid isReported flag"
            )

        if not isinstance(record["isAggregate"], bool):
            errors.append(
                f"Record {index}: Invalid isAggregate flag"
            )

    # Check duplicate commodity codes.
    if len(commodity_codes) != len(set(commodity_codes)):
        errors.append("Duplicate commodity codes detected")

    if errors:

        print("VALIDATION FAILED")

        for error in errors:
            print("-", error)

        return False

    print("VALIDATION PASSED")
    print("Records:", len(records))
    print("Unique commodity codes:", len(set(commodity_codes)))
    print("Classification:", EXPECTED_CLASSIFICATION)

    return True


def get_latest_files():

    latest_files = {}

    for file_path in RAW_DATA_DIR.glob("*.json"):

        for flow_name in EXPECTED_FLOWS:

            if flow_name in file_path.name:

                current_file = latest_files.get(flow_name)

                if (
                    current_file is None
                    or file_path.name > current_file.name
                ):
                    latest_files[flow_name] = file_path

    return latest_files


def main():

    print("Starting Commodity-Level Raw Validation")

    if not RAW_DATA_DIR.exists():
        raise FileNotFoundError("Raw data directory not found")

    latest_files = get_latest_files()

    results = []

    for flow_name in EXPECTED_FLOWS:

        if flow_name not in latest_files:
            raise FileNotFoundError(
                f"No {flow_name} data found"
            )

        results.append(
            validate_file(
                latest_files[flow_name],
                flow_name
            )
        )

    if not all(results):
        raise RuntimeError("Raw validation failed")

    print("\nAll commodity-level raw files passed validation!")


if __name__ == "__main__":
    main()
