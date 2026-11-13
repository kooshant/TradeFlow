
import json
from pathlib import Path

raw_dir = Path("data/raw/un_comtrade")

files = sorted(
    raw_dir.glob("india_exports_2023_full_*.json")
)

if not files:
    raise FileNotFoundError("Full exports file not found")

latest_file = files[-1]

with latest_file.open("r", encoding="utf-8") as file:
    payload = json.load(file)

records = payload.get("data", [])

codes = sorted({
    str(record.get("cmdCode"))
    for record in records
    if record.get("cmdCode") is not None
})

print("Full exports records:", len(records))
print("Unique commodity codes:", len(codes))
print("\nFirst 30 commodity codes:")

for code in codes[:30]:
    print(code)

print("\nCode lengths:")
for length in sorted({len(code) for code in codes}):
    count = sum(len(code) == length for code in codes)
    print(f"{length} digits: {count}")
