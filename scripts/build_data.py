from usdeathspy.scrape_all_sections import scrape_all_sections
from pathlib import Path

df = scrape_all_sections(
    url="https://www.cdc.gov/nchs/data_access/VitalStatsOnline.htm",
    url_pdf="https://www.cdc.gov/nchs/nvss/mortality_public_use_data.htm",
)

out = Path("src/usdeathspy/data/cdc_all_data.parquet")
out.parent.mkdir(parents=True, exist_ok=True)
df.write_parquet(out)
print(f"Saved {len(df)} rows to {out}")
