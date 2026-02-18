# usdeathspy

A Python package for scraping and accessing CDC Vital Statistics data.

## Installation

```bash
pip install usdeathspy
```

## Quick Start

Load the pre-scraped CDC Vital Statistics dataset bundled with the package:

```python
from usdeathspy import load_cdc_data

df = load_cdc_data()
print(df)
```

Or scrape fresh data directly from the CDC:

```python
from usdeathspy import scrape_all_sections

df = scrape_all_sections(
    url     = "https://www.cdc.gov/nchs/data_access/VitalStatsOnline.htm",
    url_pdf = "https://www.cdc.gov/nchs/nvss/mortality_public_use_data.htm"
)
```

## Documentation

See the [API](API.md) page for full function documentation.
