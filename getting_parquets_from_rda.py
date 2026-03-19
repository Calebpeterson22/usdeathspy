# %%
import requests
import polars as pl
import pyreadr
import tempfile
from pathlib import Path


# %%
def write_metadata_parquets(output_dir: Path):
    repo = "byuirpytooling/usdeaths"
    api_url = f"https://api.github.com/repos/{repo}/contents/data"
    files = requests.get(api_url).json()

    for f in files:
        if f["name"].endswith(".rda"):
            content = requests.get(f["download_url"]).content

            with tempfile.NamedTemporaryFile(suffix=".rda", delete=False) as tmp:
                tmp.write(content)
                tmp_path = tmp.name  # save path, let it close

            result = pyreadr.read_r(tmp_path)
            df = pl.from_pandas(list(result.values())[0])
            df.write_parquet(output_dir / f"{Path(f['name']).stem}.parquet")
            Path(tmp_path).unlink()  # clean up

    print(f"Written {len(files)} parquets to {output_dir}")


# %%
output_dir = Path("data")
output_dir.mkdir(exist_ok=True)
write_metadata_parquets(output_dir)
