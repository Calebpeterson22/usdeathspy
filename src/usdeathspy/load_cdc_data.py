import polars as pl
from importlib.resources import files


def load_cdc_data() -> pl.DataFrame:
    """
        Load the pre-scraped CDC Vital Statistics dataset.

        Returns a Polars DataFrame containing all CDC Vital Statistics
        download links and metadata, bundled with the package.

        Returns:
            pl.DataFrame: A DataFrame with columns:
                - section (str): Section name
                - subsection (str): Subsection name
                - link_text (str): Text of the download link
                - year (int): Four-digit year
                - file_size (str): Raw file size string
                - url (str): Absolute URL to the file
                - file_type (str): File extension
                - file_size_mb (float): File size converted to megabytes

        Example:
    ```python
            from usdeathspy import load_cdc_data
            df = load_cdc_data()
            df.filter(pl.col("section") == "births")
    ```
    """
    path = files("usdeathspy.data").joinpath("cdc_all_data.parquet")
    return pl.read_parquet(path)
