import re
import polars as pl
from usdeathspy.get_html_page import get_html_page
from usdeathspy.scrape_doc_section import scrape_doc_section
from usdeathspy.scrape_mult_mort_user_guide import scrape_mult_mort_user_guide
from usdeathspy.parse_file_size_mb import parse_file_size_mb


def scrape_all_sections(url: str, url_pdf: str | None = None) -> pl.DataFrame:
    """
        Scrape all CDC Vital Statistics sections.

        Downloads and combines all the main CDC Vital Statistics sections
        into a single DataFrame. Optionally scrapes the separate Mortality Multiple
        Cause-of-Death documentation page and merges it in.

        Args:
            url (str): The CDC Vital Stats page URL.
                Typically: https://www.cdc.gov/nchs/data_access/VitalStatsOnline.htm
            url_pdf (str | None): Optional CDC Mortality documentation page URL.
                If provided, the placeholder mortality user guide link is replaced
                with the full set of scraped links from that page.
                Typically: https://www.cdc.gov/nchs/nvss/mortality_public_use_data.htm

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

        Notes:
            A known typo in the CDC source data causes one file size to appear as
            "10.2.MB" instead of "10.2 MB". This is corrected automatically.

        Example:
    ```python
            df = scrape_all_sections(
                url     = "https://www.cdc.gov/nchs/data_access/VitalStatsOnline.htm",
                url_pdf = "https://www.cdc.gov/nchs/nvss/mortality_public_use_data.htm"
            )
    ```
    """
    page = get_html_page(url)

    sections = {
        "Births": ["User Guide", "U.S. Data", "U.S. Territories"],
        "Period_cohort": ["User Guide", "U.S. Data", "U.S. Territories"],
        "Birth_Cohort": ["User Guide", "U.S. Data", "U.S. Territories"],
        "matched-multiple": ["User Guide", "U.S. Data"],
        "Mortality_Multiple": ["User Guide", "U.S. Data", "U.S. Territories"],
        "Fetal_Death": ["User Guide", "U.S. Data", "U.S. Territories"],
    }

    all_data = []
    for anchor_id, subs in sections.items():
        df = scrape_doc_section(page, anchor_id, anchor_id.lower(), subs)
        df = df.cast({col: pl.String for col in df.columns})
        all_data.append(df)

    result = (
        pl.concat(all_data, how="vertical")
        .with_columns(
            # Fix CDC typo e.g. "10.2.MB" -> "10.2 MB"
            pl.col("file_size")
            .str.replace(r"([\d]+\.[\d]+)\.", "$1 ")
            .alias("file_size"),
            pl.col("year").str.slice(0, 4).cast(pl.Int32).alias("year"),
        )
        .with_columns(parse_file_size_mb(pl.col("file_size")).alias("file_size_mb"))
    )

    if url_pdf is not None:
        result = result.filter(
            ~(
                (pl.col("section") == "mortality_multiple")
                & (pl.col("subsection") == "User Guide")
                & (pl.col("file_type") == ".htm")
            )
        ).cast({col: pl.String for col in result.columns})
        mult_mort = scrape_mult_mort_user_guide(url_pdf).cast(
            {col: pl.String for col in scrape_mult_mort_user_guide(url_pdf).columns}
        )
        result = pl.concat([result, mult_mort], how="vertical")

    return result
