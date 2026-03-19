import re
import polars as pl
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import requests

from usdeathspy.get_html_page import get_html_page
from usdeathspy.parse_file_size_mb import parse_file_size_mb


def scrape_mult_mort_user_guide(url: str) -> pl.DataFrame:
    """
    Scrape Mortality Multiple Cause-of-Death user guide links from the CDC.

    Extracts downloadable file links from the CDC Mortality Multiple
    Cause-of-Death documentation page and returns a Polars DataFrame
    with metadata about each file.

    Args:
        url (str): URL of the CDC mortality documentation page.
            Typically: https://www.cdc.gov/nchs/nvss/mortality_public_use_data.htm

    Returns:
        pl.DataFrame: A DataFrame with columns:
            - section (str): Always "mortality_multiple"
            - subsection (str): Always "User Guide"
            - link_text (str): Text of the download link
            - year (int): Four-digit year extracted from link text, filled down for sub-items
            - file_size (str): File size string, if present
            - url (str): Absolute URL to the file
            - file_type (str): File extension
            - file_size_mb (float): File size converted to megabytes

    Notes:
        1997 and 1998 entries link to separate HTML pages containing many
        PDFs and are not scraped by this function as of 2/17/2026.
    """
    soup = get_html_page(url)
    links = soup.select(".cdc-textblock li a[href]")

    records = [
        {
            "link_text": a.get_text(separator=" ", strip=True),
            "url": urljoin("https://www.cdc.gov", a["href"]),
        }
        for a in links
        if a.get("href")
    ]

    return (
        pl.DataFrame(records)
        .filter(pl.col("url").is_not_null())
        .with_columns(
            pl.lit("mortality_multiple").alias("section"),
            pl.lit("User Guide").alias("subsection"),
            pl.col("url")
            .str.extract(r"(\.[a-zA-Z0-9]+)$", group_index=1)
            .alias("file_type"),
            pl.col("link_text")
            .str.extract(r"\[PDF \u2013 ([^\]]+)\]", group_index=1)
            .alias("file_size"),
            pl.col("link_text")
            .str.extract(r"(\d{4})", group_index=1)
            .cast(pl.Int32)
            .alias("year"),
        )
        .with_columns(
            pl.col("year").forward_fill().alias("year"),
            pl.col("file_size"),
            parse_file_size_mb(pl.col("file_size")).alias("file_size_mb"),
        )
        .select(
            [
                "section",
                "subsection",
                "link_text",
                "year",
                "file_size",
                "url",
                "file_type",
                "file_size_mb",
            ]
        )
    )
