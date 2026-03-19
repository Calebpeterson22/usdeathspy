from bs4 import BeautifulSoup
import polars as pl
from urllib.parse import urljoin

def scrape_doc_section(
        page: BeautifulSoup,
        anchor_id: str,
        section_name: str,
        subsection_names: list[str]
 )   -> pl.DataFrame:
    """
Scrape documentation links from a specific section of a CDC page.

This function extracts all download links from a particular section of
a CDC documentation page, organizing them by subsection. It parses the
HTML structure to find links within `.listScroll` elements and extracts
metadata such as file type, year, and file size.

Args:
    page (BeautifulSoup): A parsed HTML page containing the documentation.
    anchor_id (str): The HTML anchor ID identifying the section to scrape.
    section_name (str): A descriptive name for the section (e.g., "births").
    subsection_names (list[str]): Names for each subsection within the section,
        such as ["User Guide", "U.S. Data", "U.S. Territories"]. Must match
        the number of `.listScroll` elements found.

Returns:
    pl.DataFrame: A Polars DataFrame with columns:
        - section: The section name
        - subsection: The subsection name
        - link_text: The full text of the link
        - year: Extracted year from the link text
        - file_size: Extracted file size from the link text
        - url: The absolute URL to the file
        - file_type: The file extension (e.g., ".pdf", ".zip")

Raises:
    ValueError: If the anchor ID is not found or if the number of
        `.listScroll` elements doesn't match `subsection_names`.

Example:
```python
    page = get_html_page("https://www.cdc.gov/nchs/data_access/VitalStatsOnline.htm")
    df = scrape_doc_section(
        page,
        anchor_id="Births",
        section_name="births",
        subsection_names=["User Guide", "U.S. Data", "U.S. Territories"]
    )
```
    """
    anchor = page.select_one(f"a#{anchor_id}")
    if anchor is None:
        raise ValueError(f"Anchor '{anchor_id}' not found")

    section = anchor.parent.parent.parent

    list_scrolls = section.select(".listScroll")
    if len(list_scrolls) != len(subsection_names):
        raise ValueError(
            "Number of listScroll sections does not match subsection_names"
        )

    rows = []

    for block, subsection in zip(list_scrolls, subsection_names):
        links = block.select("a")

        for link in links:
            href = link.get("href")
            if href is None:
                continue

            rows.append({
                "section": section_name,
                "subsection": subsection,
                "link_text": link.get_text(strip=True),
                "url": urljoin("https://www.cdc.gov", href)
            })

    df = pl.DataFrame(rows)

    df = df.with_columns([
        pl.col("url")
          .str.extract(r"(\.[a-zA-Z0-9]+)$")
          .alias("file_type"),

        pl.col("link_text")
          .str.slice(0, 4)
          .alias("year"),

        pl.col("link_text")
          .str.extract(r"\(([^)]+)\)$")
          .alias("file_size")
    ])

    df = df.select([
        "section",
        "subsection",
        "link_text",
        "year",
        "file_size",
        "url",
        "file_type"
    ])

    return df


# scrape_cdc_section <- function(page, anchor_id, section_name, subsection_names) {
#     anchor <- page %>% html_element(paste0("a#", anchor_id))
#     stopifnot(!is.na(anchor))

#     section <- anchor %>%
#         xml_parent() %>%
#         xml_parent() %>%
#         xml_parent()

#     list_scrolls <- section %>% html_elements(".listScroll")
#     stopifnot(length(list_scrolls) == length(subsection_names))

#     files <- purrr::map2_dfr(list_scrolls, subsection_names, ~ {
#         links <- .x %>% html_elements("a")
#         tibble(
#             link_text = html_text2(links),
#             url = html_attr(links, "href"),
#             subsection = .y
#         )
#     }) %>%
#         filter(!is.na(url)) %>%
#         mutate(
#             url = url_absolute(url, "https://www.cdc.gov"),
#             file_type = str_extract(url, "\\.[a-zA-Z0-9]+$"),
#             section = section_name,
#             year = str_extract(link_text, "^[^ ]+"),
#             file_size = str_extract(link_text, "\\(([^)]+)\\)$") %>%
#                 str_remove_all("[()]")
#         ) %>%
#         select(section, subsection, link_text, year, file_size, url, file_type)

#     files
# }