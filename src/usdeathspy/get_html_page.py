from bs4 import BeautifulSoup
import requests

def get_html_page(url: str) -> BeautifulSoup:
    """
    Fetch and parse an HTML page.

    This function downloads the HTML content from a given URL and
    parses it into a BeautifulSoup object, which can then be queried
    using CSS selectors or tag-based navigation.

    Args:
        url (str): The URL of the HTML page to retrieve.

    Returns:
        BeautifulSoup: A parsed representation of the HTML document.

    Example:
        ```python
        soup = get_html_page(
            "https://www.cdc.gov/nchs/data_access/VitalStatsOnline.htm"
        )
        soup.select_one("a#births")
        ```
    """
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html.parser")
    return soup
