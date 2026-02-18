import polars as pl
import pytest
from usdeathspy.parse_file_size_mb import parse_file_size_mb


def test_parse_file_size_mb_kb():
    df = pl.DataFrame({"file_size": ["531 KB"]})
    result = df.with_columns(parse_file_size_mb(pl.col("file_size")).alias("mb"))
    assert round(result["mb"][0], 5) == round(531 / 1024, 5)


def test_parse_file_size_mb_mb():
    df = pl.DataFrame({"file_size": ["1.8 MB"]})
    result = df.with_columns(parse_file_size_mb(pl.col("file_size")).alias("mb"))
    assert result["mb"][0] == 1.8


def test_parse_file_size_mb_gb():
    df = pl.DataFrame({"file_size": ["1 GB"]})
    result = df.with_columns(parse_file_size_mb(pl.col("file_size")).alias("mb"))
    assert result["mb"][0] == 1024.0


def test_parse_file_size_mb_none():
    df = pl.DataFrame({"file_size": pl.Series([None], dtype=pl.String)})
    result = df.with_columns(parse_file_size_mb(pl.col("file_size")).alias("mb"))
    assert result["mb"][0] is None


def test_parse_file_size_mb_typo():
    # CDC known typo "10.2.MB" should be handled after fix in scrape_all_sections
    df = pl.DataFrame({"file_size": ["10.2 MB"]})
    result = df.with_columns(parse_file_size_mb(pl.col("file_size")).alias("mb"))
    assert result["mb"][0] == 10.2
