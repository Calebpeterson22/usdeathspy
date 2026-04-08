import pytest
import pandas as pd
from summarize_deaths import summarize_deaths


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mort1969():
    return pd.DataFrame({
        "sex": ["M", "M", "F", "F", "F", "M"],
        "race_recode3": ["White", "Black", "White", "White", "Black", "White"],
        "age": [45, 60, 32, 78, 55, 90],
    })


@pytest.fixture
def mort1970():
    return pd.DataFrame({
        "sex": ["M", "F", "F", "M", "M"],
        "race_recode3": ["White", "Black", "White", "Black", "White"],
        "age": [50, 40, 88, 70, 33],
    })


# ---------------------------------------------------------------------------
# Single dataset
# ---------------------------------------------------------------------------

class TestSingleDataset:

    def test_group_by_single_column(self, mort1969):
        result = summarize_deaths(mort1969, by="sex")
        assert list(result.columns) == ["sex", "n"]
        assert result["n"].iloc[0] >= result["n"].iloc[1], "Should be sorted descending"
        assert set(result["sex"]) == {"M", "F"}
        assert result["n"].sum() == len(mort1969)

    def test_group_by_multiple_columns(self, mort1969):
        result = summarize_deaths(mort1969, by=["sex", "race_recode3"])
        assert "sex" in result.columns
        assert "race_recode3" in result.columns
        assert "n" in result.columns
        assert result["n"].sum() == len(mort1969)

    def test_by_as_string_and_list_equivalent(self, mort1969):
        result_str = summarize_deaths(mort1969, by="sex")
        result_list = summarize_deaths(mort1969, by=["sex"])
        pd.testing.assert_frame_equal(result_str, result_list)

    def test_sorted_descending(self, mort1969):
        result = summarize_deaths(mort1969, by="sex")
        assert result["n"].is_monotonic_decreasing

    def test_counts_are_correct(self, mort1969):
        result = summarize_deaths(mort1969, by="sex")
        counts = dict(zip(result["sex"], result["n"]))
        assert counts["M"] == 3
        assert counts["F"] == 3

    def test_single_group_value(self):
        df = pd.DataFrame({"sex": ["M", "M", "M"]})
        result = summarize_deaths(df, by="sex")
        assert len(result) == 1
        assert result["n"].iloc[0] == 3


# ---------------------------------------------------------------------------
# Multiple datasets
# ---------------------------------------------------------------------------

class TestMultipleDatasets:

    def test_returns_year_column(self, mort1969, mort1970):
        result = summarize_deaths(mort1969=mort1969, mort1970=mort1970, by="sex")
        assert "year" in result.columns
        assert set(result["year"]) == {"mort1969", "mort1970"}

    def test_year_is_first_column(self, mort1969, mort1970):
        result = summarize_deaths(mort1969=mort1969, mort1970=mort1970, by="sex")
        assert result.columns[0] == "year"

    def test_total_counts_correct(self, mort1969, mort1970):
        result = summarize_deaths(mort1969=mort1969, mort1970=mort1970, by="sex")
        assert result["n"].sum() == len(mort1969) + len(mort1970)

    def test_sorted_by_year_then_desc_n(self, mort1969, mort1970):
        result = summarize_deaths(mort1969=mort1969, mort1970=mort1970, by="sex")
        for year, group in result.groupby("year", sort=False):
            assert group["n"].is_monotonic_decreasing, f"Year '{year}' not sorted descending by n"

    def test_multi_column_grouping(self, mort1969, mort1970):
        result = summarize_deaths(
            mort1969=mort1969, mort1970=mort1970, by=["sex", "race_recode3"]
        )
        assert "year" in result.columns
        assert "sex" in result.columns
        assert "race_recode3" in result.columns
        assert "n" in result.columns


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrors:

    def test_no_datasets_raises(self):
        with pytest.raises(ValueError, match="At least one DataFrame"):
            summarize_deaths(by="sex")

    def test_missing_column_single_dataset(self, mort1969):
        with pytest.raises(ValueError, match="not found in the data"):
            summarize_deaths(mort1969, by="nonexistent_col")

    def test_missing_column_multi_dataset(self, mort1969, mort1970):
        with pytest.raises(ValueError, match="not found in 'mort1970'"):
            summarize_deaths(mort1969=mort1969, mort1970=mort1970, by="nonexistent_col")

    def test_mixed_positional_and_named_raises(self, mort1969, mort1970):
        with pytest.raises(ValueError, match="not both"):
            summarize_deaths(mort1969, mort1970=mort1970, by="sex")

    def test_multiple_positional_raises(self, mort1969, mort1970):
        with pytest.raises(ValueError, match="must be named"):
            summarize_deaths(mort1969, mort1970, by="sex")

    def test_missing_one_of_multiple_by_columns(self, mort1969):
        with pytest.raises(ValueError, match="not found in the data"):
            summarize_deaths(mort1969, by=["sex", "does_not_exist"])

    def test_empty_dataframe(self):
        df = pd.DataFrame({"sex": pd.Series([], dtype=str)})
        result = summarize_deaths(df, by="sex")
        assert result.empty
        assert list(result.columns) == ["sex", "n"]