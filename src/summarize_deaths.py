import pandas as pd


def summarize_deaths(*args, by: list[str] | str, **kwargs) -> pd.DataFrame:
    """
    Summarize deaths by one or more grouping columns.

    Groups a CDC mortality dataset by the specified column(s) and returns a
    DataFrame with death counts for each group, sorted in descending order.
    Optionally accepts multiple named datasets to compare across years.

    Parameters
    ----------
    *args : pd.DataFrame
        A single unnamed DataFrame for single-year usage.
    by : str or list of str
        Column name(s) to group by, e.g. "sex" or ["sex", "race_recode3"].
    **kwargs : pd.DataFrame
        Named DataFrames for multi-year comparison,
        e.g. mort1969=mort1969, mort1970=mort1970.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the grouping columns, a `year` column (when multiple
        datasets are provided), and an `n` column containing death counts,
        sorted descending by `n`.

    Examples
    --------
    # Single year
    summarize_deaths(mort1969, by="sex")

    # Multiple years
    summarize_deaths(mort1969=mort1969, mort1970=mort1970, by="sex")
    """
    if isinstance(by, str):
        by = [by]

    if len(args) == 0 and len(kwargs) == 0:
        raise ValueError("At least one DataFrame must be provided.")

    if len(args) > 0 and len(kwargs) > 0:
        raise ValueError(
            "Provide either a single positional DataFrame or named DataFrames, not both."
        )

    # Single dataset — original behavior
    if len(args) == 1:
        df = args[0]
        missing_cols = [col for col in by if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"The following columns were not found in the data: {', '.join(missing_cols)}"
            )

        return (
            df.groupby(by)
            .size()
            .reset_index(name="n")
            .sort_values("n", ascending=False)
            .reset_index(drop=True)
        )

    # Multiple datasets — compare across years
    if len(args) > 1:
        raise ValueError(
            "When providing multiple datasets, all must be named, "
            "e.g. summarize_deaths(mort1969=mort1969, mort1970=mort1970, by='sex')."
        )

    frames = []
    for name, df in kwargs.items():
        missing_cols = [col for col in by if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"The following columns were not found in '{name}': {', '.join(missing_cols)}"
            )

        summary = (
            df.groupby(by)
            .size()
            .reset_index(name="n")
        )
        summary.insert(0, "year", name)
        frames.append(summary)

    combined = pd.concat(frames, ignore_index=True)
    return (
        combined
        .sort_values(["year", "n"], ascending=[True, False])
        .reset_index(drop=True)
    )