import polars as pl


def parse_file_size_mb(file_size: pl.Expr, rounding: int = 5) -> pl.Expr:
    """
    Convert a file size expression to megabytes.

    A vectorized Polars expression that parses file size strings containing
    KB, MB, or GB units and converts them to a numeric value in megabytes.

    Args:
        file_size (pl.Expr): A Polars expression resolving to file size strings
            (e.g. "531 KB", "1.8 MB", "1 GB").
        rounding (int): Number of decimal places to round to. Default is 5.

    Returns:
        pl.Expr: A Polars expression resolving to file sizes in megabytes.

    Examples:
        >>> df = pl.DataFrame({"file_size": ["531 KB", "1.8 MB", "1 GB", None]})
        >>> df.with_columns(parse_file_size_mb(pl.col("file_size")).alias("mb"))
    """
    value = file_size.str.extract(r"([\d.]+)", group_index=1).cast(pl.Float64)
    return (
        pl.when(file_size.str.contains("KB"))
        .then(value / 1024)
        .when(file_size.str.contains("MB"))
        .then(value)
        .when(file_size.str.contains("GB"))
        .then(value * 1024)
        .otherwise(None)
        .round(rounding)
    )
