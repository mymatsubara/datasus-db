import polars as pl


def upsert_column(df: pl.DataFrame, name: str, dtype: pl.PolarsDataType):
    if name in df.columns:
        return pl.col(name).cast(dtype)
    else:
        return pl.lit(None, dtype).alias(name)
