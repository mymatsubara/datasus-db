"""
Module with helper functions to work with polars dataframes.
"""

import polars as pl
from dataclasses import dataclass


@dataclass
class Column:
    name: str
    dtype: pl.PolarsDataType
    strict: bool = True

    def upsert(self, df: pl.DataFrame):
        return upsert_column(df, self.name, self.dtype, strict=self.strict)


@dataclass
class DateColumn:
    name: str
    format: str
    strict: bool = True

    def upsert(self, df: pl.DataFrame):
        return upsert_date_column(df, self.name, self.format, strict=self.strict)


def upsert_column(df: pl.DataFrame, name: str, dtype: pl.PolarsDataType, strict=True):
    if name in df.columns:
        return pl.col(name).cast(dtype, strict=strict)
    else:
        return pl.lit(None, dtype).alias(name)


def upsert_date_column(df: pl.DataFrame, name: str, format: str, strict=True):
    dtype = pl.Date
    if name in df.columns:
        return pl.col(name).str.to_date(format, strict=strict)
    else:
        return pl.lit(None, dtype).alias(name)


def to_schema(df: pl.DataFrame, schema: list[Column]):
    schema_cols = {col.name for col in schema}
    cols_to_remove = [col for col in df.columns if not col in schema_cols]
    df = df.drop(cols_to_remove)

    return df.with_columns([col.upsert(df) for col in schema])


def rename_columns(df: pl.DataFrame, mapping: dict[str, str]):
    cur_cols = set(df.columns)
    mapping_possible = {
        col: mapping[col] for (col) in mapping.keys() if col in cur_cols
    }

    if len(mapping_possible) == 0:
        return df
    else:
        return df.rename(mapping_possible)


def fill_empty(fill: object, col=pl.col(pl.Utf8)):
    return pl.when(col.str.len_chars() == 0).then(fill).otherwise(col).name.keep()


def fill_text(match: str, fill: object, col=pl.col(pl.Utf8)):
    return pl.when(col == match).then(fill).otherwise(col).name.keep()


def fill_non_numeric(fill: object, col: pl.Expr):
    return pl.when(col.str.contains(r"\D")).then(fill).otherwise(col).name.keep()
