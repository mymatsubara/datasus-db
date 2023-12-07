"""
Module with functions to deal with DATASUS convetion files (*.cnv), which are usually file which maps ids to a readable names.
"""

import io
import re
import polars as pl
from .pl_utils import Column, to_schema


def to_dataframe(cnv_bytes: bytes, id_dtype=pl.UInt32):
    df = pl.DataFrame(
        parse_from_bytes(cnv_bytes, encoding="latin-1"),
    ).rename({"column_0": "ID", "column_1": "NOME"})

    return to_schema(df, [Column("ID", id_dtype), Column("NOME", pl.Utf8)])


def parse_from_bytes(
    bytes: bytes,
    encoding="utf-8",
    skip_rows=1,
    delimiter=r"\s{2,}",
    id_idx=3,
    label_idx=2,
):
    file = io.TextIOWrapper(io.BytesIO(bytes), encoding=encoding)
    return parse(
        file,
        skip_rows=skip_rows,
        delimiter=delimiter,
        id_idx=id_idx,
        label_idx=label_idx,
    )


def parse(
    file: io.TextIOWrapper, skip_rows=1, delimiter=r"\s{2,}", id_idx=3, label_idx=2
):
    for i, row in enumerate(file):
        if i < skip_rows:
            continue

        split = re.split(delimiter, row)
        id, label = split[id_idx].split(",")[0], split[label_idx]
        yield (id, label)
