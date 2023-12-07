"""
Module with helper functions to handler with *.dbf files
"""
import os.path as path
import polars as pl
from dbfread import DBF
from .utils import rm


def read_as_df(filename: str, bytes: bytes, encoding: str = None):
    tmp_file = path.join(".tmp", path.basename(filename).split(".")[0] + ".dbf")

    with open(tmp_file, "wb") as f:
        f.write(bytes)

    try:
        dbf = DBF(tmp_file, encoding=encoding)
        df = pl.DataFrame(iter(dbf))
        return df
    except Exception as e:
        raise e
    finally:
        rm(tmp_file)
