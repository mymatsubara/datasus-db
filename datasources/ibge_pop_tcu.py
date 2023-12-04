import datasus
import polars as pl
import os.path as path
from pl_utils import to_schema, Column
from dbfread import DBF
import dbf
import ftp
import logging
import utils

MAIN_TABLE = "IBGE_POP_TCU"


def import_ibge_pop_tcu(db_file="datasus.db", years=["*"]):
    """
    Import population estimated per city by TCU (Tribunal de Contas da União).

    Args:
        `db_file (str)`: path to the duckdb file in which the data will be imported to.

        `years (list[str])`: list of years for which data will be imported (if available). Eg: `[2012, 2000, 2010]`
    """
    logging.info(f"⏳ [{MAIN_TABLE}] Starting import...")

    datasus.import_from_ftp(
        [MAIN_TABLE],
        [
            f"/dissemin/publicos/IBGE/POPTCU/POPTBR{utils.format_year(year)}.zip*"
            for year in years
        ],
        fetch_ibge_pop_tcu,
        db_file,
    )


def fetch_ibge_pop_tcu(ftp_path: str):
    dbf_file = path.basename(ftp_path).split(".")[0] + ".dbf"
    files = ftp.fetch_from_zip(ftp_path, [dbf_file])

    df = dbf.read_as_df(dbf_file, files[dbf_file])

    return {MAIN_TABLE: map_ibge_pop_tcu(df)}


def map_ibge_pop_tcu(df: pl.DataFrame):
    return to_schema(
        df,
        [
            Column("MUNIC_RES", pl.UInt32),
            Column("ANO", pl.UInt16),
            Column("POPULACAO", pl.UInt32),
        ],
    ).with_columns(
        pl.when(pl.col("MUNIC_RES") >= 1_000_000)
        .then(pl.col("MUNIC_RES") // 10)
        .otherwise(pl.col("MUNIC_RES"))
        .name.keep()
    )
