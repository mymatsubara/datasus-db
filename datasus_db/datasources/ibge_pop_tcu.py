import polars as pl
import os.path as path
import logging
from ..pl_utils import to_schema, Column
from ..dbf import read_as_df
from ..datasus import import_from_ftp
from ..utils import format_year
from ..ftp import fetch_from_zip

MAIN_TABLE = "IBGE_POP_TCU"


def import_ibge_pop_tcu(db_file="datasus.db", years=["*"]):
    """Import population estimated per city by TCU (Tribunal de Contas da União).

    Args:
        db_file (str, optional): path to the duckdb file in which the data will be imported to. Defaults to "datasus.db".
        years (list, optional): list of years for which data will be imported (if available). Eg: `[2012, 2000, 2010]`. Defaults to ["*"].

    ---

    Extra:
    - **Data description**: https://github.com/mymatsubara/datasus-db/blob/main/docs/ibge_pop_tcu.pdf
    - **ftp path**: ftp.datasus.gov.br/dissemin/publicos/IBGE/POPTCU/POPTBR*.zip
    """
    logging.info(f"⏳ [{MAIN_TABLE}] Starting import...")

    import_from_ftp(
        [MAIN_TABLE],
        [
            f"/dissemin/publicos/IBGE/POPTCU/POPTBR{format_year(year)}.zip*"
            for year in years
        ],
        fetch_ibge_pop_tcu,
        db_file,
    )


def fetch_ibge_pop_tcu(ftp_path: str):
    dbf_file = path.basename(ftp_path).split(".")[0] + ".dbf"
    files = fetch_from_zip(ftp_path, [dbf_file])

    df = read_as_df(dbf_file, files[dbf_file])

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
