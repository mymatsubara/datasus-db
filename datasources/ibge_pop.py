import datasus
import polars as pl
import os.path as path
from pl_utils import to_schema, Column
import ftp
import logging
from datasources.auxiliar import import_auxiliar_tables
from views.ibge_piramide_etaria import create_piramide_etaria_view

MAIN_TABLE = "IBGE_POP"


def import_ibge_pop(db_file="datasus.db"):
    logging.info(f"⏳ [{MAIN_TABLE}] Starting import...")

    datasus.import_from_ftp(
        [MAIN_TABLE],
        "/dissemin/publicos/IBGE/POP/POPBR*.zip",
        fetch_ibge_pop,
        db_file=db_file,
    )

    import_auxiliar_tables()
    create_piramide_etaria_view()


def fetch_ibge_pop(ftp_path: str):
    csv_file = path.basename(ftp_path).split(".")[0] + ".csv"
    files = ftp.fetch_from_zip(ftp_path, [csv_file])
    df = pl.read_csv(
        files[csv_file],
        schema={
            "MUNIC_RES": pl.UInt32,
            "ANO": pl.UInt32,
            "SEXO": pl.UInt32,
            "SITUACAO": pl.UInt32,
            "FXETARIA": pl.Utf8,
            "POPULACAO": pl.UInt32,
        },
    )

    return {MAIN_TABLE: map_ibge_pop(df)}


def map_ibge_pop(df: pl.DataFrame):
    df = (
        df.with_columns(
            pl.when(pl.col("FXETARIA").is_in(["I000", "R000"]))
            .then("-100")
            .otherwise(pl.col("FXETARIA"))
            .name.keep(),
        )
        .with_columns(
            pl.col("FXETARIA").cast(pl.Int64),
        )
        .with_columns(
            pl.when(pl.col("FXETARIA") == 0)
            .then(0)
            .otherwise(pl.col("FXETARIA") // 100)
            .alias("INICIO_FXETARIA"),
            pl.when(pl.col("FXETARIA") == 0)
            .then(0)
            .otherwise(pl.col("FXETARIA") % 100)
            .alias("FIM_FXETARIA"),
        )
    )

    return to_schema(
        df,
        [
            Column("MUNIC_RES", pl.UInt32),
            Column("ANO", pl.UInt16),
            Column("SEXO", pl.UInt8),
            Column("SITUACAO", pl.UInt8),
            Column("INICIO_FXETARIA", pl.Int8),
            Column("FIM_FXETARIA", pl.Int8),
            Column("POPULACAO", pl.UInt32),
        ],
    )
