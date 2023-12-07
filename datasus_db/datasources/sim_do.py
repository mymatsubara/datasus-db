import polars as pl
import logging
from ..pl_utils import (
    upsert_column,
    to_schema,
    Column,
    DateColumn,
    rename_columns,
    fill_empty,
    fill_text,
    fill_non_numeric,
)
from ..datasus import import_from_ftp
from ..utils import format_year
from ..ftp import fetch_dbc_as_df

MAIN_TABLE = "SIM_DO"


def import_sim_do(db_file="datasus.db", years=["*"], states=["*"]):
    """Import DO (Declaração de Óbito) from SIM (Sistema de informações de Mortalidade).

    Args:
        db_file (str, optional): path to the duckdb file in which the data will be imported to. Defaults to "datasus.db".
        years (list, optional): list of years for which data will be imported (if available). Eg: `[2012, 2000, 2010]`. Defaults to ["*"].
        states (list, optional): list of brazilian 2 letters state for which data will be imported (if available). Eg: `["SP", "RJ"]`. Defaults to ["*"].

    ---

    Extra:
    - **Data description**: https://github.com/mymatsubara/datasus-db/blob/main/docs/sim_do.pdf
    - **ftp path non preliminary data**: ftp.datasus.gov.br/dissemin/publicos/SIM/CID10/DORES/DO*.dbc
    - **ftp path preliminary data**: ftp.datasus.gov.br/dissemin/publicos/SIM/PRELIM/DORES/DO*.dbc
    """
    logging.info(f"⏳ [{MAIN_TABLE}] Starting import for non preliminary data...")
    import_from_ftp(
        [MAIN_TABLE],
        [
            f"/dissemin/publicos/SIM/CID10/DORES/DO{state.upper()}{format_year(year, digits=4)}.dbc*"
            for year in years
            for state in states
        ],
        fetch_sim_do,
        ftp_exclude_regex=r".*/DOBR.*\.dbc",
        db_file=db_file,
    )

    logging.info(f"⏳ [{MAIN_TABLE}] Starting import for preliminary data...")
    import_from_ftp(
        [MAIN_TABLE],
        [
            f"/dissemin/publicos/SIM/PRELIM/DORES/DO{state.upper()}{format_year(year, digits=4)}.dbc*"
            for year in years
            for state in states
        ],
        fetch_sim_do,
        ftp_exclude_regex=r".*/DOBR.*\.dbc",
        db_file=db_file,
    )


def fetch_sim_do(ftp_path: str):
    df = fetch_dbc_as_df(ftp_path)
    return {MAIN_TABLE: map_sim_do(df)}


def map_sim_do(df: pl.DataFrame):
    df = (
        df.with_columns(fill_empty(None))
        .with_columns(
            fill_text("NULL", None),
        )
        .with_columns(
            fill_text("00000000", None),
        )
        .with_columns(
            fill_non_numeric(None, pl.col(["PESO", "NATURAL"])),
        )
    )
    df = rename_columns(df, {"contador": "CONTADOR"})

    df = (
        df.with_columns(upsert_column(df, "DTCADASTRO", pl.Utf8))
        .with_columns(
            pl.when(pl.col("DTOBITO").str.len_chars() == 4)
            .then("0101" + pl.col("DTOBITO"))
            .otherwise(pl.col("DTOBITO"))
            .name.keep(),
            pl.when(pl.col("DTNASC").str.len_chars() == 4)
            .then("0101" + pl.col("DTNASC"))
            .otherwise(pl.col("DTNASC"))
            .name.keep(),
            pl.when(pl.col("DTCADASTRO").str.len_chars() == 7)
            .then("0" + pl.col("DTCADASTRO"))
            .otherwise(pl.col("DTCADASTRO"))
            .name.keep(),
        )
        .with_columns(
            pl.when(pl.col("DTOBITO").str.len_chars() == 6)
            .then("01" + pl.col("DTOBITO"))
            .otherwise(pl.col("DTOBITO"))
            .name.keep(),
            pl.when(pl.col("DTNASC").str.len_chars() == 6)
            .then("01" + pl.col("DTNASC"))
            .otherwise(pl.col("DTNASC"))
            .name.keep(),
        )
    )

    return to_schema(
        df,
        [
            Column("ORIGEM", pl.UInt8),
            Column("TIPOBITO", pl.UInt8),
            DateColumn("DTOBITO", "%d%m%Y", strict=False),
            Column("HORAOBITO", pl.Utf8),
            Column("NATURAL", pl.UInt32),
            Column("CODMUNNATU", pl.UInt32),
            DateColumn("DTNASC", "%d%m%Y", strict=False),
            Column("IDADE", pl.UInt16),
            Column("SEXO", pl.UInt8),
            Column("RACACOR", pl.UInt8),
            Column("ESTCIV", pl.UInt8, strict=False),
            Column("ESC", pl.UInt8, strict=False),
            Column("ESC2010", pl.UInt8, strict=False),
            Column("SERIESCFAL", pl.UInt8),
            Column("OCUP", pl.Utf8),
            Column("CODMUNRES", pl.UInt32),
            Column("LOCOCOR", pl.UInt8),
            Column("CODESTAB", pl.UInt32, strict=False),
            Column("ESTABDESCR", pl.Utf8),
            Column("CODMUNOCOR", pl.UInt32),
            Column("IDADEMAE", pl.UInt16, strict=False),
            Column("ESCMAE", pl.Int8, strict=False),
            Column("ESCMAE2010", pl.Int8, strict=False),
            Column("SERIESCMAE", pl.Int8),
            Column("OCUPMAE", pl.Utf8),
            Column("QTDFILVIVO", pl.UInt8, strict=False),
            Column("QTDFILMORT", pl.UInt8, strict=False),
            Column("GRAVIDEZ", pl.UInt8),
            Column("SEMAGESTAC", pl.UInt8),
            Column("GESTACAO", pl.UInt8, strict=False),
            Column("PARTO", pl.UInt8),
            Column("OBITOPARTO", pl.UInt8),
            Column("PESO", pl.UInt32),
            Column("TPMORTEOCO", pl.UInt8),
            Column("OBITOGRAV", pl.UInt8),
            Column("OBITOPUERP", pl.UInt8),
            Column("ASSISTMED", pl.UInt8),
            Column("EXAME", pl.UInt8),
            Column("CIRURGIA", pl.UInt8),
            Column("NECROPSIA", pl.UInt8),
            Column("LINHAA", pl.Utf8),
            Column("LINHAB", pl.Utf8),
            Column("LINHAC", pl.Utf8),
            Column("LINHAD", pl.Utf8),
            Column("LINHAII", pl.Utf8),
            Column("CAUSABAS", pl.Utf8),
            Column("CB_PRE", pl.Utf8),
            Column("COMUNSVOIM", pl.Utf8),
            DateColumn("DTATESTADO", "%d%m%Y", strict=False),
            Column("CIRCOBITO", pl.UInt8, strict=False),
            Column("ACIDTRAB", pl.UInt8),
            Column("FONTE", pl.Utf8),
            Column("NUMEROLOTE", pl.Utf8),
            Column("TPPOS", pl.Utf8),
            DateColumn("DTINVESTIG", "%d%m%Y"),
            Column("CAUSABAS_O", pl.Utf8),
            DateColumn("DTCADASTRO", "%d%m%Y"),
            Column("ATESTANTE", pl.UInt8),
            Column("STCODIFICA", pl.Utf8),
            Column("CODIFICADO", pl.Utf8),
            Column("VERSAOSIST", pl.Utf8),
            Column("VERSAOSCB", pl.Utf8),
            Column("FONTEINV", pl.Utf8),
            DateColumn("DTRECEBIM", "%d%m%Y"),
            Column("ATESTADO", pl.Utf8),
            DateColumn("DTRECORIGA", "%d%m%Y"),
            Column("CAUSAMAT", pl.Utf8),
            Column("ESCMAEAGR1", pl.UInt8),
            Column("ESCFALAGR1", pl.UInt8),
            Column("STDOEPIDEM", pl.UInt8),
            Column("STDONOVA", pl.UInt8),
            Column("DIFDATA", pl.UInt16),
            Column("NUDIASOBCO", pl.UInt16),
            Column("NUDIASOBIN", pl.UInt16),
            DateColumn("DTCADINV", "%d%m%Y"),
            Column("TPOBITOCOR", pl.UInt8),
            DateColumn("DTCONINV", "%d%m%Y"),
            Column("FONTES", pl.Utf8),
            Column("TPRESGINFO", pl.UInt8),
            Column("TPNIVELINV", pl.Utf8),
            Column("NUDIASINF", pl.UInt16),
            DateColumn("DTCADINF", "%d%m%Y"),
            Column("MORTEPARTO", pl.UInt8),
            DateColumn("DTCONCASO", "%d%m%Y"),
            Column("FONTESINF", pl.Utf8),
            Column("ALTCAUSA", pl.UInt8),
            Column("CONTADOR", pl.UInt32),
        ],
    )
