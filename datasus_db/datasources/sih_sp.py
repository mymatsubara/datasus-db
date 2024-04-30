import polars as pl
import logging
from ..pl_utils import to_schema, Column, DateColumn
from ..datasus import import_from_ftp
from ..utils import format_year, format_month
from ..ftp import fetch_dbc_as_df

MAIN_TABLE = "SIH_SP"


def import_sih_sp(db_file="datasus.db", years=["*"], states=["*"], months=["*"]):
    """Import SP (Autorização de Internação Hospitalar Saúde do Profissional) from SIHSUS (Sistema de Informações Hospitalares do SUS).

    Args:
        db_file (str, optional): path to the duckdb file in which the data will be imported to. Defaults to "datasus.db".
        years (list, optional): list of years for which data will be imported (if available). Eg: `[2012, 2000, 2010]`. Defaults to ["*"].
        states (list, optional): list of brazilian 2 letters state for which data will be imported (if available). Eg: `["SP", "RJ"]`. Defaults to ["*"].
        months (list, optional): list of months numbers (1-12) for which data will be imported (if available). Eg: `[1, 12, 6]`. Defaults to ["*"].

    ---

    Extra:
    - **Data description**: https://github.com/mymatsubara/datasus-db/blob/main/docs/sih_sp.pdf
    - **ftp path**: ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/SP*.dbc
    """
    logging.info(f"⏳ [{MAIN_TABLE}] Starting import...")

    import_from_ftp(
        [MAIN_TABLE],
        [
            f"/dissemin/publicos/SIHSUS/200801_/Dados/SP{state.upper()}{format_year(year)}{format_month(month)}.dbc*"
            for year in years
            for state in states
            for month in months
        ],
        fetch_sih_rh,
        db_file=db_file,
    )


def fetch_sih_rh(ftp_path: str):
    df = fetch_dbc_as_df(ftp_path)
    return {MAIN_TABLE: map_sih_sp(df)}


def map_sih_sp(df: pl.DataFrame):
    df = df.with_columns(
        pl.when(pl.col(pl.Utf8).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(pl.Utf8))
        .name.keep(),
    )

    return to_schema(
        df,
        [
            Column("SP_GESTOR", pl.Utf8),
            Column("SP_UF", pl.Utf8),
            Column("SP_AA", pl.Utf8),
            Column("SP_MM", pl.Utf8),
            Column("SP_CNES", pl.Utf8),
            Column("SP_NAIH", pl.Utf8),
            Column("SP_PROCREA", pl.Utf8),
            Column("SP_DTINTER", pl.Utf8),
            Column("SP_DTSAIDA", pl.Utf8),
            Column("SP_NUM_PR", pl.Utf8),
            Column("SP_TIPO", pl.Utf8),
            Column("SP_CPFCGC", pl.Utf8),
            Column("SP_ATOPROF", pl.Utf8),
            Column("SP_TP_ATO", pl.Utf8),
            Column("SP_QTD_ATO", pl.Utf8),
            Column("SP_PTSP", pl.Utf8),
            Column("SP_NF", pl.Utf8),
            Column("SP_VALATO", pl.Utf8),
            Column("SP_M_HOSP", pl.Utf8),
            Column("SP_M_PAC", pl.Utf8),
            Column("SP_DES_HOS", pl.Utf8),
            Column("SP_DES_PAC", pl.Utf8),
            Column("SP_COMPLEX", pl.Utf8),
            Column("SP_FINANC", pl.Utf8),
            Column("SP_CO_FAEC", pl.Utf8),
            Column("SP_PF_CBO", pl.Utf8),
            Column("SP_PF_DOC", pl.Utf8),
            Column("SP_PJ_DOC", pl.Utf8),
            Column("IN_TP_VAL", pl.Utf8),
            Column("SEQUENCIA", pl.Utf8),
            Column("REMESSA", pl.Utf8),
            Column("SERV_CLA", pl.Utf8),
            Column("SP_CIDPRI", pl.Utf8),
            Column("SP_CIDSEC", pl.Utf8),
            Column("SP_QT_PROC", pl.Utf8),
            Column("SP_U_AIH", pl.Utf8),
        ],
    )
