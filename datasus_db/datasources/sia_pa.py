import polars as pl
import logging
from ..pl_utils import to_schema, Column, DateColumn
from ..datasus import import_from_ftp
from ..utils import format_year, format_month
from ..ftp import fetch_dbc_as_df

MAIN_TABLE = "SIA_PA"


def import_sia_pa(db_file="datasus.db", years=["*"], states=["*"], months=["*"]):
    """Import PA (Produção Ambulatorial) from SIASUS (Sistema de Informações Ambulatorial do SUS).

    Args:
        db_file (str, optional): path to the duckdb file in which the data will be imported to. Defaults to "datasus.db".
        years (list, optional): list of years for which data will be imported (if available). Eg: `[2012, 2000, 2010]`. Defaults to ["*"].
        states (list, optional): list of brazilian 2 letters state for which data will be imported (if available). Eg: `["SP", "RJ"]`. Defaults to ["*"].
        months (list, optional): list of months numbers (1-12) for which data will be imported (if available). Eg: `[1, 12, 6]`. Defaults to ["*"].

    ---

    Extra:
    - **Data description**: https://github.com/mymatsubara/datasus-db/blob/main/docs/sia_pa.pdf
    - **ftp path**: ftp.datasus.gov.br/dissemin/publicos/SIASUS/200801_/Dados/PA*.dbc
    """
    logging.info(f"⏳ [{MAIN_TABLE}] Starting import...")
    import_from_ftp(
        [MAIN_TABLE],
        [
            f"/dissemin/publicos/SIASUS/200801_/Dados/PA{state.upper()}{format_year(year)}{format_month(month)}*.dbc"
            for year in years
            for state in states
            for month in months
        ],
        fetch_sia_rh,
        db_file=db_file,
    )


def fetch_sia_rh(ftp_path: str):
    df = fetch_dbc_as_df(ftp_path)
    return {MAIN_TABLE: map_sia_pa(df)}


def map_sia_pa(df: pl.DataFrame):
    df = df.with_columns(
        pl.when(pl.col(pl.Utf8).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(pl.Utf8))
        .name.keep(),
    )

    return to_schema(
        df,
        [
            Column("PA_CODUNI", pl.Utf8),
            Column("PA_GESTAO", pl.Utf8),
            Column("PA_CONDIC", pl.Utf8),
            Column("PA_UFMUN", pl.Utf8),
            Column("PA_REGCT", pl.Utf8),
            Column("PA_INCOUT", pl.Utf8),
            Column("PA_INCURG", pl.Utf8),
            Column("PA_TPUPS", pl.Utf8),
            Column("PA_TIPPRE", pl.Utf8),
            Column("PA_MN_IND", pl.Utf8),
            Column("PA_CNPJCPF", pl.Utf8),
            Column("PA_CNPJMNT", pl.Utf8),
            Column("PA_CNPJ_CC", pl.Utf8),
            Column("PA_MVM", pl.Utf8),
            Column("PA_CMP", pl.Utf8),
            Column("PA_PROC_ID", pl.Utf8),
            Column("PA_TPFIN", pl.Utf8),
            Column("PA_SUBFIN", pl.Utf8),
            Column("PA_NIVCPL", pl.Utf8),
            Column("PA_DOCORIG", pl.Utf8),
            Column("PA_AUTORIZ", pl.Utf8),
            Column("PA_CNSMED", pl.Utf8),
            Column("PA_CBOCOD", pl.Utf8),
            Column("PA_MOTSAI", pl.Utf8),
            Column("PA_OBITO", pl.Utf8),
            Column("PA_ENCERR", pl.Utf8),
            Column("PA_PERMAN", pl.Utf8),
            Column("PA_ALTA", pl.Utf8),
            Column("PA_TRANSF", pl.Utf8),
            Column("PA_CIDPRI", pl.Utf8),
            Column("PA_CIDSEC", pl.Utf8),
            Column("PA_CIDCAS", pl.Utf8),
            Column("PA_CATEND", pl.Utf8),
            Column("PA_IDADE", pl.Utf8),
            Column("IDADEMIN", pl.Utf8),
            Column("IDADEMAX", pl.Utf8),
            Column("PA_FLIDADE", pl.Utf8),
            Column("PA_SEXO", pl.Utf8),
            Column("PA_RACACOR", pl.Utf8),
            Column("PA_MUNPCN", pl.Utf8),
            Column("PA_QTDPRO", pl.Utf8),
            Column("PA_QTDAPR", pl.Utf8),
            Column("PA_VALPRO", pl.Utf8),
            Column("PA_VALAPR", pl.Utf8),
            Column("PA_UFDIF", pl.Utf8),
            Column("PA_MNDIF", pl.Utf8),
            Column("PA_DIF_VAL", pl.Utf8),
            Column("NU_VPA_TOT", pl.Utf8),
            Column("NU_PA_TOT", pl.Utf8),
            Column("PA_INDICA", pl.Utf8),
            Column("PA_CODOCO", pl.Utf8),
            Column("PA_FLQT", pl.Utf8),
            Column("PA_FLER", pl.Utf8),
            Column("PA_ETNIA", pl.Utf8),
            Column("PA_VL_CF", pl.Float64),
            Column("PA_VL_CL", pl.Float64),
            Column("PA_VL_INC", pl.Float64),
            Column("PA_SRV_C", pl.Utf8),
            Column("PA_INE", pl.Utf8),
            Column("PA_NAT_JUR", pl.Utf8),
        ],
    )
