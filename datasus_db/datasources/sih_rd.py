import polars as pl
import logging
from ..pl_utils import to_schema, Column, DateColumn
from ..datasus import import_from_ftp
from ..utils import format_year, format_month
from ..ftp import fetch_dbc_as_df

MAIN_TABLE = "SIH_RD"


def import_sih_rd(db_file="datasus.db", years=["*"], states=["*"], months=["*"]):
    """Import RD (Autorização de Internação Hospitalar Reduzida) from SIMSUS (Sistema de Informações Hospitalares do SUS).

    Args:
        db_file (str, optional): path to the duckdb file in which the data will be imported to. Defaults to "datasus.db".
        years (list, optional): list of years for which data will be imported (if available). Eg: `[2012, 2000, 2010]`. Defaults to ["*"].
        states (list, optional): list of brazilian 2 letters state for which data will be imported (if available). Eg: `["SP", "RJ"]`. Defaults to ["*"].
        months (list, optional): list of months numbers (1-12) for which data will be imported (if available). Eg: `[1, 12, 6]`. Defaults to ["*"].

    ---

    Extra:
    - **Data description**: https://github.com/mymatsubara/datasus-db/blob/main/docs/sih_rd.pdf
    - **ftp path**: ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/RD*.dbc
    """
    logging.info(f"⏳ [{MAIN_TABLE}] Starting import...")

    import_from_ftp(
        [MAIN_TABLE],
        [
            f"/dissemin/publicos/SIHSUS/200801_/Dados/RD{state.upper()}{format_year(year)}{format_month(month)}.dbc*"
            for year in years
            for state in states
            for month in months
        ],
        fetch_sih_rh,
        db_file=db_file,
    )


def fetch_sih_rh(ftp_path: str):
    df = fetch_dbc_as_df(ftp_path)
    return {MAIN_TABLE: map_sih_rd(df)}


def map_sih_rd(df: pl.DataFrame):
    df = df.with_columns(
        pl.when(pl.col(pl.Utf8).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(pl.Utf8))
        .name.keep(),
    ).with_columns(
        pl.when(pl.col("GESTOR_CPF").str.contains("[1-9]"))
        .then(pl.col("GESTOR_CPF"))
        .otherwise(None)
        .name.keep(),
        pl.when(pl.col("INSC_PN").str.contains("[1-9]"))
        .then(pl.col("INSC_PN"))
        .otherwise(None)
        .name.keep(),
    )

    return to_schema(
        df,
        [
            Column("UF_ZI", pl.Utf8),
            Column("ANO_CMPT", pl.Utf8),
            Column("MES_CMPT", pl.Utf8),
            Column("ESPEC", pl.Utf8),
            Column("CGC_HOSP", pl.Utf8),
            Column("N_AIH", pl.Utf8),
            Column("IDENT", pl.Utf8),
            Column("CEP", pl.Utf8),
            Column("MUNIC_RES", pl.Utf8),
            Column("NASC", pl.Utf8),
            Column("SEXO", pl.Utf8),
            Column("UTI_MES_IN", pl.Utf8),
            Column("UTI_MES_AN", pl.Utf8),
            Column("UTI_MES_AL", pl.Utf8),
            Column("UTI_MES_TO", pl.Utf8),
            Column("UTI_INT_IN", pl.Utf8),
            Column("UTI_INT_AN", pl.Utf8),
            Column("UTI_INT_AL", pl.Utf8),
            Column("UTI_INT_TO", pl.Utf8),
            Column("DIAR_ACOM", pl.Utf8),
            Column("QT_DIARIAS", pl.Utf8),
            Column("PROC_SOLIC", pl.Utf8),
            Column("PROC_REA", pl.Utf8),
            Column("VAL_SH", pl.Float64),
            Column("VAL_SP", pl.Float64),
            Column("VAL_SADT", pl.Float64),
            Column("VAL_RN", pl.Float64),
            Column("VAL_ACOMP", pl.Float64),
            Column("VAL_ORTP", pl.Float64),
            Column("VAL_SANGUE", pl.Float64),
            Column("VAL_SADTSR", pl.Float64),
            Column("VAL_TRANSP", pl.Float64),
            Column("VAL_OBSANG", pl.Float64),
            Column("VAL_PED1AC", pl.Float64),
            Column("VAL_TOT", pl.Float64),
            Column("VAL_UTI", pl.Float64),
            Column("US_TOT", pl.Float64),
            Column("DT_INTER", pl.Utf8),
            Column("DT_SAIDA", pl.Utf8),
            Column("DIAG_PRINC", pl.Utf8),
            Column("DIAG_SECUN", pl.Utf8),
            Column("COBRANCA", pl.Utf8),
            Column("NATUREZA", pl.Utf8),
            Column("NAT_JUR", pl.Utf8),
            Column("GESTAO", pl.Utf8),
            Column("RUBRICA", pl.Utf8),
            Column("IND_VDRL", pl.Utf8),
            Column("MUNIC_MOV", pl.Utf8),
            Column("COD_IDADE", pl.Utf8),
            Column("IDADE", pl.Utf8, strict=False),
            Column("DIAS_PERM", pl.Utf8),
            Column("MORTE", pl.Utf8),
            Column("NACIONAL", pl.Utf8),
            Column("NUM_PROC", pl.Utf8),
            Column("CAR_INT", pl.Utf8),
            Column("TOT_PT_SP", pl.Utf8),
            Column("CPF_AUT", pl.Utf8),
            Column("HOMONIMO", pl.Utf8),
            Column("NUM_FILHOS", pl.Utf8),
            Column("INSTRU", pl.Utf8),
            Column("CID_NOTIF", pl.Utf8),
            Column("CONTRACEP1", pl.Utf8),
            Column("CONTRACEP2", pl.Utf8),
            Column("GESTRISCO", pl.Utf8),
            Column("INSC_PN", pl.Utf8),
            Column("SEQ_AIH5", pl.Utf8),
            Column("CBOR", pl.Utf8),
            Column("CNAER", pl.Utf8),
            Column("VINCPREV", pl.Utf8),
            Column("GESTOR_COD", pl.Utf8),
            Column("GESTOR_TP", pl.Utf8),
            Column("GESTOR_CPF", pl.Utf8),
            Column("GESTOR_DT", pl.Utf8),
            Column("CNES", pl.Utf8),
            Column("CNPJ_MANT", pl.Utf8),
            Column("INFEHOSP", pl.Utf8),
            Column("CID_ASSO", pl.Utf8),
            Column("CID_MORTE", pl.Utf8),
            Column("COMPLEX", pl.UInt8),
            Column("FINANC", pl.UInt8),
            Column("FAEC_TP", pl.Utf8),
            Column("REGCT", pl.Utf8),
            Column("RACA_COR", pl.UInt8),
            Column("ETNIA", pl.Utf8),
            Column("SEQUENCIA", pl.Utf8),
            Column("REMESSA", pl.Utf8),
            Column("AUD_JUST", pl.Utf8),
            Column("SIS_JUST", pl.Utf8),
            Column("VAL_SH_FED", pl.Float64),
            Column("VAL_SP_FED", pl.Float64),
            Column("VAL_SH_GES", pl.Float64),
            Column("VAL_SP_GES", pl.Float64),
            Column("VAL_UCI", pl.Float64),
            Column("MARCA_UCI", pl.UInt8),
            Column("DIAGSEC1", pl.Utf8),
            Column("DIAGSEC2", pl.Utf8),
            Column("DIAGSEC3", pl.Utf8),
            Column("DIAGSEC4", pl.Utf8),
            Column("DIAGSEC5", pl.Utf8),
            Column("DIAGSEC6", pl.Utf8),
            Column("DIAGSEC7", pl.Utf8),
            Column("DIAGSEC8", pl.Utf8),
            Column("DIAGSEC9", pl.Utf8),
            Column("TPDISEC1", pl.UInt8),
            Column("TPDISEC2", pl.UInt8),
            Column("TPDISEC3", pl.UInt8),
            Column("TPDISEC4", pl.UInt8),
            Column("TPDISEC5", pl.UInt8),
            Column("TPDISEC6", pl.UInt8),
            Column("TPDISEC7", pl.UInt8),
            Column("TPDISEC8", pl.UInt8),
            Column("TPDISEC9", pl.UInt8),
        ],
    )
