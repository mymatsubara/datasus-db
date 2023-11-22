import polars as pl
from pl_utils import upsert_column
import datasus
import ftp

TABLE_NAME = "SIH_RD_SISTEMA_INFORMACOES_HOSPITALARES"


def import_sih_rh():
    print(f"⏳ [{TABLE_NAME}] Starting import...")

    datasus.import_from_ftp(
        [TABLE_NAME],
        "/dissemin/publicos/SIHSUS/200801_/Dados/RD*.dbc",
        fetch_sih_rh,
    )


def fetch_sih_rh(ftp_path: str):
    df = ftp.fetch_dbc_as_df(ftp_path)
    return {TABLE_NAME: map_sih_rd(df)}


def map_sih_rd(df: pl.DataFrame):
    return (
        df.with_columns(
            pl.when(pl.col(pl.Utf8).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(pl.Utf8))
            .name.keep(),
            pl.when(pl.col("IDADE").gt(200))
            .then(None)
            .otherwise(pl.col("IDADE"))
            .name.keep(),
        )
        .with_columns(
            pl.when(pl.col("GESTOR_CPF").cast(pl.UInt64) == 0)
            .then(None)
            .otherwise(pl.col("GESTOR_CPF").str.lstrip("0"))
            .name.keep(),
            pl.when(pl.col("INSC_PN").str.contains("[1-9]"))
            .then(pl.col("INSC_PN").str.lstrip("0"))
            .otherwise(None)
            .name.keep(),
        )
        .with_columns(
            [
                pl.col("UF_ZI").cast(pl.UInt32),
                pl.col("ANO_CMPT").cast(pl.UInt16),
                pl.col("MES_CMPT").cast(pl.UInt8),
                pl.col("ESPEC").cast(pl.UInt8),
                upsert_column(df, "CGC_HOSP", pl.Utf8),
                upsert_column(df, "N_AIH", pl.Utf8),
                pl.col("IDENT").cast(pl.UInt8),
                upsert_column(df, "CEP", pl.Utf8),
                pl.col("MUNIC_RES").cast(pl.UInt32),
                pl.col("NASC").str.to_date("%Y%m%d"),
                pl.col("SEXO").cast(pl.UInt8),
                pl.col("UTI_MES_IN").cast(pl.UInt8),
                pl.col("UTI_MES_AN").cast(pl.UInt8),
                pl.col("UTI_MES_AL").cast(pl.UInt8),
                pl.col("UTI_MES_TO").cast(pl.UInt16),
                pl.col("MARCA_UTI").cast(pl.UInt8),
                pl.col("UTI_INT_IN").cast(pl.UInt8),
                pl.col("UTI_INT_AN").cast(pl.UInt8),
                pl.col("UTI_INT_AL").cast(pl.UInt8),
                pl.col("UTI_INT_TO").cast(pl.UInt16),
                pl.col("DIAR_ACOM").cast(pl.UInt16),
                pl.col("QT_DIARIAS").cast(pl.Int32),
                pl.col("PROC_SOLIC").cast(pl.Utf8),
                pl.col("PROC_REA").cast(pl.Utf8),
                pl.col("VAL_SH").cast(pl.Float64),
                pl.col("VAL_SP").cast(pl.Float64),
                pl.col("VAL_SADT").cast(pl.Float64),
                pl.col("VAL_RN").cast(pl.Float64),
                pl.col("VAL_ACOMP").cast(pl.Float64),
                pl.col("VAL_ORTP").cast(pl.Float64),
                pl.col("VAL_SANGUE").cast(pl.Float64),
                pl.col("VAL_SADTSR").cast(pl.Float64),
                pl.col("VAL_TRANSP").cast(pl.Float64),
                pl.col("VAL_OBSANG").cast(pl.Float64),
                pl.col("VAL_PED1AC").cast(pl.Float64),
                pl.col("VAL_TOT").cast(pl.Float64),
                pl.col("VAL_UTI").cast(pl.Float64),
                pl.col("US_TOT").cast(pl.Float64),
                pl.col("DT_INTER").str.to_date("%Y%m%d"),
                pl.col("DT_SAIDA").str.to_date("%Y%m%d"),
                pl.col("DIAG_PRINC").cast(pl.Utf8),
                pl.col("DIAG_SECUN").cast(pl.Utf8),
                pl.col("COBRANCA").cast(pl.UInt8),
                pl.col("NATUREZA").cast(pl.UInt8),
                upsert_column(df, "NAT_JUR", pl.Utf8),
                pl.col("GESTAO").cast(pl.UInt8),
                pl.col("RUBRICA").cast(pl.Utf8),
                pl.col("IND_VDRL").cast(pl.UInt8),
                pl.col("MUNIC_MOV").cast(pl.UInt32),
                pl.col("COD_IDADE").cast(pl.UInt8),
                pl.col("IDADE").cast(pl.UInt8),
                pl.col("DIAS_PERM").cast(pl.UInt16),
                pl.col("MORTE").cast(pl.Boolean),
                pl.col("NACIONAL").cast(pl.UInt16),
                pl.col("NUM_PROC").cast(pl.UInt16),
                pl.col("CAR_INT").cast(pl.UInt8),
                pl.col("TOT_PT_SP").cast(pl.UInt32),
                pl.col("HOMONIMO").cast(pl.UInt8),
                pl.col("NUM_FILHOS").cast(pl.UInt8),
                pl.col("INSTRU").cast(pl.UInt8),
                pl.col("CID_NOTIF").cast(pl.Utf8),
                pl.col("CONTRACEP1").cast(pl.UInt8),
                pl.col("CONTRACEP2").cast(pl.UInt8),
                pl.col("GESTRISCO").cast(pl.UInt8),
                pl.col("INSC_PN").cast(pl.Utf8),
                pl.col("SEQ_AIH5").cast(pl.Utf8),
                pl.col("CBOR").cast(pl.Utf8),
                pl.col("CNAER").cast(pl.UInt16),
                pl.col("VINCPREV").cast(pl.UInt8),
                pl.col("GESTOR_COD").cast(pl.UInt16),
                pl.col("GESTOR_TP").cast(pl.UInt8),
                pl.col("GESTOR_CPF").cast(pl.Utf8),
                pl.col("GESTOR_DT").str.to_date("%Y%m%d"),
                pl.col("CNES").cast(pl.Utf8),
                pl.col("CNPJ_MANT").cast(pl.Utf8),
                pl.col("INFEHOSP").cast(pl.UInt8),
                pl.col("CID_ASSO").cast(pl.Utf8),
                pl.col("CID_MORTE").cast(pl.Utf8),
                pl.col("COMPLEX").cast(pl.UInt8),
                pl.col("FINANC").cast(pl.UInt8),
                pl.col("FAEC_TP").cast(pl.UInt32),
                pl.col("REGCT").cast(pl.UInt16),
                pl.col("RACA_COR").cast(pl.UInt16),
                pl.col("ETNIA").cast(pl.UInt16),
                pl.col("SEQUENCIA").cast(pl.UInt64),
                upsert_column(df, "AUD_JUST", pl.Utf8),
                upsert_column(df, "SIS_JUST", pl.Utf8),
                upsert_column(df, "VAL_SH_FED", pl.Float64),
                upsert_column(df, "VAL_SP_FED", pl.Float64),
                upsert_column(df, "VAL_SH_GES", pl.Float64),
                upsert_column(df, "VAL_SP_GES", pl.Float64),
                upsert_column(df, "VAL_UCI", pl.Float64),
                upsert_column(df, "MARCA_UCI", pl.UInt8),
                upsert_column(df, "DIAGSEC1", pl.Utf8),
                upsert_column(df, "DIAGSEC2", pl.Utf8),
                upsert_column(df, "DIAGSEC3", pl.Utf8),
                upsert_column(df, "DIAGSEC4", pl.Utf8),
                upsert_column(df, "DIAGSEC5", pl.Utf8),
                upsert_column(df, "DIAGSEC6", pl.Utf8),
                upsert_column(df, "DIAGSEC7", pl.Utf8),
                upsert_column(df, "DIAGSEC8", pl.Utf8),
                upsert_column(df, "DIAGSEC9", pl.Utf8),
                upsert_column(df, "TPDISEC1", pl.UInt8),
                upsert_column(df, "TPDISEC2", pl.UInt8),
                upsert_column(df, "TPDISEC3", pl.UInt8),
                upsert_column(df, "TPDISEC4", pl.UInt8),
                upsert_column(df, "TPDISEC5", pl.UInt8),
                upsert_column(df, "TPDISEC6", pl.UInt8),
                upsert_column(df, "TPDISEC7", pl.UInt8),
                upsert_column(df, "TPDISEC8", pl.UInt8),
                upsert_column(df, "TPDISEC9", pl.UInt8),
            ]
        )
    )
