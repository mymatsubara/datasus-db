import polars as pl
import datasus
import ftp

MAIN_TABLE = "PO_PAINEL_ONCOLOGIA"


def import_po():
    print(f"⏳ [{MAIN_TABLE}] Starting import...")

    datasus.import_from_ftp(
        [MAIN_TABLE],
        "/dissemin/publicos/PAINEL_ONCOLOGIA/DADOS/POBR*.dbc",
        fetch_po,
    )


def fetch_po(ftp_path: str):
    df = ftp.fetch_dbc_as_df(ftp_path)
    return {MAIN_TABLE: map_po(df)}


def map_po(df: pl.DataFrame):
    return (
        df.with_columns(
            [
                pl.when(pl.col(pl.Utf8).str.len_chars() == 0)
                .then(None)
                .otherwise(pl.col(pl.Utf8))
                .name.keep(),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("IDADE").cast(pl.UInt16).gt(200))
                .then(None)
                .otherwise(pl.col("IDADE"))
                .name.keep(),
            ]
        )
        .with_columns(
            [
                pl.col("ANO_DIAGN").cast(pl.UInt16),
                pl.col("ANO_TRATAM").cast(pl.UInt16),
                pl.col("UF_RESID").cast(pl.UInt8),
                pl.col("MUN_RESID").cast(pl.UInt32),
                pl.col("UF_TRATAM").cast(pl.UInt8),
                pl.col("MUN_TRATAM").cast(pl.UInt32),
                pl.col("UF_DIAGN").cast(pl.UInt8),
                pl.col("MUN_DIAG").cast(pl.UInt32),
                pl.col("TRATAMENTO").cast(pl.UInt8),
                pl.col("DIAGNOSTIC").cast(pl.UInt8),
                pl.col("IDADE").cast(pl.UInt8),
                pl.col("ESTADIAM").cast(pl.UInt8),
                pl.col("CNES_DIAG").cast(pl.UInt32),
                pl.col("CNES_TRAT").cast(pl.UInt32),
                pl.col("DT_DIAG").str.to_date(),
                pl.col("DT_TRAT").str.to_date(),
                pl.col("DT_NASC").str.to_date(),
            ]
        )
    )
