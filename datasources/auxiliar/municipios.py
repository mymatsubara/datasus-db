import datasus
import polars as pl
import ftp
import cnv

MAIN_TABLE = "AUX_MUNICIPIO"


def import_municipios():
    datasus.import_from_ftp(
        [MAIN_TABLE],
        "/dissemin/publicos/PAINEL_ONCOLOGIA/Auxiliar/PAINEL_ONCOLOGIA.zip*",
        fetch_municipios,
    )


def fetch_municipios(ftp_path: str):
    cnv_file = "CNV/br_municip.cnv"
    files = ftp.fetch_from_zip(ftp_path, [cnv_file])
    cnv_bytes = files[cnv_file]

    df = cnv.to_dataframe(cnv_bytes)
    df = df.with_columns(pl.col("NOME").str.split(" ").list.slice(1).list.join(" "))

    return {MAIN_TABLE: df}
