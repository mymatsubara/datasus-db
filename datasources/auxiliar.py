import datasus
import polars as pl
import ftp
import cnv

MUNICIPIO_TABLE = "AUX_MUNICIPIO"
UF_TABLE = "AUX_UF"


def import_auxiliar_tables():
    datasus.import_from_ftp(
        [MUNICIPIO_TABLE, UF_TABLE],
        "/dissemin/publicos/PAINEL_ONCOLOGIA/Auxiliar/PAINEL_ONCOLOGIA.zip*",
        fetch_painel_oncologia_auxiliar,
    )


def fetch_painel_oncologia_auxiliar(ftp_path: str):
    municipio_file = "CNV/br_municip.cnv"
    uf_file = "CNV/br_uf.cnv"

    files = ftp.fetch_from_zip(ftp_path, [municipio_file, uf_file])

    def read_as_df(file_name: str, id_dtype: pl.UInt32):
        cnv_bytes = files[file_name]
        df = cnv.to_dataframe(cnv_bytes, id_dtype=id_dtype)
        return df.with_columns(
            pl.col("NOME").str.split(" ").list.slice(1).list.join(" ")
        )

    return {
        MUNICIPIO_TABLE: read_as_df(municipio_file, id_dtype=pl.UInt32),
        UF_TABLE: read_as_df(uf_file, id_dtype=pl.UInt8),
    }
