import logging
import polars as pl
from ..pl_utils import fill_empty, Column, to_schema
from ..ftp import fetch_from_zip
from ..cnv import to_dataframe
from ..datasus import import_from_ftp
from ..dbf import read_as_df


MUNICIPIO_TABLE = "AUX_MUNICIPIO"
UF_TABLE = "AUX_UF"
CID10_DOENCA_TABLE = "AUX_CID10_DOENCA"


def import_auxiliar_tables(db_file="datasus.db"):
    """Import auxiliar tables with some datasus codes definitions (eg: municipios, doenças, ...)

    Args:
        db_file (str, optional): path to the duckdb file in which the data will be imported to.

    ---

    Extra:
    - **Municipio data description**: https://github.com/mymatsubara/datasus-db/blob/main/docs/auxiliar/municipio.pdf
    - **ftp path**: ftp.datasus.gov.br/dissemin/publicos/SIM/CID10/DOCS/Docs_Tabs_CID10.zip
    """
    logging.info(f"⏳ [AUX_TABLES] Starting import...")

    import_from_ftp(
        [CID10_DOENCA_TABLE, MUNICIPIO_TABLE, UF_TABLE],
        ["/dissemin/publicos/SIM/CID10/DOCS/Docs_Tabs_CID10.zip*"],
        fetch_sim_auxiliar,
        db_file=db_file,
    )


def fetch_sim_auxiliar(ftp_path: str):
    cid10_file = "TABELAS/CID10.DBF"
    municipio_file = "TABELAS/CADMUN.DBF"
    uf_file = "TABELAS/TABUF.DBF"
    files = fetch_from_zip(ftp_path, [cid10_file, municipio_file, uf_file])

    cid10_df = read_as_df(cid10_file, files[cid10_file], encoding="cp850")
    municipio_df = read_as_df(municipio_file, files[municipio_file], encoding="cp850")
    uf_df = read_as_df(uf_file, files[uf_file], encoding="cp850")

    return {
        CID10_DOENCA_TABLE: map_cid10(cid10_df),
        MUNICIPIO_TABLE: map_municipio(municipio_df),
        UF_TABLE: map_uf(uf_df),
    }


def map_cid10(df: pl.DataFrame):
    df = df.with_columns(fill_empty(None))

    return to_schema(
        df,
        [
            Column("CID10", pl.Utf8),
            Column("OPC", pl.Utf8),
            Column("CAT", pl.Utf8),
            Column("SUBCAT", pl.Utf8),
            Column("DESCR", pl.Utf8),
            Column("RESTRSEXO", pl.UInt8),
        ],
    )


def map_municipio(df: pl.DataFrame):
    df = df.with_columns(fill_empty(None))

    return to_schema(
        df,
        [
            Column("MUNCOD", pl.UInt32),
            Column("MUNCODDV", pl.UInt32),
            Column("SITUACAO", pl.Utf8),
            Column("MUNSINP", pl.UInt32),
            Column("MUNSIAFI", pl.UInt32),
            Column("MUNNOME", pl.Utf8),
            Column("MUNNOMEX", pl.Utf8),
            Column("OBSERV", pl.Utf8),
            Column("MUNSINON", pl.Utf8),
            Column("MUNSINONDV", pl.Utf8),
            Column("AMAZONIA", pl.Utf8),
            Column("FRONTEIRA", pl.Utf8),
            Column("CAPITAL", pl.Utf8),
            Column("UFCOD", pl.UInt8),
            Column("MESOCOD", pl.UInt16),
            Column("MICROCOD", pl.UInt16),
            Column("MSAUDCOD", pl.UInt16),
            Column("RSAUDCOD", pl.UInt16),
            Column("CSAUDCOD", pl.UInt16),
            Column("RMETRCOD", pl.UInt16),
            Column("AGLCOD", pl.UInt16),
            Column("ANOINST", pl.UInt16),
            Column("ANOEXT", pl.UInt16),
            Column("SUCESSOR", pl.UInt32),
            Column("LATITUDE", pl.Float64),
            Column("LONGITUDE", pl.Float64),
            Column("ALTITUDE", pl.Float64),
            Column("AREA", pl.Float64),
        ],
    )


def map_uf(df: pl.DataFrame):
    return to_schema(
        df,
        [
            Column("SIGLA_UF", pl.Utf8),
            Column("CODIGO", pl.UInt8),
            Column("DESCRICAO", pl.Utf8),
        ],
    )


def fetch_painel_oncologia_auxiliar(ftp_path: str):
    municipio_file = "CNV/br_municip.cnv"
    uf_file = "CNV/br_uf.cnv"

    files = fetch_from_zip(ftp_path, [municipio_file, uf_file])

    def read_as_df(file_name: str, id_dtype: pl.UInt32):
        cnv_bytes = files[file_name]
        df = to_dataframe(cnv_bytes, id_dtype=id_dtype)
        return df.with_columns(
            pl.col("NOME").str.split(" ").list.slice(1).list.join(" ")
        )

    return {
        MUNICIPIO_TABLE: read_as_df(municipio_file, id_dtype=pl.UInt32),
        UF_TABLE: read_as_df(uf_file, id_dtype=pl.UInt8),
    }
