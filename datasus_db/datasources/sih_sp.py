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
            Column("sp_gestor", pl.Utf8),
            Column("sp_uf", pl.Utf8),
            Column("sp_aa", pl.Utf8),                  
            Column("sp_mm", pl.Utf8),                  
            Column("sp_cnes", pl.Utf8),                
            Column("sp_naih", pl.Utf8),                
            Column("sp_procrea", pl.Utf8),            
            Column("sp_dtinter", pl.Utf8),            
            Column("sp_dtsaida", pl.Utf8),            
            Column("sp_num_pr", pl.Utf8),              
            Column("sp_tipo", pl.Utf8),                
            Column("sp_cpfcgc", pl.Utf8),              
            Column("sp_atoprof", pl.Utf8),              
            Column("sp_tp_ato", pl.Utf8),              
            Column("sp_qtd_ato", pl.Utf8),              
            Column("sp_ptsp", pl.Utf8),                
            Column("sp_nf", pl.Utf8),                  
            Column("sp_valato", pl.Utf8),              
            Column("sp_m_hosp", pl.Utf8),              
            Column("sp_m_pac", pl.Utf8),                
            Column("sp_des_hos", pl.Utf8),              
            Column("sp_des_pac", pl.Utf8),              
            Column("sp_complex", pl.Utf8),              
            Column("sp_financ", pl.Utf8),              
            Column("sp_co_faec", pl.Utf8),              
            Column("sp_pf_cbo", pl.Utf8),              
            Column("sp_pf_doc", pl.Utf8),              
            Column("sp_pj_doc", pl.Utf8),              
            Column("in_tp_val", pl.Utf8),              
            Column("sequencia", pl.Utf8),              
            Column("remessa", pl.Utf8),                
            Column("serv_cla", pl.Utf8),                
            Column("sp_cidpri", pl.Utf8),              
            Column("sp_cidsec", pl.Utf8),              
            Column("sp_qt_proc", pl.Utf8),              
            Column("sp_u_aih", pl.Utf8),
        ],
    )
