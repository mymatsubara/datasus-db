import datasus
import polars as pl
from maps.po import map_po
from maps.sih_rh import map_sih_rd
from maps.sim_do import map_sim_do


def main():
    # datasus.import_from_ftp(
    #     "PO_PAINEL_ONCOLOGIA",
    #     "/dissemin/publicos/PAINEL_ONCOLOGIA/DADOS/POBR*.dbc",
    #     map_po,
    # )

    datasus.import_from_ftp(
        "SIM_DO_SISTEMA_DE_INFORMACAO_DE_MORTALIDADE",
        "/dissemin/publicos/SIM/CID10/DORES/DO*.dbc",
        map_sim_do,
    )

    datasus.import_from_ftp(
        "SIM_DO_SISTEMA_DE_INFORMACAO_DE_MORTALIDADE",
        "/dissemin/publicos/SIM/PRELIM/DORES/DO*.dbc",
        map_sim_do,
    )

    # datasus.import_from_ftp(
    #     "SIH_RD_SISTEMA_INFORMACOES_HOSPITALARES",
    #     "/dissemin/publicos/SIHSUS/200801_/Dados/RD*.dbc",
    #     map_sih_rd,
    #     db_string="sih_rh.db",
    # )


if __name__ == "__main__":
    main()
