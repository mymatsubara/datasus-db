import datasus
import polars as pl
from maps.po import map_po
from maps.sih_rh import map_sih_rd


def main():
    datasus.import_from_ftp(
        "/dissemin/publicos/PAINEL_ONCOLOGIA/DADOS/POBR*.dbc",
        "PO_PAINEL_ONCOLOGIA",
        map_po,
    )

    datasus.import_from_ftp(
        "/dissemin/publicos/SIHSUS/200801_/Dados/RD*.dbc",
        "SIH_RD_SISTEMA_INFORMACOES_HOSPITALARES",
        map_sih_rd,
    )


if __name__ == "__main__":
    main()
