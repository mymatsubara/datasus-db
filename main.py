import polars as pl
from datasources.po import import_po
from datasources.sim_do import import_sim_do
from datasources.ibge_pop import import_ibge_pop
from datasources.ibge_pop_tcu import import_ibge_pop_tcu
from datasources.sih_rh import import_sih_rh
import logging


def main():
    logging.basicConfig(level=logging.INFO)
    # import_sim_do()
    # import_po()
    import_ibge_pop(years=[2021, 2012])
    # import_ibge_pop_tcu(years=[2024, 23, "19", 2012])


if __name__ == "__main__":
    main()
