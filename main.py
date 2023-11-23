import datasus
import polars as pl
from datasources.po import import_po
from datasources.sim_do import import_sim_do
from datasources.ibge_pop import import_ibge_pop
from datasources.ibge_pop_tcu import import_ibge_pop_tcu


def main():
    import_po()
    import_sim_do()
    import_ibge_pop()
    import_ibge_pop_tcu()


if __name__ == "__main__":
    main()
