import datasus
import polars as pl
from datasources.po import import_po
from datasources.sim_do import import_sim_do
from datasources.ibge_pop import import_ibge_pop


def main():
    import_po()
    import_sim_do()
    import_ibge_pop()


if __name__ == "__main__":
    main()
