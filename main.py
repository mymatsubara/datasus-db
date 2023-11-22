import datasus
import polars as pl
from datasources.po import import_po
from datasources.sim_do import import_sim_do


def main():
    import_po()
    import_sim_do()


if __name__ == "__main__":
    main()
