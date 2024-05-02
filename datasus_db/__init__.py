"""
[![PyPI version](https://badge.fury.io/py/datasus-db.svg)](https://pypi.org/project/datasus-db/)

A python package to **download and import** public available data from **DATASUS's** ftp servers into a [DuckDB](https://duckdb.org/) database.


# Import functions
Bellow is the list of all **import functions**:
- `datasus_db.datasources.sih_rd.import_sih_rd`
- `datasus_db.datasources.sih_sp.import_sih_sp`
- `datasus_db.datasources.sim_do.import_sim_do`
- `datasus_db.datasources.sia_pa.import_sia_pa`
- `datasus_db.datasources.po.import_po`
- `datasus_db.datasources.ibge_pop.import_ibge_pop`
- `datasus_db.datasources.ibge_pop_tcu.import_ibge_pop_tcu`
- `datasus_db.datasources.auxiliar.import_auxiliar_tables`


## Datasources
The list of all available DATASUS's datasources can be seen here: https://datasus.saude.gov.br/transferencia-de-arquivos/

If `datasus_db` is missing a datasource that you need, feel free to create an issue here: https://github.com/mymatsubara/datasus-db/issues/new
"""

from .datasources.sih_rd import import_sih_rd
from .datasources.sih_sp import import_sih_sp
from .datasources.sim_do import import_sim_do
from .datasources.sia_pa import import_sia_pa
from .datasources.po import import_po
from .datasources.ibge_pop import import_ibge_pop
from .datasources.ibge_pop_tcu import import_ibge_pop_tcu
from .datasources.auxiliar import import_auxiliar_tables
