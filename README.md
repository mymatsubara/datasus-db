# datasus-db

[![PyPI version](https://badge.fury.io/py/datasus-db.svg)](https://pypi.org/project/datasus-db/ "Go to project PyPi page")
[![Documentation](https://img.shields.io/badge/Documentation-3fba11)](https://mymatsubara.github.io/datasus-db/datasus_db.html "Go to project documentation")

A python package to **download and import** public available data from **DATASUS's** ftp servers into a [DuckDB](https://duckdb.org/) database.

## Why DuckDB?
[DuckDB](https://duckdb.org/) is a local database similar to [sqlite](https://www.sqlite.org/index.html), but it is tailor made with analytical processing in mind, which makes it a great tool for analysing DATASUS's data. To see all the features DuckDB provides, check out their [documentation](https://www.sqlite.org/index.html).


## Installation
```
pip install datasus-db
```

## Usage

```python
import datasus_db
import logging


def main():
    # If you want to enable logging
    logging.getLogger().setLevel(logging.INFO)

    # Import SIM DO (Sistema de Informações de Mortalidade - Declarações de Óbito) data
    # By default the data is imported to the file `datasus.db`
    datasus_db.import_sim_do()

    # Import PO (Painel de Oncologia) data
    datasus_db.import_po(db_file="other-name.db")

    # Import SIH RD (Sistema de Informações Hospitalares do SUS - AIH Reduzida) data
    datasus_db.import_sih_rd()

    # Import SP (Autorização de Internação Hospitalar Saúde do Profissional) from SIHSUS (Sistema de Informações Hospitalares do SUS).
    datasus_db.import_sih_sp()

    # Import PA (Produção Ambulatorial) from SIASUS (Sistema de Informações Ambulatorial do SUS).
    datasus_db.import_sia_pa()

    # Import IBGE POP (População IBGE - Agregada por município, sexo e faixa etaria) data
    datasus_db.import_ibge_pop()

    # Import IBGE POP TCU (População IBGE - Tribunal de Contas da União) data
    datasus_db.import_ibge_pop_tcu()

    # Import auxiliar tables (Municipios, UFs e doenças)
    datasus_db.import_auxiliar_tables()


if __name__ == "__main__":
    main()
```

## Documentation
Check out the project's documentation:

[![View documentation](https://img.shields.io/badge/VIEW-DOCUMENTATION-3fba11?style=for-the-badge)](https://mymatsubara.github.io/datasus-db/datasus_db.html "Go to project documentation")

## Found a bug or want a new feature?
Feel free to create an [issue](https://github.com/mymatsubara/datasus-dbc-py/issues/new) here if you found a bug or if you want a new feature!


