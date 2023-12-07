"""
Module with common functions used to interact with DuckDB
"""

import duckdb
import os.path as path
import polars as pl

IMPORT_TABLE = "__import"


def create_import_table(db_con: duckdb.DuckDBPyConnection):
    db_con.sql(
        f"""
CREATE TABLE IF NOT EXISTS {IMPORT_TABLE} (
    file VARCHAR(255),
    table_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (file, table_name)
)"""
    )


def check_new_files(
    files: list[str], target_tables: list[str], db_con: duckdb.DuckDBPyConnection
):
    tables = ",".join((f"'{table}'" for table in target_tables))

    imported_files = db_con.query(
        f"""
SELECT file, count(*) as count 
FROM {IMPORT_TABLE}  
WHERE table_name IN ({tables})
GROUP BY file
HAVING count = {len(target_tables)}"""
    ).pl()["file"]
    imported_files = set(imported_files)

    return [file for file in files if not path.basename(file) in imported_files]


def is_file_imported(
    file: str, target_table: str, db_con: duckdb.DuckDBPyConnection
) -> bool:
    return (
        db_con.execute(
            f"SELECT COUNT(*) as count FROM {IMPORT_TABLE} WHERE table_name = ? AND file = ?",
            [target_table, path.basename(file)],
        ).pl()["count"][0]
        == 1
    )


def import_dataframe(
    table_name: str, df: pl.DataFrame, db_con: duckdb.DuckDBPyConnection
):
    # Since this is function is running on a controlled environment we don't sanitize the table name
    if has_table(table_name, db_con):
        cols = ",".join((f'"{col}"' for col in df.columns))
        db_con.sql(f"INSERT INTO {table_name} ({cols}) SELECT * FROM df")
    else:
        db_con.sql(f"CREATE TABLE {table_name} AS SELECT * FROM df")


def mark_file_as_imported(
    file: str, table_name: str, db_con: duckdb.DuckDBPyConnection
):
    db_con.execute(
        f"INSERT INTO {IMPORT_TABLE} (file, table_name) VALUES (?, ?)",
        [path.basename(file), table_name],
    )


def has_table(table_name: str, db_con: duckdb.DuckDBPyConnection) -> bool:
    return db_con.execute(
        "SELECT count(*) == 1 as has_table FROM duckdb_tables where table_name = ?",
        [table_name],
    ).pl()["has_table"][0]
