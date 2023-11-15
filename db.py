import duckdb
import pandas as pd
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
    files: list[str], target_table: str, db_con: duckdb.DuckDBPyConnection
):
    imported_files = db_con.execute(
        f"SELECT file FROM {IMPORT_TABLE} where table_name = ?", [target_table]
    ).df()["file"]
    imported_files = set(imported_files)
    return [file for file in files if not path.basename(file) in imported_files]


def import_dataframe(
    table_name: str, df: pl.DataFrame, db_con: duckdb.DuckDBPyConnection
):
    # Since this is function is running on a controlled environment we don't sanitize the table name
    if has_table(table_name, db_con):
        db_con.sql(f"INSERT INTO {table_name} SELECT * FROM df")
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
    ).df()["has_table"][0]
