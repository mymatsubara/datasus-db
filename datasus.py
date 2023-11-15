import math
from typing import Callable
import pandas as pd
import ftp
import os.path as path
import duckdb
import db
import multiprocessing
import gc
import polars as pl
import time

MapFn = Callable[[pl.DataFrame, str], pl.DataFrame]


def import_from_ftp(
    ftp_pattern: str,
    target_table: str,
    map_fn: MapFn,
    db_string="datasus.db",
    ftp_host="ftp.datasus.gov.br",
):
    with duckdb.connect(db_string) as db_con:
        files = ftp.get_matching_files(ftp_host, ftp_pattern)
        db.create_import_table(db_con)
        new_files = db.check_new_files(files, target_table, db_con)
        new_filepaths = [f"ftp://{ftp_host}{file}" for file in new_files]

        # Fetch dataframes in parallel
        processes_count = max(min(multiprocessing.cpu_count(), len(new_filepaths)), 1)
        with multiprocessing.Pool(processes=processes_count) as pool:
            waiting = [
                (
                    filepath,
                    pool.apply_async(
                        ftp.fetch_dataframe,
                        args=(filepath,),
                    ),
                )
                for filepath in new_filepaths
            ]

            while len(waiting) != 0:
                still_wating = []

                for filepath, process in waiting:
                    if process.ready():
                        print("#####filepath: ", filepath)
                        print(f"Importing data to db: {path.basename(filepath)}")
                        # Import fetched data
                        df = map_fn(process.get(), filepath)
                        db.import_dataframe(target_table, df, db_con)
                        db.mark_file_as_imported(filepath, target_table, db_con)

                        del df
                        del process
                        gc.collect()
                    else:
                        still_wating.append((filepath, process))

                waiting = still_wating
                time.sleep(0.5)

            print("Processes finished")


def import_to_db(
    df: pl.DataFrame,
    target_table: str,
    filepath: str,
    map_fn: MapFn,
    db_con: duckdb.DuckDBPyConnection,
):
    print(f"Importing data to db: {path.basename(filepath)}")

    # Import fetched data
    db.import_dataframe(target_table, map_fn(df))
    db.mark_file_as_imported(filepath, target_table, db_con)
