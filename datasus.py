from typing import Callable
import sys
import pandas as pd
import ftp
import os.path as path
import duckdb
import db
import multiprocessing
import gc
import polars as pl
import time
import random

MapFn = Callable[[pl.DataFrame], pl.DataFrame]


def import_from_ftp(
    ftp_pattern: str,
    target_table: str,
    map_fn: MapFn,
    db_string="datasus.db",
    ftp_host="ftp.datasus.gov.br",
):
    print(f"⏳ [{target_table}] Starting data import...")

    with duckdb.connect(db_string) as db_con:
        files = ftp.get_matching_files(ftp_host, ftp_pattern)
        db.create_import_table(db_con)
        new_files = db.check_new_files(files, target_table, db_con)
        new_filepaths = [f"ftp://{ftp_host}{file}" for file in new_files]

        # Shuffle list to make it harder to process multiple huge files in parallel, which can exaust the systems resources
        random.shuffle(new_filepaths)

        # Fetch dataframes in parallel
        processes_count = max(min(multiprocessing.cpu_count(), len(new_filepaths)), 1)
        with multiprocessing.Pool(processes=processes_count) as pool:
            waiting = [
                (
                    filepath,
                    pool.apply_async(
                        fetch_and_map,
                        args=(filepath, map_fn, target_table),
                    ),
                )
                for filepath in new_filepaths
            ]

            while len(waiting) != 0:
                still_wating = []

                for filepath, process in waiting:
                    if process.ready():
                        try:
                            # Import fetched data
                            filename = path.basename(filepath)
                            (df, row_count) = process.get()

                            msg = f"💾 [{target_table}] Saving data from file to db: '{filename}'"
                            print(msg)

                            if row_count != 0:
                                db.import_dataframe(target_table, df, db_con)
                            else:
                                print(f"⚠️ [{target_table}] '{filename}' has no data")

                            db.mark_file_as_imported(filepath, target_table, db_con)

                            del df
                            del process
                            gc.collect()

                        except Exception as e:
                            print(
                                f"❌ [{target_table}] Error while importing '{filepath}'",
                                file=sys.stderr,
                            )
                            raise e

                    else:
                        still_wating.append((filepath, process))

                waiting = still_wating
                time.sleep(0.5)

    print(f"✅ [{target_table}] Data successfully imported\n")


def fetch_and_map(ftp_path: str, map_fn: MapFn, target_table: str) -> pl.DataFrame:
    print(f"⬇️  [{target_table}] Downloading file from ftp: '{ftp_path}'")
    df = ftp.fetch_dataframe(ftp_path)

    row_count = df.select(pl.count())[0, 0]
    return (map_fn(df) if row_count > 0 else df, row_count)
