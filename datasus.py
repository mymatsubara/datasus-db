from typing import Callable
import sys
import pandas as pd
import ftp
import os.path as path
import duckdb
import db
import multiprocessing
import polars as pl
import time
import random

MapFn = Callable[[pl.DataFrame], pl.DataFrame]


def import_from_ftp(
    target_table: str,
    ftp_pattern: str,
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

        # Shuffle files to import in random order to reduce the chance of importing multiple large files at the same time
        random.shuffle(new_filepaths)

        # Fetch dataframes in parallel
        processes_count = max(min(multiprocessing.cpu_count(), len(new_filepaths)), 1)
        total_files = len(new_filepaths)
        files_imported = 0
        errors: list[tuple[str, Exception]] = []

        # Batching is done to make sure the garbage collector kicks in
        for new_filepaths in batch(new_filepaths, 64):
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

                                msg = f"💾 [{files_imported + 1}/{total_files}] Saving data from file to db: '{filename}'"
                                print(msg)

                                if row_count != 0:
                                    db.import_dataframe(target_table, df, db_con)
                                else:
                                    print(
                                        f"⚠️ [{target_table}] '{filename}' has no data"
                                    )

                                db.mark_file_as_imported(filepath, target_table, db_con)
                                files_imported += 1

                            except Exception as e:
                                print(
                                    f"❌ [{target_table}] Error while importing '{filepath}'",
                                    file=sys.stderr,
                                )
                                errors.append((filepath, e))

                        else:
                            still_wating.append((filepath, process))

                    waiting = still_wating
                    time.sleep(0.5)

    if len(errors) == 0:
        print(f"✅ [{target_table}] Data successfully imported\n")
    else:
        print(
            f"⚠️ [{target_table}] {len(errors)} out of {total_files} imports failed:",
            file=sys.stderr,
        )
        for filepath, e in errors:
            print(f"    ❌ {path.basename(filepath)}: {e}")


def fetch_and_map(ftp_path: str, map_fn: MapFn, target_table: str) -> pl.DataFrame:
    print(f"⬇️  [{target_table}] Downloading file from ftp: '{ftp_path}'")
    df = ftp.fetch_dataframe(ftp_path)

    row_count = df.select(pl.count())[0, 0]
    return (map_fn(df) if row_count > 0 else df, row_count)


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]
