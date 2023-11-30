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
import re

MapFn = Callable[[pl.DataFrame], pl.DataFrame]
FetchFn = Callable[[str], dict[str, pl.DataFrame]]


def import_from_ftp(
    target_tables: list[str],
    ftp_glob: str,
    fetch_fn: FetchFn,
    db_string="datasus.db",
    ftp_host="ftp.datasus.gov.br",
    ftp_exclude_regex: str = None,
):
    with duckdb.connect(db_string) as db_con:
        target_tables_set = set(target_tables)
        files = ftp.get_matching_files(ftp_host, ftp_glob)
        if ftp_exclude_regex:
            files = remove_matching(files, ftp_exclude_regex)

        db.create_import_table(db_con)
        new_files = db.check_new_files(files, target_tables, db_con)
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
                            log_fetch,
                            args=(
                                filepath,
                                fetch_fn,
                            ),
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
                                tables_data = process.get()

                                msg = f"📂 [{files_imported + 1}/{total_files}] Importing data from file {filename}"
                                print(msg)

                                for table in tables_data.keys():
                                    if not table in target_tables_set:
                                        print(
                                            f"❌ Table name '{table}' not declared in 'target_tables': {target_tables}",
                                            file=sys.stderr,
                                        )
                                        continue

                                    if db.is_file_imported(filename, table, db_con):
                                        msg = f"🗃️ [{table}] File '{filename}' already imported"
                                        print(msg)
                                        continue

                                    df = tables_data[table]
                                    import_table_data(df, table, filepath, db_con)

                            except Exception as e:
                                print(
                                    f"❌ Error while importing '{filepath}'",
                                    file=sys.stderr,
                                )
                                print("Message: ", e, file=sys.stderr)
                                errors.append((filepath, e))

                            files_imported += 1

                        else:
                            still_wating.append((filepath, process))

                    waiting = still_wating
                    time.sleep(0.5)

    if len(errors) == 0:
        print(f"✅ Data successfully imported to tables: ", target_tables, "\n")
    else:
        print(
            f"⚠️  {len(errors)} out of {total_files} imports failed:",
            file=sys.stderr,
        )
        for filepath, e in errors:
            print(f"    ❌ {path.basename(filepath)}: {e}")


def log_fetch(ftp_path: str, fetch_fn: FetchFn):
    print(f"⬇️  Downloading file from ftp: '{ftp_path}'")
    return fetch_fn(ftp_path)


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]


def import_table_data(
    df: pl.DataFrame,
    target_table: str,
    filepath: str,
    db_con: duckdb.DuckDBPyConnection,
):
    filename = path.basename(filepath)
    print(f"💾 [{target_table}] Saving data to database from: {filename}")
    row_count = df.select(pl.count())[0, 0]

    if row_count != 0:
        db.import_dataframe(target_table, df, db_con)
    else:
        print(f"⚠️ [{target_table}] '{filename}' has no data")

    db.mark_file_as_imported(filepath, target_table, db_con)


def remove_matching(list: list[str], regex: str):
    compiled = re.compile(regex)
    return [e for e in list if not compiled.match(e)]
