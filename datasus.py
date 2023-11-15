import math
from typing import Callable
import pandas as pd
import ftp
import os.path as path
import duckdb
import db
import multiprocessing
import gc

MapFn = Callable[[pd.DataFrame, str], pd.DataFrame]


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
        processes_count = max(
            min(math.ceil(multiprocessing.cpu_count() / 2), len(new_filepaths)), 1
        )
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

                        del process
                        gc.collect()
                    else:
                        still_wating.append((filepath, process))

                waiting = still_wating

            # processes = pool.map_async(
            #     lambda filepath: (ftp.fetch_dataframe(filepath), filepath),
            #     new_filepaths,
            #     callback=lambda result: import_to_db(
            #         df=result[0],
            #         target_table=target_table,
            #         filepath=result[1],
            #         map_fn=map_fn,
            #         db_con=db_con,
            #     ),
            # )

            print("Processes finished")

        # while len(waiting) != 0:
        #     still_wating = []

        #     for filepath, process in waiting:
        #         if process.ready():
        #             print("#####filepath: ", filepath)
        #             print(f"Importing data to db: {path.basename(filepath)}")
        #             # Import fetched data
        #             df = map_fn(process.get())
        #             db.import_dataframe(target_table, df)
        #             db.mark_file_as_imported(filepath, target_table, db_con)
        #         else:
        #             still_wating.append((filepath, process))

        #     waiting = still_wating
        # time.sleep(1)


def import_to_db(
    df: pd.DataFrame,
    target_table: str,
    filepath: str,
    map_fn: MapFn,
    db_con: duckdb.DuckDBPyConnection,
):
    print("hello")
    print(df, target_table, filepath, map_fn, db_con)
    print(f"Importing data to db: {path.basename(filepath)}")
    # Import fetched data
    db.import_dataframe(target_table, map_fn(df))
    db.mark_file_as_imported(filepath, target_table, db_con)
