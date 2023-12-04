import urllib.request as request
import ftplib
import logging
import io
import dbf
from typing import Iterable
from utils import flatten
import subprocess
import shutil
import os.path as path
import os
from zipfile import ZipFile
from dbfread import DBF
import polars as pl


def fetch_dbc_as_df(ftp_path: str) -> pl.DataFrame:
    response = request.urlopen(ftp_path)
    dbc_raw = response.read()

    filename = path.basename(ftp_path).split(".")[0]
    dbc_file = f".tmp/{filename}.dbc"
    dbf_file = f".tmp/{filename}.dbf"

    os.makedirs(path.dirname(dbc_file), exist_ok=True)
    with open(
        dbc_file,
        "wb",
    ) as f:
        f.write(dbc_raw)

    dbc_2_dbf(dbc_file, dbf_file)

    df = pl.DataFrame(iter(DBF(dbf_file, encoding="iso-8859-1")))

    dbf.rm(dbc_file)
    dbf.rm(dbf_file)

    return df


def get_matching_files(host: str, patterns: Iterable[str]):
    ftp = ftplib.FTP(host)
    ftp.login()

    return set(flatten((try_nlst(pattern, ftp) for pattern in patterns)))


def try_nlst(pattern: str, ftp: ftplib.FTP):
    files = ftp.nlst(pattern)
    if len(files) == 0:
        logging.warn(f"⚠️  Could not found file matching: {pattern}")

    return files


def dbc_2_dbf(dbc: str, dbf: str):
    cwd = path.dirname(__file__)
    cmd = (
        path.join(cwd, "dbc2dbf", "dbc2dbf.exe").replace("\\", r"/")
        + " "
        + dbc.replace("/", r"\\")
    )
    subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).wait()
    generated_dbf = path.join(cwd, path.basename(dbc.replace(".dbc", ".dbf")))
    shutil.move(generated_dbf, dbf)


def fetch_from_zip(ftp_path: str, files: list[str]):
    response = request.urlopen(ftp_path)
    zip_file = ZipFile(io.BytesIO(response.read()))

    lowercase_filenames = {
        file.filename.lower(): file.filename for file in zip_file.filelist
    }

    return {file: zip_file.read(lowercase_filenames[file.lower()]) for file in files}
