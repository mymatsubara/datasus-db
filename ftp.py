import urllib.request as request
import ftplib
import io
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

    rm(dbc_file)
    rm(dbf_file)

    return df


def get_matching_files(host: str, pattern: str):
    ftp = ftplib.FTP(host)
    ftp.login()
    return ftp.nlst(pattern)


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


def rm(file: str):
    if path.exists(file):
        os.remove(file)


def fetch_from_zip(ftp_path: str, files: list[str]):
    response = request.urlopen(ftp_path)
    zip_file = ZipFile(io.BytesIO(response.read()))

    return {file: zip_file.read(file) for file in files}
