import pandas as pd
import urllib.request as request
import ftplib
import subprocess
import shutil
import os.path as path
import os
from simpledbf import Dbf5


def fetch_dataframe(ftp_path: str) -> pd.DataFrame:
    print("ftp_path: ", ftp_path, flush=True)
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

    df = Dbf5(dbf_file, codec="iso-8859-1").to_dataframe().infer_objects()

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
