import ftp
import db
import duckdb


def main():
    filepath = "POBR2013.dbc"
    target_table = "oncologia"

    df = ftp.fetch_dataframe(
        "ftp://ftp.datasus.gov.br/dissemin/publicos/PAINEL_ONCOLOGIA/DADOS/POBR2023.dbc"
    )

    with duckdb.connect("datasus.db") as db_con:
        db.import_dataframe(target_table, df, db_con)
        db.mark_file_as_imported(filepath, target_table, db_con)

    print(df)


if __name__ == "__main__":
    main()
