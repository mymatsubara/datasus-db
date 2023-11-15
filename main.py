import datasus


def main():
    datasus.import_from_ftp(
        "/dissemin/publicos/PAINEL_ONCOLOGIA/DADOS/POBR2*.dbc",
        "paciente_oncologico",
        lambda df, _: df,
    )


if __name__ == "__main__":
    main()
