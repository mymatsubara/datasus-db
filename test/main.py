from datasus_db import import_auxiliar_tables
import logging
import sys


def main():
    sys.path.append("..")
    logging.getLogger().setLevel(logging.INFO)
    import_auxiliar_tables()


if __name__ == "__main__":
    main()
