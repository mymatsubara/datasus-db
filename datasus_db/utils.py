"""Module with generic helper functions"""

import itertools
import os


def format_year(year, digits=2):
    if isinstance(year, str) and not year.isnumeric():
        return "*"

    year = str(year).zfill(digits)

    return year[-digits:]


def format_month(month):
    if isinstance(month, str) and not month.isnumeric():
        return "*"

    return str(month).zfill(2)


def flatten(list_of_lists):
    return itertools.chain.from_iterable(list_of_lists)


def rm(file: str):
    if os.path.exists(file):
        os.remove(file)
