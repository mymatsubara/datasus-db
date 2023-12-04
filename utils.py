import itertools


def format_year(n, digits=2):
    if isinstance(n, str) and not n.isnumeric():
        return "*"

    n = str(n).zfill(digits)

    return n[-digits:]


def flatten(list_of_lists):
    return itertools.chain.from_iterable(list_of_lists)
