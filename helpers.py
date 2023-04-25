"""Contains helpers for scraper

"""
import csv

from typing import Any


class CsvProcessor():
    """Helper class for to_csv_string method  
    """

    def __init__(self):
        self.csv_string = []

    def write(self, row: list[Any]) -> None:
        """Regarding csv lib documentation:
        "csvfile can be any object with a write() method".
        It useful to have a list converted to beautiful csv-safe string

        Parameters
        ----------
        row : list[Any]
            A list of items have to convert to csv string.
    
        Returns
        -------
        None
        """

        self.csv_string.append(row)


def to_csv_string(row: list[Any]) -> str:
    """Converts a list to csv-safe string.

    Parameters
    ----------
    row : list[Any]
        A list of items have to convert to csv string.

    Returns
    -------
    str
        A string ready to be written into csv file.
    """

    csvfile = CsvProcessor()
    writer = csv.writer(csvfile)
    writer.writerow(row)
    return csvfile.csv_string.pop()
