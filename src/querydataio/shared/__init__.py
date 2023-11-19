"""Shared"""

import datetime
import os
import duckdb
import pandas as pd

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 1000)
pd.set_option("display.width", 2000)


def current_year() -> int:
    """Current year"""
    return datetime.datetime.now().year


def init_duckdb() -> duckdb.DuckDBPyConnection:
    """DuckDB"""
    return duckdb.connect(database=":memory:")


def delete_db(db: str):
    print()
    print("Deleting DB")
    print("===========")
    if os.path.exists(db):
        os.remove(db)
        print(f"- {db}... done")
    else:
        print(f"- WARNING: cannot remove {db}")
