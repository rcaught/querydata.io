"""Shared"""

import datetime
import os
import duckdb
import pandas as pd
from sqlite_utils import Database
from sqlite_utils.db import Table

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

def final_database_optimisations(sqlitedb: Database, print_indent=0):
    print()
    print(f"{print_indent * ' '}Optimising database")
    print(f"{print_indent * ' '}===================")

    sqlitedb.index_foreign_keys()

    sqlitedb.analyze()
    sqlitedb.vacuum()

    print(f"{print_indent * ' '}... done")


def drop_tables(tables: list[Table], print_indent=0):
    print()
    print(f"{print_indent * ' '}Dropping tables")
    print(f"{print_indent * ' '}===============")

    for table in tables:
        table.drop(ignore=True)
        print(f"{print_indent * ' '}- {table.name}... done")
