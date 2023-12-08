"""Shared"""

import datetime
import os
import time
import duckdb
import pandas as pd
from sqlite_utils import Database

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 1000)
pd.set_option("display.width", 2000)


def current_year() -> int:
    return datetime.datetime.now().year


def init_duckdb(db: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=db)


def delete_dbs(dbs: list[str], print_indent: int = 0):
    print()
    print(f"{print_indent * ' '}Deleting DBs")
    print(f"{print_indent * ' '}============")

    for db in dbs:
        if os.path.exists(db):
            os.remove(db)
            print(f"{print_indent * ' '}- {db}... done")
        else:
            print(f"{print_indent * ' '}- WARNING: cannot remove {db}")


def final_database_optimisations(sqlitedb: Database, print_indent=0):
    print()
    print(f"{print_indent * ' '}Optimising database")
    print(f"{print_indent * ' '}===================")
    start = time.time()

    file_query = "select file from pragma_database_list where name='main';"
    print(
        f"{print_indent * ' '}- {sqlitedb.execute(file_query).fetchone()[0]}... ",
        end="",
    )

    sqlitedb.index_foreign_keys()

    sqlitedb.analyze()
    sqlitedb.vacuum()

    print(f"done ({time.time() - start})")
