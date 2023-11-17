"""Shared utilities for AWS"""

import math
import sqlite3
from typing import Optional
import pandas as pd
import duckdb
from sqlite_utils import Database
from sqlite_utils.db import Table

MAX_RECORDS_SIZE = 1000
PARTIAL_COLLECTION_SIZE = 200
SQLITE_DB = "dbs/aws.db"

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)


def download(
    con: duckdb.DuckDBPyConnection,
    url: str,
    directory_id: str,
    tag_id_prefix: str,
    paritions: list[str],
    max_records_size: Optional[int] = None,
    max_pages: Optional[int] = None,
) -> list[duckdb.DuckDBPyRelation]:
    """Break up download range"""

    all_data: list[duckdb.DuckDBPyRelation] = []

    print()
    print("Downloading data")
    print("================")
    print("  AWS")
    print(f"    {directory_id}")

    if max_records_size is None:
        max_records_size = MAX_RECORDS_SIZE

    for item in paritions:
        print(f"      item: {item}")
        result = get_data(con, url, tag_id_prefix, item, 0, max_records_size)
        all_data.append(result)

        total_hits = result.fetchall()[0][1]["totalHits"]

        if max_pages is None:
            item_max_pages = math.ceil(total_hits / max_records_size)
        else:
            item_max_pages = max_pages

        for page in range(1, item_max_pages):
            result = get_data(con, url, tag_id_prefix, item, page, max_records_size)
            all_data.append(result)

    return all_data


def get_data(
    con: duckdb.DuckDBPyConnection,
    url: str,
    tag_id_prefix: str,
    item: str,
    page: int,
    max_records_size: int = MAX_RECORDS_SIZE,
) -> duckdb.DuckDBPyRelation:
    """Gets data. Pagination limits necessitate the following year and page scopes."""

    target_url = (
        f"{url}&size={max_records_size}&tags.id={tag_id_prefix}{item}&page={page}"
    )

    data = con.sql(
        query=f"""
              SELECT
                *
              FROM
                read_json_auto("{target_url}");
            """
    )

    count = con.sql("SELECT metadata.count FROM data").fetchall()[0][0]

    print(f"        downloading page {page} - {count} records")
    # print(f"          {target_url}")

    return data


def to_sqlite(table: str, df: pd.DataFrame):
    """Export Dataframe to SQLite"""

    print("")
    print("Export to SQLite")
    print("================")

    sqlite = sqlite3.connect(SQLITE_DB)
    df.to_sql(table, sqlite, if_exists="replace", index=False)

    print(f"{table}... done")


def merge_sqlite_tables(sqlitedb: Database, old_table: Table, new_table: Table):
    """Merge"""

    print()
    print("Merging")
    print("=======")

    sqlitedb.execute(
        f"""
        INSERT OR REPLACE INTO {old_table.name} SELECT * FROM {new_table.name};
    """
    )
    sqlitedb.execute(f"DROP TABLE {new_table.name};")


    print(f"{new_table.name} => {old_table.name}... done")
