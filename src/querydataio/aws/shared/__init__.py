"""Shared utilities for AWS"""

import math
import sqlite3
from typing import Optional
import pandas as pd
import duckdb
from sqlite_utils import Database
from sqlite_utils.db import Table

MAX_RECORDS_SIZE = 1000
SQLITE_DB = "dbs/aws.db"
MAIN_URL_PREFIX = (
    "https://aws.amazon.com/api/dirs/items/search?"
    "item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime"
    "&sort_order=desc"
)
SQLITE_WHATS_NEW_TABLE_NAME = "whats_new"
SQLITE_WHATS_NEW_TAGS_TABLE_NAME = "whats_new_tags"

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)


def get_data(
    con: duckdb.DuckDBPyConnection,
    year: str,
    page: int,
    max_records_size: int = MAX_RECORDS_SIZE,
) -> duckdb.DuckDBPyRelation:
    """Gets data. Pagination limits necessitate the following year and page scopes."""

    target_url = (
        f"{MAIN_URL_PREFIX}&size={max_records_size}"
        f"&tags.id=whats-new%23year%23{year}&page={page}"
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


def process(
    con: duckdb.DuckDBPyConnection, all_data: list[duckdb.DuckDBPyRelation]
) -> list[pd.DataFrame]:
    """Clean and transform into a Dataframe"""

    processed_updates: list[pd.DataFrame] = []
    processed_tags: list[pd.DataFrame] = []

    print("")
    print("Processing data")
    print("===============")
    print("")

    for i, data in enumerate(all_data):  # pylint: disable=unused-variable
        print(f"    Page: {i + 1}")
        unnested_items = con.sql(  # pylint: disable=unused-variable
            """
              SELECT
                UNNEST(items, recursive := true)
              FROM
                data;
            """
        )

        updates = con.sql(
            """
                CREATE OR REPLACE TEMP TABLE t1 AS
                SELECT
                  * EXCLUDE (tags) REPLACE('https://aws.amazon.com' || headlineUrl AS headlineUrl)
                FROM
                  unnested_items;

                UPDATE t1 SET postDateTime = '2021-06-23T12:30:32Z'
                WHERE postDateTime = '0202-06-23T12:30:32Z';

                SELECT * FROM t1;
            """
        )

        tags = con.sql(
            """
                SELECT
                  id as whats_new_id,
                  unnest(tags, recursive := true)
                FROM
                  unnested_items;
            """
        )

        processed_updates.append(updates.df())
        processed_tags.append(tags.df())

    result_updates = pd.concat(processed_updates, ignore_index=True)
    result_tags = pd.concat(processed_tags, ignore_index=True)

    return [result_updates, result_tags]


def to_sqlite(table: str, df: pd.DataFrame):
    """Export Dataframe to SQLite"""

    print("")
    print("Export to SQLite")
    print("================")

    sqlite = sqlite3.connect(SQLITE_DB)
    df.to_sql(table, sqlite, if_exists="replace", index=False)

    print(f"{table}... done")


def download_years(
    con: duckdb.DuckDBPyConnection,
    start: int,
    end: int,
    max_records_size: Optional[int] = None,
    max_pages: Optional[int] = None,
) -> list[duckdb.DuckDBPyRelation]:
    """Break up download range"""

    all_data: list[duckdb.DuckDBPyRelation] = []

    print("Downloading data")
    print("================")
    print("")
    print("  AWS")
    print("    whats-new")

    if max_records_size is None:
        max_records_size = MAX_RECORDS_SIZE

    for year in range(start, end + 1):
        print(f"      year: {year}")
        result = get_data(con, year, 0, max_records_size)
        all_data.append(result)

        total_hits = result.fetchall()[0][1]["totalHits"]

        if max_pages is None:
            year_max_pages = math.ceil(total_hits / max_records_size)
        else:
            year_max_pages = max_pages

        for page in range(1, year_max_pages):
            result = get_data(con, year, page, max_records_size)
            all_data.append(result)

    return all_data


def source_grand_total(con: duckdb.DuckDBPyConnection) -> int:
    """Determine total records for all time from the source."""

    data = con.sql(  # pylint: disable=unused-variable
        query=f"""
              SELECT
                *
              FROM
                read_json_auto("{MAIN_URL_PREFIX}&size=1");
            """
    )

    total_hits = con.sql("SELECT metadata.totalHits FROM data").fetchall()[0][0]

    total_hits = (
        total_hits - 1
    )  # There is one extra update that must not have a year tag

    return total_hits


def local_grand_total(table: str) -> int:
    """Determine total records for all time from local store."""

    db = Database(SQLITE_DB)

    return db[table].count


def validate_totals(con: duckdb.DuckDBPyConnection, table: str):
    """Validate totals"""

    source = source_grand_total(con)
    local = local_grand_total(table)

    assert (
        source == local
    ), f"source total ({source}) not equal to local total ({local})"


def initial_sqlite_transform(whats_new_table: str):
    """Initial tranformations"""

    whats_new_table.transform(
        column_order=(
            "id",
            "postDateTime",
            "headline",
            "headlineUrl",
            "postSummary",
            "postBody",
            "dateCreated",
            "dateUpdated",
            "modifiedDate",
        ),
        types={
            "postDateTime": str,
            "dateCreated": str,
            "dateUpdated": str,
            "modifiedDate": str,
        },
    )


def final_sqlite_transform(
    sqlitedb: Database, whats_new_table: Table, whats_new_tags_table: Table
):
    """Final tranformations"""

    print()
    print("Final optimisations")
    print("===================")

    whats_new_table.transform(
        pk="id",
    )

    whats_new_table.create_index(["id"], unique=True)
    whats_new_table.create_index(["postDateTime"])
    whats_new_table.create_index(["headline"])

    whats_new_tags_table.transform(pk=["whats_new_id", "id"])

    whats_new_tags_table.add_foreign_key("whats_new_id", "whats_new", "id", ignore=True)
    sqlitedb.index_foreign_keys()

    whats_new_tags_table.create_index(["id"])
    whats_new_tags_table.create_index(["tagNamespaceId"])
    whats_new_tags_table.create_index(["name"])
    whats_new_tags_table.create_index(["tagNamespaceId", "name"])

    sqlitedb.analyze()
    sqlitedb.vacuum()

    print("... done")


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
