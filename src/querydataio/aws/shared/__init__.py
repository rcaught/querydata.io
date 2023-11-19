"""Shared utilities for AWS"""

import math
import sqlite3
from typing import Optional, Union
from types import ModuleType
import pandas as pd
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from sqlite_utils import Database
from sqlite_utils.db import Table
from querydataio.aws import shared as aws_shared
from querydataio import shared

MAX_RECORDS_SIZE = 1000
PARTIAL_COLLECTION_SIZE = 200
SQLITE_DB = "dbs/aws.db"
SQLITE_TAGS_TABLE_NAME = "tags"


def download(
    con: DuckDBPyConnection,
    url: str,
    tag_id_prefix: Optional[str] = None,
    paritions: range | list[str] = [],
    max_records_size: Optional[int] = None,
    max_pages: Optional[int] = None,
    print_indent=0,
) -> list[DuckDBPyRelation]:
    """Break up download range"""

    all_data: list[DuckDBPyRelation] = []

    print()
    print(f"{print_indent * ' '}Downloading data")
    print(f"{print_indent * ' '}================")
    print(f"{print_indent * ' '}{len(paritions)} partitions...")
    print()

    if max_records_size is None:
        max_records_size = MAX_RECORDS_SIZE

    for partition in paritions:
        print(f"{print_indent * ' '}- partition: {partition}")
        result = get_data(
            con,
            url,
            tag_id_prefix,
            partition,
            0,
            max_records_size,
            print_indent=print_indent + 2,
        )
        all_data.append(result)

        total_hits = result.fetchall()[0][1]["totalHits"]

        if max_pages is None:
            item_max_pages = math.ceil(total_hits / max_records_size)
        else:
            item_max_pages = max_pages

        for page in range(1, item_max_pages):
            result = get_data(
                con,
                url,
                tag_id_prefix,
                partition,
                page,
                max_records_size,
                print_indent=print_indent + 2,
            )
            all_data.append(result)

    return all_data


def get_data(
    con: DuckDBPyConnection,
    url: str,
    tag_id_prefix: Optional[str],
    item: str,
    page: int,
    max_records_size: int = MAX_RECORDS_SIZE,
    print_indent=0,
) -> DuckDBPyRelation:
    """Gets data. Pagination limits necessitate the following year and page scopes."""

    target_url = f"{url}&size={max_records_size}&page={page}"

    if tag_id_prefix is not None:
        target_url = f"{target_url}&tags.id={tag_id_prefix}{item}"

    data = con.sql(
        query=f"""
              SELECT
                *
              FROM
                read_json_auto("{target_url}");
            """
    )

    count = con.sql("SELECT metadata.count FROM data").fetchall()[0][0]

    print(f"{print_indent * ' '}- page {page} - {count} records")
    # print(f"          {target_url}")

    return data


def to_sqlite(items: list[tuple[pd.DataFrame, Table]], print_indent=0):
    """Export Dataframe to SQLite"""

    print("")
    print(f"{print_indent * ' '}Export to SQLite")
    print(f"{print_indent * ' '}================")

    for data, table in items:
        sqlite = sqlite3.connect(SQLITE_DB)
        data.to_sql(table.name, sqlite, if_exists="replace", index=False)

        print(f"{print_indent * ' '}- {table.name}... done")


def merge_sqlite_tables(
    sqlitedb: Database, tables: list[tuple[Table, Table]], print_indent=0
):
    print()
    print(f"{print_indent * ' '}Merging")
    print(f"{print_indent * ' '}=======")

    for old_table, new_table in tables:
        sqlitedb.execute(
            f"""
            INSERT OR REPLACE INTO {old_table.name} SELECT * FROM {new_table.name};
            """
        )
        sqlitedb.execute(f"DROP TABLE {new_table.name};")

        print(f"{print_indent * ' '}- {new_table.name} => {old_table.name}... done")


def tags_table(sqlitedb: Database) -> Table:
    return sqlitedb.table(SQLITE_TAGS_TABLE_NAME)


def process(
    con: DuckDBPyConnection,
    all_data: list[DuckDBPyRelation],
    relation_id: str,
    print_indent=0,
) -> list[pd.DataFrame]:
    """Clean and transform into a Dataframe"""

    processed_main: list[pd.DataFrame] = []
    processed_tags: list[pd.DataFrame] = []
    processed_main_tags: list[pd.DataFrame] = []

    print()
    print(f"{print_indent * ' '}Processing data")
    print(f"{print_indent * ' '}===============")
    print(f"{print_indent * ' '}{len(all_data)} pages...")
    print()

    for i, data in enumerate(all_data):  # pylint: disable=unused-variable
        print(f"{print_indent * ' '}- page {i + 1}")

        unnested_items = con.sql(  # pylint: disable=unused-variable
            """
              SELECT
                UNNEST(items, recursive := true)
              FROM
                data;
            """
        )

        if unnested_items.count("*").fetchall()[0][0] == 0:
            continue

        main = con.sql(
            """
                CREATE OR REPLACE TEMP TABLE t1 AS
                SELECT
                  * EXCLUDE (tags)
                FROM
                  unnested_items;

                SELECT * FROM t1;
            """
        )

        tags_with_id = con.sql(
            f"""
                SELECT
                  id as {relation_id},
                  unnest(tags, recursive := true)
                FROM
                  unnested_items;
            """
        )

        main_tags = con.sql(
            f"""
                SELECT
                  {relation_id},
                  id as tag_id
                FROM
                  tags_with_id;
            """
        )

        tags = con.sql(
            f"""
                SELECT DISTINCT * EXCLUDE ({relation_id}, description)
                FROM
                  tags_with_id;
            """
        )

        processed_main.append(main.df())
        processed_tags.append(tags.df())
        processed_main_tags.append(main_tags.df())

    result_main = pd.concat(processed_main, ignore_index=True)
    result_tags = pd.concat(processed_tags, ignore_index=True)
    result_main_tags = pd.concat(processed_main_tags, ignore_index=True)

    result_tags_distinct = con.sql(
        """
            SELECT
              * EXCLUDE (lastUpdatedBy, dateUpdated, name),
              MAX_BY(lastUpdatedBy, dateUpdated) as lastUpdatedBy,
              MAX(dateUpdated) as dateUpdated,
              MAX_BY(name, dateUpdated) as name
            FROM
              result_tags
            GROUP BY ALL;
        """
    ).df()

    # some items exist in multiple partitions
    result_main_distinct = con.sql(
        """
            SELECT
              DISTINCT *
            FROM
              result_main;
        """
    )

    return [result_main, result_tags_distinct, result_main_tags]


def run_full(
    sqlitedb: Database,
    duckdb: DuckDBPyConnection,
    main_module: ModuleType,
    download_range: Union[range, list[any]],
    print_indent=0,
):
    print()
    print(f"{print_indent * ' '}{main_module.DIRECTORY_ID}")
    print(f"{print_indent * ' '}{len(main_module.DIRECTORY_ID) * '='}")

    main_table: Table = sqlitedb.table(main_module.SQLITE_MAIN_TABLE_NAME)
    main_tags_table: Table = sqlitedb.table(main_module.SQLITE_MAIN_TAGS_TABLE_NAME)
    tags_table = aws_shared.tags_table(sqlitedb)

    downloaded = aws_shared.download(
        duckdb,
        main_module.URL_PREFIX,
        main_module.TAG_ID_PREFIX,
        download_range,
        print_indent=print_indent + 2,
    )

    result_main, result_tags, result_main_tags = main_module.process(
        duckdb, downloaded, print_indent=print_indent + 2
    )

    aws_shared.to_sqlite(
        [
            (result_main, main_table),
            (result_tags, tags_table),
            (result_main_tags, main_tags_table),
        ],
        print_indent=print_indent + 2,
    )

    main_module.initial_sqlite_transform(main_table)
    main_module.final_sqlite_transform(
        main_table,
        tags_table,
        main_tags_table,
        print_indent=print_indent + 2,
    )


def run_partial(
    main_module: ModuleType,
    tag_id_prefix: str | None,
    partitions: range | list[any] = [],
    print_indent=0,
):
    print()
    print(f"{print_indent * ' '}{main_module.DIRECTORY_ID}")
    print(f"{print_indent * ' '}{len(main_module.DIRECTORY_ID) * '='}")

    sqlitedb = Database(aws_shared.SQLITE_DB)
    duckdb = shared.init_duckdb()

    print(
        f"{print_indent * ' '}- downloading last {aws_shared.PARTIAL_COLLECTION_SIZE} and merging"
    )

    downloaded = aws_shared.download(
        duckdb,
        main_module.URL_PREFIX,
        tag_id_prefix,
        partitions,
        aws_shared.PARTIAL_COLLECTION_SIZE,
        1,
        print_indent=print_indent + 2,
    )

    result_main, result_tags, result_main_tags = main_module.process(
        duckdb, downloaded, print_indent + 2
    )

    # SQLite processing of new tables
    # NOTE: more would be done in duckDB, but errors on column reordering were happening

    main_new_table: Table = sqlitedb.table(main_module.SQLITE_MAIN_TABLE_NAME + "_new")
    tags_new_table: Table = sqlitedb.table(aws_shared.SQLITE_TAGS_TABLE_NAME + "_new")
    main_tags_new_table: Table = sqlitedb.table(
        main_module.SQLITE_MAIN_TAGS_TABLE_NAME + "_new"
    )

    aws_shared.to_sqlite(
        [
            (result_main, main_new_table),
            (result_tags, tags_new_table),
            (result_main_tags, main_tags_new_table),
        ],
        print_indent=print_indent + 2,
    )

    main_module.initial_sqlite_transform(main_new_table)

    # Merge into existing

    main_table: Table = sqlitedb.table(main_module.SQLITE_MAIN_TABLE_NAME)
    tags_table = aws_shared.tags_table(sqlitedb)
    main_tags_table: Table = sqlitedb.table(main_module.SQLITE_MAIN_TAGS_TABLE_NAME)

    main_table_count = main_table.count

    aws_shared.merge_sqlite_tables(
        sqlitedb,
        [
            (main_table, main_new_table),
            (tags_table, tags_new_table),
            (main_tags_table, main_tags_new_table),
        ],
        print_indent=print_indent + 2,
    )

    if main_table.count == main_table_count:
        return False

    main_module.final_sqlite_transform(
        main_table, tags_table, main_tags_table, print_indent=print_indent + 2
    )

    return True


def common_table_optimisations(
    tags_table: Table,
    main_tags_table: Table,
    main_table: Table,
    relation_id: str,
    print_indent=0,
):
    tags_table.transform(pk="id")
    tags_table.create_index(["tagNamespaceId"])
    tags_table.create_index(["name"])
    tags_table.create_index(["tagNamespaceId", "name"])

    print(f"{print_indent * ' '}- {tags_table.name}... done")

    main_tags_table.transform(pk=[relation_id, "tag_id"])
    main_tags_table.add_foreign_key(relation_id, main_table.name, "id", ignore=True)
    main_tags_table.add_foreign_key("tag_id", tags_table.name, "id", ignore=True)

    print(f"{print_indent * ' '}- {main_tags_table.name}... done")
