"""Shared utilities for AWS"""

import os
from types import ModuleType
import pandas as pd
from duckdb import DuckDBPyConnection
from sqlite_utils import Database
from sqlite_utils.db import Table
from querydataio.aws import shared as aws_shared
import time

MAX_RECORDS_SIZE = 1000
PARTIAL_COLLECTION_SIZE = 200
SQLITE_DB = "dbs/aws.db"
DUCKDB_DB = "dbs/aws.duckdb.db"
TAGS_TABLE_NAME = "tags"


def to_sqlite(
    sqlitedb: Database, items: list[tuple[pd.DataFrame, str]], print_indent=0
):
    """Export Dataframe to SQLite"""

    print("")
    print(f"{print_indent * ' '}Export to SQLite")
    print(f"{print_indent * ' '}================")

    for data, table in items:
        start = time.time()
        print(f"{print_indent * ' '}- {table}... ", end="")

        data.to_sql(table, sqlitedb.conn, if_exists="replace", index=False)

        print(f"done ({time.time() - start})")


def merge_duckdb_tables(
    ddb_con: DuckDBPyConnection, tables: list[tuple[str, str]], print_indent: int = 0
):
    print()
    print(f"{print_indent * ' '}Merging")
    print(f"{print_indent * ' '}=======")

    for old_table, new_table in tables:
        start = time.time()
        print(f"{print_indent * ' '}- {new_table} => {old_table}... ", end="")

        ddb_con.execute(
            f"""
            INSERT OR IGNORE INTO {old_table} SELECT * FROM {new_table};
            """
        )
        ddb_con.execute(f"DROP TABLE {new_table};")

        print(f"done ({time.time() - start})")


def tags_table(sqlitedb: Database) -> Table:
    return sqlitedb.table(TAGS_TABLE_NAME)


def process(
    ddb_con: DuckDBPyConnection,
    main_module: ModuleType,
    main_table: str,
    main_tags_table: str,
    tags_main_table: str,
    print_indent=0,
):
    print()
    print(f"{print_indent * ' '}Processing data")
    print(f"{print_indent * ' '}===============")

    start = time.time()
    print(f"{print_indent * ' '}- processing... ", end="")

    ddb_con.execute(
        f"""--sql
        DROP INDEX IF EXISTS {main_table}_id_idx;

        CREATE OR REPLACE TABLE {main_table} AS
        SELECT
          DISTINCT * EXCLUDE (tags)
        FROM
          __{main_table}_downloads;

        ALTER TABLE {main_table} ALTER modifiedDate SET DATA TYPE VARCHAR;
        ALTER TABLE {main_table} ALTER postDateTime SET DATA TYPE VARCHAR;

        CREATE UNIQUE INDEX {main_table}_id_idx ON {main_table} (id);
        """
    )

    ddb_con.execute(
        f"""--sql
        CREATE OR REPLACE TEMP TABLE __{main_table}_tags_with_id AS
        SELECT
          id AS {main_module.RELATION_ID},
          unnest(tags, recursive := true)
        FROM
          __{main_table}_downloads;
        """
    )

    ddb_con.execute(
        f"""--sql
        DROP INDEX IF EXISTS {main_tags_table}_ids_idx;

        CREATE OR REPLACE TABLE {main_tags_table} AS
        SELECT
          DISTINCT
          {main_module.RELATION_ID},
          id as tag_id
        FROM
          __{main_table}_tags_with_id;

        CREATE UNIQUE INDEX {main_tags_table}_ids_idx ON {main_tags_table} ({main_module.RELATION_ID}, tag_id);
        """
    )

    ddb_con.execute(
        f"""--sql
        CREATE OR REPLACE TEMP TABLE {tags_main_table} AS
        SELECT
          * EXCLUDE ({main_module.RELATION_ID}, description),
        FROM
          __{main_table}_tags_with_id;
        """
    )

    print(f"done ({time.time() - start})")


def download(
    ddb_con: DuckDBPyConnection, urls: list[str], main_table: str, print_indent: int = 0
):
    print()
    print(f"{print_indent * ' '}Downloading")
    print(f"{print_indent * ' '}===========")
    start = time.time()
    print(f"{print_indent * ' '}- {len(urls)} files... ", end="")

    temp = "TEMP" if os.getenv("CI") else ""

    ddb_con.execute(
        f"""--sql
        CREATE OR REPLACE {temp} TABLE __{main_table}_downloads AS
        SELECT
          unnest(items, recursive := true)
        FROM
          read_json_auto(?);
        """,
        [urls],
    )

    print(f"done ({time.time() - start})")


def generate_urls(main_module: ModuleType, partitions: range | list[any]):
    urls = []
    for partition in partitions:
        # It is possible to look up the totalHits, but that requires an intital
        # query and since out of bound requests still provide a structure, we
        # can just request them and roll them into the aggregate. Its more resource
        # intensive, but put it on the todo list.

        # The maximum records size is 2000, but you can never paginate past 9999
        # total results (no matter the size set). If we look at factors of 9999
        # under 2000, a 1111 size will exactly paginate 9 times.

        # Our only requirements are that partitions need to never have more than
        # 9999 records and that their union covers all records. This can be
        # verified by comparing our total with an unpartitioned totalHits total.
        for i in range(0, 9):
            urls.append(
                f"{main_module.URL_PREFIX}&size=1111"
                f"&page={i}&tags.id={main_module.TAG_ID_PREFIX}{partition}"
            )
    return urls


def common_table_optimisations(
    sqlitedb: Database,
    main_module: ModuleType,
    print_indent=0,
):
    print()
    print(f"{print_indent * ' '}Optimising tables")
    print(f"{print_indent * ' '}=================")

    tags_table = aws_shared.tags_table(sqlitedb)
    main_table: Table = sqlitedb.table(main_module.MAIN_TABLE_NAME)
    main_tags_table: Table = sqlitedb.table(main_module.MAIN_TAGS_TABLE_NAME)

    start = time.time()
    print(f"{print_indent * ' '}- {main_tags_table.name}... ", end="")

    main_tags_table.transform(pk=[main_module.RELATION_ID, "tag_id"])
    main_tags_table.add_foreign_key(
        main_module.RELATION_ID, main_table.name, "id", ignore=True
    )
    main_tags_table.add_foreign_key("tag_id", tags_table.name, "id", ignore=True)

    print(f"done ({time.time() - start})")


def merge_duckdb_tags(
    ddb_con: DuckDBPyConnection,
    primary_tag_table: str,
    other_tag_tables: list[str],
    print_indent: int = 0,
):
    print()
    print(f"{print_indent * ' '}Merge tags")
    print(f"{print_indent * ' '}==========")

    start = time.time()
    print(
        f"{print_indent * ' '}- {', '.join([primary_tag_table] + other_tag_tables)} => {primary_tag_table}... ",
        end="",
    )

    tag_union = " UNION ".join(
        [
            f"SELECT * FROM {tag_table}"
            for tag_table in [primary_tag_table] + other_tag_tables
        ]
    )

    ddb_con.execute(
        f"""--sql
        CREATE OR REPLACE TABLE __tags AS
        SELECT
          id,
          locale,
          tagNamespaceId,
          MAX_BY(name, dateUpdated) AS name,
          MAX_BY(createdBy, dateUpdated) AS createdBy,
          MAX_BY(lastUpdatedBy, dateUpdated) AS lastUpdatedBy,
          MAX_BY(dateCreated, dateUpdated) AS dateCreated,
          MAX(dateUpdated) AS dateUpdated,
        FROM
          (
            {tag_union}
          )
        GROUP BY ALL;
        """
    )

    ddb_con.execute(
        f"""--sql
        DROP TABLE tags;

        ALTER TABLE __tags RENAME TO tags;
        """
    )

    print(f"done ({time.time() - start})")


def tag_table_optimisations(sqlitedb: Database, print_indent: int = 0):
    tags_table = aws_shared.tags_table(sqlitedb)

    print()
    print(f"{print_indent * ' '}Optimising tables")
    print(f"{print_indent * ' '}=================")
    print(f"{print_indent * ' '}- {tags_table.name}... ", end="")

    start = time.time()

    tags_table.transform(
        pk="id",
    )
    tags_table.create_index(["tagNamespaceId"])
    tags_table.create_index(["name"])
    tags_table.create_index(["tagNamespaceId", "name"], unique=True)

    print(f"done ({time.time() - start})")
