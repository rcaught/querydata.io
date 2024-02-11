"""Shared utilities for AWS"""

import os
import time
from types import ModuleType
from typing import Sequence, cast

import pandas as pd
from duckdb import DuckDBPyConnection
from sqlite_utils import Database
from sqlite_utils.db import Table

from querydataio.aws import shared as aws_shared

MAX_RECORDS_SIZE = 1000
PARTIAL_COLLECTION_SIZE = 200
SQLITE_DB = "dbs/aws.db"
DUCKDB_DB = "dbs/aws.duckdb.db"
TAGS_TABLE_NAME = "tags"


def to_sqlite(
    sqlitedb: Database, items: list[tuple[pd.DataFrame, str]], print_indent: int = 0
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
    return cast(Table, sqlitedb.table(TAGS_TABLE_NAME))


def process(
    ddb_con: DuckDBPyConnection,
    main_module: ModuleType,
    main_table: str,
    main_tags_table: str,
    tags_main_table: str,
    print_indent: int = 0,
):
    print()
    print(f"{print_indent * ' '}Processing data")
    print(f"{print_indent * ' '}===============")

    start = time.time()
    print(f"{print_indent * ' '}- processing... ", end="")

    main_module.unnest(ddb_con, main_table)

    ddb_con.execute(
        f"""--sql
        DROP INDEX IF EXISTS {main_table}_hash_idx;

        CREATE OR REPLACE TABLE {main_table} AS
        SELECT
          DISTINCT * EXCLUDE (tags)
        FROM
          __{main_table}_unnested_downloads;
        """
    )

    main_module.mid_alters(ddb_con, main_table)

    ddb_con.execute(
        f"""--sql
        CREATE UNIQUE INDEX {main_table}_hash_idx ON {main_table} (hash);
        """
    )

    ddb_con.execute(
        f"""--sql
        CREATE OR REPLACE TEMP TABLE {tags_main_table} AS
        WITH unnested AS (
          SELECT
            md5(id)[:10] AS {main_module.RELATION_ID},
            unnest(tags, recursive := true)
          FROM
            __{main_table}_unnested_downloads
        )
        SELECT
          md5(id)[:10] AS hash,
          id,
          tagNamespaceId,
          name,
          dateUpdated,
          {main_module.RELATION_ID}
        FROM
          unnested;
        """
    )

    ddb_con.execute(
        f"""--sql
        DROP INDEX IF EXISTS {main_tags_table}_hash_idx;

        CREATE OR REPLACE TABLE {main_tags_table} AS
        SELECT
          DISTINCT
          {main_module.RELATION_ID},
          md5(id)[:10] as tag_hash
        FROM
          {tags_main_table};

        CREATE UNIQUE INDEX {main_tags_table}_hash_idx ON {main_tags_table} ({main_module.RELATION_ID}, tag_hash);
        """
    )

    ddb_con.execute(
        f"""--sql
        ALTER TABLE {tags_main_table} DROP {main_module.RELATION_ID};
        """
    )

    print(f"done ({time.time() - start})")


def download(
    ddb_con: DuckDBPyConnection,
    urls: Sequence[str | int],
    main_table: str,
    print_indent: int = 0,
) -> str:
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
          *
        FROM
          read_json_auto(?, filename = true, maximum_object_size = 33554432);
        """,
        [urls],
    )

    print(f"done ({time.time() - start})")

    return f"__{main_table}_downloads"


def getTotalHits(
    ddb_con: DuckDBPyConnection,
    main_module: ModuleType,
    partitions: Sequence[str | int],
    print_indent: int = 0,
) -> dict[str, int]:
    urls: list[str] = []

    prefix = f"{main_module.URL_PREFIX}&size=1"

    if partitions == []:
        urls.append(prefix)
    else:
        for partition in partitions:
            urls.append(f"{prefix}&tags.id={main_module.TAG_ID_PREFIX}{partition}")
    download_table = download(
        ddb_con, urls, f"{main_module.MAIN_TABLE_NAME}_total_hits", print_indent
    )

    result = ddb_con.sql(
        f"""
        SELECT
          regexp_extract(filename, 'tags.id=(.*)(&|$)', 1) as partition,
          metadata.totalHits
        FROM {download_table}
        """
    )

    return dict(result.fetchall())


def generate_urls(
    ddb_con: DuckDBPyConnection,
    main_module: ModuleType,
    partitions: Sequence[str | int],
    print_indent: int = 4,
):
    # The maximum records size is 2000, but you can never paginate past 9999
    # total results (no matter the size set). If we look at factors of 9999
    # under 2000, a 1111 size will exactly paginate 9 times.

    # Our only requirements are that partitions never have more than
    # 9999 records and that their union covers all records. This can be
    # verified by comparing our total with an unpartitioned totalHits request.

    partition_sizes = getTotalHits(ddb_con, main_module, partitions, print_indent)

    urls: list[str] = []
    for partition, size in partition_sizes.items():
        tags_id = f"&tags.id={partition}" if partition != "" else ""

        if size == 0:
            continue
        elif size < 10000:
            if size <= 8000:
                for i in range(0, int(size / 2000) + 1):
                    urls.append(
                        f"{main_module.URL_PREFIX}&size=2000"
                        f"&page={i}{tags_id}&sort_order=desc"
                    )
            else:
                for i in range(0, int(size / 1111) + 1):
                    urls.append(
                        f"{main_module.URL_PREFIX}&size=1111"
                        f"&page={i}{tags_id}&sort_order=desc"
                    )
        elif size >= 10000 and size <= 20000:
            for i in range(0, 9):
                urls.append(
                    f"{main_module.URL_PREFIX}&size=1111"
                    f"&page={i}{tags_id}&sort_order=desc"
                )
            if size <= 18000:
                for i in range(0, int((size - 10000) / 2000) + 1):
                    urls.append(
                        f"{main_module.URL_PREFIX}&size=2000"
                        f"&page={i}{tags_id}&sort_order=asc"
                    )
            else:
                for i in range(0, int((size - 10000) / 1111) + 1):
                    urls.append(
                        f"{main_module.URL_PREFIX}&size=1111"
                        f"&page={i}{tags_id}&sort_order=asc"
                    )
        else:
            raise Exception("Out of range downloads")
    return urls


def common_table_optimisations(
    sqlitedb: Database,
    main_module: ModuleType,
    print_indent: int = 0,
):
    print()
    print(f"{print_indent * ' '}Optimising tables")
    print(f"{print_indent * ' '}=================")

    tags_table = aws_shared.tags_table(sqlitedb)
    main_table = cast(Table, sqlitedb.table(main_module.MAIN_TABLE_NAME))
    main_tags_table = cast(Table, sqlitedb.table(main_module.MAIN_TAGS_TABLE_NAME))

    start = time.time()
    print(f"{print_indent * ' '}- {main_tags_table.name}... ", end="")

    main_tags_table.transform(pk=[main_module.RELATION_ID, "tag_hash"])
    main_tags_table.add_foreign_key(
        main_module.RELATION_ID, main_table.name, "hash", ignore=True
    )
    main_tags_table.add_foreign_key("tag_hash", tags_table.name, "hash", ignore=True)

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
        f"{print_indent * ' '}- {', '.join(other_tag_tables)} => {primary_tag_table}... ",
        end="",
    )

    tag_union = " UNION ".join(
        [f"SELECT * FROM {tag_table}" for tag_table in other_tag_tables]
    )

    ddb_con.execute(
        f"""--sql
        CREATE OR REPLACE TABLE __{primary_tag_table} AS
        SELECT
          hash,
          id,
          tagNamespaceId,
          MAX_BY(name, dateUpdated) AS name
        FROM
          (
            {tag_union}
          )
        GROUP BY ALL;
        """
    )

    ddb_con.execute(
        f"""--sql
        DROP TABLE IF EXISTS {primary_tag_table};

        ALTER TABLE __{primary_tag_table} RENAME TO {primary_tag_table};
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
        pk="hash",
    )
    tags_table.create_index(["id"], unique=True)
    tags_table.create_index(["tagNamespaceId"])
    tags_table.create_index(["tagNamespaceId", "name"], unique=True)

    print(f"done ({time.time() - start})")
