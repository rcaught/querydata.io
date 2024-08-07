import time
from types import ModuleType
from typing import cast

from duckdb import DuckDBPyConnection
from sqlite_utils import Database
from sqlite_utils.db import Table

from querydataio import shared
from querydataio.aws import shared as aws_shared

DIRECTORY_ID = "whats-new-v2"
URL_PREFIX = (
    "https://aws.amazon.com/api/dirs/items/search?"
    f"item.locale=en_US&item.directoryId={DIRECTORY_ID}"
    "&sort_by=item.additionalFields.postDateTime"
)
# https://aws.amazon.com/api/dirs/items/search?item.directoryId=whats-new-v2&sort_by=item.additionalFields.postDateTime&sort_order=asc&size=1&item.locale=en_US&page=0&tags.id=whats-new-v2%23year%232004|whats-new-v2%23year%232005|whats-new-v2%23year%232006|whats-new-v2%23year%232007|whats-new-v2%23year%232008|whats-new-v2%23year%232009|whats-new-v2%23year%232010|whats-new-v2%23year%232011|whats-new-v2%23year%232012|whats-new-v2%23year%232013|whats-new-v2%23year%232014|whats-new-v2%23year%232015|whats-new-v2%23year%232016|whats-new-v2%23year%232017|whats-new-v2%23year%232018|whats-new-v2%23year%232019|whats-new-v2%23year%232020|whats-new-v2%23year%232021|whats-new-v2%23year%232022|whats-new-v2%23year%232023
# https://aws.amazon.com/api/dirs/items/search?item.directoryId=whats-new-v2&sort_by=item.additionalFields.postDateTime&sort_order=asc&size=1&item.locale=en_US&page=0
# There is one post somewhere without a year tag
TAG_ID_PREFIX = "whats-new-v2%23year%23"
FIRST_YEAR = 2004
MAIN_TABLE_NAME = "whats_new"
MAIN_TAGS_TABLE_NAME = "whats_new_tags"
RELATION_ID = "whats_new_hash"


def process(
    ddb_con: DuckDBPyConnection,
    main_module: ModuleType,
    main_table: str,
    main_tags_table: str,
    tags_main_table: str,
    print_indent: int = 0,
):
    aws_shared.process(
        ddb_con,
        main_module,
        main_table,
        main_tags_table,
        tags_main_table,
        print_indent,
    )

    ddb_con.execute(
        f"""
        UPDATE {main_table}
          SET headlineUrl = 'https://aws.amazon.com' || headlineUrl;

        UPDATE {main_table}
          SET postDateTime = '2021-06-23T12:30:32Z'
        WHERE postDateTime = '0202-06-23T12:30:32Z';
        """
    )


def mid_alters(ddb_con: DuckDBPyConnection, main_table: str):
    ddb_con.execute(
        f"""
        ALTER TABLE {main_table} ALTER modifiedDate SET DATA TYPE VARCHAR;
        ALTER TABLE {main_table} ALTER postDateTime SET DATA TYPE VARCHAR;
        """
    )


def initial_sqlite_transform(sqlitedb: Database, main_table_name: str, print_indent=0):
    print()
    print(f"{print_indent * ' '}Optimising tables")
    print(f"{print_indent * ' '}=================")

    start = time.time()
    print(f"{print_indent * ' '}- {main_table_name}... ", end="")

    main_table = cast(Table, sqlitedb.table(main_table_name))

    main_table.transform(
        types={
            "postDateTime": str,
            "dateCreated": str,
            "dateUpdated": str,
            "modifiedDate": str,
        },
        pk="hash",
    )

    main_table.create_index(["id"])
    main_table.create_index(["postDateTime"])
    main_table.create_index(["headline"])

    print(f"done ({time.time() - start})")


def unnest(ddb_con: DuckDBPyConnection, main_table: str):
    ddb_con.execute(
        f"""--sql
        CREATE OR REPLACE TEMP TABLE __{main_table}_unnested_downloads AS
        WITH unnested AS (
          SELECT
            unnest(items, recursive := true)
          FROM
            __{main_table}_downloads
        )
        SELECT
          md5(id)[:10] as hash,
          id,
          postDateTime,
          headline,
          headlineUrl,
          postSummary,
          postBody,
          dateCreated,
          dateUpdated,
          modifiedDate,
          tags
        FROM unnested;
        """
    )


def all_years() -> range:
    return range(FIRST_YEAR, shared.current_year() + 1)
