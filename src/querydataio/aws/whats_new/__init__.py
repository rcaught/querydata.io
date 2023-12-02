import time
from types import ModuleType
from duckdb import DuckDBPyConnection
from sqlite_utils import Database
from sqlite_utils.db import Table
from querydataio.aws import shared as aws_shared


DIRECTORY_ID = "whats-new"
URL_PREFIX = (
    "https://aws.amazon.com/api/dirs/items/search?"
    f"item.locale=en_US&item.directoryId={DIRECTORY_ID}"
    "&sort_by=item.additionalFields.postDateTime"
)
# https://aws.amazon.com/api/dirs/items/search?item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&sort_order=asc&size=1&item.locale=en_US&page=0&tags.id=whats-new%23year%232004|whats-new%23year%232005|whats-new%23year%232006|whats-new%23year%232007|whats-new%23year%232008|whats-new%23year%232009|whats-new%23year%232010|whats-new%23year%232011|whats-new%23year%232012|whats-new%23year%232013|whats-new%23year%232014|whats-new%23year%232015|whats-new%23year%232016|whats-new%23year%232017|whats-new%23year%232018|whats-new%23year%232019|whats-new%23year%232020|whats-new%23year%232021|whats-new%23year%232022|whats-new%23year%232023
# https://aws.amazon.com/api/dirs/items/search?item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&sort_order=asc&size=1&item.locale=en_US&page=0
# There is one post somewhere without a year tag
TAG_ID_PREFIX = "whats-new%23year%23"
FIRST_YEAR = 2004

MAIN_TABLE_NAME = "whats_new"
MAIN_TAGS_TABLE_NAME = "whats_new_tags"

RELATION_ID = "whats_new_id"


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


def initial_sqlite_transform(sqlitedb: Database, main_table: str, print_indent=0):
    print()
    print(f"{print_indent * ' '}Optimising tables")
    print(f"{print_indent * ' '}=================")

    start = time.time()
    print(f"{print_indent * ' '}- {main_table}... ", end="")

    main_table: Table = sqlitedb.table(main_table)

    main_table.transform(
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

    main_table.transform(
        pk="id",
    )
    main_table.create_index(["postDateTime"])
    main_table.create_index(["headline"])

    print(f"done ({time.time() - start})")
