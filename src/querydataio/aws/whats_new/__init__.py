from types import ModuleType
from duckdb import DuckDBPyConnection
import pandas as pd
from sqlite_utils.db import Table
from querydataio.aws import shared as aws_shared


DIRECTORY_ID = "whats-new"
URL_PREFIX = (
    "https://aws.amazon.com/api/dirs/items/search?"
    f"item.locale=en_US&item.directoryId={DIRECTORY_ID}"
    "&sort_by=item.additionalFields.postDateTime"
    "&sort_order=desc"
)
TAG_ID_PREFIX = "whats-new%23year%23"
FIRST_YEAR = 2004

SQLITE_MAIN_TABLE_NAME = "whats_new"
SQLITE_MAIN_TAGS_TABLE_NAME = "whats_new_tags"

RELATION_ID = "whats_new_id"


def process(
    ddb_con: DuckDBPyConnection,
    main_module: ModuleType,
    urls: list[str],
    print_indent=0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    main_table, main_tags_table = aws_shared.process(
        ddb_con, main_module, urls, print_indent
    )

    main_table = ddb_con.sql(
        """
            CREATE OR REPLACE TEMP TABLE t1 AS
            SELECT
              * REPLACE('https://aws.amazon.com' || headlineUrl AS headlineUrl)
            FROM
              main_table;

            UPDATE t1 SET postDateTime = '2021-06-23T12:30:32Z'
            WHERE postDateTime = '0202-06-23T12:30:32Z';

            SELECT * FROM t1;
        """
    )

    return main_table.df(), main_tags_table.df()


def initial_sqlite_transform(whats_new_table: Table, print_indent=0):
    print()
    print(f"{print_indent * ' '}Optimising tables")
    print(f"{print_indent * ' '}=================")

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

    whats_new_table.transform(
        pk="id",
    )
    whats_new_table.create_index(["postDateTime"])
    whats_new_table.create_index(["headline"])

    print(f"{print_indent * ' '}- {whats_new_table.name}... done")
