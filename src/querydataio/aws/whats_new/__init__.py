from duckdb import DuckDBPyRelation, DuckDBPyConnection
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
    con: DuckDBPyConnection,
    all_data: list[DuckDBPyRelation],
    print_indent=0,
) -> list[pd.DataFrame]:
    result_whats_new, result_tags, result_whats_new_tags = aws_shared.process(
        con, all_data, RELATION_ID, print_indent
    )

    result_whats_new_fixed = con.sql(
        """
            CREATE OR REPLACE TEMP TABLE t1 AS
            SELECT
              * REPLACE('https://aws.amazon.com' || headlineUrl AS headlineUrl)
            FROM
              result_whats_new;

            UPDATE t1 SET postDateTime = '2021-06-23T12:30:32Z'
            WHERE postDateTime = '0202-06-23T12:30:32Z';

            SELECT * FROM t1;
        """
    ).df()

    return [result_whats_new_fixed, result_tags, result_whats_new_tags]


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
    whats_new_table: Table,
    tags_table: Table,
    whats_new_tags_table: Table,
    print_indent=0,
):
    print()
    print(f"{print_indent * ' '}Optimising tables")
    print(f"{print_indent * ' '}=================")

    whats_new_table.transform(
        pk="id",
    )
    whats_new_table.create_index(["postDateTime"])
    whats_new_table.create_index(["headline"])

    print(f"{print_indent * ' '}- {whats_new_table.name}... done")

    aws_shared.common_table_optimisations(
        tags_table, whats_new_tags_table, whats_new_table, RELATION_ID, print_indent
    )
