import duckdb
import pandas as pd
from sqlite_utils import Database
from sqlite_utils.db import Table


DIRECTORY_ID = "whats-new"
URL_PREFIX = (
    "https://aws.amazon.com/api/dirs/items/search?"
    f"item.locale=en_US&item.directoryId={DIRECTORY_ID}"
    "&sort_by=item.additionalFields.postDateTime"
    "&sort_order=desc"
)
TAG_ID_PREFIX = "whats-new%23year%23"

FIRST_YEAR = 2004

SQLITE_WHATS_NEW_TABLE_NAME = "whats_new"
SQLITE_WHATS_NEW_TAGS_TABLE_NAME = "whats_new_tags"


def process(
    con: duckdb.DuckDBPyConnection,
    all_data: list[duckdb.DuckDBPyRelation],
    print_indent=0,
) -> list[pd.DataFrame]:
    """Clean and transform into a Dataframe"""

    processed_whats_new: list[pd.DataFrame] = []
    processed_tags: list[pd.DataFrame] = []
    processed_whats_new_tags: list[pd.DataFrame] = []

    print()
    print(f"{print_indent * ' '}Processing data")
    print(f"{print_indent * ' '}===============")

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

        whats_new = con.sql(
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

        tags_with_id = con.sql(
            """
                SELECT
                  id as whats_new_id,
                  unnest(tags, recursive := true)
                FROM
                  unnested_items;
            """
        )

        whats_new_tags = con.sql(
            """
                SELECT
                  whats_new_id,
                  id as tag_id
                FROM
                  tags_with_id;
            """
        )

        tags = con.sql(
            """
                SELECT DISTINCT * EXCLUDE (whats_new_id)
                FROM
                  tags_with_id;
            """
        )

        processed_whats_new.append(whats_new.df())
        processed_tags.append(tags.df())
        processed_whats_new_tags.append(whats_new_tags.df())

    result_whats_new = pd.concat(processed_whats_new, ignore_index=True)
    result_tags = pd.concat(processed_tags, ignore_index=True)
    result_whats_new_tags = pd.concat(processed_whats_new_tags, ignore_index=True)

    result_tags_distinct = con.sql(
        """
            SELECT DISTINCT *
            FROM
              result_tags;
        """
    ).df()

    return [result_whats_new, result_tags_distinct, result_whats_new_tags]


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

    tags_table.transform(pk="id")
    tags_table.create_index(["tagNamespaceId"])
    tags_table.create_index(["name"])
    tags_table.create_index(["tagNamespaceId", "name"])

    print(f"{print_indent * ' '}- {tags_table.name}... done")

    whats_new_tags_table.transform(pk=[whats_new_table.name + "_id", "tag_id"])
    whats_new_tags_table.add_foreign_key(
        f"{whats_new_table.name}_id", whats_new_table.name, "id", ignore=True
    )
    whats_new_tags_table.add_foreign_key(f"tag_id", tags_table.name, "id", ignore=True)

    print(f"{print_indent * ' '}- {whats_new_tags_table.name}... done")
