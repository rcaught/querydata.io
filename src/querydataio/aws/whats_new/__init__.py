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