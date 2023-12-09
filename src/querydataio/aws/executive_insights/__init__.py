import time
from types import ModuleType
from duckdb import DuckDBPyConnection
from sqlite_utils import Database
from sqlite_utils.db import Table
from querydataio.aws import shared as aws_shared


DIRECTORY_ID = "executive-insights"
# AWS also uses, tags.id=executive-insights#region#global
URL_PREFIX = (
    "https://aws.amazon.com/api/dirs/items/search?"
    f"item.locale=en_US&item.directoryId={DIRECTORY_ID}"
    "&sort_by=item.additionalFields.sortDate"
)
TAG_ID_PREFIX = None
MAIN_TABLE_NAME = "executive_insights"
MAIN_TAGS_TABLE_NAME = "executive_insight_tags"
RELATION_ID = "executive_insight_hash"


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


def mid_alters(ddb_con: DuckDBPyConnection, main_table: str):
    None


def initial_sqlite_transform(sqlitedb: Database, main_table: str, print_indent=0):
    print()
    print(f"{print_indent * ' '}Optimising tables")
    print(f"{print_indent * ' '}=================")

    start = time.time()
    print(f"{print_indent * ' '}- {main_table}... ", end="")

    main_table: Table = sqlitedb.table(main_table)

    main_table.transform(
        types={"sortDate": str},
    )

    main_table.transform(
        pk="hash",
    )
    main_table.create_index(["id"])
    main_table.create_index(["name"])
    main_table.create_index(["headline"])
    main_table.create_index(["sortDate"])
    main_table.create_index(["category"])

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
          name,
          author,
          dateCreated,
          dateUpdated,
          headline,
          subHeadline,
          description,
          sortDate,
          headlineUrl,
          category,
          tags
        FROM unnested;
        """
    )
