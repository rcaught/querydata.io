import time
from types import ModuleType
from typing import cast
from duckdb import DuckDBPyConnection
from sqlite_utils import Database
from sqlite_utils.db import Table
from querydataio.aws import shared as aws_shared


DIRECTORY_ID = "aws-products"
URL_PREFIX = (
    "https://aws.amazon.com/api/dirs/items/search?"
    f"item.locale=en_US&item.directoryId={DIRECTORY_ID}"
    "&sort_by=item.additionalFields.productCategory"
)
TAG_ID_PREFIX = None
MAIN_TABLE_NAME = "products"
MAIN_TAGS_TABLE_NAME = "product_tags"
RELATION_ID = "product_hash"


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
          SET
            freeTierAvailability = regexp_replace(freeTierAvailability, '<p>(.*)<\/p>.*', '\\1', 's'),
            productSummary = regexp_replace(productSummary, '<p>(.*)<\/p>.*', '\\1', 's'),
            productUrl = regexp_replace(productUrl, '\?.*', ''),
            pricingUrl = regexp_replace(pricingUrl, '\?.*', '');

        UPDATE {main_table}
          SET freeTierAvailability = NULL
        WHERE freeTierAvailability = ' ';
        """
    )


def mid_alters(ddb_con: DuckDBPyConnection, main_table: str):
    return None


def initial_sqlite_transform(
    sqlitedb: Database, main_table_name: str, print_indent: int = 0
):
    print()
    print(f"{print_indent * ' '}Optimising tables")
    print(f"{print_indent * ' '}=================")

    start = time.time()
    print(f"{print_indent * ' '}- {main_table_name}... ", end="")

    main_table = cast(Table, sqlitedb.table(main_table_name))

    main_table.transform(
        types={"launchDate": str},
        pk="hash",
    )

    main_table.create_index(["id"])
    main_table.create_index(["name"])
    main_table.create_index(["productName"])
    main_table.create_index(["productCategory"])

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
          dateCreated,
          dateUpdated,
          pricingUrl,
          freeTierAvailability,
          productSummary,
          launchDate,
          productUrl,
          productName,
          productNameLowercase,
          productCategory,
          tags
        FROM unnested;
        """
    )
