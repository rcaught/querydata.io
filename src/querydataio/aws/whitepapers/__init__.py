import time
from types import ModuleType

from duckdb import DuckDBPyConnection
from sqlite_utils import Database
from sqlite_utils.db import Table

from querydataio.aws import shared as aws_shared

# The following tags break the normal convention. tag ids usually always match the tagNamespaceId, name combo.
# ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬───────────────────────────┬────────────────────────────────────────────┬───────┐
# │                                                                                           group_concat(id, ', ')                                                                                           │      tagNamespaceId       │                    name                    │  cnt  │
# │                                                                                                  varchar                                                                                                   │          varchar          │                  varchar                   │ int64 │
# ├────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼───────────────────────────┼────────────────────────────────────────────┼───────┤
# │ GLOBAL#solutions-use-case#uc-aer-00029, GLOBAL#solutions-use-case#uc-egd-00010, GLOBAL#solutions-use-case#uc-cpg-10001, GLOBAL#solutions-use-case#uc-agr-00005, GLOBAL#solutions-use-case#uc-ind-com-10015 │ GLOBAL#solutions-use-case │ Product Lifecycle Management               │     5 │
# │ GLOBAL#solutions-use-case#uc-ind-com-10019, GLOBAL#solutions-use-case#uc-aer-00034, GLOBAL#solutions-use-case#uc-aut-20033                                                                                 │ GLOBAL#solutions-use-case │ Computer Vision for Quality Insights       │     3 │
# │ GLOBAL#solutions-use-case#uc-ind-com-10020, GLOBAL#solutions-use-case#uc-aut-20044                                                                                                                         │ GLOBAL#solutions-use-case │ Asset Maintenance & Reliability            │     2 │
# │ GLOBAL#solutions-use-case#uc-sch-10004, GLOBAL#solutions-use-case#uc-ind-com-10002                                                                                                                         │ GLOBAL#solutions-use-case │ Demand Forecasting & Planning              │     2 │
# │ GLOBAL#solutions-use-case#uc-aut-20047, GLOBAL#solutions-use-case#uc-ind-com-10021                                                                                                                         │ GLOBAL#solutions-use-case │ Cloud Manufacturing Execution System (MES) │     2 │
# │ GLOBAL#solutions-use-case#uc-cpg-10006, GLOBAL#solutions-use-case#uc-aer-00009                                                                                                                             │ GLOBAL#solutions-use-case │ Production & Asset Optimization            │     2 │
# │ GLOBAL#solutions-use-case#uc-cpg-10003, GLOBAL#solutions-use-case#uc-ind-com-10006                                                                                                                         │ GLOBAL#solutions-use-case │ Engineering & Design Desktop               │     2 │
# │ GLOBAL#solutions-use-case#uc-agr-00004, GLOBAL#solutions-use-case#uc-aer-00031                                                                                                                             │ GLOBAL#solutions-use-case │ Engineering Design Applications & Desktops │     2 │
# └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴───────────────────────────┴────────────────────────────────────────────┴───────┘


DIRECTORY_ID = "whitepapers"
# AWS also add, tags.id=!GLOBAL#content-type#reference-arch-diagram&tags.id=!GLOBAL#content-type#reference-material
URL_PREFIX = (
    "https://aws.amazon.com/api/dirs/items/search?"
    f"item.locale=en_US&item.directoryId={DIRECTORY_ID}"
    "&sort_by=item.additionalFields.sortDate"
)
TAG_ID_PREFIX = None
MAIN_TABLE_NAME = "whitepapers"
MAIN_TAGS_TABLE_NAME = "whitepaper_tags"
RELATION_ID = "whitepaper_hash"


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
        pk="hash",
    )
    main_table.create_index(["id"])
    main_table.create_index(["name"])
    main_table.create_index(["sortDate"])
    main_table.create_index(["docTitle"])

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
          datePublished,
          sortDate,
          description,
          docTitle,
          regexp_replace(primaryURL, '\?.*', '') AS primaryURL,
          tags
        FROM unnested;
        """
    )
