"""Shared utilities for AWS"""

import sqlite3
from types import ModuleType
import pandas as pd
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from sqlite_utils import Database
from sqlite_utils.db import Table
from querydataio.aws import shared as aws_shared

MAX_RECORDS_SIZE = 1000
PARTIAL_COLLECTION_SIZE = 200
SQLITE_DB = "dbs/aws.db"
SQLITE_TAGS_TABLE_NAME = "tags"


def to_sqlite(
    sqlitedb: Database, items: list[tuple[pd.DataFrame, Table]], print_indent=0
):
    """Export Dataframe to SQLite"""

    print("")
    print(f"{print_indent * ' '}Export to SQLite")
    print(f"{print_indent * ' '}================")

    for data, table in items:
        data.to_sql(table.name, sqlitedb.conn, if_exists="replace", index=False)

        print(f"{print_indent * ' '}- {table.name}... done")


def merge_sqlite_tables(
    sqlitedb: Database, tables: list[tuple[Table, Table]], print_indent=0
):
    print()
    print(f"{print_indent * ' '}Merging")
    print(f"{print_indent * ' '}=======")

    for old_table, new_table in tables:
        sqlitedb.execute(
            f"""
            INSERT OR REPLACE INTO {old_table.name} SELECT * FROM {new_table.name};
            """
        )
        sqlitedb.execute(f"DROP TABLE {new_table.name};")

        print(f"{print_indent * ' '}- {new_table.name} => {old_table.name}... done")


def tags_table(sqlitedb: Database) -> Table:
    return sqlitedb.table(SQLITE_TAGS_TABLE_NAME)


def run_full(
    sqlitedb: Database,
    duckdb: DuckDBPyConnection,
    main_module: ModuleType,
    partitions: range | list[any],
    print_indent=0,
):
    print()
    print(f"{print_indent * ' '}{main_module.DIRECTORY_ID}")
    print(f"{print_indent * ' '}{len(main_module.DIRECTORY_ID) * '='}")

    main_table: Table = sqlitedb.table(main_module.SQLITE_MAIN_TABLE_NAME)
    main_tags_table: Table = sqlitedb.table(main_module.SQLITE_MAIN_TAGS_TABLE_NAME)

    urls = generate_urls(main_module, partitions)

    main_table_df, main_tags_table_df = main_module.process(
        duckdb, main_module, urls, print_indent + 2
    )

    aws_shared.to_sqlite(
        sqlitedb,
        [
            (main_table_df, main_table),
            (main_tags_table_df, main_tags_table),
        ],
        print_indent + 2,
    )

    main_module.initial_sqlite_transform(main_table, print_indent + 2)


def process(
    duckdb: DuckDBPyConnection, main_module: ModuleType, urls: list[str], print_indent=0
) -> tuple[DuckDBPyRelation, DuckDBPyRelation]:
    print()
    print(f"{print_indent * ' '}Downloading")
    print(f"{print_indent * ' '}===========")
    print(f"{print_indent * ' '}- {len(urls)} files")

    duckdb.execute(
        f"""--sql
        CREATE OR REPLACE TEMP TABLE __{main_module.SQLITE_MAIN_TABLE_NAME}_downloads AS
        SELECT
          unnest(items, recursive := true)
        FROM
          read_json_auto(?);
        """,
        [urls],
    )

    print(f"{print_indent * ' '}- {len(urls)} files... done")
    print()
    print(f"{print_indent * ' '}Processing data")
    print(f"{print_indent * ' '}===============")

    main_table = duckdb.sql(
        f"""--sql
        SELECT
          DISTINCT * EXCLUDE (tags)
        FROM
          __{main_module.SQLITE_MAIN_TABLE_NAME}_downloads;
        """
    )

    duckdb.execute(
        f"""--sql
        CREATE OR REPLACE TEMP TABLE __{main_module.SQLITE_MAIN_TABLE_NAME}_tags_with_id AS
        SELECT
          id AS {main_module.RELATION_ID},
          unnest(tags, recursive := true)
        FROM
          __{main_module.SQLITE_MAIN_TABLE_NAME}_downloads;
        """
    )

    main_tags_table = duckdb.sql(
        f"""--sql
        SELECT
          DISTINCT
          {main_module.RELATION_ID},
          id as tag_id
        FROM
          __{main_module.SQLITE_MAIN_TABLE_NAME}_tags_with_id;
        """
    )

    duckdb.execute(
        f"""--sql
        CREATE OR REPLACE TEMP TABLE tags_{main_module.SQLITE_MAIN_TABLE_NAME} AS
        SELECT
          * EXCLUDE ({main_module.RELATION_ID}, description),
        FROM
          __{main_module.SQLITE_MAIN_TABLE_NAME}_tags_with_id
        """
    )

    print(f"{print_indent * ' '}- processing... done")

    return main_table, main_tags_table


def generate_urls(main_module: ModuleType, partitions: range | list[any]):
    urls = []
    for partition in partitions:
        # It is possible to look up the totalHits, but that requires an intital
        # query and since out of bound requests still provide a structure, we
        # can just request them and roll them into the aggregate. Its more resource
        # intensive, but put it on the todo list.

        # The maximum records size is 2000, but you can never paginate past 9999
        # total results (no matter the size set). If we look at factors of 9999
        # under 2000, a 1111 size will exactly paginate 9 times.

        # Our only requirements are that partitions need to never have more than
        # 9999 records and that their union covers all records. This can be
        # verified by comparing our total with an unpartitioned totalHits total.
        for i in range(0, 9):
            urls.append(
                f"{main_module.URL_PREFIX}&size=1111"
                f"&page={i}&tags.id={main_module.TAG_ID_PREFIX}{partition}"
            )
    return urls


# def run_partial(
#     main_module: ModuleType,
#     tag_id_prefix: str | None,
#     partitions: range | list[any] = [],
#     print_indent=0,
# ):
#     print()
#     print(f"{print_indent * ' '}{main_module.DIRECTORY_ID}")
#     print(f"{print_indent * ' '}{len(main_module.DIRECTORY_ID) * '='}")

#     sqlitedb = Database(aws_shared.SQLITE_DB)
#     duckdb = shared.init_duckdb()

#     print(
#         f"{print_indent * ' '}- downloading last {aws_shared.PARTIAL_COLLECTION_SIZE} and merging"
#     )

#     downloaded = aws_shared.download(
#         duckdb,
#         main_module.URL_PREFIX,
#         tag_id_prefix,
#         partitions,
#         aws_shared.PARTIAL_COLLECTION_SIZE,
#         1,
#         print_indent=print_indent + 2,
#     )

#     result_main, result_tags, result_main_tags = main_module.process(
#         duckdb, downloaded, print_indent + 2
#     )

#     # SQLite processing of new tables
#     # NOTE: more would be done in duckDB, but errors on column reordering were happening

#     main_new_table: Table = sqlitedb.table(main_module.SQLITE_MAIN_TABLE_NAME + "_new")
#     tags_new_table: Table = sqlitedb.table(aws_shared.SQLITE_TAGS_TABLE_NAME + "_new")
#     main_tags_new_table: Table = sqlitedb.table(
#         main_module.SQLITE_MAIN_TAGS_TABLE_NAME + "_new"
#     )

#     aws_shared.to_sqlite(
#         [
#             (result_main, main_new_table),
#             (result_tags, tags_new_table),
#             (result_main_tags, main_tags_new_table),
#         ],
#         print_indent=print_indent + 2,
#     )

#     main_module.initial_sqlite_transform(main_new_table)

#     # Merge into existing

#     main_table: Table = sqlitedb.table(main_module.SQLITE_MAIN_TABLE_NAME)
#     tags_table = aws_shared.tags_table(sqlitedb)
#     main_tags_table: Table = sqlitedb.table(main_module.SQLITE_MAIN_TAGS_TABLE_NAME)

#     main_table_count = main_table.count

#     aws_shared.merge_sqlite_tables(
#         sqlitedb,
#         [
#             (main_table, main_new_table),
#             (tags_table, tags_new_table),
#             (main_tags_table, main_tags_new_table),
#         ],
#         print_indent=print_indent + 2,
#     )

#     if main_table.count == main_table_count:
#         return False

#     main_module.final_sqlite_transform(
#         main_table, tags_table, main_tags_table, print_indent=print_indent + 2
#     )

#     return True


def common_table_optimisations(
    tags_table: Table,
    main_tags_table: Table,
    main_table: Table,
    relation_id: str,
    print_indent=0,
):
    print()
    print(f"{print_indent * ' '}Optimising tables")
    print(f"{print_indent * ' '}=================")

    main_tags_table.transform(pk=[relation_id, "tag_id"])
    main_tags_table.add_foreign_key(relation_id, main_table.name, "id", ignore=True)
    main_tags_table.add_foreign_key("tag_id", tags_table.name, "id", ignore=True)

    print(f"{print_indent * ' '}- {main_tags_table.name}... done")


def final_tags_processing(
    ddb_con: DuckDBPyConnection, sqlitedb: Database, print_indent=0
):
    print()
    print(f"{print_indent * ' '}Final tags processing")
    print(f"{print_indent * ' '}=====================")

    tags_df = ddb_con.sql(
        f"""--sql
        SELECT
          * EXCLUDE (dateUpdated, lastUpdatedBy, name),
          MAX(dateUpdated) AS dateUpdated,
          MAX_BY(lastUpdatedBy, dateUpdated) AS lastUpdatedBy,
          MAX_BY(name, dateUpdated) AS name
        FROM
          (
            SELECT * FROM tags_whats_new
            -- UNION others here
          )
        GROUP BY ALL;
        """
    ).df()

    tags_table = aws_shared.tags_table(sqlitedb)

    aws_shared.to_sqlite(
        sqlitedb,
        [
            (tags_df, tags_table),
        ],
        4,
    )

    tags_table.transform(pk="id")
    tags_table.create_index(["tagNamespaceId"])
    tags_table.create_index(["name"])
    tags_table.create_index(["tagNamespaceId", "name"])
