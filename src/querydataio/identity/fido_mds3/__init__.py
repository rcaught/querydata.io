# import time
# from types import ModuleType

# from duckdb import DuckDBPyConnection
# from sqlite_utils import Database
# from sqlite_utils.db import Table

# MAIN_TABLE_NAME = "fido_metadata_service"


# def initial_sqlite_transform(sqlitedb: Database, main_table_name: str, print_indent=0):
#     print()
#     print(f"{print_indent * ' '}Optimising tables")
#     print(f"{print_indent * ' '}=================")

#     start = time.time()
#     print(f"{print_indent * ' '}- {main_table_name}... ", end="")

#     main_table: Table = sqlitedb.table(main_table_name)

#     main_table.transform(
#         pk="hash",
#     )
#     main_table.create_index(["id"])
#     main_table.create_index(["name"])
#     main_table.create_index(["sortDate"])
#     main_table.create_index(["docTitle"])

#     print(f"done ({time.time() - start})")


# def unnest(ddb_con: DuckDBPyConnection, main_table: str):
#     ddb_con.execute(
#         f"""--sql
#         CREATE OR REPLACE TEMP TABLE __{main_table}_unnested_downloads AS
#         WITH unnested AS (
#           SELECT
#             unnest(items, recursive := true)
#           FROM
#             __{main_table}_downloads
#         )
#         SELECT
#           md5(id)[:10] as hash,
#           id,
#           name,
#           author,
#           dateCreated,
#           dateUpdated,
#           datePublished,
#           sortDate,
#           description,
#           docTitle,
#           regexp_replace(primaryURL, '\?.*', '') AS primaryURL,
#           tags
#         FROM unnested;
#         """
#     )
