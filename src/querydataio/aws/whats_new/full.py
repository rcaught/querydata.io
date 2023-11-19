"""Full"""

from duckdb import DuckDBPyConnection
from sqlite_utils import Database
from sqlite_utils.db import Table

from querydataio import shared
from querydataio.aws import shared as aws_shared
from querydataio.aws import whats_new


def run(sqlitedb: Database, duckdb: DuckDBPyConnection, print_indent=0):
    print()
    print(f"{print_indent * ' '}Whats New")
    print(f"{print_indent * ' '}=========")

    whats_new_table: Table = sqlitedb.table(whats_new.SQLITE_WHATS_NEW_TABLE_NAME)
    whats_new_tags_table: Table = sqlitedb.table(
        whats_new.SQLITE_WHATS_NEW_TAGS_TABLE_NAME
    )
    tags_table = aws_shared.tags_table(sqlitedb)

    downloaded = aws_shared.download(
        duckdb,
        whats_new.URL_PREFIX,
        whats_new.TAG_ID_PREFIX,
        range(whats_new.FIRST_YEAR, shared.current_year() + 1),
        print_indent=print_indent + 2,
    )

    result_whats_new, result_tags, result_whats_new_tags = whats_new.process(
        duckdb, downloaded, print_indent=print_indent + 2
    )

    aws_shared.to_sqlite(
        [
            (result_whats_new, whats_new_table),
            (result_tags, tags_table),
            (result_whats_new_tags, whats_new_tags_table),
        ],
        print_indent=print_indent + 2,
    )

    whats_new.initial_sqlite_transform(whats_new_table)
    whats_new.final_sqlite_transform(
        whats_new_table,
        tags_table,
        whats_new_tags_table,
        print_indent=print_indent + 2,
    )
