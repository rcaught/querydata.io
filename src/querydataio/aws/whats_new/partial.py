"""Partial"""

from sqlite_utils import Database
from sqlite_utils.db import Table

from querydataio import shared
from querydataio.aws import shared as aws_shared
from querydataio.aws import whats_new


def run(print_indent=0) -> bool:
    print()
    print(f"{print_indent * ' '}Whats New")
    print(f"{print_indent * ' '}=========")

    sqlitedb = Database(aws_shared.SQLITE_DB)
    duckdb = shared.init_duckdb()

    print(
        f"{print_indent * ' '}- downloading last {aws_shared.PARTIAL_COLLECTION_SIZE} and merging"
    )

    downloaded = aws_shared.download(
        duckdb,
        whats_new.URL_PREFIX,
        whats_new.TAG_ID_PREFIX,
        range(shared.current_year(), shared.current_year() + 1),
        aws_shared.PARTIAL_COLLECTION_SIZE,
        1,
        print_indent=print_indent + 2,
    )

    result_whats_new, result_tags, result_whats_new_tags = whats_new.process(
        duckdb, downloaded, "whats_new_id", print_indent + 2
    )

    # SQLite processing of new tables
    # NOTE: more would be done in duckDB, but errors on column reordering were happening

    whats_new_new_table: Table = sqlitedb.table(
        whats_new.SQLITE_WHATS_NEW_TABLE_NAME + "_new"
    )
    tags_new_table: Table = sqlitedb.table(aws_shared.SQLITE_TAGS_TABLE_NAME + "_new")
    whats_new_tags_new_table: Table = sqlitedb.table(
        whats_new.SQLITE_WHATS_NEW_TAGS_TABLE_NAME + "_new"
    )

    aws_shared.to_sqlite(
        [
            (result_whats_new, whats_new_new_table),
            (result_tags, tags_new_table),
            (result_whats_new_tags, whats_new_tags_new_table),
        ],
        print_indent=print_indent + 2,
    )

    whats_new.initial_sqlite_transform(whats_new_new_table)

    # Merge into existing

    whats_new_table: Table = sqlitedb.table(whats_new.SQLITE_WHATS_NEW_TABLE_NAME)
    tags_table = aws_shared.tags_table(sqlitedb)
    whats_new_tags_table: Table = sqlitedb.table(
        whats_new.SQLITE_WHATS_NEW_TAGS_TABLE_NAME
    )

    whats_new_table_count = whats_new_table.count

    aws_shared.merge_sqlite_tables(
        sqlitedb,
        [
            (whats_new_table, whats_new_new_table),
            (tags_table, tags_new_table),
            (whats_new_tags_table, whats_new_tags_new_table),
        ],
        print_indent=print_indent + 2,
    )

    if whats_new_table.count == whats_new_table_count:
        return False

    whats_new.final_sqlite_transform(
        whats_new_table, tags_table, whats_new_tags_table, print_indent=print_indent + 2
    )

    return True
