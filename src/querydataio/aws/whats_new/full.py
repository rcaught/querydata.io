"""Full"""

from sqlite_utils import Database
from sqlite_utils.db import Table

from querydataio import shared
from querydataio.aws import shared as aws_shared
from querydataio.aws import whats_new


def run(sqlitedb: Database, duckdb: DuckDBPyConnection, print_indent=0):
    print()
    print(f"{print_indent * ' '}Whats New")
    print(f"{print_indent * ' '}=========")

    main_table: Table = sqlitedb.table(whats_new.SQLITE_WHATS_NEW_TABLE_NAME)
    join_table: Table = sqlitedb.table(whats_new.SQLITE_WHATS_NEW_TAGS_TABLE_NAME)

    aws_shared.drop_tables([main_table, join_table], print_indent=print_indent + 2)

    downloaded = aws_shared.download(
        duckdb,
        whats_new.URL_PREFIX,
        whats_new.TAG_ID_PREFIX,
        range(2023, shared.current_year() + 1),
        max_pages=1,
        print_indent=print_indent + 2,
    )
    result_updates, result_tags, result_joins = whats_new.process(
        duckdb, downloaded, print_indent=print_indent+2
    )

    aws_shared.to_sqlite(
        [
            (result_updates, main_table),
            (result_tags, aws_shared.tags_table(sqlitedb)),
            (result_joins, join_table),
        ],
        print_indent=print_indent + 2,
    )

    whats_new.initial_sqlite_transform(main_table)
    whats_new.final_sqlite_transform(
        sqlitedb, main_table, aws_shared.tags_table(sqlitedb), join_table
    )
