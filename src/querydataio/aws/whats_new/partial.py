"""Partial"""

import sys
from sqlite_utils import Database
from sqlite_utils.db import Table

from querydataio import shared
from querydataio.aws import shared as aws_shared
from querydataio.aws import whats_new

sqlitedb = Database(aws_shared.SQLITE_DB)
duckdb = shared.init_duckdb()

print()
print(f"Partial download - last {aws_shared.PARTIAL_COLLECTION_SIZE} and merging")
print()

downloaded = aws_shared.download(
    duckdb,
    whats_new.URL_PREFIX,
    whats_new.DIRECTORY_ID,
    whats_new.TAG_ID_PREFIX,
    range(shared.current_year(), shared.current_year() + 1),
    aws_shared.PARTIAL_COLLECTION_SIZE,
    1,
)
result_updates, result_tags = whats_new.process(duckdb, downloaded)

# SQLite processing

main_new_table: Table = sqlitedb.table(whats_new.SQLITE_WHATS_NEW_TABLE_NAME + "_new")
tags_new_table: Table = sqlitedb.table(
    whats_new.SQLITE_WHATS_NEW_TAGS_TABLE_NAME + "_new"
)

aws_shared.to_sqlite(main_new_table.name, result_updates)
aws_shared.to_sqlite(tags_new_table.name, result_tags)

whats_new.initial_sqlite_transform(main_new_table)

main_table: Table = sqlitedb.table(whats_new.SQLITE_WHATS_NEW_TABLE_NAME)
tags_table: Table = sqlitedb.table(whats_new.SQLITE_WHATS_NEW_TAGS_TABLE_NAME)

main_table_count = main_table.count

for table in [
    (main_table, main_new_table),
    (tags_table, tags_new_table),
]:
    aws_shared.merge_sqlite_tables(sqlitedb, table[0], table[1])

if main_table.count == main_table_count:
    print()
    print("No new data... hard exit to stop deploy")
    print("=======================================")
    sys.exit(1)

whats_new.final_sqlite_transform(sqlitedb, main_table, tags_table)
