"""Run"""

from sqlite_utils import Database
from sqlite_utils.db import Table

from data_company_tools import shared
from data_company_tools.aws import shared as aws_shared

PARTIAL_COLLECTION_SIZE = 200

sqlitedb = Database(aws_shared.SQLITE_DB)
duckdb = shared.init_duckdb()

print()
print(f"Downloading last {PARTIAL_COLLECTION_SIZE} and merging")
print()

downloaded_years = aws_shared.download_years(
    duckdb, shared.current_year(), shared.current_year(), PARTIAL_COLLECTION_SIZE, 1
)
new_updates, new_tags = aws_shared.process(duckdb, downloaded_years)

# SQLite processing

whats_new_new_table: Table = sqlitedb.table("whats_new_new")
whats_new_tags_new_table: Table = sqlitedb.table("whats_new_tags_new")

aws_shared.to_sqlite(whats_new_new_table.name, new_updates)
aws_shared.to_sqlite(whats_new_tags_new_table.name, new_tags)

aws_shared.initial_sqlite_transform(whats_new_new_table)

whats_new_old_table: Table = sqlitedb.table(aws_shared.SQLITE_WHATS_NEW_TABLE_NAME)
whats_new_tags_old_table: Table = sqlitedb.table(
    aws_shared.SQLITE_WHATS_NEW_TAGS_TABLE_NAME
)

for table in [
    (whats_new_old_table, whats_new_new_table),
    (whats_new_tags_old_table, whats_new_tags_new_table),
]:
    aws_shared.merge_sqlite_tables(sqlitedb, table[0], table[1])

aws_shared.final_sqlite_transform(
    sqlitedb, whats_new_old_table, whats_new_tags_old_table
)
