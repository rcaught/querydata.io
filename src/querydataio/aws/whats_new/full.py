"""Full"""

from sqlite_utils import Database
from sqlite_utils.db import Table

from querydataio import shared
from querydataio.aws import shared as aws_shared
from querydataio.aws import whats_new

print()
print("Full download")
print()

sqlitedb = Database(aws_shared.SQLITE_DB)
duckdb = shared.init_duckdb()

main_table: Table = sqlitedb.table(whats_new.SQLITE_WHATS_NEW_TABLE_NAME)
tags_table: Table = sqlitedb.table(whats_new.SQLITE_WHATS_NEW_TAGS_TABLE_NAME)

print()
print("Dropping tables")
print("===============")

main_table.drop(ignore=True)
print(f"{main_table.name}... done")
tags_table.drop(ignore=True)
print(f"{tags_table.name}... done")

downloaded_years = aws_shared.download(
    duckdb,
    whats_new.URL_PREFIX,
    whats_new.DIRECTORY_ID,
    whats_new.TAG_ID_PREFIX,
    range(whats_new.FIRST_YEAR, shared.current_year() + 1),
)
result_updates, result_tags = whats_new.process(duckdb, downloaded_years)

aws_shared.to_sqlite(main_table.name, result_updates)
aws_shared.to_sqlite(tags_table.name, result_tags)

whats_new.initial_sqlite_transform(main_table)
whats_new.final_sqlite_transform(sqlitedb, main_table, tags_table)
