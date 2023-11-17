"""Full"""

import os
from sqlite_utils import Database
from sqlite_utils.db import Table

from querydataio import shared
from querydataio.aws import shared as aws_shared
from querydataio.aws import whats_new

if os.path.exists(aws_shared.SQLITE_DB):
    os.remove(aws_shared.SQLITE_DB)
else:
    print(f"WARNING: cannot remove {aws_shared.SQLITE_DB}")

sqlitedb = Database(aws_shared.SQLITE_DB)
duckdb = shared.init_duckdb()

print()
print("Full download")
print()

downloaded_years = aws_shared.download(
    duckdb,
    whats_new.URL_PREFIX,
    whats_new.DIRECTORY_ID,
    whats_new.TAG_ID_PREFIX,
    range(whats_new.FIRST_YEAR, shared.current_year() + 1)
)
result_updates, result_tags = whats_new.process(duckdb, downloaded_years)

whats_new_table: Table = sqlitedb.table(whats_new.SQLITE_WHATS_NEW_TABLE_NAME)
whats_new_tags_table: Table = sqlitedb.table(
    whats_new.SQLITE_WHATS_NEW_TAGS_TABLE_NAME
)

aws_shared.to_sqlite(whats_new_table.name, result_updates)
aws_shared.to_sqlite(whats_new_tags_table.name, result_tags)

whats_new.initial_sqlite_transform(whats_new_table)
whats_new.final_sqlite_transform(sqlitedb, whats_new_table, whats_new_tags_table)
