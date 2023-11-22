from sqlite_utils import Database
import querydataio.aws.whats_new as whats_new
from querydataio.aws import shared as aws_shared
from querydataio import shared
from sqlite_utils.db import Table

print()
print("Full download")
print("=============")
print()

print("AWS")
print("===")

shared.delete_db(aws_shared.SQLITE_DB)

sqlitedb = Database(aws_shared.SQLITE_DB)
ddb_con = shared.init_duckdb()


aws_shared.run_full(
    sqlitedb,
    ddb_con,
    whats_new,
    range(whats_new.FIRST_YEAR, shared.current_year() + 1),
    print_indent=2,
)

aws_shared.final_tags_processing(ddb_con, sqlitedb, 2)

tags_table = aws_shared.tags_table(sqlitedb)
whats_new_table: Table = sqlitedb.table(whats_new.SQLITE_MAIN_TABLE_NAME)
whats_new_tags_table: Table = sqlitedb.table(whats_new.SQLITE_MAIN_TAGS_TABLE_NAME)

aws_shared.common_table_optimisations(
    tags_table, whats_new_tags_table, whats_new_table, whats_new.RELATION_ID, 4
)

shared.final_database_optimisations(sqlitedb, 2)
