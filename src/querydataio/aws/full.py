from sqlite_utils import Database
import querydataio.aws.whats_new.full as whats_new
from querydataio.aws import shared as aws_shared
from querydataio import shared

print()
print("Full download")
print("=============")
print()

sqlitedb = Database(aws_shared.SQLITE_DB)
duckdb = shared.init_duckdb()

print("AWS")
print("===")

aws_shared.drop_tables([aws_shared.tags_table(sqlitedb)], print_indent=2)

whats_new.run(sqlitedb, duckdb, print_indent=2)

aws_shared.final_database_optimisations(sqlitedb, print_indent=2)
