from sqlite_utils import Database
import querydataio.aws.whats_new.full as whats_new
from querydataio.aws import shared as aws_shared
from querydataio import shared
import querydataio.aws.blogs.full as blogs

print()
print("Full download")
print("=============")
print()

print("AWS")
print("===")

shared.delete_db(aws_shared.SQLITE_DB)

sqlitedb = Database(aws_shared.SQLITE_DB)
duckdb = shared.init_duckdb()

whats_new.run(sqlitedb, duckdb, print_indent=2)
blogs.run(sqlitedb, duckdb, print_indent=2)

aws_shared.final_database_optimisations(sqlitedb, print_indent=2)
