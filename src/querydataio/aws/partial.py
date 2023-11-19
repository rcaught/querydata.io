import sys
from sqlite_utils import Database
import querydataio.aws.whats_new.partial as whats_new
import querydataio.aws.blogs.partial as blogs
from querydataio.aws import shared as aws_shared
from querydataio import shared

print()
print("Partial download")
print("================")
print()

sqlitedb = Database(aws_shared.SQLITE_DB)
duckdb = shared.init_duckdb()

print("AWS")
print("===")

if not all(x.run(print_indent=2) for x in [whats_new, blogs]):
    print()
    print("No new data... hard exit to stop deploy")
    print("=======================================")
    sys.exit(1)

shared.final_database_optimisations(sqlitedb)
