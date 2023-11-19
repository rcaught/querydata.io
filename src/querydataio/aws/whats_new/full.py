"""Full"""

from duckdb import DuckDBPyConnection
from sqlite_utils import Database

from querydataio import shared
from querydataio.aws import shared as aws_shared
from querydataio.aws import whats_new


def run(sqlitedb: Database, duckdb: DuckDBPyConnection, print_indent=0):
    aws_shared.run_full(
        sqlitedb,
        duckdb,
        whats_new,
        range(whats_new.FIRST_YEAR, shared.current_year() + 1),
        print_indent,
    )
