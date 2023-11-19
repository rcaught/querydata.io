"""Full"""

from duckdb import DuckDBPyConnection
from sqlite_utils import Database
from sqlite_utils.db import Table

from querydataio.aws import shared as aws_shared
from querydataio.aws import blogs


def run(sqlitedb: Database, duckdb: DuckDBPyConnection, print_indent=0):
    aws_shared.run_full(
        sqlitedb,
        duckdb,
        blogs,
        blogs.aws_categories(),
        print_indent,
    )
