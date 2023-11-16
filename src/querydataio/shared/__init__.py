"""Shared"""

import datetime
import duckdb


def current_year() -> int:
    """Current year"""
    return datetime.datetime.now().year


def init_duckdb() -> duckdb.DuckDBPyConnection:
    """DuckDB"""
    return duckdb.connect(database=":memory:")
