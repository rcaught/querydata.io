import pytest
from pytest_mock import MockerFixture
from querydataio import shared
from querydataio.aws import (
    analyst_reports,
    whats_new,
)
from querydataio.aws.full import FullRun

from tests import test_utils


@pytest.fixture(autouse=True)
def run_around_tests():
    test_utils.clean_test_dbs()

    yield


def run_full(fixtures_create: bool = False) -> str:
    if fixtures_create:
        config = {"disabled_filesystems": "HTTPFileSystem"}
    else:
        config = {}

    full_run = FullRun(
        {
            "database": "tests/dbs/aws.duckdb.db",
            "config": config,
        },
        {
            f"tests/dbs/aws_{whats_new.MAIN_TABLE_NAME}.sqlite3": [
                {whats_new: range(shared.current_year(), shared.current_year() + 1)}
            ],
            "tests/dbs/aws_general.sqlite3": [
                {analyst_reports: []},
            ],
        },
        fixtures_use=True,
        fixtures_create=fixtures_create,
    )

    full_run.prepare().unwrap_or_raise(SystemExit)
    full_run.run().unwrap_or_raise(SystemExit)

    return full_run.ddb_name


def test_integration() -> None:
    # Change this to recreate fixtures
    fixtures_create = False

    run_full(fixtures_create)

    test_utils.assert_query_result(
        "tests/dbs/aws_whats_new.sqlite3",
        "SELECT * FROM whats_new ORDER BY id",
        fixtures_create,
    )

    test_utils.assert_query_result(
        "tests/dbs/aws_general.sqlite3",
        "SELECT * FROM analyst_reports ORDER BY id",
        fixtures_create,
    )
