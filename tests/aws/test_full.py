import pytest
from pytest_mock import MockerFixture
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


def run_full(mocker: MockerFixture) -> str:
    test_utils.download_side_effect(
        mocker,
        {
            "whats_new_total_hits": "tests/fixtures/aws/full/whats_new/download.1.json",
            "whats_new": "tests/fixtures/aws/full/whats_new/download.2.json",
            "analyst_reports_total_hits": "tests/fixtures/aws/full/analyst_reports/download.1.json",
            "analyst_reports": "tests/fixtures/aws/full/analyst_reports/download.2.json",
        },
    )

    full_run = FullRun(
        {
            "database": "tests/dbs/aws.duckdb.db",
            "config": {"disabled_filesystems": "HTTPFileSystem"},
        },
        {
            f"tests/dbs/aws_{whats_new.MAIN_TABLE_NAME}.sqlite3": [
                {whats_new: whats_new.all_years()}
            ],
            "tests/dbs/aws_general.sqlite3": [
                {analyst_reports: []},
            ],
        },
    )

    full_run.prepare().unwrap_or_raise(SystemExit)
    full_run.run().unwrap_or_raise(SystemExit)

    return full_run.ddb_name


def test_integration(mocker: MockerFixture) -> None:
    run_full(mocker)

    test_utils.assert_query_result(
        "tests/dbs/aws_whats_new.sqlite3",
        "SELECT * FROM whats_new ORDER BY id",
        "tests/fixtures/aws/full/whats_new/query.1.json",
    )

    test_utils.assert_query_result(
        "tests/dbs/aws_general.sqlite3",
        "SELECT * FROM analyst_reports ORDER BY id",
        "tests/fixtures/aws/full/analyst_reports/query.1.json",
    )
