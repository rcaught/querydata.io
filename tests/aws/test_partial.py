import glob
import os
import duckdb
from pytest_mock import MockerFixture
from tests import test_utils
from querydataio.aws import (
    analyst_reports,
    whats_new,
)
from querydataio.aws import shared as aws_shared
from querydataio.aws.partial import PartialRun
from tests.aws.test_full import run_full
import pytest


@pytest.fixture(autouse=True)
def run_around_tests():
    for file in glob.glob("tests/dbs/*.*"):
        os.remove(file)

    yield


default_config = [
    {
        "database": "tests/dbs/aws.duckdb.db",
        "config": {"disabled_filesystems": "HTTPFileSystem"},
    },
    {
        f"tests/dbs/aws_{whats_new.MAIN_TABLE_NAME}.sqlite3": [
            {
                whats_new: [
                    f"{whats_new.URL_PREFIX}&sort_order=desc&size={aws_shared.PARTIAL_COLLECTION_SIZE}"
                ]
            }
        ],
        "tests/dbs/aws_general.sqlite3": [
            {
                analyst_reports: [
                    f"{analyst_reports.URL_PREFIX}&sort_order=desc&size={aws_shared.PARTIAL_COLLECTION_SIZE}"
                ]
            },
        ],
    },
]

default_side_effect = {
    "whats_new_new": "tests/fixtures/aws/partial/whats_new/download.1.json",
    "analyst_reports_new": "tests/fixtures/aws/partial/analyst_reports/download.1.json",
}


def test_integration_no_updates(mocker: MockerFixture) -> None:
    run_full(mocker)

    mocker.resetall(side_effect=True)

    test_utils.download_side_effect(mocker, default_side_effect)

    partial_run = PartialRun(*default_config)

    partial_run.prepare()

    with pytest.raises(SystemExit, match="2"):
        partial_run.run()


def test_integration_handles_no_preexisting_database(mocker: MockerFixture) -> None:
    with pytest.raises(SystemExit, match="3"):
        PartialRun(*default_config)


def test_integration_handles_no_preexisting_directory_table(
    mocker: MockerFixture,
) -> None:
    ddb_name = run_full(mocker)

    duckdb.connect(ddb_name).execute(f"DROP TABLE {whats_new.MAIN_TABLE_NAME};").close()

    mocker.resetall(side_effect=True)

    test_utils.download_side_effect(mocker, default_side_effect)

    with pytest.raises(SystemExit, match="4"):
        PartialRun(*default_config)


def test_integration_success(mocker: MockerFixture) -> None:
    test_utils.download_side_effect(mocker, default_side_effect)

    partial_run = PartialRun(*default_config)

    partial_run.prepare()
    partial_run.run()
