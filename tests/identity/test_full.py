import pytest
from pytest_mock import MockerFixture
from querydataio.identity import fido_mds3
from querydataio.identity.full import FullRun
import os
from unittest.mock import Mock

from tests import test_utils


@pytest.fixture(autouse=True)
def run_around_tests():
    test_utils.clean_test_dbs()

    yield


def run_full(mocker: MockerFixture) -> str:
    download = mocker.patch("requests.get")

    class Response:
        def __init__(self, content: bytes):
            self.__dict__.update(content=content)

    def download_mds3() -> Response:
        file_path = os.path.join("tests", "fixtures", "identity", "blob.jwt")
        with open(file_path, "rb") as file:
            response = Response(content=file.read())
            return response

    def download_cert() -> Response:
        file_path = os.path.join("tests", "fixtures", "identity", "root-r3.crt")
        with open(file_path, "rb") as file:
            response = Response(content=file.read())
            return response

    download.side_effect = [download_mds3(), download_cert()]

    full_run = FullRun(
        {
            "database": "tests/dbs/identity.duckdb.db",
            "config": {"disabled_filesystems": "HTTPFileSystem"},
        },
        {
            "tests/dbs/identity_general.sqlite3": [{fido_mds3: []}],
        },
    )

    full_run.prepare().unwrap_or_raise(SystemExit)
    full_run.run().unwrap_or_raise(SystemExit)

    return full_run.ddb_name


def test_integration(mocker: MockerFixture) -> None:
    run_full(mocker)

    # test_utils.assert_query_result(
    #     "tests/dbs/identity_general.sqlite3",
    #     "SELECT * FROM fido_metadata_service_main",
    #     "tests/fixtures/identity/fido_metadata_service/query.1.json",
    # )

    # test_utils.assert_query_result(
    #     "tests/dbs/aws_general.sqlite3",
    #     "SELECT * FROM analyst_reports ORDER BY id",
    #     "tests/fixtures/aws/full/analyst_reports/query.1.json",
    # )
