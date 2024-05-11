from types import ModuleType
from typing import Sequence

import pytest
from querydataio import shared
from querydataio.aws import analyst_reports, whats_new, whitepapers
from querydataio.aws.full import FullRun

from tests import test_utils


@pytest.fixture(autouse=True)
def run_around_tests():
    test_utils.clean_test_dbs()

    yield


def run_full(
    database_modules: dict[str, list[dict[ModuleType, Sequence[str | int]]]],
    fixtures_create: bool = False,
) -> str:
    if fixtures_create:
        config = {"disabled_filesystems": "HTTPFileSystem"}
    else:
        config = {}

    full_run = FullRun(
        {
            "database": "tests/dbs/aws.duckdb.db",
            "config": config,
        },
        database_modules,
        fixtures_use=True,
        fixtures_create=fixtures_create,
    )

    full_run.prepare().unwrap_or_raise(SystemExit)
    full_run.run().unwrap_or_raise(SystemExit)

    return full_run.ddb_name


def test_integration() -> None:
    # Change this to recreate fixtures
    fixtures_recreate = False

    database_modules = {
        f"tests/dbs/aws_{whats_new.MAIN_TABLE_NAME}.sqlite3": [
            {whats_new: range(shared.current_year(), shared.current_year() + 1)}
        ],
        "tests/dbs/aws_general.sqlite3": [
            {analyst_reports: []},
            {whitepapers: []},
        ],
    }

    run_full(database_modules, fixtures_recreate)

    for database_filename, modules in database_modules.items():
        for module in modules:
            for module_name, _ in module.items():
                test_utils.assert_query_result(
                    database_filename,
                    f"SELECT * FROM {module_name.MAIN_TABLE_NAME} ORDER BY id",
                    fixtures_recreate,
                )
