import glob
import json
import os
import pytest

import sqlite_utils
from pytest_mock import MockerFixture

from querydataio.aws import (
    analyst_reports,
    whats_new,
)
from querydataio.aws.full import FullRun
from tests import test_utils



def run_full(mocker: MockerFixture) -> None:
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
            # f"tests/dbs/aws_{blog_posts.MAIN_TABLE_NAME}.sqlite3": [
            #     {blog_posts: blog_posts.aws_categories()}
            # ],
            "tests/dbs/aws_general.sqlite3": [
                {analyst_reports: []},
                #     {executive_insights: []},
                #     {media_coverage: []},
                #     {products: []},
                #     {security_bulletins: []},
            ],
        },
    )

    full_run.prepare()
    full_run.run()


def test_integration(mocker: MockerFixture) -> None:
    run_full(mocker)

    # print(have) to generate json file

    with open("tests/fixtures/aws/full/whats_new/query.1.json", "r") as file:
        want = json.dumps(json.load(file))
        have = json.dumps(
            sqlite_utils.Database(
                "tests/dbs/aws_whats_new.sqlite3"
            ).execute_returning_dicts("SELECT * FROM whats_new ORDER BY id")
        )

        assert want == have

    with open("tests/fixtures/aws/full/analyst_reports/query.1.json", "r") as file:
        want = json.dumps(json.load(file))
        have = json.dumps(
            sqlite_utils.Database(
                "tests/dbs/aws_general.sqlite3"
            ).execute_returning_dicts("SELECT * FROM analyst_reports ORDER BY id")
        )

        assert want == have
