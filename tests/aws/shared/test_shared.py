from typing import Generator
from duckdb import DuckDBPyConnection
from pytest_mock import MockerFixture
from querydataio import shared
from querydataio.aws import shared as aws_shared
from querydataio.aws import whats_new


def test_generate_urls(mocker: Generator[MockerFixture, None, None]):
    download = mocker.patch("querydataio.aws.shared.download")

    def download_side_effect(
        ddb_con: DuckDBPyConnection,
        urls: list[str],
        main_table: str,
        print_indent: int = 0,
    ) -> str:
        prefix = "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1&tags.id=whats-new%23year%23"
        if urls == [
            f"{prefix}2004",
            f"{prefix}2005",
            f"{prefix}2006",
            f"{prefix}2007",
            f"{prefix}2008",
            f"{prefix}2009",
        ]:
            ddb_con.sql(
                f"""
                CREATE OR REPLACE TEMP TABLE __{main_table}_downloads AS SELECT * FROM read_json_auto('tests/aws/shared/test_shared.test_generate_urls.1.json', format='array');
                """
            )
            return f"__{main_table}_downloads"
        else:
            Exception("Update side effect")

    download.side_effect = download_side_effect

    prefix = "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime"

    assert aws_shared.generate_urls(
        shared.init_duckdb(":memory:"), whats_new, range(2004, 2010)
    ) == [
        f"{prefix}&size=2000&page=0&tags.id=whats-new%23year%232004&sort_order=desc",
        f"{prefix}&size=2000&page=0&tags.id=whats-new%23year%232005&sort_order=desc",
        f"{prefix}&size=2000&page=0&tags.id=whats-new%23year%232006&sort_order=desc",
        f"{prefix}&size=2000&page=1&tags.id=whats-new%23year%232006&sort_order=desc",
        f"{prefix}&size=1111&page=0&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{prefix}&size=1111&page=1&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{prefix}&size=1111&page=2&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{prefix}&size=1111&page=3&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{prefix}&size=1111&page=4&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{prefix}&size=1111&page=5&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{prefix}&size=1111&page=6&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{prefix}&size=1111&page=7&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{prefix}&size=1111&page=8&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{prefix}&size=1111&page=0&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{prefix}&size=1111&page=1&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{prefix}&size=1111&page=2&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{prefix}&size=1111&page=3&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{prefix}&size=1111&page=4&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{prefix}&size=1111&page=5&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{prefix}&size=1111&page=6&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{prefix}&size=1111&page=7&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{prefix}&size=1111&page=8&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{prefix}&size=2000&page=0&tags.id=whats-new%23year%232008&sort_order=asc",
        f"{prefix}&size=1111&page=0&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{prefix}&size=1111&page=1&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{prefix}&size=1111&page=2&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{prefix}&size=1111&page=3&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{prefix}&size=1111&page=4&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{prefix}&size=1111&page=5&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{prefix}&size=1111&page=6&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{prefix}&size=1111&page=7&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{prefix}&size=1111&page=8&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{prefix}&size=1111&page=0&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{prefix}&size=1111&page=1&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{prefix}&size=1111&page=2&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{prefix}&size=1111&page=3&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{prefix}&size=1111&page=4&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{prefix}&size=1111&page=5&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{prefix}&size=1111&page=6&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{prefix}&size=1111&page=7&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{prefix}&size=1111&page=8&tags.id=whats-new%23year%232009&sort_order=asc",
    ]
