from duckdb import DuckDBPyConnection
import pytest
from querydataio import shared
from querydataio.aws import shared as aws_shared
from querydataio.aws import whats_new

BASE_URLS_PREFIX = "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime"
EXPECTED_URLS_PREFIX = f"{BASE_URLS_PREFIX}&size=1&tags.id=whats-new%23year%23"


def test_generate_urls(mocker):
    expected_urls = [
        f"{EXPECTED_URLS_PREFIX}2003",
        f"{EXPECTED_URLS_PREFIX}2004",
        f"{EXPECTED_URLS_PREFIX}2005",
        f"{EXPECTED_URLS_PREFIX}2006",
        f"{EXPECTED_URLS_PREFIX}2007",
        f"{EXPECTED_URLS_PREFIX}2008",
        f"{EXPECTED_URLS_PREFIX}2009",
    ]
    download_side_effect(
        mocker, expected_urls, "tests/aws/shared/test_shared.test_generate_urls.1.json"
    )

    assert aws_shared.generate_urls(
        shared.init_duckdb(":memory:"), whats_new, range(2003, 2010)
    ) == [
        f"{BASE_URLS_PREFIX}&size=2000&page=0&tags.id=whats-new%23year%232004&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=2000&page=0&tags.id=whats-new%23year%232005&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=2000&page=0&tags.id=whats-new%23year%232006&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=2000&page=1&tags.id=whats-new%23year%232006&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=0&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=1&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=2&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=3&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=4&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=5&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=6&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=7&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=8&tags.id=whats-new%23year%232007&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=0&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=1&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=2&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=3&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=4&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=5&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=6&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=7&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=8&tags.id=whats-new%23year%232008&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=2000&page=0&tags.id=whats-new%23year%232008&sort_order=asc",
        f"{BASE_URLS_PREFIX}&size=1111&page=0&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=1&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=2&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=3&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=4&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=5&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=6&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=7&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=8&tags.id=whats-new%23year%232009&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=0&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{BASE_URLS_PREFIX}&size=1111&page=1&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{BASE_URLS_PREFIX}&size=1111&page=2&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{BASE_URLS_PREFIX}&size=1111&page=3&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{BASE_URLS_PREFIX}&size=1111&page=4&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{BASE_URLS_PREFIX}&size=1111&page=5&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{BASE_URLS_PREFIX}&size=1111&page=6&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{BASE_URLS_PREFIX}&size=1111&page=7&tags.id=whats-new%23year%232009&sort_order=asc",
        f"{BASE_URLS_PREFIX}&size=1111&page=8&tags.id=whats-new%23year%232009&sort_order=asc",
    ]


def test_generate_urls_out_of_bounds(mocker):
    expected_urls = [
        f"{EXPECTED_URLS_PREFIX}2010",
    ]
    download_side_effect(
        mocker, expected_urls, "tests/aws/shared/test_shared.test_generate_urls.2.json"
    )

    with pytest.raises(Exception, match="Out of range downloads"):
        aws_shared.generate_urls(
            shared.init_duckdb(":memory:"), whats_new, range(2010, 2011)
        )


def download_side_effect(mocker, expected_urls, mock_json_filepath):
    download = mocker.patch("querydataio.aws.shared.download")

    def download_side_effect(
        ddb_con: DuckDBPyConnection,
        urls: list[str],
        main_table: str,
        print_indent: int = 0,
    ) -> str:
        if urls == expected_urls:
            ddb_con.sql(
                f"""
                CREATE OR REPLACE TEMP TABLE __{main_table}_downloads AS SELECT * FROM read_json_auto('{mock_json_filepath}', format='array');
                """
            )
            return f"__{main_table}_downloads"
        else:
            raise Exception("Update side effect")

    download.side_effect = download_side_effect
