import pytest
from pytest_mock import MockerFixture
from querydataio.aws import shared as aws_shared
from querydataio.aws import whats_new
from tests import test_utils

BASE_URLS_PREFIX = "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime"
EXPECTED_URLS_PREFIX = f"{BASE_URLS_PREFIX}&size=1&tags.id=whats-new%23year%23"


def test_generate_urls(mocker: MockerFixture) -> None:
    expected_urls = [
        f"{EXPECTED_URLS_PREFIX}2003",
        f"{EXPECTED_URLS_PREFIX}2004",
        f"{EXPECTED_URLS_PREFIX}2005",
        f"{EXPECTED_URLS_PREFIX}2006",
        f"{EXPECTED_URLS_PREFIX}2007",
        f"{EXPECTED_URLS_PREFIX}2008",
        f"{EXPECTED_URLS_PREFIX}2009",
    ]
    test_utils.download_side_effect(
        mocker,
        {
            "whats_new_total_hits": "tests/fixtures/aws/shared/test_shared.test_generate_urls.1.json"
        },
        True,
        expected_urls,
    )

    assert aws_shared.generate_urls(
        test_utils.duckdb_connect(),
        whats_new,
        range(2003, 2010),
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


def test_generate_urls_out_of_bounds(mocker: MockerFixture) -> None:
    expected_urls = [
        f"{EXPECTED_URLS_PREFIX}2010",
    ]
    test_utils.download_side_effect(
        mocker,
        {
            "whats_new_total_hits": "tests/fixtures/aws/shared/test_shared.test_generate_urls.2.json"
        },
        True,
        expected_urls,
    )

    with pytest.raises(Exception, match="Out of range downloads"):
        aws_shared.generate_urls(
            test_utils.duckdb_connect(),
            whats_new,
            range(2010, 2011),
        )


def test_generate_urls_no_partitions(mocker: MockerFixture) -> None:
    test_utils.download_side_effect(
        mocker,
        {
            "whats_new_total_hits": "tests/fixtures/aws/shared/test_shared.test_generate_urls.3.json"
        },
        True,
        [f"{BASE_URLS_PREFIX}&size=1"],
    )

    assert aws_shared.generate_urls(
        test_utils.duckdb_connect(),
        whats_new,
        [],
    ) == [
        f"{BASE_URLS_PREFIX}&size=1111&page=0&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=1&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=2&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=3&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=4&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=5&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=6&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=7&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=1111&page=8&sort_order=desc",
        f"{BASE_URLS_PREFIX}&size=2000&page=0&sort_order=asc",
    ]
