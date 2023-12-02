from duckdb import DuckDBPyConnection
from querydataio import shared
from querydataio.aws import shared as aws_shared
from querydataio.aws import whats_new


def test_generate_urls(mocker):
    download = mocker.patch("querydataio.aws.shared.download")

    def download_side_effect(ddb_con: DuckDBPyConnection, urls, main_table):
        if urls == [
            "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1&tags.id=whats-new%23year%232004",
            "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1&tags.id=whats-new%23year%232005",
            "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1&tags.id=whats-new%23year%232006",
            "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1&tags.id=whats-new%23year%232007",
            "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1&tags.id=whats-new%23year%232008",
            "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1&tags.id=whats-new%23year%232009",
        ]:
            ddb_con.sql(
                f"""
                CREATE OR REPLACE TEMP TABLE __{main_table}_downloads AS SELECT * FROM read_json_auto('tests/aws/shared/test_shared.test_generate_urls.1.json', format='array');
                """
            )
            return f"__{main_table}_downloads"
        else:
            return Exception("Update side effect")

    download.side_effect = download_side_effect

    assert aws_shared.generate_urls(
        shared.init_duckdb(":memory:"), whats_new, range(2004, 2010)
    ) == [
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=2000&page=0&tags.id=whats-new%23year%232004&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=2000&page=0&tags.id=whats-new%23year%232005&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=2000&page=0&tags.id=whats-new%23year%232006&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=2000&page=1&tags.id=whats-new%23year%232006&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=0&tags.id=whats-new%23year%232007&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=1&tags.id=whats-new%23year%232007&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=2&tags.id=whats-new%23year%232007&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=3&tags.id=whats-new%23year%232007&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=4&tags.id=whats-new%23year%232007&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=5&tags.id=whats-new%23year%232007&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=6&tags.id=whats-new%23year%232007&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=7&tags.id=whats-new%23year%232007&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=8&tags.id=whats-new%23year%232007&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=0&tags.id=whats-new%23year%232008&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=1&tags.id=whats-new%23year%232008&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=2&tags.id=whats-new%23year%232008&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=3&tags.id=whats-new%23year%232008&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=4&tags.id=whats-new%23year%232008&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=5&tags.id=whats-new%23year%232008&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=6&tags.id=whats-new%23year%232008&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=7&tags.id=whats-new%23year%232008&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=8&tags.id=whats-new%23year%232008&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=2000&page=0&tags.id=whats-new%23year%232008&sort_order=asc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=0&tags.id=whats-new%23year%232009&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=1&tags.id=whats-new%23year%232009&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=2&tags.id=whats-new%23year%232009&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=3&tags.id=whats-new%23year%232009&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=4&tags.id=whats-new%23year%232009&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=5&tags.id=whats-new%23year%232009&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=6&tags.id=whats-new%23year%232009&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=7&tags.id=whats-new%23year%232009&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=8&tags.id=whats-new%23year%232009&sort_order=desc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=0&tags.id=whats-new%23year%232009&sort_order=asc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=1&tags.id=whats-new%23year%232009&sort_order=asc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=2&tags.id=whats-new%23year%232009&sort_order=asc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=3&tags.id=whats-new%23year%232009&sort_order=asc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=4&tags.id=whats-new%23year%232009&sort_order=asc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=5&tags.id=whats-new%23year%232009&sort_order=asc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=6&tags.id=whats-new%23year%232009&sort_order=asc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=7&tags.id=whats-new%23year%232009&sort_order=asc",
        "https://aws.amazon.com/api/dirs/items/search?item.locale=en_US&item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&size=1111&page=8&tags.id=whats-new%23year%232009&sort_order=asc",
    ]
