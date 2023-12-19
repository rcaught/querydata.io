import duckdb
from pytest_mock import MockerFixture


def duckdb_connect():
    ddb_con = duckdb.connect(":memory:")
    ddb_con.sql(f"SET disabled_filesystems='HTTPFileSystem';")
    return ddb_con


def download_side_effect(
    mocker: MockerFixture, expected_urls: list[str], mock_json_filepath: str
):
    download = mocker.patch("querydataio.aws.shared.download")

    def download_side_effect(
        ddb_con: duckdb.DuckDBPyConnection,
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
