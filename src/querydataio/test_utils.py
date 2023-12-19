import duckdb
from pytest_mock import MockerFixture


def duckdb_connect():
    ddb_con = duckdb.connect(":memory:")
    ddb_con.sql(f"SET disabled_filesystems='HTTPFileSystem';")
    return ddb_con


def download_side_effect(
    mocker: MockerFixture,
    mock_json_filepaths: dict[str, str],
    validate_expected_urls: bool = False,
    expected_urls: list[str] | None = None,
):
    download = mocker.patch("querydataio.aws.shared.download")

    def download_side_effect(
        ddb_con: duckdb.DuckDBPyConnection,
        urls: list[str],
        table_prefix: str,
        print_indent: int = 0,
    ) -> str:
        if validate_expected_urls and urls != expected_urls:
            raise Exception("Update side effect")

        ddb_con.sql(
            f"""
                CREATE OR REPLACE TEMP TABLE __{table_prefix}_downloads AS SELECT * FROM read_json_auto('{mock_json_filepaths[table_prefix]}', format='auto');
                """
        )
        return f"__{table_prefix}_downloads"

    download.side_effect = download_side_effect
