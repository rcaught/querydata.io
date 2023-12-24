import json
import pathlib
import time
from types import ModuleType
from typing import cast

from duckdb import DuckDBPyConnection
from result import Err, Ok, Result
from sqlite_utils import Database
from sqlite_utils.db import Table

from querydataio.aws import shared as aws_shared

DIRECTORY_ID = "blog-posts"
URL_PREFIX = (
    "https://aws.amazon.com/api/dirs/items/search?"
    f"item.locale=en_US&item.directoryId={DIRECTORY_ID}"
    "&sort_by=item.additionalFields.createdDate"
)
TAG_ID_PREFIX = "blog-posts%23category%23"
MAIN_TABLE_NAME = "blog_posts"
MAIN_TAGS_TABLE_NAME = "blog_post_tags"
RELATION_ID = "blog_post_hash"


def parse_aws_categories() -> dict[str, list[dict[str, list[dict[str, str]]]]]:
    # https://aws.amazon.com/blogs/
    # //*[@id="aws-element-44ba89e5-9024-4e8f-a95c-621b7efdc78e"]
    # ^ would likely change
    # html.no-js.aws-lng-en_US.aws-with-target.aws-ember body.awsm div#aws-page-content.lb-none-pad.lb-page-content.lb-page-with-sticky-subnav main#aws-page-content-main div.lb-grid.lb-row.lb-row-max-large.lb-snap div.lb-col.lb-tiny-24.lb-mid-5 div.lb-data-attr-wrapper.data-attr-wrapper div.lb-filter-container.lb-filter-light .lb-filter-checkbox
    with open(
        pathlib.Path(__file__).with_name("categories.json"), encoding="utf8"
    ) as file:
        return json.loads(file.read())


def aws_categories() -> Result[list[str], str]:
    result: list[str] = []
    categories = parse_aws_categories()

    aws_filters = categories["filters"]
    for aws_filter in aws_filters:
        if aws_filter["value"] == "category":
            for child in aws_filter["children"]:
                result.append(child["value"])
        else:
            return Err("Not AWS filter category")
    return Ok(result)


def process(
    ddb_con: DuckDBPyConnection,
    main_module: ModuleType,
    main_table: str,
    main_tags_table: str,
    tags_main_table: str,
    print_indent: int = 0,
):
    return aws_shared.process(
        ddb_con,
        main_module,
        main_table,
        main_tags_table,
        tags_main_table,
        print_indent,
    )


def mid_alters(ddb_con: DuckDBPyConnection, main_table: str):
    return None


def initial_sqlite_transform(
    sqlitedb: Database, main_table_name: str, print_indent: int = 0
):
    print()
    print(f"{print_indent * ' '}Optimising tables")
    print(f"{print_indent * ' '}=================")

    start = time.time()
    print(f"{print_indent * ' '}- {main_table_name}... ", end="")

    main_table = cast(Table, sqlitedb.table(main_table_name))

    main_table.transform(
        column_order=[
            "id",
            "createdDate",
            "title",
            "link",
            "postExcerpt",
            "featuredImageUrl",
        ],
        pk="hash",
    )

    main_table.create_index(["id"])
    main_table.create_index(["createdDate"])
    main_table.create_index(["title"])

    print(f"done ({time.time() - start})")


def unnest(ddb_con: DuckDBPyConnection, main_table: str):
    ddb_con.execute(
        f"""--sql
        CREATE OR REPLACE TEMP TABLE __{main_table}_unnested_downloads AS
        WITH unnested AS (
          SELECT
            unnest(items, recursive := true)
          FROM
            __{main_table}_downloads
        )
        SELECT
          md5(id)[:10] as hash,
          id,
          title,
          link,
          postExcerpt,
          featuredImageUrl,
          author,
          createdDate,
          modifiedDate,
          displayDate,
          tags
        FROM unnested;
        """
    )
