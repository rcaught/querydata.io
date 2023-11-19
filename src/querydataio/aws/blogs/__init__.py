import pathlib
import sys

import duckdb
import pandas as pd

import json

from sqlite_utils import Database
from sqlite_utils.db import Table

DIRECTORY_ID = "blog-posts"
URL_PREFIX = (
    "https://aws.amazon.com/api/dirs/items/search?"
    f"item.locale=en_US&item.directoryId={DIRECTORY_ID}"
    "&sort_by=item.additionalFields.createdDate"
    "&sort_order=desc"
)
TAG_ID_PREFIX = "blog-posts%23category%23"

SQLITE_BLOGS_TABLE_NAME = "blogs"
SQLITE_BLOG_TAGS_TABLE_NAME = "blog_tags"


def parse_aws_categories() -> dict[str, str]:
    # https://aws.amazon.com/blogs/
    # //*[@id="aws-element-44ba89e5-9024-4e8f-a95c-621b7efdc78e"]
    # ^ would likely change
    # html.no-js.aws-lng-en_US.aws-with-target.aws-ember body.awsm div#aws-page-content.lb-none-pad.lb-page-content.lb-page-with-sticky-subnav main#aws-page-content-main div.lb-grid.lb-row.lb-row-max-large.lb-snap div.lb-col.lb-tiny-24.lb-mid-5 div.lb-data-attr-wrapper.data-attr-wrapper div.lb-filter-container.lb-filter-light .lb-filter-checkbox
    with open(
        pathlib.Path(__file__).with_name("categories.json"), encoding="utf8"
    ) as file:
        return json.loads(file.read())


def aws_categories() -> list[str]:
    result: list[str] = []

    categories = parse_aws_categories()

    aws_filters: list[dict[str, str]] = categories["filters"]
    for aws_filter in aws_filters:
        if aws_filter["value"] == "category":
            for child in aws_filter["children"]:
                result.append(child["value"])
        else:
            sys.exit(1)  # not AWS filter category

    return result


def process(
    con: duckdb.DuckDBPyConnection,
    all_data: list[duckdb.DuckDBPyRelation],
    print_indent=0,
) -> list[pd.DataFrame]:
    """Clean and transform into a Dataframe"""

    processed_blogs: list[pd.DataFrame] = []
    processed_tags: list[pd.DataFrame] = []
    processed_blog_tags: list[pd.DataFrame] = []

    print()
    print(f"{print_indent * ' '}Processing data")
    print(f"{print_indent * ' '}===============")
    print(f"{print_indent * ' '}{len(all_data)} pages...")
    print()

    for i, data in enumerate(all_data):  # pylint: disable=unused-variable
        print(f"{print_indent * ' '}- page {i + 1}")

        unnested_items = con.sql(  # pylint: disable=unused-variable
            """
              SELECT
                UNNEST(items, recursive := true)
              FROM
                data;
            """
        )

        if unnested_items.count("*").fetchall()[0][0] == 0:
            continue

        blogs = con.sql(
            """
                CREATE OR REPLACE TEMP TABLE t1 AS
                SELECT
                  * EXCLUDE (tags)
                FROM
                  unnested_items;

                SELECT * FROM t1;
            """
        )

        tags_with_id = con.sql(
            """
                SELECT
                  id as blog_id,
                  unnest(tags, recursive := true)
                FROM
                  unnested_items;
            """
        )

        blog_tags = con.sql(
            """
                SELECT
                  blog_id,
                  id as tag_id
                FROM
                  tags_with_id;
            """
        )

        tags = con.sql(
            """
                SELECT DISTINCT * EXCLUDE (blog_id, description)
                FROM
                  tags_with_id;
            """
        )

        processed_blogs.append(blogs.df())
        processed_tags.append(tags.df())
        processed_blog_tags.append(blog_tags.df())

    result_blogs = pd.concat(processed_blogs, ignore_index=True)
    result_tags = pd.concat(processed_tags, ignore_index=True)
    result_blog_tags = pd.concat(processed_blog_tags, ignore_index=True)

    result_tags_distinct = con.sql(
        """
            SELECT
              * EXCLUDE (lastUpdatedBy, dateUpdated, name),
              MAX_BY(lastUpdatedBy, dateUpdated) as lastUpdatedBy,
              MAX(dateUpdated) as dateUpdated,
              MAX_BY(name, dateUpdated) as name
            FROM
              result_tags
            GROUP BY ALL;
        """
    ).df()

    result_blogs.astype({"author": "str", "createdBy": "str", "lastUpdatedBy": "str"})

    return [result_blogs, result_tags_distinct, result_blog_tags]


def initial_sqlite_transform(blogs_table: str):
    blogs_table.transform(
        column_order=(
            "id",
            "createdDate",
            "title",
            "link",
            "postExcerpt",
            "featuredImageUrl",
        )
    )


def final_sqlite_transform(
    blogs_table: Table,
    tags_table: Table,
    blog_tags_table: Table,
    print_indent=0,
):
    print()
    print(f"{print_indent * ' '}Optimising tables")
    print(f"{print_indent * ' '}=================")

    blogs_table.transform(
        pk="id",
    )
    blogs_table.create_index(["createdDate"])
    blogs_table.create_index(["title"])

    print(f"{print_indent * ' '}- {blogs_table.name}... done")

    tags_table.transform(pk="id")
    tags_table.create_index(["tagNamespaceId"])
    tags_table.create_index(["name"])
    tags_table.create_index(["tagNamespaceId", "name"])

    print(f"{print_indent * ' '}- {tags_table.name}... done")

    blog_tags_table.transform(pk=["blog_id", "tag_id"])
    blog_tags_table.add_foreign_key(f"blog_id", blogs_table.name, "id", ignore=True)
    blog_tags_table.add_foreign_key(f"tag_id", tags_table.name, "id", ignore=True)

    print(f"{print_indent * ' '}- {blog_tags_table.name}... done")
