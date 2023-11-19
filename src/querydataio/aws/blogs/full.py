"""Full"""

from duckdb import DuckDBPyConnection
from sqlite_utils import Database
from sqlite_utils.db import Table

from querydataio.aws import shared as aws_shared
from querydataio.aws import blogs


def run(sqlitedb: Database, duckdb: DuckDBPyConnection, print_indent=0):
    print()
    print(f"{print_indent * ' '}Blogs")
    print(f"{print_indent * ' '}=====")

    blogs_table: Table = sqlitedb.table(blogs.SQLITE_BLOGS_TABLE_NAME)
    blog_tags_table: Table = sqlitedb.table(blogs.SQLITE_BLOG_TAGS_TABLE_NAME)
    tags_table = aws_shared.tags_table(sqlitedb)

    downloaded = aws_shared.download(
        duckdb,
        blogs.URL_PREFIX,
        blogs.TAG_ID_PREFIX,
        blogs.aws_categories(),
        print_indent=print_indent + 2,
    )

    result_blogs, result_tags, result_blog_tags = blogs.process(
        duckdb, downloaded, print_indent=print_indent + 2
    )

    aws_shared.to_sqlite(
        [
            (result_blogs, blogs_table),
            (result_tags, tags_table),
            (result_blog_tags, blog_tags_table),
        ],
        print_indent=print_indent + 2,
    )

    blogs.initial_sqlite_transform(blogs_table)
    blogs.final_sqlite_transform(
        blogs_table,
        tags_table,
        blog_tags_table,
        print_indent=print_indent + 2,
    )
