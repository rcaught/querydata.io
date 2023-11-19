"""Partial"""

import sys
from sqlite_utils import Database
from sqlite_utils.db import Table

from querydataio import shared
from querydataio.aws import shared as aws_shared
from querydataio.aws import blogs


def run(print_indent=0) -> bool:
    print()
    print(f"{print_indent * ' '}Blogs")
    print(f"{print_indent * ' '}=====")

    sqlitedb = Database(aws_shared.SQLITE_DB)
    duckdb = shared.init_duckdb()

    print(
        f"{print_indent * ' '}- downloading last {aws_shared.PARTIAL_COLLECTION_SIZE} and merging"
    )

    downloaded = aws_shared.download(
        duckdb,
        blogs.URL_PREFIX,
        blogs.TAG_ID_PREFIX,
        None,
        aws_shared.PARTIAL_COLLECTION_SIZE,
        1,
    )

    result_blogs, result_tags, result_blog_tags = blogs.process(
        duckdb, downloaded, print_indent=print_indent + 2
    )

    # SQLite processing of new tables
    # NOTE: more would be done in duckDB, but errors on column reordering were happening

    blogs_new_table: Table = sqlitedb.table(blogs.SQLITE_MAIN_TABLE_NAME + "_new")
    tags_new_table: Table = sqlitedb.table(aws_shared.SQLITE_TAGS_TABLE_NAME + "_new")
    blog_tags_new_table: Table = sqlitedb.table(
        blogs.SQLITE_MAIN_TAGS_TABLE_NAME + "_new"
    )

    aws_shared.to_sqlite(
        [
            (result_blogs, blogs_new_table),
            (result_tags, tags_new_table),
            (result_blog_tags, blog_tags_new_table),
        ],
        print_indent=print_indent + 2,
    )

    blogs.initial_sqlite_transform(blogs_new_table)

    # Merge into existing

    blogs_table: Table = sqlitedb.table(blogs.SQLITE_MAIN_TABLE_NAME)
    tags_table = aws_shared.tags_table(sqlitedb)
    blogs_tag_table: Table = sqlitedb.table(blogs.SQLITE_MAIN_TAGS_TABLE_NAME)

    blogs_table_count = blogs_table.count

    aws_shared.merge_sqlite_tables(
        sqlitedb,
        [
            (blogs_table, blogs_new_table),
            (tags_table, tags_new_table),
            (blogs_tag_table, blog_tags_new_table),
        ],
        print_indent=print_indent + 2,
    )

    if blogs_table.count == blogs_table_count:
        return False

    blogs.final_sqlite_transform(
        blogs_table, tags_table, blogs_tag_table, print_indent=print_indent + 2
    )

    return True
