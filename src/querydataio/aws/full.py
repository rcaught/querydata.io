from types import ModuleType
from sqlite_utils import Database
from querydataio.aws import (
    analyst_reports,
    media_coverage,
    products,
    whats_new,
    blog_posts,
    security_bulletins,
)
from querydataio.aws import shared as aws_shared
from querydataio import shared

print()
print("Full download")
print("=============")

print()
print("AWS")
print("===")

shared.delete_dbs([aws_shared.DUCKDB_DB], 2)

ddb_con = shared.init_duckdb(aws_shared.DUCKDB_DB)

databases_modules: dict[str, list[dict[ModuleType, list[str | int]]]] = {
    f"dbs/aws_{whats_new.MAIN_TABLE_NAME}.sqlite3": [
        {whats_new: whats_new.all_years()}
    ],
    f"dbs/aws_{blog_posts.MAIN_TABLE_NAME}.sqlite3": [
        {blog_posts: blog_posts.aws_categories()}
    ],
    "dbs/aws_general.sqlite3": [
        {analyst_reports: []},
        {media_coverage: []},
        {products: []},
        {security_bulletins: []},
    ],
}

for database_filename, modules in databases_modules.items():
    print()
    print(f"  Database: {database_filename}")
    print(f"  =========={len(database_filename) * '='}")

    shared.delete_dbs([database_filename], 4)
    sqlitedb = Database(database_filename)

    tags_tables = []
    for module in modules:
        for main_module, partitions in module.items():
            print()
            print(f"    {main_module.DIRECTORY_ID}")
            print(f"    {'=' * len(main_module.DIRECTORY_ID)}")

            main_table: str = main_module.MAIN_TABLE_NAME
            main_tags_table: str = main_module.MAIN_TAGS_TABLE_NAME
            tags_table: str = f"__tags_{main_module.MAIN_TABLE_NAME}"
            tags_tables.append(tags_table)

            aws_shared.download(
                ddb_con,
                aws_shared.generate_urls(ddb_con, main_module, partitions, 6),
                main_table,
                6,
            )

            main_module.process(
                ddb_con,
                main_module,
                main_table,
                main_tags_table,
                tags_table,
                6,
            )

            aws_shared.to_sqlite(
                sqlitedb,
                [
                    [ddb_con.table(main_table).df(), main_table],
                    [ddb_con.table(main_tags_table).df(), main_tags_table],
                ],
                6,
            )

            main_module.initial_sqlite_transform(sqlitedb, main_table, 6)

    aws_shared.merge_duckdb_tags(ddb_con, aws_shared.TAGS_TABLE_NAME, tags_tables, 4)

    aws_shared.to_sqlite(
        sqlitedb,
        [
            [
                ddb_con.table(aws_shared.TAGS_TABLE_NAME).df(),
                aws_shared.TAGS_TABLE_NAME,
            ],
        ],
        4,
    )

    aws_shared.tag_table_optimisations(sqlitedb, 4)

    for module in modules:
        for main_module, partitions in module.items():
            print()
            print(f"    {main_module.DIRECTORY_ID}")
            print(f"    {'=' * len(main_module.DIRECTORY_ID)}")

            aws_shared.common_table_optimisations(sqlitedb, main_module, 6)

    shared.final_database_optimisations(sqlitedb, 2)
