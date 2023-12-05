from sqlite_utils import Database
from querydataio.aws import blog_posts
from querydataio.aws import whats_new
from querydataio.aws import shared as aws_shared
from querydataio import shared

print()
print("Full download")
print("=============")

print()
print("AWS")
print("===")

shared.delete_dbs([aws_shared.SQLITE_DB, aws_shared.DUCKDB_DB], 2)

ddb_con = shared.init_duckdb(aws_shared.DUCKDB_DB)

main_modules = {
    whats_new: whats_new.all_years(),
    blog_posts: blog_posts.aws_categories(),
}
tag_tables = []

for main_module, partitions in main_modules.items():
    print()
    print(f"  {main_module.DIRECTORY_ID}")
    print(f"  {'=' * len(main_module.DIRECTORY_ID)}")

    sqlitedb = Database(f"dbs/aws_{main_module.MAIN_TABLE_NAME}.sqlite3")

    main_table: str = main_module.MAIN_TABLE_NAME
    main_tags_table: str = main_module.MAIN_TAGS_TABLE_NAME
    tags_table: str = f"__tags_{main_module.MAIN_TABLE_NAME}"

    aws_shared.download(
        ddb_con,
        aws_shared.generate_urls(ddb_con, main_module, partitions, 4),
        main_table,
        4,
    )

    main_module.process(
        ddb_con,
        main_module,
        main_table,
        main_tags_table,
        tags_table,
        4,
    )

    # TODO: fix this
    aws_shared.merge_duckdb_tags(ddb_con, aws_shared.TAGS_TABLE_NAME, [tags_table], 4)

    aws_shared.to_sqlite(
        sqlitedb,
        [
            [ddb_con.table(main_table).df(), main_table],
            [ddb_con.table(main_tags_table).df(), main_tags_table],
        ],
        4,
    )

    main_module.initial_sqlite_transform(sqlitedb, main_table, 4)

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

    aws_shared.common_table_optimisations(sqlitedb, main_module, 4)

    shared.final_database_optimisations(sqlitedb, 4)
