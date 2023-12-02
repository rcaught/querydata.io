import sys
from sqlite_utils import Database
from querydataio.aws import whats_new
from querydataio.aws import shared as aws_shared
from querydataio import shared

print()
print("Partial download")
print("================")

print()
print("AWS")
print("===")

sqlitedb = Database(aws_shared.SQLITE_DB)
ddb_con = shared.init_duckdb(aws_shared.DUCKDB_DB)

main_modules = {
    whats_new: [f"{whats_new.URL_PREFIX}&size={aws_shared.PARTIAL_COLLECTION_SIZE}"]
}

tag_tables = []

for main_module, partitions in main_modules.items():
    print()
    print(f"  {main_module.DIRECTORY_ID}")
    print(f"  {'=' * len(main_module.DIRECTORY_ID)}")

    main_new_table: str = main_module.MAIN_TABLE_NAME + "_new"
    main_tags_new_table: str = main_module.MAIN_TAGS_TABLE_NAME + "_new"
    tags_main_new_table: str = "tags_" + main_module.MAIN_TABLE_NAME + "_new"

    aws_shared.download(
        ddb_con,
        partitions,
        main_new_table,
        4,
    )

    main_module.process(
        ddb_con,
        main_module,
        main_new_table,
        main_tags_new_table,
        tags_main_new_table,
        4,
    )

    main_table: str = main_module.MAIN_TABLE_NAME
    main_tags_table: str = main_module.MAIN_TAGS_TABLE_NAME

    before = ddb_con.sql(f"SELECT COUNT(*) FROM {main_table}").fetchone()[0]

    aws_shared.merge_duckdb_tables(
        ddb_con,
        [[main_table, main_new_table], [main_tags_table, main_tags_new_table]],
        4,
    )

    after = ddb_con.sql(f"SELECT COUNT(*) FROM {main_table}").fetchone()[0]

    if before == after:
        print()
        print("    No new data... hard exiting to stop deploy")
        print("    ==========================================")
        sys.exit(1)
    else:
        print()
        print("    New data")
        print("    ========")
        print(f"    - {after - before} new updates... continuing")

    aws_shared.to_sqlite(
        sqlitedb,
        [
            [ddb_con.table(main_table).df(), main_table],
            [ddb_con.table(main_tags_table).df(), main_tags_table],
        ],
        4,
    )

    main_module.initial_sqlite_transform(sqlitedb, main_table, 4)

    tag_tables.append(tags_main_new_table)

aws_shared.merge_duckdb_tags(
    ddb_con, aws_shared.TAGS_TABLE_NAME, [aws_shared.TAGS_TABLE_NAME] + tag_tables, 2
)

aws_shared.to_sqlite(
    sqlitedb,
    [
        [
            ddb_con.table(aws_shared.TAGS_TABLE_NAME).df(),
            aws_shared.TAGS_TABLE_NAME,
        ],
    ],
    2,
)

aws_shared.tag_table_optimisations(sqlitedb, 2)

for main_module, _ in main_modules.items():
    aws_shared.common_table_optimisations(sqlitedb, main_module, 2)

shared.final_database_optimisations(sqlitedb)
