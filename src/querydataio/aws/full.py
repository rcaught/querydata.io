from sqlite_utils import Database
import querydataio.aws.whats_new as whats_new
from querydataio.aws import shared as aws_shared
from querydataio import shared

print()
print("Full download")
print("=============")

print()
print("AWS")
print("===")

shared.delete_dbs([aws_shared.SQLITE_DB, aws_shared.DUCKDB_DB], 2)

sqlitedb = Database(aws_shared.SQLITE_DB)
ddb_con = shared.init_duckdb(aws_shared.DUCKDB_DB)

main_modules = {whats_new: range(whats_new.FIRST_YEAR, shared.current_year() + 1)}
tag_tables = []

for main_module, partitions in main_modules.items():
    print()
    print(f"  {main_module.DIRECTORY_ID}")
    print(f"  {'=' * len(main_module.DIRECTORY_ID)}")

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

    aws_shared.to_sqlite(
        sqlitedb,
        [
            [ddb_con.table(main_table).df(), main_table],
            [ddb_con.table(main_tags_table).df(), main_tags_table],
        ],
        4,
    )

    main_module.initial_sqlite_transform(sqlitedb, main_table, 4)

    tag_tables.append(tags_table)

aws_shared.merge_duckdb_tags(ddb_con, aws_shared.TAGS_TABLE_NAME, tag_tables, 2)

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
