import os
import sys
from types import ModuleType
from typing import Any, Sequence

import duckdb
from sqlite_utils import Database

from querydataio import shared
from querydataio.aws import shared as aws_shared


class PartialRun:
    def __init__(
        self,
        ddb_connect: dict[str, Any],
        databases_modules: dict[str, list[dict[ModuleType, Sequence[str | int]]]],
    ) -> None:
        self.ddb_name = ddb_connect["database"]

        dfs = ddb_connect.get("config", {}).pop("disabled_filesystems", None)
        self.ddb_con = duckdb.connect(**ddb_connect)

        self.databases_modules = databases_modules

        if dfs:
            self.ddb_con.sql(f"SET disabled_filesystems='{dfs}';")

    def prepare(self):
        print()
        print("Partial download")
        print("================")

        print()
        print("AWS")
        print("===")

    def run(self):
        for database_filename, modules in self.databases_modules.items():
            print()
            print(f"  Database: {database_filename}")
            print(f"  =========={len(database_filename) * '='}")

            shared.delete_dbs([database_filename], 4)
            sqlitedb = Database(database_filename)

            tags_tables: list[str] = []
            for module in modules:
                for main_module, partitions in module.items():
                    print()
                    print(f"  {main_module.DIRECTORY_ID}")
                    print(f"  {'=' * len(main_module.DIRECTORY_ID)}")

                    main_new_table: str = main_module.MAIN_TABLE_NAME + "_new"
                    main_tags_new_table: str = main_module.MAIN_TAGS_TABLE_NAME + "_new"
                    tags_main_new_table: str = (
                        "tags_" + main_module.MAIN_TABLE_NAME + "_new"
                    )

                    aws_shared.download(
                        self.ddb_con,
                        partitions,
                        main_new_table,
                        6,
                    )

                    main_module.process(
                        self.ddb_con,
                        main_module,
                        main_new_table,
                        main_tags_new_table,
                        tags_main_new_table,
                        6,
                    )

                    main_table: str = main_module.MAIN_TABLE_NAME
                    main_tags_table: str = main_module.MAIN_TAGS_TABLE_NAME

                    result = self.ddb_con.table(main_table).count("*").fetchone()
                    if result is not None:
                        before = result[0]
                    else:
                        raise Exception("No result")

                    aws_shared.merge_duckdb_tables(
                        self.ddb_con,
                        [
                            (main_table, main_new_table),
                            (main_tags_table, main_tags_new_table),
                        ],
                        4,
                    )

                    result = self.ddb_con.table(main_table).count("*").fetchone()
                    if result is not None:
                        after = result[0]
                    else:
                        raise Exception("No result")

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
                            (self.ddb_con.table(main_table).df(), main_table),
                            (self.ddb_con.table(main_tags_table).df(), main_tags_table),
                        ],
                        4,
                    )

                    main_module.initial_sqlite_transform(sqlitedb, main_table, 4)

                    tags_tables.append(tags_main_new_table)

                aws_shared.merge_duckdb_tags(
                    self.ddb_con,
                    aws_shared.TAGS_TABLE_NAME,
                    [aws_shared.TAGS_TABLE_NAME] + tags_tables,
                    2,
                )

                aws_shared.to_sqlite(
                    sqlitedb,
                    [
                        (
                            self.ddb_con.table(aws_shared.TAGS_TABLE_NAME).df(),
                            aws_shared.TAGS_TABLE_NAME,
                        ),
                    ],
                    2,
                )

                aws_shared.tag_table_optimisations(sqlitedb, 2)

            for module in modules:
                for main_module, partitions in module.items():
                    print()
                    print(f"    {main_module.DIRECTORY_ID}")
                    print(f"    {'=' * len(main_module.DIRECTORY_ID)}")

                    aws_shared.common_table_optimisations(sqlitedb, main_module, 6)

            shared.final_database_optimisations(sqlitedb, 2)
