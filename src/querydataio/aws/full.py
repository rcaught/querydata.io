from types import ModuleType
from typing import Any, Sequence

import duckdb
from result import Ok, Result
from sqlite_utils import Database

from querydataio import shared
from querydataio.aws import shared as aws_shared


class FullRun:
    def __init__(
        self,
        ddb_connect: dict[str, Any],
        databases_modules: dict[str, list[dict[ModuleType, Sequence[str | int]]]],
    ) -> None:
        self.ddb_connect = ddb_connect
        self.databases_modules = databases_modules

    def prepare(self) -> Result[None, str]:
        try:
            print()
            print("Full download")
            print("=============")

            print()
            print("AWS")
            print("===")

            self.ddb_name = self.ddb_connect["database"]

            shared.delete_dbs([self.ddb_name], 2)

            dfs = self.ddb_connect.get("config", {}).pop("disabled_filesystems", None)
            self.ddb_con = duckdb.connect(**self.ddb_connect)

            if dfs:
                self.ddb_con.sql(f"SET disabled_filesystems='{dfs}';")

            return Ok(None)
        finally:
            if hasattr(self, "ddb_con"):
                self.ddb_con.close()

    def run(self) -> Result[None, str]:
        try:
            self.ddb_con = duckdb.connect(**self.ddb_connect)

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
                        print(f"    {main_module.DIRECTORY_ID}")
                        print(f"    {'=' * len(main_module.DIRECTORY_ID)}")

                        main_table: str = main_module.MAIN_TABLE_NAME
                        main_tags_table: str = main_module.MAIN_TAGS_TABLE_NAME
                        tags_table: str = f"__tags_{main_module.MAIN_TABLE_NAME}"
                        tags_tables.append(tags_table)

                        aws_shared.download(
                            self.ddb_con,
                            aws_shared.generate_urls(
                                self.ddb_con, main_module, partitions, 6
                            ),
                            main_table,
                            6,
                        )

                        main_module.process(
                            self.ddb_con,
                            main_module,
                            main_table,
                            main_tags_table,
                            tags_table,
                            6,
                        )

                        aws_shared.to_sqlite(
                            sqlitedb,
                            [
                                (self.ddb_con.table(main_table).df(), main_table),
                                (
                                    self.ddb_con.table(main_tags_table).df(),
                                    main_tags_table,
                                ),
                            ],
                            6,
                        )

                        main_module.initial_sqlite_transform(sqlitedb, main_table, 6)

                aws_shared.merge_duckdb_tags(
                    self.ddb_con, aws_shared.TAGS_TABLE_NAME, tags_tables, 4
                )

                aws_shared.to_sqlite(
                    sqlitedb,
                    [
                        (
                            self.ddb_con.table(aws_shared.TAGS_TABLE_NAME).df(),
                            aws_shared.TAGS_TABLE_NAME,
                        ),
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

            return Ok(None)
        finally:
            if hasattr(self, "ddb_con"):
                self.ddb_con.close()
