from types import ModuleType
from typing import Any, Optional, Sequence

import duckdb
from result import Ok, Result
from sqlite_utils import Database
from sqlite_utils.db import Table, ForeignKeysType

# import jwt
import fido2.mds3
import fido2.webauthn
import fido2.attestation.base
import requests

from querydataio import shared


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
            print("Identity")
            print("========")

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
                sqlitedb = Database(database_filename, strict=True)

                for module in modules:
                    for main_module, partitions in module.items():
                        print()
                        print("FIDO MDS3")
                        print("=========")

                        # Download blob from URL
                        blob = requests.get("https://mds3.fidoalliance.org/").content

                        cert = requests.get(
                            "https://secure.globalsign.com/cacert/root-r3.crt"
                        ).content

                        metadata = fido2.mds3.parse_blob(blob, cert)

                        metadata_blob_payload_entries_table: Table = (
                            sqlitedb.create_table(
                                "fido_mds3_metadata_blob_payload_entries",
                                columns={
                                    "id": int,
                                    "aaid": str,
                                    "aaguid": str,
                                    "rogue_list_hash": str,
                                    "rogue_list_url": str,
                                    "time_of_last_status_change": str,
                                    "metadata_statement_description": str,
                                    "metadata_statement_authenticator_version": int,
                                    "metadata_statement_protocol_family": str,
                                    "metadata_statement_schema": str,
                                    # TODO: upv
                                },
                                pk="id",
                            )
                        )

                        attestation_types_table: Table = sqlitedb.create_table(
                            "fido_mds3_attestation_types",
                            columns={
                                "id": str,
                            },
                            pk="id",
                        ).insert_all(
                            [
                                {"id": "NONE"},
                                {"id": "BASIC"},
                                {"id": "ATT_CA"},
                                {"id": "ANON_CA"},
                            ]
                        )

                        AUTHENTICATOR_STATUSES_CATEGORIES_CERTIFICATION_RELATED = (
                            "Certification Related"
                        )
                        AUTHENTICATOR_STATUSES_CATEGORIES_SECURITY_NOTIFICATION = (
                            "Security Notification"
                        )
                        AUTHENTICATOR_STATUSES_CATEGORIES_INFO = "Info"

                        authenticator_statuses_table: Table = sqlitedb.create_table(
                            "fido_mds3_authenticator_statuses",
                            columns={
                                "id": str,
                                "category": str,
                            },
                            pk="id",
                        ).insert_all(
                            [
                                # Certification Related Statuses
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.NOT_FIDO_CERTIFIED,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_CERTIFICATION_RELATED,
                                },
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.SELF_ASSERTION_SUBMITTED,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_CERTIFICATION_RELATED,
                                },
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.FIDO_CERTIFIED,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_CERTIFICATION_RELATED,
                                },
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.FIDO_CERTIFIED_L1,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_CERTIFICATION_RELATED,
                                },
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.FIDO_CERTIFIED_L1plus,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_CERTIFICATION_RELATED,
                                },
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.FIDO_CERTIFIED_L2,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_CERTIFICATION_RELATED,
                                },
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.FIDO_CERTIFIED_L2plus,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_CERTIFICATION_RELATED,
                                },
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.FIDO_CERTIFIED_L3,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_CERTIFICATION_RELATED,
                                },
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.FIDO_CERTIFIED_L3plus,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_CERTIFICATION_RELATED,
                                },
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.REVOKED,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_CERTIFICATION_RELATED,
                                },
                                # Security Notification Statuses
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.USER_VERIFICATION_BYPASS,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_SECURITY_NOTIFICATION,
                                },
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.ATTESTATION_KEY_COMPROMISE,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_SECURITY_NOTIFICATION,
                                },
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.USER_KEY_REMOTE_COMPROMISE,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_SECURITY_NOTIFICATION,
                                },
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.USER_KEY_PHYSICAL_COMPROMISE,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_SECURITY_NOTIFICATION,
                                },
                                # Info Statuses
                                {
                                    "id": fido2.mds3.AuthenticatorStatus.UPDATE_AVAILABLE,
                                    "category": AUTHENTICATOR_STATUSES_CATEGORIES_INFO,
                                },
                            ]
                        )

                        status_reports_table: Table = sqlitedb.create_table(
                            "fido_mds3_status_reports",
                            columns={
                                "id": int,
                                "fido_mds3_authenticator_status_id": str,
                                "effective_date": str,
                                "authenticator_version": int,
                                "certificate": str,
                                "url": str,
                                "certification_descriptor": str,
                                "certificate_number": str,
                                "certification_policy_version": str,
                                "certification_requirements_version": str,
                            },
                            pk="id",
                            foreign_keys=[
                                (
                                    "fido_mds3_authenticator_status_id",
                                    "fido_mds3_authenticator_statuses",
                                    "id",
                                )
                            ],
                        )

                        attestation_certificate_key_identifiers_table: Table = (
                            sqlitedb.create_table(
                                "fido_mds3_attestation_certificate_key_identifiers",
                                columns={"id": str},
                                pk="id",
                            )
                        )

                        biometric_status_reports_table: Table = sqlitedb.create_table(
                            "fido_mds3_biometric_status_reports",
                            columns={
                                "cert_level": str,
                                "certificate_number": str,
                                "certification_descriptor": str,
                                "certification_policy_version": str,
                                "certification_requirements_value": str,
                                "effective_date": str,
                                "modality": str,
                            },
                        )

                        metadata_statement_authentication_algorithms_table: Table = sqlitedb.create_table(
                            "fido_mds3_metadata_statement_authentication_algorithms",
                            columns={"id": str},
                            pk="id",
                        )

                        for e in metadata.entries:
                            if e.attestation_certificate_key_identifiers:
                                for acki in e.attestation_certificate_key_identifiers:
                                    attestation_certificate_key_identifiers_table.insert(
                                        {"id": acki}
                                    )
                            if e.biometric_status_reports:
                                for bsr in e.biometric_status_reports:
                                    biometric_status_reports_table.insert_all(bsr)

                            if e.status_reports:
                                for sr in e.status_reports:
                                    status_reports_table.insert(
                                        {
                                            "fido_mds3_authenticator_status_id": sr.status,
                                            "effective_date": sr.effective_date,
                                            "authenticator_version": sr.authenticator_version,
                                            "certificate": sr.certificate,
                                            "url": sr.url,
                                            "certification_descriptor": sr.certification_descriptor,
                                            "certificate_number": sr.certificate_number,
                                            "certification_policy_version": sr.certification_policy_version,
                                            "certification_requirements_version": sr.certification_requirements_version,
                                        }
                                    )

                            metadata_blob_payload_entry = {
                                "aaid": None if e.aaid is None else str(e.aaid),
                                "aaguid": fido2.webauthn.Aaguid.NONE
                                if e.aaguid is None
                                else str(e.aaguid),
                                "rogue_list_hash": e.rogue_list_hash,
                                "rogue_list_url": e.rogue_list_url,
                                "time_of_last_status_change": e.time_of_last_status_change,
                            }

                            if e.metadata_statement is not None:
                                if e.metadata_statement.authentication_algorithms:
                                    for (
                                        aa
                                    ) in e.metadata_statement.authentication_algorithms:
                                        metadata_statement_authentication_algorithms_table.upsert(
                                            {"id": aa}
                                        )
                                        # .m2m(
                                        #     main_table,
                                        # )

                                metadata_blob_payload_entry.update(
                                    {
                                        "metadata_statement_description": e.metadata_statement.description,
                                        "metadata_statement_authenticator_version": e.metadata_statement.authenticator_version,
                                        "metadata_statement_protocol_family": e.metadata_statement.protocol_family,
                                        "metadata_statement_schema": e.metadata_statement.schema,
                                    },
                                )

                            metadata_blob_payload_entries_table.insert(
                                metadata_blob_payload_entry
                            )
                        pass

                #         aws_shared.to_sqlite(
                #             sqlitedb,
                #             [
                #                 (self.ddb_con.table(main_table).df(), main_table),
                #                 (
                #                     self.ddb_con.table(main_tags_table).df(),
                #                     main_tags_table,
                #                 ),
                #             ],
                #             6,
                #         )

                #         main_module.initial_sqlite_transform(sqlitedb, main_table, 6)

                # aws_shared.merge_duckdb_tags(
                #     self.ddb_con, aws_shared.TAGS_TABLE_NAME, tags_tables, 4
                # )

                # aws_shared.to_sqlite(
                #     sqlitedb,
                #     [
                #         (
                #             self.ddb_con.table(aws_shared.TAGS_TABLE_NAME).df(),
                #             aws_shared.TAGS_TABLE_NAME,
                #         ),
                #     ],
                #     4,
                # )

                # aws_shared.tag_table_optimisations(sqlitedb, 4)

                # for module in modules:
                #     for main_module, partitions in module.items():
                #         print()
                #         print(f"    {main_module.DIRECTORY_ID}")
                #         print(f"    {'=' * len(main_module.DIRECTORY_ID)}")

                #         aws_shared.common_table_optimisations(sqlitedb, main_module, 6)

                # shared.final_database_optimisations(sqlitedb, 2)

            return Ok(None)
        finally:
            if hasattr(self, "ddb_con"):
                self.ddb_con.close()
