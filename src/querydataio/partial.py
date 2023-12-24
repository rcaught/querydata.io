from result import Ok, do
from querydataio.aws import (
    whats_new,
)
from querydataio.aws import shared as aws_shared
from querydataio.aws.partial import PartialRun


partial_run = PartialRun(
    {
        "database": "dbs/aws.duckdb.db",
    },
    {
        f"dbs/aws_{whats_new.MAIN_TABLE_NAME}.sqlite3": [
            {
                whats_new: [
                    f"{whats_new.URL_PREFIX}&size={aws_shared.PARTIAL_COLLECTION_SIZE}"
                ]
            }
        ],
    },
)

do(Ok(None) for b in partial_run.run() for a in partial_run.prepare()).unwrap_or_raise(
    SystemExit
)
