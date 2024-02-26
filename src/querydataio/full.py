from result import Ok, do
from querydataio import aws
from querydataio import identity

aws_full_run = aws.full.FullRun(
    {
        "database": "dbs/aws.duckdb.db",
    },
    {
        f"dbs/aws_{aws.whats_new.MAIN_TABLE_NAME}.sqlite3": [
            {aws.analyst_reports: []},
            # {aws.whats_new: aws.whats_new.all_years()}
        ],
        f"dbs/aws_{aws.blog_posts.MAIN_TABLE_NAME}.sqlite3": [
            {aws.analyst_reports: []},
            # {
            #     aws.blog_posts: aws.blog_posts.aws_categories().unwrap_or_raise(
            #         SystemExit
            #     )
            # }
        ],
        "dbs/aws_general.sqlite3": [
            {aws.analyst_reports: []},
            {aws.executive_insights: []},
            {aws.media_coverage: []},
            {aws.products: []},
            {aws.security_bulletins: []},
        ],
    },
)

identity_full_run = identity.full.FullRun(
    {
        "database": "dbs/identity.duckdb.db",
    },
    {
        "dbs/identity_general.sqlite3": [{identity.fido_mds3: []}],
    },
)

do(
    Ok(None)
    for b in aws_full_run.run() and identity_full_run.run()
    for a in aws_full_run.prepare() and aws_full_run.prepare()
).unwrap_or_raise(SystemExit)
