from result import Ok, do
from querydataio.aws import (
    analyst_reports,
    blog_posts,
    executive_insights,
    media_coverage,
    products,
    security_bulletins,
    whats_new,
)
from querydataio.aws.full import FullRun


full_run = FullRun(
    {
        "database": "dbs/aws.duckdb.db",
    },
    {
        f"dbs/aws_{whats_new.MAIN_TABLE_NAME}.sqlite3": [
            {whats_new: whats_new.all_years()}
        ],
        f"dbs/aws_{blog_posts.MAIN_TABLE_NAME}.sqlite3": [
            {blog_posts: blog_posts.aws_categories().unwrap_or_raise(SystemExit)}
        ],
        "dbs/aws_general.sqlite3": [
            {analyst_reports: []},
            {executive_insights: []},
            {media_coverage: []},
            {products: []},
            {security_bulletins: []},
        ],
    },
)

do(Ok(None) for b in full_run.run() for a in full_run.prepare()).unwrap_or_raise(
    SystemExit
)
