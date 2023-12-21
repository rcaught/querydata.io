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
            {blog_posts: blog_posts.aws_categories()}
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

full_run.prepare()
full_run.run()
