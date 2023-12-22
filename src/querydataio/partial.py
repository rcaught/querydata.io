from querydataio.aws import (
    analyst_reports,
    blog_posts,
    executive_insights,
    media_coverage,
    products,
    security_bulletins,
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
        # f"dbs/aws_{blog_posts.MAIN_TABLE_NAME}.sqlite3": [
        #     {blog_posts: blog_posts.aws_categories()}
        # ],
        # "dbs/aws_general.sqlite3": [
        #     {analyst_reports: []},
        #     {executive_insights: []},
        #     {media_coverage: []},
        #     {products: []},
        #     {security_bulletins: []},
        # ],
    },
)

partial_run.prepare()
partial_run.run()
