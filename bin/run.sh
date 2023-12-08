#!/bin/sh

docker run -p 8001:8001 -v `pwd`:/mnt \
    datasetteproject/datasette \
    datasette -p 8001 -h 0.0.0.0 \
    -i /mnt/dbs/aws_general.sqlite3 \
    -i /mnt/dbs/aws_blog_posts.sqlite3 \
    -i /mnt/dbs/aws_whats_new.sqlite3 \
    --metadata /mnt/metadata.yml --crossdb --reload \
    --plugins-dir /mnt/plugins --load-extension=/mnt/extensions/stats.so