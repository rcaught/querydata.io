#!/bin/sh

mkdir -p plugins/datasette-updated/ && \
echo '{
  "plugins": {
    "datasette-updated": {
      "updated": "'"$(date -Iseconds)"'"
    }
  }
}' > plugins/datasette-updated/metadata.json && \
poetry run datasette package \
  dbs/aws_general.sqlite3 \
  dbs/aws_blog_posts.sqlite3 \
  dbs/aws_whats_new.sqlite3 \
  --tag ds \
  --load-extension=extensions/stats.so \
  --metadata metadata.yml \
  --plugins-dir plugins \
  --install=datasette-vega \
  --install=datasette-atom \
  --install=datasette-updated && \
docker run -it -p 8081:8001 ds --crossdb
