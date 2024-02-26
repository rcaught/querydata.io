#!/bin/bash
set -eu

mkdir -p plugins/datasette-updated/
echo '{
  "plugins": {
    "datasette-updated": {
      "updated": "'"$(date -Iseconds)"'"
    }
  }
}' > plugins/datasette-updated/metadata.json

# poetry run python -u src/querydataio/full.py

# --load-extension=extensions/stats.so \
poetry run datasette \
  dbs/aws_general.sqlite3 \
  dbs/aws_blog_posts.sqlite3 \
  dbs/aws_whats_new.sqlite3 \
  dbs/identity_general.sqlite3 \
  --metadata metadata.yml \
  --plugins-dir plugins \


docker run -it -p 8081:8001 ds --crossdb

  # --install=datasette-vega \
  # --install=datasette-atom \
  # --install=datasette-updated
