#!/bin/sh

npm install -g vercel@32.2.5 && # breaks after this version
poetry run datasette publish vercel \
  dbs/aws_whats_new.sqlite3 \
  dbs/aws_blog_posts.sqlite3 \
  dbs/aws_general.sqlite3 \
  --project querydataio \
  --metadata metadata.yml \
  --crossdb \
  --install=datasette-vega \
  --install=datasette-atom \
  --plugins-dir=plugins \
  --load-extension=extensions/stats.so \
  --vercel-json=vercel.json \
  --setting default_page_size 100 \
  --setting allow_csv_stream off \
  --token $VERCEL_TOKEN &&
echo &&
CLOUDFLARE_API_KEY=$CLOUDFLARE_PURGE_API_TOKEN poetry run \
  cli4 --delete purge_everything=true /zones/:bf147aba15972081173a0047f501e229/purge_cache