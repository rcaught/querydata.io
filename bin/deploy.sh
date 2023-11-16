#!/bin/sh

npm install -g vercel@32.2.5 && # breaks after this version
poetry run datasette publish vercel dbs/aws.db \
  --project querydataio \
  --metadata metadata.yml \
  --install=datasette-vega \
  --install=datasette-atom \
  --install=datasette-block-robots \
  --vercel-json=vercel.json \
  --setting default_page_size 100 \
  --setting allow_csv_stream off \
  --token $VERCEL_TOKEN &&
echo &&
CLOUDFLARE_API_KEY=$CLOUDFLARE_PURGE_API_TOKEN poetry run \
  cli4 --delete purge_everything=true /zones/:bf147aba15972081173a0047f501e229/purge_cache