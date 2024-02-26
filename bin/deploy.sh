#!/bin/bash
set -eu

npm install -g vercel@32.2.5 # breaks after this version

mkdir -p plugins/datasette-updated/
echo '{
  "plugins": {
    "datasette-updated": {
      "updated": "'"$(date -Iseconds)"'"
    }
  }
}' > plugins/datasette-updated/metadata.json

command="poetry run datasette publish vercel \
  dbs/aws_whats_new.sqlite3 \
  dbs/aws_blog_posts.sqlite3 \
  dbs/aws_general.sqlite3 \
  dbs/identity_general.sqlite3 \
  --project querydataio \
  --metadata metadata.yml \
  --crossdb \
  --install=datasette-vega \
  --install=datasette-atom \
  --install=datasette-updated \
  --plugins-dir=plugins \
  --template-dir=templates \
  --vercel-json=vercel.json \
  --setting default_page_size 100 \
  --setting allow_csv_stream off \
  --token $VERCEL_TOKEN "

if [ "$GITHUB_REF_NAME" != "$GITHUB_REF" ]; then
    command+=" --no-prod"
fi

eval "$command"

printf "\n\nCF\n\n" &&
CLOUDFLARE_API_KEY=$CLOUDFLARE_PURGE_API_TOKEN poetry run \
  cli4 --delete purge_everything=true /zones/:querydata.io/purge_cache