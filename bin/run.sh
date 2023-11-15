#!/bin/sh

poetry run datasette package dbs/aws.db --metadata metadata.yml --tag data-company-tools
# https://github.com/simonw/datasette/pull/2155 --extra-options "--reload"
docker run -it -p 8081:8001 data-company-tools