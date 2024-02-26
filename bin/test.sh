#!/bin/bash
set -eu

docker run --rm -v "$PWD:/mnt" koalaman/shellcheck:stable bin/*.sh
poetry run pytest --cov=querydataio --cov-report html