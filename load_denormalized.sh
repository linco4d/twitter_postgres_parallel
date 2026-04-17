#!/bin/sh
set -e

file="$1"

unzip -p "$file" \
  | sed 's/\\u0000//g' \
  | psql "postgresql://postgres:pass@localhost:1981/postgres" \
      -c "COPY tweets_jsonb (data) FROM STDIN csv quote e'\x01' delimiter e'\x02';"
