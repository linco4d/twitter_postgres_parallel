#!/bin/bash
set -euo pipefail

files=$(find data -maxdepth 1 -type f -name '*.zip' | sort)

echo 'load normalized'
{ time for file in $files; do
    echo "$file"
    python3 load_tweets.py --db="postgresql://postgres:pass@localhost:1076/postgres" --inputs="$file"
done; } 2>&1 | tee normalized_time.txt

echo 'load denormalized'
{ time for file in $files; do
    echo "$file"
    unzip -p "$file" \
      | sed 's/\\u0000//g' \
      | psql "postgresql://postgres:pass@localhost:1981/postgres" \
          -c "COPY tweets_jsonb (data) FROM STDIN csv quote e'\x01' delimiter e'\x02';"
done; } 2>&1 | tee denormalized_time.txt

echo 'load normalized batch'
{ time for file in $files; do
    echo "$file"
    python3 -u load_tweets_batch.py --db="postgresql://postgres:pass@localhost:1982/postgres" --inputs="$file"
done; } 2>&1 | tee normalized_batch_time.txt
