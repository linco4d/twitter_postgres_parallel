#!/bin/sh

set -e

files=$(find data -maxdepth 1 -type f -name '*.zip' | sort)

NORMALIZED_DB='postgresql://postgres:pass@localhost:1076/postgres'
DENORMALIZED_DB='postgresql://postgres:pass@localhost:1981/postgres'
NORMALIZED_BATCH_DB='postgresql://postgres:pass@localhost:1982/postgres'

echo 'load normalized'
for file in $files; do
    echo "$file"
    python3 load_tweets.py --db "$NORMALIZED_DB" --inputs "$file"
done

echo 'load denormalized'
for file in $files; do
    echo "$file"
    unzip -p "$file" \
      | python3 -c "import sys; [sys.stdout.write(line.replace(r'\\u0000', '')) for line in sys.stdin]" \
      | psql "$DENORMALIZED_DB" -c "\copy tweets_jsonb(data) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', QUOTE E'\b')"
done

echo 'load normalized batch'
for file in $files; do
    echo "$file"
    python3 -u load_tweets_batch.py --db "$NORMALIZED_BATCH_DB" --inputs "$file"
done
