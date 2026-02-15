#!/usr/bin/env bash
# Generate and run 1000 point DELETEs in one explicit transaction.
# The transaction's write pipeline should dispatch these in parallel.
#
# Usage: bash scripts/gen-parallel-deletes.sh <cluster> <gateway-node>

set -euo pipefail

CLUSTER=${1:?usage: $0 <cluster> <gateway-node>}
NODE=${2:?usage: $0 <cluster> <gateway-node>}
N=1000

# Generate a SQL file with 1000 DELETEs in a single transaction.
sql_file=$(mktemp /tmp/parallel-deletes-XXXX.sql)
echo "BEGIN;" > "$sql_file"
for i in $(seq 1 $N); do
  # Use random int64 keys scattered across the key space.
  key=$((RANDOM * RANDOM * RANDOM))
  echo "DELETE FROM kv.kv WHERE k = $key;" >> "$sql_file"
done
echo "COMMIT;" >> "$sql_file"

echo "Generated $N DELETEs in $sql_file"
echo "Start: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"

roachprod sql "${CLUSTER}:${NODE}" -- < "$sql_file" 2>&1

echo "End: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
rm -f "$sql_file"
