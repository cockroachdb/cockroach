#!/usr/bin/env bash
# Run parallel DELETE queries against the kv database in a loop for ~1 minute.
# Each DELETE fans out 1000 point deletes across random keys, which DistSender
# should parallelize (no TargetBytes/MaxSpanRequestKeys).
#
# Usage: bash scripts/run-parallel-deletes.sh <cluster> <gateway-node>
# Example: bash scripts/run-parallel-deletes.sh tobias-spike 3

set -euo pipefail

CLUSTER=${1:?usage: $0 <cluster> <gateway-node>}
NODE=${2:?usage: $0 <cluster> <gateway-node>}
DURATION=60
SLEEP=1

echo "Running parallel DELETEs on ${CLUSTER}:${NODE} for ${DURATION}s..."
echo "Start: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"

end=$((SECONDS + DURATION))
i=0
while [ $SECONDS -lt $end ]; do
  i=$((i + 1))
  echo -n "  attempt $i @ $(date -u '+%H:%M:%S')... "
  roachprod sql "${CLUSTER}:${NODE}" -- -e \
    "DELETE FROM kv.kv WHERE k IN (SELECT unique_rowid() FROM generate_series(1, 1000));" \
    2>/dev/null
  echo "done"
  sleep "$SLEEP"
done

echo "End: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "Completed $i iterations."
