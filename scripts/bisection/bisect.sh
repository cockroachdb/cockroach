#!/bin/bash
set -x

test=$1
count=$2
duration_mins=$3

. ./scripts/bisection/bisect-util.sh

BISECT_DIR="$(clean_test_name "$test")"
LOG_NAME="$BISECT_DIR/bisection.log"
CONF_NAME="$BISECT_DIR/config.json"
CURRENT_HASH="$(current_hash)"

#echo "dummy ops"
#read opsPerSec
#./pkg/cmd/roachtest/roachstress.sh -b -c 2 "^$test\$" -- --parallelism 1 -c "$cluster" --debug-reuse

#trap 'echo error on line $LINENO bisecting $CURRENT_HASH. aborting; git reset --hard; exit 128' ERR

build_sha "$CURRENT_HASH" "$duration_mins" &> "$BISECT_DIR/$CURRENT_HASH-build.log"
stress_sha "$CURRENT_HASH" "$test" "$count" &> "$BISECT_DIR/$CURRENT_HASH-run.log"

git reset --hard

#looks under the artifacts directory for the current runs stats to calculate the average/second
opsPerSec=$(calc_avg_ops "artifacts/$CURRENT_HASH*/$test/run_*/*.perf/stats.json")

goodThreshold=$(get_conf_val ".goodThreshold" "$CONF_NAME")
badThreshold=$(get_conf_val ".badThreshold" "$CONF_NAME")

if [ -n "$goodThreshold" ] && [[ opsPerSec -ge goodThreshold ]]; then
  log "[$CURRENT_HASH] Average ops/s: [$opsPerSec]. Auto marked as good." "$LOG_NAME"
  set_conf_val "goodHash" "$CURRENT_HASH" "$CONF_NAME"
  exit 0;
elif [ -n "$badThreshold" ] && [[ opsPerSec -le badThreshold ]]; then
  log "[$CURRENT_HASH] Average ops/s: [$opsPerSec]. Auto marked as bad." "$LOG_NAME"
  set_conf_val "badHash" "$CURRENT_HASH" "$CONF_NAME"
  exit 1;
else
  prompt_user "$CURRENT_HASH" "$opsPerSec" "$CONF_NAME" "$LOG_NAME"
fi
