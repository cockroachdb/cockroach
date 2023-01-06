#!/bin/bash

test=$1
count=$2
duration_mins=$3

SCRIPT_DIR=$(dirname "$0")
. "$SCRIPT_DIR"/bisect-util.sh

CURRENT_HASH="$(current_hash)"

#first lets check our saved results
opsPerSec=$(get_hash_result "$CURRENT_HASH")

case $opsPerSec in
  USER_GOOD)
    exit 0
    ;;
  USER_BAD)
    exit 1
    ;;
  USER_SKIP)
    exit 128
    ;;
  *)
    ;;
esac

#./pkg/cmd/roachtest/roachstress.sh -b -c 2 "^$test\$" -- --parallelism 1 -c "$cluster" --debug-reuse

#if opsPerSec was not previously saved, we calculate it by building and running
if [[ -z "$opsPerSec" ]]; then
  build_sha "$CURRENT_HASH" "$duration_mins" &> "$BISECT_DIR/$CURRENT_HASH-build.log"
  stress_sha "$CURRENT_HASH" "$test" "$count" &> "$BISECT_DIR/$CURRENT_HASH-run.log"

  git reset --hard

  #looks under the artifacts directory for the current runs stats to calculate the average/second
  opsPerSec=$(calc_avg_ops "artifacts/$CURRENT_HASH*/$test/run_*/*.perf/stats.json")
  set_hash_result "$CURRENT_HASH" "$opsPerSec"
fi

goodThreshold=$(get_conf_val ".goodThreshold")
badThreshold=$(get_conf_val ".badThreshold")

if [ -n "$goodThreshold" ] && [[ opsPerSec -ge goodThreshold ]]; then
  log "[$CURRENT_HASH] Average ops/s: [$opsPerSec]. Auto marked as good." "$LOG_NAME"
  exit 0;
elif [ -n "$badThreshold" ] && [[ opsPerSec -le badThreshold ]]; then
  log "[$CURRENT_HASH] Average ops/s: [$opsPerSec]. Auto marked as bad." "$LOG_NAME"
  exit 1;
else
  prompt_user "$CURRENT_HASH" "$opsPerSec"
fi
