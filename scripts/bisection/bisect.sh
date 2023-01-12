#!/bin/bash

test=$1
count=$2
duration_mins=$3

SCRIPT_DIR=$(dirname "$0")
. "$SCRIPT_DIR"/bisect-util.sh

CURRENT_HASH="$(git rev-parse --short HEAD)"

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
  "")
    build_hash "$CURRENT_HASH" "$duration_mins" &> "$BISECT_DIR/$CURRENT_HASH-build.log"
    test_hash "$CURRENT_HASH" "$test" "$count" &> "$BISECT_DIR/$CURRENT_HASH-run.log"

    opsPerSec=$(calc_avg_ops "$CURRENT_HASH" "$test")
    set_hash_result "$CURRENT_HASH" "$opsPerSec"
    ;;
  *)
    ;;
esac

goodThreshold=$(get_conf_val ".goodThreshold")
badThreshold=$(get_conf_val ".badThreshold")

if [ -n "$goodThreshold" ] && [[ opsPerSec -ge goodThreshold ]]; then
  log "[$CURRENT_HASH] Average ops/s: [$opsPerSec]. Auto marked as good." "$LOG_NAME"
  exit 0;
elif [ -n "$badThreshold" ] && [[ opsPerSec -le badThreshold ]]; then
  log "[$CURRENT_HASH] Average ops/s: [$opsPerSec]. Auto marked as bad." "$LOG_NAME"
  exit 1;
else
  # we don't have thresholds to compare, or the value doesn't meet them
  prompt_user "$CURRENT_HASH" "$opsPerSec"
fi
