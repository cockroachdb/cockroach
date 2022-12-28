#!/bin/bash

set -ex
#e.g. kv95/enc=false/nodes=3
test=$1
#2022-09-01
from=$2
#2022-09-03
to=$3

test="kv95/enc=false/nodes=3/cpu=32"
branch="origin/release-22.1"
from="2022-07-06 09:00:00Z"
to="2022-07-08 22:00:00Z"
count=4
duration_mins=15

. ./scripts/bisection/bisect-util.sh

git reset --hard
BISECT_DIR="$(clean_test_name "$test")"
LOG_NAME="$BISECT_DIR/bisection.log"

mkdir -p "$BISECT_DIR"
CONF_NAME="$BISECT_DIR/config.json"

test -d ./scripts/bisection || { echo "bisection must be run from cockroach root"; exit 1; }

trapped() {
  echo "interrupt!"
  prompt_user "$(current_hash)" -1 "$CONF_NAME" "$LOG_NAME"

  if [[ $? -gt 125 ]]; then
    exit 1
  fi

  # relaunch this script and restart bisection with updated config
  exec "$0" "$@"
}

trap 'trapped' INT

if [ -f "$CONF_NAME" ]; then
  echo "Bisection using config: $CONF_NAME"
  good=$(get_conf_val ".goodHash" "$CONF_NAME")
  bad=$(get_conf_val ".badHash" "$CONF_NAME")
else
  hashes="$(git log "$branch" --merges --pretty=format:'%h' --date=short --since "$from" --until "$to")"
  good=$(echo "$hashes" | tail -1)
  bad=$(echo "$hashes" | head -1)
  set_conf_val "goodHash" "$good" "$CONF_NAME"
  set_conf_val "badHash" "$bad" "$CONF_NAME"
fi

log "Bisecting regression in [$test] using commit range [$good (known good),$bad (known bad)]" "$LOG_NAME"

goodThreshold="$(get_conf_val ".goodThreshold" "$CONF_NAME")"
if [ -z "$goodThreshold" ]; then
 echo "[$good] No good threshold specified. Will build/run this hash to collect an initial good value."
 build_sha "$good" "$duration_mins" &> "$BISECT_DIR/$good-build.log"
 stress_sha "$good" "$test" $count &> "$BISECT_DIR/$good-run.log" &
fi

badThreshold="$(get_conf_val ".badThreshold" "$CONF_NAME")"
if [ -z "$badThreshold" ]; then
 echo "[$bad] No bad threshold specified. Will build/run this hash to collect an initial bad value."
 build_sha "$bad" "$duration_mins" &> "$BISECT_DIR/$bad-build.log"
 stress_sha "$bad" "$test" $count &> "$BISECT_DIR/$bad-run.log" &
fi

wait
if [ -z "$goodThreshold" ]; then
  goodThreshold="$(calc_avg_ops "artifacts/$good*/$test/run_*/*.perf/stats.json")"
  set_conf_val "goodThreshold" "$goodThreshold" "$CONF_NAME"
fi

if [ -z "$badThreshold" ]; then
  badThreshold="$(calc_avg_ops "artifacts/$bad*/$test/run_*/*.perf/stats.json")"
  set_conf_val "badThreshold" "$badThreshold" "$CONF_NAME"
fi

if [ -z "$goodThreshold" ] || [ -z "$badThreshold" ]; then
  echo "Unable to calculate initial thresholds. Manually set in json, and re-invoke this bisection. Aborting."
  exit 1
fi

if [[ goodThreshold -le badThreshold ]]; then
  echo "Good threshold is <= bad threshold. Cannot bisect. Aborting."
  exit 1
fi

log "Initial thresholds [good >= $goodThreshold, bad <= $badThreshold]" "$LOG_NAME"

git bisect start --first-parent
git bisect good "$good"
git bisect bad "$bad"
git bisect run ./scripts/bisection/bisect.sh $test $count $duration_mins
log "Bisection complete. Suspect commit:" "$LOG_NAME"
git bisect visualize &>> "$LOG_NAME"
git bisect log >> "$BISECT_DIR/bisect-log.log"
