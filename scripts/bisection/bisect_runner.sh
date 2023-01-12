#!/bin/bash

set -ex

#these can be parameterised
test="ycsb/B/nodes=3"
branch="origin/master"

#use dates OR hashes, but not both
from="2022-12-21 09:00:00Z"
#to="2022-12-23 22:00:00Z"
good=
bad=

count=4
duration_mins=10

#bisect_dir="${test//[^[:alnum:]]/-}/${branch//[^[:alnum:]]/-}/$from,$to"
bisect_dir=/home/miral/workspace/bisections/ycsb
#make this visible to bisect-util
export BISECT_DIR=$bisect_dir


# first-parent is good for release branches where we generally know the merge parents are OK
# git bisect start --first-parent
export BISECT_START_CMD="git bisect start"

SCRIPT_DIR=$(dirname "$0")
. "$SCRIPT_DIR"/bisect-util.sh

git reset --hard

trapped() {
  #we need to be able to collect a non-zero return code from prompt_user
  set +e
  echo "interrupt!"
  prompt_user "$(git rev-parse --short HEAD)" "-1"

  if [[ $? -gt 125 ]]; then
    exit 1
  fi

  # relaunch this script and restart bisection with updated config
  exec "$0" "$@"
}

trap 'trapped' INT

if [ -f "$BISECT_LOG" ]; then
  echo "Bisect log found. Replaying"
  $BISECT_START_CMD
  git bisect replay "$BISECT_LOG"
else
  if [[ -z $good || -z $bad ]]; then
    [[ -n $from && -n $to ]] || { echo "You must specify (good AND bad hashes) OR (to AND from dates)"; exit 1; }
    hashes="$(git log "$branch" --merges --pretty=format:'%h' --date=short --since "$from" --until "$to")"
    good=$(echo "$hashes" | tail -1)
    bad=$(echo "$hashes" | head -1)
  fi

  goodVal="$(get_hash_result "$good")"

  # running in parallel is fine, but building saturates CPU so we do that sequentially
  if [ -z "$goodVal" ]; then
   echo "[$good] No good threshold found. Will build/run this hash to collect an initial good value."
   build_hash "$good" "$duration_mins"
   test_hash "$good" "$test" $count &
  fi

  badVal="$(get_hash_result "$bad")"
  if [ -z "$badVal" ]; then
   echo "[$bad] No bad threshold specified. Will build/run this hash to collect an initial bad value."
   build_hash "$bad" "$duration_mins"
   test_hash "$bad" "$test" $count &
  fi

  wait

  # testing this variable again here as a way to determine whether we ran the test above
  if [ -z "$goodVal" ]; then
    goodVal="$(calc_avg_ops "$good" "$test")"
    set_hash_result "$good" "$goodVal"
  fi

  if [ -z "$badVal" ]; then
    badVal="$(calc_avg_ops "$bad" "$test")"
    set_hash_result "$bad" "$badVal"
  fi

  [[ goodVal -gt badVal ]] || { echo "Initial good threshold [$goodVal] must be > initial bad threshold [$badVal]. Cannot bisect. Aborting."; exit 1;  }

  set_conf_val "goodThreshold" "$goodVal"
  set_conf_val "badThreshold" "$badVal"

  log "Bisecting regression in [$test] using commit range [$good (known good),$bad (known bad)]"
  log "Thresholds [good >= $goodVal, bad <= $badVal]"

  $BISECT_START_CMD
  git bisect good "$good"
  git bisect bad "$bad"
fi

git bisect run "$SCRIPT_DIR"/bisect.sh "$test" "$count" "$duration_mins"

log "Bisection complete. Suspect commit:"
git bisect visualize &>> "$INFO_LOG"

git bisect log > "$BISECT_LOG"
