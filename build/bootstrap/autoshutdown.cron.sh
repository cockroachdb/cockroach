#!/usr/bin/env bash

# Copyright 2017 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# This script is intended to be called periodical. If it doesn't detect remote
# sessions in a given number of consecutive runs, a shutdown is initiated.
#
# To disable auto-shutdown: `sudo touch /.active`

set -euxo pipefail

if [ -z "${1-}" ]; then
  echo "Usage: $0 <num_periods> [shutdown command...]"
  exit 1
fi

MAX_COUNT="$1"
shift

if ! [ "$MAX_COUNT" -gt 0 -a "$MAX_COUNT" -lt 10000 ]; then
  echo "Invalid argument '$MAX_COUNT'"
  exit 1
fi

# We maintain the count of how many consecutive iterations of this script did
# NOT detect a remote session. Once we exceed MAX_COUNT, we shut down.
# We use /dev/shm which is not persistent over reboots.
FILE=/dev/shm/autoshutdown-count
COUNT=0

# We ignore active if it has not been modified in the last two
# days. If one must keep their GCE worker alive longer than that
# without touching the file in the middle, you can set the mod
# time on the file into the future. To set a mod time to the future
# you can use `touch -t [[CC]YY]MMDDhhmm /.active`.
ACTIVE_FILE=/.active
AUTO_SHUTDOWN_DURATION=$(( 2 * 24 * 60 * 60 ))

active_exists() {
  if [[ ! -f "$ACTIVE_FILE" ]]; then
    return 1
  fi
  active=$(date -r "$ACTIVE_FILE" +%s)
  age=$(( $(date +%s) - $active ))
  [[ $age -lt $AUTO_SHUTDOWN_DURATION ]]
}

if active_exists || w -hs | grep pts | grep -vq "pts/[0-9]* *tmux" || pgrep unison || pgrep -f remote-dev-server.sh; then
  # Auto-shutdown is disabled (via /.active) or there is a remote session.
  echo 0 > $FILE
  exit 0
fi

if [ -f $FILE ]; then
  COUNT=$(cat $FILE)
fi

COUNT=$((COUNT+1))

if [ $COUNT -le $MAX_COUNT ]; then
  echo $COUNT > $FILE
  exit 0
fi

# Shut down.

if [ -z "${1-}" ]; then
  /sbin/shutdown -h
else
  # Run whatever command was passed on the command line.
  # shellcheck disable=SC2068
  $@
fi
