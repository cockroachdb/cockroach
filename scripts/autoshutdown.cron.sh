#!/usr/bin/env bash

# This script is intended to be called periodical. If it doesn't detect remote
# sessions in a given number of consecutive runs, a shutdown is initiated.
#
# To disable auto-shutdown: `sudo touch /.active`

set -euxo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <num_periods>"
  exit 1
fi

MAX_COUNT="$1"

if ! [ "$MAX_COUNT" -gt 0 -a "$MAX_COUNT" -lt 10000 ]; then
  echo "Invalid argument '$MAX_COUNT'"
  exit 1
fi

# We maintain the count of how many consecutive iterations of this script did
# NOT detect a remote session. Once we exceed MAX_COUNT, we shut down.
# We use /dev/shm which is not persistent over reboots.
FILE=/dev/shm/autoshutdown-count
COUNT=0

if [ -f /.active ] || w -hs | grep -q pts; then
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
else
  /sbin/shutdown -h
fi
