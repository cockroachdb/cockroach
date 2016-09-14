#!/usr/bin/env bash

# This script is intended to be called once per minute. If it doesn't detect
# remote sessions in 10 consecutive runs, a shutdown is initiated.
# To disable auto-shutdown: `sudo touch /.active`

# We maintain the count of how many consecutive iterations of this script did
# NOT detect a remote session. Once we exceed MAX_COUNT, we shut down.
# We use /dev/shm which is not persistent over reboots.
FILE=/dev/shm/autoshutdown-count
COUNT=0
MAX_COUNT=10

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
