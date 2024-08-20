#!/usr/bin/env bash

# This script is intended to be called periodical. If it doesn't detect remote
# sessions in a given number of consecutive runs, a shutdown is initiated.
#
# To disable auto-shutdown: `sudo touch /.active`

set -euxo pipefail

if [[ -z ${1-} ]]; then
  echo "Usage: $0 <num_periods>"
  exit 1
fi

MAX_COUNT="$1"

export PATH=$PATH:/snap/bin

if ! [[ $MAX_COUNT -gt 0 && $MAX_COUNT -lt 1000 ]]; then
  echo "Invalid argument '$MAX_COUNT'"
  exit 1
fi

# We maintain the count of how many consecutive iterations of this script did
# NOT detect a remote session. Once we exceed MAX_COUNT, we shut down.
# We use /dev/shm which is not persistent over reboots.
COUNT_FILE=/dev/shm/autoshutdown-count
COUNT=0

# We ignore active if it has not been modified in the last two
# days. If one must keep their GCE worker alive longer than that
# without touching the file in the middle, you can set the mod
# time on the file into the future. To set a mod time to the future
# you can use `touch -t [[CC]YY]MMDDhhmm /.active` or `touch -d "+3 days" /.active`.
ACTIVE_FILE=/.active
AUTO_SHUTDOWN_DURATION=$(( 2 * 24 * 60 * 60 ))
# warn about /.active every day
WARN_EVERY=$(( 1 * 24 * 60 * 60 ))
WARN_URL=https://us-east4-cockroach-workers.cloudfunctions.net/gceworker-autoshutdown

active_exists() {
  if [[ ! -f $ACTIVE_FILE ]]; then
    return 1
  fi
  active=$(date -r "$ACTIVE_FILE" +%s)
  age=$(( $(date +%s) - active ))
  if [[ $age -lt 0 ]]; then
    # The active file is set to a future date
    maybe_warn
  fi
  [[ $age -lt $AUTO_SHUTDOWN_DURATION ]]
}

function maybe_warn() {
  warned_file=/root/last_warned
  if [[ -e $warned_file ]]; then
    warned_file_ts="$(date -r "$warned_file" +%s)"
    age=$(( $(date +%s) - warned_file_ts ))
    if [[ $age -lt $WARN_EVERY ]]; then
      return
    fi
  fi
  name="$(curl -fSs -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/name)"
  zone="$(curl -fSs -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/zone)"
  # Use the `created-by` label to help identify the owner. The label uses
  # user's email with "@" replaced by "__at__" and "." replaced by "__dot__".
  creator="$(gcloud compute instances describe --zone "$zone" "$name" --format="value(labels.created-by)")"
  email="$(echo "$creator" | sed -e 's/__at__/@/g' -e 's/__dot__/./g')"

  if [[ -z $email ]]; then
    return
  fi
  set +x
  token=$(gcloud auth print-identity-token)
  # Call the warning cloud function, which will slack the owner.
  curl -fSs -H "Authorization: Bearer $token" "$WARN_URL?email=$email&worker=$name"
  set -x
  touch "$warned_file"
}

if active_exists || w -hs | grep pts | grep -vq "pts/[0-9]* *tmux" || pgrep -f remote-dev-server.sh; then
  # Auto-shutdown is disabled (via /.active) or there is a remote session.
  echo 0 > $COUNT_FILE
  exit 0
fi

if [[ -f $COUNT_FILE ]]; then
  COUNT="$(cat $COUNT_FILE)"
fi

COUNT=$((COUNT+1))

if [[ $COUNT -le $MAX_COUNT ]]; then
  echo "$COUNT" > "$COUNT_FILE"
  exit 0
fi

/sbin/shutdown -h
