#!/usr/bin/env bash

set -euxo pipefail

autoshutdown_script=/root/autoshutdown.cron.sh
autoshutdown_script_url=https://raw.githubusercontent.com/cockroachdb/cockroach/master/build/bootstrap/autoshutdown.cron.sh

if [[ ! -e $autoshutdown_script ]]; then
  echo "skipping update, still bootstrapping?"
  exit 0
fi

export PATH=$PATH:/snap/bin

echo "downloading and updating the autoshutdown script"
curl --retry 3 --retry-delay 2 --show-error --fail --location --silent --output "$autoshutdown_script.tmp" "$autoshutdown_script_url"
mv -f "$autoshutdown_script.tmp" "$autoshutdown_script"
touch "$autoshutdown_script"
chmod 755 "$autoshutdown_script"
