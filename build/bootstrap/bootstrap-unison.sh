#!/usr/bin/env bash

# Copyright 2018 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#
# On a Debian/Ubuntu system, bootstraps the Unison file-syncer.

set -euxo pipefail


extract_to=$(mktemp -d)
curl -L https://github.com/bcpierce00/unison/releases/download/v2.51.4/unison-v2.51.4+ocaml-4.12.0+x86_64.linux.tar.gz | tar -C "$extract_to" -xz

for bin in unison unison-fsmonitor; do
  sudo install -m 755 "$extract_to/bin/$bin" "/usr/local/bin/$bin"
done

rm -rf "$extract_to"

echo fs.inotify.max_user_watches=524288 | sudo tee /etc/sysctl.d/60-max-user-watches.conf
sudo service procps restart

echo "UNISONLOCALHOSTNAME=$(hostname)-$(date +%Y%m%d-%H%M%S)" | sudo tee -a /etc/environment
