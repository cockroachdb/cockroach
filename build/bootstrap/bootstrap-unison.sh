#!/usr/bin/env bash
#
# On a Debian/Ubuntu system, bootstraps the Unison file-syncer.

set -euxo pipefail

sudo apt-get install -y --no-install-recommends ocaml-nox

# Ubuntu's "unison" package does not ship unison-fsmonitor, which is needed for
# the "-repeat watch" option to function properly.
git clone --branch=v2.51.2 https://github.com/bcpierce00/unison
(cd unison && make)
for bin in unison unison-fsmonitor; do
  sudo install -m 755 "unison/src/$bin" "/usr/local/bin/$bin"
done

echo fs.inotify.max_user_watches=524288 | sudo tee /etc/sysctl.d/60-max-user-watches.conf
sudo service procps restart

echo "UNISONLOCALHOSTNAME=$(hostname)-$(date +%Y%m%d-%H%M%S)" | sudo tee -a /etc/environment
