#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

write_teamcity_config() {
  sudo -u agent tee /home/agent/conf/buildAgent.properties <<EOF
serverUrl=https://teamcity.cockroachdb.com
name=
workDir=../work
tempDir=../temp
systemDir=../system
EOF
}

sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 0EBFCD88
sudo tee /etc/apt/sources.list.d/docker.list <<EOF
deb https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable
EOF

sudo apt-get update

sudo apt-get install --yes \
  autoconf \
  binfmt-support \
  bison \
  build-essential \
  cmake \
  curl \
  docker-ce \
  docker-compose \
  flex \
  git \
  gnome-keyring \
  gnupg2 \
  jq \
  openjdk-11-jre-headless \
  pass \
  python3 \
  qemu-user-static \
  sudo \
  unzip \
  zip

# Give the user for the TeamCity agent Docker rights.
# NB: We expect the agent user to already exist. See the documentation.
sudo usermod -a -G docker agent

# Download the TeamCity agent code and install its configuration.
# N.B.: This must be done as the agent user.
sudo su - agent <<'EOF'
set -euxo pipefail

ARCH=$(uname -m)

# Set the default branch name for git. (Out of an abundance of caution because
# I don't know how well TC handles having a different default branch name, stick
# with "master".)
git config --global init.defaultBranch master

wget https://teamcity.cockroachdb.com/update/buildAgent.zip
unzip buildAgent.zip
rm buildAgent.zip

# Cache the current version of the main Cockroach repository on the agent to
# speed up the first build. As of 2017-10-13, the main repository is 450MB (!).
# The other repositories we run CI on are small enough not to be worth caching,
# but feel free to add them if it becomes necessary.
#
# WARNING: This uses undocumented implementation details of TeamCity's Git
# alternate system.
git clone --bare https://github.com/cockroachdb/cockroach system/git/cockroach.git
cat > system/git/map <<EOS
https://github.com/cockroachdb/cockroach = cockroach.git
EOS

EOF
write_teamcity_config

# Configure the Teamcity agent to start when the server starts.
#
# systemd will nuke the auto-upgrade process unless we mark the service as
# "oneshot". This has the unfortunate side-effect of making `systemctl start
# teamcity-agent` hang forever when run manually, but it at least works when the
# system starts the service at bootup.
#
# TODO(benesch): see if we can fix this with Type=forking, KillMode=process.
sudo tee /etc/systemd/system/teamcity-agent.service <<EOF
[Unit]
Description=TeamCity Build Agent
After=network.target
Requires=network.target

[Service]
Type=oneshot
RemainAfterExit=yes
User=agent
PIDFile=/home/agent/logs/buildAgent.pid
ExecStart=/home/agent/bin/agent.sh start
ExecStop=/home/agent/bin/agent.sh stop
SuccessExitStatus=0 143

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl enable teamcity-agent.service

# Boot the TeamCity agent so it can be upgraded by the server (i.e., download
# and install whatever plugins the server has installed) before we bake the
# image.
#
# WARNING: There seems to be no clean way to check when the upgrade is complete.
# As a hack, the string below seems to appear in the logs iff the upgrade is
# successful.
sudo systemctl start teamcity-agent.service
until sudo grep -q 'Updating agent parameters on the server' /home/agent/logs/teamcity-agent.log
do
  echo .
  sleep 5
done

# Re-write the TeamCity config to discard the name and authorization token
# assigned by the TeamCity server; otherwise, agents created from this image
# might look like unauthorized duplicates to the TeamCity server.
sudo systemctl stop teamcity-agent.service
write_teamcity_config

# Prepare for imaging by removing unnecessary files.
sudo rm -rf /home/agent/logs
sudo apt-get clean
sync
