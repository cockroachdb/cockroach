#!/usr/bin/env bash

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

# Avoid saving any Bash history.
HISTSIZE=0

# Add third-party APT repositories.
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 0EBFCD88
cat > /etc/apt/sources.list.d/docker.list <<EOF
deb https://download.docker.com/linux/ubuntu xenial stable
EOF
apt-add-repository ppa:webupd8team/java
add-apt-repository ppa:gophers/archive
# Git 2.7, which ships with Xenial, has a bug where submodule metadata sometimes
# uses absolute paths instead of relative paths, which means the affected
# submodules cannot be mounted in Docker containers. Use the latest version of
# Git until we upgrade to a newer Ubuntu distribution.
add-apt-repository ppa:git-core/ppa
apt-get update --yes

# Auto-accept the Oracle Java license agreement.
debconf-set-selections <<< "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true"

# Install the necessary dependencies. Keep this list small!
apt-get install --yes \
  docker-ce \
  git \
  golang-1.9 \
  oracle-java8-installer \
  unzip

# Link Go into the PATH; the PPA installs it into /usr/lib/go-1.x/bin.
ln -s /usr/lib/go-1.9/bin/go /usr/bin/go

# Add a user for the TeamCity agent with Docker rights.
adduser agent --disabled-password
adduser agent docker

# Download the TeamCity agent code and install its configuration.
# N.B.: This must be done as the agent user.
su - agent <<'EOF'
set -euxo pipefail

echo 'export GOPATH="$HOME"/work/.go' >> .profile && source .profile

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

# For master and the last two release, download the builder and postgres-test
# containers.
repo="$GOPATH"/src/github.com/cockroachdb/cockroach
git clone --shared system/git/cockroach.git "$repo"
cd "$repo"
for branch in $(git branch --all --list --sort=-committerdate 'origin/release-*' | head -n1) master
do
  git checkout "$branch"
  COCKROACH_BUILDER_CCACHE=1 build/builder.sh make gotestdashi
  # TODO(benesch): store the postgres-test version somewhere more accessible.
  docker pull $(git grep cockroachdb/postgres-test -- '*.go' | sed -E 's/.*"([^"]*).*"/\1/') || true
done
cd -
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
cat > /etc/systemd/system/teamcity-agent.service <<EOF
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
systemctl enable teamcity-agent.service

# Boot the TeamCity agent so it can be upgraded by the server (i.e., download
# and install whatever plugins the server has installed) before we bake the
# image.
#
# WARNING: There seems to be no clean way to check when the upgrade is complete.
# As a hack, the string below seems to appear in the logs iff the upgrade is
# successful.
systemctl start teamcity-agent.service
until grep -q 'Updating agent parameters on the server' /home/agent/logs/teamcity-agent.log
do
  echo .
  sleep 5
done

# Re-write the TeamCity config to discard the name and authorization token
# assigned by the TeamCity server; otherwise, agents created from this image
# might look like unauthorized duplicates to the TeamCity server.
systemctl stop teamcity-agent.service
write_teamcity_config

# Prepare for imaging by removing unnecessary files.
rm -rf /home/agent/logs
apt-get clean
sync
