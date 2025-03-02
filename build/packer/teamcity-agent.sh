#!/usr/bin/env bash

# Copyright 2017 The Cockroach Authors.
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

# Avoid saving any Bash history.
HISTSIZE=0
ARCH=$(uname -m)

# Add third-party APT repositories.
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 0EBFCD88
cat > /etc/apt/sources.list.d/docker.list <<EOF
deb https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable
EOF

curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
echo 'deb https://packages.cloud.google.com/apt cloud-sdk main' > /etc/apt/sources.list.d/gcloud.list

curl -sLS https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor | apt-key add -
echo "deb https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" > /etc/apt/sources.list.d/azure-cli.list

# Some images come with apt autoupgrade job running at start, let's give it a few minutes to finish to avoid races.
echo "Sleeping for 3 minutes to allow apt daily cronjob to finish..."
sleep 3m

add-apt-repository ppa:git-core/ppa
apt-get update

python_packages="python3"
if [[ $(lsb_release -cs) = "focal" ]]; then
    python_packages="$python_packages python2"
fi
# Installing gnome-keyring prevents the error described in
# https://github.com/moby/moby/issues/34048
apt-get install --yes \
  autoconf \
  azure-cli \
  bison \
  build-essential \
  curl \
  docker-ce \
  docker-compose \
  flex \
  git \
  gnome-keyring \
  google-cloud-sdk \
  google-cloud-cli-gke-gcloud-auth-plugin \
  gnupg2 \
  jq \
  openjdk-11-jre-headless \
  pass \
  $python_packages \
  sudo \
  unzip \
  zip

# Enable support for executing binaries of all architectures via qemu emulation
# (necessary for building arm64 Docker images)
apt-get install --yes binfmt-support qemu-user-static

# Verify that both of the platforms we support Docker for can be built.
if [[ $ARCH = x86_64 ]]; then
    docker run --attach=stdout --attach=stderr --platform=linux/amd64 --rm --pull=always registry.access.redhat.com/ubi9/ubi-minimal uname -p
    docker run --attach=stdout --attach=stderr --platform=linux/arm64 --rm --pull=always registry.access.redhat.com/ubi9/ubi-minimal uname -p
fi

case $ARCH in
    x86_64) WHICH=x86_64; SHASUM=97bf730372f9900b2dfb9206fccbcf92f5c7f3b502148b832e77451aa0f9e0e6 ;;
    aarch64) WHICH=aarch64; SHASUM=77620f99e9d5f39cf4a49294c6a68c89a978ecef144894618974b9958efe3c2a ;;
esac
curl -fsSL https://github.com/Kitware/CMake/releases/download/v3.20.3/cmake-3.20.3-linux-$WHICH.tar.gz -o /tmp/cmake.tar.gz
sha256sum -c - <<EOF
$SHASUM /tmp/cmake.tar.gz
EOF
tar --strip-components=1 -C /usr -xzf /tmp/cmake.tar.gz
rm -f /tmp/cmake.tar.gz

# NB: The Go version should match up to the version required by managed-service.
# CRDB builds use Bazel and therefore have no dependency on the globally-installed Go CLI.
if [[ $ARCH = x86_64 ]]; then
    curl -fsSL https://dl.google.com/go/go1.21.3.linux-amd64.tar.gz > /tmp/go.tgz
    sha256sum -c - <<EOF
1241381b2843fae5a9707eec1f8fb2ef94d827990582c7c7c32f5bdfbfd420c8  /tmp/go.tgz
EOF
    tar -C /usr/local -zxf /tmp/go.tgz && rm /tmp/go.tgz
    # Explicitly symlink the pinned version to /usr/bin.
    for f in /usr/local/go/bin/*; do
        ln -s "$f" /usr/bin
    done
fi

case $ARCH in
    x86_64) WHICH=amd64; SHASUM=4cb534c52cdd47a6223d4596d530e7c9c785438ab3b0a49ff347e991c210b2cd ;;
    aarch64) WHICH=arm64; SHASUM=c1de6860dd4f8d5e2ec270097bd46d6a211b971a0b8b38559784bd051ea950a1 ;;
esac

# Install Bazelisk.
# Keep this in sync with `build/bazelbuilder/Dockerfile` and `build/bootstrap/bootstrap-debian.sh`.
curl -fsSL https://github.com/bazelbuild/bazelisk/releases/download/v1.10.1/bazelisk-linux-$WHICH > /tmp/bazelisk
sha256sum -c - <<EOF
$SHASUM /tmp/bazelisk
EOF
chmod +x /tmp/bazelisk
mv /tmp/bazelisk /usr/bin/bazel

# Install protoc.
case $ARCH in
    x86_64) WHICH=x86_64; SHASUM=ed8fca87a11c888fed329d6a59c34c7d436165f662a2c875246ddb1ac2b6dd50 ;;
    aarch64) WHICH=aarch_64; SHASUM=99975a8c11b83cd65c3e1151ae1714bf959abc0521acb659bf720524276ab0c8 ;;
esac

curl -fsSL https://github.com/protocolbuffers/protobuf/releases/download/v25.1/protoc-25.1-linux-$WHICH.zip > /tmp/protoc.zip
sha256sum -c - <<EOF
$SHASUM /tmp/protoc.zip
EOF
unzip /tmp/protoc.zip -d /tmp/protoc
rm /tmp/protoc.zip
mv /tmp/protoc/bin/protoc /usr/bin/protoc
mv /tmp/protoc/include/google /usr/include/google
rm -rf /tmp/protoc

# Add a user for the TeamCity agent if it doesn't exist already.
id -u agent &>/dev/null 2>&1 || adduser agent --disabled-password

# Give the user for the TeamCity agent Docker rights.
usermod -a -G docker agent

# Download the TeamCity agent code and install its configuration.
# N.B.: This must be done as the agent user.
su - agent <<'EOF'
set -euxo pipefail

ARCH=$(uname -m)

# Set the default branch name for git. (Out of an abundance of caution because
# I don't know how well TC handles having a different default branch name, stick
# with "master".)
git config --global init.defaultBranch master

if [ $ARCH = x86_64 ]; then
    echo 'export GOPATH="$HOME"/work/.go' >> .profile && source .profile
else
    GOPATH="$HOME"/work/.go
fi

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

# For master and the last two release, download the builder and acceptance
# containers.
repo="$GOPATH"/src/github.com/cockroachdb/cockroach
git clone --shared system/git/cockroach.git "$repo"
if [ $ARCH = x86_64 ]; then
    cd "$repo"
    # Work around a bug in the builder's git version (at the time of writing)
    # which would corrupt the submodule defs. Probably good to remove once the
    # builder uses Ubuntu 18.04 or higher.
    git submodule update --init --recursive
    for branch in $(git branch --all --list --sort=-committerdate 'origin/release-*' | head -n2) master
    do
        git checkout "$branch"
        # TODO(benesch): store the acceptanceversion somewhere more accessible.
        docker pull $(git grep us-east1-docker.pkg.dev/crl-ci-images/cockroach/acceptance -- '*.go' | sed -E 's/.*"([^"]*).*"/\1/') || true
    done
    cd -
fi
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

# Enable LRU pruning of Docker images.
# https://github.com/stepchowfun/docuum#running-docuum-in-a-docker-container
DOCUUM_VERSION=0.9.4
cat > /etc/systemd/system/docuum.service <<EOF
[Unit]
Description=Remove Stale Docker Images
After=docker.service
Requires=docker.service

[Service]
ExecStart=/usr/bin/docker run \
          --init \
          --rm \
          --tty \
          --name docuum \
          --volume /var/run/docker.sock:/var/run/docker.sock \
          --volume docuum:/root stephanmisc/docuum:$DOCUUM_VERSION \
          --threshold '128 GB'
Restart=always

[Install]
WantedBy=multi-user.target
EOF
systemctl enable docuum.service
# Prefetch the image
docker pull stephanmisc/docuum:$DOCUUM_VERSION

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
