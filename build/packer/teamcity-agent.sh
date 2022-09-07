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
ARCH=$(uname -m)

# Add third-party APT repositories.
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 0EBFCD88
cat > /etc/apt/sources.list.d/docker.list <<EOF
deb https://download.docker.com/linux/ubuntu bionic stable
EOF
# Git 2.7, which ships with Xenial, has a bug where submodule metadata sometimes
# uses absolute paths instead of relative paths, which means the affected
# submodules cannot be mounted in Docker containers. Use the latest version of
# Git until we upgrade to a newer Ubuntu distribution.
add-apt-repository ppa:git-core/ppa
apt-get update --yes

# Install the sudo version patched for CVE-2021-3156
apt-get install --yes sudo

apt-get install --yes \
  autoconf \
  bison \
  build-essential \
  curl \
  docker-ce \
  docker-compose \
  flex \
  gnome-keyring \
  gnupg2 \
  git \
  jq \
  openjdk-11-jre-headless \
  pass \
  python2 \
  python3 \
  unzip

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

if [ $ARCH = x86_64 ]; then
    curl -fsSL https://dl.google.com/go/go1.19.1.linux-amd64.tar.gz > /tmp/go.tgz
    sha256sum -c - <<EOF
acc512fbab4f716a8f97a8b3fbaa9ddd39606a28be6c2515ef7c6c6311acffde  /tmp/go.tgz
EOF
    tar -C /usr/local -zxf /tmp/go.tgz && rm /tmp/go.tgz

    # Install the older version in parallel in order to run the acceptance test on older branches
    # TODO: Remove this when 21.1 is EOL (2022-11-18, according to
    # https://www.cockroachlabs.com/docs/releases/release-support-policy.html)
    curl -fsSL https://dl.google.com/go/go1.15.14.linux-amd64.tar.gz > /tmp/go_old.tgz
    sha256sum -c - <<EOF
6f5410c113b803f437d7a1ee6f8f124100e536cc7361920f7e640fedf7add72d /tmp/go_old.tgz
EOF
    mkdir -p /usr/local/go1.15
    tar -C /usr/local/go1.15 --strip-components=1 -zxf /tmp/go_old.tgz && rm /tmp/go_old.tgz

# Explicitly symlink the pinned version to /usr/bin.
    for f in `ls /usr/local/go/bin`; do
        ln -s /usr/local/go/bin/$f /usr/bin
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

# gcloud won't be automatically installed for ARM machines (which run on AWS).
if [ $ARCH = aarch64 ]; then
    curl -fsSL https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-377.0.0-linux-arm.tar.gz > /tmp/gcloud.tar.gz
    sha256sum -c - <<EOF
42dd29714b052d3460b005b3d09faacdb0818e70edd60c20d447a1594fd6aa83 /tmp/gcloud.tar.gz
EOF
    tar -C /usr/local -zxf /tmp/gcloud.tar.gz
    rm /tmp/gcloud.tar.gz
    /usr/local/google-cloud-sdk/install.sh -q --usage-reporting false
    ln -s /usr/local/google-cloud-sdk/bin/gcloud /usr/bin
    ln -s /usr/local/google-cloud-sdk/bin/gsutil /usr/bin
fi

# Installing gnome-keyring prevents the error described in
# https://github.com/moby/moby/issues/34048

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
    for branch in $(git branch --all --list --sort=-committerdate 'origin/release-*' | head -n1) master
    do
        # Clean out all non-checked-in files. This is because of the check-in of
        # the generated execgen files. Once we are no longer building 20.1 builds,
        # the `git clean -dxf` line can be removed.
        git clean -dxf

        git checkout "$branch"
        # Stupid submodules.
        rm -rf vendor; git checkout vendor; git submodule update --init --recursive
        COCKROACH_BUILDER_CCACHE=1 build/builder.sh make test testrace TESTTIMEOUT=45m TESTS=-
        # TODO(benesch): store the acceptanceversion somewhere more accessible.
       docker pull $(git grep cockroachdb/acceptance -- '*.go' | sed -E 's/.*"([^"]*).*"/\1/') || true
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
