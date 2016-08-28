#!/usr/bin/env bash
#
# On a (recent enough) Debian/Ubuntu system, bootstraps a source Go install
# (with improved parallel build patches) and the cockroach repo.

set -eu

GOVERSION="1.7"

sudo apt-get update -q && sudo apt-get install -q -y --no-install-recommends g++ gcc libc6-dev make git gdb patch bzip2

mkdir -p go-bootstrap
curl "https://storage.googleapis.com/golang/go${GOVERSION}.linux-amd64.tar.gz" | tar -C go-bootstrap -xvz --strip=1
curl "https://storage.googleapis.com/golang/go${GOVERSION}.src.tar.gz" | tar -xvz

patch -p1 -d go < ~/scripts/parallelbuilds-go1.7.patch

(cd ~/go/src && GOROOT_BOOTSTRAP=~/go-bootstrap ./make.bash)

echo 'export GOPATH='"${HOME}"'; export PATH="'"${HOME}"'/go/bin:${GOPATH}/bin:${PATH}"' >> ~/.bashrc_go
echo ". .bashrc_go" >> ~/.bashrc

. .bashrc_go

go get -d github.com/cockroachdb/cockroach
