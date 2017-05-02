#!/usr/bin/env bash
#
# On a Debian/Ubuntu system, bootstraps a Go toolchain.

set -euxo pipefail

GOVERSION=1.8

sudo apt-get install -y gcc g++ make

# Install Go from source so we can add a patch to run cgo builds in parallel.
GOROOT="go${GOVERSION}"
rm -fr ~/go-bootstrap ~/${GOROOT}
mkdir -p ~/go-bootstrap ~/${GOROOT} ~/go
curl "https://storage.googleapis.com/golang/go${GOVERSION}.linux-amd64.tar.gz" | tar -C ~/go-bootstrap -xz --strip=1
curl "https://storage.googleapis.com/golang/go${GOVERSION}.src.tar.gz" | tar --strip-components=1 -C ~/${GOROOT} -xz

# Apply the patch for the "major" go version (e.g. 1.6, 1.7).
GOPATCHVER=$(echo ${GOVERSION} | grep -o "^[0-9]\+\.[0-9]\+")
patch -p1 -d ~/${GOROOT} < "bootstrap/parallelbuilds-go${GOPATCHVER}.patch"

(cd ~/${GOROOT}/src && GOROOT_BOOTSTRAP=~/go-bootstrap ./make.bash)

mkdir -p ~/bin
ln -sf ~/${GOROOT}/bin/go* ~/bin
