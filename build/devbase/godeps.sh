#!/bin/bash
# Grab Go-related dependencies.

set -eu

# Ensure we only have one entry in GOPATH (glock gets confused
# by more).
export GOPATH=$(cd $(dirname $0)/../../../../../.. && pwd)

# go vet is special: it installs into $GOROOT (which $USER may not have
# write access to) instead of $GOPATH. It is usually but not always
# installed along with the rest of the go toolchain. Don't try to
# install it if it's already there.
if ! go vet 2>/dev/null; then
    go get golang.org/x/tools/cmd/vet
fi

if ! test -e ${GOPATH}/bin/glock ; then
    # glock is used to manage the rest of our dependencies (and to update
    # itself, so no -u here)
    go get github.com/robfig/glock
fi

${GOPATH}/bin/glock sync github.com/cockroachdb/cockroach

# We can't just use "go list" here because this script is run during docker
# container builds before the cockroach code is present. The GLOCKFILE is
# present, but we can't use it because it deals in repos and not packages (and
# adding /... to repo paths will match packages that have been downloaded but
# whose dependencies have not).
pkgs=$(cat build/devbase/deps)

set -x

# NOTE: "glock sync" downloads the source but does not install libraries
# into GOPATH/pkg. Install both race and non-race builds here.
go install ${pkgs}
go install -race ${pkgs}
