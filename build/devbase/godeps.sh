#!/bin/bash
# Grab Go-related dependencies.

set -e

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

# NOTE: Use "make listdeps" to update this list. We can't just use "go
# list" here because this script is run during docker container builds
# before the cockroach code is present. The GLOCKFILE is present,
# but we can't use it because it deals in repos and not packages
# (and adding /... to repo paths will match packages that have been
# downloaded but whose dependencies have not).
pkgs="
github.com/biogo/store/interval
github.com/biogo/store/llrb
github.com/cockroachdb/c-lz4
github.com/cockroachdb/c-protobuf
github.com/cockroachdb/c-rocksdb
github.com/cockroachdb/c-snappy
github.com/coreos/etcd/Godeps/_workspace/src/github.com/gogo/protobuf/proto
github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context
github.com/coreos/etcd/raft
github.com/coreos/etcd/raft/raftpb
github.com/elazarl/go-bindata-assetfs
github.com/gogo/protobuf/proto
github.com/golang/glog
github.com/inconshreveable/mousetrap
github.com/spf13/cobra
github.com/spf13/pflag
golang.org/x/net/context
gopkg.in/yaml.v1
"

set -x

# NOTE: "glock sync" downloads the source but does not install libraries
# into GOPATH/pkg. Install both race and non-race builds here.
go install ${pkgs}
go install -race ${pkgs}
