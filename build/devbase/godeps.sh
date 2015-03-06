#!/bin/bash
# Grab Go-related dependencies.

set -e

# Ensure we only have one entry in GOPATH (glock gets confused
# by more). Adding $GOPATH/bin to $PATH is common but optional
# so add it here to make sure we can find glock.
export GOPATH=$(cd $(dirname $0)/../../../../../.. && pwd)
export PATH=$GOPATH/bin:$PATH

glock sync github.com/cockroachdb/cockroach

# NOTE: Use "make godeps" to update this list. We can't just use "go
# list" here because this script is run during docker container builds
# before the cockroach code is present. The GLOCKFILE is present,
# but we can't use it because it deals in repos and not packages
# (and adding /... to repo paths will match packages that have been
# downloaded but whose dependencies have not).
pkgs="
code.google.com/p/biogo.store/interval
code.google.com/p/biogo.store/llrb
code.google.com/p/go-commander
code.google.com/p/go-uuid/uuid
code.google.com/p/snappy-go/snappy
github.com/cockroachdb/c-protobuf
github.com/cockroachdb/c-rocksdb
github.com/cockroachdb/c-snappy
github.com/coreos/etcd/Godeps/_workspace/src/code.google.com/p/gogoprotobuf/proto
github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context
github.com/coreos/etcd/raft
github.com/coreos/etcd/raft/raftpb
github.com/gogo/protobuf/proto
github.com/golang/glog
gopkg.in/yaml.v1
"

set -x

# NOTE: "glock sync" downloads the source but does not install libraries
# into GOPATH/pkg. Install both race and non-race builds here.
go install ${pkgs}
go install -race ${pkgs}
