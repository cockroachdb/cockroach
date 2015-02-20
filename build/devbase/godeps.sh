#!/bin/bash
# Grab Go-related dependencies.
# TODO(bdarnell): make these submodules like etcd/raft, so we can pin versions?

# We normally use a GOPATH with two elements: the directory containing
# cockroach and our _vendor directory. The _vendor directory appears
# first in the GOPATH when it is used (it is typically not present
# when bootstrap.sh is being run), but it is treated specially during
# the docker build so we want to install all our other dependencies
# into the outer GOPATH whether the _vendor directory is present or not.
#
# The cd/pwd dance converts the possibly-relative $(dirname $0) into
# an absolute path.
export GOPATH=$(cd $(dirname $0)/../../../../../.. && pwd)

function go_get() {
  go get -u -v "$@"
}

go_get code.google.com/p/biogo.store/llrb
go_get code.google.com/p/go-commander
go_get code.google.com/p/go-uuid/uuid
go_get github.com/gogo/protobuf/{proto,protoc-gen-gogo,gogoproto}
go_get code.google.com/p/snappy-go/snappy
go_get github.com/golang/glog
go_get gopkg.in/yaml.v1
