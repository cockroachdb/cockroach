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

set -ex

# NOTE: Use "make godeps" to update this list. We can't just use "go
# list" here because this script is run during docker container builds
# before the cockroach code is present.
go get -u \
   code.google.com/p/biogo.store/interval \
   code.google.com/p/biogo.store/llrb  \
   code.google.com/p/go-commander \
   code.google.com/p/go-uuid/uuid \
   code.google.com/p/snappy-go/snappy \
   github.com/cockroachdb/c-protobuf \
   github.com/cockroachdb/c-rocksdb \
   github.com/gogo/protobuf/proto \
   github.com/golang/glog \
   gopkg.in/yaml.v1
