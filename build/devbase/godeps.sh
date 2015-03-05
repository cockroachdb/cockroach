#!/bin/bash
# Grab Go-related dependencies.
# TODO(bdarnell): make these submodules, so we can pin versions?

# NOTE: Use "make godeps" to update this list. We can't just use "go
# list" here because this script is run during docker container builds
# before the cockroach code is present.
pkgs="code.google.com/p/biogo.store/interval
      code.google.com/p/biogo.store/llrb
      code.google.com/p/go-commander
      code.google.com/p/go-uuid/uuid
      code.google.com/p/snappy-go/snappy
      github.com/cockroachdb/c-protobuf
      github.com/cockroachdb/c-rocksdb
      github.com/coreos/etcd
      github.com/gogo/protobuf/proto
      github.com/golang/glog
      gopkg.in/yaml.v1"

set -ex

# NOTE: "go get" does a "go install" for the listed packages. We also
# explicitly "go install -race" in order to cache race builds of these
# packages.
go get -u ${pkgs}
go install -race ${pkgs}
