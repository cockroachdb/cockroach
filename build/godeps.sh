#!/bin/bash
# Grab Go-related dependencies.
# TODO(bdarnell): make these submodules like etcd/raft, so we can pin versions?

function go_get() {
  go get -u -v "$@"
}

go_get code.google.com/p/biogo.store/llrb
go_get code.google.com/p/go-commander
go_get code.google.com/p/go-uuid/uuid
go_get code.google.com/p/gogoprotobuf/{proto,protoc-gen-gogo,gogoproto}
go_get github.com/golang/glog
go_get gopkg.in/yaml.v1
