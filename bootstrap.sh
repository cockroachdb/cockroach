#!/bin/bash

# Bootstrap sets up all needed dependencies.
# Its idempotent so if you don't hack often, your best bet is to just tun this.
# Assumes you are running from the top of the project.
#
# 1) Update all source code and submodules
# 2) Update go dependencies
# 3) Build a shadow toolchain containing our dependencies in _vendor/build

GO_GET="go get"
GO_GET_FLAGS="-u -v"

# TODO(shawn) make rocksdb build less magic
# TODO(shawn) make sure rocksdb still links against jemalloc (and that it makes sense when embedding in go)

set -e -x

# Update the code
git pull && git submodule update --init

# Grab binaries required by git hooks.
$GO_GET $GO_GET_FLAGS github.com/golang/lint/golint
$GO_GET $GO_GET_FLAGS code.google.com/p/go.tools/cmd/vet
$GO_GET $GO_GET_FLAGS code.google.com/p/go.tools/cmd/goimports

# Grab dependencies.
# TODO(bdarnell): make these submodules like etcd/raft, so we can pin versions?
$GO_GET $GO_GET_FLAGS code.google.com/p/biogo.store/llrb
$GO_GET $GO_GET_FLAGS code.google.com/p/go-commander
$GO_GET $GO_GET_FLAGS code.google.com/p/go-uuid/uuid
$GO_GET $GO_GET_FLAGS code.google.com/p/gogoprotobuf/{proto,protoc-gen-gogo,gogoproto}
$GO_GET $GO_GET_FLAGS github.com/golang/glog
$GO_GET $GO_GET_FLAGS gopkg.in/yaml.v1

# Create symlinks to all git hooks in your own .git dir.
for f in $(ls -d githooks/*); do
  rm .git/hooks/$(basename $f)
  ln -s ../../$f .git/hooks/$(basename $f)
done && ls -al .git/hooks | grep githooks


# Build dependencies into our shadow environment.
CURR="${PWD}"
VENDOR="${PWD}/_vendor"
USR="${VENDOR}/usr"
LIB="${USR}/lib"
INCLUDE="${USR}/include"
TMP="${USR}/tmp"

ROCKSDB="${VENDOR}/rocksdb"

make clean
rm -rf ${USR}
cd _vendor/rocksdb && make clean

mkdir -p ${TMP}
mkdir -p ${LIB}
mkdir -p ${INCLUDE}

cd ${TMP}
curl -L https://gflags.googlecode.com/files/gflags-2.0-no-svn-files.tar.gz > gflags-2.0-no-svn-files.tar.gz
tar -xzvf gflags-2.0-no-svn-files.tar.gz
cd gflags-2.0
./configure --prefix ${USR} && make && make install

cd ${TMP}
curl -L https://snappy.googlecode.com/files/snappy-1.1.1.tar.gz > snappy-1.1.1.tar.gz
tar -xzvf snappy-1.1.1.tar.gz
cd snappy-1.1.1
./configure --prefix ${USR} && make && make install

cd ${ROCKSDB}
make static_lib
cp librocksdb.a ${LIB}/
cp -R ./include/rocksdb ${INCLUDE}
