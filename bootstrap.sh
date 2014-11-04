#!/bin/bash

# Bootstrap sets up all needed dependencies.
# Its idempotent so if you don't hack often, your best bet is to just tun this.
# Assumes you are running from the top of the project.
#
# 1) Update all source code and submodules
# 2) Update go dependencies
# 3) Build a shadow toolchain containing our dependencies in _vendor/build

# TODO(shawn) make rocksdb build less magic
# TODO(shawn) make sure rocksdb still links against jemalloc (and that it makes sense when embedding in go)
# TODO(pmattis): check for pkg-config and curl.

set -e -x

function go_get() {
  go get -u -v "$@"
}

# Update submodules
git submodule update --init

# Grab binaries required by git hooks.
go_get github.com/golang/lint/golint
go_get code.google.com/p/go.tools/cmd/goimports
# go vet is special: it installs into $GOROOT (which $USER may not have
# write access to) instead of $GOPATH. It is usually but not always
# installed along with the rest of the go toolchain. Don't try to
# install it if it's already there.
go vet 2>/dev/null || go_get code.google.com/p/go.tools/cmd/vet

# Grab dependencies.
# TODO(bdarnell): make these submodules like etcd/raft, so we can pin versions?
go_get code.google.com/p/biogo.store/llrb
go_get code.google.com/p/go-commander
go_get code.google.com/p/go-uuid/uuid
go_get code.google.com/p/gogoprotobuf/{proto,protoc-gen-gogo,gogoproto}
go_get github.com/golang/glog
go_get gopkg.in/yaml.v1

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

echo "Bootstrapped successfully!"
