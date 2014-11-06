#!/bin/bash
set -ex

# Everything is relative to the repo root.
# Resolve symlinks.
cd -P "$(dirname $0)/.."

# Build dependencies into our shadow environment.
VENDOR="$(pwd -P)/_vendor"
USR="${VENDOR}/usr"
LIB="${USR}/lib"
INCLUDE="${USR}/include"
TMP="${USR}/tmp"

ROCKSDB="${VENDOR}/rocksdb"

rm -rf ${USR}
mkdir -p "${USR}" "${TMP}" "${LIB}" "${INCLUDE}" "${ROCKSDB}"

make clean || true
(cd _vendor/rocksdb && make clean) || true


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

rm -rf ${TMP}
