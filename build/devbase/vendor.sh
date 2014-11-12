#!/bin/bash
set -ex

# Everything is relative to the repo root.
# Resolve symlinks.
cd -P "$(dirname $0)/../.."

# Build dependencies into our shadow environment.
VENDOR="$(pwd -P)/_vendor"
USR="${VENDOR}/usr"
LIB="${USR}/lib"
INCLUDE="${USR}/include"
TMP="${USR}/tmp"

ROCKSDB="${VENDOR}/rocksdb"


TAR="tar --no-same-owner"

rm -rf ${USR}
mkdir -p "${USR}" "${TMP}" "${LIB}" "${INCLUDE}" "${ROCKSDB}"

make clean || true
(cd _vendor/rocksdb && make clean) || true


cd ${TMP}
curl -L https://gflags.googlecode.com/files/gflags-2.0-no-svn-files.tar.gz | ${TAR} -xz
cd gflags-2.0
./configure --prefix ${USR} --disable-shared --enable-static && make && make install

cd ${TMP}
curl -L https://snappy.googlecode.com/files/snappy-1.1.1.tar.gz | ${TAR} -xz
cd snappy-1.1.1
./configure --prefix ${USR} --disable-shared --enable-static && make && make install

cd ${TMP}
curl -L http://zlib.net/zlib-1.2.8.tar.gz | ${TAR} -xz
cd zlib-1.2.8
./configure --static && make test && make && make install prefix="${USR}"

cd ${TMP}
curl -L http://www.bzip.org/1.0.6/bzip2-1.0.6.tar.gz | ${TAR} -xz
cd bzip2-1.0.6
make && make install PREFIX="${VENDOR}/usr"

cd ${TMP}
curl -L https://github.com/google/protobuf/releases/download/2.6.1/protobuf-2.6.1.tar.bz2 | ${TAR} -xj
cd protobuf-2.6.1
./configure --prefix ${USR} --disable-shared --enable-static && make && make install

cd ${ROCKSDB}
LIBRARY_PATH="${LIB}" CPLUS_INCLUDE_PATH="${INCLUDE}" make static_lib

rm -rf ${TMP}
