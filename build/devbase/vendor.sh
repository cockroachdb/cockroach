#!/bin/bash
set -ex

# Everything is relative to the repo root.
# Resolve symlinks.
cd -P "$(dirname $0)/../.."

# Build dependencies into our shadow environment.
VENDOR="$(pwd -P)/_vendor"
GO_MD5SUM="$(pwd -P)/build/devbase/md5sum.go"
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

# Download and verify a file.
# ${1} is the URL
# ${2} is the expected md5sum
function download_verify()
{
    curl -LOs ${1}
    filename=`echo ${1} | sed 's/.*\///'`
    if [ -f ${filename} ]; then
        dl_md5sum=`go run ${GO_MD5SUM} < ${filename}`
        if [ ${dl_md5sum} != ${2} ]; then
            echo "$filename md5sum did not match."
            exit 1
        fi
    else
        echo "${1} download failed"
        exit 1
    fi
}

cd ${TMP}
curl -L https://gflags.googlecode.com/files/gflags-2.0-no-svn-files.tar.gz | ${TAR} -xz
cd gflags-2.0
./configure --prefix ${USR} --disable-shared --enable-static && make && make install

cd ${TMP}
curl -L https://snappy.googlecode.com/files/snappy-1.1.1.tar.gz | ${TAR} -xz
cd snappy-1.1.1
./configure --prefix ${USR} --disable-shared --enable-static && make && make install

cd ${TMP}
download_verify \
    http://zlib.net/zlib-1.2.8.tar.gz \
    44d667c142d7cda120332623eab69f40
${TAR} -xzf zlib-1.2.8.tar.gz
cd zlib-1.2.8
./configure --static && make test && make && make install prefix="${USR}"

cd ${TMP}
download_verify \
    http://www.bzip.org/1.0.6/bzip2-1.0.6.tar.gz \
    00b516f4704d4a7cb50a1d97e6e8e15b
${TAR} -xzf bzip2-1.0.6.tar.gz
cd bzip2-1.0.6
make && make install PREFIX="${VENDOR}/usr"

cd ${TMP}
curl -L https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.bz2 | ${TAR} -xj
cd protobuf-2.6.1
./configure --prefix ${USR} --disable-shared --enable-static && make && make install

cd ${ROCKSDB}
LIBRARY_PATH="${LIB}" CPLUS_INCLUDE_PATH="${INCLUDE}" make static_lib

rm -rf ${TMP}
