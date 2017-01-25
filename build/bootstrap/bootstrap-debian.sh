#!/usr/bin/env bash
#
# On a (recent enough) Debian/Ubuntu system, bootstraps a source Go install
# (with improved parallel build patches) and the cockroach repo.

set -euxo pipefail

cd "$(dirname "${0}")"

sudo apt-get update -q && sudo apt-get install -q -y --no-install-recommends build-essential git gdb patch bzip2 docker.io clang cmake subversion

sudo adduser "${USER}" docker


# Install an msan-enabled build of libc++, following instructions from
# https://github.com/google/sanitizers/wiki/MemorySanitizerLibcxxHowTo

# Build libcxx from head, because rocksdb 4.11 doesn't build with libcxx 3.9 on linux.
# TODO(bdarnell): after we upgrade to rocksdb 5.0, replace these svn commands with the
# commented-out tarball installation below.

svn co -q http://llvm.org/svn/llvm-project/llvm/trunk ~/llvm
(cd ~/llvm/projects && svn co -q http://llvm.org/svn/llvm-project/libcxx/trunk libcxx)
(cd ~/llvm/projects && svn co -q http://llvm.org/svn/llvm-project/libcxxabi/trunk libcxxabi)

#mkdir ~/llvm && curl -sfSL http://releases.llvm.org/3.9.1/llvm-3.9.1.src.tar.xz | tar --strip-components=1 -C ~/llvm -xJ
#mkdir ~/llvm/projects/libcxx && curl -sfSL http://releases.llvm.org/3.9.1/libcxx-3.9.1.src.tar.xz | tar --strip-components=1 -C ~/llvm/projects/libcxx -xJ
#mkdir ~/llvm/projects/libcxxabi && curl -sfSL http://releases.llvm.org/3.9.1/libcxxabi-3.9.1.src.tar.xz | tar --strip-components=1 -C ~/llvm/projects/libcxxabi -xJ



mkdir ~/libcxx_msan
(cd ~/libcxx_msan &&
cmake ../llvm -DCMAKE_BUILD_TYPE=Release -DLLVM_USE_SANITIZER=Memory -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ &&
make cxx -j16)


# Install Go from source so we can add a patch to run cgo builds in parallel.
mkdir -p ~/go-bootstrap ~/goroot ~/go
curl "https://storage.googleapis.com/golang/go${GOVERSION}.linux-amd64.tar.gz" | tar -C ~/go-bootstrap -xz --strip=1
curl "https://storage.googleapis.com/golang/go${GOVERSION}.src.tar.gz" | tar --strip-components=1 -C ~/goroot -xz

# Apply the patch for the "major" go version (e.g. 1.6, 1.7).
GOPATCHVER=$(echo ${GOVERSION} | grep -o "^[0-9]\+\.[0-9]\+")
patch -p1 -d ../goroot < "parallelbuilds-go${GOPATCHVER}.patch"

(cd ~/goroot/src && GOROOT_BOOTSTRAP=~/go-bootstrap ./make.bash)


# Configure environment variables
echo 'export GOPATH=${HOME}/go; export PATH="${HOME}/goroot/bin:${HOME}/go/bin:${PATH}"' >> ~/.bashrc_go
echo '. ~/.bashrc_go' >> ~/.bashrc

. ~/.bashrc_go

# Allow Go support files in gdb.
echo "add-auto-load-safe-path ${HOME}/goroot/src/runtime/runtime-gdb.py" >> ~/.gdbinit

go get -d github.com/cockroachdb/cockroach
