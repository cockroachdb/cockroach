# The resulting docker image is suitable for testing.
# Do NOT use this docker image in a production setting.

FROM ubuntu:14.04

MAINTAINER Spencer Kimball <spencer.kimball@gmail.com>

# Setup the toolchain.
RUN apt-get update -y -qq
RUN apt-get dist-upgrade -y -qq
RUN apt-get install -y -qq wget curl git mercurial build-essential bzr zlib1g-dev libbz2-dev libsnappy-dev libjemalloc-dev libprotobuf-dev protobuf-compiler g++-4.8 libgflags-dev
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.8 50
RUN update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-4.8 50

# Get Go and set PATH and GO* env variables.
RUN curl -L -s http://golang.org/dl/go1.3.1.linux-amd64.tar.gz | tar -v -C /usr/local/ -xz
ENV PATH /usr/local/go/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:/go/bin
ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV ROACHPATH $GOPATH/src/github.com/cockroachdb
ENV VENDORPATH $ROACHPATH/cockroach/_vendor
ENV ROCKSDBPATH $VENDORPATH
ENV VENDORGOPATH $VENDORPATH/src
ENV COREOSPATH $VENDORGOPATH/github.com/coreos

# Symlink our mounted source directory to appropriate go src location.
RUN mkdir -p $ROACHPATH
RUN mkdir -p $ROCKSDBPATH
RUN mkdir -p $COREOSPATH

# Get Cockroach Go dependencies.
RUN go get code.google.com/p/biogo.store/llrb
RUN go get code.google.com/p/go-commander
RUN go get code.google.com/p/go-uuid/uuid
RUN go get code.google.com/p/gogoprotobuf/proto
RUN go get code.google.com/p/gogoprotobuf/protoc-gen-gogo
RUN go get code.google.com/p/gogoprotobuf/gogoproto
RUN go get github.com/golang/glog
RUN go get gopkg.in/yaml.v1

# Get RocksDB, Etcd sources from github.
# See the NOTE below if hacking directly on the _vendor/
# submodules. In that case, uncomment the "_vendor" exclude from
# .dockerignore and comment out the following lines.
RUN cd $ROCKSDBPATH && git clone --depth=1 https://github.com/facebook/rocksdb.git
RUN cd $ROCKSDBPATH/rocksdb && make static_lib
RUN cd $COREOSPATH && git clone --depth=1 https://github.com/coreos/etcd.git

# Copy the contents of the cockroach source directory to the image.
# Any changes which have been made to the source directory will cause
# the docker image to be rebuilt starting at this cached step.
#
# NOTE: the .dockerignore file excludes the _vendor subdirectory. This
# is done to avoid rebuilding rocksdb in the common case where changes
# are only made to cockroach. If rocksdb is being hacked, remove the
# "_vendor" exclude from .dockerignore.
ADD . $ROACHPATH/cockroach

# Now build the cockroach executable and run the tests.
RUN cd $ROACHPATH/cockroach && make

# Expose the http status port.
EXPOSE 8080

# This is the command to run when this image is launched as a container.
ENTRYPOINT ["/go/src/github.com/cockroachdb/cockroach/cockroach"]

# These are default arguments to the cockroach binary.
CMD ["--help"]
