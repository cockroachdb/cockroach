FROM golang:latest

MAINTAINER Spencer Kimball <spencer.kimball@gmail.com>

# Setup the toolchain. Make a lame attempt to reduce image size
# by cleaning up right away.
# TODO(pmattis): Use the vendored snappy and gflags.
RUN apt-get update -y && \
 apt-get dist-upgrade -y && \
 apt-get install --auto-remove -y git mercurial build-essential pkg-config bzr zlib1g-dev libbz2-dev libsnappy-dev libgflags-dev libprotobuf-dev protobuf-compiler && \
 apt-get clean autoclean && apt-get autoremove -y && rm -rf /var/lib/{apt,dpkg,cache,log}


ENV GOPATH /go
ENV ROACHPATH $GOPATH/src/github.com/cockroachdb
ENV VENDORPATH $ROACHPATH/cockroach/_vendor
ENV ROCKSDBPATH $VENDORPATH
ENV VENDORGOPATH $VENDORPATH/src
ENV COREOSPATH $VENDORGOPATH/github.com/coreos

RUN mkdir -p $ROACHPATH && \
 mkdir -p $ROCKSDBPATH && \
 mkdir -p $COREOSPATH

# TODO(pmattis): Switch to using bootstrap.sh for retrieving go
# dependencies and building rocksdb. The current road block is that
# doing so causes rocksdb/snappy/etc to be rebuilt any time there is a
# change to the cockroach directory.

# Get Cockroach Go dependencies.
RUN go get code.google.com/p/biogo.store/llrb && \
 go get code.google.com/p/go-commander && \
 go get code.google.com/p/go-uuid/uuid && \
 go get code.google.com/p/gogoprotobuf/proto && \
 go get code.google.com/p/gogoprotobuf/protoc-gen-gogo && \
 go get code.google.com/p/gogoprotobuf/gogoproto && \
 go get github.com/golang/glog && \
 go get gopkg.in/yaml.v1

# Get RocksDB, Etcd sources from github.
# We will run 'git submodule update' below which will ensure we have the correct
# version, but running an initial download here speeds things up by baking
# the bulk of the download into a lower layer of the image.
# See the NOTE below if hacking directly on the _vendor/
# submodules. In that case, uncomment the "_vendor" exclude from
# .dockerignore and comment out the following lines.
# Build rocksdb before adding the current directory. If there are
# changes made by 'git submodule update' it will get rebuilt below but
# this lets us reuse most of the results of an earlier build of the
# image.
RUN cd $ROCKSDBPATH && git clone https://github.com/cockroachdb/rocksdb.git && \
 cd $COREOSPATH && git clone https://github.com/cockroachdb/etcd.git && \
 cd $ROCKSDBPATH/rocksdb && make static_lib

# Copy the contents of the cockroach source directory to the image.
# Any changes which have been made to the source directory will cause
# the docker image to be rebuilt starting at this cached step.
#
# NOTE: the .dockerignore file excludes the _vendor subdirectory. This
# is done to avoid rebuilding rocksdb in the common case where changes
# are only made to cockroach. If rocksdb is being hacked, remove the
# "_vendor" exclude from .dockerignore.
ADD . $ROACHPATH/cockroach

# Update to the correct version of our submodules and rebuild any changes
# in rocksdb (in case the submodule revision is different from the current
# master)
# Build the cockroach executable and run the tests.
RUN cd $ROACHPATH/cockroach && git submodule update --init && \
 cd $ROCKSDBPATH/rocksdb && make static_lib && \
 cd $ROACHPATH/cockroach && make

# Expose the http status port.
EXPOSE 8080

# This is the command to run when this image is launched as a container.
ENTRYPOINT ["/go/src/github.com/cockroachdb/cockroach/deploy/wrapper.sh"]

# These are default arguments to the cockroach binary.
CMD ["--help"]
