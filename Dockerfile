FROM golang:latest

MAINTAINER Tobias Schottdorf <tobias.schottdorf@gmail.com>

# Setup the toolchain.
RUN apt-get update -y && apt-get dist-upgrade -y && \
 apt-get install --no-install-recommends --auto-remove -y \
 git build-essential pkg-config file \
# for gogoproto
 libprotobuf-dev \
 protobuf-compiler \
# for RocksDB
 libbz2-dev && \
 apt-get clean autoclean && apt-get autoremove -y && \
 rm -rf /tmp/* /var/lib/{apt,dpkg,cache,log}

ENV GOPATH /go
ENV ROACHPATH $GOPATH/src/github.com/cockroachdb
ENV VENDORPATH $ROACHPATH/cockroach/_vendor
ENV ROCKSDBPATH $VENDORPATH
ENV VENDORGOPATH $VENDORPATH/src
ENV COREOSPATH $VENDORGOPATH/github.com/coreos

# Make our vendored libraries visible everywhere.
ENV LIBRARY_PATH $VENDORPATH/usr/lib:$LD_LIBRARY_PATH
ENV LD_LIBRARY_PATH $VENDORPATH/usr/lib:$LD_LIBRARY_PATH
ENV LD_RUN_PATH $VENDORPATH:$LD_RUN_PATH
ENV CFLAGS -I$VENDORPATH/usr/include
RUN echo $VENDORPATH/usr/lib >> /etc/ld.so.conf.d/cockroach.conf

RUN mkdir -p $ROACHPATH && \
 mkdir -p $ROCKSDBPATH && \
 mkdir -p $COREOSPATH && \
 ln -s "${ROACHPATH}/cockroach" /cockroach 

# Get RocksDB, Etcd sources from github and build the vendored libs.
# We will run 'git submodule update' later which will ensure we have the correct
# version, but running an initial download here speeds things up by baking
# the bulk of the download into a lower layer of the image.
#
# See the NOTE below if hacking directly on the _vendor/
# submodules. In that case, uncomment the "_vendor" exclude from
# .dockerignore and comment out the following lines cloning RocksDB.
# Build rocksdb before adding the current directory. If there are
# changes made by 'git submodule update' it will get rebuilt later but
# this lets us reuse most of the results of an earlier build of the
# image.
RUN cd $ROCKSDBPATH && git clone https://github.com/cockroachdb/rocksdb.git && \
 cd $COREOSPATH && git clone https://github.com/cockroachdb/etcd.git
ADD ./setup/ /cockroach/setup
RUN ["/cockroach/setup/godeps.sh"]
RUN ["/cockroach/setup/vendor.sh"]

# Copy the contents of the cockroach source directory to the image.
# Any changes which have been made to the source directory will cause
# the docker image to be rebuilt starting at this cached step.
#
# NOTE: the .dockerignore file excludes the _vendor subdirectory. This
# is done to avoid rebuilding rocksdb in the common case where changes
# are only made to cockroach. If rocksdb is being hacked, remove the
# "_vendor" exclude from .dockerignore.
ADD . /cockroach/

# Update to the correct version of our submodules and rebuild any changes
# in RocksDB (in case the submodule revision is different from the current
# master)
# Build the cockroach executable.
RUN cd -P /cockroach && git submodule update --init && \
 cd -P /cockroach/_vendor/rocksdb && make static_lib
RUN cd -P /cockroach && make GOFLAGS=-x build

# Expose the http status port.
EXPOSE 8080

# This is the command to run when this image is launched as a container.
ENTRYPOINT ["/cockroach/deploy/wrapper.sh"]

# These are default arguments to the cockroach binary.
CMD ["--help"]
