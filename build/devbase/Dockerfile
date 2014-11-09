FROM golang:latest

MAINTAINER Tobias Schottdorf <tobias.schottdorf@gmail.com>

# Setup the toolchain.
RUN apt-get update -y && apt-get dist-upgrade -y && \
 apt-get install --no-install-recommends --auto-remove -y git build-essential pkg-config file &&\
 apt-get clean autoclean && apt-get autoremove -y && rm -rf /tmp/* /var/lib/{apt,dpkg,cache,log}

ENV GOPATH /go
ENV ROACHPATH $GOPATH/src/github.com/cockroachdb
ENV VENDORPATH $ROACHPATH/cockroach/_vendor
ENV ROCKSDBPATH $VENDORPATH
ENV VENDORGOPATH $VENDORPATH/src
ENV COREOSPATH $VENDORGOPATH/github.com/coreos
ENV VENDOR_LIB $VENDORPATH/usr/lib
ENV VENDOR_INCLUDE $VENDORPATH/usr/include

# Make our vendored libraries visible everywhere.
ENV LIBRARY_PATH $VENDOR_LIB:$LIBRARY_PATH
ENV CPLUS_INCLUDE_PATH $VENDOR_INCLUDE:$CPLUS_INCLUDE_PATH
ENV PATH $VENDORPATH/usr/bin:$PATH

# Create required directories and a convenience symlink.
RUN mkdir -p $ROACHPATH && \
 mkdir -p $ROCKSDBPATH && \
 mkdir -p $COREOSPATH && \
 ln -s "${ROACHPATH}/cockroach" /cockroach 

WORKDIR /cockroach

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

ADD . /cockroach/build/devbase/
RUN ["./build/devbase/godeps.sh"]
RUN ["./build/devbase/vendor.sh"]

CMD ["/bin/bash"]
