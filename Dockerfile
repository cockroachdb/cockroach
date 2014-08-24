# The resulting docker image is suitable for testing.
# Do NOT use this docker image in a production setting.

FROM ubuntu:12.04
MAINTAINER Shawn Morel <shawn@strangemonad.com>

# add user and group before anything
RUN groupadd -r cockroach && useradd -r -g cockroach cockroach

# Setup the toolchain
RUN apt-get update -qq
RUN apt-get install -qy python-software-properties
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN apt-get update -qq
RUN apt-get install -y -qq gcc-4.8 g++-4.8 zlib1g-dev libbz2-dev libsnappy-dev libjemalloc-dev libprotobuf-dev protobuf-compiler
RUN apt-get install -y -qq wget build-essential curl git bzr mercurial
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.8 50
RUN update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-4.8 50
RUN wget https://gflags.googlecode.com/files/libgflags0_2.0-1_amd64.deb
RUN dpkg -i libgflags0_2.0-1_amd64.deb
RUN wget https://gflags.googlecode.com/files/libgflags-dev_2.0-1_amd64.deb
RUN dpkg -i libgflags-dev_2.0-1_amd64.deb

RUN curl -L -s http://golang.org/dl/go1.3.1.linux-amd64.tar.gz | tar -v -C /usr/local/ -xz
ENV PATH  /usr/local/go/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin
ENV GOPATH  /go

# Build cockroach (we aren't go-getable)
RUN go get -u code.google.com/p/gogoprotobuf/proto
RUN go get -u code.google.com/p/gogoprotobuf/protoc-gen-gogo
RUN go get -u code.google.com/p/gogoprotobuf/gogoproto
RUN mkdir -p $GOPATH/src
RUN cd $GOPATH/src && git clone --depth=1 https://github.com/cockroachdb/cockroach.git
RUN cd $GOPATH/src/cockroach && git submodule init && git submodule update
RUN cd $GOPATH/src/cockroach && make
