# The resulting docker image is suitable for testing.
# Do NOT use this docker image in a production setting.

FROM ubuntu

FROM ubuntu:14.04
MAINTAINER Shawn Morel <shawn@strangemonad.com>

# add user and group before anything
RUN groupadd -r cockroach && useradd -r -g cockroach cockroach

# Setup the toolchain
RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y curl git bzr mercurial
RUN curl -L -s http://golang.org/dl/go1.3.1.linux-amd64.tar.gz | tar -v -C /usr/local/ -xz

RUN curl -s https://protobuf.googlecode.com/files/protobuf-2.5.0.tar.gz | tar -v -C . -xz
RUN cd protobuf-2.5.0 && ./configure && make && sudo make install && cd ../ && rm -rf protobuf*

ENV PATH  /usr/local/go/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin
ENV GOPATH  /go
ENV GOROOT  /usr/local/go

RUN mkdir -p $GOPATH/src

# Build cockroach (we aren't go-getable)
RUN apt-get install -y libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev
RUN go get -u code.google.com/p/gogoprotobuf/proto
RUN go get -u code.google.com/p/gogoprotobuf/protoc-gen-gogo
RUN go get -u code.google.com/p/gogoprotobuf/gogoproto

RUN cd $GOPATH/src && git clone --depth=1 https://github.com/cockroachdb/cockroach.git
RUN cd $GOPATH/src/cockroach && git submodule update --depth=1 --init
RUN cd $GOPATH/src/cockroach && make
