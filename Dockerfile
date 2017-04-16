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
RUN curl -L -s http://golang.org/dl/go1.3.linux-amd64.tar.gz | tar -v -C /usr/local/ -xz

ENV PATH  /usr/local/go/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin
ENV GOPATH  /go
ENV GOROOT  /usr/local/go

RUN mkdir -p $GOPATH/src

# Build cockroach (we aren't go-getable)
RUN apt-get install -y libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev
RUN cd $GOPATH/src && git clone --depth=1 https://github.com/cockroachdb/cockroach.git
RUN cd $GOPATH/src/cockroach && git submodule update --depth=1 --init
RUN cd $GOPATH/src/cockroach && make
