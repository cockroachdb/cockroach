FROM golang:1.4.2

MAINTAINER Tobias Schottdorf <tobias.schottdorf@gmail.com>

# Setup the toolchain.
RUN apt-get update -y && \
	apt-get dist-upgrade -y && \
	apt-get install --no-install-recommends --auto-remove -y git build-essential pkg-config file npm && \
 	apt-get clean autoclean && \
 	apt-get autoremove -y && \
 	rm -rf /tmp/* /var/lib/{apt,dpkg,cache,log} && \
	ln -s /usr/bin/nodejs /usr/bin/node

ENV GOPATH /go
ENV ROACHPATH $GOPATH/src/github.com/cockroachdb

# Create required directories and a convenience symlink.
RUN mkdir -p $ROACHPATH && \
	ln -s "${ROACHPATH}/cockroach" /cockroach

WORKDIR ${ROACHPATH}/cockroach

ADD . ${ROACHPATH}/cockroach/build/devbase/
RUN ["./build/devbase/deps-docker.sh"]

CMD ["/bin/bash"]
