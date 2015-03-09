FROM debian:jessie

MAINTAINER Tobias Schottdorf <tobias.schottdorf@gmail.com>

RUN mkdir -p /cockroach
# TODO remove this next copy once the tests do not need the test cert any more.
COPY build/resource /cockroach/resource
COPY cockroach.sh test.sh build/cockroach /cockroach/
# Set working directory  so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /cockroach/

EXPOSE 8080
ENTRYPOINT ["/cockroach/cockroach.sh"]
