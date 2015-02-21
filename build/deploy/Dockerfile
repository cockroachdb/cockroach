FROM debian:jessie

MAINTAINER Tobias Schottdorf <tobias.schottdorf@gmail.com>

RUN mkdir -p /cockroach
COPY cockroach.sh test.sh build/cockroach /cockroach/
COPY build/resources /cockroach/resources
COPY build/ui /cockroach/ui
# Set working directory  so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /cockroach/

EXPOSE 8080
ENTRYPOINT ["/cockroach/cockroach.sh"]
