FROM buildpack-deps:jessie-curl

MAINTAINER Tobias Schottdorf <tobias.schottdorf@gmail.com>

RUN mkdir -p /cockroach
COPY cockroach.sh cockroach /cockroach/
# Set working directory so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /cockroach/

EXPOSE 26257 8080
ENTRYPOINT ["/cockroach/cockroach.sh"]
