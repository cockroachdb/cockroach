FROM debian:jessie

MAINTAINER Tobias Schottdorf <tobias.schottdorf@gmail.com>

RUN mkdir -p /cockroach
ADD cockroach /cockroach/
ADD cockroach.sh /cockroach/
ADD test.sh /cockroach/
ADD ./resources /cockroach/resources/

# Set working directory  so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /cockroach/

EXPOSE 8080
ENTRYPOINT ["/cockroach/cockroach.sh"]
