FROM debian:8.9

# Install root CAs so we can make SSL connections to phone home and
# do backups to GCE/AWS/Azure.
RUN apt-get update && \
	apt-get -y upgrade && \
	apt-get install -y ca-certificates  && \
	rm -rf /var/lib/apt/lists/*

RUN mkdir -p /cockroach
COPY cockroach.sh cockroach /cockroach/
# Set working directory so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /cockroach/

ENV COCKROACH_CHANNEL=official-docker

EXPOSE 26257 8080
ENTRYPOINT ["/cockroach/cockroach.sh"]
