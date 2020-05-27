FROM debian:9.12-slim

# For deployment, we need
# libc6 - dynamically linked by cockroach binary
# ca-certificates - to authenticate TLS connections for telemetry and
#                   bulk-io with S3/GCS/Azure
# tzdata - for time zone functions
RUN apt-get update && \
	apt-get -y upgrade && \
	apt-get install -y libc6 ca-certificates tzdata && \
	rm -rf /var/lib/apt/lists/*

RUN mkdir -p /cockroach
COPY cockroach.sh cockroach /cockroach/
# Set working directory so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /cockroach/

# Include the directory into the path
# to make it easier to invoke commands
# via Docker
ENV PATH=/cockroach:$PATH

ENV COCKROACH_CHANNEL=official-docker

EXPOSE 26257 8080
ENTRYPOINT ["/cockroach/cockroach.sh"]
