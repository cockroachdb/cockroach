FROM registry.access.redhat.com/rhel-atomic:latest

LABEL name="cockroachdb/cockroach" \
      vendor="Cockroach Labs" \
      version="2.0.0" \
      release="1" \
      summary="CockroachDB is a cloud-native SQL database for building global, scalable cloud services that survive disasters." \
      description="CockroachDB is a distributed SQL database that uses the PostgreSQL wire protocol, so there are a huge variety of client drivers and ORMs that you can use to talk to it. To get started, see https://www.cockroachlabs.com/docs/stable/build-an-app-with-cockroachdb.html" \
      url="https://www.cockroachlabs.com" \
      io.k8s.description="CockroachDB is a distributed SQL database that uses the PostgreSQL wire protocol, so there are a huge variety of client drivers and ORMs that you can use to talk to it. To get started, see https://www.cockroachlabs.com/docs/stable/build-an-app-with-cockroachdb.html" \
      io.k8s.display-name="CockroachDB" \
      io.openshift.expose-services="26257/tcp,8080/tcp" \
      io.openshift.tags="cockroachdb,sql"

# Atomic Help File
COPY help.1 /help.1

# Licenses
COPY licenses/* /licenses/

# Start script and CockroachDB binary
RUN mkdir -p /cockroach
COPY cockroach.sh cockroach /cockroach/

# Set perms to support non-root & arbitrary uid runtime in OpenShift
RUN chown -R 99:0 /cockroach && \
    chmod -R 775 /cockroach

# Set working directory so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /cockroach/    

# Expose TCP Ports
EXPOSE 26257/tcp 8080/tcp    

# Non-root runtime
USER 99

ENV COCKROACH_CHANNEL=rhel-docker

# Run the script to start up the CockroachDB process
ENTRYPOINT ["/cockroach/cockroach.sh"] 
