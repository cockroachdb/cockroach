FROM registry.access.redhat.com/rhel-atomic

MAINTAINER Alex Robinson <alex@cockroachlabs.com>

LABEL name="cockroachdb/cockroach" \
      vendor="Cockroach Labs" \
      version="1.0.3" \
      release="1" \
      summary="CockroachDB is ..." \
      description="CockroachDB will ....." \
      url="https://www.cockroachlabs.com" \
      io.k8s.description="CockroachDB will do ....." \
      io.k8s.display-name="CockroachDB" \
      io.openshift.expose-services="26257/tcp,8080/tcp" \
      io.openshift.tags="cockroachdb,sql"

# Atomic Help File
COPY help.1 /help.1

# Licenses
COPY LICENSE APL.txt /licenses/

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

# Run the script to start up the CockroachDB process
ENTRYPOINT ["/cockroach/cockroach.sh"] 
