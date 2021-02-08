FROM registry.access.redhat.com/ubi8/ubi-minimal

# For deployment, we need the following additionally installed:
# tzdata - for time zone functions; reinstalled to replace the missing
#          files in /usr/share/zoneinfo/
# hostname - used in cockroach k8s manifests
# tar - used by kubectl cp
RUN microdnf update -y \
    && rpm --erase --nodeps tzdata \
    && microdnf install tzdata hostname tar -y \
    && rm -rf /var/cache/yum

RUN mkdir /usr/local/lib/cockroach /cockroach
COPY cockroach.sh cockroach /cockroach/
# Install GEOS libraries.
COPY libgeos.so libgeos_c.so /usr/local/lib/cockroach/

# Set working directory so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /cockroach/

# Include the directory in the path to make it easier to invoke
# commands via Docker
ENV PATH=/cockroach:$PATH

ENV COCKROACH_CHANNEL=official-docker

LABEL name="Cockroach DB"
LABEL vendor="Cockroach Labs"
LABEL summary="CockroachDB is a Distributed SQL database."
LABEL description="CockroachDB is a PostgreSQL wire-compatable Distributed SQL database."
# TODO(rail): add k8s/redshift specific labels too
# io.k8s.display-name, io.k8s.description and io.openshift.tags
# Action-oriented labels, such as run or stop.

EXPOSE 26257 8080
# TODO(rail): verify this works as expected
USER 1001:1001
ENTRYPOINT ["/cockroach/cockroach.sh"]
