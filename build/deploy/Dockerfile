FROM registry.access.redhat.com/ubi8/ubi-minimal

# For deployment, we need the following additionally installed:
# tzdata - for time zone functions; reinstalled to replace the missing
#          files in /usr/share/zoneinfo/
# hostname - used in cockroach k8s manifests
# tar - used by kubectl cp
RUN microdnf update -y \
    && rpm --erase --nodeps tzdata \
    && microdnf install tzdata hostname tar gzip xz -y \
    && rm -rf /var/cache/yum

RUN mkdir /usr/local/lib/cockroach /cockroach /licenses /docker-entrypoint-initdb.d
COPY cockroach.sh cockroach /cockroach/
COPY licenses/* /licenses/
# Install GEOS libraries.
COPY libgeos.so libgeos_c.so /usr/local/lib/cockroach/

# Copy init scripts in docker-entrypoint-initdb.d.
# There must be at least one file in the local docker-entrypoint-initdb.d.

# Set working directory so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /cockroach/

# Set up the certs dir.
RUN mkdir certs
RUN ./cockroach cert create-ca --certs-dir=certs --ca-key=certs/ca.key
RUN ./cockroach cert create-client --certs-dir=certs --ca-key=certs/ca.key root

# Include the directory in the path to make it easier to invoke
# commands via Docker
ENV PATH=/cockroach:$PATH

ENV COCKROACH_CHANNEL=official-docker

EXPOSE 26257 8080
ENTRYPOINT ["/cockroach/cockroach.sh"]
