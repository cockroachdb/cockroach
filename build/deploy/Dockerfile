# We use a docker image mirror to avoid pulling from 3rd party repos, which sometimes have reliability issues.
# See https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/3462594561/Docker+image+sync for the details.
FROM us-east1-docker.pkg.dev/crl-docker-sync/registry-access-redhat-com/ubi10/ubi-minimal

# For deployment, we need the following additional packages:
# ca-certificates - to validate TLS certs
# hostname - used in cockroach k8s manifests
# tar, gzip, xz - used by kubectl cp
# tzdata - for time zone functions
RUN microdnf update -y && microdnf install -y \
      ca-certificates \
      tzdata \
      hostname \
      tar \
      gzip \
      xz \
    && rm -rf /var/cache/yum

# TARGETARCH is set automatically by `docker buildx build --platform`.
# The build context must place per-arch files under ${TARGETARCH}/ subdirectories
# (e.g. amd64/cockroach, amd64/cockroach.sh, amd64/libgeos.so, etc.).
ARG TARGETARCH
RUN mkdir /usr/local/lib/cockroach /cockroach /licenses /docker-entrypoint-initdb.d
COPY ${TARGETARCH}/cockroach.sh ${TARGETARCH}/cockroach /cockroach/
COPY ${TARGETARCH}/LICENSE ${TARGETARCH}/THIRD-PARTY-NOTICES.txt /licenses/
# Install GEOS libraries.
COPY ${TARGETARCH}/libgeos.so ${TARGETARCH}/libgeos_c.so /usr/local/lib/cockroach/

# Set working directory so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /cockroach/

# Include the directory in the path to make it easier to invoke
# commands via Docker
ENV PATH=/cockroach:$PATH

ENV COCKROACH_CHANNEL=official-docker

EXPOSE 26257 8080
ENTRYPOINT ["/cockroach/cockroach.sh"]
