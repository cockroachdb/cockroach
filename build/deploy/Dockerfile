FROM registry.access.redhat.com/ubi8/ubi

# For deployment, we need the following installed (they are installed
# by default in RedHat UBI standard):
# glibc - dynamically linked by cockroach binary
# ca-certificates - to authenticate TLS connections for telemetry and
#                   bulk-io with S3/GCS/Azure
# tzdata - for time zone functions
RUN yum update --disablerepo=* --enablerepo=ubi-8-appstream --enablerepo=ubi-8-baseos -y && rm -rf /var/cache/yum

# Install GEOS libraries.
RUN mkdir /usr/local/lib/cockroach
COPY libgeos.so libgeos_c.so /usr/local/lib/cockroach/

RUN mkdir -p /cockroach
COPY cockroach.sh cockroach /cockroach/

# Set working directory so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /cockroach/

# Include the directory in the path to make it easier to invoke
# commands via Docker
ENV PATH=/cockroach:$PATH

ENV COCKROACH_CHANNEL=official-docker

EXPOSE 26257 8080
ENTRYPOINT ["/cockroach/cockroach.sh"]
