# We use a docker image mirror to avoid pulling from 3rd party repos, which sometimes have reliability issues.
# See https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/3462594561/Docker+image+sync for the details.
FROM us-east1-docker.pkg.dev/crl-docker-sync/registry-access-redhat-com/ubi9/ubi-minimal
ARG fips_enabled

# For deployment, we need the following additionally installed:
# tzdata - for time zone functions; reinstalled to replace the missing
#          files in /usr/share/zoneinfo/
# hostname - used in cockroach k8s manifests
# tar - used by kubectl cp
RUN microdnf update -y \
    && rpm --erase --nodeps tzdata \
    && microdnf install tzdata hostname tar gzip xz -y \
    && rm -rf /var/cache/yum
# FIPS mode requires the `openssl` package installed. Also we need to temporarily
# install the `crypto-policies-scripts` packege to tweak some configs. Because
# `microdnf` doesn't support `autoremove`, we need to record the list of
# packages before and after, and remove the installed ones afterward.
RUN if [ "$fips_enabled" == "1" ]; then \
    microdnf install -y openssl && \
    rpm -qa | sort > /before.txt && \
    microdnf install -y crypto-policies-scripts && \
    fips-mode-setup --enable --no-bootcfg && \
    rpm -qa | sort > /after.txt && \
    microdnf remove -y $(comm -13 /before.txt /after.txt) && \
    microdnf clean all && \
    rm -rf /var/cache/yum /before.txt /after.txt; \
    fi


RUN mkdir /usr/local/lib/cockroach /cockroach /licenses /docker-entrypoint-initdb.d
COPY cockroach.sh cockroach /cockroach/
COPY LICENSE THIRD-PARTY-NOTICES.txt /licenses/
# Install GEOS libraries.
COPY libgeos.so libgeos_c.so /usr/local/lib/cockroach/

# Set working directory so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /cockroach/

# Include the directory in the path to make it easier to invoke
# commands via Docker
ENV PATH=/cockroach:$PATH

ENV COCKROACH_CHANNEL=official-docker

EXPOSE 26257 8080
ENTRYPOINT ["/cockroach/cockroach.sh"]
