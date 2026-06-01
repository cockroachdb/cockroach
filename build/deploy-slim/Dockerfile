# Slim CockroachDB image for CI use cases. Parallel to build/deploy/Dockerfile;
# it is NOT a replacement. The standard image remains the canonical release
# artifact.
#
# Optimized for fast pulls in CI. Drops the interactive/bash entrypoint,
# kubectl-cp helpers (tar/gzip/xz), and the embedded DB Console UI. Uses a
# distroless base so the image contains only glibc + CA certs + tzdata +
# /etc/passwd alongside the cockroach binary and GEOS libraries.
#
# Consumers invoke the binary directly:
#   docker run --rm cockroachdb/cockroach-slim:latest version
#   docker run --rm cockroachdb/cockroach-slim:latest \
#       start-single-node --insecure --store=/tmp/data
#
# There is no cockroach.sh wrapper, so the auto-cert-generation and
# /docker-entrypoint-initdb.d processing from the standard image are not
# available here. CI harnesses must bring their own cert and init flow.
#
# The distroless base is pulled directly from gcr.io rather than a CRL
# mirror. Follow-up work (tracked in the PR body) may mirror it into
# us-east1-docker.pkg.dev for reliability parity with the standard image's
# UBI base.
FROM gcr.io/distroless/base-debian12:latest

# TARGETARCH is set automatically by `docker buildx build --platform`.
# The build context must place per-arch files under ${TARGETARCH}/ subdirectories
# (e.g. amd64/cockroach, amd64/libgeos.so, etc.).
ARG TARGETARCH
COPY ${TARGETARCH}/cockroach /cockroach/cockroach
COPY ${TARGETARCH}/libgeos.so ${TARGETARCH}/libgeos_c.so /usr/local/lib/cockroach/
COPY ${TARGETARCH}/LICENSE ${TARGETARCH}/THIRD-PARTY-NOTICES.txt /licenses/

WORKDIR /cockroach/

ENV PATH=/cockroach:$PATH
ENV COCKROACH_CHANNEL=official-docker-slim

EXPOSE 26257 8080
ENTRYPOINT ["/cockroach/cockroach"]
