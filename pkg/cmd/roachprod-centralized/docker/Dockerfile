ARG BAZEL_IMAGE
ARG BAZEL_CONFIG=crosslinux

# ======================================================= #
# Source preparation & build - REMOTE (clone from GitHub) #
# ======================================================= #
FROM $BAZEL_IMAGE AS builder-remote

ARG OWNER=cockroachdb
ARG REPO=cockroach
ARG SHA=HEAD
ARG BAZEL_CONFIG

WORKDIR /build
RUN git clone https://github.com/$OWNER/$REPO . && \
    git checkout $SHA

# Build the roachprod-centralized binary
RUN bazel build --config=${BAZEL_CONFIG} //pkg/cmd/roachprod-centralized:roachprod-centralized
RUN cp $(bazel info bazel-bin --config=${BAZEL_CONFIG})/pkg/cmd/roachprod-centralized/roachprod-centralized_/roachprod-centralized ./

# Final image for remote builds
FROM us-east1-docker.pkg.dev/crl-docker-sync/docker-io/debian:bookworm-slim AS final-remote
COPY --from=builder-remote /build/pkg/cmd/roachprod-centralized/docker/image/install-deps.sh /build/install-deps.sh
RUN /build/install-deps.sh && rm /build/install-deps.sh
COPY --from=builder-remote /build/pkg/cmd/roachprod-centralized/docker/image/entrypoint.sh /entrypoint.sh
COPY --from=builder-remote /build/roachprod-centralized /usr/local/bin/roachprod-centralized
ENTRYPOINT ["/entrypoint.sh"]

# ============================================================ #
# Source preparation & build - LOCAL (copy from build context) #
# ============================================================ #
FROM $BAZEL_IMAGE AS builder-local

ARG BAZEL_CONFIG

WORKDIR /build
COPY . /build/

# Build the roachprod-centralized binary
RUN bazel build --config=${BAZEL_CONFIG} //pkg/cmd/roachprod-centralized:roachprod-centralized
RUN cp $(bazel info bazel-bin --config=${BAZEL_CONFIG})/pkg/cmd/roachprod-centralized/roachprod-centralized_/roachprod-centralized ./

# Final image for local builds
FROM us-east1-docker.pkg.dev/crl-docker-sync/docker-io/debian:bookworm-slim AS final-local
COPY --from=builder-local /build/pkg/cmd/roachprod-centralized/docker/image/install-deps.sh /build/install-deps.sh
RUN /build/install-deps.sh && rm /build/install-deps.sh
COPY --from=builder-local /build/pkg/cmd/roachprod-centralized/docker/image/entrypoint.sh /entrypoint.sh
COPY --from=builder-local /build/roachprod-centralized /usr/local/bin/roachprod-centralized
ENTRYPOINT ["/entrypoint.sh"]
