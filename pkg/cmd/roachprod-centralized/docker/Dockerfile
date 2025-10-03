ARG BAZEL_IMAGE
FROM $BAZEL_IMAGE AS builder

# ARGs used by the BUILD stage have to be declared after FROM
ARG OWNER
ARG REPO
ARG SHA

RUN git clone https://github.com/$OWNER/$REPO /build
WORKDIR /build
RUN git checkout $SHA

# Build the roachprod-centralized binary
RUN bazel build --config=crosslinux //pkg/cmd/roachprod-centralized:roachprod-centralized
RUN cp $(bazel info bazel-bin --config=crosslinux)/pkg/cmd/roachprod-centralized/roachprod-centralized_/roachprod-centralized ./


# Build the final image
FROM us-east1-docker.pkg.dev/crl-docker-sync/docker-io/debian:bookworm-slim

# Copy build script
COPY --from=builder /build/pkg/cmd/roachprod-centralized/docker/build.sh /build/build.sh

# Run the build script to install final image dependencies then remove it
RUN /build/build.sh
RUN rm /build/build.sh

# Copy the roachprod-centralized binary and the entrypoint script
COPY --from=builder /build/pkg/cmd/roachprod-centralized/docker/entrypoint.sh /entrypoint.sh
COPY --from=builder /build/roachprod-centralized /usr/local/bin/roachprod-centralized

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
