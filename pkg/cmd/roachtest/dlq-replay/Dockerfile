ARG BAZEL_IMAGE
FROM $BAZEL_IMAGE AS builder

ARG OWNER=cockroachdb
ARG REPO=cockroach
ARG SHA=HEAD

RUN git clone https://github.com/$OWNER/$REPO /build
WORKDIR /build
RUN git checkout $SHA
RUN bazel build --config=crosslinux //pkg/cmd/roachtest/dlq-replay:dlq-replay
RUN cp $(bazel info bazel-bin --config=crosslinux)/pkg/cmd/roachtest/dlq-replay/dlq-replay_/dlq-replay /dlq-replay

FROM us-east1-docker.pkg.dev/crl-docker-sync/docker-io/debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /dlq-replay /usr/local/bin/dlq-replay
ENTRYPOINT ["/usr/local/bin/dlq-replay"]
