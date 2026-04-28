# Build the binary with Bazel before running docker build:
#   ./dev build --cross=linux pkg/cmd/roachtest/dlq/test_server
#   docker build -t github-dlq-replay pkg/cmd/roachtest/dlq/test_server
FROM us-east1-docker.pkg.dev/crl-docker-sync/docker-hub/library/alpine:latest

RUN apk add --no-cache ca-certificates

COPY test_server /usr/local/bin/github-dlq-replay

ENTRYPOINT ["/usr/local/bin/github-dlq-replay"]
