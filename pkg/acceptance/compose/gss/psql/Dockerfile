# Build the test binary in a multistage build.
FROM golang:1.16 AS builder
WORKDIR /workspace
COPY . .
# go 1.16 requires go.mod to be present unless GO111MODULE is set to off
RUN GO111MODULE=off go get -d -t -tags gss_compose
RUN GO111MODULE=off go test -v -c -tags gss_compose -o gss.test

# Copy the test binary to an image with psql and krb installed.
FROM postgres:11

RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
  krb5-user

COPY --from=builder /workspace/gss.test .

ENTRYPOINT ["/start.sh"]
