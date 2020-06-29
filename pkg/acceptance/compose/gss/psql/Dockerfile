# Build the test binary in a multistage build.
FROM golang:1.14 AS builder
WORKDIR /workspace
COPY . .
RUN go get -d -t -tags gss_compose
RUN go test -v -c -tags gss_compose -o gss.test

# Copy the test binary to an image with psql and krb installed.
FROM postgres:11

RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
  krb5-user

COPY --from=builder /workspace/gss.test .

ENTRYPOINT ["/start.sh"]
