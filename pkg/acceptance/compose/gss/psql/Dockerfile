# Build the test binary in a multistage build.
FROM golang:1.23 AS builder
WORKDIR /workspace
COPY . .
RUN go test -v -c -tags gss_compose -o gss.test

# Copy the test binary to an image with psql and krb installed.
FROM postgres:15
ARG TARGETPLATFORM
RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
  ca-certificates \
  curl \
  krb5-user

COPY --from=builder /workspace/gss.test .

RUN \
if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
  curl -fsSL "https://github.com/benesch/autouseradd/releases/download/1.3.0/autouseradd-1.3.0-arm64.tar.gz" -o autouseradd.tar.gz && \
  SHASUM=b216bebfbe30c3c156144cff07233654e23025e26ab5827058c9b284e130599e; \
else \
  curl -fsSL "https://github.com/benesch/autouseradd/releases/download/1.3.0/autouseradd-1.3.0-amd64.tar.gz" -o autouseradd.tar.gz && \
  SHASUM=442dae58b727a79f81368127fac141d7f95501ffa05f8c48943d27c4e807deb7; \
fi; \
echo "$SHASUM autouseradd.tar.gz" | sha256sum -c -; \
tar xzf autouseradd.tar.gz --strip-components 1; \
rm autouseradd.tar.gz;

ENTRYPOINT ["autouseradd", "--user", "roach", "--no-create-home", "/start.sh"]
