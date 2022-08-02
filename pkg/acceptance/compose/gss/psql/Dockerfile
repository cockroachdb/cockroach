# Build the test binary in a multistage build.
FROM golang:1.17 AS builder
WORKDIR /workspace
COPY . .
RUN go test -v -c -tags gss_compose -o gss.test

# Copy the test binary to an image with psql and krb installed.
FROM postgres:11

RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
  ca-certificates \
  curl \
  krb5-user

COPY --from=builder /workspace/gss.test .

RUN curl -fsSL "https://github.com/benesch/autouseradd/releases/download/1.3.0/autouseradd-1.3.0-amd64.tar.gz" -o autouseradd.tar.gz \
  && echo "442dae58b727a79f81368127fac141d7f95501ffa05f8c48943d27c4e807deb7 autouseradd.tar.gz" | sha256sum -c - \
  && tar xzf autouseradd.tar.gz --strip-components 1 \
  && rm autouseradd.tar.gz

ENTRYPOINT ["autouseradd", "--user", "roach", "--no-create-home", "/start.sh"]
