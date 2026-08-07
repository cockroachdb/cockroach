ARG BAZEL_IMAGE
FROM $BAZEL_IMAGE AS builder

COPY . /build
WORKDIR /build
RUN bazel build --config=crosslinux //pkg/cmd/roachprod:roachprod
# Copy the roachprod binary to a stable location
RUN cp $(bazel info bazel-bin --config=crosslinux)/pkg/cmd/roachprod/roachprod_/roachprod ./

# We use a docker image mirror to avoid pulling from 3rd party repos, which sometimes have reliability issues.
# See https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/3462594561/Docker+image+sync for the details.
FROM us-east1-docker.pkg.dev/crl-docker-sync/docker-io/library/golang:1.25
COPY pkg/cmd/roachprod/docker/build.sh /build/
RUN ["/build/build.sh"]
COPY --from=builder /build/roachprod /usr/local/bin/roachprod
ENTRYPOINT ["/usr/local/bin/roachprod"]
