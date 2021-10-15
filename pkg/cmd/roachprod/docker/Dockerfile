ARG SHA
ARG BAZEL_IMAGE
FROM ${BAZEL_IMAGE} AS builder

RUN git clone https://github.com/cockroachdb/cockroach /build
WORKDIR /build
RUN git checkout ${SHA}
RUN ./dev build roachprod
# bin/roachprod is a symlink. By copying it, we create a normal file, which can
# be copied to the next build stage.
RUN cp bin/roachprod ./

FROM golang:1.16
COPY entrypoint.sh build.sh /build/
RUN ["/build/build.sh"]
COPY --from=builder /build/roachprod /usr/local/bin/roachprod
ENTRYPOINT ["/build/entrypoint.sh"]
