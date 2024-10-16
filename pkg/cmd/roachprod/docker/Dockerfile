ARG BAZEL_IMAGE
FROM $BAZEL_IMAGE AS builder
# ARGs used by the BUILD stage have to be declared after FROM
ARG OWNER
ARG REPO
ARG SHA

RUN git clone https://github.com/$OWNER/$REPO /build
WORKDIR /build
RUN git checkout $SHA
RUN bazel build --config=crosslinux //pkg/cmd/roachprod:roachprod
# Copy the roachprod binary to a stable location
RUN cp $(bazel info bazel-bin --config=crosslinux)/pkg/cmd/roachprod/roachprod_/roachprod ./

FROM golang:1.22
COPY entrypoint.sh build.sh /build/
RUN ["/build/build.sh"]
COPY --from=builder /build/roachprod /usr/local/bin/roachprod
ENTRYPOINT ["/build/entrypoint.sh"]
