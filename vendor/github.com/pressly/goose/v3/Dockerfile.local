FROM golang:1.17-buster@sha256:3e663ba6af8281b04975b0a34a14d538cdd7d284213f83f05aaf596b80a8c725 as builder

COPY . /src
WORKDIR /src
RUN CGO_ENABLED=0 make dist

FROM scratch AS exporter
COPY --from=builder /src/bin/ /