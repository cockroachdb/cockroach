FROM alpine:3.3

RUN apk --update add ca-certificates

ADD . /go/src/github.com/2tvenom/go-test-teamcity

ENV GOPATH=/go
RUN apk add go && \
    go install github.com/2tvenom/go-test-teamcity && \
    apk del go && \
    cp /go/bin/go-test-teamcity /converter && \
    rm -rf /go /usr/local/go

ENTRYPOINT ["/converter"]
