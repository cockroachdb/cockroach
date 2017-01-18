FROM alpine:3.5

MAINTAINER Tamir Duberstein <tamird@gmail.com>

# curl, gnupg are used by the yarn installer. TODO(tamird): consider removing
# when https://github.com/alpinelinux/aports/pull/714 is merged.
#
# gcc/g++ are needed for libgcc.
# bash, go are used to bootstrap go.
# nodejs is used to build and test the UI.
# bzip2 and fontconfig are used by phantomjs-prebuilt to test the UI.
# iptables is used in the acceptance tests' partition nemesis.
# linux-headers is needed for rocksdb.
# rsync is needed for updating c-jemalloc.
# yarn is the dependency manager for the UI, as an alternative to npm.
RUN apk add --no-cache \
    bash \
    bzip2 \
    curl \
    fontconfig \
    g++ \
    gcc \
    git \
    gnupg \
    go \
    iptables \
    linux-headers \
    make \
    nodejs \
    rsync \
    unzip

RUN curl -fsSL https://yarnpkg.com/install.sh | bash -s

# START https://raw.githubusercontent.com/docker-library/golang/master/1.7/alpine/Dockerfile

ENV GOLANG_VERSION 1.7.4
ENV GOLANG_SRC_URL https://golang.org/dl/go$GOLANG_VERSION.src.tar.gz
ENV GOLANG_SRC_SHA256 4c189111e9ba651a2bb3ee868aa881fab36b2f2da3409e80885ca758a6b614cc

ADD parallelbuilds.patch /
ADD https://raw.githubusercontent.com/docker-library/golang/master/1.7/alpine/no-pic.patch /
ADD https://raw.githubusercontent.com/docker-library/golang/master/1.7/alpine/17847.patch /

RUN curl -fsSL "$GOLANG_SRC_URL" -o golang.tar.gz \
 && echo "$GOLANG_SRC_SHA256  golang.tar.gz" | sha256sum -c - \
 && tar -C /usr/local -xzf golang.tar.gz \
 && rm golang.tar.gz \
 && cd /usr/local/go/src \
 && patch -p2 -i /parallelbuilds.patch \
 && patch -p2 -i /no-pic.patch \
 && patch -p2 -i /17847.patch \
 && rm /*.patch \
 && GOROOT_BOOTSTRAP=$(go env GOROOT) ./make.bash

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
WORKDIR $GOPATH

# END https://raw.githubusercontent.com/docker-library/golang/master/1.7/alpine/Dockerfile

RUN apk del \
    go

RUN curl -fsSL https://releases.hashicorp.com/terraform/0.8.4/terraform_0.8.4_linux_amd64.zip -o terraform_linux_amd64.zip && \
    unzip terraform_linux_amd64.zip -d /usr/local/bin && rm terraform_linux_amd64.zip

ENV PATH /opt/backtrace/bin:$HOME/.yarn/bin:$PATH

RUN chmod -R a+w $(go env GOTOOLDIR)

CMD ["/bin/bash"]
