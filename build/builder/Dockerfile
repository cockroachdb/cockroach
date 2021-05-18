FROM ubuntu:focal-20210119
# If you update the base image above, make sure to also update
# build/toolchains/toolchainbuild/buildtoolchains.sh accordingly.

# This is the CockroachDB "builder" image, which bundles cross-compiling
# toolchains that can build CockroachDB on all supported platforms.

# autoconf - c-deps: jemalloc
# automake - sed build
# autopoint - sed build
# bison - CRDB build system
# clang-10 - compiler
# cmake - msan / c-deps: libroach, protobuf, et al.
# gettext - sed build
# gnupg2 - for apt
# libncurses-dev - CRDB build system
# make - CRDB build system
# python - awscli install
# rsync - sed build
# texinfo - sed build
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    apt-transport-https \
    autoconf \
    automake \
    autopoint \
    bison \
    ca-certificates \
    clang-10 \
    cmake \
    curl \
    gettext \
    git \
    gnupg2 \
    libncurses-dev \
    make \
    patch \
    patchelf \
    python \
    rsync \
    texinfo \
 && apt-get clean \
 && update-alternatives --install /usr/bin/clang clang /usr/bin/clang-10 100 \
    --slave /usr/bin/clang++ clang++ /usr/bin/clang++-10

RUN curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/20210519-210227/aarch64-unknown-linux-gnu.tar.gz -o aarch64-unknown-linux-gnu.tar.gz \
 && echo '44aac8c8c00366deeff0743a27890fa22302123aec816721d04d1b81459dd9e6 aarch64-unknown-linux-gnu.tar.gz' | sha256sum -c - \
 && curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/20210519-210227/libcxx_msan.tar.gz -o libcxx_msan.tar.gz \
 && echo 'da938f2c2657db9903b819c227d49bdd8468f21abc5e1f93593a2bb89ef0cc3b libcxx_msan.tar.gz' | sha256sum -c - \
 && curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/20210519-210227/s390x-ibm-linux-gnu.tar.gz -o s390x-ibm-linux-gnu.tar.gz \
 && echo '6a5329112de732ce5d64465e42718c2480bf5909654663c81219509e72144bd9 s390x-ibm-linux-gnu.tar.gz' | sha256sum -c - \
 && curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/20210519-210227/x86_64-apple-darwin19.tar.gz -o x86_64-apple-darwin19.tar.gz \
 && echo '6f3a818e09f2cb7286ffdd7db43c650784ae540b5004daf9fdcc10734918a344 x86_64-apple-darwin19.tar.gz' | sha256sum -c - \
 && curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/20210519-210227/x86_64-unknown-linux-gnu.tar.gz -o x86_64-unknown-linux-gnu.tar.gz \
 && echo '788d392ae96a3261791b7b3e3a8de7fec08b22c47d368dc37783f9e607c41497 x86_64-unknown-linux-gnu.tar.gz' | sha256sum -c - \
 && curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/20210519-210227/x86_64-w64-mingw32.tar.gz -o x86_64-w64-mingw32.tar.gz \
 && echo '7e168a28cb8e187a7a9b3337eb6ec11c846a95aabe26613cb48c51a76a677260 x86_64-w64-mingw32.tar.gz' | sha256sum -c - \
 && echo *.tar.gz | xargs -n1 tar -xzf \
 && rm *.tar.gz

# libtapi is required for later versions of MacOSX.
RUN git clone https://github.com/tpoechtrager/apple-libtapi.git \
    && cd apple-libtapi \
    && git checkout a66284251b46d591ee4a0cb4cf561b92a0c138d8 \
    && ./build.sh \
    && ./install.sh \
    && cd .. \
    && rm -rf apple-libtapi

RUN mkdir -p /usr/local/lib/ccache \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-unknown-linux-gnu-cc \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-unknown-linux-gnu-c++ \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-w64-mingw32-cc \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-w64-mingw32-c++ \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/aarch64-unknown-linux-gnueabi-cc \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/aarch64-unknown-linux-gnueabi-c++ \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/s390x-ibm-linux-gnu-c++ \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/s390x-ibm-linux-gnu-cc \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-apple-darwin19-cc \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-apple-darwin19-c++

ENV PATH $PATH:/x-tools/x86_64-unknown-linux-gnu/bin:/x-tools/x86_64-w64-mingw32/bin:/x-tools/aarch64-unknown-linux-gnueabi/bin:/x-tools/s390x-ibm-linux-gnu/bin:/x-tools/x86_64-apple-darwin19/bin

# Compile GNU sed from source to pick up an unreleased change that buffers
# output. This speeds up compiles on Docker for Mac by *minutes*.
RUN git clone git://git.sv.gnu.org/sed \
 && cd sed \
 && git checkout 8e52c0aff039f0a88127ca131b060050c107b0e2 \
 && ./bootstrap \
 && ./configure \
 && make \
 && make install \
 && cd .. \
 && rm -rf sed

# We need a newer version of cmake.
#
# NOTE: When upgrading cmake, bump the rebuild counters in
# c-deps/*-rebuild to force recreating the makefiles. This prevents
# strange build errors caused by those makefiles depending on the
# installed version of cmake.
RUN curl -fsSL https://github.com/Kitware/CMake/releases/download/v3.17.0/cmake-3.17.0-Linux-x86_64.tar.gz -o cmake.tar.gz \
 && echo 'b44685227b9f9be103e305efa2075a8ccf2415807fbcf1fc192da4d36aacc9f5 cmake.tar.gz' | sha256sum -c - \
 && tar --strip-components=1 -C /usr -xzf cmake.tar.gz \
 && rm cmake.tar.gz

# Compile Go from source so that CC defaults to clang instead of gcc. This
# requires a Go toolchain to bootstrap.
#
# NB: care needs to be taken when updating this version because earlier
# releases of Go will no longer be run in CI once it is changed. Consider
# bumping the minimum allowed version of Go in /build/go-version-check.sh.
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends golang \
 && curl -fsSL https://storage.googleapis.com/golang/go1.15.11.src.tar.gz -o golang.tar.gz \
 && echo 'f25b2441d4c76cf63cde94d59bab237cc33e8a2a139040d904c8630f46d061e5 golang.tar.gz' | sha256sum -c - \
 && tar -C /usr/local -xzf golang.tar.gz \
 && rm golang.tar.gz \
 && cd /usr/local/go/src \
 && GOROOT_BOOTSTRAP=$(go env GOROOT) CC=clang CXX=clang++ ./make.bash

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
WORKDIR $GOPATH

RUN chmod -R a+w $(go env GOTOOLDIR)

# Allow Go support files in gdb.
RUN echo "add-auto-load-safe-path $(go env GOROOT)/src/runtime/runtime-gdb.py" > ~/.gdbinit

# ccache - speed up C and C++ compilation
# lsof - roachprod monitor
# netcat - roachprod monitor
# netbase - /etc/services etc
# nodejs - ui
# openjdk-8-jre - railroad diagram generation
# google-cloud-sdk - roachprod acceptance tests
# yarn - ui
# chrome - ui
# unzip - for installing awscli
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add - \
 && echo 'deb https://deb.nodesource.com/node_12.x focal main' | tee /etc/apt/sources.list.d/nodesource.list \
 && curl -fsSL https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - \
 && echo 'deb https://dl.yarnpkg.com/debian/ stable main' | tee /etc/apt/sources.list.d/yarn.list \
 && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
 && echo 'deb https://packages.cloud.google.com/apt cloud-sdk main' | tee /etc/apt/sources.list.d/gcloud.list \
 && curl -fsSL https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
 && echo "deb [arch=amd64] https://dl.google.com/linux/chrome/deb/ stable main" | tee /etc/apt/sources.list.d/google.list \
 && curl https://bazel.build/bazel-release.pub.gpg | apt-key add - \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ccache \
    google-cloud-sdk \
    lsof \
    netcat \
    netbase \
    nodejs \
    openjdk-8-jre \
    openssh-client \
    yarn \
    google-chrome-stable \
    unzip

# awscli - roachtests
# NB: we don't use apt-get because we need an up to date version of awscli
RUN curl -fsSL "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip" && \
  unzip awscli-bundle.zip && \
  ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws && \
  rm -rf awscli-bundle.zip awscli-bundle

# git - Upgrade to a more modern version
RUN DEBIAN_FRONTEND=noninteractive apt-get install dh-autoreconf libcurl4-gnutls-dev libexpat1-dev gettext libz-dev libssl-dev -y && \
    curl -fsSL https://github.com/git/git/archive/v2.29.2.zip -o "git-2.29.2.zip" && \
    unzip "git-2.29.2.zip" && \
    cd git-2.29.2 && \
    make configure && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm -rf git-2.29.2.zip git-2.29.2

ENV PATH /opt/backtrace/bin:$PATH

RUN apt-get purge -y \
    apt-transport-https \
    automake \
    autopoint \
    gettext \
    golang \
    python \
    rsync \
    texinfo \
 && apt-get autoremove -y

RUN rm -rf /tmp/* /var/lib/apt/lists/*

RUN ln -s /go/src/github.com/cockroachdb/cockroach/build/builder/mkrelease.sh /usr/local/bin/mkrelease \
    && ln -s /usr/bin/bazel-3.6.0 /usr/bin/bazel

RUN curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.2.0/autouseradd-1.2.0-amd64.tar.gz \
    | tar xz -C / --strip-components 1

COPY entrypoint.sh /usr/local/bin

ENTRYPOINT ["autouseradd", "--user", "roach", "--no-create-home", "--", "entrypoint.sh"]
