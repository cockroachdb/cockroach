FROM ubuntu:xenial-20170915

# This is the CockroachDB "builder" image, which bundles cross-compiling
# toolchains that can build CockroachDB on all supported platforms.

# WARNING: Rebuilding this image can take several hours. Keep the slower steps
# (specifically, the compilation of the release toolchains) near the top to
# minimize how often they need to be rebuilt.

# autoconf - crosstool-ng / c-deps: jemalloc
# bison - crosstool-ng
# bzip2 - crosstool-ng
# file - crosstool-ng
# flex - crosstool-ng
# g++ - crosstool-ng
# gawk - crosstool-ng
# git - crosstool-ng
# gperf - crosstool-ng
# help2man - crosstool-ng
# libncurses-dev - crosstool-ng / CRDB build system
# make - crosstool-ng / CRDB build system
# patch - crosstool-ng
# texinfo - crosstool-ng
# xz-utils - crosstool-ng / msan
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-transport-https \
    autoconf \
    bison \
    bzip2 \
    ca-certificates \
    curl \
    file \
    flex \
    g++ \
    gawk \
    git \
    gperf \
    help2man \
    libncurses-dev \
    make \
    patch \
    texinfo \
    xz-utils \
 && apt-get clean

RUN mkdir crosstool-ng \
 && curl -fsSL http://crosstool-ng.org/download/crosstool-ng/crosstool-ng-1.23.0.tar.xz | tar --strip-components=1 -C crosstool-ng -xJ \
 && cd crosstool-ng \
 && ./configure --prefix /usr/local/ct-ng \
 && make -j$(nproc) \
 && make install \
 && cp ct-ng.comp /etc/bash_completion.d/ \
 && cd .. \
 && rm -rf crosstool-ng

COPY x86_64-unknown-linux-gnu.defconfig x86_64-w64-mingw.defconfig aarch64-unknown-linux-gnueabi.defconfig ./
RUN mkdir src \
 && mkdir build && (cd build && DEFCONFIG=../x86_64-unknown-linux-gnu.defconfig      /usr/local/ct-ng/bin/ct-ng defconfig && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && mkdir build && (cd build && DEFCONFIG=../x86_64-w64-mingw.defconfig              /usr/local/ct-ng/bin/ct-ng defconfig && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && mkdir build && (cd build && DEFCONFIG=../aarch64-unknown-linux-gnueabi.defconfig /usr/local/ct-ng/bin/ct-ng defconfig && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && rm -rf src

RUN mkdir -p /usr/local/lib/ccache \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-unknown-linux-gnu-cc \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-unknown-linux-gnu-c++ \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-w64-mingw32-cc \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-w64-mingw32-c++ \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/aarch64-unknown-linux-gnueabi-cc \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/aarch64-unknown-linux-gnueabi-c++

ENV PATH $PATH:/x-tools/x86_64-unknown-linux-gnu/bin:/x-tools/x86_64-w64-mingw32/bin:/x-tools/aarch64-unknown-linux-gnueabi/bin

# Build & install the terminfo lib (incl. in ncurses) for the linux targets (x86 and arm).
# (on BSD or BSD-derived like macOS it's already built-in; on windows we don't need it.)
#
# The patch is needed to work around a bug in Debian mawk, see
# http://lists.gnu.org/archive/html/bug-ncurses/2015-08/msg00008.html
COPY ncurses.patch ./
#
# Run the three builds.
# As per the Debian rule file for ncurses, the two configure tests for
# the type of bool and poll(2) are broken when cross-compiling, so we
# need to feed the test results manually to configure via an environment
# variable; see debian/rules on the Debian ncurses source package.
#
# The configure other settings in ncurses.conf are also sourced from the
# Debian source package.
#
COPY ncurses.conf ./
RUN mkdir ncurses \
 && curl -fsSL http://ftp.gnu.org/gnu/ncurses/ncurses-6.0.tar.gz | tar --strip-components=1 -C ncurses -xz \
 && cd ncurses \
 && patch -p0 <../ncurses.patch \
 && export cf_cv_type_of_bool='unsigned char' \
 && export cf_cv_working_poll=yes \
 && mkdir build-x86_64-unknown-linux-gnu \
 && (cd build-x86_64-unknown-linux-gnu \
    && CC=/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-cc \
       CXX=/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-c++ \
       ../configure --prefix=/x-tools/x86_64-unknown-linux-gnu/x86_64-unknown-linux-gnu/sysroot/usr \
         --host=x86_64-unknown-linux-gnu \
         $(cat /ncurses.conf) \
    && make install.libs) \
 && mkdir build-aarch64-unknown-linux-gnueabi \
 && (cd build-aarch64-unknown-linux-gnueabi \
    && CC=/x-tools/aarch64-unknown-linux-gnueabi/bin/aarch64-unknown-linux-gnueabi-cc \
       CXX=/x-tools/aarch64-unknown-linux-gnueabi/bin/aarch64-unknown-linux-gnueabi-c++ \
       ../configure --prefix=/x-tools/aarch64-unknown-linux-gnueabi/aarch64-unknown-linux-gnueabi/sysroot/usr \
         --host=aarch64-unknown-linux-gnueabi \
         $(cat /ncurses.conf) \
    && make install.libs) \
 && cd .. \
 && rm -rf ncurses ncurses.conf ncurses.patch

RUN apt-get purge -y gcc g++ && apt-get autoremove -y

# clang - msan
# cmake - msan / c-deps: libroach, protobuf, et al.
# python - msan
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang \
    cmake \
    python

# Build an msan-enabled build of libc++, following instructions from
# https://github.com/google/sanitizers/wiki/MemorySanitizerLibcxxHowTo
RUN mkdir llvm                    && curl -sfSL http://releases.llvm.org/3.9.1/llvm-3.9.1.src.tar.xz      | tar --strip-components=1 -C llvm -xJ \
 && mkdir llvm/projects/libcxx    && curl -sfSL http://releases.llvm.org/3.9.1/libcxx-3.9.1.src.tar.xz    | tar --strip-components=1 -C llvm/projects/libcxx -xJ \
 && mkdir llvm/projects/libcxxabi && curl -sfSL http://releases.llvm.org/3.9.1/libcxxabi-3.9.1.src.tar.xz | tar --strip-components=1 -C llvm/projects/libcxxabi -xJ \
 && curl -fsSL https://github.com/llvm-mirror/libcxx/commit/b640da0b315ead39690d4d65c76938ab8aeb5449.patch | git -C llvm/projects/libcxx apply \
 && mkdir libcxx_msan && (cd libcxx_msan && cmake ../llvm -DCMAKE_BUILD_TYPE=Release -DLLVM_USE_SANITIZER=Memory && make cxx -j$(nproc)) \
 && rm -rf llvm

# Install osxcross. This needs the min supported osx version (we bump that
# whenever Go does, in which case the builder image stops working). The SDK
# can either be generated from Xcode or we let someone else do the work.
# See for example:
# https://github.com/docker/golang-cross/pull/11#issuecomment-428741406.
#
# See https://en.wikipedia.org/wiki/Uname for the right suffix in the `mv` step
# below. For example, Yosemite is 10.10 and has kernel release (uname -r)
# 14.0.0. Similar edits are needed in mkrelease.sh.
#
# The osxcross SHA should be bumped. It's fixed merely to avoid an obvious
# highjack of the upstream repo from slipping in unnoticed.
RUN git clone https://github.com/tpoechtrager/osxcross.git \
 && (cd osxcross && git checkout 6525b2b7d33abc371ad889f205377dc5cf81f23e) \
 && (cd osxcross/tarballs && curl -sfSL https://s3.dockerproject.org/darwin/v2/MacOSX10.10.sdk.tar.xz -O) \
 && echo "631b4144c6bf75bf7a4d480d685a9b5bda10ee8d03dbf0db829391e2ef858789 osxcross/tarballs/MacOSX10.10.sdk.tar.xz" | sha256sum -c - \
 && OSX_VERSION_MIN=10.10 PORTABLE=1 UNATTENDED=1 osxcross/build.sh \
 && mv osxcross/target /x-tools/x86_64-apple-darwin14 \
 && rm -rf osxcross

RUN ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-apple-darwin14-cc \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-apple-darwin14-c++

ENV PATH $PATH:/x-tools/x86_64-apple-darwin14/bin

# automake - sed build
# autopoint - sed build
# gettext - sed build
# rsync - sed build
RUN apt-get update && apt-get install -y --no-install-recommends \
    automake \
    autopoint \
    gettext \
    rsync

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

# xenial installs cmake 3.5.1. We need a newer version. Run this step
# after the llvm/cross-compile step which is exceedingly slow.
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
# bumping the minimum allowed version of Go in /build/go-version-chech.sh.
RUN apt-get install -y --no-install-recommends golang \
 && curl -fsSL https://storage.googleapis.com/golang/go1.14.4.src.tar.gz -o golang.tar.gz \
 && echo '7011af3bbc2ac108d1b82ea8abb87b2e63f78844f0259be20cde4d42c5c40584 golang.tar.gz' | sha256sum -c - \
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

RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add - \
 && echo 'deb https://deb.nodesource.com/node_12.x xenial main' | tee /etc/apt/sources.list.d/nodesource.list \
 && curl -fsSL https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - \
 && echo 'deb https://dl.yarnpkg.com/debian/ stable main' | tee /etc/apt/sources.list.d/yarn.list \
 && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
 && echo 'deb https://packages.cloud.google.com/apt cloud-sdk-xenial main' | tee /etc/apt/sources.list.d/gcloud.list \
 && curl -fsSL https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
 && echo "deb [arch=amd64] https://dl.google.com/linux/chrome/deb/ stable main" | tee /etc/apt/sources.list.d/google.list \
 && apt-get update

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
RUN apt-get install -y --no-install-recommends \
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
RUN apt-get install -y python-software-properties software-properties-common && \
    add-apt-repository ppa:git-core/ppa -y && \
    apt-get update && \
    apt-get install -y git

ENV PATH /opt/backtrace/bin:$PATH

RUN apt-get purge -y \
    apt-transport-https \
    automake \
    autopoint \
    bzip2 \
    file \
    flex \
    gawk \
    gettext \
    golang \
    gperf \
    help2man \
    python \
    rsync \
    texinfo \
 && apt-get autoremove -y

RUN rm -rf /tmp/* /var/lib/apt/lists/*

RUN ln -s /go/src/github.com/cockroachdb/cockroach/build/builder/mkrelease.sh /usr/local/bin/mkrelease

RUN curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.1.0/autouseradd-1.1.0-amd64.tar.gz \
    | tar xz -C /usr --strip-components 1

COPY entrypoint.sh /usr/local/bin

ENTRYPOINT ["autouseradd", "--user", "roach", "--no-create-home", "--", "entrypoint.sh"]
