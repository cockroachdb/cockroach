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
    xz-utils

RUN mkdir crosstool-ng \
 && curl -fsSL http://crosstool-ng.org/download/crosstool-ng/crosstool-ng-1.23.0.tar.xz | tar --strip-components=1 -C crosstool-ng -xJ \
 && cd crosstool-ng \
 && ./configure --prefix /usr/local/ct-ng \
 && make -j$(nproc) \
 && make install \
 && cp ct-ng.comp /etc/bash_completion.d/ \
 && cd .. \
 && rm -rf crosstool-ng

COPY x86_64-unknown-linux-gnu.defconfig x86_64-unknown-linux-musl.defconfig x86_64-w64-mingw.defconfig aarch64-unknown-linux-gnueabi.defconfig ./
RUN mkdir src \
 && mkdir build && (cd build && DEFCONFIG=../x86_64-unknown-linux-gnu.defconfig      /usr/local/ct-ng/bin/ct-ng defconfig && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && mkdir build && (cd build && DEFCONFIG=../x86_64-unknown-linux-musl.defconfig     /usr/local/ct-ng/bin/ct-ng defconfig && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && mkdir build && (cd build && DEFCONFIG=../x86_64-w64-mingw.defconfig              /usr/local/ct-ng/bin/ct-ng defconfig && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && mkdir build && (cd build && DEFCONFIG=../aarch64-unknown-linux-gnueabi.defconfig /usr/local/ct-ng/bin/ct-ng defconfig && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && rm -rf src

# Build & install libtinfo (terminfo) for the linux targets (x86 and arm).
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
 && mkdir build-x86_64-unknown-linux-musl \
 && (cd build-x86_64-unknown-linux-musl \
    && CC=/x-tools/x86_64-unknown-linux-musl/bin/x86_64-unknown-linux-musl-cc \
       CXX=/x-tools/x86_64-unknown-linux-musl/bin/x86_64-unknown-linux-musl-c++ \
       ../configure --prefix=/x-tools/x86_64-unknown-linux-musl/x86_64-unknown-linux-musl/sysroot/usr \
         --host=x86_64-unknown-linux-musl \
         $(cat /ncurses.conf) --without-shared --without-dlsym \
    && (cd ncurses && make all && make install.tinfo)) \
 && mkdir build-x86_64-unknown-linux-gnu \
 && (cd build-x86_64-unknown-linux-gnu \
    && CC=/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-cc \
       CXX=/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-c++ \
       ../configure --prefix=/x-tools/x86_64-unknown-linux-gnu/x86_64-unknown-linux-gnu/sysroot/usr \
         --host=x86_64-unknown-linux-gnu \
         $(cat /ncurses.conf) \
    && (cd ncurses && make all && make install.tinfo)) \
 && mkdir build-aarch64-unknown-linux-gnueabi \
 && (cd build-aarch64-unknown-linux-gnueabi \
    && CC=/x-tools/aarch64-unknown-linux-gnueabi/bin/aarch64-unknown-linux-gnueabi-cc \
       CXX=/x-tools/aarch64-unknown-linux-gnueabi/bin/aarch64-unknown-linux-gnueabi-c++ \
       ../configure --prefix=/x-tools/aarch64-unknown-linux-gnueabi/aarch64-unknown-linux-gnueabi/sysroot/usr \
         --host=aarch64-unknown-linux-gnueabi \
         $(cat /ncurses.conf) \
    && (cd ncurses && make all && make install.tinfo)) \
 && cd .. \
 && rm -rf ncurses ncurses.conf ncurses.patch

RUN apt-get purge -y gcc g++ && apt-get autoremove -y

# clang - msan
# cmake - msan / c-deps: libroach, protobuf, et al.
# python - msan
RUN apt-get install -y --no-install-recommends \
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

RUN git clone --depth 1 https://github.com/tpoechtrager/osxcross.git \
 && (cd osxcross/tarballs && curl -sfSL https://s3.amazonaws.com/andrew-osx-sdks/MacOSX10.9.sdk.tar.xz -O) \
 && OSX_VERSION_MIN=10.9 PORTABLE=1 UNATTENDED=1 osxcross/build.sh \
 && mv osxcross/target /x-tools/x86_64-apple-darwin13 \
 && rm -rf osxcross

# Compile Go from source so that CC defaults to clang instead of gcc. This
# requires a Go toolchain to bootstrap.
#
# NB: care needs to be taken when updating this version because earlier
# releases of Go will no longer be run in CI once it is changed. Consider
# bumping the minimum allowed version of Go in /build/go-version-chech.sh.
RUN apt-get install -y --no-install-recommends golang \
 && curl -fsSL https://storage.googleapis.com/golang/go1.10.src.tar.gz -o golang.tar.gz \
 && echo 'f3de49289405fda5fd1483a8fe6bd2fa5469e005fd567df64485c4fa000c7f24 golang.tar.gz' | sha256sum -c - \
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
 && echo 'deb https://deb.nodesource.com/node_6.x xenial main' | tee /etc/apt/sources.list.d/nodesource.list \
 && curl -fsSL https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - \
 && echo 'deb https://dl.yarnpkg.com/debian/ stable main' | tee /etc/apt/sources.list.d/yarn.list \
 && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
 && echo 'deb https://packages.cloud.google.com/apt cloud-sdk-xenial main' | tee /etc/apt/sources.list.d/gcloud.list \
 && apt-get update

# ccache - speed up C and C++ compilation
# nodejs - ui
# openjdk-8-jre - railroad diagram generation
# google-cloud-sdk - roachprod acceptance tests
# yarn - ui
RUN apt-get install -y --no-install-recommends \
    ccache \
    google-cloud-sdk \
    nodejs \
    openjdk-8-jre \
    openssh-client \
    yarn

# Without CCACHE_CPP2=1, ccache can generate spurious warnings. See the manpage
# for details. This option is enabled by default in ccache v3.3+, but our
# version of Ubuntu ships v3.2.4.
ENV CCACHE_CPP2=1

ENV PATH /opt/backtrace/bin:$PATH

RUN apt-get purge -y \
    apt-transport-https \
    bison \
    bzip2 \
    file \
    flex \
    gawk \
    golang \
    gperf \
    help2man \
    python \
    texinfo \
 && apt-get autoremove -y

RUN rm -rf /tmp/* /var/lib/apt/lists/*
