FROM ubuntu:focal-20210119

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
# gnupg2 - for apt
# gperf - crosstool-ng
# help2man - crosstool-ng
# libncurses-dev - crosstool-ng / CRDB build system
# libtool-bin - crosstool-ng
# make - crosstool-ng / CRDB build system
# patch - crosstool-ng
# texinfo - crosstool-ng
# xz-utils - crosstool-ng / msan
# unzip - crosstool-ng
# zlib1g[-dev] - osxcross
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
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
    gnupg2 \
    gperf \
    help2man \
    libncurses-dev \
    libtool-bin \
    make \
    patch \
    patchelf \
    texinfo \
    xz-utils \
    unzip \
    zlib1g \
    zlib1g-dev \
 && apt-get clean

RUN mkdir crosstool-ng \
 && curl -fsSL http://crosstool-ng.org/download/crosstool-ng/crosstool-ng-1.24.0.tar.xz | tar --strip-components=1 -C crosstool-ng -xJ \
 && cd crosstool-ng \
 && ./configure --prefix /usr/local/ct-ng \
 && make -j$(nproc) \
 && make -j$(nproc) bash-completion/ct-ng \
 && make install \
 && cp bash-completion/ct-ng /etc/bash_completion.d/ct-ng.comp \
 && cd .. \
 && rm -rf crosstool-ng

COPY x86_64-unknown-linux-gnu.config x86_64-w64-mingw.config aarch64-unknown-linux-gnueabi.config s390x-ibm-linux-gnu.config ./
RUN mkdir src \
 && mkdir build && (cd build && cp ../x86_64-unknown-linux-gnu.config .config && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && mkdir build && (cd build && cp ../x86_64-w64-mingw.config .config && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && mkdir build && (cd build && cp ../aarch64-unknown-linux-gnueabi.config .config && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && mkdir build && (cd build && cp ../s390x-ibm-linux-gnu.config .config && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && rm -rf src

RUN mkdir -p /usr/local/lib/ccache \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-unknown-linux-gnu-cc \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-unknown-linux-gnu-c++ \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-w64-mingw32-cc \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-w64-mingw32-c++ \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/aarch64-unknown-linux-gnueabi-cc \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/aarch64-unknown-linux-gnueabi-c++ \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/s390x-ibm-linux-gnu-c++ \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/s390x-ibm-linux-gnu-cc

ENV PATH $PATH:/x-tools/x86_64-unknown-linux-gnu/bin:/x-tools/x86_64-w64-mingw32/bin:/x-tools/aarch64-unknown-linux-gnueabi/bin:/x-tools/s390x-ibm-linux-gnu/bin

# Build & install the terminfo lib (incl. in ncurses) for the linux targets (x86, arm and s390x).
# (on BSD or BSD-derived like macOS it's already built-in; on windows we don't need it.)
#
# The patch is needed to work around a bug in Debian mawk, see
# http://lists.gnu.org/archive/html/bug-ncurses/2015-08/msg00008.html
COPY ncurses.patch ./
#
# Run the four builds.
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
 && mkdir build-aarch64-unknown-linux-gnu \
 && (cd build-aarch64-unknown-linux-gnu \
    && CC=/x-tools/aarch64-unknown-linux-gnu/bin/aarch64-unknown-linux-gnu-cc \
       CXX=/x-tools/aarch64-unknown-linux-gnu/bin/aarch64-unknown-linux-gnu-c++ \
       ../configure --prefix=/x-tools/aarch64-unknown-linux-gnu/aarch64-unknown-linux-gnu/sysroot/usr \
         --host=aarch64-unknown-linux-gnu \
         $(cat /ncurses.conf) \
    && make install.libs) \
 && mkdir build-s390x-ibm-linux-gnu \
 && (cd build-s390x-ibm-linux-gnu \
    && CC=/x-tools/s390x-ibm-linux-gnu/bin/s390x-ibm-linux-gnu-cc \
       CXX=/x-tools/s390x-ibm-linux-gnu/bin/s390x-ibm-linux-gnu-c++ \
       ../configure --prefix=/x-tools/s390x-ibm-linux-gnu/s390x-ibm-linux-gnu/sysroot/usr \
         --host=s390x-ibm-linux-gnu \
         $(cat /ncurses.conf) \
    && make install.libs) \
 && cd .. \
 && rm -rf ncurses ncurses.conf ncurses.patch

RUN apt-get purge -y gcc g++ && apt-get autoremove -y

# clang-10 - compiler
# cmake - msan / c-deps: libroach, protobuf, et al.
# libssl-dev - osxcross
# libxml2-dev - osxcross
# python - msan
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    clang-10 \
    cmake \
    libssl-dev \
    libxml2-dev \
    python \
  && update-alternatives --install /usr/bin/clang clang /usr/bin/clang-10 100 \
    --slave /usr/bin/clang++ clang++ /usr/bin/clang++-10

# Build an msan-enabled build of libc++, following instructions from
# https://github.com/google/sanitizers/wiki/MemorySanitizerLibcxxHowTo
RUN mkdir llvm                    && curl -sfSL https://github.com/llvm/llvm-project/releases/download/llvmorg-10.0.0/llvm-10.0.0.src.tar.xz      | tar --strip-components=1 -C llvm -xJ \
 && mkdir libcxx    && curl -sfSL https://github.com/llvm/llvm-project/releases/download/llvmorg-10.0.0/libcxx-10.0.0.src.tar.xz    | tar --strip-components=1 -C libcxx -xJ \
 && mkdir libcxxabi && curl -sfSL https://github.com/llvm/llvm-project/releases/download/llvmorg-10.0.0/libcxxabi-10.0.0.src.tar.xz | tar --strip-components=1 -C libcxxabi -xJ \
 && mkdir libcxx_msan && (cd libcxx_msan && cmake ../llvm -DCMAKE_BUILD_TYPE=Release -DLLVM_USE_SANITIZER=Memory -DLLVM_ENABLE_PROJECTS="libcxx;libcxxabi" && make cxx -j$(nproc)) \
 && rm -rf llvm libcxx libcxxabi

# libtapi is required for later versions of MacOSX.
RUN git clone https://github.com/tpoechtrager/apple-libtapi.git \
    && cd apple-libtapi \
    && git checkout a66284251b46d591ee4a0cb4cf561b92a0c138d8 \
    && ./build.sh \
    && ./install.sh \
    && cd .. \
    && rm -rf apple-libtapi

# Install osxcross. This needs the min supported osx version (we bump that
# whenever Go does, in which case the builder image stops working). The SDK
# can be generated from Xcode by following
# https://github.com/tpoechtrager/osxcross#packaging-the-sdk.
#
# See https://en.wikipedia.org/wiki/Uname for the right suffix in the `mv` step
# below. For example, Yosemite is 10.10 and has kernel release (uname -r)
# 14.0.0. Similar edits are needed in mkrelease.sh.
#
# The osxcross SHA should be bumped. It's fixed merely to avoid an obvious
# highjack of the upstream repo from slipping in unnoticed.
RUN git clone https://github.com/tpoechtrager/osxcross.git \
 && (cd osxcross && git checkout 9d7f6c2461dccb2b2781fff323f231a4b096fe41) \
 && (cd osxcross/tarballs && curl -sfSL https://cockroach-builder-assets.s3.amazonaws.com/MacOSX10.15.sdk.tar.xz -O) \
 && echo "c0b910e485bd24aba62b879a724c48bcb2520a8ab92067a79e3762dac0d7f47c osxcross/tarballs/MacOSX10.15.sdk.tar.xz" | sha256sum -c - \
 && OSX_VERSION_MIN=10.15 PORTABLE=1 UNATTENDED=1 osxcross/build.sh \
 && mv osxcross/target /x-tools/x86_64-apple-darwin19 \
 && rm -rf osxcross

RUN ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-apple-darwin19-cc \
 && ln -s /usr/bin/ccache /usr/local/lib/ccache/x86_64-apple-darwin19-c++

ENV PATH $PATH:/x-tools/x86_64-apple-darwin19/bin

# automake - sed build
# autopoint - sed build
# gettext - sed build
# rsync - sed build
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
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

# We need a newer version of cmake. Run this step after the llvm/cross-compile
# step which is exceedingly slow.
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
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends golang \
 && curl -fsSL https://storage.googleapis.com/golang/go1.15.14.src.tar.gz -o golang.tar.gz \
 && echo '60a4a5c48d63d0a13eca8849009b624629ff429c8bc5d1a6a8c3c4da9f34e70a golang.tar.gz' | sha256sum -c - \
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

# bazel - build system
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
 && echo "deb [arch=amd64] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list \
 && curl https://bazel.build/bazel-release.pub.gpg | apt-key add - \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    bazel-3.6.0 \
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

RUN ln -s /go/src/github.com/cockroachdb/cockroach/build/builder/mkrelease.sh /usr/local/bin/mkrelease \
    && ln -s /usr/bin/bazel-3.6.0 /usr/bin/bazel

RUN curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.2.0/autouseradd-1.2.0-amd64.tar.gz \
    | tar xz -C / --strip-components 1

COPY entrypoint.sh /usr/local/bin

ENTRYPOINT ["autouseradd", "--user", "roach", "--no-create-home", "--", "entrypoint.sh"]
