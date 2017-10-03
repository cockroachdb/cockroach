FROM ubuntu:xenial-20170915

MAINTAINER Tamir Duberstein <tamird@gmail.com>

RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    curl

RUN curl -fsSL -O https://github.com/Yelp/dumb-init/releases/download/v1.2.0/dumb-init_1.2.0_amd64.deb \
 && dpkg -i dumb-init_1.2.0_amd64.deb && rm dumb-init_1.2.0_amd64.deb

ENTRYPOINT ["/usr/bin/dumb-init", "--"]

RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add - \
 && echo 'deb https://deb.nodesource.com/node_6.x xenial main' | tee /etc/apt/sources.list.d/nodesource.list \
 && curl -fsSL https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - \
 && echo 'deb https://dl.yarnpkg.com/debian/ stable main' | tee /etc/apt/sources.list.d/yarn.list

# autoconf - crosstool-ng/bootstrap / c-deps: jemalloc
# bison - crosstool-ng/configure
# bzip2 - crosstool-ng/configure
# clang - msan: -fsanitize
# cmake - msan: libcxx
# file - crosstool-ng/build
# flex - crosstool-ng/configure
# g++ - crosstool-ng/build
# gawk - crosstool-ng/configure
# git - crosstool-ng/configure
# golang - go: bootstrap
# gperf - crosstool-ng/configure
# help2man - crosstool-ng/configure
# iptables - acceptance tests' partition nemesis
# libncurses-dev - crosstool-ng/configure
# make - crosstool-ng boostrap / CRDB build system
# nodejs - ui: all
# openssh-client - terraform / jepsen
# patch - crosstool-ng/configure
# python - msan: libcxx
# texinfo - crosstool-ng/configure
# unzip - terraform
# xz-utils - msan: libcxx / CRDB build system
# yarn - ui: all
RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf \
    bison \
    bzip2 \
    clang \
    cmake \
    file \
    flex \
    g++ \
    gawk \
    git \
    golang \
    gperf \
    help2man \
    iptables \
    libncurses-dev \
    make \
    nodejs \
    openjdk-8-jre \
    openssh-client \
    patch \
    python \
    texinfo \
    unzip \
    xz-utils \
    yarn

RUN mkdir crosstool-ng \
 && curl -fsSL http://crosstool-ng.org/download/crosstool-ng/crosstool-ng-1.23.0.tar.xz | tar --strip-components=1 -C crosstool-ng -xJ \
 && cd crosstool-ng \
 && ./configure --prefix /usr/local/ct-ng \
 && make -j$(nproc) \
 && make install \
 && cp ct-ng.comp /etc/bash_completion.d/ \
 && cd .. \
 && rm -rf crosstool-ng

COPY x86_64-unknown-linux-gnu.defconfig x86_64-unknown-linux-musl.defconfig x86_64-w64-mingw.defconfig ./
RUN mkdir src \
 && mkdir build && (cd build && DEFCONFIG=../x86_64-unknown-linux-gnu.defconfig  /usr/local/ct-ng/bin/ct-ng defconfig && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && mkdir build && (cd build && DEFCONFIG=../x86_64-unknown-linux-musl.defconfig /usr/local/ct-ng/bin/ct-ng defconfig && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && mkdir build && (cd build && DEFCONFIG=../x86_64-w64-mingw.defconfig          /usr/local/ct-ng/bin/ct-ng defconfig && /usr/local/ct-ng/bin/ct-ng build) && rm -rf build \
 && rm -rf src

RUN apt-get autoremove -y gcc g++

# Build & install libtinfo (terminfo) for the linux targets.
# (on BSD or BSD-derived like macOS it's already built-in; on windows we don't need it.)
#
# The patch is needed to work around a bug in Debian mawk, see
# http://lists.gnu.org/archive/html/bug-ncurses/2015-08/msg00008.html
COPY ncurses.patch ./
#
# Run the two builds.
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
 && cd .. \
 && rm -rf ncurses ncurses.conf ncurses.patch

# Build an msan-enabled build of libc++, following instructions from
# https://github.com/google/sanitizers/wiki/MemorySanitizerLibcxxHowTo
RUN mkdir llvm                    && curl -sfSL http://releases.llvm.org/3.9.1/llvm-3.9.1.src.tar.xz      | tar --strip-components=1 -C llvm -xJ \
 && mkdir llvm/projects/libcxx    && curl -sfSL http://releases.llvm.org/3.9.1/libcxx-3.9.1.src.tar.xz    | tar --strip-components=1 -C llvm/projects/libcxx -xJ \
 && mkdir llvm/projects/libcxxabi && curl -sfSL http://releases.llvm.org/3.9.1/libcxxabi-3.9.1.src.tar.xz | tar --strip-components=1 -C llvm/projects/libcxxabi -xJ \
 && curl -fsSL https://github.com/llvm-mirror/libcxx/commit/b640da0b315ead39690d4d65c76938ab8aeb5449.patch | git -C llvm/projects/libcxx apply \
 && mkdir libcxx_msan && (cd libcxx_msan && cmake ../llvm -DCMAKE_BUILD_TYPE=Release -DLLVM_USE_SANITIZER=Memory && make cxx -j$(nproc))

RUN git clone --depth 1 https://github.com/tpoechtrager/osxcross.git \
 && (cd osxcross/tarballs && curl -sfSL https://s3.amazonaws.com/andrew-osx-sdks/MacOSX10.9.sdk.tar.xz -O) \
 && OSX_VERSION_MIN=10.9 PORTABLE=1 UNATTENDED=1 osxcross/build.sh \
 && mv osxcross/target /x-tools/x86_64-apple-darwin13 \
 && rm -rf osxcross

# BEGIN https://github.com/docker-library/golang/blob/94e49ca/1.9/alpine3.6/Dockerfile

RUN curl -fsSL https://storage.googleapis.com/golang/go1.9.src.tar.gz -o golang.tar.gz \
 && echo 'a4ab229028ed167ba1986825751463605264e44868362ca8e7accc8be057e993  golang.tar.gz' | sha256sum -c - \
 && tar -C /usr/local -xzf golang.tar.gz \
 && rm golang.tar.gz \
 && cd /usr/local/go/src \
 && GOROOT_BOOTSTRAP=$(go env GOROOT) CC=clang CXX=clang++ ./make.bash \
 && apt-get autoremove -y golang

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
WORKDIR $GOPATH

# END https://github.com/docker-library/golang/blob/94e49ca/1.9/alpine3.6/Dockerfile

RUN chmod -R a+w $(go env GOTOOLDIR)

# Allow Go support files in gdb.
RUN echo "add-auto-load-safe-path $(go env GOROOT)/src/runtime/runtime-gdb.py" > ~/.gdbinit

RUN curl -fsSL https://releases.hashicorp.com/terraform/0.8.7/terraform_0.8.7_linux_amd64.zip -o terraform.zip \
 && unzip -d /usr/local/bin terraform.zip \
 && rm terraform.zip

ENV PATH /opt/backtrace/bin:$PATH
