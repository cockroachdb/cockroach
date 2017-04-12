FROM ubuntu:xenial-20170214

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

# autoconf - crosstool-ng/bootstrap
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
# python - msan: libxx
# texinfo - crosstool-ng/configure
# unzip - terraform
# xz-utils - msan: libcxx
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
    openssh-client \
    patch \
    python \
    texinfo \
    unzip \
    xz-utils \
    yarn

# TODO(tamird): use the shallow clone when crosstool-ng 1.23 is released.
# RUN git clone --depth 1 --branch crosstool-ng-1.23.0 https://github.com/crosstool-ng/crosstool-ng \
RUN git clone https://github.com/crosstool-ng/crosstool-ng && git -C crosstool-ng checkout ee7662fa58a7f45ccd3e3520919ee70301ab7648 \
 && cd crosstool-ng \
 && ./bootstrap \
 && ./configure --prefix /usr/local/ct-ng \
 && make -j$(nproc) \
 && make install \
 && cp ct-ng.comp /etc/bash_completion.d/ \
 && cd .. \
 && rm -rf crosstool-ng

COPY x86_64-unknown-linux-gnu.defconfig x86_64-unknown-linux-musl.defconfig x86_64-w64-mingw.defconfig ./
RUN mkdir build src && cd build \
 && rm -rf * && DEFCONFIG=../x86_64-unknown-linux-gnu.defconfig  /usr/local/ct-ng/bin/ct-ng defconfig && /usr/local/ct-ng/bin/ct-ng build \
 && rm -rf * && DEFCONFIG=../x86_64-unknown-linux-musl.defconfig /usr/local/ct-ng/bin/ct-ng defconfig && /usr/local/ct-ng/bin/ct-ng build \
 && rm -rf * && DEFCONFIG=../x86_64-w64-mingw.defconfig          /usr/local/ct-ng/bin/ct-ng defconfig && /usr/local/ct-ng/bin/ct-ng build \
 && cd .. \
 && rm -rf build src

RUN apt-get autoremove -y gcc g++

# Build an msan-enabled build of libc++, following instructions from
# https://github.com/google/sanitizers/wiki/MemorySanitizerLibcxxHowTo
#
# Note that this is done before CC and CXX are set below because our cross
# compilers are too old to build libc++.
RUN mkdir llvm                    && curl -sfSL http://releases.llvm.org/3.9.1/llvm-3.9.1.src.tar.xz      | tar --strip-components=1 -C llvm -xJ \
 && mkdir llvm/projects/libcxx    && curl -sfSL http://releases.llvm.org/3.9.1/libcxx-3.9.1.src.tar.xz    | tar --strip-components=1 -C llvm/projects/libcxx -xJ \
 && mkdir llvm/projects/libcxxabi && curl -sfSL http://releases.llvm.org/3.9.1/libcxxabi-3.9.1.src.tar.xz | tar --strip-components=1 -C llvm/projects/libcxxabi -xJ \
 && curl -fsSL https://github.com/llvm-mirror/libcxx/commit/b640da0b315ead39690d4d65c76938ab8aeb5449.patch | git -C llvm/projects/libcxx apply \
 && mkdir libcxx_msan && (cd libcxx_msan && cmake ../llvm -DCMAKE_BUILD_TYPE=Release -DLLVM_USE_SANITIZER=Memory && make cxx -j$(nproc))

# We use a custom toolchain to target old Linux and glibc versions. However,
# this toolchain's libstdc++ version is quite recent and must be statically
# linked to avoid depending on the target's available libstdc++.
#
# Note that even this image does not have a recent enough libstdc++ in the
# default location. It is possible to point to the correct libstdc++ using
# LD_LIBRARY_PATH, but statically linking it is supported and much simpler.
ENV CGO_LDFLAGS '-static-libgcc -static-libstdc++'
ENV CC  /x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-gcc
ENV CXX /x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-g++

# BEGIN https://github.com/docker-library/golang/blob/master/1.8/alpine/Dockerfile

COPY parallelbuilds-go1.8.patch /
RUN curl -fsSL https://storage.googleapis.com/golang/go1.8.1.src.tar.gz -o golang.tar.gz \
 && echo '33daf4c03f86120fdfdc66bddf6bfff4661c7ca11c5da473e537f4d69b470e57  golang.tar.gz' | sha256sum -c - \
 && tar -C /usr/local -xzf golang.tar.gz \
 && rm golang.tar.gz \
 && cd /usr/local/go/src \
 && patch -p2 -i /parallelbuilds-go1.8.patch \
 && GOROOT_BOOTSTRAP=$(go env GOROOT) ./make.bash \
 && rm -rf /*.patch \
 && apt-get autoremove -y golang

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
WORKDIR $GOPATH

# END https://github.com/docker-library/golang/blob/master/1.8/alpine/Dockerfile

RUN chmod -R a+w $(go env GOTOOLDIR)

# Allow Go support files in gdb.
RUN echo "add-auto-load-safe-path $(go env GOROOT)/src/runtime/runtime-gdb.py" > ~/.gdbinit

RUN curl -fsSL https://releases.hashicorp.com/terraform/0.8.7/terraform_0.8.7_linux_amd64.zip -o terraform.zip \
 && unzip -d /usr/local/bin terraform.zip \
 && rm terraform.zip

ENV PATH /opt/backtrace/bin:$PATH
